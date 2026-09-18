// transport_fake.go implements FakeTransport, a scriptable in-memory stand-in
// for MTGOTransport. It is used by bot/stream characterization tests and any
// future unit tests that need the Transport + BotBackend seam without a
// Telegram connection — no network, no mtgo imports (one-import rule, see
// transport.go).
//
// Usage shape:
//
//	ft := tgutil.NewFakeTransport()
//	ft.DefaultHandle = tgutil.FileHandle{ChatID: 1, MsgID: 2, Size: 4096}
//	ft.OnCommand("start", func(ctx context.Context, m tgutil.IncomingMsg) error { ... })
//	ft.InjectMessage(ctx, tgutil.IncomingMsg{Text: "/start hi"})
//	ft.SendHTML(ctx, 1, "<b>ok</b>", nil, 0)
//	call := ft.LastCall("SendHTML")
//
// Concurrency: all internal state (recorded calls, handlers, counters) is
// mutex-guarded and the injection/record methods are safe for concurrent use.
// The exported configuration fields (DefaultHandle, Users, Messages, ...) are
// plain fields: set them up-front, before handlers run.
package tgutil

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

// Compile-time gates: the fake must satisfy the full seam, exactly like the
// production transport.
var (
	_ Transport  = (*FakeTransport)(nil)
	_ BotBackend = (*FakeTransport)(nil)
)

// FakeNotFound is returned by FakeTransport.GetUser / ResolveUsername when the
// corresponding lookup table has no entry. Test with errors.Is.
var FakeNotFound = errors.New("tgutil(fake): not found")

// Recorded method names (FakeCall.Method). Every mutating/outbound call the
// interfaces expose is recorded; read-style calls (ResolveMedia, FetchChunk,
// Me, GetUser, ...) are scripted instead and not recorded.
const (
	CallStart           = "Start"
	CallStop            = "Stop"
	CallSendText        = "SendText"
	CallSendHTML        = "SendHTML"
	CallEditHTML        = "EditHTML"
	CallCallbackEdit    = "CallbackEditHTML"
	CallDeleteMessages  = "DeleteMessages"
	CallForwardMessages = "ForwardMessages"
	CallAnswerCallback  = "AnswerCallback"
	CallSetCommands     = "SetCommands"
	CallSendFileDoc     = "SendFileDocument"
	CallLeaveChat       = "LeaveChat"
	CallChatAction      = "SendChatAction"
)

// FakeCall is one recorded outbound call. Only the fields relevant to Method
// are populated; the rest stay zero.
type FakeCall struct {
	Method string
	Time   time.Time

	// Shared shapes.
	ChatID int64
	MsgID  int
	MsgIDs []int // DeleteMessages, ForwardMessages
	Text   string
	HTML   string
	Markup any // exactly the value passed in (nil stays nil)

	// ForwardMessages.
	DestChatID int64
	FromChatID int64
	HideAuthor bool

	// SendHTML.
	ReplyTo int

	// Callbacks.
	Query CallbackQuery
	Alert bool
	URL   string

	// SetCommands.
	Commands []BotCommand

	// SendFileDocument.
	LocalPath     string
	Caption       string
	ForceDocument bool

	// SendChatAction.
	Action string

	// ResultMsgIDs carries what ForwardMessages returned (nil unless scripted
	// by a test wrapping the fake).
	ResultMsgIDs []int
}

// FakeTransport is a scriptable Transport + BotBackend. The zero value is
// usable; NewFakeTransport exists for readability. Every outbound mutating
// call is appended to Calls; inbound traffic arrives via InjectMessage /
// InjectCallback and is dispatched to the registered handlers exactly like
// MTGOTransport dispatches mtgo updates (catch-all handlers first, then the
// command handler; longest callback prefix wins).
type FakeTransport struct {
	// --- configuration fields (set before handlers run) ---

	// DefaultHandle is returned by ResolveMedia when no resolve function is
	// scripted via SetResolve.
	DefaultHandle FileHandle
	// StartErr / StopErr are returned by Start / Stop when non-nil.
	StartErr error
	StopErr  error
	// FakeMe is returned by Me.
	FakeMe BotInfo
	// FakeDC is returned by OwnDC; 0 means "unset" and maps to DC 2.
	FakeDC int
	// Users backs GetUser (userID → profile). Missing entries fail with
	// FakeNotFound.
	Users map[int64]UserInfo
	// Usernames backs ResolveUsername (without the leading "@"). Missing
	// entries fail with FakeNotFound.
	Usernames map[string]UserInfo
	// MemberStatus backs GetChatMemberStatus as chatID → userID → status.
	// Missing entries default to "member".
	MemberStatus map[int64]map[int64]string
	// Messages backs GetMessagesBulk and GetReplyMessage as
	// chatID → msgID → message.
	Messages map[int64]map[int]IncomingMsg
	// Chats backs ResolveChat (ref → chat). Refs are the exact strings the
	// caller passes (numeric marked IDs as decimal strings, or usernames
	// without the leading "@"). Missing entries fail with FakeNotFound.
	Chats map[string]ChatInfo

	// Calls records every outbound mutating call in order (see the Call*
	// constants). Appends are mutex-guarded; tests read it between actions.
	Calls []FakeCall

	// --- internal state (mutex-guarded) ---
	mu          sync.Mutex
	injected    int
	msgSeq      int // synthetic message IDs handed out by SendHTMLMsg
	handlerErrs []error
	unrouted    []CallbackQuery
	anyMsg      []msgHandler
	commands    map[string]msgHandler
	callbacks   []callbackRoute
	resolveFn   func(ctx context.Context, chatID int64, msgID int) (FileHandle, error)
	fetchFn     func(ctx context.Context, fh FileHandle, offset int64, limit int32) ([]byte, error)
	refreshFn   func(ctx context.Context, fh *FileHandle) error
}

// fakeCommandRe matches a leading bot command: "/name", "/name@Bot" followed
// by whitespace or end of string. Mirrors how the production transport detects
// commands (BotCommand entity at offset 0) closely enough for tests.
var fakeCommandRe = regexp.MustCompile(`^/([a-z0-9_]+)(@[\w]+)?(\s|$)`)

// NewFakeTransport returns a ready-to-use fake. (The zero value works too.)
func NewFakeTransport() *FakeTransport {
	return &FakeTransport{}
}

// ---------------------------------------------------------------------------
// Handler registration
// ---------------------------------------------------------------------------

// OnCommand registers a handler for an exact command name (case-insensitive,
// leading slash tolerated), mirroring MTGOTransport.OnCommand.
func (f *FakeTransport) OnCommand(cmd string, h func(ctx context.Context, m IncomingMsg) error) {
	cmd = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(cmd), "/"))
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commands == nil {
		f.commands = make(map[string]msgHandler)
	}
	f.commands[cmd] = h
}

// OnCallback registers a handler for callback data with the given prefix.
// Longest prefix wins; at most one handler fires per query.
func (f *FakeTransport) OnCallback(prefix string, h func(ctx context.Context, q CallbackQuery) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callbacks = append(f.callbacks, callbackRoute{prefix: prefix, h: h})
}

// OnAnyMessage registers a catch-all message handler (commands included).
// Handlers run in registration order before command routing.
func (f *FakeTransport) OnAnyMessage(h func(ctx context.Context, m IncomingMsg) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.anyMsg = append(f.anyMsg, h)
}

// ---------------------------------------------------------------------------
// Inbound injection
// ---------------------------------------------------------------------------

// InjectMessage delivers m to the registered handlers: every OnAnyMessage
// handler in order, then the OnCommand handler when m is a command with a
// registered name. Command fields (IsCommand/CommandName/Args) are auto-filled
// from Text when they are not already set. The first handler error is returned
// (and recorded — see HandlerErrs).
func (f *FakeTransport) InjectMessage(ctx context.Context, m IncomingMsg) error {
	m = f.autocompleteCommand(m)

	f.mu.Lock()
	f.injected++
	anyMsg := slices.Clone(f.anyMsg)
	var cmdH msgHandler
	if m.IsCommand {
		cmdH = f.commands[strings.ToLower(m.CommandName)]
	}
	f.mu.Unlock()

	var firstErr error
	for _, h := range anyMsg {
		if err := h(ctx, m); err != nil {
			f.recordHandlerErr(err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if cmdH != nil {
		if err := cmdH(ctx, m); err != nil {
			f.recordHandlerErr(err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// autocompleteCommand fills IsCommand/CommandName/Args from Text unless the
// caller preset them.
func (f *FakeTransport) autocompleteCommand(m IncomingMsg) IncomingMsg {
	if m.IsCommand && m.CommandName != "" {
		return m
	}
	loc := fakeCommandRe.FindStringSubmatchIndex(m.Text)
	if loc == nil {
		return m
	}
	m.IsCommand = true
	m.CommandName = m.Text[loc[2]:loc[3]] // group 1: command word, already lowercase
	if loc[1] < len(m.Text) {
		m.Args = strings.TrimSpace(m.Text[loc[1]:])
	}
	return m
}

// InjectCallback delivers q to the handler registered for the longest matching
// data prefix. Unmatched queries are recorded (see UnroutedCallbacks) and
// return nil, mirroring the production transport's debug-and-drop behavior.
func (f *FakeTransport) InjectCallback(ctx context.Context, q CallbackQuery) error {
	f.mu.Lock()
	f.injected++
	best, bestLen := -1, -1
	for i, r := range f.callbacks {
		if len(r.prefix) > bestLen && strings.HasPrefix(q.Data, r.prefix) {
			best, bestLen = i, len(r.prefix)
		}
	}
	var h cbHandler
	if best >= 0 {
		h = f.callbacks[best].h
	}
	f.mu.Unlock()

	if h == nil {
		f.mu.Lock()
		f.unrouted = append(f.unrouted, q)
		f.mu.Unlock()
		return nil
	}
	if err := h(ctx, q); err != nil {
		f.recordHandlerErr(err)
		return err
	}
	return nil
}

// Injected reports how many messages and callbacks have been injected since
// the last Reset (or since creation).
func (f *FakeTransport) Injected() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.injected
}

// HandlerErrs returns the errors returned by injected handlers, in occurrence
// order. Cleared by Reset.
func (f *FakeTransport) HandlerErrs() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.handlerErrs)
}

// UnroutedCallbacks returns the callback queries that matched no registered
// prefix. Cleared by Reset.
func (f *FakeTransport) UnroutedCallbacks() []CallbackQuery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.unrouted)
}

func (f *FakeTransport) recordHandlerErr(err error) {
	f.mu.Lock()
	f.handlerErrs = append(f.handlerErrs, err)
	f.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Scripting knobs
// ---------------------------------------------------------------------------

// SetResolve scripts ResolveMedia. When unset, ResolveMedia returns the
// stored DefaultHandle.
func (f *FakeTransport) SetResolve(fn func(ctx context.Context, chatID int64, msgID int) (FileHandle, error)) {
	f.mu.Lock()
	f.resolveFn = fn
	f.mu.Unlock()
}

// SetFetchChunk scripts FetchChunk. When unset, a deterministic generator
// serves the bytes (see fakeChunk) so stream tests can assert byte-exact
// output.
func (f *FakeTransport) SetFetchChunk(fn func(ctx context.Context, fh FileHandle, offset int64, limit int32) ([]byte, error)) {
	f.mu.Lock()
	f.fetchFn = fn
	f.mu.Unlock()
}

// SetRefresh scripts RefreshFileRef. When unset it is a no-op returning nil.
func (f *FakeTransport) SetRefresh(fn func(ctx context.Context, fh *FileHandle) error) {
	f.mu.Lock()
	f.refreshFn = fn
	f.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Transport lifecycle
// ---------------------------------------------------------------------------

// Start records the call and returns StartErr when set.
func (f *FakeTransport) Start(context.Context) error {
	f.record(FakeCall{Method: CallStart})
	return f.StartErr
}

// Stop records the call and returns StopErr when set.
func (f *FakeTransport) Stop() error {
	f.record(FakeCall{Method: CallStop})
	return f.StopErr
}

// ---------------------------------------------------------------------------
// Outbound calls (recorded)
// ---------------------------------------------------------------------------

// SendText records the call and returns nil.
func (f *FakeTransport) SendText(_ context.Context, chatID int64, text string, markup any) error {
	f.record(FakeCall{Method: CallSendText, ChatID: chatID, Text: text, Markup: markup})
	return nil
}

// SendHTML records the call and returns nil.
func (f *FakeTransport) SendHTML(_ context.Context, chatID int64, html string, markup any, replyTo int) error {
	f.record(FakeCall{Method: CallSendHTML, ChatID: chatID, HTML: html, Markup: markup, ReplyTo: replyTo})
	return nil
}

// SendHTMLMsg records the call (under CallSendHTML, with MsgID set to the
// returned message ID) and returns a fresh non-zero synthetic message ID so
// status-edit flows can be asserted end to end.
func (f *FakeTransport) SendHTMLMsg(_ context.Context, chatID int64, html string, markup any, replyTo int) (int, error) {
	f.mu.Lock()
	f.msgSeq++
	id := f.msgSeq
	f.mu.Unlock()
	f.record(FakeCall{Method: CallSendHTML, ChatID: chatID, MsgID: id, HTML: html, Markup: markup, ReplyTo: replyTo})
	return id, nil
}

// EditHTML records the call and returns nil.
func (f *FakeTransport) EditHTML(_ context.Context, chatID int64, msgID int, html string, markup any) error {
	f.record(FakeCall{Method: CallEditHTML, ChatID: chatID, MsgID: msgID, HTML: html, Markup: markup})
	return nil
}

// CallbackEditHTML records the call and returns nil.
func (f *FakeTransport) CallbackEditHTML(_ context.Context, q CallbackQuery, html string, markup any) error {
	f.record(FakeCall{Method: CallCallbackEdit, Query: q, HTML: html, Markup: markup})
	return nil
}

// DeleteMessages records the call and returns nil.
func (f *FakeTransport) DeleteMessages(_ context.Context, chatID int64, msgIDs ...int) error {
	f.record(FakeCall{Method: CallDeleteMessages, ChatID: chatID, MsgIDs: slices.Clone(msgIDs)})
	return nil
}

// ForwardMessages records the call. Without extra scripting it returns no
// message IDs (nil, nil) — tests that need IDs should assert on the recorded
// call, not the result.
func (f *FakeTransport) ForwardMessages(_ context.Context, destChatID, fromChatID int64, msgIDs []int, hideAuthor bool) ([]int, error) {
	call := FakeCall{
		Method:     CallForwardMessages,
		DestChatID: destChatID,
		FromChatID: fromChatID,
		MsgIDs:     slices.Clone(msgIDs),
		HideAuthor: hideAuthor,
	}
	f.record(call)
	return call.ResultMsgIDs, nil
}

// AnswerCallback records the call and returns nil.
func (f *FakeTransport) AnswerCallback(_ context.Context, q CallbackQuery, text string, alert bool, url string) error {
	f.record(FakeCall{Method: CallAnswerCallback, Query: q, Text: text, Alert: alert, URL: url})
	return nil
}

// SetCommands records the call and returns nil.
func (f *FakeTransport) SetCommands(_ context.Context, cmds []BotCommand) error {
	f.record(FakeCall{Method: CallSetCommands, Commands: slices.Clone(cmds)})
	return nil
}

// SendFileDocument records the call and returns nil.
func (f *FakeTransport) SendFileDocument(_ context.Context, chatID int64, localPath, caption string, forceDocument bool) error {
	f.record(FakeCall{Method: CallSendFileDoc, ChatID: chatID, LocalPath: localPath, Caption: caption, ForceDocument: forceDocument})
	return nil
}

// LeaveChat records the call and returns nil.
func (f *FakeTransport) LeaveChat(_ context.Context, chatID int64) error {
	f.record(FakeCall{Method: CallLeaveChat, ChatID: chatID})
	return nil
}

// SendChatAction records the call and returns nil.
func (f *FakeTransport) SendChatAction(_ context.Context, chatID int64, action string) error {
	f.record(FakeCall{Method: CallChatAction, ChatID: chatID, Action: action})
	return nil
}

// ---------------------------------------------------------------------------
// Outbound calls (scripted / lookup-backed)
// ---------------------------------------------------------------------------

// ResolveMedia returns the scripted result, or DefaultHandle when none is set.
func (f *FakeTransport) ResolveMedia(ctx context.Context, chatID int64, msgID int) (FileHandle, error) {
	f.mu.Lock()
	fn := f.resolveFn
	def := f.DefaultHandle
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, chatID, msgID)
	}
	return def, nil
}

// FetchChunk returns the scripted result, or the deterministic default
// generator's bytes when none is set.
func (f *FakeTransport) FetchChunk(ctx context.Context, fh FileHandle, offset int64, limit int32) ([]byte, error) {
	f.mu.Lock()
	fn := f.fetchFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, fh, offset, limit)
	}
	return fakeChunk(fh, offset, limit), nil
}

// fakeChunk deterministically synthesizes chunk bytes: byte i of the chunk is
// (offset+i)&0xFF, truncated at min(limit, fh.Size-offset). A Size of 0 (or an
// offset at/after Size) therefore yields EOF (empty slice).
func fakeChunk(fh FileHandle, offset int64, limit int32) []byte {
	n := min64(int64(limit), fh.Size-offset) // min64 lives in transport.go
	if n < 0 {
		n = 0
	}
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((offset + int64(i)) & 0xFF)
	}
	return out
}

// RefreshFileRef runs the scripted refresh; without one it is a no-op.
func (f *FakeTransport) RefreshFileRef(ctx context.Context, fh *FileHandle) error {
	f.mu.Lock()
	fn := f.refreshFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, fh)
	}
	return nil
}

// Me returns the FakeMe field.
func (f *FakeTransport) Me(context.Context) (BotInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.FakeMe, nil
}

// OwnDC returns FakeDC, defaulting to 2 when unset.
func (f *FakeTransport) OwnDC(context.Context) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FakeDC > 0 {
		return f.FakeDC
	}
	return 2
}

// GetUser looks up Users; missing entries fail with FakeNotFound.
func (f *FakeTransport) GetUser(_ context.Context, userID int64) (UserInfo, error) {
	f.mu.Lock()
	u, ok := f.Users[userID]
	f.mu.Unlock()
	if !ok {
		return UserInfo{}, fmt.Errorf("%w: user %d", FakeNotFound, userID)
	}
	return u, nil
}

// ResolveUsername looks up Usernames (a leading "@" on the query is ignored);
// missing entries fail with FakeNotFound.
func (f *FakeTransport) ResolveUsername(_ context.Context, username string) (UserInfo, error) {
	username = strings.TrimPrefix(strings.TrimSpace(username), "@")
	f.mu.Lock()
	u, ok := f.Usernames[username]
	f.mu.Unlock()
	if !ok {
		return UserInfo{}, fmt.Errorf("%w: username %q", FakeNotFound, username)
	}
	return u, nil
}

// ResolveChat looks up Chats by ref (a leading "@" is ignored); missing
// entries fail with FakeNotFound.
func (f *FakeTransport) ResolveChat(_ context.Context, ref string) (ChatInfo, error) {
	ref = strings.TrimPrefix(strings.TrimSpace(ref), "@")
	f.mu.Lock()
	c, ok := f.Chats[ref]
	f.mu.Unlock()
	if !ok {
		return ChatInfo{}, fmt.Errorf("%w: chat %q", FakeNotFound, ref)
	}
	return c, nil
}

// GetChatMemberStatus reads MemberStatus (chatID → userID → status); missing
// entries default to "member".
func (f *FakeTransport) GetChatMemberStatus(_ context.Context, chatID, userID int64) (string, error) {
	f.mu.Lock()
	status := f.MemberStatus[chatID][userID]
	f.mu.Unlock()
	if status == "" {
		return "member", nil
	}
	return status, nil
}

// GetMessagesBulk synthesizes the chat's stored messages with
// minID < msgID <= maxID (no upper bound when maxID == 0), newest-limit like
// the production transport, returned in ascending (chronological) order.
func (f *FakeTransport) GetMessagesBulk(_ context.Context, chatID int64, minID, maxID, limit int) ([]IncomingMsg, error) {
	f.mu.Lock()
	byID := f.Messages[chatID]
	f.mu.Unlock()
	if limit <= 0 || len(byID) == 0 {
		return nil, nil
	}
	ids := make([]int, 0, len(byID))
	for id := range byID {
		if id > minID && (maxID == 0 || id <= maxID) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	if len(ids) > limit {
		ids = ids[len(ids)-limit:] // newest `limit`, like GetChatHistory
	}
	out := make([]IncomingMsg, 0, len(ids))
	for _, id := range ids {
		out = append(out, byID[id])
	}
	return out, nil
}

// GetReplyMessage resolves the message that msgID replies to using the
// Messages table (like the production transport: fetch msgID, follow its
// ReplyToMsgID). Missing entries wrap ErrStaleMedia; a message without a
// reply is a plain error.
func (f *FakeTransport) GetReplyMessage(_ context.Context, chatID int64, msgID int) (IncomingMsg, error) {
	f.mu.Lock()
	byID := f.Messages[chatID]
	f.mu.Unlock()
	base, ok := byID[msgID]
	if !ok {
		return IncomingMsg{}, fmt.Errorf("%w: message %d in chat %d", ErrStaleMedia, msgID, chatID)
	}
	if base.ReplyToMsgID == 0 {
		return IncomingMsg{}, fmt.Errorf("tgutil(fake): message %d in chat %d has no reply", msgID, chatID)
	}
	reply, ok := byID[base.ReplyToMsgID]
	if !ok {
		return IncomingMsg{}, fmt.Errorf("%w: reply target %d in chat %d", ErrStaleMedia, base.ReplyToMsgID, chatID)
	}
	return reply, nil
}

// ---------------------------------------------------------------------------
// Recording helpers
// ---------------------------------------------------------------------------

// record appends c to Calls under the mutex.
func (f *FakeTransport) record(c FakeCall) {
	c.Time = time.Now()
	f.mu.Lock()
	f.Calls = append(f.Calls, c)
	f.mu.Unlock()
}

// CallsOf returns every recorded call with the given method name.
func (f *FakeTransport) CallsOf(method string) []FakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []FakeCall
	for _, c := range f.Calls {
		if c.Method == method {
			out = append(out, c)
		}
	}
	return out
}

// LastCall returns the most recent recorded call with the given method name.
func (f *FakeTransport) LastCall(method string) (FakeCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.Calls) - 1; i >= 0; i-- {
		if f.Calls[i].Method == method {
			return f.Calls[i], true
		}
	}
	return FakeCall{}, false
}

// Reset clears recorded calls, handler errors, unrouted callbacks and the
// injection counter. Handlers and scripting/configuration survive.
func (f *FakeTransport) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = nil
	f.injected = 0
	f.handlerErrs = nil
	f.unrouted = nil
}
