// transport_test.go gates the transport seam: the error taxonomy, the filter
// combinators, the keyboard builder and the FakeTransport scripting surface.
// It must stay mtgo-free (one-import rule, see transport.go) — mtgo-shaped
// errors are built with NewFloodWaitTestError / NewFloodPremiumWaitTestError
// and mtgo-shaped locations with NewTestDocumentLocation / NewTestPhotoLocation.
package tgutil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// fakeTimeoutNetError is a net.Error with Timeout() == true, used to exercise
// Classify's network-timeout branch without importing mtgo.
type fakeTimeoutNetError struct{}

func (fakeTimeoutNetError) Error() string   { return "i/o timeout" }
func (fakeTimeoutNetError) Timeout() bool   { return true }
func (fakeTimeoutNetError) Temporary() bool { return true }

var _ net.Error = fakeTimeoutNetError{}

func TestMapFloodWait(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		err     error
		wantSec int
		wantOK  bool
	}{
		{"nil", nil, 0, false},
		{"plain error", errors.New("boom"), 0, false},
		{"flood wait via constructor", NewFloodWaitTestError(42), 42, true},
		{"premium flood wait via constructor", NewFloodPremiumWaitTestError(13), 13, true},
		{"tgutil FloodWait", &FloodWait{Seconds: 5}, 5, true},
		{"RPCError FLOOD_WAIT", &RPCError{Code: 420, Type: "FLOOD_WAIT", Message: "FLOOD_WAIT_9", Argument: 9}, 9, true},
		{"RPCError FLOOD_PREMIUM_WAIT", &RPCError{Code: 420, Type: "FLOOD_PREMIUM_WAIT", Argument: 30}, 30, true},
		{"wrapped constructor error", fmt.Errorf("send failed: %w", NewFloodWaitTestError(7)), 7, true},
		{"wrapped FloodWait", fmt.Errorf("chunk: %w", &FloodWait{Seconds: 11}), 11, true},
		{"wrapped RPCError flood", fmt.Errorf("rpc: %w", &RPCError{Type: "FLOOD_PREMIUM_WAIT", Argument: 30}), 30, true},
		{"wrapped non-flood", fmt.Errorf("chunk: %w", errors.New("boom")), 0, false},
		{"non-flood RPCError", &RPCError{Code: 400, Type: "CHAT_WRITE_FORBIDDEN"}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sec, ok := MapFloodWait(tt.err)
			if ok != tt.wantOK || sec != tt.wantSec {
				t.Errorf("MapFloodWait(%v) = (%d, %v), want (%d, %v)", tt.err, sec, ok, tt.wantSec, tt.wantOK)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want ErrClass
	}{
		// Documented: nil has nothing to classify → ErrClassOther.
		{"nil", nil, ErrClassOther},
		{"flood wait error", NewFloodWaitTestError(42), ErrClassFlood},
		{"tgutil FloodWait", &FloodWait{Seconds: 3}, ErrClassFlood},
		{"RPCError flood", &RPCError{Type: "FLOOD_WAIT", Argument: 60}, ErrClassFlood},
		{"stale media", ErrStaleMedia, ErrClassPermanent},
		{"wrapped stale media", fmt.Errorf("resolve: %w", ErrStaleMedia), ErrClassPermanent},
		{"media missing", ErrMediaMissing, ErrClassPermanent},
		{"record mismatch", ErrRecordMismatch, ErrClassPermanent},
		{"media unsupported", ErrMediaUnsupported, ErrClassPermanent},
		{"rpc MESSAGE_ID_INVALID", &RPCError{Code: 400, Type: "MESSAGE_ID_INVALID"}, ErrClassPermanent},
		{"rpc MESSAGE_DELETED", &RPCError{Code: 400, Type: "MESSAGE_DELETED"}, ErrClassPermanent},
		{"rpc CHANNEL_INVALID", &RPCError{Code: 400, Type: "CHANNEL_INVALID"}, ErrClassPermanent},
		{"rpc CHANNEL_PRIVATE", &RPCError{Code: 400, Type: "CHANNEL_PRIVATE"}, ErrClassPermanent},
		{"file ref expired is healable", ErrFileRefExpired, ErrClassTransient},
		{"transient sentinel", ErrTransient, ErrClassTransient},
		{"context deadline", context.DeadlineExceeded, ErrClassTransient},
		{"net timeout", fakeTimeoutNetError{}, ErrClassTransient},
		{"wrapped net timeout", fmt.Errorf("dial: %w", fakeTimeoutNetError{}), ErrClassTransient},
		{"rpc 5xx", &RPCError{Code: 502, Type: "WHATEVER"}, ErrClassTransient},
		{"rpc TIMEOUT type", &RPCError{Code: 420, Type: "TIMEOUT", Argument: 3}, ErrClassTransient},
		{"plain network error", errors.New("connection reset"), ErrClassOther},
		{"unclassified rpc", &RPCError{Code: 400, Type: "CHAT_WRITE_FORBIDDEN"}, ErrClassOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Classify(tt.err); got != tt.want {
				t.Errorf("Classify(%v) = %s, want %s", tt.err, got, tt.want)
			}
		})
	}

	t.Run("class names", func(t *testing.T) {
		t.Parallel()
		names := map[ErrClass]string{
			ErrClassOther:     "other",
			ErrClassPermanent: "permanent",
			ErrClassTransient: "transient",
			ErrClassFlood:     "flood",
		}
		for class, want := range names {
			if got := class.String(); got != want {
				t.Errorf("ErrClass(%d).String() = %q, want %q", int(class), got, want)
			}
		}
	})
}

func TestFakeTransportHandlers(t *testing.T) {
	ctx := context.Background()

	t.Run("routes commands and catch-all", func(t *testing.T) {
		t.Parallel()
		ft := NewFakeTransport()
		var anyMsgs []IncomingMsg
		ft.OnAnyMessage(func(_ context.Context, m IncomingMsg) error {
			anyMsgs = append(anyMsgs, m)
			return nil
		})
		var started []string
		ft.OnCommand("start", func(_ context.Context, m IncomingMsg) error {
			started = append(started, m.CommandName+":"+m.Args)
			return nil
		})

		if err := ft.InjectMessage(ctx, IncomingMsg{ChatID: 1, Text: "/start hi"}); err != nil {
			t.Fatalf("InjectMessage: %v", err)
		}
		if len(anyMsgs) != 1 {
			t.Fatalf("catch-all handler calls = %d, want 1", len(anyMsgs))
		}
		m := anyMsgs[0]
		if !m.IsCommand || m.CommandName != "start" || m.Args != "hi" {
			t.Errorf("auto-filled command = (%v, %q, %q), want (true, \"start\", \"hi\")", m.IsCommand, m.CommandName, m.Args)
		}
		if len(started) != 1 || started[0] != "start:hi" {
			t.Errorf("command handler got %v, want [start:hi]", started)
		}
		if got := ft.Injected(); got != 1 {
			t.Errorf("Injected() = %d, want 1", got)
		}
	})

	t.Run("parses bot-suffixed and bare commands", func(t *testing.T) {
		t.Parallel()
		ft := NewFakeTransport()
		var args []string
		ft.OnCommand("help", func(_ context.Context, m IncomingMsg) error {
			args = append(args, m.CommandName+"|"+m.Args)
			return nil
		})

		for _, in := range []IncomingMsg{
			{ChatID: 1, Text: "/help@MyBot"},
			{ChatID: 1, Text: "/help@OtherBot hi there"},
			{ChatID: 1, Text: "/help"},
		} {
			if err := ft.InjectMessage(ctx, in); err != nil {
				t.Fatalf("InjectMessage(%q): %v", in.Text, err)
			}
		}
		want := []string{"help|", "help|hi there", "help|"}
		if !slices.Equal(args, want) {
			t.Errorf("handler args = %q, want %q", args, want)
		}
	})

	t.Run("leaves non-commands to catch-all", func(t *testing.T) {
		t.Parallel()
		ft := NewFakeTransport()
		called := false
		ft.OnCommand("start", func(context.Context, IncomingMsg) error {
			called = true
			return nil
		})
		var got IncomingMsg
		ft.OnAnyMessage(func(_ context.Context, m IncomingMsg) error { got = m; return nil })

		if err := ft.InjectMessage(ctx, IncomingMsg{ChatID: 1, Text: "plain text"}); err != nil {
			t.Fatalf("InjectMessage: %v", err)
		}
		if called {
			t.Error("command handler fired for non-command text")
		}
		if got.IsCommand {
			t.Errorf("IsCommand = true for plain text, want false")
		}
	})

	t.Run("uppercase command words are not auto-detected", func(t *testing.T) {
		t.Parallel()
		ft := NewFakeTransport()
		var got IncomingMsg
		ft.OnAnyMessage(func(_ context.Context, m IncomingMsg) error { got = m; return nil })
		if err := ft.InjectMessage(ctx, IncomingMsg{ChatID: 1, Text: "/START hi"}); err != nil {
			t.Fatalf("InjectMessage: %v", err)
		}
		if got.IsCommand {
			t.Error("IsCommand = true for /START, want false (regex is lowercase-only)")
		}
	})

	t.Run("respects preset command fields", func(t *testing.T) {
		t.Parallel()
		ft := NewFakeTransport()
		var got []string
		ft.OnCommand("custom", func(_ context.Context, m IncomingMsg) error {
			got = append(got, m.CommandName+":"+m.Args)
			return nil
		})
		ft.OnCommand("start", func(context.Context, IncomingMsg) error {
			got = append(got, "start")
			return nil
		})
		err := ft.InjectMessage(ctx, IncomingMsg{ChatID: 1, IsCommand: true, CommandName: "custom", Args: "x", Text: "/start hi"})
		if err != nil {
			t.Fatalf("InjectMessage: %v", err)
		}
		if !slices.Equal(got, []string{"custom:x"}) {
			t.Errorf("routed handlers = %q, want [custom:x] (preset fields win)", got)
		}
	})

	t.Run("propagates handler errors", func(t *testing.T) {
		t.Parallel()
		errAny := errors.New("any failed")
		errCmd := errors.New("cmd failed")

		ft := NewFakeTransport()
		ft.OnAnyMessage(func(context.Context, IncomingMsg) error { return errAny })
		ft.OnCommand("boom", func(context.Context, IncomingMsg) error { return errCmd })
		err := ft.InjectMessage(ctx, IncomingMsg{ChatID: 1, Text: "/boom now"})
		if !errors.Is(err, errAny) {
			t.Errorf("InjectMessage err = %v, want first handler failure (errAny)", err)
		}
		errs := ft.HandlerErrs()
		if len(errs) != 2 || !errors.Is(errs[0], errAny) || !errors.Is(errs[1], errCmd) {
			t.Errorf("HandlerErrs() = %v, want [errAny, errCmd]", errs)
		}

		ft2 := NewFakeTransport()
		ft2.OnCommand("boom", func(context.Context, IncomingMsg) error { return errCmd })
		if err := ft2.InjectMessage(ctx, IncomingMsg{ChatID: 1, Text: "/boom"}); !errors.Is(err, errCmd) {
			t.Errorf("InjectMessage err = %v, want errCmd", err)
		}
	})

	t.Run("callback longest prefix wins", func(t *testing.T) {
		t.Parallel()
		ft := NewFakeTransport()
		var got []string
		ft.OnCallback("help", func(_ context.Context, q CallbackQuery) error {
			got = append(got, "help:"+q.Data)
			return nil
		})
		ft.OnCallback("help_page", func(_ context.Context, q CallbackQuery) error {
			got = append(got, "help_page:"+q.Data)
			return nil
		})

		if err := ft.InjectCallback(ctx, CallbackQuery{ID: 1, ChatID: 2, MessageID: 3, Data: "help_page:x"}); err != nil {
			t.Fatalf("InjectCallback: %v", err)
		}
		if err := ft.InjectCallback(ctx, CallbackQuery{ID: 2, Data: "help:1"}); err != nil {
			t.Fatalf("InjectCallback: %v", err)
		}
		want := []string{"help_page:help_page:x", "help:help:1"}
		if !slices.Equal(got, want) {
			t.Errorf("routed callbacks = %q, want %q", got, want)
		}
		if got := ft.Injected(); got != 2 {
			t.Errorf("Injected() = %d, want 2 (callbacks count too)", got)
		}
	})

	t.Run("unrouted callbacks are recorded, not errors", func(t *testing.T) {
		t.Parallel()
		ft := NewFakeTransport()
		ft.OnCallback("help", func(context.Context, CallbackQuery) error { return nil })
		if err := ft.InjectCallback(ctx, CallbackQuery{ID: 3, Data: "nomatch"}); err != nil {
			t.Fatalf("InjectCallback(nomatch) = %v, want nil", err)
		}
		unrouted := ft.UnroutedCallbacks()
		if len(unrouted) != 1 || unrouted[0].Data != "nomatch" {
			t.Errorf("UnroutedCallbacks() = %v, want [nomatch]", unrouted)
		}
	})

	t.Run("callback handler errors propagate", func(t *testing.T) {
		t.Parallel()
		ft := NewFakeTransport()
		errCb := errors.New("cb failed")
		ft.OnCallback("cb", func(context.Context, CallbackQuery) error { return errCb })
		if err := ft.InjectCallback(ctx, CallbackQuery{Data: "cb:1"}); !errors.Is(err, errCb) {
			t.Errorf("InjectCallback err = %v, want errCb", err)
		}
		if errs := ft.HandlerErrs(); len(errs) != 1 {
			t.Errorf("HandlerErrs() = %v, want 1 entry", errs)
		}
	})
}

func TestFakeTransportRecording(t *testing.T) {
	t.Parallel()
	ft := NewFakeTransport()
	ctx := context.Background()

	var routed []string
	ft.OnCommand("ping", func(_ context.Context, m IncomingMsg) error {
		routed = append(routed, m.CommandName)
		return nil
	})

	mk := NewKeyboard().Callback("b", "d").Build()
	if err := ft.SendHTML(ctx, 111, "<b>hi</b>", mk, 42); err != nil {
		t.Fatalf("SendHTML: %v", err)
	}
	if err := ft.SendText(ctx, 111, "plain", nil); err != nil {
		t.Fatalf("SendText: %v", err)
	}
	pm := &mk
	if err := ft.SendText(ctx, 112, "ptr", pm); err != nil {
		t.Fatalf("SendText: %v", err)
	}
	q := CallbackQuery{ID: 7, ChatID: 111, SenderID: 5, MessageID: 42, Data: "cb:1"}
	if err := ft.AnswerCallback(ctx, q, "done", true, "https://x"); err != nil {
		t.Fatalf("AnswerCallback: %v", err)
	}
	if err := ft.DeleteMessages(ctx, 111, 1, 2, 3); err != nil {
		t.Fatalf("DeleteMessages: %v", err)
	}
	if ids, err := ft.ForwardMessages(ctx, 222, 111, []int{5, 6}, true); err != nil || ids != nil {
		t.Fatalf("ForwardMessages = (%v, %v), want (nil, nil)", ids, err)
	}
	if err := ft.SendChatAction(ctx, 111, "typing"); err != nil {
		t.Fatalf("SendChatAction: %v", err)
	}
	if err := ft.SetCommands(ctx, []BotCommand{{Command: "start", Description: "Start the bot"}}); err != nil {
		t.Fatalf("SetCommands: %v", err)
	}
	if err := ft.SendFileDocument(ctx, 111, "/tmp/report.bin", "cap", true); err != nil {
		t.Fatalf("SendFileDocument: %v", err)
	}
	if err := ft.LeaveChat(ctx, 111); err != nil {
		t.Fatalf("LeaveChat: %v", err)
	}

	c, ok := ft.LastCall("SendHTML")
	if !ok {
		t.Fatal("no SendHTML call recorded")
	}
	if c.ChatID != 111 || c.HTML != "<b>hi</b>" || c.ReplyTo != 42 {
		t.Errorf("SendHTML call = (%d, %q, reply %d), want (111, <b>hi</b>, 42)", c.ChatID, c.HTML, c.ReplyTo)
	}
	if !reflect.DeepEqual(c.Markup, mk) {
		t.Errorf("SendHTML markup = %#v, want %#v", c.Markup, mk)
	}
	if c.Time.IsZero() {
		t.Error("recorded call has zero Time")
	}

	sendTexts := ft.CallsOf("SendText")
	if len(sendTexts) != 2 {
		t.Fatalf("CallsOf(SendText) = %d, want 2", len(sendTexts))
	}
	if sendTexts[0].Markup != nil {
		t.Errorf("nil Markup became %#v, want nil (pass-through)", sendTexts[0].Markup)
	}
	if sendTexts[1].ChatID != 112 {
		t.Errorf("second SendText chat = %d, want 112", sendTexts[1].ChatID)
	}
	if ptr, isPtr := sendTexts[1].Markup.(*Markup); !isPtr || ptr != pm {
		t.Errorf("*Markup identity lost: got %#v, want same pointer", sendTexts[1].Markup)
	}

	c, ok = ft.LastCall("AnswerCallback")
	if !ok || c.Query != q || c.Text != "done" || !c.Alert || c.URL != "https://x" {
		t.Errorf("AnswerCallback call = %#v, want args intact", c)
	}

	c, ok = ft.LastCall("DeleteMessages")
	if !ok || c.ChatID != 111 || !slices.Equal(c.MsgIDs, []int{1, 2, 3}) {
		t.Errorf("DeleteMessages call = %#v, want chat 111 ids [1 2 3]", c)
	}

	c, ok = ft.LastCall("ForwardMessages")
	if !ok || c.DestChatID != 222 || c.FromChatID != 111 || !slices.Equal(c.MsgIDs, []int{5, 6}) || !c.HideAuthor {
		t.Errorf("ForwardMessages call = %#v, want args intact", c)
	}

	if len(ft.CallsOf("NoSuchMethod")) != 0 {
		t.Error("CallsOf matched an unrecorded method")
	}
	// 10 outbound calls above (SendHTML, 2×SendText, AnswerCallback,
	// DeleteMessages, ForwardMessages, SendChatAction, SetCommands,
	// SendFileDocument, LeaveChat) — Start/Stop excluded (not called here).
	if len(ft.Calls) != 10 {
		t.Errorf("len(Calls) = %d, want 10", len(ft.Calls))
	}

	// Start/Stop record and surface their error fields.
	ft2 := NewFakeTransport()
	errStart := errors.New("no network")
	errStop := errors.New("stop failed")
	ft2.StartErr = errStart
	ft2.StopErr = errStop
	if err := ft2.Start(ctx); !errors.Is(err, errStart) {
		t.Errorf("Start err = %v, want errStart", err)
	}
	if err := ft2.Stop(); !errors.Is(err, errStop) {
		t.Errorf("Stop err = %v, want errStop", err)
	}
	if _, ok := ft2.LastCall("Start"); !ok {
		t.Error("Start call not recorded")
	}
	if _, ok := ft2.LastCall("Stop"); !ok {
		t.Error("Stop call not recorded")
	}

	// Reset clears calls (and counters) but keeps handlers.
	ft.Reset()
	if len(ft.Calls) != 0 || len(ft.CallsOf("SendHTML")) != 0 {
		t.Errorf("Reset left calls behind: %d total", len(ft.Calls))
	}
	if _, ok := ft.LastCall("SendHTML"); ok {
		t.Error("LastCall found a call after Reset")
	}
	if got := ft.Injected(); got != 0 {
		t.Errorf("Injected() = %d after Reset, want 0", got)
	}
	if err := ft.InjectMessage(ctx, IncomingMsg{ChatID: 111, Text: "/ping"}); err != nil {
		t.Fatalf("InjectMessage after Reset: %v", err)
	}
	if !slices.Equal(routed, []string{"ping"}) {
		t.Errorf("handlers lost after Reset: routed = %q", routed)
	}
}

func TestFakeTransportChunks(t *testing.T) {
	t.Parallel()
	ft := NewFakeTransport()
	ctx := context.Background()

	t.Run("default generator is deterministic and size-clamped", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name      string
			size      int64
			offset    int64
			limit     int32
			wantLen   int
			wantFirst byte
			wantLast  byte
		}{
			{"short file", 10, 0, 4096, 10, 0x00, 0x09},
			{"full 1k chunk", 5000, 0, 4096, 4096, 0x00, 0xFF},
			{"tail chunk", 5000, 4096, 4096, 904, 0x00, 0x87},
			{"empty file is EOF", 0, 0, 4096, 0, 0, 0},
			{"offset past EOF", 10, 16, 4096, 0, 0, 0},
			{"limit clamped to remaining", 10, 8, 4, 2, 0x08, 0x09},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				b, err := ft.FetchChunk(ctx, FileHandle{ChatID: 1, MsgID: 2, Size: tt.size}, tt.offset, tt.limit)
				if err != nil {
					t.Fatalf("FetchChunk: %v", err)
				}
				if len(b) != tt.wantLen {
					t.Fatalf("len = %d, want %d", len(b), tt.wantLen)
				}
				if tt.wantLen == 0 {
					return
				}
				if b[0] != tt.wantFirst || b[len(b)-1] != tt.wantLast {
					t.Errorf("bytes = %x..%x, want %x..%x", b[0], b[len(b)-1], tt.wantFirst, tt.wantLast)
				}
				for i := range b {
					if want := byte((tt.offset + int64(i)) & 0xFF); b[i] != want {
						t.Fatalf("byte %d = %#x, want %#x", i, b[i], want)
					}
				}
			})
		}

		fh := FileHandle{ChatID: 1, MsgID: 2, Size: 10}
		b1, err := ft.FetchChunk(ctx, fh, 0, 4096)
		if err != nil {
			t.Fatalf("FetchChunk: %v", err)
		}
		b2, err := ft.FetchChunk(ctx, fh, 0, 4096)
		if err != nil {
			t.Fatalf("FetchChunk: %v", err)
		}
		if !slices.Equal(b1, b2) {
			t.Error("same offset+limit produced different bytes")
		}
	})

	t.Run("scripted fetch overrides and errors", func(t *testing.T) {
		t.Parallel()
		ft := NewFakeTransport()
		ft.SetFetchChunk(func(_ context.Context, _ FileHandle, _ int64, _ int32) ([]byte, error) {
			return nil, fmt.Errorf("%w: getFile blew up", ErrTransient)
		})
		_, err := ft.FetchChunk(ctx, FileHandle{Size: 10}, 0, 4096)
		if !errors.Is(err, ErrTransient) {
			t.Fatalf("FetchChunk err = %v, want ErrTransient", err)
		}

		want := []byte{9, 9, 9}
		ft.SetFetchChunk(func(_ context.Context, _ FileHandle, _ int64, _ int32) ([]byte, error) {
			return want, nil
		})
		got, err := ft.FetchChunk(ctx, FileHandle{Size: 10}, 0, 4096)
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("FetchChunk = (%x, %v), want (%x, nil)", got, err, want)
		}
	})
}

func TestFakeTransportLookups(t *testing.T) {
	t.Parallel()
	ft := NewFakeTransport()
	ctx := context.Background()

	ft.Users = map[int64]UserInfo{
		7: {ID: 7, FirstName: "Ada", Username: "ada", PhotoDC: 2},
	}
	u, err := ft.GetUser(ctx, 7)
	if err != nil || u.FirstName != "Ada" || u.PhotoDC != 2 {
		t.Errorf("GetUser(7) = (%#v, %v), want Ada", u, err)
	}
	if _, err := ft.GetUser(ctx, 99); !errors.Is(err, FakeNotFound) {
		t.Errorf("GetUser(99) err = %v, want FakeNotFound", err)
	}

	ft.Usernames = map[string]UserInfo{"ada": {ID: 7, Username: "ada"}}
	if u, err := ft.ResolveUsername(ctx, "@ada"); err != nil || u.ID != 7 {
		t.Errorf("ResolveUsername(@ada) = (%#v, %v), want ada", u, err)
	}
	if _, err := ft.ResolveUsername(ctx, "ghost"); !errors.Is(err, FakeNotFound) {
		t.Errorf("ResolveUsername(ghost) err = %v, want FakeNotFound", err)
	}

	ft.MemberStatus = map[int64]map[int64]string{100: {5: "administrator"}}
	if s, _ := ft.GetChatMemberStatus(ctx, 100, 5); s != "administrator" {
		t.Errorf("GetChatMemberStatus(100,5) = %q, want administrator", s)
	}
	if s, _ := ft.GetChatMemberStatus(ctx, 100, 6); s != "member" {
		t.Errorf("GetChatMemberStatus(100,6) = %q, want default member", s)
	}

	ft.Messages = map[int64]map[int]IncomingMsg{
		200: {
			1: {ChatID: 200, MsgID: 1, Text: "first"},
			2: {ChatID: 200, MsgID: 2, Text: "second"},
			3: {ChatID: 200, MsgID: 3, Text: "third", ReplyToMsgID: 1},
		},
	}
	msgs, err := ft.GetMessagesBulk(ctx, 200, 0, 0, 10)
	if err != nil || len(msgs) != 3 || msgs[0].MsgID != 1 || msgs[2].MsgID != 3 {
		t.Fatalf("GetMessagesBulk = (%v, %v), want ids [1 2 3] ascending", msgs, err)
	}
	if msgs, _ := ft.GetMessagesBulk(ctx, 200, 1, 2, 10); len(msgs) != 1 || msgs[0].MsgID != 2 {
		t.Errorf("GetMessagesBulk(1,2) = %v, want [2]", msgs)
	}
	if msgs, _ := ft.GetMessagesBulk(ctx, 200, 0, 0, 2); len(msgs) != 2 || msgs[0].MsgID != 2 || msgs[1].MsgID != 3 {
		t.Errorf("GetMessagesBulk(limit 2) = %v, want newest two [2 3]", msgs)
	}
	if msgs, err := ft.GetMessagesBulk(ctx, 300, 0, 0, 10); err != nil || len(msgs) != 0 {
		t.Errorf("GetMessagesBulk(unknown chat) = (%v, %v), want empty", msgs, err)
	}

	reply, err := ft.GetReplyMessage(ctx, 200, 3)
	if err != nil || reply.MsgID != 1 || reply.Text != "first" {
		t.Errorf("GetReplyMessage(3) = (%#v, %v), want msg 1", reply, err)
	}
	if _, err := ft.GetReplyMessage(ctx, 200, 2); err == nil || !strings.Contains(err.Error(), "no reply") {
		t.Errorf("GetReplyMessage(2) err = %v, want no-reply error", err)
	}
	if _, err := ft.GetReplyMessage(ctx, 200, 99); !errors.Is(err, ErrStaleMedia) {
		t.Errorf("GetReplyMessage(99) err = %v, want ErrStaleMedia", err)
	}

	ft.FakeMe = BotInfo{ID: 42, Username: "testbot", DC: 4}
	if me, err := ft.Me(ctx); err != nil || me != ft.FakeMe {
		t.Errorf("Me() = (%#v, %v), want FakeMe", me, err)
	}
	if dc := ft.OwnDC(ctx); dc != 2 {
		t.Errorf("OwnDC() = %d, want default 2", dc)
	}
	ft.FakeDC = 5
	if dc := ft.OwnDC(ctx); dc != 5 {
		t.Errorf("OwnDC() = %d, want FakeDC 5", dc)
	}

	// ResolveMedia: DefaultHandle by default, scripted fn overrides.
	ft.DefaultHandle = FileHandle{ChatID: 9, MsgID: 8, Size: 3}
	if got, err := ft.ResolveMedia(ctx, 1, 1); err != nil || got != ft.DefaultHandle {
		t.Errorf("ResolveMedia = (%#v, %v), want DefaultHandle", got, err)
	}
	ft.SetResolve(func(_ context.Context, chatID int64, msgID int) (FileHandle, error) {
		return FileHandle{ChatID: chatID, MsgID: msgID, Name: "scripted"}, nil
	})
	if got, err := ft.ResolveMedia(ctx, 3, 4); err != nil || got.Name != "scripted" || got.ChatID != 3 || got.MsgID != 4 {
		t.Errorf("ResolveMedia = (%#v, %v), want scripted handle", got, err)
	}
	ft.SetResolve(func(_ context.Context, _ int64, _ int) (FileHandle, error) {
		return FileHandle{}, fmt.Errorf("%w: gone", ErrStaleMedia)
	})
	if _, err := ft.ResolveMedia(ctx, 1, 1); !errors.Is(err, ErrStaleMedia) {
		t.Errorf("ResolveMedia err = %v, want ErrStaleMedia", err)
	}

	// RefreshFileRef: no-op by default, scripted otherwise.
	fh := FileHandle{Size: 5}
	if err := ft.RefreshFileRef(ctx, &fh); err != nil || fh.Size != 5 {
		t.Errorf("default RefreshFileRef = (%v, size %d), want nil no-op", err, fh.Size)
	}
	ft.SetRefresh(func(_ context.Context, h *FileHandle) error {
		h.Size = 99
		return nil
	})
	if err := ft.RefreshFileRef(ctx, &fh); err != nil || fh.Size != 99 {
		t.Errorf("scripted RefreshFileRef = (%v, size %d), want size 99", err, fh.Size)
	}
	ft.SetRefresh(func(_ context.Context, _ *FileHandle) error {
		return fmt.Errorf("%w: ref died", ErrFileRefExpired)
	})
	if err := ft.RefreshFileRef(ctx, &fh); !errors.Is(err, ErrFileRefExpired) {
		t.Errorf("RefreshFileRef err = %v, want ErrFileRefExpired", err)
	}
}

func TestFilterCombinators(t *testing.T) {
	t.Parallel()
	media := func(kind string) IncomingMsg {
		return IncomingMsg{Media: &MediaInfo{Kind: kind}}
	}
	tests := []struct {
		name string
		f    MessageFilter
		m    IncomingMsg
		want bool
	}{
		{"command match", Command("start"), IncomingMsg{IsCommand: true, CommandName: "start"}, true},
		{"command case-insensitive", Command("start"), IncomingMsg{IsCommand: true, CommandName: "START"}, true},
		{"command leading slash tolerated", Command("/start"), IncomingMsg{IsCommand: true, CommandName: "start"}, true},
		{"command wrong name", Command("start"), IncomingMsg{IsCommand: true, CommandName: "stop"}, false},
		{"command requires IsCommand", Command("start"), IncomingMsg{CommandName: "start"}, false},
		{"photo", Photo(), media("photo"), true},
		{"photo vs document", Photo(), media("document"), false},
		{"photo nil media", Photo(), IncomingMsg{}, false},
		{"document", Document(), media("document"), true},
		{"video", Video(), media("video"), true},
		{"audio", Audio(), media("audio"), true},
		{"voice", Voice(), media("voice"), true},
		{"animation", Animation(), media("animation"), true},
		{"sticker", Sticker(), media("sticker"), true},
		{"media only", MediaOnly(), media("document"), true},
		{"media only none", MediaOnly(), IncomingMsg{}, false},
		{"regex match", Regex(regexp.MustCompile(`^https?://`)), IncomingMsg{Text: "https://x/y"}, true},
		{"regex no match", Regex(regexp.MustCompile(`^https?://`)), IncomingMsg{Text: "ftp://x"}, false},
		{"and both true", Command("start").And(MediaOnly()), IncomingMsg{IsCommand: true, CommandName: "start", Media: &MediaInfo{Kind: "photo"}}, true},
		{"and one false", Command("start").And(MediaOnly()), IncomingMsg{IsCommand: true, CommandName: "start"}, false},
		{"or first", Photo().Or(Document()), media("photo"), true},
		{"or second", Photo().Or(Document()), media("document"), true},
		{"or neither", Photo().Or(Document()), media("audio"), false},
		{"not true", Command("start").Not(), IncomingMsg{IsCommand: true, CommandName: "stop"}, true},
		{"not false", Command("start").Not(), IncomingMsg{IsCommand: true, CommandName: "start"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.f(tt.m); got != tt.want {
				t.Errorf("filter = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeriveFileKey(t *testing.T) {
	t.Parallel()
	doc := DeriveFileKey(MediaInfo{Location: NewTestDocumentLocation(111, 222)})
	if doc != "doc:111:222" {
		t.Errorf("document key = %q, want doc:111:222", doc)
	}
	docAgain := DeriveFileKey(MediaInfo{Location: NewTestDocumentLocation(111, 222)})
	if docAgain != doc {
		t.Errorf("document key not deterministic: %q vs %q", docAgain, doc)
	}
	if other := DeriveFileKey(MediaInfo{Location: NewTestDocumentLocation(999, 222)}); other == doc {
		t.Error("distinct document ids produced the same key")
	}
	if other := DeriveFileKey(MediaInfo{Location: NewTestDocumentLocation(111, 223)}); other == doc {
		t.Error("distinct access hashes produced the same key")
	}

	photo := DeriveFileKey(MediaInfo{Location: NewTestPhotoLocation(333, 444)})
	if photo != "photo:333:444" {
		t.Errorf("photo key = %q, want photo:333:444", photo)
	}
	if photoAgain := DeriveFileKey(MediaInfo{Location: NewTestPhotoLocation(333, 444)}); photoAgain != photo {
		t.Errorf("photo key not deterministic: %q vs %q", photoAgain, photo)
	}
	if same := DeriveFileKey(MediaInfo{Location: NewTestPhotoLocation(111, 222)}); same == doc {
		t.Error("photo and document with equal ids must not collide")
	}
	if DeriveFileKey(MediaInfo{Location: nil}) != "" {
		t.Error("nil location must yield an empty key")
	}
	if DeriveFileKey(MediaInfo{Location: "not-a-location"}) != "" {
		t.Error("unknown location type must yield an empty key")
	}
}

func TestMarkupBuilder(t *testing.T) {
	t.Parallel()
	m := NewKeyboard().Callback("a", "b").Next().URL("c", "https://x").Build()
	if len(m.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(m.Rows))
	}
	if want := (MarkupButton{Text: "a", CallbackData: "b"}); m.Rows[0][0] != want {
		t.Errorf("Rows[0][0] = %#v, want %#v", m.Rows[0][0], want)
	}
	if want := (MarkupButton{Text: "c", URL: "https://x"}); m.Rows[1][0] != want {
		t.Errorf("Rows[1][0] = %#v, want %#v", m.Rows[1][0], want)
	}

	if m2 := NewKeyboard().Callback("x", "y").Build(); len(m2.Rows) != 1 || m2.Rows[0][0].Text != "x" {
		t.Errorf("Build must close the open row: got %#v", m2.Rows)
	}
	if m3 := NewKeyboard().Next().Callback("a", "b").Build(); len(m3.Rows) != 1 {
		t.Errorf("Next() on an empty row must be a no-op: got %d rows", len(m3.Rows))
	}

	m4 := NewKeyboard().AddRow(InlineCallback("p", "q"), InlineURL("r", "https://s")).Build()
	if len(m4.Rows) != 1 || len(m4.Rows[0]) != 2 {
		t.Fatalf("AddRow = %#v, want one row of two buttons", m4.Rows)
	}
	if want := (MarkupButton{Text: "p", CallbackData: "q"}); m4.Rows[0][0] != want {
		t.Errorf("AddRow callback button = %#v, want %#v", m4.Rows[0][0], want)
	}
	if want := (MarkupButton{Text: "r", URL: "https://s"}); m4.Rows[0][1] != want {
		t.Errorf("AddRow url button = %#v, want %#v", m4.Rows[0][1], want)
	}

	long := strings.Repeat("d", 70)
	m5 := NewKeyboard().Callback("t", long).Build()
	if got := m5.Rows[0][0].CallbackData; len(got) != 64 || got != long[:64] {
		t.Errorf("callback data not truncated to 64 bytes: len %d", len(got))
	}

	if want := (MarkupButton{Text: "t", URL: "u"}); InlineURL("t", "u") != want {
		t.Errorf("InlineURL = %#v, want %#v", InlineURL("t", "u"), want)
	}
	if want := (MarkupButton{Text: "t", CallbackData: "d"}); InlineCallback("t", "d") != want {
		t.Errorf("InlineCallback = %#v, want %#v", InlineCallback("t", "d"), want)
	}
}
