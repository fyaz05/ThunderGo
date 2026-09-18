// Package bot implements command dispatch, pre-flight checks, and handler
// routing for the Telegram bot.
//
// All Telegram I/O goes through the tgutil.BotBackend seam (no transport
// library is imported here). Pre-flight chain: banned → private-mode →
// token-activation → force-sub → rate-limit. Owner bypasses every check;
// authorized users bypass private-mode, token-activation, and rate-limit.
// /start always passes activation and force-sub.
package bot

import (
	"context"
	crypto_rand "crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/fyaz05/ThunderGo/internal/config"
	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/ratelimit"
	"github.com/fyaz05/ThunderGo/internal/shortener"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

type Bot struct {
	Cfg           *config.Config
	Backend       tgutil.BotBackend
	Pool          *pool.Pool
	Store         *store.Store
	Ingester      *ingest.Ingester
	Shortener     *shortener.Shortener
	Limiter       *ratelimit.Limiter
	globalLimiter *ratelimit.GlobalLimiter
	Log           *slog.Logger

	mu       sync.RWMutex
	commands map[string]*Command

	broadcastCancelMu sync.Mutex
	broadcastCancels  map[int]context.CancelFunc

	botUserID   int64
	botUsername string

	forceSubLink  string
	forceSubTitle string

	startTime time.Time

	sem chan struct{}

	// handlerWg tracks in-flight dispatchAsync goroutines so Stop() can drain.
	handlerWg sync.WaitGroup

	// stopCh is closed by Stop() to drop new messages.
	stopCh   chan struct{}
	stopOnce sync.Once

	baseCtx       context.Context
	baseCtxCancel context.CancelFunc

	// db is the test seam for the store: nil in production (b.st() then
	// returns b.Store). Tests install a MongoDB-free fake here.
	db botStore
}

// botStore narrows *store.Store to the methods the bot layer uses. Production
// always passes the real *store.Store; the interface exists only so tests can
// exercise the gate chain without MongoDB.
type botStore interface {
	IsUserBanned(ctx context.Context, userID int64) (bool, error)
	IsChannelBanned(ctx context.Context, channelID int64) (bool, error)
	IsAuthorized(ctx context.Context, userID int64) (bool, error)
	IsUserActivated(ctx context.Context, userID int64) (bool, error)
	UpsertUser(ctx context.Context, u store.User) (bool, error)
	SaveActivationToken(ctx context.Context, token string, ttl time.Duration) error
	ConsumeActivationToken(ctx context.Context, token string) error
	ActivateUser(ctx context.Context, userID int64, ttl time.Duration) error
	HasUser(ctx context.Context, userID int64) (bool, error)
	StreamUsers(ctx context.Context, fn func(u store.User) error) error
	DeleteUser(ctx context.Context, userID int64) error
	CountUsers(ctx context.Context) (int64, error)
	BanUser(ctx context.Context, b store.BannedUser) error
	BanChannel(ctx context.Context, b store.BannedChannel) error
	UnbanUser(ctx context.Context, userID int64) error
	UnbanChannel(ctx context.Context, channelID int64) error
	Authorize(ctx context.Context, a store.AuthorizedUser) error
	Deauthorize(ctx context.Context, userID int64) error
	ListAuthorized(ctx context.Context) ([]store.AuthorizedUser, error)
	InvalidateActivatedUser(ctx context.Context, userID int64) error
	SaveRestartMarker(ctx context.Context, m store.RestartMarker) error
	PopRestartMarker(ctx context.Context) (*store.RestartMarker, error)
}

// st returns the store facade: the test seam when installed, else the real
// store. Compile-time checks keep the two aligned.
func (b *Bot) st() botStore {
	if b.db != nil {
		return b.db
	}
	return b.Store
}

// Compile-time gate: the real store satisfies the narrowed interface.
var _ botStore = (*store.Store)(nil)

// OwnerOnly commands are silently ignored when invoked by a non-owner.
type Command struct {
	Name        string
	Description string
	OwnerOnly   bool
	Handler     func(c *Context) error
}

// Sent is a backend-agnostic handle to a message the bot just sent. It
// carries just enough identity to edit the message later (status messages).
type Sent struct {
	ChatID int64
	ID     int
}

type Context struct {
	Bot          *Bot
	Cmd          *Command
	Msg          tgutil.IncomingMsg
	Ctx          context.Context // per-dispatch context for backend calls
	Args         string
	IsOwner      bool
	IsAuthorized bool
}

// Reply sends an HTML message quoting the user's message. Callers must escape
// user-controlled text. Returns a Sent handle usable for later edits.
func (c *Context) Reply(text string) (Sent, error) {
	id, err := c.Bot.Backend.SendHTMLMsg(c.Ctx, c.Msg.ChatID, text, nil, c.Msg.MsgID)
	if err != nil {
		return Sent{}, err
	}
	return Sent{ChatID: c.Msg.ChatID, ID: id}, nil
}

// ReplyFormatted is Reply with HTML parse mode (kept for parity with the
// original handler vocabulary).
func (c *Context) ReplyFormatted(text string) (Sent, error) {
	return c.Reply(text)
}

// ReplyMarkup is Reply with an inline keyboard attached.
func (c *Context) ReplyMarkup(text string, markup any) (Sent, error) {
	id, err := c.Bot.Backend.SendHTMLMsg(c.Ctx, c.Msg.ChatID, text, markup, c.Msg.MsgID)
	if err != nil {
		return Sent{}, err
	}
	return Sent{ChatID: c.Msg.ChatID, ID: id}, nil
}

// Respond is like Reply but doesn't quote the original message.
func (c *Context) Respond(text string) (Sent, error) {
	id, err := c.Bot.Backend.SendHTMLMsg(c.Ctx, c.Msg.ChatID, text, nil, 0)
	if err != nil {
		return Sent{}, err
	}
	return Sent{ChatID: c.Msg.ChatID, ID: id}, nil
}

// RespondMarkup is Respond with an inline keyboard attached.
func (c *Context) RespondMarkup(text string, markup any) (Sent, error) {
	id, err := c.Bot.Backend.SendHTMLMsg(c.Ctx, c.Msg.ChatID, text, markup, 0)
	if err != nil {
		return Sent{}, err
	}
	return Sent{ChatID: c.Msg.ChatID, ID: id}, nil
}

// The caller must invoke Start to connect the backend.
func New(cfg *config.Config, backend tgutil.BotBackend, p *pool.Pool, s *store.Store, in *ingest.Ingester, sh *shortener.Shortener, lim *ratelimit.Limiter, log *slog.Logger) *Bot {
	b := &Bot{
		Cfg:       cfg,
		Backend:   backend,
		Pool:      p,
		Store:     s,
		Ingester:  in,
		Shortener: sh,
		Limiter:   lim,
		// nil when TG_GLOBAL_RPS=0 (disabled); preflight nil-checks before use.
		globalLimiter:    ratelimit.NewGlobal(cfg.GlobalRPS, 0), // burst=0 → defaults to 2x RPS inside
		Log:              log,
		commands:         make(map[string]*Command),
		broadcastCancels: make(map[int]context.CancelFunc),
		startTime:        time.Now(),
		sem:              make(chan struct{}, 128),
		stopCh:           make(chan struct{}),
	}
	b.baseCtx, b.baseCtxCancel = context.WithCancel(context.Background())
	return b
}

func (b *Bot) Register(cmd *Command) {
	if err := validateCommandName(cmd.Name); err != nil {
		b.Log.Error("refusing to register command with invalid name", "name", cmd.Name, "error", err)
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commands[cmd.Name] = cmd
}

// GetMe failure is fatal: admin detection and DM features depend on it.
func (b *Bot) Start(ctx context.Context) error {
	if b.Backend == nil {
		return errors.New("no bot backend available")
	}
	me, err := b.Backend.Me(ctx)
	if err != nil || me.ID == 0 {
		return fmt.Errorf("GetMe failed (admin detection and DM features require it): %w", err)
	}
	b.botUserID = me.ID
	b.botUsername = me.Username

	// Cache force-sub channel info (best-effort — non-fatal on failure).
	if b.Cfg.ForceSubChannelID != 0 {
		if ch, chErr := b.Backend.ResolveChat(ctx, fmt.Sprintf("%d", b.Cfg.ForceSubChannelID)); chErr == nil {
			b.forceSubTitle = ch.Title
			if ch.Username != "" {
				b.forceSubLink = "https://t.me/" + ch.Username
			}
		}
		if b.forceSubLink == "" {
			b.forceSubLink = fmt.Sprintf("https://t.me/c/%d", config.ChannelIDToRaw(b.Cfg.ForceSubChannelID))
		}
		if b.forceSubTitle == "" {
			b.forceSubTitle = "our channel"
		}
	}

	b.registerBuiltinCommands()
	// ONE catch-all message hook: the dispatcher owns command parsing, the
	// unknown-command silence, OwnerOnly denial, preflight and routing —
	// exactly like the previous transport-level message handler.
	b.Backend.OnAnyMessage(b.dispatch)
	b.Backend.OnCallback("broadcast_cancel", b.handleBroadcastCancel)
	// Inline-button navigation callbacks: help/about re-send their messages;
	// close deletes the host message. restart_broadcast is intentionally not
	// wired (a button press can't supply a replied-to message).
	b.Backend.OnCallback("help", b.handleHelpCallback)
	b.Backend.OnCallback("about", b.handleAboutCallback)
	b.Backend.OnCallback("close", b.handleCloseCallback)
	// Catch-all must be registered LAST: the backend matches callbacks by
	// longest prefix, so the specific routes above win over the empty
	// prefix below.
	b.Backend.OnCallback("", b.handleUnsupportedCallback)

	if err := b.registerCommandList(ctx); err != nil {
		b.Log.Warn("failed to register bot commands with Telegram", "error", err)
	}

	b.Log.Info("bot started",
		"username", b.botUsername,
		"bot_user_id", b.botUserID,
		"owner_id", b.Cfg.OwnerUserID,
		"private_mode", b.Cfg.PrivateMode,
		"force_sub_channel", b.Cfg.ForceSubChannelID,
		"rate_limit", b.Cfg.RateLimit,
		"client_count", b.Pool.Len(),
	)
	return nil
}

func (b *Bot) registerBroadcastCancel(msgID int, cancel context.CancelFunc) {
	b.broadcastCancelMu.Lock()
	defer b.broadcastCancelMu.Unlock()
	b.broadcastCancels[msgID] = cancel
}

func (b *Bot) unregisterBroadcastCancel(msgID int) {
	b.broadcastCancelMu.Lock()
	defer b.broadcastCancelMu.Unlock()
	delete(b.broadcastCancels, msgID)
}

// Owner-only.
func (b *Bot) handleBroadcastCancel(ctx context.Context, cq tgutil.CallbackQuery) error {
	if !b.Cfg.IsOwner(cq.SenderID) {
		_ = b.Backend.AnswerCallback(ctx, cq, msgCallbackBroadcastCancelDenied, true, "")
		return nil
	}
	msgID := cq.MessageID
	b.broadcastCancelMu.Lock()
	cancel, ok := b.broadcastCancels[msgID]
	b.broadcastCancelMu.Unlock()
	if !ok {
		_ = b.Backend.AnswerCallback(ctx, cq, msgCallbackBroadcastNotFound, true, "")
		return nil
	}
	cancel()
	_ = b.Backend.AnswerCallback(ctx, cq, msgCallbackBroadcastCancelled, false, "")
	return nil
}

// Stop signals the bot to stop accepting new messages and waits for in-flight
// handlers to drain. Idempotent.
func (b *Bot) Stop() {
	b.baseCtxCancel()
	b.stopOnce.Do(func() { close(b.stopCh) })
	b.handlerWg.Wait()
}

func (b *Bot) registerCommandList(ctx context.Context) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	cmds := make([]tgutil.BotCommand, 0, len(b.commands))
	for _, c := range b.commands {
		if c.OwnerOnly {
			continue
		}
		desc := c.Description
		if utf8.RuneCountInString(desc) > 256 {
			desc = string([]rune(desc)[:256])
		}
		cmds = append(cmds, tgutil.BotCommand{Command: c.Name, Description: desc})
	}
	if len(cmds) == 0 {
		return nil
	}
	return b.Backend.SetCommands(ctx, cmds)
}

// After Stop(), new messages are silently dropped.
func (b *Bot) dispatch(ctx context.Context, m tgutil.IncomingMsg) error {
	// Add to WaitGroup BEFORE the stopCh check so Stop()'s Wait() can't return
	// before this goroutine is launched.
	b.handlerWg.Add(1)
	select {
	case <-b.stopCh:
		b.handlerWg.Done()
		return nil
	default:
	}
	// Acquire semaphore slot; non-blocking fallback so shutdown isn't stalled.
	select {
	case b.sem <- struct{}{}:
	case <-b.stopCh:
		b.handlerWg.Done()
		return nil
	}
	go func() {
		defer b.handlerWg.Done()
		defer func() { <-b.sem }()
		// Per-dispatch context: derived from baseCtx (cancelled on Stop)
		// and cancelled when the dispatch returns. The transport-supplied
		// ctx is deliberately ignored — it may be tied to update
		// processing that ends as soon as this hook returns, while the
		// async handler (e.g. a broadcast) runs far beyond that.
		dispatchCtx, cancel := context.WithCancel(b.baseCtx)
		defer cancel()
		b.dispatchAsync(dispatchCtx, m)
	}()
	return nil
}

func (b *Bot) dispatchAsync(ctx context.Context, m tgutil.IncomingMsg) {
	defer func() {
		if r := recover(); r != nil {
			b.Log.Error("panic in bot handler", "recover", r, "stack", string(debugStack()))
		}
	}()

	// IsChannel covers broadcast channels; IsGroup covers supergroups.
	if m.IsChannel || m.IsGroup {
		// IncomingMsg.ChatID is already the marked (-100-prefixed) chat ID.
		channelID := m.ChatID
		if channelID != 0 {
			banCtx, banCancel := context.WithTimeout(b.baseCtx, 10*time.Second)
			banned, banErr := b.st().IsChannelBanned(banCtx, channelID)
			banCancel()
			if banErr != nil {
				b.Log.Warn("chat banned check DB error; denying message", "chat_id", channelID, "error", banErr)
				return
			}
			if banned {
				b.Log.Info("rejecting message from banned chat", "chat_id", channelID)
				_ = b.Backend.LeaveChat(b.baseCtx, channelID)
				return
			}
		}
		for _, bannedID := range b.Cfg.BannedChannelIDs {
			if bannedID == channelID {
				b.Log.Info("rejecting message from config-banned channel", "channel_id", channelID)
				_ = b.Backend.LeaveChat(b.baseCtx, channelID)
				return
			}
		}
	}

	senderID := m.SenderID
	isOwner := b.Cfg.IsOwner(senderID)
	authCtx, authCancel := context.WithTimeout(b.baseCtx, 10*time.Second)
	isAuthorized, authErr := b.st().IsAuthorized(authCtx, senderID)
	authCancel()
	if authErr != nil {
		b.Log.Warn("authorized check DB error; treating as unauthorized", "user_id", senderID, "error", authErr)
	}

	if m.IsCommand {
		// The transport strips the "/cmd@bot" suffix into CommandName;
		// re-apply the same normalization defensively.
		cmdName := strings.TrimPrefix(m.CommandName, "/")
		if i := strings.IndexByte(cmdName, '@'); i > 0 {
			cmdName = cmdName[:i]
		}
		b.mu.RLock()
		cmd, ok := b.commands[cmdName]
		b.mu.RUnlock()
		if !ok {
			return
		}

		if cmd.OwnerOnly && !isOwner {
			return
		}

		if !isOwner {
			c := &Context{
				Bot:          b,
				Cmd:          cmd,
				Msg:          m,
				Ctx:          ctx,
				Args:         strings.TrimSpace(m.Args),
				IsOwner:      isOwner,
				IsAuthorized: isAuthorized,
			}

			if stop, err := b.preflight(c); stop {
				if err != nil {
					b.Log.Debug("preflight rejected command", "cmd", cmdName, "reason", err.Error(), "user_id", senderID)
				}
				return
			}
		}

		c := &Context{
			Bot:          b,
			Cmd:          cmd,
			Msg:          m,
			Ctx:          ctx,
			Args:         strings.TrimSpace(m.Args),
			IsOwner:      isOwner,
			IsAuthorized: isAuthorized,
		}
		if err := cmd.Handler(c); err != nil {
			b.Log.Error("command handler error", "cmd", cmdName, "error", err)
			_, _ = c.Reply("⚠️ An error occurred. Please try again.")
		}
		return
	}

	// Non-command media in private chat: treat as file-ingest request.
	if m.IsPrivate && m.Media != nil {
		if !isOwner {
			cmdCtx := &Context{Bot: b, Msg: m, Ctx: ctx, IsOwner: isOwner, IsAuthorized: isAuthorized}
			if stop, err := b.preflight(cmdCtx); stop {
				if err != nil {
					b.Log.Debug("preflight rejected private media", "reason", err.Error(), "user_id", senderID)
				}
				return
			}
		}
		b.handlePrivateMedia(ctx, m, isOwner, isAuthorized)
		return
	}

	// m.IsChannel is true only for broadcast channels (not supergroups);
	// channel posts that were forwarded to the bot are handled the same way.
	if m.IsChannel && b.Cfg.ChannelAutoProcess && m.Media != nil {
		b.handleChannelAutoProcess(ctx, m)
		return
	}
}

// preflight runs the pre-flight chain. Returns (true, err) to reject.
// Chain: banned → private-mode → activation → force-sub → rate-limit.
// /start always passes activation and force-sub.
func (b *Bot) preflight(c *Context) (bool, error) {
	ctx, cancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer cancel()
	senderID := c.Msg.SenderID

	// Banned check (no bypass). Fail closed on DB error.
	banned, banErr := b.st().IsUserBanned(ctx, senderID)
	if banErr != nil {
		b.Log.Warn("banned check DB error; denying access", "user_id", senderID, "error", banErr)
		_, _ = c.Respond(msgTempDBError)
		return true, fmt.Errorf("banned check DB error")
	}
	if banned {
		_, _ = c.Respond(msgBannedNotice)
		return true, fmt.Errorf("banned user")
	}

	// Private-mode check (owner + authorized bypass).
	if b.Cfg.PrivateMode && !c.IsAuthorized {
		_, _ = c.Respond(msgPrivateMode)
		return true, fmt.Errorf("private mode")
	}

	isStart := c.Cmd != nil && c.Cmd.Name == "start"

	// Token activation check (owner + authorized bypass). Fail closed on DB error.
	if b.Cfg.TokenEnabled && !c.IsAuthorized && !isStart {
		activated, actErr := b.st().IsUserActivated(ctx, senderID)
		if actErr != nil {
			b.Log.Warn("activation check DB error; denying access", "user_id", senderID, "error", actErr)
			_, _ = c.Respond(msgTempDBError)
			return true, fmt.Errorf("activation check DB error")
		}
		if !activated {
			b.promptActivation(c)
			return true, fmt.Errorf("not activated")
		}
	}

	// Force-subscription check. Only member/admin/creator pass; left/kicked/
	// restricted are rejected (restricted may lack read perms).
	if !isStart && b.Cfg.ForceSubChannelID != 0 {
		status, err := b.Backend.GetChatMemberStatus(ctx, b.Cfg.ForceSubChannelID, senderID)
		if err != nil {
			b.Log.Debug("force-sub check error", "error", err)
		}
		// USER_NOT_PARTICIPANT (and other lookup failures) arrive as
		// err != nil or a non-member status — both reject with the
		// join prompt, exactly like the previous transport's behavior.
		if err != nil || (status != "member" && status != "administrator" && status != "creator") {
			joinURL := b.forceSubLink
			var markup any
			if joinURL != "" {
				markup = tgutil.NewKeyboard().AddRow(tgutil.InlineURL(msgForceSubButton, joinURL)).Build()
			}
			_ = b.Backend.SendHTML(ctx, c.Msg.ChatID, fmt.Sprintf(msgForceSub, b.forceSubTitle), markup, 0)
			return true, fmt.Errorf("force-sub")
		}
	}

	// Rate-limit check (owner + authorized bypass). Applies to file requests:
	// /link and private media ingestion (Cmd == nil).
	isFileRequest := (c.Cmd != nil && c.Cmd.Name == "link") || c.Cmd == nil
	if b.Limiter != nil && !c.IsAuthorized && isFileRequest {
		allowed, retryAfter := b.Limiter.AllowN(senderID, 1)
		if !allowed {
			secs := int(retryAfter.Seconds())
			if secs < 1 {
				secs = 1
			}
			_, _ = c.Respond(fmt.Sprintf(msgRateLimited, secs, b.Cfg.RateLimit))
			return true, fmt.Errorf("rate limited")
		}
	}

	// Global rate-limit circuit-breaker (owner + authorized bypass). Caps total
	// RPS across all users to avoid Telegram FLOOD_WAITs.
	if b.globalLimiter != nil && !c.IsAuthorized {
		if !b.globalLimiter.Allow() {
			delay := b.globalLimiter.RetryAfter()
			secs := int(delay.Seconds()) + 1
			_, _ = c.Respond(fmt.Sprintf(msgGlobalRateLimited, secs))
			return true, fmt.Errorf("global rate limited")
		}
	}

	return false, nil
}

// The token is a one-time 128-bit Crockford-base32 value with a 10-minute TTL.
func (b *Bot) promptActivation(c *Context) {
	token, err := generateActivationToken()
	if err != nil {
		b.Log.Error("generating activation token", "error", err)
		_, _ = c.Respond(msgErrInternal)
		return
	}

	actCtx, actCancel := context.WithTimeout(b.baseCtx, 10*time.Second)
	defer actCancel()
	if err := b.st().SaveActivationToken(actCtx, token, 10*time.Minute); err != nil {
		b.Log.Warn("saving activation token", "error", err)
		_, _ = c.Respond(msgErrInternal)
		return
	}

	activateURL := fmt.Sprintf("%s/activate/%s", b.Cfg.BaseURL, token)

	// Best-effort shortening; keep the long URL if the shortener returns nothing useful.
	if b.Shortener != nil {
		shortenCtx, shortenCancel := context.WithTimeout(b.baseCtx, 10*time.Second)
		shortened := b.Shortener.Shorten(shortenCtx, activateURL)
		shortenCancel()
		if shortened != "" && shortened != activateURL {
			activateURL = shortened
		}
	}

	markup := tgutil.NewKeyboard().AddRow(tgutil.InlineURL(msgActivationButton, activateURL)).Build()
	_, _ = c.RespondMarkup(msgActivationRequired, markup)
}

func generateActivationToken() (string, error) {
	buf := make([]byte, 16) // 128 bits
	if _, err := crypto_rand.Read(buf); err != nil {
		return "", fmt.Errorf("crypto/rand.Read failed: %w", err)
	}
	return tgutil.EncodeBase32(buf), nil
}

// broadcastUnreachable reports whether err is a permanent per-user delivery
// failure: the target blocked the bot, was deactivated, cannot be addressed
// (PEER_ID_INVALID) or the chat forbids writes. Such users are pruned after a
// broadcast instead of retried.
//
// PEER_ID_INVALID is mapped to ErrStaleMedia by the transport's error mapping
// (it is a permanent-class RPC) with the symbolic type preserved in the error
// text; the other three surface as *tgutil.RPCError.
func broadcastUnreachable(err error) bool {
	if err == nil {
		return false
	}
	var rpc *tgutil.RPCError
	if errors.As(err, &rpc) {
		switch rpc.Type {
		case "USER_IS_BLOCKED", "PEER_ID_INVALID", "USER_DEACTIVATED", "CHAT_WRITE_FORBIDDEN":
			return true
		}
		return false
	}
	return errors.Is(err, tgutil.ErrStaleMedia) && strings.Contains(err.Error(), "PEER_ID_INVALID")
}
