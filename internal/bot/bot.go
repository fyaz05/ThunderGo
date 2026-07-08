// Package bot implements command dispatch, pre-flight checks, and handler
// routing for the Telegram bot.
//
// Pre-flight chain: banned → private-mode → token-activation → force-sub →
// rate-limit. Owner bypasses every check; authorized users bypass private-mode,
// token-activation, and rate-limit. /start always passes activation and force-sub.
package bot

import (
	"context"
	crypto_rand "crypto/rand"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/amarnathcjd/gogram/telegram"

	"github.com/fyaz05/ThunderGo/internal/config"
	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/ratelimit"
	"github.com/fyaz05/ThunderGo/internal/shortener"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// Bot is the running bot instance.
type Bot struct {
	Cfg           *config.Config
	Pool          *pool.Pool
	Store         *store.Store
	Ingester      *ingest.Ingester
	Shortener     *shortener.Shortener
	Limiter       *ratelimit.Limiter
	globalLimiter *ratelimit.GlobalLimiter
	Log           *slog.Logger

	primary *telegram.Client

	mu       sync.RWMutex
	commands map[string]*Command

	broadcastCancelMu sync.Mutex
	broadcastCancels  map[int32]context.CancelFunc

	// Cached at startup.
	botUserID   int64
	botUsername string

	// Cached at startup.
	forceSubLink  string
	forceSubTitle string

	startTime time.Time

	// sem bounds concurrent handler goroutines.
	sem chan struct{}

	// handlerWg tracks in-flight dispatchAsync goroutines so Stop() can drain.
	handlerWg sync.WaitGroup

	// stopCh is closed by Stop() to drop new messages.
	stopCh   chan struct{}
	stopOnce sync.Once

	baseCtx       context.Context
	baseCtxCancel context.CancelFunc
}

// Command describes a single bot command. OwnerOnly commands are silently
// ignored when invoked by a non-owner.
type Command struct {
	Name        string
	Description string
	OwnerOnly   bool
	Handler     func(c *Context) error
}

// Context is passed to every command handler.
type Context struct {
	Bot          *Bot
	Cmd          *Command
	Msg          *telegram.NewMessage
	Args         string
	IsOwner      bool
	IsAuthorized bool
}

// Reply wraps m.Reply with HTML parse mode. Callers must escape user-controlled text.
func (c *Context) Reply(text string) (*telegram.NewMessage, error) {
	return c.Msg.Reply(text, &telegram.SendOptions{ParseMode: "HTML"})
}

// ReplyFormatted is an alias for Reply.
func (c *Context) ReplyFormatted(text string) (*telegram.NewMessage, error) {
	return c.Reply(text)
}

// Respond is like Reply but doesn't quote the original message.
func (c *Context) Respond(text string) (*telegram.NewMessage, error) {
	return c.Msg.Respond(text, &telegram.SendOptions{ParseMode: "HTML"})
}

// New constructs a Bot. The caller must invoke Start to connect.
func New(cfg *config.Config, p *pool.Pool, s *store.Store, in *ingest.Ingester, sh *shortener.Shortener, lim *ratelimit.Limiter, log *slog.Logger) *Bot {
	b := &Bot{
		Cfg:       cfg,
		Pool:      p,
		Store:     s,
		Ingester:  in,
		Shortener: sh,
		Limiter:   lim,
		// nil when TG_GLOBAL_RPS=0 (disabled); preflight nil-checks before use.
		globalLimiter:    ratelimit.NewGlobal(cfg.GlobalRPS, 0), // burst=0 → defaults to 2x RPS inside
		Log:              log,
		commands:         make(map[string]*Command),
		broadcastCancels: make(map[int32]context.CancelFunc),
		startTime:        time.Now(),
		sem:              make(chan struct{}, 128),
		stopCh:           make(chan struct{}),
	}
	b.baseCtx, b.baseCtxCancel = context.WithCancel(context.Background())
	return b
}

// Register adds a command to the dispatcher.
func (b *Bot) Register(cmd *Command) {
	if err := validateCommandName(cmd.Name); err != nil {
		b.Log.Error("refusing to register command with invalid name", "name", cmd.Name, "error", err)
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commands[cmd.Name] = cmd
}

// Start connects the primary client, registers handlers, and registers the
// bot command list with Telegram. GetMe failure is fatal: admin detection and
// DM features depend on it.
func (b *Bot) Start(ctx context.Context) error {
	primary := b.Pool.Primary()
	if primary == nil {
		return fmt.Errorf("no primary client available")
	}
	b.primary = primary.Client

	me, err := b.primary.GetMe()
	if err != nil || me == nil {
		return fmt.Errorf("GetMe failed (admin detection and DM features require it): %w", err)
	}
	b.botUserID = me.ID
	b.botUsername = me.Username

	// Cache force-sub channel info (best-effort — non-fatal on failure).
	if b.Cfg.ForceSubChannelID != 0 {
		if ch, err := b.primary.GetChannel(b.Cfg.ForceSubChannelID); err == nil && ch != nil {
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
	b.primary.On(telegram.OnMessage, b.dispatch)
	b.primary.OnCallback("broadcast_cancel", b.handleBroadcastCancel)
	// Inline-button navigation callbacks: help/about re-send their messages;
	// close deletes the host message. restart_broadcast is intentionally not
	// wired (a button press can't supply a replied-to message).
	b.primary.OnCallback("help", b.handleHelpCallback)
	b.primary.OnCallback("about", b.handleAboutCallback)
	b.primary.OnCallback("close", b.handleCloseCallback)

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

// registerBroadcastCancel associates a broadcast's cancel func with its status message ID.
func (b *Bot) registerBroadcastCancel(msgID int32, cancel context.CancelFunc) {
	b.broadcastCancelMu.Lock()
	defer b.broadcastCancelMu.Unlock()
	b.broadcastCancels[msgID] = cancel
}

// unregisterBroadcastCancel removes a broadcast's cancel func after it completes.
func (b *Bot) unregisterBroadcastCancel(msgID int32) {
	b.broadcastCancelMu.Lock()
	defer b.broadcastCancelMu.Unlock()
	delete(b.broadcastCancels, msgID)
}

// handleBroadcastCancel is the callback handler for the "broadcast_cancel"
// inline button. Owner-only.
func (b *Bot) handleBroadcastCancel(cq *telegram.CallbackQuery) error {
	if cq == nil {
		return nil
	}
	if !b.Cfg.IsOwner(cq.SenderID) {
		_, _ = cq.Answer("Only the owner can cancel a broadcast.", &telegram.CallbackOptions{Alert: true})
		return nil
	}
	msgID := cq.MessageID
	b.broadcastCancelMu.Lock()
	cancel, ok := b.broadcastCancels[msgID]
	b.broadcastCancelMu.Unlock()
	if !ok {
		_, _ = cq.Answer("No active broadcast found for this message.", &telegram.CallbackOptions{Alert: true})
		return nil
	}
	cancel()
	_, _ = cq.Answer("Broadcast cancelled.", nil)
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
	cmds := make([]*telegram.BotCommand, 0, len(b.commands))
	for _, c := range b.commands {
		if c.OwnerOnly {
			continue
		}
		desc := c.Description
		if utf8.RuneCountInString(desc) > 256 {
			desc = string([]rune(desc)[:256])
		}
		cmds = append(cmds, &telegram.BotCommand{Command: c.Name, Description: desc})
	}
	if len(cmds) == 0 {
		return nil
	}
	var scope telegram.BotCommandScope = &telegram.BotCommandScopeDefault{}
	_, err := b.primary.SetBotCommands(cmds, &scope, "en")
	return err
}

// dispatch is the single entry point for all incoming messages. After Stop(),
// new messages are silently dropped.
func (b *Bot) dispatch(m *telegram.NewMessage) error {
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
		b.dispatchAsync(m)
	}()
	return nil
}

func (b *Bot) dispatchAsync(m *telegram.NewMessage) {
	defer func() {
		if r := recover(); r != nil {
			b.Log.Error("panic in bot handler", "recover", r, "stack", string(debugStack()))
		}
	}()

	// Ignore messages from banned channels and supergroups. IsChannel() covers
	// broadcast channels; IsGroup() covers supergroups.
	if m.IsChannel() || m.IsGroup() {
		channelID := m.ChannelID()
		if channelID == 0 {
			channelID = m.ChatID()
		}
		if channelID != 0 {
			banCtx, banCancel := context.WithTimeout(b.baseCtx, 10*time.Second)
			banned, banErr := b.Store.IsChannelBanned(banCtx, channelID)
			banCancel()
			if banErr != nil {
				b.Log.Warn("chat banned check DB error; denying message", "chat_id", channelID, "error", banErr)
				return
			}
			if banned {
				b.Log.Info("rejecting message from banned chat", "chat_id", channelID)
				_ = m.Client.LeaveChannel(channelID)
				return
			}
		}
		for _, bannedID := range b.Cfg.BannedChannelIDs {
			if bannedID == channelID {
				b.Log.Info("rejecting message from config-banned channel", "channel_id", channelID)
				_ = m.Client.LeaveChannel(channelID)
				return
			}
		}
	}

	senderID := m.SenderID()
	isOwner := b.Cfg.IsOwner(senderID)
	authCtx, authCancel := context.WithTimeout(b.baseCtx, 10*time.Second)
	isAuthorized, authErr := b.Store.IsAuthorized(authCtx, senderID)
	authCancel()
	if authErr != nil {
		b.Log.Warn("authorized check DB error; treating as unauthorized", "user_id", senderID, "error", authErr)
	}

	if m.IsCommand() {
		cmdName := strings.TrimPrefix(m.GetCommand(), "/")
		if i := strings.IndexByte(cmdName, '@'); i > 0 {
			cmdName = cmdName[:i]
		}
		b.mu.RLock()
		cmd, ok := b.commands[cmdName]
		b.mu.RUnlock()
		if !ok {
			return // Unknown command — silently ignore.
		}

		// Owner-only gate: non-owners get no response.
		if cmd.OwnerOnly && !isOwner {
			return
		}

		ctx := &Context{
			Bot:          b,
			Cmd:          cmd,
			Msg:          m,
			Args:         strings.TrimSpace(m.Args()),
			IsOwner:      isOwner,
			IsAuthorized: isAuthorized,
		}

		// Owner bypasses preflight.
		if !isOwner {
			if stop, err := b.preflight(ctx); stop {
				if err != nil {
					b.Log.Debug("preflight rejected command", "cmd", cmdName, "reason", err.Error(), "user_id", senderID)
				}
				return
			}
		}

		if err := cmd.Handler(ctx); err != nil {
			b.Log.Error("command handler error", "cmd", cmdName, "error", err)
			_, _ = ctx.Reply("⚠️ An error occurred. Please try again.")
		}
		return
	}

	// Non-command media in private chat: treat as file-ingest request.
	if m.IsPrivate() && m.IsMedia() {
		if !isOwner {
			ctx := &Context{Bot: b, Msg: m, IsOwner: isOwner, IsAuthorized: isAuthorized}
			if stop, err := b.preflight(ctx); stop {
				if err != nil {
					b.Log.Debug("preflight rejected private media", "reason", err.Error(), "user_id", senderID)
				}
				return
			}
		}
		b.handlePrivateMedia(m, isOwner, isAuthorized)
		return
	}

	// Channel auto-processing. m.IsChannel() is true only for broadcast channels
	// (not supergroups); IsChannelPost() only matches forwarded posts.
	if m.IsChannel() && b.Cfg.ChannelAutoProcess && m.IsMedia() {
		b.handleChannelAutoProcess(m)
		return
	}
}

// preflight runs the pre-flight chain. Returns (true, err) to reject.
// Chain: banned → private-mode → activation → force-sub → rate-limit.
// /start always passes activation and force-sub.
func (b *Bot) preflight(c *Context) (bool, error) {
	ctx, cancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer cancel()
	senderID := c.Msg.SenderID()

	// Banned check (no bypass). Fail closed on DB error.
	banned, banErr := b.Store.IsUserBanned(ctx, senderID)
	if banErr != nil {
		b.Log.Warn("banned check DB error; denying access", "user_id", senderID, "error", banErr)
		_, _ = c.Msg.Respond(msgTempDBError)
		return true, fmt.Errorf("banned check DB error")
	}
	if banned {
		_, _ = c.Msg.Respond(msgBannedNotice)
		return true, fmt.Errorf("banned user")
	}

	// Private-mode check (owner + authorized bypass).
	if b.Cfg.PrivateMode && !c.IsAuthorized {
		_, _ = c.Msg.Respond(msgPrivateMode)
		return true, fmt.Errorf("private mode")
	}

	isStart := c.Cmd != nil && c.Cmd.Name == "start"

	// Token activation check (owner + authorized bypass). Fail closed on DB error.
	if b.Cfg.TokenEnabled && !c.IsAuthorized && !isStart {
		activated, actErr := b.Store.IsUserActivated(ctx, senderID)
		if actErr != nil {
			b.Log.Warn("activation check DB error; denying access", "user_id", senderID, "error", actErr)
			_, _ = c.Msg.Respond(msgTempDBError)
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
		primary := b.Pool.Primary()
		if primary == nil {
			b.Log.Warn("preflight: no primary client for force-sub check")
			_, _ = c.Msg.Respond(msgTempDBError)
			return true, fmt.Errorf("no primary client")
		}
		member, err := primary.GetChatMember(b.Cfg.ForceSubChannelID, senderID)
		if err != nil || member == nil ||
			(member.Status != "member" && member.Status != "admin" && member.Status != "creator") {
			if err != nil && !telegram.MatchError(err, "USER_NOT_PARTICIPANT") {
				b.Log.Debug("force-sub check error", "error", err)
			}
			joinURL := b.forceSubLink
			opts := &telegram.SendOptions{ParseMode: "HTML"}
			if joinURL != "" {
				opts.ReplyMarkup = telegram.InlineURL(msgForceSubButton, joinURL)
			}
			_, _ = c.Msg.Respond(msgForceSub, opts)
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
			_, _ = c.Msg.Respond(fmt.Sprintf(msgRateLimited, secs))
			return true, fmt.Errorf("rate limited")
		}
	}

	// Global rate-limit circuit-breaker (owner + authorized bypass). Caps total
	// RPS across all users to avoid Telegram FLOOD_WAITs.
	if b.globalLimiter != nil && !c.IsAuthorized {
		if !b.globalLimiter.Allow() {
			delay := b.globalLimiter.RetryAfter()
			secs := int(delay.Seconds()) + 1
			_, _ = c.Msg.Respond(fmt.Sprintf(msgRateLimited, secs))
			return true, fmt.Errorf("global rate limited")
		}
	}

	return false, nil
}

// promptActivation sends the activation prompt with an inline button linking
// to the /activate/{token} HTTP route. The token is a one-time 128-bit
// Crockford-base32 value with a 10-minute TTL.
func (b *Bot) promptActivation(c *Context) {
	token, err := generateActivationToken()
	if err != nil {
		b.Log.Error("generating activation token", "error", err)
		_, _ = c.Msg.Respond(msgErrInternal)
		return
	}

	actCtx, actCancel := context.WithTimeout(b.baseCtx, 10*time.Second)
	defer actCancel()
	if err := b.Store.SaveActivationToken(actCtx, token, 10*time.Minute); err != nil {
		b.Log.Warn("saving activation token", "error", err)
		_, _ = c.Msg.Respond(msgErrInternal)
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

	opts := &telegram.SendOptions{ParseMode: "HTML"}
	opts.ReplyMarkup = telegram.InlineURL(msgActivationButton, activateURL)
	_, _ = c.Msg.Respond(msgActivationRequired, opts)
}

// generateActivationToken returns a 128-bit random Crockford-base32 token (26 chars).
func generateActivationToken() (string, error) {
	buf := make([]byte, 16) // 128 bits
	if _, err := crypto_rand.Read(buf); err != nil {
		return "", fmt.Errorf("crypto/rand.Read failed: %w", err)
	}
	return tgutil.EncodeBase32(buf), nil
}
