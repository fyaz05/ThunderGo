package bot

import (
	"context"
	"fmt"
	"html"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/amarnathcjd/gogram/telegram"

	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// handleStart welcomes the user, registers them, and notifies the vault on first contact.
// With TG_TOKEN_ENABLED, /start may carry an activation token to consume.
func (b *Bot) handleStart(c *Context) error {
	// Activation-token payload. Preflight lets /start through for this.
	if b.Cfg.TokenEnabled && c.Args != "" {
		if err := b.consumeActivation(c); err != nil {
			return nil // consumeActivation already replied.
		}
		return nil // consumeActivation succeeded; welcome already sent
	}

	sender := c.Msg.Sender
	firstName := ""
	lastName := ""
	username := ""
	if sender != nil {
		firstName = sender.FirstName
		lastName = sender.LastName
		username = sender.Username
	}

	now := time.Now()
	upsertCtx, upsertCancel := context.WithTimeout(b.baseCtx, 10*time.Second)
	defer upsertCancel()
	inserted, err := b.Store.UpsertUser(upsertCtx, store.User{
		UserID:    c.Msg.SenderID(),
		FirstName: firstName,
		LastName:  lastName,
		Username:  username,
		JoinedAt:  now,
	})
	if err != nil {
		b.Log.Warn("upserting user", "error", err)
	}

	// Notify the vault on first contact.
	if inserted && b.Cfg.OwnerUserID != 0 {
		b.notifyNewUser(firstName, lastName, username, c.Msg.SenderID())
	}

	welcome := fmt.Sprintf(msgWelcome, html.EscapeString(firstName))
	_, _ = c.ReplyFormatted(welcome)
	return nil
}

// consumeActivation validates the /start activation payload, activates the
// user, and replies. Returns nil on success and an error on every failure path;
// the reply has already been sent.
func (b *Bot) consumeActivation(c *Context) error {
	payload := c.Args
	actCtx, actCancel := context.WithTimeout(b.baseCtx, 10*time.Second)
	defer actCancel()

	// ConsumeActivationToken is an atomic FindOneAndDelete, so concurrent
	// /start requests with the same token cannot both succeed.
	if err := b.Store.ConsumeActivationToken(actCtx, payload); err != nil {
		b.Log.Info("activation token rejected", "user_id", c.Msg.SenderID(), "error", err)
		_, _ = c.ReplyFormatted(msgActivationInvalid)
		return err
	}

	// Valid token — activate the user.
	ttl := time.Duration(b.Cfg.TokenTTLHours) * time.Hour
	if err := b.Store.ActivateUser(actCtx, c.Msg.SenderID(), ttl); err != nil {
		b.Log.Warn("activating user", "user_id", c.Msg.SenderID(), "error", err)
		_, _ = c.ReplyFormatted(msgErrInternal)
		return err
	}

	b.Log.Info("user activated", "user_id", c.Msg.SenderID(), "ttl_hours", b.Cfg.TokenTTLHours)
	_, _ = c.ReplyFormatted(fmt.Sprintf(msgActivated, formatDuration(ttl)))
	return nil
}

// formatDuration renders a whole-hour duration as natural English
// ("1 hour", "12 hours", "1 day", "3 days").
func formatDuration(d time.Duration) string {
	h := int(d.Hours())
	switch {
	case h >= 48:
		return fmt.Sprintf("%d days", h/24)
	case h >= 24:
		return "1 day"
	case h == 1:
		return "1 hour"
	default:
		return fmt.Sprintf("%d hours", h)
	}
}

// notifyNewUser posts a "new user" notification to the vault channel.
func (b *Bot) notifyNewUser(first, last, username string, userID int64) {
	primary := b.Pool.Primary()
	if primary == nil {
		return
	}
	name := html.EscapeString(strings.TrimSpace(first + " " + last))
	userLine := "<i>(none)</i>"
	if username != "" {
		userLine = "@" + html.EscapeString(username)
	}
	text := fmt.Sprintf(msgNewUser, name, userLine, userID)
	_, err := primary.SendMessage(b.Cfg.VaultChannelID, text, &telegram.SendOptions{ParseMode: "HTML"})
	if err != nil {
		b.Log.Warn("posting new-user notification to vault", "error", err)
	}
}

// handleHelp shows usage instructions, auto-generated from the registry.
func (b *Bot) handleHelp(c *Context) error {
	_, _ = c.ReplyFormatted(b.buildHelpText())
	return nil
}

// buildHelpText assembles the /help message body. Shared by the /help command
// and the "help" inline-button callback. OwnerOnly commands are hidden.
func (b *Bot) buildHelpText() string {
	var sb strings.Builder
	sb.WriteString("<b>📖 Usage</b>\n\n")
	sb.WriteString("<b>Private chat:</b>\n")
	sb.WriteString("Send any media file (document, video, audio, photo, voice, animation, video note, or sticker). The bot replies with a stream link and a download link.\n\n")
	sb.WriteString("<b>Groups:</b>\n")
	sb.WriteString("Reply to a media message with <code>/link</code> to generate a link. Use <code>/link N</code> to process N consecutive messages at once.\n\n")
	sb.WriteString("<b>Commands:</b>\n")

	// Build the command list, sorted by name. Skip OwnerOnly commands.
	b.mu.RLock()
	cmds := make([]*Command, 0, len(b.commands))
	for _, cmd := range b.commands {
		if cmd == nil || cmd.OwnerOnly {
			continue
		}
		cmds = append(cmds, cmd)
	}
	b.mu.RUnlock()
	sort.Slice(cmds, func(i, j int) bool { return cmds[i].Name < cmds[j].Name })

	for _, cmd := range cmds {
		sb.WriteString(fmt.Sprintf("<code>/%s</code> — %s\n",
			html.EscapeString(cmd.Name),
			html.EscapeString(cmd.Description)))
	}

	sb.WriteString("\n<b>Note:</b> The bot needs admin rights in groups to read messages and post replies.")
	return sb.String()
}

// handlePing replies "Pinging…" then edits it to show the round-trip time.
func (b *Bot) handlePing(c *Context) error {
	start := time.Now()
	sent, _ := c.Reply(msgPinging)
	if sent == nil {
		// c.Reply can return (nil, nil); fall back to Respond.
		_, _ = c.Respond(msgErrPostStatus)
		return nil
	}
	elapsed := time.Since(start)
	editText := fmt.Sprintf(msgPong, elapsed.Milliseconds())
	_, _ = c.Msg.Client.EditMessage(c.Msg.ChatID(), sent.ID, editText, &telegram.SendOptions{ParseMode: "HTML"})
	return nil
}

// handleDc reports the data center of a file or user: no arg (caller's DC),
// replied to a user (their DC), replied to a media file (the file's DC). The
// owner can pass a user ID or @username as an argument.
func (b *Bot) handleDc(c *Context) error {
	// Replied to a media file → file DC.
	if c.Msg.IsReply() {
		reply, err := c.Msg.GetReplyMessage()
		if err == nil && reply != nil && reply.IsMedia() {
			dc := 0
			if doc := reply.Document(); doc != nil {
				dc = int(doc.DcID)
			} else if p := reply.Photo(); p != nil {
				dc = int(p.DcID)
			}
			name, _ := tgutil.ExtractFileName(reply)
			// Drop the media type prefix when it's the generic "document" fallback;
			// the MIME type alone is more informative in that case.
			mediaType := tgutil.MediaType(reply)
			mimeType := tgutil.ExtractMIME(reply)
			typeLine := mimeType
			if mediaType != "document" && mediaType != "" {
				typeLine = fmt.Sprintf("%s (%s)", mediaType, mimeType)
			}
			text := fmt.Sprintf(msgFileDC,
				dc, html.EscapeString(name), tgutil.ExtractSize(reply), html.EscapeString(typeLine))
			_, _ = c.ReplyFormatted(text)
			return nil
		}
		// Replied to a user → user DC (from their profile photo).
		if err == nil && reply != nil && reply.Sender != nil {
			dc := userDCFromPhoto(reply.Sender)
			text := fmt.Sprintf(msgUserDC, dc, html.EscapeString(reply.Sender.FirstName+" "+reply.Sender.LastName))
			_, _ = c.ReplyFormatted(text)
			return nil
		}
	}

	// Owner passed a user ID or @username.
	if c.IsOwner && c.Args != "" {
		target, err := resolveUser(b.Pool.Primary().Client, c.Args)
		if err == nil && target != nil {
			dc := userDCFromPhoto(target)
			text := fmt.Sprintf(msgUserDC, dc, html.EscapeString(target.FirstName+" "+target.LastName))
			_, _ = c.ReplyFormatted(text)
			return nil
		}
	}

	// No argument → caller's own DC.
	sender := c.Msg.Sender
	if sender != nil {
		dc := userDCFromPhoto(sender)
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgYourDC, dc))
		return nil
	}
	_, _ = c.Reply("Could not determine DC.")
	return nil
}

func userDCFromPhoto(u *telegram.UserObj) int {
	if u == nil || u.Photo == nil {
		return 0
	}
	// The user's profile photo carries the DC where it lives.
	if p, ok := u.Photo.(*telegram.UserProfilePhotoObj); ok {
		return int(p.DcID)
	}
	return 0
}

func resolveUser(c *telegram.Client, ref string) (*telegram.UserObj, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("empty reference")
	}
	// Username resolution.
	if strings.HasPrefix(ref, "@") {
		ent, err := c.ResolveUsername(strings.TrimPrefix(ref, "@"))
		if err != nil {
			return nil, err
		}
		if u, ok := ent.(*telegram.UserObj); ok {
			return u, nil
		}
		return nil, fmt.Errorf("not a user")
	}
	// Numeric ID — must be the entire string.
	userID, err := strconv.ParseInt(ref, 10, 64)
	if err != nil {
		return nil, err
	}
	return c.GetUser(userID)
}

// handleAbout shows a brief bot description with a GitHub inline button.
// The text also carries an <a href> tag so the link is copy-pasteable on
// clients that hide inline-button URLs.
func (b *Bot) handleAbout(c *Context) error {
	opts := &telegram.SendOptions{ParseMode: "HTML"}
	opts.ReplyMarkup = buildAboutMarkup()
	_, _ = c.Msg.Respond(buildAboutText(), opts)
	return nil
}

// buildAboutText returns the /about message body. Shared by the /about
// command and the "about" inline-button callback.
func buildAboutText() string {
	return `🤖 <b>About ThunderGo</b>

Turn Telegram files into HTTP direct links. Send a file → get a streaming + download link. Anyone with the link can stream or download with seek support.

<b>Tech:</b> Go 1.26 · gogram · MongoDB · chi
<b>License:</b> MIT
<b>Source:</b> <a href="https://github.com/fyaz05/ThunderGo">github.com/fyaz05/ThunderGo</a>`
}

// buildAboutMarkup returns the inline-button markup for /about. Shared by the
// /about command and the "about" inline-button callback.
func buildAboutMarkup() *telegram.ReplyInlineMarkup {
	return telegram.InlineURL("📖 GitHub", "https://github.com/fyaz05/ThunderGo")
}

// handleStats reports runtime statistics. Owner-only — the metrics leak
// build details an attacker could use to target toolchain CVEs.
func (b *Bot) handleStats(c *Context) error {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	uptime := formatReadableDuration(time.Since(b.startTime))

	var sb strings.Builder
	sb.WriteString("📊 <b>Runtime Stats</b>\n\n")
	sb.WriteString(fmt.Sprintf("<b>Uptime:</b> %s\n", uptime))
	sb.WriteString(fmt.Sprintf("<b>Goroutines:</b> %d\n", runtime.NumGoroutine()))
	sb.WriteString(fmt.Sprintf("<b>Go version:</b> %s\n", runtime.Version()))
	sb.WriteString(fmt.Sprintf("<b>CPUs:</b> %d\n", runtime.NumCPU()))
	sb.WriteString(fmt.Sprintf("<b>CGO:</b> %s\n\n", cgoEnabled))

	sb.WriteString("<b>Memory:</b>\n")
	// #nosec G115 — runtime metrics safely fit in int64
	sb.WriteString(fmt.Sprintf("  Alloc: %s\n", tgutil.FormatBytes(int64(ms.Alloc))))
	// #nosec G115 — runtime metrics safely fit in int64
	sb.WriteString(fmt.Sprintf("  Total Alloc: %s\n", tgutil.FormatBytes(int64(ms.TotalAlloc))))
	// #nosec G115 — runtime metrics safely fit in int64
	sb.WriteString(fmt.Sprintf("  Sys: %s\n", tgutil.FormatBytes(int64(ms.Sys))))
	sb.WriteString(fmt.Sprintf("  Heap Objects: %d\n", ms.HeapObjects))
	sb.WriteString(fmt.Sprintf("  GC Cycles: %d\n\n", ms.NumGC))

	sb.WriteString("<b>Pool:</b>\n")
	sb.WriteString(fmt.Sprintf("  Clients: %d\n", b.Pool.Len()))
	sb.WriteString(fmt.Sprintf("  Total In-flight: %d\n", b.Pool.TotalInflight()))

	_, _ = c.ReplyFormatted(sb.String())
	return nil
}

// formatReadableDuration renders a duration as "1d 2h 3m 4s", omitting
// leading zero components. Negative durations clamp to 0.
func formatReadableDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	days := int(d.Hours()) / 24
	d %= 24 * time.Hour
	hours := int(d.Hours())
	d %= time.Hour
	mins := int(d.Minutes())
	d %= time.Minute
	secs := int(d.Seconds())

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm %ds", days, hours, mins, secs)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, mins, secs)
	}
	if mins > 0 {
		return fmt.Sprintf("%dm %ds", mins, secs)
	}
	return fmt.Sprintf("%ds", secs)
}

// --- Inline-button callback handlers ---

// handleHelpCallback re-sends the /help text to the chat that originated the
// "help" button press. A fresh message is sent (not an edit) because the
// button may live on an old message.
func (b *Bot) handleHelpCallback(cq *telegram.CallbackQuery) error {
	if cq == nil {
		return nil
	}
	// Best-effort: clears the spinner; proceed even on failure.
	_, _ = cq.Answer(msgCallbackHelpAnswered, nil)

	opts := &telegram.SendOptions{ParseMode: "HTML"}
	_, err := cq.Client.SendMessage(cq.GetChatID(), b.buildHelpText(), opts)
	if err != nil {
		b.Log.Debug("help callback: could not send help message", "chat_id", cq.GetChatID(), "error", err)
	}
	return nil
}

// handleAboutCallback re-sends the /about text (with the GitHub inline button)
// to the chat that originated the "about" button press.
func (b *Bot) handleAboutCallback(cq *telegram.CallbackQuery) error {
	if cq == nil {
		return nil
	}
	_, _ = cq.Answer(msgCallbackAboutAnswered, nil)

	opts := &telegram.SendOptions{ParseMode: "HTML"}
	opts.ReplyMarkup = buildAboutMarkup()
	_, err := cq.Client.SendMessage(cq.GetChatID(), buildAboutText(), opts)
	if err != nil {
		b.Log.Debug("about callback: could not send about message", "chat_id", cq.GetChatID(), "error", err)
	}
	return nil
}

// handleCloseCallback deletes the message the "close" button is attached to.
// Best-effort: may fail if the bot lacks admin rights or the message is >48h old.
// Only the original message recipient (in private chats) or the bot owner can close.
func (b *Bot) handleCloseCallback(cq *telegram.CallbackQuery) error {
	if cq == nil {
		return nil
	}
	if cq.SenderID != cq.GetChatID() && !b.Cfg.IsOwner(cq.SenderID) {
		return nil
	}
	_, _ = cq.Answer(msgCallbackCloseAnswered, nil)
	_, err := cq.Client.DeleteMessages(cq.GetChatID(), []int32{cq.MessageID})
	if err != nil {
		b.Log.Debug("close callback: could not delete message",
			"chat_id", cq.GetChatID(), "msg_id", cq.MessageID, "error", err)
	}
	return nil
}
