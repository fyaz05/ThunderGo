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

var knownCallbacks = map[string]bool{
	"help":             true,
	"about":            true,
	"close":            true,
	"broadcast_cancel": true,
}

// handleStart may consume an activation token if TG_TOKEN_ENABLED.
func (b *Bot) handleStart(c *Context) error {

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

	if inserted && b.Cfg.OwnerUserID != 0 {
		b.notifyNewUser(firstName, lastName, username, c.Msg.SenderID())
	}

	welcome := fmt.Sprintf(msgWelcome, html.EscapeString(firstName), b.Cfg.BatchCap)
	opts := &telegram.SendOptions{ParseMode: "HTML", ReplyMarkup: buildStartMarkup(b.Cfg.ForceSubChannelID != 0, b.forceSubLink, b.forceSubTitle)}
	_, _ = c.Msg.Respond(welcome, opts)
	return nil
}

// consumeActivation validates the /start payload, activates the user, and
// replies. On any error path the reply has already been sent.
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

// formatDuration renders a duration as natural English
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

func (b *Bot) notifyNewUser(first, last, username string, userID int64) {
	primary := b.Pool.Primary()
	if primary == nil {
		return
	}
	name := html.EscapeString(strings.TrimSpace(first + " " + last))
	// Use @username as display name if present, else the full name.
	displayName := name
	if username != "" {
		displayName = "@" + html.EscapeString(username)
	}
	text := fmt.Sprintf(msgNewUser, userID, displayName, userID)
	_, err := primary.SendMessage(b.Cfg.VaultChannelID, text, &telegram.SendOptions{ParseMode: "HTML"})
	if err != nil {
		b.Log.Warn("posting new-user notification to vault", "error", err)
	}
}

func (b *Bot) handleHelp(c *Context) error {
	opts := &telegram.SendOptions{ParseMode: "HTML", ReplyMarkup: buildHelpMarkup(b.Cfg.ForceSubChannelID != 0, b.forceSubLink, b.forceSubTitle)}
	_, _ = c.Msg.Respond(b.buildHelpText(), opts)
	return nil
}

// buildHelpText assembles the /help message body. Shared by the /help command
// and the "help" inline-button callback. OwnerOnly commands are hidden.
func (b *Bot) buildHelpText() string {
	var sb strings.Builder
	sb.WriteString("<b>📘 ThunderGo — Help Guide</b>\n\n")

	sb.WriteString("<b>🚀 Private Chat (with me)</b>\n")
	sb.WriteString("<blockquote>")
	sb.WriteString("📦 Send me <b>any file</b> — video, audio, document, photo, voice, animation, video note, or sticker.\n")
	sb.WriteString("⚡ I'll instantly reply with your <b>stream</b> + <b>download</b> links.")
	sb.WriteString("</blockquote>\n\n")

	sb.WriteString("<b>👥 In Groups</b>\n")
	sb.WriteString("<blockquote>")
	sb.WriteString("👆 Reply to a media message with <code>/link</code> to generate a link.\n")
	sb.WriteString(fmt.Sprintf("📚 <b>Batch mode:</b> <code>/link 5</code> processes the next 5 messages at once (up to <code>%d</code>).\n", b.Cfg.BatchCap))
	sb.WriteString("🔐 I need <b>admin rights</b> in the group to read messages and post replies.\n")
	sb.WriteString("📩 Links are posted in the group <b>and</b> sent to you privately.")
	sb.WriteString("</blockquote>\n\n")

	sb.WriteString("<b>📢 In Channels</b>\n")
	sb.WriteString("<blockquote>")
	sb.WriteString("🤖 Add me as an admin and I can auto-attach stream/download buttons to new media.")
	sb.WriteString("</blockquote>\n\n")

	sb.WriteString("<b>⚙️ Available Commands</b>\n")

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

	sb.WriteString("<blockquote>")
	for _, cmd := range cmds {
		sb.WriteString(fmt.Sprintf("⌨️ <code>/%s</code> — %s\n",
			html.EscapeString(cmd.Name),
			html.EscapeString(cmd.Description)))
	}
	sb.WriteString("</blockquote>\n\n")

	sb.WriteString("<b>💡 Pro Tips</b>\n")
	sb.WriteString("<blockquote>")
	sb.WriteString("📤 Forward files from other chats directly to me.\n")
	sb.WriteString("⏳ If you hit a rate limit, wait the specified time.\n")
	sb.WriteString("💬 Join our <a href=\"" + communityLink + "\">" + communityName + "</a> for support and updates.")
	sb.WriteString("</blockquote>")
	return sb.String()
}

func (b *Bot) handlePing(c *Context) error {
	start := time.Now()
	sent, _ := c.Reply(msgPinging)
	if sent == nil {
		// c.Reply can return (nil, nil); fall back to Respond.
		_, _ = c.Respond(msgErrPostStatus)
		return nil
	}
	elapsed := time.Since(start)
	editText := fmt.Sprintf(msgPong, float64(elapsed.Microseconds())/1000.0)
	_, _ = c.Msg.Client.EditMessage(c.Msg.ChatID(), sent.ID, editText, &telegram.SendOptions{
		ParseMode:   "HTML",
		ReplyMarkup: buildPingMarkup(),
	})
	return nil
}

func buildPingMarkup() *telegram.ReplyInlineMarkup {
	return telegram.NewKeyboard().
		AddRow(
			telegram.Button.Data(theme.Help+" Help", "help"),
			telegram.Button.Data(theme.Close+" Close", "close"),
		).
		Build()
}

// handleDc reports the data center of a file or user. The owner can pass a
// user ID or @username as an argument.
func (b *Bot) handleDc(c *Context) error {
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
			typeDisplay := friendlyFileType(reply)
			text := fmt.Sprintf(msgFileDC,
				html.EscapeString(name),
				tgutil.FormatBytes(tgutil.ExtractSize(reply)),
				html.EscapeString(typeDisplay),
				dc)
			opts := &telegram.SendOptions{ParseMode: "HTML", ReplyMarkup: buildCloseMarkup()}
			_, _ = c.Msg.Respond(text, opts)
			return nil
		}
		if err == nil && reply != nil && reply.Sender != nil {
			dc := userDCFromPhoto(reply.Sender)
			name := reply.Sender.FirstName
			if name == "" {
				name = msgFallbackUserName
			}
			text := fmt.Sprintf(msgUserDC, reply.Sender.ID, html.EscapeString(name), reply.Sender.ID, dc)
			opts := &telegram.SendOptions{ParseMode: "HTML", ReplyMarkup: buildUserDCMarkup(reply.Sender)}
			_, _ = c.Msg.Respond(text, opts)
			return nil
		}
	}

	if c.IsOwner && c.Args != "" {
		target, err := resolveUser(b.Pool.Primary().Client, c.Args)
		if err == nil && target != nil {
			dc := userDCFromPhoto(target)
			name := target.FirstName
			if name == "" {
				name = msgFallbackUserName
			}
			text := fmt.Sprintf(msgUserDC, target.ID, html.EscapeString(name), target.ID, dc)
			opts := &telegram.SendOptions{ParseMode: "HTML", ReplyMarkup: buildUserDCMarkup(target)}
			_, _ = c.Msg.Respond(text, opts)
			return nil
		}
		_, _ = c.ReplyFormatted(msgDCInvalidUsage)
		return nil
	}

	sender := c.Msg.Sender
	if sender != nil {
		dc := userDCFromPhoto(sender)
		name := sender.FirstName
		if name == "" {
			name = msgFallbackUserName
		}
		text := fmt.Sprintf(msgYourDC, sender.ID, html.EscapeString(name), sender.ID, dc)
		opts := &telegram.SendOptions{ParseMode: "HTML", ReplyMarkup: buildCloseMarkup()}
		_, _ = c.Msg.Respond(text, opts)
		return nil
	}
	_, _ = c.ReplyFormatted(msgDCAnonError)
	return nil
}

func friendlyFileType(m *telegram.NewMessage) string {
	if m == nil {
		return msgFileTypeUnknown
	}
	switch tgutil.MediaType(m) {
	case "video":
		return msgFileTypeVideo
	case "photo":
		return msgFileTypePhoto
	case "audio":
		return msgFileTypeAudio
	case "voice":
		return msgFileTypeVoice
	case "sticker":
		return msgFileTypeSticker
	case "animation":
		return msgFileTypeAnimation
	case "document":
		return msgFileTypeDocument
	default:
		return msgFileTypeUnknown
	}
}

func buildUserDCMarkup(u *telegram.UserObj) *telegram.ReplyInlineMarkup {
	profileURL := fmt.Sprintf("tg://user?id=%d", u.ID)
	if u.Username != "" {
		profileURL = "https://t.me/" + u.Username
	}
	return telegram.NewKeyboard().
		AddRow(telegram.Button.URL("👤 View Profile", profileURL)).
		AddRow(telegram.Button.Data(theme.Close+" Close", "close")).
		Build()
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
	userID, err := strconv.ParseInt(ref, 10, 64)
	if err != nil {
		return nil, err
	}
	return c.GetUser(userID)
}

// handleAbout: <a href> in the text makes the link copy-pasteable on
// clients that hide inline-button URLs.
func (b *Bot) handleAbout(c *Context) error {
	opts := &telegram.SendOptions{ParseMode: "HTML", ReplyMarkup: buildAboutMarkup()}
	_, _ = c.Msg.Respond(buildAboutText(), opts)
	return nil
}

// buildAboutText is shared by /about and the "about" callback.
func buildAboutText() string {
	return `<b>⚡ About ThunderGo</b>

I'm your go-to bot for <b>instant download &amp; streaming links</b> from Telegram files. 🚀

<b>🌟 Key Features</b>
<blockquote>⚡ <b>Instant Links</b> — get stream + download URLs within seconds
🎬 <b>Online Streaming</b> — watch videos or listen to audio directly in the browser
📦 <b>Universal Support</b> — documents, videos, audio, photos, voice, stickers &amp; more
🔍 <b>Seek Support</b> — jump to any position in streamed media
📚 <b>Batch Mode</b> — process multiple files at once with <code>/link N</code>
💬 <b>Multi-Chat</b> — works in private chats, groups, and channels
🚀 <b>High-Speed</b> — multi-client pool for concurrent independent downloads</blockquote>

<b>🛠️ Tech Stack</b>
<blockquote>🐹 <b>Language:</b> Go 1.26
📚 <b>Telegram Library:</b> gogram
🍃 <b>Database:</b> MongoDB
🌐 <b>HTTP Router:</b> chi</blockquote>

<blockquote>📄 <b>License:</b> Apache License 2.0
📦 <b>Source:</b> <a href="https://github.com/fyaz05/ThunderGo">github.com/fyaz05/ThunderGo</a></blockquote>

<i>💬 Join our <a href="` + communityLink + `">` + communityName + `</a> for support &amp; updates!
💖 If you find me useful, please share me with your friends!</i>`
}

// buildAboutMarkup is shared by /about and the "about" callback.
func buildAboutMarkup() *telegram.ReplyInlineMarkup {
	return telegram.NewKeyboard().
		AddRow(telegram.Button.Data(theme.Help+" Help", "help")).
		AddRow(
			telegram.Button.URL(theme.GitHub+" GitHub", "https://github.com/fyaz05/ThunderGo"),
			telegram.Button.URL(theme.Community+" Community", communityLink),
		).
		AddRow(telegram.Button.Data(theme.Close+" Close", "close")).
		Build()
}

// buildStartMarkup adds a Join Channel row when force-sub is configured.
func buildStartMarkup(hasForceSub bool, forceSubLink, forceSubTitle string) *telegram.ReplyInlineMarkup {
	kb := telegram.NewKeyboard().
		AddRow(
			telegram.Button.Data(theme.Help+" Help", "help"),
			telegram.Button.Data(theme.About+" About", "about"),
		).
		AddRow(
			telegram.Button.URL(theme.GitHub+" GitHub", "https://github.com/fyaz05/ThunderGo"),
			telegram.Button.URL(theme.Community+" Community", communityLink),
			telegram.Button.Data(theme.Close+" Close", "close"),
		)
	if hasForceSub && forceSubLink != "" {
		label := theme.Join + " Join Channel"
		if forceSubTitle != "" {
			label = theme.Join + " Join " + forceSubTitle
		}
		kb = kb.AddRow(telegram.Button.URL(label, forceSubLink))
	}
	return kb.Build()
}

// buildHelpMarkup adds a Join Channel row when force-sub is configured.
func buildHelpMarkup(hasForceSub bool, forceSubLink, forceSubTitle string) *telegram.ReplyInlineMarkup {
	kb := telegram.NewKeyboard().
		AddRow(telegram.Button.Data(theme.About+" About", "about"))
	if hasForceSub && forceSubLink != "" {
		label := theme.Join + " Join Channel"
		if forceSubTitle != "" {
			label = theme.Join + " Join " + forceSubTitle
		}
		kb = kb.AddRow(telegram.Button.URL(label, forceSubLink))
	}
	kb = kb.AddRow(telegram.Button.Data(theme.Close+" Close", "close"))
	return kb.Build()
}

func buildCloseMarkup() *telegram.ReplyInlineMarkup {
	return telegram.NewKeyboard().
		AddRow(telegram.Button.Data(theme.Close+" Close", "close")).
		Build()
}

// handleStats is owner-only because the metrics leak build details an attacker
// could use to target toolchain CVEs.
func (b *Bot) handleStats(c *Context) error {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	uptime := formatReadableDuration(time.Since(b.startTime))

	var sb strings.Builder
	sb.WriteString("<b>📊 Runtime Statistics</b>\n\n")

	sb.WriteString("<b>⏱️ Uptime</b>\n<blockquote>")
	sb.WriteString(fmt.Sprintf("⏰ <b>Duration:</b> <code>%s</code>", uptime))
	sb.WriteString("</blockquote>\n\n")

	sb.WriteString("<b>⚙️ Runtime</b>\n<blockquote>")
	sb.WriteString(fmt.Sprintf("🐹 <b>Go version:</b> <code>%s</code>\n", runtime.Version()))
	sb.WriteString(fmt.Sprintf("🧵 <b>Goroutines:</b> <code>%d</code>\n", runtime.NumGoroutine()))
	sb.WriteString(fmt.Sprintf("💻 <b>CPUs:</b> <code>%d</code>\n", runtime.NumCPU()))
	sb.WriteString(fmt.Sprintf("🔧 <b>CGO enabled:</b> <code>%s</code>", cgoEnabled))
	sb.WriteString("</blockquote>\n\n")

	sb.WriteString("<b>💾 Memory</b>\n<blockquote>")
	// #nosec G115 — runtime metrics safely fit in int64
	sb.WriteString(fmt.Sprintf("📦 <b>Allocated:</b> <code>%s</code>\n", tgutil.FormatBytes(int64(ms.Alloc))))
	// #nosec G115 — runtime metrics safely fit in int64
	sb.WriteString(fmt.Sprintf("📊 <b>Total allocated:</b> <code>%s</code>\n", tgutil.FormatBytes(int64(ms.TotalAlloc))))
	// #nosec G115 — runtime metrics safely fit in int64
	sb.WriteString(fmt.Sprintf("🖥️ <b>System:</b> <code>%s</code>\n", tgutil.FormatBytes(int64(ms.Sys))))
	sb.WriteString(fmt.Sprintf("🧩 <b>Heap objects:</b> <code>%d</code>\n", ms.HeapObjects))
	sb.WriteString(fmt.Sprintf("♻️ <b>GC cycles:</b> <code>%d</code>", ms.NumGC))
	sb.WriteString("</blockquote>\n\n")

	sb.WriteString("<b>🔗 Client Pool</b>\n<blockquote>")
	sb.WriteString(fmt.Sprintf("🤖 <b>Active clients:</b> <code>%d</code>\n", b.Pool.Len()))
	sb.WriteString(fmt.Sprintf("⚡ <b>Total in-flight:</b> <code>%d</code>", b.Pool.TotalInflight()))
	sb.WriteString("</blockquote>")

	opts := &telegram.SendOptions{ParseMode: "HTML", ReplyMarkup: buildCloseMarkup()}
	_, _ = c.Msg.Respond(sb.String(), opts)
	return nil
}

// formatReadableDuration omits leading zero components. Negative clamps to 0.
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

// handleHelpCallback edits the message so the chat stays clean.

func (b *Bot) handleHelpCallback(cq *telegram.CallbackQuery) error {
	if cq == nil {
		return nil
	}
	_, _ = cq.Answer(msgCallbackHelpAnswered)

	opts := &telegram.SendOptions{
		ParseMode:   "HTML",
		ReplyMarkup: buildHelpMarkup(b.Cfg.ForceSubChannelID != 0, b.forceSubLink, b.forceSubTitle),
	}
	_, err := cq.Edit(b.buildHelpText(), opts)
	if err != nil {
		// Fallback: send as a new message if edit fails (e.g. message too old).
		b.Log.Debug("help callback: edit failed, sending new message", "error", err)
		_, _ = cq.Client.SendMessage(cq.GetChatID(), b.buildHelpText(), opts)
	}
	return nil
}

func (b *Bot) handleAboutCallback(cq *telegram.CallbackQuery) error {
	if cq == nil {
		return nil
	}
	_, _ = cq.Answer(msgCallbackAboutAnswered)

	opts := &telegram.SendOptions{
		ParseMode:   "HTML",
		ReplyMarkup: buildAboutMarkup(),
	}
	_, err := cq.Edit(buildAboutText(), opts)
	if err != nil {
		b.Log.Debug("about callback: edit failed, sending new message", "error", err)
		_, _ = cq.Client.SendMessage(cq.GetChatID(), buildAboutText(), opts)
	}
	return nil
}

// handleCloseCallback: best-effort delete. Only the original recipient (private
// chats) or the bot owner can close.
func (b *Bot) handleCloseCallback(cq *telegram.CallbackQuery) error {
	if cq == nil {
		return nil
	}
	if cq.SenderID != cq.GetChatID() && !b.Cfg.IsOwner(cq.SenderID) {
		_, _ = cq.Answer(msgCallbackCloseDenied, &telegram.CallbackOptions{Alert: true})
		return nil
	}
	_, _ = cq.Answer(msgCallbackCloseAnswered)
	_, err := cq.Client.DeleteMessages(cq.GetChatID(), []int32{cq.MessageID})
	if err != nil {
		b.Log.Debug("close callback: could not delete message",
			"chat_id", cq.GetChatID(), "msg_id", cq.MessageID, "error", err)
	}
	return nil
}

// handleUnsupportedCallback prevents unknown buttons leaving the user with a
// perpetual "Loading…" spinner.
func (b *Bot) handleUnsupportedCallback(cq *telegram.CallbackQuery) error {
	if cq == nil {
		return nil
	}
	// OnCallbackQuery matches all callbacks, but specific handlers in the
	// DefaultGroup may have already answered this one.
	data := cq.DataString()
	if knownCallbacks[data] || strings.HasPrefix(data, "cancel_") {
		return nil
	}
	_, _ = cq.Answer(msgCallbackUnsupported, &telegram.CallbackOptions{Alert: true})
	b.Log.Debug("unsupported callback", "data", data, "sender_id", cq.SenderID)
	return nil
}
