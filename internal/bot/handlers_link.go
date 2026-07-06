package bot

import (
	"context"
	"fmt"
	"html"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amarnathcjd/gogram/telegram"

	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/store"
)

// handleLink is the group /link command. With no arg it processes the replied-to
// media message; with /link N it processes the next N consecutive messages.
func (b *Bot) handleLink(c *Context) error {
	batchCap := b.Cfg.BatchCap

	if c.Msg.IsPrivate() {
		_, _ = c.ReplyFormatted(msgUsageLinkGroup)
		return nil
	}

	if !b.botIsAdminIn(c.Msg) {
		_, _ = c.ReplyFormatted(msgErrNotAdmin)
		return nil
	}

	if !c.Msg.IsReply() {
		_, _ = c.ReplyFormatted(msgUsageLinkReply)
		return nil
	}

	n := 1
	if c.Args != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(c.Args))
		if err != nil {
			_, _ = c.ReplyFormatted(msgUsageLinkN)
			return nil
		}
		if parsed < 1 || parsed > batchCap {
			_, _ = c.ReplyFormatted(fmt.Sprintf(msgUsageLinkNRange, batchCap))
			return nil
		}
		n = parsed
	}

	if n == 1 {
		return b.linkSingle(c)
	}
	return b.linkBatch(c, n)
}

func (b *Bot) linkSingle(c *Context) error {
	status, _ := c.Reply(msgProcessing)
	if status == nil {
		_, _ = c.ReplyFormatted(msgErrPostStatus)
		return nil
	}
	reply, err := c.Msg.GetReplyMessage()
	if err != nil || reply == nil {
		_, _ = b.editStatusSafe(status, msgErrFetchMsg)
		return nil
	}
	if !reply.IsMedia() {
		_, _ = b.editStatusSafe(status, msgErrNoMedia)
		return nil
	}

	// The user must have started the bot in private chat first.
	if !b.userHasStarted(c.Msg.SenderID()) {
		_, _ = b.editStatusSafe(status, msgUsageLinkPrivate)
		b.userStartedPrompt(c.Msg)
		return nil
	}

	ctx, cancel := context.WithTimeout(b.baseCtx, 60*time.Second)
	defer cancel()
	result := b.Ingester.Ingest(ctx, reply)
	if result.Err != nil {
		b.Log.Warn("link single ingest failed", "error", result.Err)
		_, _ = b.editStatusSafe(status, msgErrProcessFile)
		return nil
	}

	streamURL := b.Cfg.FileURL(result.File.Hash, result.File.FileName)
	downloadURL := b.Cfg.FileRawURL(result.File.Hash, result.File.FileName)
	streamURL, downloadURL = b.maybeShorten(c, streamURL, downloadURL)

	text := formatLinkMessage(result.File, streamURL, downloadURL, result.Reused)
	b.editStatusWithButtons(status, text, streamURL, downloadURL)

	// Send the same links in a private message. If the DM fails (user blocked
	// the bot), tell them in the group.
	if !b.sendPrivateLinksChecked(c.Msg.SenderID(), result.File, streamURL, downloadURL, result.Reused) {
		_, _ = c.Msg.Respond(msgErrDMBlocked)
	}

	// Vault log for fresh ingests only.
	if !result.Reused {
		source := ingest.Source{
			Kind:      "group",
			UserName:  userName(c.Msg.Sender),
			UserID:    c.Msg.SenderID(),
			ChatTitle: chatTitle(c.Msg),
			ChatID:    c.Msg.ChatID(),
		}
		logCtx, logCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
		defer logCancel()
		_ = b.Ingester.PostVaultLog(logCtx, result.File, source, streamURL, downloadURL)
	}
	return nil
}

func (b *Bot) linkBatch(c *Context, n int) error {
	status, _ := c.Reply(fmt.Sprintf(msgProcessingN, n))
	if status == nil {
		_, _ = c.ReplyFormatted(msgErrPostStatus)
		return nil
	}
	reply, err := c.Msg.GetReplyMessage()
	if err != nil || reply == nil {
		_, _ = b.editStatusSafe(status, msgErrFetchMsg)
		return nil
	}

	// The user must have started the bot in private chat first.
	if !b.userHasStarted(c.Msg.SenderID()) {
		_, _ = b.editStatusSafe(status, msgUsageLinkPrivate)
		b.userStartedPrompt(c.Msg)
		return nil
	}

	// Batch context: 5-worker pool, deadline scales as 30+2n seconds.
	batchCtx, batchCancel := context.WithTimeout(b.baseCtx, time.Duration(30+2*n)*time.Second)
	defer batchCancel()

	// Fetch the next N messages starting from reply.ID. messages.search returns
	// newest-first, so we reverse the slice below to restore chronological order.
	primary := b.Pool.Primary()
	if primary == nil {
		b.Log.Warn("link batch: no primary client")
		_, _ = b.editStatusSafe(status, msgErrProcessFile)
		return nil
	}
	//gosec:disable G115 // n is bounded [1, BatchCap] at the caller; BatchCap defaults to 50; reply.ID is int32, +BatchCap cannot overflow
	msgs, err := primary.GetMessages(c.Msg.ChatID(), &telegram.SearchOption{
		MinID: reply.ID - 1,
		MaxID: reply.ID + int32(n),
		Limit: int32(n),
	})
	//gosec:enable G115
	if err != nil {
		b.Log.Warn("link batch get messages failed", "error", err)
		_, _ = b.editStatusSafe(status, msgErrProcessFile)
		return nil
	}
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}

	// chunkSize: max file links per Telegram message (10 links × ~300 chars ≈ ~3000,
	// safely under Telegram's 4096-char limit).
	const chunkSize = 10
	// chunkDelay stays under Telegram's per-chat rate limit.
	const chunkDelay = 1500 * time.Millisecond

	succeeded := 0
	skipped := 0 // non-media messages in the batch range.
	failed := 0
	dmFailed := 0 // chunks that could not be DM'd to the requester.
	var chunk []string

	flushChunk := func() {
		if len(chunk) == 0 {
			return
		}
		text := strings.Join(chunk, "\n\n---\n\n")
		_, _ = c.Msg.Respond(text, &telegram.SendOptions{ParseMode: "HTML"})
		// Best-effort DM; failures surfaced via dmFailed at the end.
		if _, err := primary.SendMessage(c.Msg.SenderID(), text, &telegram.SendOptions{ParseMode: "HTML"}); err != nil {
			dmFailed++
			b.Log.Debug("batch DM send failed",
				"user_id", c.Msg.SenderID(), "chunk_size", len(chunk), "error", err)
		}
		chunk = chunk[:0]
		select {
		case <-time.After(chunkDelay):
		case <-batchCtx.Done():
		}
	}

	// Process ingest+shorten concurrently in a 5-worker pool. Results are written
	// to an index-keyed slice so chronological flush order is preserved.
	const numWorkers = 5

	type batchResult struct {
		index     int    // original chronological index into msgs
		text      string // formatted link message (after shortening)
		succeeded bool
		skipped   bool
		failed    bool
	}

	results := make([]batchResult, len(msgs))

	// Producer: feed message indices into a channel; close on cancel.
	indices := make(chan int, numWorkers)
	go func() {
		defer close(indices)
		for i := range msgs {
			select {
			case indices <- i:
			case <-batchCtx.Done():
				return
			}
		}
	}()

	// Workers: each pulls indices and runs ingest → format → maybeShorten.
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					b.Log.Error("panic in batch worker", "recover", r, "stack", string(debugStack()))
				}
			}()
			for i := range indices {
				if batchCtx.Err() != nil {
					return
				}
				m := &msgs[i]
				if !m.IsMedia() {
					// Non-media messages in the range are expected; count as skipped
					// so the failure count reflects only genuine ingestion errors.
					results[i] = batchResult{index: i, skipped: true}
					continue
				}
				result := b.Ingester.Ingest(batchCtx, m)
				if result.Err != nil {
					b.Log.Warn("batch ingest failed", "error", result.Err)
					results[i] = batchResult{index: i, failed: true}
					continue
				}
				streamURL := b.Cfg.FileURL(result.File.Hash, result.File.FileName)
				downloadURL := b.Cfg.FileRawURL(result.File.Hash, result.File.FileName)
				streamURL, downloadURL = b.maybeShorten(c, streamURL, downloadURL)
				results[i] = batchResult{
					index:     i,
					text:      formatBatchLinkMessage(result.File, streamURL, downloadURL, result.Reused),
					succeeded: true,
				}
			}
		}()
	}
	wg.Wait()

	// Drain results in chronological order, flushing chunks.
	for i := range results {
		r := results[i]
		if r.skipped {
			skipped++
			continue
		}
		if r.failed {
			failed++
			continue
		}
		if r.succeeded {
			succeeded++
			chunk = append(chunk, r.text)
			if len(chunk) >= chunkSize {
				flushChunk()
			}
		}
	}
	flushChunk()

	summary := fmt.Sprintf(msgBatchSummary, succeeded, skipped, failed)
	_, _ = b.editStatusSafe(status, summary)
	if dmFailed > 0 {
		_, _ = c.Msg.Respond(
			fmt.Sprintf(msgBatchDMFailed, dmFailed),
			&telegram.SendOptions{ParseMode: "HTML"})
	}
	return nil
}

// handlePrivateMedia ingests a file sent in private chat: posts a status
// message, processes the file, replaces the status with the links and inline
// Stream/Download buttons, and posts a vault log.
func (b *Bot) handlePrivateMedia(m *telegram.NewMessage, isOwner, isAuthorized bool) {
	status, _ := m.Respond(msgProcessing)
	if status == nil {
		b.Log.Warn("could not post processing status")
		return
	}

	ctx, cancel := context.WithTimeout(b.baseCtx, 60*time.Second)
	defer cancel()
	result := b.Ingester.Ingest(ctx, m)
	if result.Err != nil {
		b.Log.Warn("private media ingest failed", "error", result.Err)
		_, _ = b.editStatusSafe(status, msgErrProcessFile)
		return
	}

	streamURL := b.Cfg.FileURL(result.File.Hash, result.File.FileName)
	downloadURL := b.Cfg.FileRawURL(result.File.Hash, result.File.FileName)
	if !isOwner && !isAuthorized {
		streamURL, downloadURL = b.maybeShortenRaw(streamURL, downloadURL)
	}

	text := formatLinkMessage(result.File, streamURL, downloadURL, result.Reused)
	b.editStatusWithButtons(status, text, streamURL, downloadURL)

	source := ingest.Source{Kind: "private", UserName: userName(m.Sender), UserID: m.SenderID()}
	if !result.Reused {
		logCtx, logCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
		defer logCancel()
		if err := b.Ingester.PostVaultLog(logCtx, result.File, source, streamURL, downloadURL); err != nil {
			b.Log.Warn("posting vault log", "error", err)
		}
	}
}

// handleChannelAutoProcess edits a channel post to attach stream/download
// buttons. The bot must be admin in the channel to receive the post at all;
// we still verify admin status defensively.
func (b *Bot) handleChannelAutoProcess(m *telegram.NewMessage) {
	if !b.botIsAdminIn(m) {
		b.Log.Debug("channel auto-process: bot is not admin in channel; skipping", "chat_id", m.ChatID())
		return
	}
	ctx, cancel := context.WithTimeout(b.baseCtx, 60*time.Second)
	defer cancel()
	result := b.Ingester.Ingest(ctx, m)
	if result.Err != nil {
		b.Log.Warn("channel auto-process ingest failed", "error", result.Err)
		return
	}

	streamURL := b.Cfg.FileURL(result.File.Hash, result.File.FileName)
	downloadURL := b.Cfg.FileRawURL(result.File.Hash, result.File.FileName)

	kb := telegram.NewKeyboard().
		AddRow(telegram.Button.URL(theme.Stream+" Stream", streamURL), telegram.Button.URL(theme.Download+" Download", downloadURL)).
		Build()
	// The original caption is plain text; escape and request ParseMode=HTML so the
	// wire payload is deterministic across gogram versions.
	caption := html.EscapeString(m.Text())
	_, err := m.Client.EditMessage(m.ChatID(), m.ID, caption, &telegram.SendOptions{
		ParseMode:   "HTML",
		ReplyMarkup: kb,
	})
	if err != nil {
		b.Log.Warn("editing channel post to attach buttons", "error", err)
	}

	source := ingest.Source{Kind: "channel", ChatTitle: chatTitle(m), ChatID: m.ChatID()}
	if !result.Reused {
		logCtx, logCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
		defer logCancel()
		if err := b.Ingester.PostVaultLog(logCtx, result.File, source, streamURL, downloadURL); err != nil {
			b.Log.Warn("posting vault log for channel post", "error", err)
		}
	}
}

// --- helpers ---

// maybeShorten shortens URLs for non-owner, non-authorized users when the
// shortener is configured. Returns the original URLs unchanged otherwise.
func (b *Bot) maybeShorten(c *Context, streamURL, downloadURL string) (string, string) {
	if c.IsOwner || c.IsAuthorized {
		return streamURL, downloadURL
	}
	return b.maybeShortenRaw(streamURL, downloadURL)
}

func (b *Bot) maybeShortenRaw(streamURL, downloadURL string) (string, string) {
	if b.Shortener == nil {
		return streamURL, downloadURL
	}
	ctx, cancel := context.WithTimeout(b.baseCtx, 10*time.Second)
	defer cancel()
	// Shorten both URLs concurrently; both share the same ctx.
	var sStream, sDownload string
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sStream = b.Shortener.Shorten(ctx, streamURL)
	}()
	go func() {
		defer wg.Done()
		sDownload = b.Shortener.Shorten(ctx, downloadURL)
	}()
	wg.Wait()
	return sStream, sDownload
}

// formatLinkMessage renders the user-facing reply for an ingested file
// (single-file / private-media path — includes size and type).
func formatLinkMessage(rec *store.FileRecord, streamURL, downloadURL string, reused bool) string {
	reuseTag := ""
	if reused {
		reuseTag = " " + theme.Recycle
	}
	return fmt.Sprintf(msgReady,
		reuseTag,
		html.EscapeString(rec.FileName),
		rec.Size,
		html.EscapeString(rec.MimeType),
		html.EscapeString(streamURL),
		html.EscapeString(downloadURL),
	)
}

// formatBatchLinkMessage renders a compact link message for the batch path,
// omitting size and type so a 50-file batch stays scannable.
func formatBatchLinkMessage(rec *store.FileRecord, streamURL, downloadURL string, reused bool) string {
	reuseTag := ""
	if reused {
		reuseTag = " " + theme.Recycle
	}
	return fmt.Sprintf(msgBatchReady,
		reuseTag,
		html.EscapeString(rec.FileName),
		html.EscapeString(streamURL),
		html.EscapeString(downloadURL),
	)
}

// userHasStarted reports whether the user has ever /started the bot.
func (b *Bot) userHasStarted(userID int64) bool {
	ctx, cancel := context.WithTimeout(b.baseCtx, 10*time.Second)
	defer cancel()
	has, err := b.Store.HasUser(ctx, userID)
	if err != nil {
		b.Log.Debug("HasUser lookup failed", "user_id", userID, "error", err)
		return false
	}
	return has
}

// sendPrivateLinksChecked DMs the user a copy of the links. Returns false
// if the DM could not be delivered (user blocked the bot).
func (b *Bot) sendPrivateLinksChecked(userID int64, rec *store.FileRecord, streamURL, downloadURL string, reused bool) bool {
	primary := b.Pool.Primary()
	if primary == nil || userID == 0 {
		return false
	}
	text := formatLinkMessage(rec, streamURL, downloadURL, reused)
	_, err := primary.SendMessage(userID, text, &telegram.SendOptions{ParseMode: "HTML"})
	if err != nil {
		b.Log.Debug("could not DM user (likely blocked)", "user_id", userID, "error", err)
		return false
	}
	return true
}

// botIsAdminIn reports whether the bot is an admin in the chat. Uses the
// cached bot user ID (no GetMe call).
func (b *Bot) botIsAdminIn(m *telegram.NewMessage) bool {
	if m == nil || b.primary == nil || b.botUserID == 0 {
		return false
	}
	member, err := b.primary.GetChatMember(m.ChatID(), b.botUserID)
	if err != nil {
		b.Log.Debug("botIsAdminIn: GetChatMember failed", "chat_id", m.ChatID(), "error", err)
		return false
	}
	if member == nil {
		return false
	}
	return member.Status == "admin" || member.Status == "creator"
}

// userStartedPrompt is shown when a /link user hasn't started the bot in
// private chat yet. Uses the cached bot username.
func (b *Bot) userStartedPrompt(m *telegram.NewMessage) {
	botUsername := b.botUsername
	opts := &telegram.SendOptions{ParseMode: "HTML"}
	if botUsername != "" {
		opts.ReplyMarkup = telegram.InlineURL(theme.Stream+" Start", "https://t.me/"+botUsername+"?start=link")
	}
	_, _ = m.Respond(msgUsageLinkPrivate, opts)
}

// editStatusSafe edits the status message; logs on error.
func (b *Bot) editStatusSafe(status *telegram.NewMessage, text string) (*telegram.NewMessage, error) {
	if status == nil {
		return nil, nil
	}
	primary := b.Pool.Primary()
	if primary == nil {
		b.Log.Warn("editStatusSafe: no primary client")
		return nil, nil
	}
	return primary.EditMessage(status.ChatID(), status.ID, text, &telegram.SendOptions{ParseMode: "HTML"})
}

// editStatusWithButtons edits the status message and attaches Stream + Download
// inline URL buttons.
func (b *Bot) editStatusWithButtons(status *telegram.NewMessage, text, streamURL, downloadURL string) {
	if status == nil {
		return
	}
	kb := telegram.NewKeyboard().
		AddRow(
			telegram.Button.URL(theme.Stream+" Stream", streamURL),
			telegram.Button.URL(theme.Download+" Download", downloadURL),
		).
		Build()
	_, err := b.Pool.Primary().EditMessage(status.ChatID(), status.ID, text, &telegram.SendOptions{
		ParseMode:   "HTML",
		ReplyMarkup: kb,
	})
	if err != nil {
		b.Log.Debug("editStatusWithButtons failed", "error", err)
		// Fallback: edit without buttons.
		_, _ = b.editStatusSafe(status, text)
	}
}

func userName(u *telegram.UserObj) string {
	if u == nil {
		return ""
	}
	return strings.TrimSpace(u.FirstName + " " + u.LastName)
}

func chatTitle(m *telegram.NewMessage) string {
	if m == nil {
		return ""
	}
	if m.Channel != nil {
		return m.Channel.Title
	}
	if m.Chat != nil {
		return m.Chat.Title
	}
	return ""
}
