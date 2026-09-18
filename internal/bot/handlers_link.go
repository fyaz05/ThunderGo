package bot

import (
	"context"
	"fmt"
	"html"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// handleLink is the group /link command. With no arg it processes the replied-to
// media message; with /link N it processes the next N consecutive messages.
func (b *Bot) handleLink(c *Context) error {
	batchCap := b.Cfg.BatchCap

	if c.Msg.IsPrivate {
		_, _ = c.ReplyFormatted(msgUsageLinkGroup)
		return nil
	}

	if !b.botIsAdminIn(c.Ctx, c.Msg.ChatID) {
		_, _ = c.ReplyFormatted(msgErrNotAdmin)
		return nil
	}

	if c.Msg.ReplyToMsgID == 0 {
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
	status, err := c.Reply(msgProcessing)
	if err != nil || status.ID == 0 {
		_, _ = c.ReplyFormatted(msgErrPostStatus)
		return nil
	}
	reply, err := b.Backend.GetReplyMessage(c.Ctx, c.Msg.ChatID, c.Msg.MsgID)
	if err != nil {
		_ = b.editStatusSafe(c.Ctx, status, msgErrFetchMsg)
		return nil
	}
	if reply.Media == nil {
		_ = b.editStatusSafe(c.Ctx, status, msgErrNoMedia)
		return nil
	}

	// The user must have started the bot in private chat first.
	if !b.userHasStarted(c.Msg.SenderID) {
		_ = b.editStatusSafe(c.Ctx, status, msgUsageLinkPrivate)
		b.userStartedPrompt(c.Ctx, c.Msg)
		return nil
	}

	ctx, cancel := context.WithTimeout(b.baseCtx, 60*time.Second)
	defer cancel()
	result := b.Ingester.Ingest(ctx, reply)
	if result.Err != nil {
		b.Log.Warn("link single ingest failed", "error", result.Err)
		_ = b.editStatusSafe(c.Ctx, status, msgErrProcessFile)
		return nil
	}

	streamURL := b.Cfg.FileURL(result.File.Hash, result.File.FileName)
	downloadURL := b.Cfg.FileRawURL(result.File.Hash, result.File.FileName)
	streamURL, downloadURL = b.maybeShorten(c, streamURL, downloadURL)

	text := formatLinkMessage(result.File, streamURL, downloadURL, result.Reused, b.Cfg.FileTTLDays)
	b.editStatusWithButtons(c.Ctx, status, text, streamURL, downloadURL)

	// Send the same links in a private message. If the DM fails (user blocked
	// the bot), tell them in the group.
	if !b.sendPrivateLinksChecked(c.Ctx, c.Msg.SenderID, result.File, streamURL, downloadURL, result.Reused, c.Msg.ChatTitle) {
		_, _ = c.Respond(msgErrDMBlocked)
	}

	// Vault log for fresh ingests only.
	if !result.Reused {
		source := ingest.Source{
			Kind:      "group",
			UserName:  b.senderName(ctx, c.Msg.SenderID),
			UserID:    c.Msg.SenderID,
			ChatTitle: c.Msg.ChatTitle,
			ChatID:    c.Msg.ChatID,
		}
		logCtx, logCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
		defer logCancel()
		_ = b.Ingester.PostVaultLog(logCtx, result.File, source, streamURL, downloadURL)
	}
	return nil
}

func (b *Bot) linkBatch(c *Context, n int) error {
	status, err := c.Reply(fmt.Sprintf(msgProcessingN, n))
	if err != nil || status.ID == 0 {
		_, _ = c.ReplyFormatted(msgErrPostStatus)
		return nil
	}
	reply, err := b.Backend.GetReplyMessage(c.Ctx, c.Msg.ChatID, c.Msg.MsgID)
	if err != nil {
		_ = b.editStatusSafe(c.Ctx, status, msgErrFetchMsg)
		return nil
	}

	// The user must have started the bot in private chat first.
	if !b.userHasStarted(c.Msg.SenderID) {
		_ = b.editStatusSafe(c.Ctx, status, msgUsageLinkPrivate)
		b.userStartedPrompt(c.Ctx, c.Msg)
		return nil
	}

	// Batch context: 5-worker pool, deadline scales as 30+2n seconds.
	batchCtx, batchCancel := context.WithTimeout(b.baseCtx, time.Duration(30+2*n)*time.Second)
	defer batchCancel()

	// Fetch the next N messages starting after the reply. The backend
	// returns the (minID, maxID] window in chronological order already.
	msgs, err := b.Backend.GetMessagesBulk(batchCtx, c.Msg.ChatID, reply.MsgID-1, reply.MsgID+n, n)
	if err != nil {
		b.Log.Warn("link batch get messages failed", "error", err)
		_ = b.editStatusSafe(c.Ctx, status, msgErrProcessFile)
		return nil
	}

	// chunkSize: max file links per Telegram message (10 links × ~300 chars ≈ ~3000,
	// safely under Telegram's 4096-char limit).
	const chunkSize = 10
	// chunkDelay stays under Telegram's per-chat rate limit.
	const chunkDelay = 1500 * time.Millisecond

	succeeded := 0
	skipped := 0
	failed := 0
	dmFailed := 0
	var chunk []string

	flushChunk := func() {
		if len(chunk) == 0 {
			return
		}
		groupText := fmt.Sprintf(msgBatchLinksReady, len(chunk)) + "\n\n" + strings.Join(chunk, "\n\n---\n\n")
		_ = b.Backend.SendHTML(b.baseCtx, c.Msg.ChatID, groupText, nil, 0)
		// Best-effort DM with batch prefix; failures surfaced via dmFailed at the end.
		title := c.Msg.ChatTitle
		if title == "" {
			title = msgFallbackChatTitle
		}
		dmText := fmt.Sprintf(msgDMBatchPrefix, html.EscapeString(title)) + "\n" + groupText
		if err := b.Backend.SendHTML(b.baseCtx, c.Msg.SenderID, dmText, nil, 0); err != nil {
			dmFailed++
			b.Log.Debug("batch DM send failed",
				"user_id", c.Msg.SenderID, "chunk_size", len(chunk), "error", err)
		}
		chunk = chunk[:0]
		select {
		case <-time.After(chunkDelay):
		case <-batchCtx.Done():
		}
	}

	// Process ingest+shorten concurrently in a 5-worker pool. Results are written
	// to an index-keyed slice so chronological flush order is preserved.
	// Live progress is shown by editing the status message every 5 completions.
	const numWorkers = 5

	type batchResult struct {
		index     int
		text      string
		succeeded bool
		skipped   bool
		failed    bool
	}

	results := make([]batchResult, len(msgs))

	var atomicProcessed atomic.Int64
	var atomicFailed atomic.Int64
	total := int64(len(msgs))
	lastProgressUpdate := atomic.Int64{}
	var progressMu sync.Mutex

	// updateProgress edits the status message to show live progress.
	// Throttled to every 5 completions to avoid Telegram flood-waits.
	updateProgress := func() {
		processed := atomicProcessed.Load()
		failed := atomicFailed.Load()
		if processed-lastProgressUpdate.Load() < 5 && processed < total {
			return
		}
		lastProgressUpdate.Store(processed)
		text := fmt.Sprintf(msgProcessingStatus, processed, total, failed)
		progressMu.Lock()
		_ = b.editStatusSafe(batchCtx, status, text)
		progressMu.Unlock()
	}

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
				m := msgs[i]
				if m.Media == nil {
					// Non-media messages in the range are expected; count as skipped
					// so the failure count reflects only genuine ingestion errors.
					results[i] = batchResult{index: i, skipped: true}
					atomicProcessed.Add(1)
					updateProgress()
					continue
				}
				result := b.Ingester.Ingest(batchCtx, m)
				if result.Err != nil {
					b.Log.Warn("batch ingest failed", "error", result.Err)
					results[i] = batchResult{index: i, failed: true}
					atomicFailed.Add(1)
					atomicProcessed.Add(1)
					updateProgress()
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
				atomicProcessed.Add(1)
				updateProgress()
			}
		}()
	}
	wg.Wait()

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
	_ = b.editStatusSafe(b.baseCtx, status, summary)
	if dmFailed > 0 {
		_ = b.Backend.SendHTML(b.baseCtx, c.Msg.ChatID,
			fmt.Sprintf(msgBatchDMFailed, dmFailed), nil, 0)
	}
	return nil
}

// handlePrivateMedia ingests a file sent in private chat: posts a status
// message, processes the file, replaces the status with the links and inline
// Stream/Download buttons, and posts a vault log.
func (b *Bot) handlePrivateMedia(ctx context.Context, m tgutil.IncomingMsg, isOwner, isAuthorized bool) {
	id, err := b.Backend.SendHTMLMsg(ctx, m.ChatID, msgProcessing, nil, 0)
	if err != nil || id == 0 {
		b.Log.Warn("could not post processing status")
		return
	}
	status := Sent{ChatID: m.ChatID, ID: id}

	ingestCtx, cancel := context.WithTimeout(b.baseCtx, 60*time.Second)
	defer cancel()
	result := b.Ingester.Ingest(ingestCtx, m)
	if result.Err != nil {
		b.Log.Warn("private media ingest failed", "error", result.Err)
		_ = b.editStatusSafe(ctx, status, msgErrProcessFile)
		return
	}

	streamURL := b.Cfg.FileURL(result.File.Hash, result.File.FileName)
	downloadURL := b.Cfg.FileRawURL(result.File.Hash, result.File.FileName)
	if !isOwner && !isAuthorized {
		streamURL, downloadURL = b.maybeShortenRaw(streamURL, downloadURL)
	}

	text := formatLinkMessage(result.File, streamURL, downloadURL, result.Reused, b.Cfg.FileTTLDays)
	b.editStatusWithButtons(ctx, status, text, streamURL, downloadURL)

	if !result.Reused {
		source := ingest.Source{Kind: "private", UserName: b.senderName(ctx, m.SenderID), UserID: m.SenderID}
		logCtx, logCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
		defer logCancel()
		if err := b.Ingester.PostVaultLog(logCtx, result.File, source, streamURL, downloadURL); err != nil {
			b.Log.Warn("posting vault log", "error", err)
		}
	}
}

// The bot must be admin in the channel to receive the post at all;
// we still verify admin status defensively.
func (b *Bot) handleChannelAutoProcess(ctx context.Context, m tgutil.IncomingMsg) {
	if !b.botIsAdminIn(ctx, m.ChatID) {
		b.Log.Debug("channel auto-process: bot is not admin in channel; skipping", "chat_id", m.ChatID)
		return
	}
	ingestCtx, cancel := context.WithTimeout(b.baseCtx, 60*time.Second)
	defer cancel()
	result := b.Ingester.Ingest(ingestCtx, m)
	if result.Err != nil {
		b.Log.Warn("channel auto-process ingest failed", "error", result.Err)
		return
	}

	streamURL := b.Cfg.FileURL(result.File.Hash, result.File.FileName)
	downloadURL := b.Cfg.FileRawURL(result.File.Hash, result.File.FileName)

	kb := tgutil.NewKeyboard().
		AddRow(tgutil.InlineURL(theme.Stream+" Stream", streamURL), tgutil.InlineURL(theme.Download+" Download", downloadURL)).
		Build()
	// The original caption is plain text; escape and request HTML parse mode
	// so the wire payload is deterministic across transport versions.
	caption := html.EscapeString(m.Text)
	if err := b.Backend.EditHTML(ctx, m.ChatID, m.MsgID, caption, kb); err != nil {
		b.Log.Warn("editing channel post to attach buttons", "error", err)
	}

	source := ingest.Source{Kind: "channel", ChatTitle: m.ChatTitle, ChatID: m.ChatID}
	if !result.Reused {
		logCtx, logCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
		defer logCancel()
		if err := b.Ingester.PostVaultLog(logCtx, result.File, source, streamURL, downloadURL); err != nil {
			b.Log.Warn("posting vault log for channel post", "error", err)
		}
	}
}

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

func formatLinkMessage(rec *store.FileRecord, streamURL, downloadURL string, reused bool, ttlDays int) string {
	reuseTag := ""
	if reused {
		reuseTag = " " + theme.Recycle
	}
	return fmt.Sprintf(msgReady,
		reuseTag,
		html.EscapeString(rec.FileName),
		tgutil.FormatBytes(rec.Size),
		html.EscapeString(rec.MimeType),
		html.EscapeString(downloadURL),
		html.EscapeString(streamURL),
		fileExpiryNote(ttlDays),
	)
}

func fileExpiryNote(ttlDays int) string {
	if ttlDays <= 0 {
		return msgFileExpiryNever
	}
	return fmt.Sprintf(msgFileExpiryDays, ttlDays)
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
		html.EscapeString(downloadURL),
		html.EscapeString(streamURL),
	)
}

func (b *Bot) userHasStarted(userID int64) bool {
	ctx, cancel := context.WithTimeout(b.baseCtx, 10*time.Second)
	defer cancel()
	has, err := b.st().HasUser(ctx, userID)
	if err != nil {
		b.Log.Debug("HasUser lookup failed", "user_id", userID, "error", err)
		return false
	}
	return has
}

// Returns false if the DM could not be delivered (user blocked the bot).
// Includes a "📬 From {chat_title}" prefix when chatTitle is non-empty.
func (b *Bot) sendPrivateLinksChecked(ctx context.Context, userID int64, rec *store.FileRecord, streamURL, downloadURL string, reused bool, chatTitle string) bool {
	if b.Backend == nil || userID == 0 {
		return false
	}
	text := formatLinkMessage(rec, streamURL, downloadURL, reused, b.Cfg.FileTTLDays)
	if chatTitle != "" {
		text = fmt.Sprintf(msgDMSinglePrefix, html.EscapeString(chatTitle)) + text
	}
	if err := b.Backend.SendHTML(ctx, userID, text, nil, 0); err != nil {
		b.Log.Debug("could not DM user (likely blocked)", "user_id", userID, "error", err)
		return false
	}
	return true
}

// botIsAdminIn reports whether the bot is an admin in the chat. Uses the
// cached bot user ID (no GetMe call).
func (b *Bot) botIsAdminIn(ctx context.Context, chatID int64) bool {
	if b.Backend == nil || b.botUserID == 0 {
		return false
	}
	status, err := b.Backend.GetChatMemberStatus(ctx, chatID, b.botUserID)
	if err != nil {
		b.Log.Debug("botIsAdminIn: GetChatMemberStatus failed", "chat_id", chatID, "error", err)
		return false
	}
	return status == "administrator" || status == "creator"
}

// userStartedPrompt is shown when a /link user hasn't started the bot in
// private chat yet. Uses the cached bot username.
func (b *Bot) userStartedPrompt(ctx context.Context, m tgutil.IncomingMsg) {
	var markup any
	if b.botUsername != "" {
		markup = tgutil.NewKeyboard().AddRow(tgutil.InlineURL(theme.Start+" Start", "https://t.me/"+b.botUsername+"?start=link")).Build()
	}
	_ = b.Backend.SendHTML(ctx, m.ChatID, msgUsageLinkPrivate, markup, 0)
}

// senderName resolves a trimmed "First Last" display name for vault-log
// provenance. Best-effort: returns "" on lookup failure (PostVaultLog then
// falls back to "User <id>", like an empty sender name did before).
func (b *Bot) senderName(ctx context.Context, userID int64) string {
	if userID == 0 {
		return ""
	}
	u, err := b.Backend.GetUser(ctx, userID)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(u.FirstName + " " + u.LastName)
}

func (b *Bot) editStatusSafe(ctx context.Context, status Sent, text string) error {
	if status.ID == 0 {
		return nil
	}
	return b.Backend.EditHTML(ctx, status.ChatID, status.ID, text, nil)
}

func (b *Bot) editStatusWithButtons(ctx context.Context, status Sent, text, streamURL, downloadURL string) {
	if status.ID == 0 {
		return
	}
	kb := tgutil.NewKeyboard().
		AddRow(
			tgutil.InlineURL(theme.Stream+" Stream", streamURL),
			tgutil.InlineURL(theme.Download+" Download", downloadURL),
		).
		Build()
	err := b.Backend.EditHTML(ctx, status.ChatID, status.ID, text, kb)
	if err != nil {
		b.Log.Debug("editStatusWithButtons failed", "error", err)
		// Fallback: edit without buttons.
		_ = b.editStatusSafe(ctx, status, text)
	}
}
