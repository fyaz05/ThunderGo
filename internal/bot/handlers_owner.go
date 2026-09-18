package bot

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	tghttp "github.com/fyaz05/ThunderGo/internal/http"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// handleBan /ban <user_id> [reason]. Bans a user or, if the ID is negative,
// a channel. The owner cannot ban themselves.
func (b *Bot) handleBan(c *Context) error {
	parts := strings.Fields(c.Args)
	if len(parts) == 0 {
		_, _ = c.ReplyFormatted(msgUsageBan)
		return nil
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		_, _ = c.ReplyFormatted(msgErrInvalidIDInt)
		return nil
	}
	if id == c.Msg.SenderID {
		_, _ = c.ReplyFormatted(msgBannedSelf)
		return nil
	}
	reason := ""
	if len(parts) > 1 {
		reason = strings.Join(parts[1:], " ")
	}

	dbCtx, dbCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer dbCancel()
	now := time.Now()
	if id < 0 {
		// Channel ban. Validate the ID looks like a real Telegram channel/supergroup
		// ID (carry a -100 prefix, i.e. <= -1_000_000_000_000).
		if id >= -1000000000000 {
			_, _ = c.ReplyFormatted(msgBannedInvalidChannel)
			return nil
		}
		if err := b.st().BanChannel(dbCtx, store.BannedChannel{
			ChannelID: id,
			BannedBy:  c.Msg.SenderID,
			BannedAt:  now,
			Reason:    reason,
		}); err != nil {
			_, _ = c.ReplyFormatted(fmt.Sprintf(msgErrDBOperation, html.EscapeString(err.Error())))
			return nil
		}
		// Best-effort: leave the channel.
		_ = b.Backend.LeaveChat(b.baseCtx, id)
		reasonSuffix := ""
		if reason != "" {
			reasonSuffix = fmt.Sprintf(msgBannedReasonSuffix, html.EscapeString(reason))
		}
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgBannedChannel, id, reasonSuffix))
		return nil
	}

	if err := b.st().BanUser(dbCtx, store.BannedUser{
		UserID:   id,
		BannedBy: c.Msg.SenderID,
		BannedAt: now,
		Reason:   reason,
	}); err != nil {
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgErrDBOperation, html.EscapeString(err.Error())))
		return nil
	}
	// Notify the banned user (best-effort).
	notice := msgBannedNotice
	if reason != "" {
		notice = fmt.Sprintf(msgBannedNoticeReason, html.EscapeString(reason))
	}
	if err := b.Backend.SendHTML(b.baseCtx, id, notice, nil, 0); err != nil {
		b.Log.Debug("ban: could not notify banned user", "user_id", id, "error", err)
	}
	reasonSuffix := ""
	if reason != "" {
		reasonSuffix = fmt.Sprintf(msgBannedReasonSuffix, html.EscapeString(reason))
	}
	_, _ = c.ReplyFormatted(fmt.Sprintf(msgBannedUser, id, reasonSuffix))
	return nil
}

// handleUnban /unban <user_id>.
func (b *Bot) handleUnban(c *Context) error {
	parts := strings.Fields(c.Args)
	if len(parts) == 0 {
		_, _ = c.ReplyFormatted(msgUsageUnban)
		return nil
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		_, _ = c.ReplyFormatted(msgErrInvalidID)
		return nil
	}
	dbCtx, dbCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer dbCancel()
	if id < 0 {
		if err := b.st().UnbanChannel(dbCtx, id); err != nil {
			_, _ = c.ReplyFormatted(fmt.Sprintf(msgErrDBOperation, html.EscapeString(err.Error())))
			return nil
		}
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgUnbannedChannel, id))
		return nil
	}
	if err := b.st().UnbanUser(dbCtx, id); err != nil {
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgErrDBOperation, html.EscapeString(err.Error())))
		return nil
	}
	if err := b.Backend.SendHTML(b.baseCtx, id, msgUnbannedNotice, nil, 0); err != nil {
		b.Log.Debug("unban: could not notify user", "user_id", id, "error", err)
	}
	_, _ = c.ReplyFormatted(fmt.Sprintf(msgUnbannedUser, id))
	return nil
}

// handleBroadcast /broadcast (reply to a message). Modes: all/authorized/regular;
// retries transient errors 3×, prunes unreachable users in the background.
func (b *Bot) handleBroadcast(c *Context) error {
	if c.Msg.ReplyToMsgID == 0 {
		_, _ = c.ReplyFormatted(msgBroadcastUsage)
		return nil
	}
	reply, err := b.Backend.GetReplyMessage(c.Ctx, c.Msg.ChatID, c.Msg.MsgID)
	if err != nil {
		_, _ = c.ReplyFormatted(msgErrFetchMsg)
		return nil
	}

	mode := "all"
	if c.Args != "" {
		mode = strings.TrimSpace(strings.ToLower(c.Args))
		if mode != "all" && mode != "authorized" && mode != "regular" {
			_, _ = c.ReplyFormatted(msgBroadcastUsage)
			return nil
		}
	}

	// Authorized-mode: check IsAuthorized per-user during streaming, not in bulk.

	kb := tgutil.NewKeyboard().
		AddRow(tgutil.InlineCallback(theme.Cancel+" Cancel", "broadcast_cancel")).
		Build()
	status, err := c.ReplyMarkup(msgBroadcastStart, kb)
	if err != nil || status.ID == 0 {
		_, _ = c.ReplyFormatted(msgErrPostStatus)
		return nil
	}

	broadcastCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.registerBroadcastCancel(status.ID, cancel)
	defer b.unregisterBroadcastCancel(status.ID)

	broadcastStart := time.Now()

	// 4 workers; each sleeps 200ms between sends to avoid flood-waits.
	const numWorkers = 4
	const sendDelay = 200 * time.Millisecond

	type result struct {
		ok          bool
		unreachable bool  // DM blocked, account deleted, etc.
		userID      int64 // set when unreachable=true, for pruning
	}
	userCh := make(chan store.User, numWorkers*2)
	resCh := make(chan result, numWorkers*2)

	// totalUsers is atomic; shared with producer.
	var totalUsers atomic.Int64

	// Stream users via cursor (bounded memory). listCtx has no timeout (large
	// broadcasts can take minutes); short-circuits on Cancel-button context.
	listCtx, listCancel := context.WithCancel(context.Background())
	defer listCancel()
	go func() {
		defer close(userCh)
		defer func() {
			if r := recover(); r != nil {
				b.Log.Error("panic in broadcast producer", "recover", r)
			}
		}()
		streamErr := b.st().StreamUsers(listCtx, func(u store.User) error {
			if mode == "authorized" || mode == "regular" {
				auth, authErr := b.st().IsAuthorized(listCtx, u.UserID)
				if authErr != nil {
					b.Log.Debug("broadcast: IsAuthorized check failed", "user_id", u.UserID, "error", authErr)
				}
				if mode == "authorized" && !auth {
					return nil
				}
				if mode == "regular" && auth {
					return nil
				}
			}
			totalUsers.Add(1)
			if broadcastCtx.Err() != nil {
				return broadcastCtx.Err()
			}
			select {
			case userCh <- u:
				return nil
			case <-broadcastCtx.Done():
				return broadcastCtx.Err()
			}
		})
		if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
			b.Log.Warn("StreamUsers error during broadcast", "error", streamErr)
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					b.Log.Error("panic in broadcast worker", "recover", r, "stack", string(debugStack()))
				}
			}()
			if b.Backend == nil {
				b.Log.Warn("broadcast worker: no backend")
				return
			}
			for u := range userCh {
				if broadcastCtx.Err() != nil {
					return
				}
				// Retry transient errors 3× with exponential backoff.
				// Permanent per-user failures (blocked/deactivated/
				// unreachable) and FLOOD_WAIT are not retried.
				var sendErr error
				var unreachable bool
				for attempt := 0; attempt < 3; attempt++ {
					if attempt > 0 {
						select {
						case <-time.After(time.Duration(attempt*attempt) * time.Second):
						case <-broadcastCtx.Done():
							return
						}
					}
					_, sendErr = b.Backend.ForwardMessages(broadcastCtx, u.UserID, reply.ChatID, []int{reply.MsgID}, true)
					if sendErr == nil {
						break // success
					}
					if broadcastUnreachable(sendErr) {
						unreachable = true
						break
					}
					// FLOOD_WAIT: never retried — the wait is owed to
					// Telegram, and sleeping here would stall the worker.
					if _, isFlood := tgutil.MapFloodWait(sendErr); isFlood {
						break
					}
				}
				if sendErr == nil {
					resCh <- result{ok: true}
				} else if unreachable {
					resCh <- result{ok: false, unreachable: true, userID: u.UserID}
				} else {
					resCh <- result{ok: false, unreachable: false}
				}
				select {
				case <-time.After(sendDelay):
				case <-broadcastCtx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	succeeded, failed, unreachable := 0, 0, 0
	var unreachableUsers []int64
	for r := range resCh {
		if r.ok {
			succeeded++
		} else if r.unreachable {
			unreachable++
			unreachableUsers = append(unreachableUsers, r.userID)
		} else {
			failed++
		}
	}

	// Prune unreachable users in the background (best-effort).
	if len(unreachableUsers) > 0 {
		b.handlerWg.Add(1)
		go func(uids []int64) {
			defer b.handlerWg.Done()
			pruneCtx, pruneCancel := context.WithTimeout(b.baseCtx, 60*time.Second)
			defer pruneCancel()
			for _, uid := range uids {
				if err := b.st().DeleteUser(pruneCtx, uid); err != nil {
					b.Log.Debug("could not prune unreachable user", "user_id", uid, "error", err)
				}
			}
			b.Log.Info("pruned unreachable users", "count", len(uids))
		}(unreachableUsers)
	}

	modeLabel := msgBroadcastModeAll
	if mode == "authorized" {
		modeLabel = msgBroadcastModeAuthorized
	} else if mode == "regular" {
		modeLabel = msgBroadcastModeRegular
	}

	total := int(totalUsers.Load())
	cancelled := succeeded+failed+unreachable < total
	elapsed := formatReadableDuration(time.Since(broadcastStart))
	var summary string
	if cancelled {
		summary = fmt.Sprintf(msgBroadcastCancelled, elapsed, modeLabel, total, succeeded, failed, unreachable)
	} else {
		summary = fmt.Sprintf(msgBroadcastComplete, elapsed, modeLabel, total, succeeded, failed, unreachable)
	}
	// nil markup clears the Cancel button; an empty keyboard triggers
	// REPLY_MARKUP_INVALID.
	if err := b.Backend.EditHTML(b.baseCtx, status.ChatID, status.ID, summary, nil); err != nil {
		b.Log.Warn("broadcast: could not edit status to summary; sending as new message", "error", err)
		_, _ = c.Respond(summary)
	}
	return nil
}

// handleAuthorize /authorize <user_id>. Abort if GetUser fails to avoid
// silently authorizing a non-existent user.
func (b *Bot) handleAuthorize(c *Context) error {
	parts := strings.Fields(c.Args)
	if len(parts) == 0 {
		_, _ = c.ReplyFormatted(msgUsageAuthorize)
		return nil
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		_, _ = c.ReplyFormatted(msgErrInvalidID)
		return nil
	}
	u, err := b.Backend.GetUser(c.Ctx, id)
	if err != nil || u.ID == 0 {
		_, _ = c.ReplyFormatted(msgAuthUserNotFound)
		return nil
	}
	firstName := u.FirstName
	authCtx, authCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer authCancel()
	if err := b.st().Authorize(authCtx, store.AuthorizedUser{
		UserID:    id,
		FirstName: firstName,
		AddedBy:   c.Msg.SenderID,
		AddedAt:   time.Now(),
	}); err != nil {
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgErrDBOperation, html.EscapeString(err.Error())))
		return nil
	}
	if b.Limiter != nil {
		b.Limiter.Reset(id)
	}
	_, _ = c.ReplyFormatted(fmt.Sprintf(msgAuthorized, id, html.EscapeString(firstName)))
	return nil
}

// handleDeauthorize /deauthorize <user_id>.
func (b *Bot) handleDeauthorize(c *Context) error {
	parts := strings.Fields(c.Args)
	if len(parts) == 0 {
		_, _ = c.ReplyFormatted(msgUsageDeauthorize)
		return nil
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		_, _ = c.ReplyFormatted(msgErrInvalidID)
		return nil
	}
	dctx, dcancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer dcancel()
	if err := b.st().Deauthorize(dctx, id); err != nil {
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgErrDBOperation, html.EscapeString(err.Error())))
		return nil
	}
	// Best-effort: revoke active token-activation so deauthorized users can't keep using the bot.
	if b.Cfg.TokenEnabled {
		if err := b.st().InvalidateActivatedUser(dctx, id); err != nil {
			b.Log.Debug("could not invalidate activated user", "user_id", id, "error", err)
		}
	}
	_, _ = c.ReplyFormatted(fmt.Sprintf(msgDeauthorized, id))
	return nil
}

// handleListAuth /listauth.
func (b *Bot) handleListAuth(c *Context) error {
	laCtx, laCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer laCancel()
	auths, err := b.st().ListAuthorized(laCtx)
	if err != nil {
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgErrDBOperation, html.EscapeString(err.Error())))
		return nil
	}

	var body strings.Builder
	for i, a := range auths {
		name := a.FirstName
		if name == "" {
			name = msgFallbackUnknownName
		}
		authBy := strconv.FormatInt(a.AddedBy, 10)
		authTime := a.AddedAt.Format("2006-01-02 15:04")
		body.WriteString(fmt.Sprintf("<blockquote>%d. 👤 <b>Name:</b> %s\n🆔 <b>User ID:</b> <code>%d</code>\n🔑 <b>Authorized by:</b> <code>%s</code>\n📅 <b>Date:</b> <code>%s</code></blockquote>\n\n",
			i+1, html.EscapeString(name), a.UserID, html.EscapeString(authBy), html.EscapeString(authTime)))
	}
	body.WriteString(fmt.Sprintf(msgAuthOwnerImplicit, b.Cfg.OwnerUserID))

	// Paginate at 3500 chars (headroom under Telegram's 4096-char cap);
	// 1s delay between pages avoids flood-waits.
	const pageMax = 3500
	var pages []string
	var cur strings.Builder
	bodyStr := body.String()
	entries := strings.Split(bodyStr, "\n\n")
	for _, entry := range entries {
		if cur.Len() > 0 && cur.Len()+len(entry)+2 > pageMax {
			pages = append(pages, cur.String())
			cur.Reset()
		}
		if cur.Len() > 0 {
			cur.WriteString("\n\n")
		}
		cur.WriteString(entry)
	}
	if cur.Len() > 0 || len(pages) == 0 {
		pages = append(pages, cur.String())
	}
	total := len(pages)
	for i, pageBody := range pages {
		header := msgAuthListHeader
		if total > 1 {
			header += fmt.Sprintf("\n\n<blockquote>📄 <b>Page %d of %d</b> — %d user(s)</blockquote>", i+1, total, len(auths))
		}
		_, _ = c.RespondMarkup(header+"\n\n"+pageBody, buildCloseMarkup())
		if i < total-1 {
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}

// handleStatus /status (owner bot command, distinct from the HTTP /status).
// Shows uptime, version, active client count, total workload, and per-client breakdown.
func (b *Bot) handleStatus(c *Context) error {
	uptime := formatReadableDuration(time.Since(b.startTime))
	botUsername := b.botUsername
	perClient := b.Pool.PerClientInflight()
	perDC := b.Pool.PerClientDC()
	var workload strings.Builder
	for i, n := range perClient {
		dc := 0
		if i < len(perDC) {
			dc = perDC[i]
		}
		workload.WriteString(fmt.Sprintf("🤖 <code>bot_%02d</code> — ⚡ inflight <b>%d</b>, 🌍 DC <code>%d</code>\n", i, n, dc))
	}
	text := fmt.Sprintf(msgBotStatus,
		uptime,
		html.EscapeString(botUsername),
		tghttp.Version,
		b.Pool.Len(),
		b.Pool.TotalInflight(),
		workload.String())
	_, _ = c.RespondMarkup(text, buildCloseMarkup())
	return nil
}

// handleUsers /users.
func (b *Bot) handleUsers(c *Context) error {
	ucCtx, ucCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer ucCancel()
	count, err := b.st().CountUsers(ucCtx)
	if err != nil {
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgErrDBOperation, html.EscapeString(err.Error())))
		return nil
	}
	_, _ = c.RespondMarkup(fmt.Sprintf(msgUserCount, count), buildCloseMarkup())
	return nil
}

// handleLog /log. Sends the log tailed to 45 MiB (Telegram's 50 MiB cap),
// with bot tokens and MongoDB credentials redacted via a temp file.
func (b *Bot) handleLog(c *Context) error {
	path := os.Getenv("TG_LOG_FILE")
	if path == "" {
		path = "thundergo.log"
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() == 0 {
		_, _ = c.ReplyFormatted(msgLogEmpty)
		return nil
	}

	caption := fmt.Sprintf(msgLogCaption, info.Size())

	//gosec:disable G304 // path is TG_LOG_FILE env (operator-supplied), not user input; owner-only command
	f, err := os.Open(path)
	//gosec:enable G304
	if err != nil {
		b.Log.Warn("opening log file failed", "error", err)
		_, _ = c.ReplyFormatted(msgLogReadErr)
		return nil
	}
	defer f.Close()

	// Telegram upload cap is 50 MiB; tail to 45 MiB for headroom.
	const maxSendSize = 45 << 20 // 45 MiB
	if info.Size() > maxSendSize {
		if _, err := f.Seek(-maxSendSize, io.SeekEnd); err != nil {
			b.Log.Warn("seeking log file failed", "error", err)
			_, _ = c.ReplyFormatted(msgLogReadErr)
			return nil
		}
		caption = fmt.Sprintf(msgLogCaptionTailed, maxSendSize>>20, info.Size())
	}

	tmp, err := os.CreateTemp("", "thundergo-log-redacted-*.log")
	if err != nil {
		b.Log.Warn("creating redacted log temp file failed", "error", err)
		_, _ = c.ReplyFormatted(msgLogPrepErr)
		return nil
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	// Stream the log through redaction — no double-buffering 45 MiB.
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		redacted := redactLogSecrets(scanner.Bytes())
		if _, err := tmp.Write(redacted); err != nil {
			b.Log.Warn("writing redacted log temp file failed", "error", err)
			_, _ = c.ReplyFormatted(msgLogPrepErr)
			return nil
		}
		if _, err := tmp.WriteString("\n"); err != nil {
			b.Log.Warn("writing newline to redacted log temp file failed", "error", err)
			_, _ = c.ReplyFormatted(msgLogPrepErr)
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		b.Log.Warn("scanning log file for redaction failed", "error", err)
		_, _ = c.ReplyFormatted(msgLogReadErr)
		return nil
	}
	// Sync errors mean redacted bytes may not reach disk before SendMedia reads them.
	if err := tmp.Sync(); err != nil {
		b.Log.Warn("sync log temp file failed", "error", err)
	}

	if err := b.Backend.SendFileDocument(c.Ctx, c.Msg.ChatID, tmp.Name(), caption, true); err != nil {
		b.Log.Warn("sending log file failed", "error", err)
		_, _ = c.ReplyFormatted(msgLogSendErr)
	}
	return nil
}

// redactBotTokenRe matches Telegram bot tokens: 8-10 digit bot ID, colon,
// 35-character alphanumeric/underscore/hyphen secret.
var redactBotTokenRe = regexp.MustCompile(`\d{8,10}:[A-Za-z0-9_-]{35,}`)

// redactMongoURIRe matches MongoDB connection strings with embedded credentials.
// Capture group 1 preserves the optional "+srv" suffix for the replacement.
var redactMongoURIRe = regexp.MustCompile(`mongodb(\+srv)?://[^:]+:[^@]+@`)

// redactLogSecrets masks bot tokens and MongoDB credentials in the input.
func redactLogSecrets(in []byte) []byte {
	out := redactBotTokenRe.ReplaceAll(in, []byte("[REDACTED_TOKEN]"))
	out = redactMongoURIRe.ReplaceAll(out, []byte("mongodb$1://[REDACTED]:[REDACTED]@"))
	return out
}

// handleRestart /restart. Runs git pull --ff-only && go build in a goroutine,
// then signals self (SIGTERM) for supervisor restart. On failure, edits the
// status message with the error; on next startup, marker → "Restart Successful".
func (b *Bot) handleRestart(c *Context) error {
	status, err := c.Reply(msgRestarting)
	if err != nil || status.ID == 0 {
		_, _ = c.ReplyFormatted(msgErrPostStatus)
		return nil
	}

	marker := store.RestartMarker{
		ID:        fmt.Sprintf("restart-%d", time.Now().UnixNano()),
		ChatID:    status.ChatID,
		MessageID: int32(status.ID), //nosec G115 // message IDs fit int32
		CreatedAt: time.Now(),
	}
	rmCtx, rmCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer rmCancel()
	if err := b.st().SaveRestartMarker(rmCtx, marker); err != nil {
		b.Log.Warn("saving restart marker", "error", err)
	}

	go func() {
		repo := b.Cfg.UpstreamRepo
		if repo == "" {
			repo = "https://github.com/fyaz05/ThunderGo.git"
		}
		branch := b.Cfg.UpstreamBranch
		if branch == "" {
			branch = "main"
		}

		gitCtx, gitCancel := context.WithTimeout(b.baseCtx, 2*time.Minute)
		defer gitCancel()
		cwd, err := os.Getwd()
		if err != nil {
			b.editRestartError(status, "could not determine CWD: "+err.Error())
			return
		}

		var gitRemote, gitBranch string
		if repo != "" {
			// Replace origin if UPSTREAM_REPO differs.
			// #nosec G204 — admin-only, cwd is from os.Getwd()
			checkRemote := exec.CommandContext(gitCtx, "git", "-C", cwd, "remote", "get-url", "origin")
			remoteBytes, err := checkRemote.Output()
			if err != nil {
				b.editRestartError(status, "failed to get origin remote URL: "+err.Error())
				return
			}
			if strings.TrimSpace(string(remoteBytes)) != repo {
				// #nosec G204 — admin-only, repo is from UPSTREAM_REPO env var
				if out, err := exec.CommandContext(gitCtx, "git", "-C", cwd, "remote", "set-url", "origin", repo).CombinedOutput(); err != nil {
					b.editRestartError(status, fmt.Sprintf("failed to set origin URL: %s; output: %s", err, string(out)))
					return
				}
			}
			gitRemote = "origin"
			gitBranch = branch
		} else {
			gitRemote = "origin"
			gitBranch = "main"
		}
		gitCmd := exec.CommandContext(gitCtx, "git", "-C", cwd, "pull", "--ff-only", gitRemote, gitBranch) // #nosec G204 — admin-only, cwd/repo/branch are from trusted sources
		gitCmd.Stdout = io.Discard
		var gitStderr bytes.Buffer
		gitCmd.Stderr = &gitStderr
		if err := gitCmd.Run(); err != nil {
			b.editRestartError(status, "git pull failed: "+err.Error()+"; stderr: "+gitStderr.String())
			return
		}
		b.Log.Info("restart: git pull complete", "remote", gitRemote, "branch", gitBranch)

		modCtx, modCancel := context.WithTimeout(b.baseCtx, 2*time.Minute)
		defer modCancel()
		modCmd := exec.CommandContext(modCtx, "go", "mod", "verify")
		modCmd.Dir = cwd
		var modStderr bytes.Buffer
		modCmd.Stderr = &modStderr
		if err := modCmd.Run(); err != nil {
			b.editRestartError(status, "go mod verify failed: "+err.Error()+"; stderr: "+modStderr.String())
			return
		}

		// Rebuild in place: the kernel keeps the old inode open, so overwriting is safe.
		buildCtx, buildCancel := context.WithTimeout(b.baseCtx, 5*time.Minute)
		defer buildCancel()
		binaryPath, err := os.Executable()
		if err != nil {
			b.editRestartError(status, "could not determine binary path: "+err.Error())
			return
		}
		buildCmd := exec.CommandContext(buildCtx, "go", "build", "-o", binaryPath, "./cmd/thundergo") // #nosec G204 — admin-only, binaryPath is from os.Executable()
		buildCmd.Dir = cwd
		buildCmd.Stdout = io.Discard
		var buildStderr bytes.Buffer
		buildCmd.Stderr = &buildStderr
		if err := buildCmd.Run(); err != nil {
			b.editRestartError(status, "go build failed: "+err.Error()+"; stderr: "+buildStderr.String())
			return
		}
		b.Log.Info("restart: go build complete")

		// Signal self for graceful shutdown (SIGTERM; falls back to os.Interrupt on
		// Windows). If both fail, call b.Stop() then os.Exit(1) to trigger restart.
		b.Log.Info("restart requested; signalling self for graceful shutdown")
		p, _ := os.FindProcess(os.Getpid())
		if err := p.Signal(syscall.SIGTERM); err != nil {
			b.Log.Warn("SIGTERM not supported on this platform; trying os.Interrupt",
				"error", err)
			if ierr := p.Signal(os.Interrupt); ierr != nil {
				b.Log.Error("could not signal self for restart; exiting with error",
					"sigterm_err", err, "interrupt_err", ierr)
				b.Stop()
				os.Exit(1)
			}
		}
	}()
	return nil
}

// editRestartError edits the restart status message with an error and aborts
// the restart (does NOT signal self). Used when git pull or go build fails.
func (b *Bot) editRestartError(status Sent, msg string) {
	if status.ID == 0 {
		return
	}
	b.Log.Error("restart aborted", "error", msg)
	if err := b.Backend.EditHTML(b.baseCtx, status.ChatID, status.ID,
		fmt.Sprintf(msgRestartFailed, html.EscapeString(msg)), nil); err != nil {
		b.Log.Warn("editRestartError: could not edit status", "error", err)
	}
}
