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

	"github.com/amarnathcjd/gogram/telegram"

	tghttp "github.com/fyaz05/ThunderGo/internal/http"
	"github.com/fyaz05/ThunderGo/internal/store"
)

// handleBan /ban <user_id> [reason]. Bans a user or, if the ID is negative,
// a channel. The owner cannot ban themselves.
func (b *Bot) handleBan(c *Context) error {
	parts := strings.Fields(c.Args)
	if len(parts) == 0 {
		_, _ = c.ReplyFormatted("Usage: <code>/ban &lt;user_id&gt; [reason]</code>")
		return nil
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		_, _ = c.ReplyFormatted("❌ Invalid ID. Must be an integer.")
		return nil
	}
	if id == c.Msg.SenderID() {
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
		if err := b.Store.BanChannel(dbCtx, store.BannedChannel{
			ChannelID: id,
			BannedBy:  c.Msg.SenderID(),
			BannedAt:  now,
			Reason:    reason,
		}); err != nil {
			_, _ = c.ReplyFormatted("❌ " + html.EscapeString(err.Error())) // best-effort reply
			return nil
		}
		// Best-effort: leave the channel.
		if primary := b.Pool.Primary(); primary != nil {
			_ = primary.LeaveChannel(id)
		} else {
			b.Log.Warn("ban: no primary client to leave channel")
		}
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgBannedChannel, id))
		return nil
	}

	if err := b.Store.BanUser(dbCtx, store.BannedUser{
		UserID:   id,
		BannedBy: c.Msg.SenderID(),
		BannedAt: now,
		Reason:   reason,
	}); err != nil {
		_, _ = c.ReplyFormatted("❌ " + html.EscapeString(err.Error())) // best-effort reply
		return nil
	}
	// Notify the banned user (best-effort).
	notice := msgBannedNotice
	if reason != "" {
		notice = fmt.Sprintf(msgBannedNoticeReason, html.EscapeString(reason))
	}
	if primary := b.Pool.Primary(); primary != nil {
		_, _ = primary.SendMessage(id, notice, &telegram.SendOptions{ParseMode: "HTML"})
	} else {
		b.Log.Warn("ban: no primary client to notify user")
	}
	_, _ = c.ReplyFormatted(fmt.Sprintf(msgBannedUser, id))
	return nil
}

// handleUnban /unban <user_id>.
func (b *Bot) handleUnban(c *Context) error {
	parts := strings.Fields(c.Args)
	if len(parts) == 0 {
		_, _ = c.ReplyFormatted("Usage: <code>/unban &lt;user_id&gt;</code>")
		return nil
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		_, _ = c.ReplyFormatted("❌ Invalid ID.")
		return nil
	}
	dbCtx, dbCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer dbCancel()
	if id < 0 {
		if err := b.Store.UnbanChannel(dbCtx, id); err != nil {
			_, _ = c.ReplyFormatted("❌ " + html.EscapeString(err.Error())) // best-effort reply
			return nil
		}
		_, _ = c.ReplyFormatted(fmt.Sprintf(msgUnbannedChannel, id))
		return nil
	}
	if err := b.Store.UnbanUser(dbCtx, id); err != nil {
		_, _ = c.ReplyFormatted("❌ " + html.EscapeString(err.Error())) // best-effort reply
		return nil
	}
	if primary := b.Pool.Primary(); primary != nil {
		_, _ = primary.SendMessage(id, msgUnbannedNotice, &telegram.SendOptions{ParseMode: "HTML"})
	} else {
		b.Log.Warn("unban: no primary client to notify user")
	}
	_, _ = c.ReplyFormatted(fmt.Sprintf(msgUnbannedUser, id))
	return nil
}

// handleBroadcast /broadcast (reply to a message). Copies the replied-to
// message to every user who has started the bot, posting a status message
// with a Cancel button at the start and a summary at the end.
//
// Modes: "all" (default), "authorized", "regular" (all EXCEPT authorized).
// 4 workers send concurrently (200ms apart). Transient errors retry 3× with
// exponential backoff; permanent errors (USER_IS_BLOCKED, PEER_ID_INVALID,
// USER_DEACTIVATED, CHAT_WRITE_FORBIDDEN, FLOOD_WAIT) are not retried.
// Unreachable users are pruned from the DB in the background.
func (b *Bot) handleBroadcast(c *Context) error {
	if !c.Msg.IsReply() {
		_, _ = c.ReplyFormatted(msgBroadcastUsage)
		return nil
	}
	reply, err := c.Msg.GetReplyMessage()
	if err != nil || reply == nil {
		_, _ = c.ReplyFormatted(msgErrFetchMsg)
		return nil
	}

	// Parse broadcast mode.
	mode := "all"
	if c.Args != "" {
		mode = strings.TrimSpace(strings.ToLower(c.Args))
		if mode != "all" && mode != "authorized" && mode != "regular" {
			_, _ = c.ReplyFormatted(msgBroadcastUsage)
			return nil
		}
	}

	// Authorized-mode filtering: use IsAuthorized per user during streaming
	// (cached by store) instead of materializing the entire auth list in memory.

	// Post the initial status with a Cancel button.
	kb := telegram.NewKeyboard().
		AddRow(telegram.Button.Data(theme.Cancel+" Cancel", "broadcast_cancel")).
		Build()
	status, _ := c.Msg.Reply(msgBroadcastStart, &telegram.SendOptions{
		ParseMode:   "HTML",
		ReplyMarkup: kb,
	})
	if status == nil {
		_, _ = c.ReplyFormatted(msgErrPostStatus)
		return nil
	}

	broadcastCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.registerBroadcastCancel(status.ID, cancel)
	defer b.unregisterBroadcastCancel(status.ID)

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

	// totalUsers counts every streamed user; atomic (shared with producer).
	var totalUsers atomic.Int64

	// Producer: stream users from the DB via a cursor (bounded memory).
	// listCtx has no timeout (large broadcasts can take minutes); the
	// producer short-circuits on Cancel-button context.
	listCtx, listCancel := context.WithCancel(context.Background())
	defer listCancel()
	go func() {
		defer close(userCh)
		defer func() {
			if r := recover(); r != nil {
				b.Log.Error("panic in broadcast producer", "recover", r)
			}
		}()
		streamErr := b.Store.StreamUsers(listCtx, func(u store.User) error {
			if mode == "authorized" || mode == "regular" {
				auth, authErr := b.Store.IsAuthorized(listCtx, u.UserID)
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

	// Workers: pull from userCh, send, report the result.
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
			primary := b.Pool.Primary()
			if primary == nil {
				b.Log.Warn("broadcast worker: no primary client")
				return
			}
			for u := range userCh {
				if broadcastCtx.Err() != nil {
					return
				}
				// 3-retry on transient errors with exponential backoff.
				// Permanent errors and FLOOD_WAIT are NOT retried.
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
					_, sendErr = primary.Forward(u.UserID, reply.ChatID(), []int32{reply.ID}, &telegram.ForwardOptions{HideAuthor: true})
					if sendErr == nil {
						break // success
					}
					if telegram.MatchError(sendErr, "USER_IS_BLOCKED") ||
						telegram.MatchError(sendErr, "PEER_ID_INVALID") ||
						telegram.MatchError(sendErr, "USER_DEACTIVATED") ||
						telegram.MatchError(sendErr, "CHAT_WRITE_FORBIDDEN") {
						unreachable = true
						break
					}
					// FLOOD_WAIT is handled by the pool's FloodHandler; if it
					// reaches us, the handler gave up (wait > 600s) — don't retry.
					if telegram.MatchError(sendErr, "FLOOD_WAIT") {
						break
					}
					// Other errors (network, timeout) — retry.
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

	// Collector: wait for workers, then close resCh.
	go func() {
		wg.Wait()
		close(resCh)
	}()

	// Track unreachable users for pruning.
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
				if err := b.Store.DeleteUser(pruneCtx, uid); err != nil {
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
	summary := fmt.Sprintf(msgBroadcastComplete, modeLabel, succeeded, failed, unreachable, total)
	if cancelled {
		summary = fmt.Sprintf(msgBroadcastCancelled, modeLabel, succeeded, failed, unreachable, total)
	}
	// Clear the Cancel button.
	if primary := b.Pool.Primary(); primary != nil {
		_, _ = primary.EditMessage(status.ChatID(), status.ID, summary, &telegram.SendOptions{
			ParseMode:   "HTML",
			ReplyMarkup: telegram.NewKeyboard().Build(),
		})
	} else {
		b.Log.Warn("broadcast summary: no primary client")
	}
	return nil
}

// handleAuthorize /authorize <user_id>. Abort if GetUser fails to avoid
// silently authorizing a non-existent user.
func (b *Bot) handleAuthorize(c *Context) error {
	parts := strings.Fields(c.Args)
	if len(parts) == 0 {
		_, _ = c.ReplyFormatted("Usage: <code>/authorize &lt;user_id&gt;</code>")
		return nil
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		_, _ = c.ReplyFormatted("❌ Invalid ID.")
		return nil
	}
	// Verify the user exists before authorizing.
	primary := b.Pool.Primary()
	if primary == nil {
		_, _ = c.ReplyFormatted("❌ Bot client is not ready. Please try again later.")
		return nil
	}
	u, err := primary.GetUser(id)
	if err != nil || u == nil {
		_, _ = c.ReplyFormatted(msgAuthUserNotFound)
		return nil
	}
	firstName := u.FirstName
	authCtx, authCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer authCancel()
	if err := b.Store.Authorize(authCtx, store.AuthorizedUser{
		UserID:    id,
		FirstName: firstName,
		AddedBy:   c.Msg.SenderID(),
		AddedAt:   time.Now(),
	}); err != nil {
		_, _ = c.ReplyFormatted("❌ " + html.EscapeString(err.Error()))
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
		_, _ = c.ReplyFormatted("Usage: <code>/deauthorize &lt;user_id&gt;</code>")
		return nil
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		_, _ = c.ReplyFormatted("❌ Invalid ID.")
		return nil
	}
	dctx, dcancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer dcancel()
	if err := b.Store.Deauthorize(dctx, id); err != nil {
		_, _ = c.ReplyFormatted("❌ " + html.EscapeString(err.Error()))
		return nil
	}
	// Best-effort: revoke any active token-activation so a deauthorized
	// user can't keep using the bot until their activation expires.
	if b.Cfg.TokenEnabled {
		if err := b.Store.InvalidateActivatedUser(dctx, id); err != nil {
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
	auths, err := b.Store.ListAuthorized(laCtx)
	if err != nil {
		_, _ = c.ReplyFormatted("❌ " + html.EscapeString(err.Error())) // best-effort reply
		return nil
	}

	// Build body lines once for consistent pagination.
	lines := make([]string, 0, len(auths)+1)
	for _, a := range auths {
		name := a.FirstName
		if name == "" {
			name = "(unknown)"
		}
		lines = append(lines, fmt.Sprintf("• <code>%d</code> — %s\n", a.UserID, html.EscapeString(name)))
	}
	// Always include the owner implicitly.
	lines = append(lines, fmt.Sprintf(msgAuthOwnerImplicit, b.Cfg.OwnerUserID))

	// Paginate at 3500 chars (headroom under Telegram's 4096-char cap);
	// 1s delay between pages avoids flood-waits.
	const pageMax = 3500
	var pages []string
	var cur strings.Builder
	for _, line := range lines {
		if cur.Len() > 0 && cur.Len()+len(line) > pageMax {
			pages = append(pages, cur.String())
			cur.Reset()
		}
		cur.WriteString(line)
	}
	if cur.Len() > 0 || len(pages) == 0 {
		pages = append(pages, cur.String())
	}
	total := len(pages)
	for i, body := range pages {
		header := fmt.Sprintf(msgAuthListHeader, len(auths), i+1, total) + "\n\n"
		_, _ = c.ReplyFormatted(header + body)
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
	all := b.Pool.All()
	perClient := b.Pool.PerClientInflight()
	var workload strings.Builder
	for i, n := range perClient {
		dc := 0
		if i < len(all) && all[i] != nil {
			dc = all[i].GetDC()
		}
		workload.WriteString(fmt.Sprintf("• bot_%02d — inflight %d, DC %d\n", i, n, dc))
	}
	_, _ = c.ReplyFormatted(fmt.Sprintf(msgBotStatus,
		tghttp.Version,
		html.EscapeString(botUsername),
		uptime,
		b.Pool.Len(),
		b.Pool.TotalInflight(),
		workload.String()))
	return nil
}

// handleUsers /users.
func (b *Bot) handleUsers(c *Context) error {
	ucCtx, ucCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer ucCancel()
	count, err := b.Store.CountUsers(ucCtx)
	if err != nil {
		_, _ = c.ReplyFormatted("❌ " + html.EscapeString(err.Error()))
		return nil
	}
	_, _ = c.ReplyFormatted(fmt.Sprintf(msgUserCount, count))
	return nil
}

// handleLog /log. Sends the current log file as a document. If the log exceeds
// 45 MiB (Telegram's bot upload limit is ~50 MiB), it's tailed to the last 45 MiB.
//
// The file is always copied through a temp file with sensitive patterns (bot
// tokens, MongoDB credentials) redacted before sending.
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

	// Always copy bytes (potentially tailed) into a redacted temp file.
	//gosec:disable G304 // path is TG_LOG_FILE env (operator-supplied), not user input; owner-only command
	f, err := os.Open(path)
	//gosec:enable G304
	if err != nil {
		b.Log.Warn("opening log file failed", "error", err)
		_, _ = c.ReplyFormatted(msgLogReadErr)
		return nil
	}
	defer f.Close()

	// Telegram's bot API limits file uploads to 50 MiB. Tail to 45 MiB
	// (leaving headroom) before reading.
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
	// Don't ignore sync errors — a failed sync could mean the redacted
	// bytes never reached disk before SendMedia reads them.
	if err := tmp.Sync(); err != nil {
		b.Log.Warn("sync log temp file failed", "error", err)
	}

	primary := b.Pool.Primary()
	if primary == nil {
		b.Log.Warn("log: no primary client to send file")
		_, _ = c.ReplyFormatted(msgLogSendErr)
		return nil
	}
	_, err = primary.SendMedia(c.Msg.ChatID(), tmp.Name(), &telegram.MediaOptions{
		Caption:       caption,
		ParseMode:     "HTML",
		ForceDocument: true,
	})
	if err != nil {
		b.Log.Warn("sending log file failed", "error", err)
		_, _ = c.ReplyFormatted(msgLogSendErr)
	}
	return nil
}

// redactBotTokenRe matches Telegram bot tokens: 8-10 digit bot ID, colon,
// 35-character alphanumeric/underscore/hyphen secret.
var redactBotTokenRe = regexp.MustCompile(`\d{8,10}:[A-Za-z0-9_-]{35}`)

// redactMongoURIRe matches MongoDB connection strings with embedded credentials.
// Capture group 1 preserves the optional "+srv" suffix for the replacement.
var redactMongoURIRe = regexp.MustCompile(`mongodb(\+srv)?://[^:]+:[^@]+@`)

// redactLogSecrets masks bot tokens and MongoDB credentials in in.
func redactLogSecrets(in []byte) []byte {
	out := redactBotTokenRe.ReplaceAll(in, []byte("[REDACTED_TOKEN]"))
	out = redactMongoURIRe.ReplaceAll(out, []byte("mongodb$1://[REDACTED]:[REDACTED]@"))
	return out
}

// handleRestart /restart. Posts "Restarting…", saves a restart marker, then in
// a goroutine runs `git pull --ff-only`, rebuilds the running binary in place via
// `go build`, and signals self with SIGTERM so the supervisor restarts with the
// freshly-built binary.
//
// If git pull or go build fails, the status message is edited with the error
// and the process is NOT signalled. On next startup, EditRestartMarkerIfPending
// picks up the marker and edits the original "Restarting…" message to "Restart Successful".
func (b *Bot) handleRestart(c *Context) error {
	status, _ := c.Reply(msgRestarting)
	if status == nil {
		_, _ = c.ReplyFormatted(msgErrPostStatus)
		return nil
	}

	marker := store.RestartMarker{
		ID:        fmt.Sprintf("restart-%d", time.Now().UnixNano()),
		ChatID:    status.ChatID(),
		MessageID: status.ID,
		CreatedAt: time.Now(),
	}
	rmCtx, rmCancel := context.WithTimeout(b.baseCtx, 30*time.Second)
	defer rmCancel()
	if err := b.Store.SaveRestartMarker(rmCtx, marker); err != nil {
		b.Log.Warn("saving restart marker", "error", err)
	}

	go func() {
		repo := b.Cfg.UpstreamRepo
		if repo == "" {
			repo = "https://github.com/fyaz05/ThunderGo.git"
		}
		branch := b.Cfg.UpstreamBranch

		// 1. git pull — fast-forward to the tracked branch.
		gitCtx, gitCancel := context.WithTimeout(b.baseCtx, 2*time.Minute)
		defer gitCancel()
		cwd, err := os.Getwd()
		if err != nil {
			b.editRestartError(status, "could not determine CWD: "+err.Error())
			return
		}

		var gitRemote, gitBranch string
		if repo != "" {
			// If UPSTREAM_REPO is not the current origin, replace it.
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

		// 2. go mod verify — check module integrity before building.
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

		// 3. go build — rebuild the running binary in place. On Linux the
		// kernel keeps the old inode open for the running process, so
		// overwriting is safe; the new inode is used on the next exec.
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

		// 4. Signal self for graceful shutdown. The supervisor restarts the
		// process with the freshly-built binary; on next startup the restart
		// marker is edited to "Restart Successful".
		//
		// On platforms where SIGTERM isn't supported (notably Windows), fall
		// back to os.Interrupt (SIGINT) which triggers the same signal handler.
		// If both signal deliveries fail, call b.Stop() then os.Exit(1) so the
		// supervisor restarts the process.
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
func (b *Bot) editRestartError(status *telegram.NewMessage, msg string) {
	if status == nil {
		return
	}
	b.Log.Error("restart aborted", "error", msg)
	if primary := b.Pool.Primary(); primary != nil {
		_, _ = primary.EditMessage(status.ChatID(), status.ID,
			fmt.Sprintf(msgRestartFailed, html.EscapeString(msg)),
			&telegram.SendOptions{ParseMode: "HTML"})
	} else {
		b.Log.Warn("editRestartError: no primary client")
	}
}
