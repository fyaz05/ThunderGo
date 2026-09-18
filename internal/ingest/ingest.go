// Package ingest turns incoming Telegram media into a storable vault message
// plus a file record, with deduplication: the same file (by stable ID) never
// creates a second vault message. Concurrent ingesters race for a per-file-key
// mutex; the first writer wins, subsequent writers reuse the existing record.
//
// All Telegram I/O goes through the tgutil.BotBackend seam (no transport
// library is imported here).
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"sync"
	"time"

	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// Result is the outcome of an ingest. Either File is set (success) or Err is
// set (failure). On a successful dedup, Reused=true means the file was already
// in the vault and no new vault message was created.
type Result struct {
	File   *store.FileRecord
	Reused bool
	Err    error
}

// maxDedupEntries bounds the in-memory dedup mutex map to prevent unbounded
// memory growth under high file-key churn.
const (
	maxDedupEntries        = 10000
	canonicalLookupTimeout = 30 * time.Second
)

// Ingester coordinates file ingestion. It is safe for concurrent use.
type Ingester struct {
	backend tgutil.BotBackend
	store   *store.Store
	log     *slog.Logger

	vaultID int64

	// dedupMu provides per-file-key locking. The map is guarded by dedupMuMu;
	// each entry is held during the "check store, then write vault message and
	// insert record" critical section. CleanupDedupMu periodically removes
	// entries whose mutex is not currently held.
	dedupMuMu sync.Mutex
	dedupMu   map[string]*sync.Mutex
}

func New(backend tgutil.BotBackend, s *store.Store, vaultChannelID int64, log *slog.Logger) *Ingester {
	return &Ingester{
		backend: backend,
		store:   s,
		log:     log,
		vaultID: vaultChannelID,
		dedupMu: make(map[string]*sync.Mutex),
	}
}

// VaultChannelID returns the vault channel ID used by the stream handler.
func (in *Ingester) VaultChannelID() int64 { return in.vaultID }

// fetchVaultMessage re-fetches a single vault message by ID through the
// backend's bulk lookup ((msgID-1, msgID] window, limit 1).
func (in *Ingester) fetchVaultMessage(ctx context.Context, msgID int) (*tgutil.IncomingMsg, error) {
	msgs, err := in.backend.GetMessagesBulk(ctx, in.vaultID, msgID-1, msgID, 1)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("vault message %d not found", msgID)
	}
	return &msgs[0], nil
}

// validateCanonicalRecord verifies that a record still points at media in the
// vault and that the vault media has the same stable file key. It deliberately
// distinguishes a missing/mismatched copy (false, nil) from a transient
// Telegram failure (false, err): a transient failure must not trigger a new
// forward and create duplicate vault media.
func (in *Ingester) validateCanonicalRecord(ctx context.Context, rec *store.FileRecord) (bool, error) {
	if in.backend == nil || rec == nil || rec.VaultMsgID == 0 {
		return false, nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, canonicalLookupTimeout)
	defer cancel()
	msg, err := in.fetchVaultMessage(lookupCtx, int(rec.VaultMsgID))
	if err != nil {
		return false, err
	}
	if msg.Media == nil || msg.Media.FileKey != rec.FileKey {
		return false, nil
	}
	if actualSize := msg.Media.Size; actualSize <= 0 || actualSize != rec.Size {
		return false, nil
	}
	return true, nil
}

// deleteVaultMsg removes a vault message with its own deadline. Cleanup must
// complete even when the caller's ingest context is cancelled — the previous
// transport's ctx-less deletes behaved the same way. logMsg is the warning
// text used when the delete fails.
func (in *Ingester) deleteVaultMsg(logMsg string, msgID int) {
	ctx, cancel := context.WithTimeout(context.Background(), canonicalLookupTimeout)
	defer cancel()
	if err := in.backend.DeleteMessages(ctx, in.vaultID, msgID); err != nil {
		in.log.Warn(logMsg, "vault_msg_id", msgID, "error", err)
	}
}

// CleanupDedupMu evicts unlocked dedup mutex entries. The DB unique index on
// file_key prevents duplicates if a concurrent ingest races against cleanup.
func (in *Ingester) CleanupDedupMu() {
	in.dedupMuMu.Lock()
	defer in.dedupMuMu.Unlock()
	for key, mu := range in.dedupMu {
		if mu.TryLock() {
			mu.Unlock()
			delete(in.dedupMu, key)
		}
	}
}

// StartDedupCleanup runs periodic dedup mutex cleanup. Returns a stop function.
func (in *Ingester) StartDedupCleanup(interval time.Duration) (stop func()) {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				in.CleanupDedupMu()
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

func (in *Ingester) fileMutex(key string) *sync.Mutex {
	in.dedupMuMu.Lock()
	defer in.dedupMuMu.Unlock()
	if m, ok := in.dedupMu[key]; ok {
		return m
	}
	if len(in.dedupMu) >= maxDedupEntries {
		for k := range in.dedupMu {
			delete(in.dedupMu, k)
			break
		}
	}
	m := &sync.Mutex{}
	in.dedupMu[key] = m
	return m
}

// Ingest forwards media to the vault, inserts a DB record, returns the result.
// On a dedup hit it returns the existing record with Reused=true.
func (in *Ingester) Ingest(ctx context.Context, m tgutil.IncomingMsg) Result {
	if m.Media == nil {
		return Result{Err: errors.New("message has no media")}
	}

	key := m.Media.FileKey
	if key == "" {
		return Result{Err: errors.New("could not derive stable file key")}
	}

	// Per-file-key mutex — in-process dedup. The cross-process lock below
	// covers other bot processes; this is cheaper than a DB round-trip.
	mu := in.fileMutex(key)
	mu.Lock()
	defer mu.Unlock()

	if in.backend == nil {
		return Result{Err: errors.New("no transport backend available")}
	}

	// FileToLink validates an existing canonical vault copy before reusing it.
	// Keep the stale record so a successful replacement can preserve its
	// provenance and counters.
	var staleExisting *store.FileRecord
	if existing, err := in.store.FindFileByKey(ctx, key); err != nil {
		return Result{Err: fmt.Errorf("checking existing file: %w", err)}
	} else if existing != nil {
		valid, validateErr := in.validateCanonicalRecord(ctx, existing)
		if validateErr != nil {
			return Result{Err: fmt.Errorf("validating existing vault copy: %w", validateErr)}
		}
		if valid {
			if incErr := in.store.IncrementReuseCount(ctx, key); incErr != nil {
				in.log.Debug("incrementing reuse count", "file_key", key, "error", incErr)
			}
			in.log.Debug("dedup hit; reusing verified file record", "file_key", key, "vault_msg_id", existing.VaultMsgID)
			return Result{File: existing, Reused: true}
		}
		staleExisting = existing
		in.log.Warn("canonical vault copy is stale; replacing", "file_key", key, "vault_msg_id", existing.VaultMsgID)
	}

	hash := deterministicHash(key)

	// Cross-process ingest lock prevents two bot processes from each creating a
	// vault message for the same file (the unique file_key index prevents duplicate
	// DB rows but the orphan vault message would leak). TTL-bounded so a crash
	// doesn't hold the lock forever.
	locked, err := in.store.AcquireIngestLock(ctx, key, 60*time.Second)
	if err != nil {
		return Result{Err: fmt.Errorf("acquiring ingest lock: %w", err)}
	}
	if !locked {
		for attempt := 0; attempt < 2; attempt++ {
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return Result{Err: ctx.Err()}
			}
			if existing, lookupErr := in.store.FindFileByKey(ctx, key); lookupErr != nil {
				return Result{Err: fmt.Errorf("checking file after ingest-lock wait: %w", lookupErr)}
			} else if existing != nil {
				valid, validateErr := in.validateCanonicalRecord(ctx, existing)
				if validateErr != nil {
					return Result{Err: fmt.Errorf("validating file after ingest-lock wait: %w", validateErr)}
				}
				if valid {
					if incErr := in.store.IncrementReuseCount(ctx, key); incErr != nil {
						in.log.Debug("incrementing reuse count (after lock wait)", "file_key", key, "error", incErr)
					}
					return Result{File: existing, Reused: true}
				}
				staleExisting = existing
			}
			locked, err = in.store.AcquireIngestLock(ctx, key, 60*time.Second)
			if err != nil {
				return Result{Err: fmt.Errorf("re-acquiring ingest lock: %w", err)}
			}
			if locked {
				break
			}
		}
		if !locked {
			return Result{Err: errors.New("could not acquire ingest lock after retry")}
		}
	}
	defer in.store.ReleaseIngestLock(ctx, key)

	// A different process may have completed ingestion between the earlier
	// check and lock acquisition. Validate again rather than blindly reusing
	// its record.
	if existing, lookupErr := in.store.FindFileByKey(ctx, key); lookupErr != nil {
		return Result{Err: fmt.Errorf("checking file after ingest lock: %w", lookupErr)}
	} else if existing != nil {
		valid, validateErr := in.validateCanonicalRecord(ctx, existing)
		if validateErr != nil {
			return Result{Err: fmt.Errorf("validating file after ingest lock: %w", validateErr)}
		}
		if valid {
			if incErr := in.store.IncrementReuseCount(ctx, key); incErr != nil {
				in.log.Debug("incrementing reuse count (after ingest lock)", "file_key", key, "error", incErr)
			}
			return Result{File: existing, Reused: true}
		}
		staleExisting = existing
	}

	// Forward the media to the vault channel with the author hidden. The
	// backend call is context-aware; retried up to 3× with exponential
	// backoff (1s, 4s) on transient errors. FLOOD_WAIT is NOT retried — a
	// surfaced FLOOD_WAIT means the wait belongs to the caller/limiter, not
	// to a busy-loop here.
	var vaultMsgID int
	var vaultMsg *tgutil.IncomingMsg
	var fwdErr error
forwardLoop:
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt*attempt) * time.Second): // 1s, 4s
			case <-ctx.Done():
				return Result{Err: fmt.Errorf("forwarding file to vault (context cancelled): %w", ctx.Err())}
			}
		}
		if ctx.Err() != nil {
			return Result{Err: fmt.Errorf("forwarding file to vault (context cancelled): %w", ctx.Err())}
		}
		ids, err := in.backend.ForwardMessages(ctx, in.vaultID, m.ChatID, []int{m.MsgID}, true)
		switch {
		case err != nil:
			fwdErr = err
			if _, isFlood := tgutil.MapFloodWait(err); isFlood {
				break forwardLoop
			}
		case len(ids) == 0 || ids[0] == 0:
			fwdErr = errors.New("forwarding returned no messages")
		default:
			vaultMsgID = ids[0]
			// The backend returns only new message IDs, so the forwarded
			// copy is re-fetched for validation (media present, same stable
			// file key) — the same accept/reject logic the forward result
			// object was checked against before.
			candidate, fetchErr := in.fetchVaultMessage(ctx, vaultMsgID)
			if fetchErr != nil {
				// The forward succeeded but the copy cannot be validated.
				// Do NOT retry the forward (it would duplicate the vault
				// message); clean up and fail so the user can retry.
				in.deleteVaultMsg("failed to delete invalid forwarded vault message", vaultMsgID)
				fwdErr = fmt.Errorf("validating forwarded vault message: %w", fetchErr)
				break forwardLoop
			}
			if candidate.Media != nil && candidate.Media.FileKey == key {
				vaultMsg = candidate
				fwdErr = nil
				break forwardLoop
			}
			// A non-media or mismatched forward cannot become canonical. Remove
			// it immediately so a malformed forward does not leak vault rows.
			in.deleteVaultMsg("failed to delete invalid forwarded vault message", vaultMsgID)
			fwdErr = errors.New("forwarded vault message has missing or mismatched media")
			break forwardLoop
		}
	}
	if vaultMsgID == 0 || vaultMsg == nil {
		if fwdErr != nil {
			return Result{Err: fmt.Errorf("forwarding file to vault after retries: %w", fwdErr)}
		}
		return Result{Err: errors.New("forwarding returned no valid media message after retries")}
	}

	// The vault copy is the canonical object that will later be served. Derive
	// every response-facing field from it, as FileToLink does.
	fileName := vaultMsg.Media.Name
	if fileName == "" {
		fileName = fmt.Sprintf("Thunder_%d.bin", vaultMsgID)
	}
	mime := mediaMime(vaultMsg.Media)
	size := vaultMsg.Media.Size
	if size <= 0 {
		in.deleteVaultMsg("failed to delete zero-size vault message", vaultMsgID)
		return Result{Err: errors.New("forwarded vault media has no usable file size")}
	}
	dcID := vaultMsg.Media.DC
	mediaType := vaultMsg.Media.Kind

	firstSourceChatID := m.ChatID
	firstSourceMsgID := m.MsgID

	now := time.Now()
	rec := &store.FileRecord{
		FileKey:           key,
		Hash:              hash,
		FileName:          fileName,
		MimeType:          mime,
		Size:              size,
		MediaType:         mediaType,
		VaultMsgID:        int32(vaultMsgID), //nosec G115 // message IDs fit int32
		DCID:              int32(dcID),       //nosec G115 // DC ids fit int32
		CreatedAt:         now,
		LastSeenAt:        now,
		SeenCount:         0,
		ReuseCount:        0,
		FirstSourceChatID: firstSourceChatID,
		FirstSourceMsgID:  int32(firstSourceMsgID), //nosec G115 // message IDs fit int32
	}
	if staleExisting != nil {
		// Preserve the lifecycle/provenance fields of the old canonical record,
		// matching FileToLink's stale-copy replacement behavior.
		rec.CreatedAt = staleExisting.CreatedAt
		rec.SeenCount = staleExisting.SeenCount + 1
		rec.ReuseCount = staleExisting.ReuseCount
		if staleExisting.FirstSourceChatID != 0 {
			rec.FirstSourceChatID = staleExisting.FirstSourceChatID
			rec.FirstSourceMsgID = staleExisting.FirstSourceMsgID
		}
	}

	var persistErr error
	if staleExisting != nil {
		persistErr = in.store.ReplaceFile(ctx, *rec)
	} else {
		persistErr = in.store.InsertFile(ctx, *rec)
	}
	if persistErr != nil {
		// Re-read before deleting: an Insert may have committed despite a
		// transient response failure. Validate any colliding record before reuse.
		existing, lookupErr := in.store.FindFileByKey(ctx, key)
		if lookupErr == nil && existing != nil {
			valid, validateErr := in.validateCanonicalRecord(ctx, existing)
			if validateErr == nil && valid {
				if int(existing.VaultMsgID) != vaultMsgID {
					in.deleteVaultMsg("failed to delete orphaned vault message", vaultMsgID)
					return Result{File: existing, Reused: true}
				}
				return Result{File: existing, Reused: false}
			}
		}
		in.deleteVaultMsg("failed to delete orphaned vault message", vaultMsgID)
		return Result{Err: fmt.Errorf("persisting canonical file record: %w", persistErr)}
	}
	in.log.Info("file ingested",
		"file_key", key,
		"hash", hash,
		"file_name", fileName,
		"size", size,
		"mime", mime,
		"vault_msg_id", vaultMsgID,
	)
	return Result{File: rec, Reused: false}
}

// mediaMime returns the MIME type for a seam media item, replicating the
// previous extractor's fallbacks: explicit document/photo MIME first, then a
// media-type-based default, then application/octet-stream.
func mediaMime(mi *tgutil.MediaInfo) string {
	if mi.Mime != "" {
		return mi.Mime
	}
	switch mi.Kind {
	case "photo":
		return "image/jpeg"
	case "video":
		return "video/mp4"
	case "audio":
		return "audio/mpeg"
	case "voice":
		return "audio/ogg"
	case "animation":
		return "video/mp4"
	case "sticker":
		return "image/webp"
	}
	return "application/octet-stream"
}

// PostVaultLog posts a log reply to the stored vault message. Format mirrors msgReady.
func (in *Ingester) PostVaultLog(ctx context.Context, rec *store.FileRecord, source Source, streamURL, downloadURL string) error {
	if in.backend == nil {
		return errors.New("no transport backend available")
	}

	sourceInfo := source.UserName
	if sourceInfo == "" {
		sourceInfo = fmt.Sprintf("User %d", source.UserID)
	}
	if source.ChatTitle != "" {
		sourceInfo = fmt.Sprintf("%s in %s", sourceInfo, source.ChatTitle)
	}

	text := fmt.Sprintf(
		"<blockquote>👤 <b>Source:</b> <a href=\"tg://user?id=%d\">%s</a>\n🆔 <b>User ID:</b> <code>%d</code></blockquote>\n\n"+
			"<blockquote><code>%s</code></blockquote>\n\n"+
			"📂 <b>File Size:</b> <code>%s</code>\n"+
			"📎 <b>Type:</b> <code>%s</code>\n\n"+
			"🚀 <b>Download Link:</b>\n<code>%s</code>\n\n"+
			"🖥️ <b>Stream Link:</b>\n<code>%s</code>",
		source.UserID,
		html.EscapeString(sourceInfo),
		source.UserID,
		html.EscapeString(rec.FileName),
		tgutil.FormatBytes(rec.Size),
		html.EscapeString(rec.MimeType),
		html.EscapeString(downloadURL),
		html.EscapeString(streamURL),
	)
	return in.backend.SendHTML(ctx, in.vaultID, text, nil, int(rec.VaultMsgID)) //nosec G115 // message IDs fit int32
}

// Source describes where a file came from. Used in vault log messages.
type Source struct {
	Kind      string // "private" | "group" | "channel"
	UserName  string
	UserID    int64
	ChatTitle string
	ChatID    int64
}

func (s Source) String() string {
	out := s.Kind
	if s.UserName != "" {
		out += " from " + s.UserName
		if s.UserID != 0 {
			out += fmt.Sprintf(" (%d)", s.UserID)
		}
	} else if s.UserID != 0 {
		out += fmt.Sprintf(" from %d", s.UserID)
	}
	if s.ChatTitle != "" {
		out += fmt.Sprintf(" in %s", s.ChatTitle)
		if s.ChatID != 0 {
			out += fmt.Sprintf(" (%d)", s.ChatID)
		}
	} else if s.ChatID != 0 {
		out += fmt.Sprintf(" in %d", s.ChatID)
	}
	return out
}

// deterministicHash derives a public file hash from the file's stable key:
// sha256 truncated to 16 bytes (128 bits) → 32 hex chars. The same file always
// gets the same hash, enabling dedup at the URL level. Derived from the
// transport's stable file key (encodes document ID + access hash) —
// unguessable without the file.
func deterministicHash(fileKey string) string {
	h := sha256.Sum256([]byte(fileKey))
	return hex.EncodeToString(h[:16]) // 32 hex chars = 128 bits
}
