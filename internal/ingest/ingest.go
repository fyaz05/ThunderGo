// Package ingest turns incoming Telegram media into a storable vault message
// plus a file record, with deduplication: the same file (by stable ID) never
// creates a second vault message. Concurrent ingesters race for a per-file-key
// mutex; the first writer wins, subsequent writers reuse the existing record.
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/amarnathcjd/gogram"
	"github.com/amarnathcjd/gogram/telegram"

	"github.com/fyaz05/ThunderGo/internal/pool"
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
const maxDedupEntries = 10000

// Ingester coordinates file ingestion. It is safe for concurrent use.
type Ingester struct {
	pool  *pool.Pool
	store *store.Store
	log   *slog.Logger

	// vaultPeer caches the InputPeer for the vault channel so we don't
	// resolve it on every ingest.
	vaultPeerMu sync.RWMutex
	vaultPeer   telegram.InputPeer
	vaultID     int64

	// dedupMu provides per-file-key locking. The map is guarded by dedupMuMu;
	// each entry is held during the "check store, then write vault message and
	// insert record" critical section. CleanupDedupMu periodically removes
	// entries whose mutex is not currently held.
	dedupMuMu sync.Mutex
	dedupMu   map[string]*sync.Mutex
}

func New(p *pool.Pool, s *store.Store, vaultChannelID int64, log *slog.Logger) *Ingester {
	return &Ingester{
		pool:    p,
		store:   s,
		log:     log,
		vaultID: vaultChannelID,
		dedupMu: make(map[string]*sync.Mutex),
	}
}

// VaultChannelID returns the vault channel ID used by the stream handler.
func (in *Ingester) VaultChannelID() int64 { return in.vaultID }

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

// vaultPeerResolved returns the cached vault InputPeer, resolving on first
// use. Cache is invalidated on failure; one retry before surfacing error.
func (in *Ingester) vaultPeerResolved(ctx context.Context) (telegram.InputPeer, error) {
	in.vaultPeerMu.RLock()
	if in.vaultPeer != nil {
		p := in.vaultPeer
		in.vaultPeerMu.RUnlock()
		return p, nil
	}
	in.vaultPeerMu.RUnlock()

	in.vaultPeerMu.Lock()
	defer in.vaultPeerMu.Unlock()
	if in.vaultPeer != nil {
		return in.vaultPeer, nil
	}
	primary := in.pool.Primary()
	if primary == nil {
		return nil, errors.New("no primary client available")
	}
	// GetInputPeer is undocumented; prefer ResolvePeer() as documented alternative.
	peer, err := primary.GetInputPeer(in.vaultID)
	if err != nil {
		in.vaultPeer = nil // invalidate cache
		// Retry once after invalidation
		peer, err = primary.GetInputPeer(in.vaultID)
		if err != nil {
			return nil, fmt.Errorf("resolving vault channel: %w", err)
		}
	}
	in.vaultPeer = peer
	return peer, nil
}

// Ingest forwards media to the vault, inserts a DB record, returns the result.
// On a dedup hit it returns the existing record with Reused=true.
func (in *Ingester) Ingest(ctx context.Context, msg *telegram.NewMessage) Result {
	if msg == nil || !msg.IsMedia() {
		return Result{Err: errors.New("message has no media")}
	}

	key := tgutil.FileKey(msg)
	if key == "" {
		return Result{Err: errors.New("could not derive stable file key")}
	}

	// Per-file-key mutex — in-process dedup. The cross-process lock below
	// covers other bot processes; this is cheaper than a DB round-trip.
	mu := in.fileMutex(key)
	mu.Lock()
	defer mu.Unlock()

	if existing, err := in.store.FindFileByKey(ctx, key); err != nil {
		return Result{Err: fmt.Errorf("checking existing file: %w", err)}
	} else if existing != nil {
		// Best-effort reuse-count bump; non-blocking.
		if incErr := in.store.IncrementReuseCount(ctx, key); incErr != nil {
			in.log.Debug("incrementing reuse count", "file_key", key, "error", incErr)
		}
		in.log.Debug("dedup hit; reusing file record", "file_key", key, "vault_msg_id", existing.VaultMsgID)
		return Result{File: existing, Reused: true}
	}

	primary := in.pool.Primary()
	if primary == nil {
		return Result{Err: errors.New("no primary client available")}
	}

	fileName, _ := tgutil.ExtractFileName(msg)
	mime := tgutil.ExtractMIME(msg)
	size := tgutil.ExtractSize(msg)
	dcID := tgutil.ExtractDcID(msg)
	mediaType := tgutil.MediaType(msg)
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
			if existing, _ := in.store.FindFileByKey(ctx, key); existing != nil {
				if incErr := in.store.IncrementReuseCount(ctx, key); incErr != nil {
					in.log.Debug("incrementing reuse count (after lock wait)", "file_key", key, "error", incErr)
				}
				return Result{File: existing, Reused: true}
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

	// Forward the media to the vault channel. gogram's Forward has no context,
	// so we wrap it in a deadline-bearing goroutine selecting on the caller's
	// ctx. Retried up to 3× with exponential backoff (1s, 4s) on transient
	// errors. FLOOD_WAIT is NOT retried — the pool's FloodHandler already
	// slept + retried inside Forward; a surfaced FLOOD_WAIT means the wait
	// exceeded maxFloodWaitSecs (600s).
	vaultPeer, err := in.vaultPeerResolved(ctx)
	if err != nil {
		return Result{Err: err}
	}
	type fwdResult struct {
		msgs []telegram.NewMessage
		err  error
	}
	var vaultMsgID int32
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
		fwdCh := make(chan fwdResult, 1)
		go func() {
			if ctx.Err() != nil {
				fwdCh <- fwdResult{nil, ctx.Err()}
				return
			}
			msgs, err := primary.Forward(vaultPeer, msg.Peer, []int32{msg.ID}, &telegram.ForwardOptions{HideAuthor: true})
			fwdCh <- fwdResult{msgs, err}
		}()
		select {
		case fwd := <-fwdCh:
			if fwd.err == nil && len(fwd.msgs) > 0 && fwd.msgs[0].ID != 0 {
				vaultMsgID = fwd.msgs[0].ID
				fwdErr = nil
				break forwardLoop
			}
			fwdErr = fwd.err
			var rpcErr *gogram.ErrResponseCode
			if fwd.err != nil && errors.As(fwd.err, &rpcErr) && strings.HasPrefix(rpcErr.Message, "FLOOD_WAIT") {
				break forwardLoop
			}
		case <-ctx.Done():
			return Result{Err: fmt.Errorf("forwarding file to vault (context cancelled): %w", ctx.Err())}
		}
	}
	if vaultMsgID == 0 {
		if fwdErr != nil {
			return Result{Err: fmt.Errorf("forwarding file to vault after retries: %w", fwdErr)}
		}
		return Result{Err: errors.New("forwarding returned no messages after retries")}
	}

	// ChatID may return 0 for private chats on some gogram paths.
	firstSourceChatID := msg.ChatID()
	firstSourceMsgID := msg.ID

	now := time.Now()
	rec := &store.FileRecord{
		FileKey:           key,
		Hash:              hash,
		FileName:          fileName,
		MimeType:          mime,
		Size:              size,
		MediaType:         mediaType,
		VaultMsgID:        vaultMsgID,
		DCID:              dcID,
		CreatedAt:         now,
		LastSeenAt:        now,
		SeenCount:         0,
		ReuseCount:        0,
		FirstSourceChatID: firstSourceChatID,
		FirstSourceMsgID:  firstSourceMsgID,
	}
	if err := in.store.InsertFile(ctx, *rec); err != nil {
		// Re-read before deleting: the insert may have committed despite a
		// transient error — deleting would orphan the record.
		existing, lookupErr := in.store.FindFileByKey(ctx, key)
		if lookupErr == nil && existing != nil && existing.VaultMsgID != vaultMsgID {
			// Another ingester's record is canonical; our vault message is an orphan.
			in.log.Warn("file record insert collided; reusing existing",
				"file_key", key, "our_vault_msg_id", vaultMsgID, "existing_vault_msg_id", existing.VaultMsgID)
			if _, delErr := primary.DeleteMessages(vaultPeer, []int32{vaultMsgID}); delErr != nil {
				in.log.Warn("failed to delete orphaned vault message",
					"vault_msg_id", vaultMsgID, "error", delErr)
			}
			return Result{File: existing, Reused: true}
		}
		if lookupErr == nil && existing != nil && existing.VaultMsgID == vaultMsgID {
			// Error was a transient response-path failure; the insert committed.
			in.log.Warn("file record insert reported error but row is present; treating as committed",
				"file_key", key, "vault_msg_id", vaultMsgID, "insert_error", err)
			return Result{File: existing, Reused: false}
		}
		// Genuine failure — no record exists, delete the orphan vault message.
		if _, delErr := primary.DeleteMessages(vaultPeer, []int32{vaultMsgID}); delErr != nil {
			in.log.Warn("failed to delete orphaned vault message",
				"vault_msg_id", vaultMsgID, "error", delErr)
		}
		return Result{Err: fmt.Errorf("inserting file record: %w", err)}
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

// PostVaultLog posts a log reply to the stored vault message. Format mirrors msgReady.
func (in *Ingester) PostVaultLog(ctx context.Context, rec *store.FileRecord, source Source, streamURL, downloadURL string) error {
	primary := in.pool.Primary()
	if primary == nil {
		return errors.New("no primary client")
	}
	vaultPeer, err := in.vaultPeerResolved(ctx)
	if err != nil {
		return err
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
	opts := &telegram.SendOptions{
		ParseMode: "HTML",
		ReplyID:   rec.VaultMsgID,
	}
	_, err = primary.SendMessage(vaultPeer, text, opts)
	return err
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
// gets the same hash, enabling dedup at the URL level. Derived from
// PackBotFileID (encodes document ID + access hash) — unguessable without the file.
func deterministicHash(fileKey string) string {
	h := sha256.Sum256([]byte(fileKey))
	return hex.EncodeToString(h[:16]) // 32 hex chars = 128 bits
}
