// Package stream implements the HTTP file handler. It resolves a vault copy
// through the leased pool transport and pipes its bytes to the HTTP socket
// with an adaptive windowed download pipeline — parallel Telegram chunk
// fetches delivered strictly in order, flushed per chunk, never fully
// buffered (Phase 3, plan §4).
package stream

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fyaz05/ThunderGo/internal/config"
	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

const (
	// overloadRetryAfterSeconds is the Retry-After for capacity/503 paths.
	overloadRetryAfterSeconds = 2
	// brownoutRetryAfterFloor is the minimum Retry-After when the whole
	// fleet is flood-cooling (the pool's estimate may be lower).
	brownoutRetryAfterFloor = 5
	// floodRetryAfterCap caps the Retry-After on 429 responses.
	floodRetryAfterCap = 60
	// selfHealTimeout bounds the DeleteFileByHash self-heal call.
	selfHealTimeout = 5 * time.Second
	// vaultLookupTimeout bounds one ResolveMedia call.
	vaultLookupTimeout = 30 * time.Second

	// fetchWithRetry policy (plan §4): StreamMaxRetries attempts with
	// 100ms doubling backoff capped at 15s; FLOOD_WAIT sleeps
	// min(wait, cap) and counts against retries; file-reference
	// expirations refresh + retry within a per-request budget.
	retryBaseBackoff       = 100 * time.Millisecond
	retryMaxBackoff        = 15 * time.Second
	maxRefreshesPerRequest = 3
	// crossDCPaceDelay paces FetchChunk dispatches when the file lives on
	// a different DC than the leased slot (avoids cross-DC hammering).
	crossDCPaceDelay = 25 * time.Millisecond

	// Pipeline defaults when the Handler is built without config.
	defaultStreamConcurrency = 4
	defaultStreamBufferCount = 8
	defaultStreamMaxRetries  = 3
	defaultStreamTimeout     = 30 * time.Second
)

// FileStore is the slice of the metadata store the stream handler needs.
// *store.Store satisfies it in production (compile-asserted below); tests
// supply fakes so the serving contract can be pinned without MongoDB.
type FileStore interface {
	FindFileByHash(ctx context.Context, hash string) (*store.FileRecord, error)
	IncrementSeenCount(ctx context.Context, fileKey string) error
	DeleteFileByHash(ctx context.Context, hash string) error
}

var _ FileStore = (*store.Store)(nil)

// Handler is the HTTP file-streaming handler.
type Handler struct {
	Pool     *pool.Pool
	Store    FileStore
	Log      *slog.Logger
	Ingester *ingest.Ingester

	// Stream tuning, wired from config by New (normalized for zero values
	// so a directly-constructed Handler stays usable).
	concurrency int
	bufferCount int
	timeout     time.Duration
	maxRetries  int

	// vault caches resolved vault media per file hash so seek-heavy
	// clients skip repeated ResolveMedia RPCs. nil-safe.
	vault *vaultCache
}

// New wires the windowed serving path. One admitted HTTP request leases one
// pool slot and fetches chunks with up to cfg.StreamConcurrency parallel
// workers, delivered in order through a StreamBufferCount-slot window.
func New(cfg *config.Config, p *pool.Pool, s *store.Store, in *ingest.Ingester, log *slog.Logger) *Handler {
	// A nil *store.Store must not become a typed-nil FileStore interface
	// (h.Store == nil would be false and the guard in ServeHTTP useless).
	var fs FileStore
	if s != nil {
		fs = s
	}
	h := &Handler{Pool: p, Store: fs, Log: log, Ingester: in, vault: newVaultCache()}
	if cfg != nil {
		h.concurrency = cfg.StreamConcurrency
		h.bufferCount = cfg.StreamBufferCount
		h.timeout = cfg.StreamTimeout
		h.maxRetries = cfg.StreamMaxRetries
	}
	h.concurrency, h.bufferCount, h.maxRetries, h.timeout = normalizeStreamTuning(
		h.concurrency, h.bufferCount, h.maxRetries, h.timeout)
	return h
}

func normalizeStreamTuning(concurrency, bufferCount, maxRetries int, timeout time.Duration) (int, int, int, time.Duration) {
	if concurrency < 1 {
		concurrency = defaultStreamConcurrency
	}
	if bufferCount < 2 {
		bufferCount = defaultStreamBufferCount
	}
	if maxRetries < 0 {
		maxRetries = defaultStreamMaxRetries
	}
	if timeout <= 0 {
		timeout = defaultStreamTimeout
	}
	return concurrency, bufferCount, maxRetries, timeout
}

// ServeHTTP resolves a stored file record, reserves one stream lease on the
// least-loaded pool transport, and serves either metadata or bytes. The
// lease is acquired before route-specific work and released exactly once on
// every path.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := tokenFromContext(r.Context())
	if token == "" {
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}
	// chi normally rejects unsupported methods before this handler is reached.
	// Keep this guard here as well so an alternate mount cannot turn POST/PUT
	// into a Telegram download.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.Store == nil {
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}

	rec, err := h.Store.FindFileByHash(r.Context(), token)
	if err != nil {
		h.internalError(w, err, "file lookup failed", "token", tgutil.TokenHash(token))
		return
	}
	if rec == nil || rec.Size <= 0 {
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}

	// Store uses a non-blocking touch buffer in production, so this does not
	// delay bytes. Seen counters are advisory and must never affect streaming.
	if err := h.Store.IncrementSeenCount(r.Context(), rec.FileKey); err != nil {
		h.Log.Debug("incrementing seen count", "token", tgutil.TokenHash(token), "error", err)
	}

	// Match FileToLink's admission lifecycle: choose and reserve a transport
	// before request-specific work. Do not queue work on a saturated fleet.
	lease, err := h.Pool.AcquireBest()
	if err != nil {
		h.serveUnavailable(w, err)
		return
	}
	defer lease.Release()

	rng, hasRange, err := tgutil.ParseRange(r.Header.Get("Range"), rec.Size)
	if err != nil {
		w.Header().Set("Cache-Control", "no-store")
		if errors.Is(err, tgutil.ErrUnsatisfiableRange) {
			w.Header().Set("Content-Range", tgutil.UnsatisfiableContentRange(rec.Size))
			http.Error(w, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		// Malformed Range: constant body, the input is never reflected.
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if hasRange && rng.Start == 0 && rng.End == rec.Size-1 {
		hasRange = false // full-range request collapses to a plain 200
	}

	var contentLength int64
	status := http.StatusOK
	if hasRange {
		contentLength = rng.End - rng.Start + 1
		status = http.StatusPartialContent
	} else {
		contentLength = rec.Size
	}

	// HEAD intentionally uses stored metadata only, as FileToLink does. It
	// still owns a stream lease so load accounting remains consistent, and
	// returns identical headers incl. Content-Length/Content-Range.
	if r.Method == http.MethodHead {
		h.setCommonHeaders(w, rec, tgutil.QueryDisposition(r))
		w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
		if hasRange {
			w.Header().Set("Content-Range", tgutil.ContentRangeValue(rng, rec.Size))
		}
		w.WriteHeader(status)
		return
	}

	// Resolve and validate the vault media with the same transport that
	// will download it. This supplies a fresh file reference for that
	// Telegram session and prevents a corrupt DB row from serving
	// unrelated media.
	fh, err := h.resolveVaultMedia(r.Context(), lease, rec)
	if err != nil {
		h.vault.invalidate(rec.Hash)
		h.serveResolveError(w, r, token, rec, lease, err)
		return
	}

	// Wire window: round the requested start down to a chunk boundary of
	// the served range's tier so Telegram fetches stay 4KiB-aligned. The
	// leading bytes are discarded at emit time — the network fetch is NOT
	// skipped (that's what makes it resume-at-offset for the transport).
	streamEnd := rec.Size - 1
	if hasRange {
		streamEnd = rng.End
	}
	tier := int64(chunkSizeFor(contentLength))
	wireStart := (rng.Start / tier) * tier
	skip := rng.Start - wireStart

	pipe := h.newPipeline(r.Context(), lease, fh, wireStart, streamEnd)
	defer pipe.close()

	// Probe the FIRST chunk before committing the response headers: a
	// total failure with zero bytes written can then be answered with a
	// real status (429/503/404/500) instead of a truncated 200.
	first, ok := pipe.next()
	if !ok {
		h.internalError(w, errors.New("stream: empty chunk plan"), "empty chunk plan", "token", tgutil.TokenHash(token))
		return
	}
	if first.err != nil {
		pipe.abort()
		h.servePipelineError(w, r, token, rec, lease, first.err)
		return
	}

	// Header DISCIPLINE: every header below is set before WriteHeader.
	h.setCommonHeaders(w, rec, tgutil.QueryDisposition(r))
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	if hasRange {
		w.Header().Set("Content-Range", tgutil.ContentRangeValue(rng, rec.Size))
	}
	w.WriteHeader(status)

	flusher, _ := w.(http.Flusher)
	fw := &flushWriter{w: w, f: flusher}
	written := int64(0)

	emit := func(data []byte) bool {
		if int64(len(data)) > contentLength-written {
			data = data[:contentLength-written] // clamp (defensive; the plan is exact)
		}
		n, werr := fw.Write(data)
		written += int64(n)
		return werr == nil
	}
	trimSkip := func(data []byte) ([]byte, bool) {
		if skip <= 0 {
			return data, true
		}
		if int64(len(data)) <= skip {
			skip -= int64(len(data))
			return nil, true
		}
		data = data[skip:]
		skip = 0
		return data, true
	}

	firstData, _ := trimSkip(first.data)
	if len(firstData) > 0 && !emit(firstData) {
		return // client went away; nothing left to answer
	}

	// Ordered emit loop: every subsequent chunk is written as its turn
	// comes. Any failure after the first byte → truncate (clients resume
	// with Range); never write an error body mid-stream.
	for {
		res, ok := pipe.next()
		if !ok {
			break
		}
		if res.err != nil {
			h.Log.Warn("stream ended early",
				"token", tgutil.TokenHash(token),
				"written", written,
				"content_length", contentLength,
				"error", res.err)
			return
		}
		data, _ := trimSkip(res.data)
		if len(data) > 0 && !emit(data) {
			return
		}
	}
	if written < contentLength {
		h.Log.Warn("download ended prematurely",
			"token", tgutil.TokenHash(token),
			"written", written,
			"content_length", contentLength)
	}
}

// --- error mapping (plan §8) ---

// serveUnavailable maps AcquireBest failures onto 503 responses:
// Retry-After 2 for capacity, the brownout estimate (floor 5s) when the
// whole fleet is flood-cooling. Constant bodies; never queue.
func (h *Handler) serveUnavailable(w http.ResponseWriter, err error) {
	w.Header().Set("Cache-Control", "no-store")
	if errors.Is(err, pool.ErrPoolBrownout) {
		ra := h.Pool.BrownoutRetryAfter()
		if ra < brownoutRetryAfterFloor {
			ra = brownoutRetryAfterFloor
		}
		if ra > maxFloodCooldownSecs {
			ra = maxFloodCooldownSecs
		}
		w.Header().Set("Retry-After", strconv.Itoa(ra))
		h.Log.Warn("pool brownout; every transport flood-cooling", "retry_after", ra, "error", err)
	} else {
		w.Header().Set("Retry-After", strconv.Itoa(overloadRetryAfterSeconds))
		h.Log.Warn("pool at capacity", "error", err)
	}
	http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
}

// maxFloodCooldownSecs mirrors the pool's cooldown cap for Retry-After math.
const maxFloodCooldownSecs = 600

// serveResolveError maps resolveVaultMedia failures (headers not committed):
// flood → ReportFlood + 503 with Retry-After from the wait (min 2);
// permanent → self-heal delete + 404; transient → 503; other → 500.
func (h *Handler) serveResolveError(w http.ResponseWriter, r *http.Request, token string, rec *store.FileRecord, lease *pool.Lease, err error) {
	if r.Context().Err() != nil {
		return // client gone; nothing to answer
	}
	if wait, isFlood := tgutil.MapFloodWait(err); isFlood {
		if t := lease.Transport(); t != nil {
			h.Pool.ReportFlood(t, time.Duration(wait)*time.Second)
		}
		ra := wait
		if ra < overloadRetryAfterSeconds {
			ra = overloadRetryAfterSeconds
		}
		if ra > maxFloodCooldownSecs {
			ra = maxFloodCooldownSecs
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Retry-After", strconv.Itoa(ra))
		h.Log.Warn("vault lookup flood-limited", "token", tgutil.TokenHash(token), "wait_secs", wait)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	if tgutil.Classify(err) == tgutil.ErrClassPermanent {
		// Re-check before self-healing: a request that died mid-resolve
		// must never delete a record on classification that raced the
		// cancellation. The stale record will self-heal on the next live
		// request that observes it.
		if r.Context().Err() != nil {
			return
		}
		h.Log.Warn("stale vault file record",
			"token", tgutil.TokenHash(token),
			"vault_msg_id", rec.VaultMsgID,
			"error", err)
		h.deleteRecord(token)
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}
	if tgutil.Classify(err) == tgutil.ErrClassTransient {
		h.Log.Warn("transient vault media error", "token", tgutil.TokenHash(token), "error", err)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Retry-After", strconv.Itoa(overloadRetryAfterSeconds))
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	h.internalError(w, err, "vault media resolve failed", "token", tgutil.TokenHash(token))
}

// servePipelineError maps a first-chunk pipeline failure (still zero bytes
// written, headers not committed). Flood exhaustion → 429 with a capped
// wait estimate; permanent → self-heal + 404; transient → 503; else 500.
func (h *Handler) servePipelineError(w http.ResponseWriter, r *http.Request, token string, rec *store.FileRecord, lease *pool.Lease, err error) {
	if r.Context().Err() != nil {
		return // client gone; nothing to answer
	}
	if wait, isFlood := tgutil.MapFloodWait(err); isFlood {
		if t := lease.Transport(); t != nil {
			h.Pool.ReportFlood(t, time.Duration(wait)*time.Second)
		}
		ra := wait
		if ra < overloadRetryAfterSeconds {
			ra = overloadRetryAfterSeconds
		}
		if ra > floodRetryAfterCap {
			ra = floodRetryAfterCap
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Retry-After", strconv.Itoa(ra))
		h.Log.Warn("stream flood-limited before first byte",
			"token", tgutil.TokenHash(token), "wait_secs", wait)
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}
	if tgutil.Classify(err) == tgutil.ErrClassPermanent {
		// Same TOCTOU guard as serveResolveError: never self-heal off a
		// request whose context died mid-flight.
		if r.Context().Err() != nil {
			return
		}
		h.Log.Warn("vault media went stale during stream start",
			"token", tgutil.TokenHash(token),
			"vault_msg_id", rec.VaultMsgID,
			"error", err)
		h.vault.invalidate(token)
		h.deleteRecord(token)
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}
	if tgutil.Classify(err) == tgutil.ErrClassTransient {
		h.Log.Warn("transient stream start failure", "token", tgutil.TokenHash(token), "error", err)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Retry-After", strconv.Itoa(overloadRetryAfterSeconds))
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	h.internalError(w, err, "stream start failed", "token", tgutil.TokenHash(token))
}

// deleteRecord self-heals a permanently stale file record. Detached 5s ctx:
// the record must be deleted even though the request context may already be
// failing.
func (h *Handler) deleteRecord(token string) {
	deleteCtx, cancel := context.WithTimeout(context.Background(), selfHealTimeout)
	defer cancel()
	if err := h.Store.DeleteFileByHash(deleteCtx, token); err != nil {
		h.Log.Debug("could not delete stale file record", "token", tgutil.TokenHash(token), "error", err)
	}
}

// internalError answers 500 with a constant body carrying a random support
// id (16 hex chars from 8 crypto/rand bytes); the full error is logged with
// the same id for correlation. no-store + CORS so proxies and browsers never
// cache an error, and third-party players still see the failure.
func (h *Handler) internalError(w http.ResponseWriter, err error, msg string, args ...any) {
	id := supportID()
	h.Log.Error(msg, append(args, "support_id", id, "error", err)...)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	http.Error(w, "internal error (support id: "+id+")", http.StatusInternalServerError)
}

// supportID returns 16 hex chars of 8 crypto/rand bytes.
func supportID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is extraordinary; derive a unique-enough
		// id from the clock rather than failing the response.
		binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))
	}
	return hex.EncodeToString(b[:])
}

// --- vault media resolution ---

// resolveVaultMedia returns a validated FileHandle for the record's vault
// message, resolved on the leased transport (same session will download it)
// and guarded by the per-slot lookup semaphore. Results are TTL-cached per
// file hash so seeks skip repeated lookups.
//
// Validation note: FileHandle carries no FileKey, so the legacy key
// comparison is gone — (vault msg ID + record hash + exact size match) is
// the identity check now. The size check also guards Content-Length against
// describing a different vault object.
func (h *Handler) resolveVaultMedia(ctx context.Context, lease *pool.Lease, rec *store.FileRecord) (tgutil.FileHandle, error) {
	if rec == nil {
		return tgutil.FileHandle{}, tgutil.ErrStaleMedia
	}
	now := time.Now()
	if fh, ok := h.vault.get(rec.Hash, now); ok {
		return fh, nil
	}
	if h.Ingester == nil {
		return tgutil.FileHandle{}, tgutil.ErrStaleMedia
	}
	type result struct {
		fh  tgutil.FileHandle
		err error
	}
	lookupCtx, cancel := context.WithTimeout(ctx, vaultLookupTimeout)
	defer cancel()
	releaseLookup, ok := lease.AcquireLookup(lookupCtx)
	if !ok {
		return tgutil.FileHandle{}, lookupCtx.Err()
	}
	ch := make(chan result, 1)
	go func() {
		defer releaseLookup()
		fh, err := lease.Transport().ResolveMedia(lookupCtx, h.Ingester.VaultChannelID(), int(rec.VaultMsgID))
		ch <- result{fh: fh, err: err}
	}()

	select {
	case got := <-ch:
		// Cancellation raced with the result. A zero-Location handle
		// returned while the lookup context is already dead is NOT proof
		// of staleness — treating it as such would self-heal (delete) a
		// perfectly good record off a dying request. Surface the ctx
		// error instead; the mapper treats it as client-gone/other.
		if lookupCtx.Err() != nil {
			return tgutil.FileHandle{}, lookupCtx.Err()
		}
		if got.err != nil {
			return tgutil.FileHandle{}, got.err
		}
		if got.fh.Location == nil {
			return tgutil.FileHandle{}, tgutil.ErrStaleMedia
		}
		if got.fh.Size <= 0 || got.fh.Size != rec.Size {
			return tgutil.FileHandle{}, tgutil.ErrRecordMismatch
		}
		h.vault.put(rec.Hash, got.fh, now)
		return got.fh, nil
	case <-lookupCtx.Done():
		return tgutil.FileHandle{}, lookupCtx.Err()
	}
}

// --- vault TTL cache ---

const (
	vaultCacheTTL      = 5 * time.Minute
	vaultCacheCapacity = 1024
)

type vaultEntry struct {
	fh      tgutil.FileHandle
	expires time.Time
}

// vaultCache is a bounded, mutex-guarded TTL cache of resolved vault media
// keyed by file hash, evict-oldest on insert. All methods are nil-safe so a
// zero-value Handler works.
type vaultCache struct {
	mu      sync.Mutex
	entries map[string]vaultEntry
	order   []string // insertion order for evict-oldest
}

func newVaultCache() *vaultCache {
	return &vaultCache{entries: make(map[string]vaultEntry)}
}

func (c *vaultCache) get(hash string, now time.Time) (tgutil.FileHandle, bool) {
	if c == nil {
		return tgutil.FileHandle{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[hash]
	if !ok {
		return tgutil.FileHandle{}, false
	}
	if now.After(e.expires) {
		c.removeLocked(hash)
		return tgutil.FileHandle{}, false
	}
	return e.fh, true
}

func (c *vaultCache) put(hash string, fh tgutil.FileHandle, now time.Time) {
	if c == nil || hash == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[hash]; !exists {
		for len(c.order) >= vaultCacheCapacity {
			oldest := c.order[0]
			c.order = c.order[1:]
			delete(c.entries, oldest)
		}
		c.order = append(c.order, hash)
	}
	c.entries[hash] = vaultEntry{fh: fh, expires: now.Add(vaultCacheTTL)}
}

func (c *vaultCache) invalidate(hash string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(hash)
}

func (c *vaultCache) removeLocked(hash string) {
	if _, ok := c.entries[hash]; !ok {
		return
	}
	delete(c.entries, hash)
	for i, k := range c.order {
		if k == hash {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
}

// --- adaptive windowed pipeline ---

// chunkSizeFor picks the transfer window size for a remaining byte count
// (plan §4 tiers): <512KB→64KiB; <4MiB→256KiB; <32MiB→512KiB; else 1MiB.
// Every tier is a 4KiB multiple and ≤ 1MiB, so each FetchChunk call respects
// Telegram's offset/limit alignment and ceiling constraints by construction.
func chunkSizeFor(remaining int64) int32 {
	switch {
	case remaining < 512<<10:
		return 64 << 10
	case remaining < 4<<20:
		return 256 << 10
	case remaining < 32<<20:
		return 512 << 10
	default:
		return 1 << 20
	}
}

type chunkPlanEntry struct {
	offset int64
	size   int32
}

// buildChunkPlan splits [start..end] into sequential windows: each window's
// size comes from chunkSizeFor(bytes still remaining at that offset), and the
// last window is clamped to the remainder. Window starts are 4KiB-aligned
// relative to the file (all tier sizes are 4KiB multiples).
func buildChunkPlan(start, end int64) []chunkPlanEntry {
	plan := make([]chunkPlanEntry, 0, (end-start)/int64(64<<10)+2)
	for offset := start; offset <= end; {
		remaining := end - offset + 1
		size := chunkSizeFor(remaining)
		if int64(size) > remaining {
			size = int32(remaining) // last window clamps to the remainder
		}
		plan = append(plan, chunkPlanEntry{offset: offset, size: size})
		offset += int64(size)
	}
	return plan
}

type chunkResult struct {
	data []byte
	err  error
}

// pipeline fetches the planned chunks with parallel workers and delivers
// the results strictly in order through a bounded slot window. Nothing is
// ever fully buffered: the consumer (ServeHTTP) pulls one chunk at a time
// and writes it straight to the flush writer.
type pipeline struct {
	transport   tgutil.Transport
	lease       *pool.Lease
	plan        []chunkPlanEntry
	concurrency int
	maxRetries  int
	timeout     time.Duration
	crossDC     bool

	pool *pool.Pool
	log  *slog.Logger

	// fh is shared by all workers; RefreshFileRef swaps it in place under
	// fhMu. FetchChunk receives value snapshots so it never races.
	fh   tgutil.FileHandle
	fhMu sync.Mutex

	refreshes     atomic.Int32 // total RefreshFileRef attempts (budget 3)
	lastFloodWait atomic.Int32 // seconds; from the most recent flood error
	abortFlag     atomic.Bool

	// freeSlots implements the window cap: one token per buffer slot.
	// The dispatcher takes a token before dispatching a chunk; the
	// consumer returns one after emitting it.
	freeSlots chan struct{}

	// dispatchClosed flips true when the dispatcher goroutine exits (plan
	// exhausted, aborted, or context cancelled); sent counts jobs handed to
	// workers. Together they let next() prove whether a pending chunk can
	// still arrive instead of waiting forever.
	dispatchClosed bool
	sent           int

	mu       sync.Mutex
	cond     *sync.Cond
	slots    []chunkResult // len = bufferCount; workers write slots[i%len]
	filled   []bool
	nextEmit int

	jobs   chan int
	wg     sync.WaitGroup
	cancel context.CancelFunc
	ctx    context.Context
}

func (h *Handler) newPipeline(ctx context.Context, lease *pool.Lease, fh tgutil.FileHandle, start, end int64) *pipeline {
	concurrency, bufferCount, maxRetries, timeout := normalizeStreamTuning(
		h.concurrency, h.bufferCount, h.maxRetries, h.timeout)
	pctx, cancel := context.WithCancel(ctx)
	p := &pipeline{
		transport:   lease.Transport(),
		lease:       lease,
		plan:        buildChunkPlan(start, end),
		concurrency: concurrency,
		maxRetries:  maxRetries,
		timeout:     timeout,
		fh:          fh,
		pool:        h.Pool,
		log:         h.Log,
		freeSlots:   make(chan struct{}, bufferCount),
		slots:       make([]chunkResult, bufferCount),
		filled:      make([]bool, bufferCount),
		jobs:        make(chan int),
		cancel:      cancel,
		ctx:         pctx,
	}
	p.cond = sync.NewCond(&p.mu)
	p.crossDC = fh.DC != 0 && lease.DC() != 0 && fh.DC != lease.DC()
	for i := 0; i < bufferCount; i++ {
		p.freeSlots <- struct{}{}
	}
	p.start()
	return p
}

// start launches the dispatcher + worker pool.
func (p *pipeline) start() {
	workers := p.concurrency
	if workers > len(p.plan) {
		workers = len(p.plan)
	}
	if workers < 1 {
		workers = 1
	}
	p.wg.Add(1)
	go p.dispatch()
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// dispatch feeds chunk indices to the workers, flow-controlled by the slot
// window: a token is taken per dispatched chunk and only returned when the
// consumer emits that chunk's result. Exiting closes the jobs channel so
// workers drain out.
func (p *pipeline) dispatch() {
	defer p.wg.Done()
	defer close(p.jobs)
	defer func() {
		p.mu.Lock()
		p.dispatchClosed = true
		p.cond.Broadcast() // wake a consumer waiting on a chunk that will never come
		p.mu.Unlock()
	}()
	for i := range p.plan {
		if p.abortFlag.Load() {
			return
		}
		select {
		case <-p.ctx.Done():
			return
		case <-p.freeSlots:
		}
		select {
		case p.jobs <- i:
			p.mu.Lock()
			p.sent = i + 1
			p.mu.Unlock()
		case <-p.ctx.Done():
			return
		}
	}
}

// worker fetches its assigned chunks and publishes results into the ordered
// slot buffer. The token discipline in dispatch guarantees the slot is free
// when its owner writes it.
func (p *pipeline) worker() {
	defer p.wg.Done()
	for idx := range p.jobs {
		entry := p.plan[idx]
		data, err := p.fetchWithRetry(p.ctx, entry)
		p.mu.Lock()
		slotIdx := idx % len(p.slots)
		p.slots[slotIdx] = chunkResult{data: data, err: err}
		p.filled[slotIdx] = true
		p.cond.Broadcast()
		p.mu.Unlock()
	}
}

// next returns the next chunk result in plan order; ok=false once the plan
// is exhausted. It blocks until the result arrives (fetches are bounded by
// the per-attempt stall timeout and by the pipeline ctx).
func (p *pipeline) next() (chunkResult, bool) {
	p.mu.Lock()
	for {
		if p.nextEmit >= len(p.plan) {
			p.mu.Unlock()
			return chunkResult{}, false
		}
		slotIdx := p.nextEmit % len(p.slots)
		if p.filled[slotIdx] {
			res := p.slots[slotIdx]
			p.filled[slotIdx] = false
			p.nextEmit++
			p.mu.Unlock()
			// Hand the window token back; this never blocks while
			// the dispatcher is running (outstanding ≤ bufferCount).
			p.freeSlots <- struct{}{}
			return res, true
		}
		if p.ctx.Err() != nil {
			p.mu.Unlock()
			return chunkResult{err: p.ctx.Err()}, true
		}
		if p.dispatchClosed && p.sent <= p.nextEmit {
			// The dispatcher is gone and every chunk it handed out has been
			// emitted: nothing can fill this slot. Terminate the stream
			// instead of hanging (found by the cancel-storm test).
			p.mu.Unlock()
			return chunkResult{err: errors.New("stream: chunk dispatcher ended before plan completion")}, true
		}
		p.cond.Wait()
	}
}

// abort stops further dispatch and makes workers exit between attempts.
// In-flight fetches are cut by the pipeline ctx (see close).
func (p *pipeline) abort() {
	p.abortFlag.Store(true)
	p.mu.Lock()
	p.cond.Broadcast()
	p.mu.Unlock()
}

// close cancels the pipeline ctx and waits for dispatcher + workers to
// drain. Fetches are ctx- and timeout-bounded, so the wait is short.
func (p *pipeline) close() {
	p.cancel()
	p.abort()
	p.wg.Wait()
}

// handle snapshots the shared file handle for one FetchChunk call.
func (p *pipeline) handle() tgutil.FileHandle {
	p.fhMu.Lock()
	defer p.fhMu.Unlock()
	return p.fh
}

// refreshFileRef re-resolves the file reference in place (budget: 3 per
// request). Reports whether a retry should proceed.
func (p *pipeline) refreshFileRef(ctx context.Context) bool {
	if p.refreshes.Add(1) > maxRefreshesPerRequest {
		return false
	}
	p.fhMu.Lock()
	defer p.fhMu.Unlock()
	if err := p.transport.RefreshFileRef(ctx, &p.fh); err != nil {
		p.log.Debug("RefreshFileRef failed",
			"refresh", p.refreshes.Load(), "error", err)
		return false
	}
	return true
}

// fetchWithRetry wraps one FetchChunk with the plan §4 retry policy:
//
//   - per-attempt stall timeout (StreamTimeout);
//   - permanent errors abort the pipeline immediately;
//   - FLOOD_WAIT → pool.ReportFlood + sleep min(wait, 15s) + retry (counts
//     against StreamMaxRetries);
//   - ErrFileRefExpired → RefreshFileRef + immediate retry (≤3 per request);
//   - transient errors → exponential backoff 100ms doubling, cap 15s;
//   - ctx done or aborted → stop.
func (p *pipeline) fetchWithRetry(ctx context.Context, entry chunkPlanEntry) ([]byte, error) {
	var backoff time.Duration
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.abortFlag.Load() {
			return nil, errors.New("stream: pipeline aborted")
		}
		if p.crossDC {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(crossDCPaceDelay):
			}
		}
		fh := p.handle()
		cctx, cancel := context.WithTimeout(ctx, p.timeout)
		data, err := p.transport.FetchChunk(cctx, fh, entry.offset, entry.size)
		cancel()
		if err == nil {
			return data, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if tgutil.Classify(err) == tgutil.ErrClassPermanent {
			return nil, err
		}
		if attempt >= p.maxRetries {
			return nil, err
		}
		if wait, isFlood := tgutil.MapFloodWait(err); isFlood {
			p.lastFloodWait.Store(int32(wait))
			if p.pool != nil {
				p.pool.ReportFlood(p.transport, time.Duration(wait)*time.Second)
			}
			sleepFor := time.Duration(wait) * time.Second
			if sleepFor > retryMaxBackoff {
				sleepFor = retryMaxBackoff
			}
			if sleepErr := sleepCtx(ctx, sleepFor); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}
		if errors.Is(err, tgutil.ErrFileRefExpired) {
			if !p.refreshFileRef(ctx) {
				return nil, err
			}
			continue // refreshed; retry immediately without backoff
		}
		// Transient: 100ms doubling, cap 15s.
		if backoff == 0 {
			backoff = retryBaseBackoff
		} else {
			backoff *= 2
			if backoff > retryMaxBackoff {
				backoff = retryMaxBackoff
			}
		}
		if sleepErr := sleepCtx(ctx, backoff); sleepErr != nil {
			return nil, sleepErr
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// --- headers ---

// setCommonHeaders writes the common file response headers. Gateway middleware
// owns the CORS headers.
func (h *Handler) setCommonHeaders(w http.ResponseWriter, rec *store.FileRecord, disposition string) {
	hdr := w.Header()
	mime := rec.MimeType
	if mime == "" {
		mime = "application/octet-stream"
	}
	hdr.Set("Content-Type", mime)
	hdr.Set("Content-Disposition", tgutil.DispositionHeader(disposition, rec.FileName))
	hdr.Set("Accept-Ranges", "bytes")
	hdr.Set("Cache-Control", "public, max-age=31536000")
	hdr.Set("Connection", "keep-alive")
	hdr.Set("X-Content-Type-Options", "nosniff")
}

// --- context plumbing ---

type ctxKey int

const ctxKeyToken ctxKey = 1

func WithToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, ctxKeyToken, tgutil.NormalizeBase32(token))
}

func tokenFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyToken).(string)
	return v
}

type flushWriter struct {
	w io.Writer
	f http.Flusher
}

func (fw *flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	if err == nil && fw.f != nil {
		fw.f.Flush()
	}
	return n, err
}
