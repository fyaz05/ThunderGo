// Package stream implements the HTTP streaming handler. It pipes bytes from
// Telegram to the HTTP socket with zero intermediate disk writes.
//
// Mounted at /f/{token}/{filename}/raw. Supports Range requests, HEAD, and
// CORS preflight (handled by the gateway).
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/amarnathcjd/gogram/telegram"

	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// Handler is the streaming HTTP handler.
type Handler struct {
	Pool     *pool.Pool
	Store    *store.Store
	Log      *slog.Logger
	Ingester *ingest.Ingester

	// handlerWg tracks in-flight fire-and-forget goroutines so Close() can drain.
	handlerWg sync.WaitGroup

	// StreamThreads is the number of concurrent download workers per full-body
	// stream (1 = sequential; 2-8 = parallel via orderedWriter). Range path is
	// always sequential. Set from TG_STREAM_THREADS (validated [1, 8] in config).
	StreamThreads int
}

// New wires up the Handler. streamThreads <= 1 keeps the sequential path.
func New(p *pool.Pool, s *store.Store, in *ingest.Ingester, log *slog.Logger, streamThreads int) *Handler {
	return &Handler{Pool: p, Store: s, Log: log, Ingester: in, StreamThreads: streamThreads}
}

// ServeHTTP implements http.Handler. The chi router extracts the {token}
// URL parameter and stashes it in the request context.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token := tokenFromContext(r.Context())
	if token == "" {
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}

	rec, err := h.Store.FindFileByHash(r.Context(), token)
	if err != nil {
		h.Log.Error("file lookup failed", "token", tgutil.TokenHash(token), "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if rec == nil || rec.Size <= 0 {
		w.Header().Set("Cache-Control", "no-store")
		http.NotFound(w, r)
		return
	}

	// Best-effort seen_count++ on a detached 5s context: a slow DB write must
	// not delay bytes. Caches are not invalidated (last_seen_at is advisory).
	h.handlerWg.Add(1)
	go func() {
		defer h.handlerWg.Done()
		defer func() { recover() }()
		incCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.Store.IncrementSeenCount(incCtx, rec.FileKey); err != nil {
			h.Log.Debug("incrementing seen count", "token", tgutil.TokenHash(token), "error", err)
		}
	}()

	rng, hasRange, err := tgutil.ParseRange(r.Header.Get("Range"), rec.Size)
	if err != nil {
		if errors.Is(err, tgutil.ErrUnsatisfiableRange) {
			w.Header().Set("Content-Range", tgutil.UnsatisfiableContentRange(rec.Size))
			http.Error(w, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if hasRange && rng.Start == 0 && rng.End == rec.Size-1 {
		hasRange = false
	}

	var contentLength int64
	var status int
	if hasRange {
		contentLength = rng.End - rng.Start + 1
		status = http.StatusPartialContent
	} else {
		contentLength = rec.Size
		status = http.StatusOK
	}

	// HEAD returns metadata headers without consuming a download slot or
	// resolving vault media. Headers come from the DB record alone.
	if r.Method == http.MethodHead {
		h.setCommonHeaders(w, rec, tgutil.QueryDisposition(r))
		w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
		if hasRange {
			w.Header().Set("Content-Range", tgutil.ContentRangeValue(rng, rec.Size))
		}
		w.WriteHeader(status)
		return
	}

	// Acquire download semaphore BEFORE pool.Acquire — caps total concurrent
	// workers at 12 per bot session to prevent FLOOD_WAIT.
	effectiveThreads := h.StreamThreads
	if rec.Size < 5*1024*1024 {
		effectiveThreads = 1
		h.Log.Debug("reduced to 1 thread for small file", "token", tgutil.TokenHash(token), "size", rec.Size)
	}
	if err := h.Pool.AcquireDownloadSlots(r.Context(), effectiveThreads); err != nil {
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	defer h.Pool.ReleaseDownloadSlots(effectiveThreads)

	// Acquire a client from the pool (least-loaded).
	client, release := h.Pool.Acquire()
	if client == nil {
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "no client available", http.StatusServiceUnavailable)
		return
	}
	defer release()

	// Resolve media BEFORE setting headers so a 404/503 returns clean with
	// Cache-Control: no-store. Same client that downloads → valid file_reference.
	media, err := h.resolveVaultMedia(r.Context(), client, rec.VaultMsgID)
	if err != nil {
		// Only delete on definitive-gone errors; transient errors must not delete.
		var rpcErr *telegram.RpcError
		isGone := errors.As(err, &rpcErr) &&
			(rpcErr.Message == "MESSAGE_ID_INVALID" || rpcErr.Message == "MESSAGE_DELETED")

		w.Header().Set("Cache-Control", "no-store")

		if isGone {
			h.Log.Warn("vault message gone; deleting stale record",
				"token", tgutil.TokenHash(token), "vault_msg_id", rec.VaultMsgID)
			if delErr := h.Store.DeleteFileByHash(r.Context(), token); delErr != nil {
				h.Log.Debug("could not delete stale file record", "token", tgutil.TokenHash(token), "error", delErr)
			}
			http.NotFound(w, r)
		} else {
			h.Log.Warn("transient vault media error; returning 503",
				"token", tgutil.TokenHash(token), "error", err)
			http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		}
		return
	}

	// All headers must be set before WriteHeader.
	h.setCommonHeaders(w, rec, tgutil.QueryDisposition(r))
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	if hasRange {
		w.Header().Set("Content-Range", tgutil.ContentRangeValue(rng, rec.Size))
	}

	w.WriteHeader(status)
	h.streamBody(w, r, client, media, token, hasRange, rng, contentLength, effectiveThreads)
}

// streamBody handles both full-body and range streaming.
func (h *Handler) streamBody(w http.ResponseWriter, r *http.Request, client *pool.Client, media telegram.MessageMedia, token string, hasRange bool, rng tgutil.Range, contentLength int64, threads int) {
	flusher, _ := w.(http.Flusher)

	// Timeout scales with content length (1s/MiB, 60s floor, 60m cap) + 60s grace.
	const downloadTimeoutFloor = 60 * time.Second
	const downloadTimeoutCeiling = 60 * time.Minute
	secs := contentLength/(1<<20) + 1 // 1s per MiB, min 1s
	if secs > math.MaxInt64/int64(time.Second) {
		secs = math.MaxInt64 / int64(time.Second)
	}
	needed := time.Duration(secs) * time.Second
	if needed < downloadTimeoutFloor {
		needed = downloadTimeoutFloor
	}
	if needed > downloadTimeoutCeiling {
		needed = downloadTimeoutCeiling
	}
	timeout := needed + 60*time.Second // grace period
	dlCtx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	if hasRange {
		// Range path: fetch in 1 MiB sub-ranges aligned to StreamChunkSize.
		start := rng.Start
		end := rng.End + 1
		alignedStart := (start / StreamChunkSize) * StreamChunkSize
		skip := start - alignedStart
		written := int64(0)

		for off := alignedStart; off < end; {
			if off > int64(math.MaxInt)-int64(StreamChunkSize) {
				h.Log.Error("file too large for this build (32-bit int overflow)",
					"token", tgutil.TokenHash(token), "offset", off)
				http.Error(w, "file too large for this build", http.StatusInternalServerError)
				return
			}

			// watchdog for Background context
			ch := make(chan struct {
				data []byte
				err  error
			}, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						ch <- struct {
							data []byte
							err  error
						}{nil, fmt.Errorf("panic inside DownloadChunk: %v", r)}
					}
				}()
				// Defensive wrapper call: ensures aligned offsets and sizes
				data, _, err := client.DownloadChunkAligned(media, int(off), int(off+StreamChunkSize), StreamChunkSize)
				ch <- struct {
					data []byte
					err  error
				}{data, err}
			}()

			var data []byte
			select {
			case r := <-ch:
				if r.err != nil {
					h.Log.Warn("download chunk failed mid-stream",
						"token", tgutil.TokenHash(token), "offset", off, "error", r.err)
					return
				}
				data = r.data
			case <-dlCtx.Done():
				return
			}

			untrimmedLen := int64(len(data))
			if untrimmedLen == 0 {
				break
			}

			s := 0
			if off == alignedStart {
				s = int(skip)
			}

			if s >= len(data) {
				data = nil
			} else if int64(len(data))-int64(s) > contentLength-written {
				data = data[s : s+int(contentLength-written)]
			} else if s > 0 {
				data = data[s:]
			}

			if len(data) == 0 {
				off += untrimmedLen
				continue
			}

			if _, wErr := w.Write(data); wErr != nil {
				return // client disconnected
			}
			if flusher != nil {
				flusher.Flush()
			}
			written += int64(len(data))
			off += untrimmedLen

			if written >= contentLength {
				break
			}
		}

		if written < contentLength {
			h.Log.Warn("range download ended prematurely",
				"token", tgutil.TokenHash(token), "written", written, "contentLength", contentLength)
		}
		return
	}

	// Full-body path. StreamThreads > 1 → parallel workers via orderedWriter;
	// <= 1 → sequential path (zero overhead).
	fw := &flushWriter{w: w, f: flusher}

	if threads > 1 {
		// Cap peak memory at max(threads×chunk, 5 MiB) for true parallel throughput.
		maxBuffer := max(int64(threads)*StreamChunkSize, 5*1024*1024)
		ow := newOrderedWriter(dlCtx, fw, maxBuffer)
		defer ow.Close() // ensure cleanup even if DownloadMedia panics
		_, dErr := client.DownloadMedia(media, &telegram.DownloadOptions{
			Buffer:    ow,
			Threads:   threads,
			ChunkSize: StreamChunkSize,
			Ctx:       dlCtx,
		})
		if dErr != nil {
			h.Log.Debug("download media ended", "token", tgutil.TokenHash(token), "threads", threads, "error", dErr)
			return
		}
		return
	}

	// Sequential path: pipes 1 MiB chunks directly to the ResponseWriter via flushWriter.
	_, dErr := client.DownloadMedia(media, &telegram.DownloadOptions{
		Buffer:    fw,
		ChunkSize: StreamChunkSize,
		Ctx:       dlCtx,
	})
	if dErr != nil {
		h.Log.Debug("download media ended", "token", tgutil.TokenHash(token), "error", dErr)
		return
	}
}

// setCommonHeaders writes Content-Type, Content-Disposition, and cache
// headers. CORS headers are owned by the gateway's corsMiddleware.
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

// resolveVaultMedia fetches the vault message and returns its media. gogram's
// GetMessages has no context, so we wrap it in a goroutine and select on ctx
// to avoid pinning a bot client after the request is cancelled.
func (h *Handler) resolveVaultMedia(ctx context.Context, c *pool.Client, vaultMsgID int32) (telegram.MessageMedia, error) {
	type result struct {
		msgs []telegram.NewMessage
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		msgs, err := c.GetMessages(h.Ingester.VaultChannelID(), &telegram.SearchOption{IDs: int(vaultMsgID)})
		ch <- result{msgs: msgs, err: err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		if len(r.msgs) == 0 || r.msgs[0].Message == nil {
			return nil, errors.New("vault message not found")
		}
		media := r.msgs[0].Media()
		if media == nil {
			return nil, errors.New("vault message has no media")
		}
		return media, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
	n, e := fw.w.Write(p)
	if fw.f != nil {
		fw.f.Flush()
	}
	return n, e
}

// StreamChunkSize is the size of each chunk downloaded from Telegram.
// 1 MiB balances memory usage and throughput.
const StreamChunkSize = 1 << 20

// orderedWriter adapts gogram's parallel WriteAt (out-of-order chunks) into
// the sequential io.Writer that http.ResponseWriter requires. It buffers
// out-of-order chunks in a map keyed by offset, flushes them in-order as the
// next expected offset arrives, and applies backpressure via a bounded
// pending-byte budget (maxBuffer). Only used on the full-body path with
// StreamThreads > 1.
var _ io.WriterAt = (*orderedWriter)(nil) // compile-time interface check

type orderedWriter struct {
	w            io.Writer // underlying http.ResponseWriter (sequential)
	mu           sync.Mutex
	nextOff      int64 // next byte offset we expect to write to w
	pending      map[int64][]byte
	pendingBytes int64
	maxBuffer    int64
	notFull      *sync.Cond // signaled when pendingBytes shrinks or on error/close
	closed       bool
	err          error         // sticky error: once set, all future WriteAt calls fail
	closeCh      chan struct{} // signals watchdog goroutine to unblock on Close()
}

// newOrderedWriter constructs an adapter. When ctx cancels, Close is called
// automatically so gogram's workers stop fetching promptly.
func newOrderedWriter(ctx context.Context, w io.Writer, maxBuffer int64) *orderedWriter {
	ow := &orderedWriter{
		w:         w,
		pending:   make(map[int64][]byte),
		maxBuffer: maxBuffer,
		closeCh:   make(chan struct{}),
	}
	ow.notFull = sync.NewCond(&ow.mu)
	// Cancel producers the instant the HTTP client disconnects or Close is called.
	go func() {
		select {
		case <-ctx.Done():
			ow.Close()
		case <-ow.closeCh:
		}
	}()
	return ow
}

// WriteAt implements io.WriterAt. Fast path writes straight through when off
// matches nextOff; otherwise buffers a copy. Backpressure blocks on notFull
// when pendingBytes >= maxBuffer. The fast path is exempt from backpressure —
// blocking it would deadlock (chunk 0 drains the buffer).
func (ow *orderedWriter) WriteAt(p []byte, off int64) (int, error) {
	ow.mu.Lock()
	defer ow.mu.Unlock()

	// Fast path: chunk at exactly the expected offset — write straight through.
	// Must complete before returning (gogram reuses its buffer on return).
	if off == ow.nextOff {
		return ow.writeFastPath(p)
	}

	// Slow path: out-of-order chunk that needs buffering. Backpressure blocks
	// until space frees or the writer closes/errors.
	for ow.pendingBytes >= ow.maxBuffer && !ow.closed && ow.err == nil {
		ow.notFull.Wait()
	}
	if ow.closed || ow.err != nil {
		return 0, ow.err
	}

	// Re-check fast path after waiting: another writer may have advanced nextOff.
	if off == ow.nextOff {
		return ow.writeFastPath(p)
	}

	// Out of order — buffer a COPY (gogram reuses its slice on return).
	chunk := make([]byte, len(p))
	copy(chunk, p)
	ow.pending[off] = chunk
	ow.pendingBytes += int64(len(chunk))
	return len(p), nil
}

// writeFastPath writes p directly to the underlying writer, then drains
// contiguous buffered chunks. MUST be called with ow.mu held. On error sets
// ow.err, broadcasts to wake blocked producers, and propagates the error so
// gogram's worker exits immediately.
func (ow *orderedWriter) writeFastPath(p []byte) (int, error) {
	if ow.closed || ow.err != nil {
		return 0, ow.err
	}
	n, err := ow.w.Write(p)
	ow.nextOff += int64(n)
	// Short-write + sticky-error: intentional pattern. Once ow.err is set all
	// future WriteAt calls fail immediately, preventing silent data loss.
	if n < len(p) {
		if err == nil {
			err = io.ErrShortWrite
		}
		ow.err = err
		ow.notFull.Broadcast() // wake blocked producers so they see ow.err
		return n, err
	}
	if err != nil {
		ow.err = err
		ow.notFull.Broadcast() // wake blocked producers
		return n, err
	}
	ow.flushContiguous()
	// Propagate flushContiguous errors so gogram's worker exits now, not next WriteAt.
	if ow.err != nil {
		return n, ow.err
	}
	return n, nil
}

// flushContiguous drains buffered chunks whose offsets now match nextOff.
// MUST be called with ow.mu held. On error sets ow.err, broadcasts, returns.
func (ow *orderedWriter) flushContiguous() {
	for {
		chunk, ok := ow.pending[ow.nextOff]
		if !ok {
			break
		}
		delete(ow.pending, ow.nextOff)
		ow.pendingBytes -= int64(len(chunk))
		n, err := ow.w.Write(chunk)
		ow.nextOff += int64(n)
		if n < len(chunk) {
			if err == nil {
				err = io.ErrShortWrite
			}
			ow.err = err
			ow.notFull.Broadcast()
			return
		}
		if err != nil {
			ow.err = err
			ow.notFull.Broadcast() // wake ALL blocked producers so they see ow.err
			return
		}
		// Wake one producer blocked on backpressure (one slot freed).
		ow.notFull.Signal()
	}
}

// Close marks the adapter closed and wakes all blocked producers. Sets
// ow.err = io.ErrClosedPipe so gogram's workers see non-nil and stop fetching
// (nil would let them treat (0, nil) as a successful zero-byte write). A
// previously-recorded error is preserved so callers see the root cause.
func (ow *orderedWriter) Close() error {
	ow.mu.Lock()
	defer ow.mu.Unlock()
	if !ow.closed {
		ow.closed = true
		if ow.err == nil {
			ow.err = io.ErrClosedPipe
		}
		ow.notFull.Broadcast() // wake all blocked producers
		close(ow.closeCh)      // unblock watchdog goroutine
	}
	return ow.err
}
