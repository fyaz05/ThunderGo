// Package stream implements the HTTP file handler. It resolves a vault copy
// with the selected Telegram client and pipes its bytes to the HTTP socket
// without intermediate disk writes.
package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	gogram "github.com/amarnathcjd/gogram"
	"github.com/amarnathcjd/gogram/telegram"

	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// Resolver sentinels separate a permanently stale vault record from a
// transient Telegram/network failure. A stale record must be evicted; a
// transient error must not destroy a still-valid public link.
var (
	ErrVaultMessageMissing = errors.New("vault message missing")
	ErrVaultMessageNoMedia = errors.New("vault message has no media")
	ErrVaultRecordMismatch = errors.New("vault media does not match file record")
)

const (
	overloadRetryAfterSeconds = 2
	vaultLookupTimeout        = 30 * time.Second
)

// Handler is the HTTP file-streaming handler.
type Handler struct {
	Pool     *pool.Pool
	Store    *store.Store
	Log      *slog.Logger
	Ingester *ingest.Ingester
}

// New wires the sequential FileToLink-compatible serving path. One admitted
// HTTP file request always uses one sequential Telegram download; it never
// splits a file into parallel Telegram workers.
func New(p *pool.Pool, s *store.Store, in *ingest.Ingester, log *slog.Logger) *Handler {
	return &Handler{Pool: p, Store: s, Log: log, Ingester: in}
}

// ServeHTTP resolves a stored file record, reserves one stream ticket on a
// Telegram client, and serves either metadata or bytes. The ticket is acquired
// before route-specific work and is released exactly once on every path.
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

	// Store uses a non-blocking touch buffer in production, so this does not
	// delay bytes. Seen counters are advisory and must never affect streaming.
	if err := h.Store.IncrementSeenCount(r.Context(), rec.FileKey); err != nil {
		h.Log.Debug("incrementing seen count", "token", tgutil.TokenHash(token), "error", err)
	}

	// Match FileToLink's admission lifecycle: choose and reserve a client before
	// request-specific work. Do not queue work on an already saturated client.
	client, release := h.Pool.AcquireBest()
	if client == nil {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Retry-After", strconv.Itoa(overloadRetryAfterSeconds))
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	defer release()

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
	status := http.StatusOK
	if hasRange {
		contentLength = rng.End - rng.Start + 1
		status = http.StatusPartialContent
	} else {
		contentLength = rec.Size
	}

	// DownloadChunkCtx accepts int offsets. Reject an unsupported Range before
	// headers are committed, rather than trying to write an HTTP error mid-body.
	if hasRange && (rng.Start > maxNativeInt() || rng.End > maxNativeInt()) {
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "file range is unsupported on this build", http.StatusInternalServerError)
		return
	}

	// HEAD intentionally uses stored metadata only, as FileToLink does. It still
	// owns a short-lived stream ticket so load accounting remains consistent.
	if r.Method == http.MethodHead {
		h.setCommonHeaders(w, rec, tgutil.QueryDisposition(r))
		w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
		if hasRange {
			w.Header().Set("Content-Range", tgutil.ContentRangeValue(rng, rec.Size))
		}
		w.WriteHeader(status)
		return
	}

	// Resolve and validate the vault media with the same client that will
	// download it. This supplies a fresh file reference for that Telegram
	// session and prevents a corrupt DB row from serving unrelated media.
	media, err := h.resolveVaultMedia(r.Context(), client, rec)
	if err != nil {
		w.Header().Set("Cache-Control", "no-store")
		if isPermanentVaultError(err) {
			h.Log.Warn("stale vault file record", "token", tgutil.TokenHash(token), "vault_msg_id", rec.VaultMsgID, "error", err)
			deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if delErr := h.Store.DeleteFileByHash(deleteCtx, token); delErr != nil {
				h.Log.Debug("could not delete stale file record", "token", tgutil.TokenHash(token), "error", delErr)
			}
			cancel()
			http.NotFound(w, r)
			return
		}
		h.Log.Warn("transient vault media error", "token", tgutil.TokenHash(token), "error", err)
		w.Header().Set("Retry-After", strconv.Itoa(overloadRetryAfterSeconds))
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	h.setCommonHeaders(w, rec, tgutil.QueryDisposition(r))
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	if hasRange {
		w.Header().Set("Content-Range", tgutil.ContentRangeValue(rng, rec.Size))
	}
	w.WriteHeader(status)
	h.streamBody(w, r, client, media, rec, token, hasRange, rng, contentLength)
}

// streamBody sends the requested bytes sequentially. It relies on the request
// context plus gogram's per-RPC timeouts/retries; there is deliberately no
// short whole-transfer deadline that can truncate a healthy slow large file.
func (h *Handler) streamBody(w http.ResponseWriter, r *http.Request, client *pool.Client, media telegram.MessageMedia, rec *store.FileRecord, token string, hasRange bool, rng tgutil.Range, contentLength int64) {
	flusher, _ := w.(http.Flusher)
	dlCtx := r.Context()
	refetch := func() (any, error) {
		return h.resolveVaultMedia(dlCtx, client, rec)
	}

	if hasRange {
		// Match FileToLink's serving method: Telegram is read in 1 MiB
		// chunk-aligned windows, then leading/trailing bytes are trimmed for
		// the exact HTTP range. Besides matching PyroFork stream_media(), this
		// keeps CDN requests on the same stable chunk boundaries as its hashes.
		wireStart := int((rng.Start / StreamChunkSize) * StreamChunkSize)
		skip := int(rng.Start - int64(wireStart))
		written := int64(0)
		consecutiveFails := 0
		for written < contentLength {
			if int64(wireStart) > maxNativeInt()-int64(StreamChunkSize) {
				h.Log.Warn("range offset overflow before chunk fetch", "token", tgutil.TokenHash(token), "offset", wireStart)
				return
			}
			wireEnd := wireStart + StreamChunkSize
			raw, _, dErr := client.DownloadChunkCtx(dlCtx, media, wireStart, wireEnd, StreamChunkSize,
				&telegram.DownloadOptions{RefetchFileReference: refetch})
			if dErr != nil {
				consecutiveFails++
				h.Log.Warn("range chunk failed", "token", tgutil.TokenHash(token), "offset", wireStart, "error", dErr, "fails", consecutiveFails)
				if consecutiveFails >= maxConsecutiveChunkFails {
					return
				}
				continue
			}
			consecutiveFails = 0
			if len(raw) == 0 {
				break
			}

			rawLen := len(raw)
			if skip >= rawLen {
				skip -= rawLen
				wireStart += rawLen
				continue
			}
			data := raw[skip:]
			skip = 0
			remaining := contentLength - written
			if int64(len(data)) > remaining {
				data = data[:remaining]
			}
			if len(data) > 0 {
				if _, err := w.Write(data); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
				written += int64(len(data))
			}
			wireStart += rawLen
		}
		if written < contentLength {
			h.Log.Warn("range download ended prematurely", "token", tgutil.TokenHash(token), "written", written, "content_length", contentLength)
		}
		return
	}

	fw := &flushWriter{w: w, f: flusher}
	_, err := client.DownloadMedia(media, &telegram.DownloadOptions{
		Buffer:               fw,
		Threads:              1,
		ChunkSize:            StreamChunkSize,
		Ctx:                  dlCtx,
		RefetchFileReference: refetch,
	})
	if err != nil {
		h.Log.Debug("sequential media download ended", "token", tgutil.TokenHash(token), "error", err)
	}
}

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

// resolveVaultMedia fetches the stored message using the selected download
// client. GetMessages has no context-aware variant; the buffered result channel
// lets a late lookup exit and the per-client lookup semaphore bounds detached
// calls after an HTTP timeout.
func (h *Handler) resolveVaultMedia(ctx context.Context, c *pool.Client, rec *store.FileRecord) (telegram.MessageMedia, error) {
	if h.Ingester == nil || rec == nil {
		return nil, ErrVaultMessageMissing
	}
	type result struct {
		msgs []telegram.NewMessage
		err  error
	}
	lookupCtx, cancel := context.WithTimeout(ctx, vaultLookupTimeout)
	defer cancel()
	releaseLookup, ok := c.AcquireLookup(lookupCtx)
	if !ok {
		return nil, lookupCtx.Err()
	}
	ch := make(chan result, 1)
	go func() {
		defer releaseLookup()
		msgs, err := c.GetMessages(h.Ingester.VaultChannelID(), &telegram.SearchOption{IDs: int(rec.VaultMsgID)})
		ch <- result{msgs: msgs, err: err}
	}()

	select {
	case got := <-ch:
		if got.err != nil {
			return nil, got.err
		}
		if len(got.msgs) == 0 || got.msgs[0].Message == nil {
			return nil, ErrVaultMessageMissing
		}
		msg := &got.msgs[0]
		media := msg.Media()
		if media == nil {
			return nil, ErrVaultMessageNoMedia
		}
		if rec.FileKey != "" && tgutil.FileKey(msg) != rec.FileKey {
			return nil, ErrVaultRecordMismatch
		}
		// FileKey includes document size but photo keys do not. Verify the stored
		// response length independently so Content-Length cannot describe a
		// different vault object and leave clients stalled near completion.
		if actualSize := tgutil.ExtractSize(msg); actualSize <= 0 || actualSize != rec.Size {
			return nil, ErrVaultRecordMismatch
		}
		return media, nil
	case <-lookupCtx.Done():
		return nil, lookupCtx.Err()
	}
}

func isPermanentVaultError(err error) bool {
	if errors.Is(err, ErrVaultMessageMissing) || errors.Is(err, ErrVaultMessageNoMedia) || errors.Is(err, ErrVaultRecordMismatch) {
		return true
	}
	var codeErr *gogram.ErrResponseCode
	if errors.As(err, &codeErr) {
		return codeErr.Message == "MESSAGE_ID_INVALID" || codeErr.Message == "MESSAGE_DELETED"
	}
	var rpcErr *telegram.RpcError
	if errors.As(err, &rpcErr) {
		return rpcErr.Message == "MESSAGE_ID_INVALID" || rpcErr.Message == "MESSAGE_DELETED"
	}
	return false
}

func maxNativeInt() int64 {
	return int64(^uint(0) >> 1)
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

// StreamChunkSize is the Telegram transfer chunk size used by FileToLink.
const StreamChunkSize = 1 << 20

// maxConsecutiveChunkFails is one because gogram already retries each part
// internally. Retrying that exhausted cycle at the HTTP layer is what makes a
// browser appear stuck at 99% instead of receiving a prompt failed transfer
// that it can reconnect/resume.
const maxConsecutiveChunkFails = 1
