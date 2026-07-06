// Package stream is white-box tested here to access unexported helpers
// (setCommonHeaders, tokenFromContext, WithToken, ctxKeyToken, StreamChunkSize,
// orderedWriter, newOrderedWriter).
//
// The full ServeHTTP flow depends on pool.Pool (Telegram) and store.Store
// (MongoDB), so it is not exercised end-to-end here. Instead, this file tests
// the paths that short-circuit BEFORE needing those dependencies:
//
//   - WithToken / tokenFromContext context-plumbing round-trip
//   - ServeHTTP with a missing or empty token → 404 (returns before any
//     Store/Pool access, so a nil Store/Pool is safe)
//   - setCommonHeaders (called directly with a *store.FileRecord and an
//     httptest.ResponseRecorder — touches neither Pool nor Store)
//   - StreamChunkSize constant (spec §3.9: 1 MiB)
//   - orderedWriter adapter (audit §3.1-3.6) — exercised directly with mock
//     writers; no Telegram/HTTP/Store dependencies.
package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// discardLogger returns a slog.Logger that writes to io.Discard. Used so the
// handler never nil-panics on h.Log even on paths the test does not expect to
// reach.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- context plumbing ---

// TestWithTokenTokenFromContext verifies the round-trip: a token set via
// WithToken is retrievable via tokenFromContext, a context that never had a
// token returns "", and the token survives nesting inside derived contexts
// (cancel / timeout / value).
func TestWithTokenTokenFromContext(t *testing.T) {
	t.Parallel()
	t.Run("round_trip", func(t *testing.T) {
		ctx := WithToken(context.Background(), "abc123")
		if got := tokenFromContext(ctx); got != "abc123" {
			t.Errorf("tokenFromContext = %q, want %q", got, "abc123")
		}
	})

	t.Run("no_token_returns_empty", func(t *testing.T) {
		if got := tokenFromContext(context.Background()); got != "" {
			t.Errorf("tokenFromContext(empty ctx) = %q, want %q", got, "")
		}
	})

	t.Run("empty_token_returns_empty", func(t *testing.T) {
		ctx := WithToken(context.Background(), "")
		if got := tokenFromContext(ctx); got != "" {
			t.Errorf("tokenFromContext(empty token) = %q, want %q", got, "")
		}
	})

	t.Run("survives_nested_cancel_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(WithToken(context.Background(), "tck-xyz"))
		defer cancel()
		if got := tokenFromContext(ctx); got != "tck-xyz" {
			t.Errorf("tokenFromContext after WithCancel = %q, want %q", got, "tck-xyz")
		}
	})

	t.Run("survives_nested_timeout_context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(WithToken(context.Background(), "tck-tmeaut"), 0)
		defer cancel()
		// Even though the timeout is 0 (already expired), the token value
		// must still be present in the derived context.
		if got := tokenFromContext(ctx); got != "tck-tmeaut" {
			t.Errorf("tokenFromContext after WithTimeout = %q, want %q", got, "tck-tmeaut")
		}
	})

	t.Run("survives_nested_value_context", func(t *testing.T) {
		type k int
		ctx := context.WithValue(WithToken(context.Background(), "tck-vx"), k(1), "other")
		if got := tokenFromContext(ctx); got != "tck-vx" {
			t.Errorf("tokenFromContext after WithValue = %q, want %q", got, "tck-vx")
		}
	})

	t.Run("does_not_collide_with_other_value_keys", func(t *testing.T) {
		type otherKey int
		ctx := context.WithValue(WithToken(context.Background(), "reau-tckn"), otherKey(1), "decoy")
		if got := tokenFromContext(ctx); got != "reau-tckn" {
			t.Errorf("tokenFromContext with decoy value = %q, want %q", got, "reau-tckn")
		}
	})
}

// --- ServeHTTP: token-missing / token-empty short-circuits ---

// TestServeHTTP_MissingToken constructs a Handler with nil Store/Pool/Ingester
// (safe: the empty-token branch returns BEFORE touching any of them) and
// verifies that a request with NO token in its context yields 404.
func TestServeHTTP_MissingToken(t *testing.T) {
	t.Parallel()
	h := &Handler{Log: discardLogger()} // Store, Pool, Ingester all nil
	req := httptest.NewRequest(http.MethodGet, "/f//file.mp4/raw", nil)
	// Plain context — no WithToken applied (simulates chi router not matching).
	req = req.WithContext(context.Background())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d (missing token must 404 before touching Store)", rec.Code, http.StatusNotFound)
	}
	if body := rec.Body.String(); body == "" {
		t.Errorf("expected non-empty 404 body, got empty")
	}
}

// TestServeHTTP_EmptyToken verifies that an explicitly-empty token (e.g. from
// WithToken(ctx, "")) still short-circuits to 404 without touching Store.
func TestServeHTTP_EmptyToken(t *testing.T) {
	t.Parallel()
	h := &Handler{Log: discardLogger()}
	req := httptest.NewRequest(http.MethodGet, "/f//file.mp4/raw", nil)
	req = req.WithContext(WithToken(context.Background(), ""))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d (empty token must 404 before touching Store)", rec.Code, http.StatusNotFound)
	}
}

// TestServeHTTP_NilStoreWithTokenDoesNotPanicOnMissingToken is a belt-and-
// braces check that the empty-token branch is reached before the nil-Store
// dereference. If the handler were to dereference h.Store with the token set,
// it would panic; this test ensures we DON'T set the token here, so the
// short-circuit fires first. (Documented contract: ServeHTTP step 1 runs
// before step 2.)
func TestServeHTTP_NilStoreWithTokenDoesNotPanicOnMissingToken(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ServeHTTP panicked on missing-token path before reaching nil Store: %v", r)
		}
	}()
	h := &Handler{Log: discardLogger()} // Store nil deliberately
	req := httptest.NewRequest(http.MethodHead, "/f/anything/x.bin/raw", nil)
	req = req.WithContext(context.Background()) // no token

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// --- setCommonHeaders (called directly; never touches Pool/Store/Ingester) ---

// TestSetCommonHeaders verifies the full set of immutable response headers for
// the common cases (attachment + inline, with explicit MIME and a fallback to
// application/octet-stream when MIME is empty).
func TestSetCommonHeaders(t *testing.T) {
	t.Parallel()
	// A zero-value *Handler is safe for setCommonHeaders — the method only
	// reads its receiver to call tgutil.DispositionHeader; it never touches
	// Pool/Store/Ingester/Log.
	h := &Handler{}

	tests := []struct {
		name        string
		rec         *store.FileRecord
		disposition string
		wantType    string
		wantDisp    string
	}{
		{
			name:        "attachment_video",
			rec:         &store.FileRecord{FileName: "movie.mp4", MimeType: "video/mp4", Size: 1024},
			disposition: "attachment",
			wantType:    "video/mp4",
			wantDisp:    `attachment; filename="movie.mp4"; filename*=UTF-8''movie.mp4`,
		},
		{
			name:        "inline_audio",
			rec:         &store.FileRecord{FileName: "audio.mp3", MimeType: "audio/mpeg", Size: 5000},
			disposition: "inline",
			wantType:    "audio/mpeg",
			wantDisp:    `inline; filename="audio.mp3"; filename*=UTF-8''audio.mp3`,
		},
		{
			name:        "attachment_audio_same_record_as_inline",
			rec:         &store.FileRecord{FileName: "audio.mp3", MimeType: "audio/mpeg", Size: 5000},
			disposition: "attachment",
			wantType:    "audio/mpeg",
			wantDisp:    `attachment; filename="audio.mp3"; filename*=UTF-8''audio.mp3`,
		},
		{
			name:        "empty_mime_fallback_octet_stream_inline",
			rec:         &store.FileRecord{FileName: "file.bin", MimeType: "", Size: 100},
			disposition: "inline",
			wantType:    "application/octet-stream",
			wantDisp:    `inline; filename="file.bin"; filename*=UTF-8''file.bin`,
		},
		{
			name:        "empty_mime_fallback_octet_stream_attachment",
			rec:         &store.FileRecord{FileName: "file.bin", MimeType: "", Size: 100},
			disposition: "attachment",
			wantType:    "application/octet-stream",
			wantDisp:    `attachment; filename="file.bin"; filename*=UTF-8''file.bin`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.setCommonHeaders(rec, tt.rec, tt.disposition)
			hdr := rec.Header()

			// Content-Type (with fallback when empty).
			if got := hdr.Get("Content-Type"); got != tt.wantType {
				t.Errorf("Content-Type = %q, want %q", got, tt.wantType)
			}

			// Content-Disposition (exact match — tgutil.DispositionHeader is
			// tested separately in util_test.go; here we just confirm the
			// handler wires it through unchanged).
			if got := hdr.Get("Content-Disposition"); got != tt.wantDisp {
				t.Errorf("Content-Disposition = %q, want %q", got, tt.wantDisp)
			}

			// Static immutable headers (same for every record / disposition).
			if got := hdr.Get("Accept-Ranges"); got != "bytes" {
				t.Errorf("Accept-Ranges = %q, want %q", got, "bytes")
			}
			if got := hdr.Get("Cache-Control"); got != "public, max-age=31536000" {
				t.Errorf("Cache-Control = %q, want %q", got, "public, max-age=31536000")
			}
			if got := hdr.Get("Connection"); got != "keep-alive" {
				t.Errorf("Connection = %q, want %q", got, "keep-alive")
			}
			if got := hdr.Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options = %q, want %q", got, "nosniff")
			}
		})
	}
}

// TestSetCommonHeaders_NonASCIIFilename verifies that a non-ASCII filename
// produces a percent-encoded filename* parameter per RFC 5987/6266, while the
// ASCII fallback is sanitized (the non-ASCII bytes are replaced with
// underscores). This is a quick sanity check that the handler does not strip
// or mangle the extended form — the encoding itself is covered in util tests.
func TestSetCommonHeaders_NonASCIIFilename(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	rec := &store.FileRecord{FileName: "фильм.mp4", MimeType: "video/mp4", Size: 1}

	w := httptest.NewRecorder()
	h.setCommonHeaders(w, rec, "attachment")

	disp := w.Header().Get("Content-Disposition")
	// Extended form must be present and start with `attachment; filename="`;
	// `filename*=` must include `UTF-8''` followed by percent-encoded bytes.
	if !startsWith(disp, `attachment; filename="`) {
		t.Errorf("Content-Disposition missing attachment prefix: %q", disp)
	}
	if !contains(disp, "filename*=UTF-8''") {
		t.Errorf("Content-Disposition missing RFC 5987 extended form: %q", disp)
	}
	// The raw non-ASCII filename must NOT appear verbatim in the header —
	// the extended form percent-encodes every non-attr-char byte.
	if contains(disp, "фильм") {
		t.Errorf("Content-Disposition leaks raw non-ASCII filename: %q", disp)
	}
	// Content-Type must still be the explicit video/mp4 (not fallback).
	if got := w.Header().Get("Content-Type"); got != "video/mp4" {
		t.Errorf("Content-Type = %q, want %q", got, "video/mp4")
	}
}

// TestSetCommonHeaders_DoesNotSetCORSHeaders confirms that setCommonHeaders
// does NOT emit any Access-Control-* headers — those are owned exclusively by
// the gateway's corsMiddleware (spec §3.10). Re-setting them here would be
// redundant and could mask a future gateway-side CORS change.
func TestSetCommonHeaders_DoesNotSetCORSHeaders(t *testing.T) {
	t.Parallel()
	h := &Handler{}
	rec := &store.FileRecord{FileName: "x.mp4", MimeType: "video/mp4", Size: 1}

	w := httptest.NewRecorder()
	h.setCommonHeaders(w, rec, "attachment")

	for _, hdr := range []string{
		"Access-Control-Allow-Origin",
		"Access-Control-Allow-Methods",
		"Access-Control-Allow-Headers",
		"Access-Control-Expose-Headers",
		"Access-Control-Max-Age",
		"Access-Control-Allow-Credentials",
	} {
		if v := w.Header().Get(hdr); v != "" {
			t.Errorf("setCommonHeaders must not set %s, got %q (CORS is owned by gateway)", hdr, v)
		}
	}
}

// --- StreamChunkSize constant (spec §3.9) ---

// TestStreamChunkSize verifies the chunk size is exactly 1 MiB (1<<20 bytes).
// The handler's streaming memory profile (~1 MiB per concurrent stream) and
// the download-timeout formula (1s per MiB) both depend on this value.
func TestStreamChunkSize(t *testing.T) {
	t.Parallel()
	const oneMiB = 1 << 20 // 1048576 bytes
	if StreamChunkSize != oneMiB {
		t.Errorf("StreamChunkSize = %d, want %d (1 MiB per spec §3.9)", StreamChunkSize, oneMiB)
	}
	if StreamChunkSize <= 0 {
		t.Errorf("StreamChunkSize must be positive, got %d", StreamChunkSize)
	}
}

// --- helpers ---

// startsWith is a tiny alias to avoid pulling strings into the test just for
// two prefix checks. (Keeps the import list minimal and the test file
// self-contained.)
func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// contains is a minimal substring check; same rationale as startsWith.
func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Ensure util is referenced (used in production code via setCommonHeaders ->
// tgutil.DispositionHeader); this keeps the import live in case future tests
// want to compute expected disposition values via tgutil.DispositionHeader
// directly instead of hardcoding them.
var _ = tgutil.TokenHash

// --- orderedWriter tests (audit §3.1-3.6) ---
//
// The orderedWriter is exercised directly with mock writers — no Telegram,
// no HTTP server, no Store. Each test picks the smallest maxBuffer that
// still demonstrates the behavior under test (backpressure tests use a
// tight buffer; correctness tests use an unbounded one so the test fails
// for the RIGHT reason).
//
// Conventions:
//   - chunk contents are unique per offset (chunk[i] = byte('a' + i)) so a
//     misordered flush is immediately visible in the output bytes.
//   - we always `defer ow.Close()` to mirror production usage (audit §3.6).
//   - tests that exercise blocking paths use `time.After(...)` in a select
//     as their own deadlock backstop rather than a context timeout — this
//     avoids the audit §3.4 watchdog firing spuriously mid-test and
//     clobbering ow.err with io.ErrClosedPipe.

// newTestOrderedWriter builds an adapter bound to a cancellable context. The
// caller is responsible for cancelling (typically via `defer cancel()`) —
// there is NO timeout so the audit §3.4 watchdog does NOT fire spuriously
// during a long test and clobber ow.err with io.ErrClosedPipe. Tests that
// need a deadlock backstop use their own `time.After(...)` in select
// statements, which produces a clearer failure message than the watchdog.
func newTestOrderedWriter(t *testing.T, w io.Writer, maxBuffer int64) (*orderedWriter, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	return newOrderedWriter(ctx, w, maxBuffer), cancel
}

// makeChunk returns a chunk of size n filled with the byte b. Used so each
// offset's chunk has a recognizable fingerprint in the output buffer.
func makeChunk(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

// TestOrderedWriter_InOrder verifies the simplest case: gogram's workers
// happen to deliver chunks at offsets 0, N, 2N, 3N in order. Every chunk
// should take the fast path (off == ow.nextOff) and be written straight
// through — nothing should land in the pending map.
func TestOrderedWriter_InOrder(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	ow, cancel := newTestOrderedWriter(t, &buf, 1<<20) // 1 MiB — plenty
	defer cancel()
	defer ow.Close()

	chunkA := makeChunk('A', 100)
	chunkB := makeChunk('B', 100)
	chunkC := makeChunk('C', 100)

	// In-order: offsets 0, 100, 200.
	if n, err := ow.WriteAt(chunkA, 0); n != 100 || err != nil {
		t.Fatalf("WriteAt(A,0) = (%d, %v), want (100, nil)", n, err)
	}
	if n, err := ow.WriteAt(chunkB, 100); n != 100 || err != nil {
		t.Fatalf("WriteAt(B,100) = (%d, %v), want (100, nil)", n, err)
	}
	if n, err := ow.WriteAt(chunkC, 200); n != 100 || err != nil {
		t.Fatalf("WriteAt(C,200) = (%d, %v), want (100, nil)", n, err)
	}

	// Nothing should be buffered (all fast-path hits).
	ow.mu.Lock()
	pendingLen := len(ow.pending)
	pendingBytes := ow.pendingBytes
	nextOff := ow.nextOff
	ow.mu.Unlock()
	if pendingLen != 0 {
		t.Errorf("pending map = %d entries, want 0 (all chunks should fast-path)", pendingLen)
	}
	if pendingBytes != 0 {
		t.Errorf("pendingBytes = %d, want 0", pendingBytes)
	}
	if nextOff != 300 {
		t.Errorf("nextOff = %d, want 300", nextOff)
	}

	// Output must be exactly A||B||C — in-order delivery means no reordering.
	want := strings.Repeat("A", 100) + strings.Repeat("B", 100) + strings.Repeat("C", 100)
	if got := buf.String(); got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

// TestOrderedWriter_FastPath is the surgical version of InOrder: a single
// chunk at offset 0 must take the fast path (no buffering). Verifies the
// fast-path branch in isolation, with no preceding state.
func TestOrderedWriter_FastPath(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	ow, cancel := newTestOrderedWriter(t, &buf, 1<<20)
	defer cancel()
	defer ow.Close()

	chunk := makeChunk('X', 250)
	n, err := ow.WriteAt(chunk, 0)
	if n != 250 || err != nil {
		t.Fatalf("WriteAt(chunk,0) = (%d, %v), want (250, nil)", n, err)
	}

	ow.mu.Lock()
	pendingLen := len(ow.pending)
	ow.mu.Unlock()
	if pendingLen != 0 {
		t.Errorf("pending = %d entries, want 0 (fast path must not buffer)", pendingLen)
	}
	if got := buf.String(); got != strings.Repeat("X", 250) {
		t.Errorf("output = %q, want 250 'X'", got)
	}
}

// TestOrderedWriter_OutOfOrder is the central correctness test: chunks
// arrive in arbitrary order (3,1,0,2), but the output stream must be
// delivered 0,1,2,3. Confirms both buffering (chunks 3,1 buffered when 0
// is missing) and the flushContiguous cascade (once 0 arrives, 0→1→2→3
// all flush in one pass).
func TestOrderedWriter_OutOfOrder(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	ow, cancel := newTestOrderedWriter(t, &buf, 1<<20)
	defer cancel()
	defer ow.Close()

	const size = 64
	chunks := [][]byte{
		makeChunk('0', size), // offset 0
		makeChunk('1', size), // offset 64
		makeChunk('2', size), // offset 128
		makeChunk('3', size), // offset 192
	}
	offsets := []int64{0, 64, 128, 192}

	// Deliver in order: 3, 1, 0, 2.
	// - WriteAt(3, 192): 192 != 0 → buffer.
	// - WriteAt(1, 64):  64  != 0 → buffer.
	// - WriteAt(0, 0):   0   == 0 → fast-path write, then flushContiguous
	//                   should immediately also flush 1 (offset 64 now
	//                   matches nextOff=64).
	// - WriteAt(2, 128): 128 != 192 (nextOff is now 128 after flushing 0+1)
	//                   → buffer. WAIT — after flushing 0 and 1, nextOff=128.
	//                   So WriteAt(2,128) hits the fast path too. Then
	//                   flushContiguous immediately flushes 3 (offset 192,
	//                   but nextOff is 192 → wait, no: after writing 2
	//                   nextOff=192, so chunk 3 at offset 192 flushes too).
	if n, err := ow.WriteAt(chunks[3], offsets[3]); n != size || err != nil {
		t.Fatalf("WriteAt(3,192) = (%d, %v), want (%d, nil)", n, err, size)
	}
	if n, err := ow.WriteAt(chunks[1], offsets[1]); n != size || err != nil {
		t.Fatalf("WriteAt(1,64) = (%d, %v), want (%d, nil)", n, err, size)
	}
	if n, err := ow.WriteAt(chunks[0], offsets[0]); n != size || err != nil {
		t.Fatalf("WriteAt(0,0) = (%d, %v), want (%d, nil)", n, err, size)
	}
	// After 0 lands, flushContiguous should have flushed chunk 1 too
	// (offset 64 now matches nextOff=64). So pending should hold ONLY
	// chunk 3 (offset 192), and nextOff should be 128.
	ow.mu.Lock()
	if got := len(ow.pending); got != 1 {
		t.Errorf("after WriteAt(0,0): pending = %d entries, want 1 (only chunk 3)", got)
	}
	if got := ow.nextOff; got != 128 {
		t.Errorf("after WriteAt(0,0): nextOff = %d, want 128", got)
	}
	ow.mu.Unlock()

	if n, err := ow.WriteAt(chunks[2], offsets[2]); n != size || err != nil {
		t.Fatalf("WriteAt(2,128) = (%d, %v), want (%d, nil)", n, err, size)
	}
	// After 2 lands at offset 128 (== nextOff), fast-path write; then
	// flushContiguous should immediately flush chunk 3 (offset 192 now
	// matches nextOff=192). Pending should be EMPTY and nextOff=256.
	ow.mu.Lock()
	pendingLen := len(ow.pending)
	nextOff := ow.nextOff
	ow.mu.Unlock()
	if pendingLen != 0 {
		t.Errorf("after WriteAt(2,128): pending = %d entries, want 0 (cascade flush)", pendingLen)
	}
	if nextOff != 256 {
		t.Errorf("after WriteAt(2,128): nextOff = %d, want 256", nextOff)
	}

	want := strings.Repeat("0", size) + strings.Repeat("1", size) +
		strings.Repeat("2", size) + strings.Repeat("3", size)
	if got := buf.String(); got != want {
		t.Errorf("output misordered:\n  got  = %q\n  want = %q", got, want)
	}
}

// slowWriter is an io.Writer that sleeps a fixed duration per Write call.
// Used by TestOrderedWriter_BufferFull to slow the consumer so producers
// pile up against maxBuffer and block.
type slowWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	delay time.Duration
	calls int32
}

func (s *slowWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	atomic.AddInt32(&s.calls, 1)
	time.Sleep(s.delay)
	return s.buf.Write(p)
}

// TestOrderedWriter_BufferFull verifies backpressure: when pendingBytes
// reaches maxBuffer, a producer's WriteAt must BLOCK until the consumer
// drains. We verify the block by:
//  1. Filling the buffer to maxBuffer with one out-of-order chunk.
//  2. Launching a goroutine that calls WriteAt again (must block).
//  3. Confirming via `started` flag that the goroutine entered but did NOT
//     return within 50ms (consumer is intentionally slow).
//  4. Letting the consumer drain (WriteAt at offset 0 fast-paths and
//     flushContiguous clears the buffer).
//  5. Confirming the blocked goroutine then completes.
//
// After the drain, the parked goroutine wakes and — because ow.nextOff has
// advanced to 200 — its WriteAt(chunk2, 200) hits the fast path (off ==
// nextOff) and writes straight through. So the final output is
// chunk0||chunk1||chunk2 (300 bytes) and pending is empty.
//
// This test would deadlock under audit §3.2's bug (no Broadcast on error)
// — but here we trigger the success path (Signal on drain), which is the
// more common case.
func TestOrderedWriter_BufferFull(t *testing.T) {
	t.Parallel()
	sw := &slowWriter{delay: 20 * time.Millisecond}
	// maxBuffer = 100 bytes — tiny, so a single 100-byte chunk fills it.
	ow, cancel := newTestOrderedWriter(t, sw, 100)
	defer cancel()
	defer ow.Close()

	const size = 100
	chunk1 := makeChunk('1', size) // offset 100 — out of order, will be buffered
	chunk0 := makeChunk('0', size) // offset 0 — will trigger fast path + flush

	// Step 1: buffer chunk at offset 100. pendingBytes = 100 = maxBuffer.
	if n, err := ow.WriteAt(chunk1, 100); n != size || err != nil {
		t.Fatalf("WriteAt(chunk1,100) = (%d, %v), want (%d, nil)", n, err, size)
	}

	// Step 2: launch a goroutine that tries to buffer another chunk. It
	// MUST block because pendingBytes (100) >= maxBuffer (100).
	blockedDone := make(chan struct{})
	var blockedN int
	var blockedErr error
	go func() {
		defer close(blockedDone)
		n, err := ow.WriteAt(makeChunk('2', size), 200)
		blockedN, blockedErr = n, err
	}()

	// Step 3: confirm the goroutine is blocked. 50ms is much longer than
	// the goroutine's startup; if it returned, backpressure is broken.
	select {
	case <-blockedDone:
		t.Fatalf("blocked WriteAt returned before consumer drained — backpressure broken")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	// Step 4: drain the consumer by writing chunk0 at offset 0. This
	// fast-paths chunk0 (writes 0..100), then flushContiguous drains
	// chunk1 (100..200). pendingBytes drops to 0, which Signals the
	// blocked goroutine.
	if n, err := ow.WriteAt(chunk0, 0); n != size || err != nil {
		t.Fatalf("WriteAt(chunk0,0) = (%d, %v), want (%d, nil)", n, err, size)
	}

	// Step 5: the blocked goroutine should now complete. By the time it
	// wakes, ow.nextOff has advanced to 200, so its WriteAt(chunk2, 200)
	// hits the fast path and writes straight through.
	select {
	case <-blockedDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("blocked WriteAt did not complete after consumer drained — Signal missing")
	}
	if blockedN != size || blockedErr != nil {
		t.Errorf("blocked WriteAt returned (%d, %v), want (%d, nil)", blockedN, blockedErr, size)
	}

	// After the parked goroutine completes, chunk2 has been written via
	// the fast path (off == nextOff after the cascade). pending MUST be
	// empty and output MUST be chunk0||chunk1||chunk2.
	ow.mu.Lock()
	pendingLen := len(ow.pending)
	pendingBytes := ow.pendingBytes
	ow.mu.Unlock()
	if pendingLen != 0 {
		t.Errorf("pending = %d entries, want 0 (chunk2 fast-pathed after cascade)", pendingLen)
	}
	if pendingBytes != 0 {
		t.Errorf("pendingBytes = %d, want 0", pendingBytes)
	}

	// Verify the full output: chunk0 (100) + chunk1 (100) + chunk2 (100).
	sw.mu.Lock()
	got := sw.buf.String()
	sw.mu.Unlock()
	want := strings.Repeat("0", size) + strings.Repeat("1", size) + strings.Repeat("2", size)
	if got != want {
		t.Errorf("flushed output = %q, want %q", got, want)
	}
}

// TestOrderedWriter_ClientDisconnect verifies audit §3.4: when the context
// (which in production is r.Context()) cancels, the orderedWriter's
// watchdog goroutine calls Close, and any subsequent WriteAt must return a
// non-nil error. This is what stops gogram's workers from continuing to
// fetch after the HTTP client disconnects.
func TestOrderedWriter_ClientDisconnect(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	ow := newOrderedWriter(ctx, &buf, 1<<20)
	defer ow.Close()

	// Write one chunk normally — establishes that the adapter is healthy.
	if n, err := ow.WriteAt(makeChunk('A', 50), 0); n != 50 || err != nil {
		t.Fatalf("WriteAt(A,0) before disconnect = (%d, %v), want (50, nil)", n, err)
	}

	// Simulate HTTP client disconnect.
	cancel()

	// The watchdog goroutine calls Close asynchronously — give it a moment.
	// If this races, the test is flaky; if it never happens, §3.4 is broken.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		ow.mu.Lock()
		closed := ow.closed
		ow.mu.Unlock()
		if closed {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	ow.mu.Lock()
	closed := ow.closed
	ow.mu.Unlock()
	if !closed {
		t.Fatalf("cancel(ctx) did not trigger Close() within 500ms — audit §3.4 regression")
	}

	// Subsequent WriteAt MUST fail (non-nil error). Per §3.3 the error is
	// io.ErrClosedPipe.
	n, err := ow.WriteAt(makeChunk('B', 50), 50)
	if err == nil {
		t.Errorf("WriteAt after disconnect returned (%d, nil), want non-nil error", n)
	}
	if n != 0 {
		t.Errorf("WriteAt after disconnect returned n=%d, want 0 (no bytes accepted)", n)
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("WriteAt after disconnect returned err=%v, want io.ErrClosedPipe", err)
	}

	// Output should be exactly the first chunk — chunk B must NOT have
	// been written through.
	if got := buf.String(); got != strings.Repeat("A", 50) {
		t.Errorf("output after disconnect = %q, want 50 'A's (no post-cancel writes)", got)
	}
}

// TestOrderedWriter_CloseStopsWorkers verifies audit §3.3 directly: calling
// Close() on a fresh adapter sets ow.err = io.ErrClosedPipe (NOT nil), so
// gogram's workers that have a WriteAt in flight (or about to start) see a
// non-nil error and stop fetching. Without §3.3's fix, Close would leave
// ow.err=nil and workers would interpret (0, nil) as success and continue.
func TestOrderedWriter_CloseStopsWorkers(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	ow, cancel := newTestOrderedWriter(t, &buf, 1<<20)
	defer cancel()

	// Close before any writes. Per §3.3, Close sets ow.err = io.ErrClosedPipe
	// (and returns it) so gogram's workers see a non-nil error.
	if err := ow.Close(); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("Close() = %v, want io.ErrClosedPipe (audit §3.3 — must be non-nil)", err)
	}

	// Verify the internal state: closed=true, err=io.ErrClosedPipe.
	ow.mu.Lock()
	closed := ow.closed
	err := ow.err
	ow.mu.Unlock()
	if !closed {
		t.Errorf("ow.closed = false, want true")
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("ow.err = %v, want io.ErrClosedPipe (audit §3.3 — must be non-nil)", err)
	}

	// WriteAt must now fail with io.ErrClosedPipe.
	n, wErr := ow.WriteAt(makeChunk('Z', 10), 0)
	if n != 0 {
		t.Errorf("WriteAt after Close returned n=%d, want 0", n)
	}
	if !errors.Is(wErr, io.ErrClosedPipe) {
		t.Errorf("WriteAt after Close returned err=%v, want io.ErrClosedPipe", wErr)
	}
	if got := buf.String(); got != "" {
		t.Errorf("buf = %q, want empty (no writes after Close)", got)
	}

	// Double-close is a no-op and must not panic or change the error.
	if err := ow.Close(); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("second Close() = %v, want io.ErrClosedPipe (idempotent, preserves err)", err)
	}
	ow.mu.Lock()
	err2 := ow.err
	ow.mu.Unlock()
	if !errors.Is(err2, io.ErrClosedPipe) {
		t.Errorf("after second Close, ow.err = %v, want io.ErrClosedPipe (preserved)", err2)
	}
}

// TestOrderedWriter_FlushContiguousBroadcastsOnError verifies audit §3.2
// (the deadlock fix) directly: if the underlying writer returns an error
// during flushContiguous, ALL blocked producers must be woken so they can
// observe ow.err and exit. Without the Broadcast, a producer blocked in
// WriteAt's backpressure loop would wait forever.
//
// We construct the scenario as follows:
//   - maxBuffer = 100 (tiny).
//   - errorWriter fails on the SECOND Write call (returns sentinel).
//   - Producer 1: WriteAt(chunkA, 100) → buffers (100 bytes, fills buffer).
//   - Producer 2 (goroutine): WriteAt(chunkB, 200) → MUST block (buffer full).
//   - Producer 3: WriteAt(chunkC, 0)   → fast-path; first Write succeeds
//     (this is errorWriter's first call), then flushContiguous tries to
//     flush chunkA at offset 100 — this is errorWriter's SECOND call,
//     which fails. §3.2 requires Broadcast so Producer 2 wakes.
//   - Producer 2 then wakes, sees ow.err, returns the sentinel error.
type errorWriter struct {
	mu       sync.Mutex
	calls    int
	failOn   int // 1-indexed call number that should fail
	failWith error
	written  []byte // bytes successfully written (for assertions)
}

func (e *errorWriter) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	if e.calls == e.failOn {
		return 0, e.failWith
	}
	e.written = append(e.written, p...)
	return len(p), nil
}

func TestOrderedWriter_FlushContiguousBroadcastsOnError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("test: simulated writer failure")
	ew := &errorWriter{failOn: 2, failWith: sentinel}
	ow, cancel := newTestOrderedWriter(t, ew, 100)
	defer cancel()
	defer ow.Close()

	const size = 100
	// Step 1: buffer chunkA at offset 100. pendingBytes = 100 = maxBuffer.
	if _, err := ow.WriteAt(makeChunk('A', size), 100); err != nil {
		t.Fatalf("WriteAt(A,100) failed: %v", err)
	}

	// Step 2: launch Producer 2 — it MUST block in WriteAt(chunkB, 200)
	// because pendingBytes (100) >= maxBuffer (100).
	producer2Done := make(chan error, 1)
	go func() {
		_, err := ow.WriteAt(makeChunk('B', size), 200)
		producer2Done <- err
	}()

	// Confirm Producer 2 is parked.
	select {
	case err := <-producer2Done:
		t.Fatalf("Producer 2 returned before flushContiguous error: err=%v (should have blocked)", err)
	case <-time.After(50 * time.Millisecond):
		// expected: blocked
	}

	// Step 3: WriteAt(chunkC, 0). Fast-path writes chunkC (call #1, OK),
	// then flushContiguous writes chunkA at offset 100 (call #2, FAILS).
	// §3.2: this MUST Broadcast so Producer 2 wakes and sees ow.err.
	n, err := ow.WriteAt(makeChunk('C', size), 0)
	if err == nil {
		t.Fatalf("WriteAt(C,0) returned nil err — expected sentinel failure from flushContiguous")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("WriteAt(C,0) err = %v, want sentinel %v", err, sentinel)
	}
	if n != size {
		t.Errorf("WriteAt(C,0) n = %d, want %d (chunkC WAS written before flushContiguous failed)", n, size)
	}

	// Step 4: Producer 2 must wake and return the sentinel error.
	select {
	case err2 := <-producer2Done:
		if !errors.Is(err2, sentinel) {
			t.Errorf("Producer 2 returned err=%v, want sentinel %v (if nil, audit §3.2 fix is broken: deadlock)", err2, sentinel)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Producer 2 never returned — audit §3.2 deadlock regression (Broadcast missing on flushContiguous error)")
	}

	// Step 5: subsequent WriteAt must also fail with the sticky sentinel error.
	if _, err := ow.WriteAt(makeChunk('D', size), 300); !errors.Is(err, sentinel) {
		t.Errorf("post-failure WriteAt err = %v, want sentinel (sticky)", err)
	}

	// Step 6: only chunkC was written; chunkA's flush failed.
	if got := string(ew.written); got != strings.Repeat("C", size) {
		t.Errorf("written bytes = %q, want %q (only chunkC; chunkA flush failed)", got, strings.Repeat("C", size))
	}
}

// TestOrderedWriter_PreservesExistingErrorOnClose verifies that if the
// underlying writer already failed (ow.err set), a later Close() does NOT
// clobber ow.err with io.ErrClosedPipe. Callers want to see the root cause.
// Close() returns ow.err (preserved) rather than nil.
func TestOrderedWriter_PreservesExistingErrorOnClose(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("underlying write failure")
	ew := &errorWriter{failOn: 1, failWith: sentinel}
	ow, cancel := newTestOrderedWriter(t, ew, 1<<20)
	defer cancel()

	// First WriteAt hits the fast path; ew.Write returns sentinel immediately.
	_, err := ow.WriteAt(makeChunk('A', 50), 0)
	if !errors.Is(err, sentinel) {
		t.Fatalf("WriteAt(A,0) err = %v, want sentinel", err)
	}

	// Close must NOT overwrite ow.err with io.ErrClosedPipe — it preserves
	// and returns the existing sentinel error.
	if cerr := ow.Close(); !errors.Is(cerr, sentinel) {
		t.Errorf("Close() returned %v, want sentinel (preserved, NOT io.ErrClosedPipe)", cerr)
	}
	ow.mu.Lock()
	sticky := ow.err
	ow.mu.Unlock()
	if !errors.Is(sticky, sentinel) {
		t.Errorf("after Close, ow.err = %v, want sentinel (Close must preserve the root cause)", sticky)
	}
}

// TestOrderedWriter_ParallelStress is a smoke test that hammers the adapter
// from many goroutines with random offsets and verifies (a) no deadlock,
// (b) the final output is the exact concatenation of all chunks in offset
// order, (c) no panics. Run with -race to catch data races on the pending
// map / nextOff.
func TestOrderedWriter_ParallelStress(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}
	const numChunks = 64
	const chunkSize = 256

	var buf bytes.Buffer
	ow, cancel := newTestOrderedWriter(t, &buf, int64(numChunks*chunkSize))
	defer cancel()
	defer ow.Close()

	// Build deterministic chunks: chunk i = bytes('a'+i, chunkSize).
	chunks := make([][]byte, numChunks)
	for i := range chunks {
		chunks[i] = makeChunk(byte('a'+i%26), chunkSize)
	}

	// Shuffle delivery order across numChunks goroutines. Use a worker pool
	// of size 16 to model gogram's parallel workers.
	var wg sync.WaitGroup
	jobs := make(chan int, numChunks)
	for i := 0; i < numChunks; i++ {
		jobs <- i
	}
	close(jobs)
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				off := int64(i * chunkSize)
				if n, err := ow.WriteAt(chunks[i], off); n != chunkSize || err != nil {
					t.Errorf("WriteAt(chunk=%d, off=%d) = (%d, %v), want (%d, nil)", i, off, n, err, chunkSize)
					return
				}
			}
		}()
	}
	wg.Wait()

	// After all workers finish, every chunk must have been flushed (the
	// last fast-path call's flushContiguous cascades through any remaining
	// buffered chunks).
	ow.mu.Lock()
	pendingLen := len(ow.pending)
	ow.mu.Unlock()
	if pendingLen != 0 {
		t.Errorf("pending = %d entries after all writes, want 0 (full cascade)", pendingLen)
	}

	// Build expected output: chunks in offset order.
	var want bytes.Buffer
	for i := 0; i < numChunks; i++ {
		want.Write(chunks[i])
	}
	if got := buf.Bytes(); !bytes.Equal(got, want.Bytes()) {
		t.Errorf("output mismatch: got %d bytes, want %d bytes", len(got), want.Len())
		// Find first divergence for a useful failure message.
		min := len(got)
		if want.Len() < min {
			min = want.Len()
		}
		for i := 0; i < min; i++ {
			if got[i] != want.Bytes()[i] {
				t.Errorf("first divergence at byte %d: got %q, want %q", i, got[i], want.Bytes()[i])
				break
			}
		}
	}
}

// Compiling-time guard: ensure orderedWriter satisfies io.WriterAt (so the
// type assertion in gogram's downloadDestination switch case succeeds).
var _ io.WriterAt = (*orderedWriter)(nil)
