// Package stream is white-box tested here to access unexported helpers
// (setCommonHeaders, tokenFromContext, WithToken, ctxKeyToken, StreamChunkSize).
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
package stream

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

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
func TestServeHTTP_RejectsUnsupportedMethodBeforeStoreAccess(t *testing.T) {
	t.Parallel()
	h := &Handler{Log: discardLogger()}
	req := httptest.NewRequest(http.MethodPost, "/f/token/file/raw", nil)
	req = req.WithContext(WithToken(req.Context(), "token"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != "GET, HEAD, OPTIONS" {
		t.Errorf("Allow = %q, want GET, HEAD, OPTIONS", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
}

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

// TestStreamChunkSize verifies the sequential transfer chunk matches
// FileToLink's 1 MiB stream_media chunk size.
func TestStreamChunkSize(t *testing.T) {
	t.Parallel()
	const oneMiB = 1 << 20 // 1048576 bytes
	if StreamChunkSize != oneMiB {
		t.Errorf("StreamChunkSize = %d, want %d (1 MiB)", StreamChunkSize, oneMiB)
	}
	if StreamChunkSize <= 0 {
		t.Errorf("StreamChunkSize must be positive, got %d", StreamChunkSize)
	}
}

func TestRangeFailureDoesNotRepeatAnExhaustedGogramRetryCycle(t *testing.T) {
	t.Parallel()
	if maxConsecutiveChunkFails != 1 {
		t.Errorf("maxConsecutiveChunkFails = %d, want 1; gogram already retries each failed part", maxConsecutiveChunkFails)
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
