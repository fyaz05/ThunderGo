package http

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// TestRedactPath verifies that the token segment in /f/{token}/{filename}
// paths is replaced with tgutil.TokenHash(token), while non-/f/ paths pass
// through unchanged. Expected values are computed via tgutil.TokenHash directly
// so the test does not hardcode hash values that could drift.
func TestRedactPath(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"basic file path", "/f/abc123/movie.mp4"},
		{"file path with raw suffix", "/f/abc123/movie.mp4/raw"},
		{"token empty filename", "/f/abc/"},
		{"empty token empty filename", "/f/"},
		{"long token", "/f/abcdefghijklmnop/movie.mp4"},
		{"health passthrough", "/health"},
		{"status passthrough", "/status"},
		{"root passthrough", "/"},
		{"other prefix passthrough", "/api/something"},
		{"foo prefix passthrough (no trailing /f/)", "/foo/bar"},
		{"f-similar prefix passthrough", "/frog/x"},
		{"frog with slash prefix", "/frog/"},
		{"just slash f", "/f"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactPath(tt.path)
			if !strings.HasPrefix(tt.path, "/f/") {
				if got != tt.path {
					t.Errorf("redactPath(%q) = %q, want %q (unchanged)", tt.path, got, tt.path)
				}
				return
			}
			// /f/ path: token segment should be hashed, rest unchanged.
			parts := strings.SplitN(tt.path, "/", 4)
			wantParts := make([]string, len(parts))
			copy(wantParts, parts)
			if len(parts) >= 3 && parts[2] != "" {
				wantParts[2] = tgutil.TokenHash(parts[2])
			}
			want := strings.Join(wantParts, "/")
			if got != want {
				t.Errorf("redactPath(%q) = %q, want %q", tt.path, got, want)
			}
			// Verify original token never appears in output (when token non-empty).
			if len(parts) >= 3 && parts[2] != "" && strings.Contains(got, parts[2]) {
				t.Errorf("redactPath(%q) = %q leaks original token %q", tt.path, got, parts[2])
			}
		})
	}
}

// TestFormatBytes verifies the local wrapper delegates to tgutil.FormatBytes
// (which the util package tests for tier correctness). We keep a small
// table here to ensure the wrapper passes through unmodified for the
// values the player page actually renders (file sizes from a few bytes up
// to multi-TiB).
func TestFormatBytes(t *testing.T) {
	const (
		KiB = 1024
		MiB = KiB * 1024
		GiB = MiB * 1024
		TiB = GiB * 1024
	)
	tests := []struct {
		name string
		n    int64
		want string
	}{
		{"zero", 0, "0 B"},
		{"one byte", 1, "1 B"},
		{"1023 bytes", 1023, "1023 B"},
		{"1 KiB exactly", 1024, "1.00 KiB"},
		{"1.5 KiB", 1536, "1.50 KiB"},
		{"1 MiB exactly", MiB, "1.00 MiB"},
		{"1 GiB exactly", GiB, "1.00 GiB"},
		{"1 TiB exactly", TiB, "1.00 TiB"},
		{"5 KiB", 5 * KiB, "5.00 KiB"},
		{"3.5 MiB", int64(3.5 * MiB), "3.50 MiB"},
		{"2.25 GiB", int64(2.25 * GiB), "2.25 GiB"},
		{"1 byte above 1 TiB", 1 + TiB, "1.00 TiB"},
		{"negative (fallback to %d B)", -1, "-1 B"},
		// tgutil.FormatBytes now supports PiB / EiB (added in 5-F), so
		// 1<<62 (4 EiB) renders as "4.00 EiB" rather than the old
		// "4194304.00 TiB" overflow.
		{"1<<62 now in EiB branch", 1 << 62, "4.00 EiB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatBytes(tt.n)
			if got != tt.want {
				t.Errorf("formatBytes(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}

// TestHandleRoot verifies handleRoot issues a 302 redirect to the project
// GitHub URL. handleRoot accesses no Server fields, so an empty Server works.
func TestHandleRoot(t *testing.T) {
	srv := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.handleRoot(rec, req)

	if rec.Code != http.StatusFound {
		t.Errorf("status = %d, want %d (StatusFound)", rec.Code, http.StatusFound)
	}
	const wantURL = "https://github.com/fyaz05/ThunderGo"
	if loc := rec.Header().Get("Location"); loc != wantURL {
		t.Errorf("Location = %q, want %q", loc, wantURL)
	}
}

// TestHandleHealth verifies the health endpoint returns 200 OK with a JSON
// body containing {"status":"ok"} and the correct Content-Type.
func TestHandleHealth(t *testing.T) {
	srv := &Server{}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	srv.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"status":"ok"`) {
		t.Errorf("body = %q, want it to contain \"status\":\"ok\"", body)
	}
	// Body should be exactly the documented JSON.
	const wantBody = `{"status":"ok"}`
	if body != wantBody {
		t.Errorf("body = %q, want exactly %q", body, wantBody)
	}
}

// TestHandleStatus_Skipped documents why handleStatus cannot be unit-tested
// from the http package: it requires a *pool.Pool, whose fields (all, primary,
// maxConcurrent, stopped) are all unexported, and the only constructor
// (pool.New) connects to Telegram. The pool package's own fakePool helper is
// inaccessible from outside the pool package. See worklog Task 2-E.
func TestHandleStatus_Skipped(t *testing.T) {
	t.Skip("handleStatus requires *pool.Pool which cannot be constructed externally without connecting to Telegram; covered by pool package tests + manual /status verification")
}

// TestCorsMiddleware verifies the wildcard CORS headers are present on all
// responses, OPTIONS preflight short-circuits to 200 with an empty body,
// and the inner handler is bypassed on preflight.
func TestCorsMiddleware(t *testing.T) {
	srv := &Server{}
	innerCalled := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerCalled = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	wrapped := srv.corsMiddleware(inner)

	t.Run("GET sets wildcard CORS headers and passes through", func(t *testing.T) {
		innerCalled = false
		req := httptest.NewRequest(http.MethodGet, "/anything", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)

		if !innerCalled {
			t.Errorf("inner handler was not called for GET")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		checks := map[string]string{
			"Access-Control-Allow-Origin":   "*",
			"Access-Control-Allow-Methods":  "GET, HEAD, OPTIONS",
			"Access-Control-Allow-Headers":  "Range, Content-Type, Accept, Authorization",
			"Access-Control-Expose-Headers": "Content-Length, Content-Range, Content-Disposition",
			"Access-Control-Max-Age":        "86400",
		}
		for h, want := range checks {
			if got := rec.Header().Get(h); got != want {
				t.Errorf("header %s = %q, want %q", h, got, want)
			}
		}
		if rec.Body.String() != "ok" {
			t.Errorf("body = %q, want \"ok\"", rec.Body.String())
		}
	})

	t.Run("OPTIONS preflight returns 200 without calling inner", func(t *testing.T) {
		innerCalled = false
		req := httptest.NewRequest(http.MethodOptions, "/anything", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)

		if innerCalled {
			t.Errorf("inner handler should NOT be called for OPTIONS preflight")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if rec.Body.Len() != 0 {
			t.Errorf("body should be empty for preflight, got %q", rec.Body.String())
		}
		// CORS headers still present on preflight response.
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
			t.Errorf("Access-Control-Allow-Origin = %q, want *", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET, HEAD, OPTIONS" {
			t.Errorf("Access-Control-Allow-Methods = %q, want GET, HEAD, OPTIONS", got)
		}
	})

	t.Run("HEAD passes through with CORS headers", func(t *testing.T) {
		innerCalled = false
		req := httptest.NewRequest(http.MethodHead, "/anything", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)

		if !innerCalled {
			t.Errorf("inner handler was not called for HEAD")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
			t.Errorf("Access-Control-Allow-Origin = %q, want *", got)
		}
	})

	t.Run("POST passes through (method not in Allow-Methods but still allowed)", func(t *testing.T) {
		innerCalled = false
		req := httptest.NewRequest(http.MethodPost, "/anything", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)

		// corsMiddleware only short-circuits OPTIONS; all other methods pass through.
		if !innerCalled {
			t.Errorf("inner handler was not called for POST")
		}
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
		}
	})
}

// TestLogMiddleware verifies the log middleware passes the response through
// unchanged (status, body), defaults to 200 when inner handler doesn't call
// WriteHeader, and that the access log entry has the token redacted.
func TestLogMiddleware(t *testing.T) {
	t.Run("passthrough status and body", func(t *testing.T) {
		srv := &Server{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte("hello"))
		})
		wrapped := srv.logMiddleware(inner)

		req := httptest.NewRequest(http.MethodGet, "/f/secret-token/file.mp4", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)

		if rec.Code != http.StatusTeapot {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusTeapot)
		}
		if rec.Body.String() != "hello" {
			t.Errorf("body = %q, want \"hello\"", rec.Body.String())
		}
	})

	t.Run("default status 200 when inner does not WriteHeader", func(t *testing.T) {
		srv := &Server{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("no explicit status"))
		})
		wrapped := srv.logMiddleware(inner)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want %d (default 200)", rec.Code, http.StatusOK)
		}
		if rec.Body.String() != "no explicit status" {
			t.Errorf("body = %q, want \"no explicit status\"", rec.Body.String())
		}
	})

	t.Run("redacts token in log output", func(t *testing.T) {
		var buf strings.Builder
		srv := &Server{Log: slog.New(slog.NewTextHandler(&buf, nil))}
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		wrapped := srv.logMiddleware(inner)

		req := httptest.NewRequest(http.MethodGet, "/f/secret-token/file.mp4", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)

		logOut := buf.String()
		if !strings.Contains(logOut, "http") {
			t.Errorf("log output should contain \"http\" message, got %q", logOut)
		}
		// Original token MUST NOT appear in log output (CWE-532).
		if strings.Contains(logOut, "secret-token") {
			t.Errorf("log output leaked original token: %q", logOut)
		}
		// Hashed token should appear (for log correlation).
		hashed := tgutil.TokenHash("secret-token")
		if !strings.Contains(logOut, hashed) {
			t.Errorf("log output should contain hashed token %q, got %q", hashed, logOut)
		}
	})

	t.Run("non-f path unchanged in log output", func(t *testing.T) {
		var buf strings.Builder
		srv := &Server{Log: slog.New(slog.NewTextHandler(&buf, nil))}
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		wrapped := srv.logMiddleware(inner)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)

		logOut := buf.String()
		if !strings.Contains(logOut, "path=/health") {
			t.Errorf("log output should contain path=/health, got %q", logOut)
		}
	})
}

// TestStatusRecorder verifies the ResponseWriter wrapper used by logMiddleware
// correctly captures status code, accumulates byte count, and safely proxies
// Flush to the underlying writer when it implements http.Flusher (or no-ops
// when it doesn't).
func TestStatusRecorder(t *testing.T) {
	t.Run("default fields", func(t *testing.T) {
		rec := httptest.NewRecorder()
		sr := &statusRecorder{ResponseWriter: rec, status: 200}
		if sr.status != 200 {
			t.Errorf("status = %d, want 200", sr.status)
		}
		if sr.bytes != 0 {
			t.Errorf("bytes = %d, want 0", sr.bytes)
		}
	})

	t.Run("WriteHeader sets status and propagates", func(t *testing.T) {
		rec := httptest.NewRecorder()
		sr := &statusRecorder{ResponseWriter: rec, status: 200}
		sr.WriteHeader(http.StatusNotFound)
		if sr.status != http.StatusNotFound {
			t.Errorf("sr.status = %d, want %d", sr.status, http.StatusNotFound)
		}
		if rec.Code != http.StatusNotFound {
			t.Errorf("underlying Code = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("WriteHeader called twice keeps last", func(t *testing.T) {
		rec := httptest.NewRecorder()
		sr := &statusRecorder{ResponseWriter: rec, status: 200}
		sr.WriteHeader(http.StatusTeapot)
		sr.WriteHeader(http.StatusBadGateway)
		if sr.status != http.StatusBadGateway {
			t.Errorf("sr.status = %d, want %d (last call wins)", sr.status, http.StatusBadGateway)
		}
	})

	t.Run("Write increments bytes and propagates", func(t *testing.T) {
		rec := httptest.NewRecorder()
		sr := &statusRecorder{ResponseWriter: rec, status: 200}
		n, err := sr.Write([]byte("hello world"))
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
		if n != 11 {
			t.Errorf("Write returned n = %d, want 11", n)
		}
		if sr.bytes != 11 {
			t.Errorf("sr.bytes = %d, want 11", sr.bytes)
		}
		n2, _ := sr.Write([]byte("again"))
		if n2 != 5 {
			t.Errorf("second Write returned n = %d, want 5", n2)
		}
		if sr.bytes != 16 {
			t.Errorf("sr.bytes after second write = %d, want 16", sr.bytes)
		}
		if rec.Body.String() != "hello worldagain" {
			t.Errorf("underlying body = %q, want \"hello worldagain\"", rec.Body.String())
		}
	})

	t.Run("Header proxies to underlying writer", func(t *testing.T) {
		rec := httptest.NewRecorder()
		sr := &statusRecorder{ResponseWriter: rec, status: 200}
		h := sr.Header()
		h.Set("X-Custom", "yes")
		if got := rec.Header().Get("X-Custom"); got != "yes" {
			t.Errorf("underlying header X-Custom = %q, want \"yes\"", got)
		}
	})

	t.Run("Flush propagates to Flusher", func(t *testing.T) {
		rec := httptest.NewRecorder()
		sr := &statusRecorder{ResponseWriter: rec, status: 200}
		sr.Flush()
		if !rec.Flushed {
			t.Errorf("expected underlying ResponseRecorder.Flushed to be true after Flush()")
		}
	})

	t.Run("Flush is a no-op on non-Flusher underlying (no panic)", func(t *testing.T) {
		// nonFlusher wraps an http.ResponseWriter interface field. The method
		// set of *nonFlusher only includes the http.ResponseWriter methods
		// (Header, Write, WriteHeader) — NOT Flush, because Flush is part of
		// the separate http.Flusher interface and is therefore not promoted
		// from the embedded interface field.
		nf := &nonFlusher{ResponseWriter: httptest.NewRecorder()}
		sr := &statusRecorder{ResponseWriter: nf, status: 200}
		// Must not panic.
		sr.Flush()
	})
}

// nonFlusher wraps an http.ResponseWriter (interface) but intentionally does
// NOT implement http.Flusher, so the type assertion in statusRecorder.Flush
// fails and the call is a safe no-op.
type nonFlusher struct {
	http.ResponseWriter
}
