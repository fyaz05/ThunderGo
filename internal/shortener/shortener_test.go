package shortener

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestShortener_NilSafe(t *testing.T) {
	t.Parallel()
	var s *Shortener
	got := s.Shorten(context.Background(), "https://example.com")
	if got != "https://example.com" {
		t.Errorf("nil shortener should return the original URL, got %q", got)
	}
	got = s.Shorten(context.Background(), "")
	if got != "" {
		t.Errorf("nil shortener with empty URL should return empty, got %q", got)
	}
}

func TestShortener_Disabled(t *testing.T) {
	t.Parallel()
	// New returns nil if either credential is empty.
	s, _ := New("", "https://is.gd", nil)
	if s != nil {
		t.Error("New with empty API key should return nil")
	}
	s, _ = New("apikey", "", nil)
	if s != nil {
		t.Error("New with empty site should return nil")
	}
}

// TestShortener_HTTPSEnforced verifies New rejects non-HTTPS sites
// (C-006 / CWE-319). An http:// site is treated as disabled with a
// warning; a schemeless or other-scheme site is also rejected.
func TestShortener_HTTPSEnforced(t *testing.T) {
	t.Parallel()
	cases := []string{
		"http://is.gd",      // explicit http:// — rejected
		"ftp://is.gd",       // unsupported scheme — rejected
		"is.gd",             // schemeless — rejected
		"is.gd/api?url=foo", // schemeless with path — rejected
	}
	for _, site := range cases {
		if s, _ := New("apikey", site, nil); s != nil {
			t.Errorf("New(apikey, %q) should return nil (non-HTTPS site rejected)", site)
		}
	}
	// HTTPS site is accepted.
	if s, _ := New("apikey", "https://is.gd", nil); s == nil {
		t.Error("New with https:// site should not return nil")
	}
}

// newTestShortener constructs a Shortener pointing at an httptest server,
// bypassing the HTTPS-scheme check in New (which is tested separately in
// TestShortener_HTTPSEnforced). This lets us exercise the cache, API, and
// host-validation logic against an in-process HTTP server.
func newTestShortener(t *testing.T, srvURL, apiKey string) *Shortener {
	t.Helper()
	return &Shortener{
		apiKey: apiKey,
		site:   strings.TrimRight(srvURL, "/"),
		log:    slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
		client: &http.Client{Timeout: 5 * time.Second},
		cache:  newLRUCache(10000),
	}
}

// TestShortener_Caching verifies that repeated Shorten calls for the same
// long URL hit the upstream API exactly once (cache hit on the second call)
// — replacing the previous misleading test that only exercised a nil
// Shortener (D-016). Also verifies the Authorization header is sent on
// the API call (C-005 / D-005).
func TestShortener_Caching(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)

		// Authorization header must be sent on every API call.
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("API call %d: missing/invalid Authorization header: got %q, want %q", n, got, "Bearer test-key")
		}
		// The long URL is passed in the query string (it is not a secret).
		if v := r.URL.Query().Get("url"); v == "" {
			t.Errorf("API call %d: expected non-empty 'url' query param", n)
		}

		// Respond with a short URL whose host matches the test server
		// (otherwise hostMatches would reject it). Use r.Host instead
		// of srv.URL to avoid closing over the unassigned srv variable.
		short := "http://" + r.Host + "/abc"
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"shorturl": short})
	}))
	defer srv.Close()

	s := newTestShortener(t, srv.URL, "test-key")

	long := "https://example.com/very/long/path"
	got1 := s.Shorten(context.Background(), long)
	if got1 == long {
		t.Fatalf("first Shorten call should have returned a short URL, got long URL back")
	}
	if !strings.HasPrefix(got1, srv.URL) {
		t.Errorf("first Shorten: short URL %q should start with server URL %q", got1, srv.URL)
	}

	got2 := s.Shorten(context.Background(), long)
	if got2 != got1 {
		t.Errorf("second Shorten should return cached value: got %q, want %q", got2, got1)
	}

	if n := calls.Load(); n != 1 {
		t.Errorf("API should have been called exactly once (cache hit on 2nd call), got %d calls", n)
	}
}

// TestShortener_DifferentURLsNotCached verifies that two different long
// URLs result in two separate API calls (cache is keyed by long URL).
func TestShortener_DifferentURLsNotCached(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		short := "http://" + r.Host + "/" + r.URL.Query().Get("url")
		_ = json.NewEncoder(w).Encode(map[string]string{"shorturl": short})
	}))
	defer srv.Close()

	s := newTestShortener(t, srv.URL, "test-key")

	a := s.Shorten(context.Background(), "https://example.com/a")
	b := s.Shorten(context.Background(), "https://example.com/b")
	if a == b {
		t.Errorf("different long URLs should produce different short URLs: both %q", a)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("two different URLs should result in 2 API calls, got %d", n)
	}
}

// TestShortener_HostMismatch verifies that a short URL whose host doesn't
// match the configured site is rejected and the long URL is returned
// (C-004 / CWE-601, CWE-918).
func TestShortener_HostMismatch(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return a short URL on a different host than the test server.
		_ = json.NewEncoder(w).Encode(map[string]string{"shorturl": "https://evil.example.com/abc"})
	}))
	defer srv.Close()

	s := newTestShortener(t, srv.URL, "test-key")

	long := "https://example.com/long"
	got := s.Shorten(context.Background(), long)
	if got != long {
		t.Errorf("mismatched host should fall back to long URL: got %q, want %q", got, long)
	}
}

// TestShortener_NonHTTPResponse verifies that a short URL which isn't an
// http(s) URL is rejected (e.g. javascript:, mailto:, data:).
func TestShortener_NonHTTPResponse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("javascript:alert(1)"))
	}))
	defer srv.Close()

	s := newTestShortener(t, srv.URL, "test-key")

	long := "https://example.com/long"
	got := s.Shorten(context.Background(), long)
	if got != long {
		t.Errorf("non-http(s) short URL should fall back to long URL: got %q, want %q", got, long)
	}
}

// TestShortener_Non200Status verifies that a non-200 response causes the
// shortener to fall back to the long URL.
func TestShortener_Non200Status(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := newTestShortener(t, srv.URL, "test-key")

	long := "https://example.com/long"
	got := s.Shorten(context.Background(), long)
	if got != long {
		t.Errorf("non-200 response should fall back to long URL: got %q, want %q", got, long)
	}
}

// TestShortener_PlainTextResponse verifies that a plain-text (non-JSON)
// response is accepted as a short URL when its host matches the site.
func TestShortener_PlainTextResponse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("http://" + r.Host + "/plain"))
	}))
	defer srv.Close()

	s := newTestShortener(t, srv.URL, "test-key")

	long := "https://example.com/long"
	got := s.Shorten(context.Background(), long)
	if got == long {
		t.Errorf("plain-text short URL should have been returned, got long URL")
	}
	if !strings.HasPrefix(got, srv.URL) {
		t.Errorf("plain-text short URL %q should start with server URL %q", got, srv.URL)
	}
}

// TestShortener_MalformedJSON verifies that a JSON response with no
// recognized short-URL field falls back to the long URL.
func TestShortener_MalformedJSON(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer srv.Close()

	s := newTestShortener(t, srv.URL, "test-key")

	long := "https://example.com/long"
	got := s.Shorten(context.Background(), long)
	if got != long {
		t.Errorf("JSON without recognized short-URL field should fall back to long URL: got %q, want %q", got, long)
	}
}
