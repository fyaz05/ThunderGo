package http

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fyaz05/ThunderGo/internal/config"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// The legacy URL shapes (plan §8) answer 410 Gone with a constant body while
// ENABLE_LEGACY_LINKS is off (the default), and route to the live handlers
// when it is on. This pins the matrix through the real chi router.

func newLegacyTestServer(t *testing.T, enableLegacy bool) *Server {
	t.Helper()
	cfg := &config.Config{
		BaseURL:           "https://files.example",
		EnableLegacyLinks: enableLegacy,
	}
	srv, err := New(cfg, pool.NewForTests(tgutil.NewFakeTransport()), nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv
}

func TestLegacyRoutes_DisabledMatrix(t *testing.T) {
	t.Parallel()
	srv := newLegacyTestServer(t, false)

	shapes := []struct {
		method, target string
	}{
		{http.MethodGet, "/watch/f/0123456789abcdef/movie.mp4"},
		{http.MethodHead, "/watch/f/0123456789abcdef/movie.mp4"},
		{http.MethodGet, "/watch/f/0123456789abcdef/movie.mp4/raw"},
		{http.MethodHead, "/watch/f/0123456789abcdef/movie.mp4/raw"},
		{http.MethodGet, "/watch/0123456789abcdef"},
		{http.MethodHead, "/watch/0123456789abcdef"},
		{http.MethodGet, "/f/0123456789abcdef"},
		{http.MethodHead, "/f/0123456789abcdef"},
	}
	for _, s := range shapes {
		rr := httptest.NewRecorder()
		srv.Router.ServeHTTP(rr, httptest.NewRequest(s.method, s.target, nil))
		if rr.Code != http.StatusGone {
			t.Errorf("%s %s = %d, want 410", s.method, s.target, rr.Code)
		}
		if body := rr.Body.String(); body != "Gone\n" {
			t.Errorf("%s %s body = %q, want constant \"Gone\"", s.method, s.target, body)
		}
		if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("%s %s missing Cache-Control: no-store", s.method, s.target)
		}
		if acao := rr.Header().Get("Access-Control-Allow-Origin"); acao != "*" {
			t.Errorf("%s %s missing CORS header", s.method, s.target)
		}
	}
}

func TestLegacyRoutes_EnabledRoutesLive(t *testing.T) {
	t.Parallel()
	srv := newLegacyTestServer(t, true)

	// The raw shape reaches the stream handler (no-store 404 for an unknown
	// hash with a nil store) instead of the 410 — proving live routing.
	rr := httptest.NewRecorder()
	srv.Router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/watch/f/deadbeefdeadbeef/x.mp4/raw", nil))
	if rr.Code == http.StatusGone {
		t.Fatal("enabled legacy raw route still answers 410")
	}
	if rr.Code != http.StatusNotFound {
		t.Fatalf("enabled legacy raw route = %d, want 404 (unknown hash, nil store)", rr.Code)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}
}

func TestLegacyRoutes_NonLegacyRoutesUnaffected(t *testing.T) {
	t.Parallel()
	srv := newLegacyTestServer(t, false)

	rr := httptest.NewRecorder()
	srv.Router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rr.Code != http.StatusOK || rr.Body.String() != `{"status":"ok"}` {
		t.Fatalf("/health = %d %q", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	srv.Router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusFound {
		t.Fatalf("/ = %d, want 302 (repo redirect is not legacy)", rr.Code)
	}

	// The two-segment /f/{token}/{filename} player route is current, not
	// legacy — with a nil store it 404s rather than 410s.
	rr = httptest.NewRecorder()
	srv.Router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/f/0123456789abcdef/movie.mp4", nil))
	if rr.Code == http.StatusGone {
		t.Fatal("current player route treated as legacy")
	}
}

func TestPlayerPage_NoIndexHeader(t *testing.T) {
	t.Parallel()
	// renderPlayerPage sets X-Robots-Tag (mirrors the template meta tag);
	// pin it through the real template render.
	srv := newLegacyTestServer(t, false)
	rr := httptest.NewRecorder()
	srv.renderPlayerPage(rr, &store.FileRecord{
		Hash:     "0123456789abcdef",
		FileName: "movie.mp4",
		MimeType: "video/mp4",
		Size:     12345,
	})
	if got := rr.Header().Get("X-Robots-Tag"); got != "noindex, nofollow" {
		t.Fatalf("X-Robots-Tag = %q, want noindex, nofollow", got)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if body := rr.Body.String(); !strings.Contains(body, "noindex, nofollow") {
		t.Fatal("player page missing the noindex meta tag")
	}
}
