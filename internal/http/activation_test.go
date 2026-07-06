// Package http — unit tests for the /activate/{token} route.
//
// These are NOT integration tests — they use httptest to exercise
// handleActivate in isolation. No build tag is needed. handleActivate is
// stateless (it doesn't touch the DB or the Telegram pool — token
// validation/consumption is deferred to the bot's /start handler), so we
// only need to test the redirect logic + the two early-return branches:
//
//   - empty token (post-NormalizeBase32) → 404
//   - empty botUsername (misconfigured bot) → 503
//   - happy path → 302 redirect to tg://<bot>?start=<token>
//
// handleActivate reads the {token} URL parameter via chi.URLParam, which
// requires a chi route context on the request. The helper newActivateReq
// attaches one so we can drive the handler directly without standing up
// the whole chi router (which would need a *pool.Pool).
package http

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// newActivateReq builds a GET /activate/{token} request with the chi route
// context populated, so handleActivate's chi.URLParam(r, "token") returns
// the given token. The URL path is a cosmetic placeholder (the chi context
// overrides it) — httptest.NewRequest rejects malformed URLs, so we use a
// fixed path and inject the real token via chi's URLParams. handleActivate
// never reads r.URL.Path, only chi.URLParam.
func newActivateReq(token string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/activate/placeholder", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("token", token)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

// TestHandleActivate_Redirect verifies the happy path: a valid (non-empty
// post-NormalizeBase32) token yields a 302 redirect to
// `tg://<botUsername>?start=<normalizedToken>`. The token is NOT consumed
// here — consumption happens in the bot's /start handler so the redirect
// doesn't burn a token the user might not actually use.
func TestHandleActivate_Redirect(t *testing.T) {
	srv := &Server{
		botUsername: "TestBot",
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	const token = "abc1234xyz"
	req := newActivateReq(token)
	rec := httptest.NewRecorder()
	srv.handleActivate(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d, want %d (StatusFound)", rec.Code, http.StatusFound)
	}
	wantLoc := "tg://TestBot?start=" + token
	if got := rec.Header().Get("Location"); got != wantLoc {
		t.Errorf("Location = %q, want %q", got, wantLoc)
	}
}

// TestHandleActivate_NormalizesToken verifies that the token is run through
// tgutil.NormalizeBase32 before being placed in the redirect URL (lowercase +
// Crockford O→0, I→1, L→1 normalization). This is the same normalization the
// bot's /start handler applies, so the redirect must produce a canonical
// token to match what /start will look up in the DB.
func TestHandleActivate_NormalizesToken(t *testing.T) {
	srv := &Server{
		botUsername: "TestBot",
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	// "OIL" should normalize to "011".
	req := newActivateReq("OILxyz")
	rec := httptest.NewRecorder()
	srv.handleActivate(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusFound)
	}
	// "OILxyz" → lowercase "oilxyz" → "011xyz" (O→0, I→1, L→1).
	const wantToken = "011xyz"
	wantLoc := "tg://TestBot?start=" + wantToken
	if got := rec.Header().Get("Location"); got != wantLoc {
		t.Errorf("Location = %q, want %q (token not normalized)", got, wantLoc)
	}
}

// TestHandleActivate_EmptyToken verifies that an empty {token} URL parameter
// (after NormalizeBase32) yields 404. This is the documented contract for
// the /activate/{token} route when no token is supplied.
func TestHandleActivate_EmptyToken(t *testing.T) {
	srv := &Server{
		botUsername: "TestBot",
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	// Build a request with NO chi route context — chi.URLParam returns "".
	req := httptest.NewRequest(http.MethodGet, "/activate/", nil)
	rec := httptest.NewRecorder()
	srv.handleActivate(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d (StatusNotFound for empty token)", rec.Code, http.StatusNotFound)
	}
	// Should not have issued a redirect.
	if loc := rec.Header().Get("Location"); loc != "" {
		t.Errorf("Location = %q, want empty (no redirect on empty token)", loc)
	}
}

// TestHandleActivate_WhitespaceOnlyToken verifies that a whitespace-only
// token (which NormalizeBase32 collapses to "") yields 404 — same as
// TestHandleActivate_EmptyToken, but exercising the TrimSpace path inside
// NormalizeBase32.
func TestHandleActivate_WhitespaceOnlyToken(t *testing.T) {
	srv := &Server{
		botUsername: "TestBot",
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	req := newActivateReq("   ")
	rec := httptest.NewRecorder()
	srv.handleActivate(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d (whitespace token normalizes to empty)", rec.Code, http.StatusNotFound)
	}
	if loc := rec.Header().Get("Location"); loc != "" {
		t.Errorf("Location = %q, want empty (no redirect on whitespace token)", loc)
	}
}

// TestHandleActivate_NoBotUsername verifies that when the Server has no
// cached botUsername (which happens when GetMe failed at startup — bot is
// misconfigured), the handler returns 503 Service Unavailable. This makes
// the misconfiguration visible to monitoring rather than silently producing
// a broken `tg://?start=...` redirect.
func TestHandleActivate_NoBotUsername(t *testing.T) {
	srv := &Server{
		botUsername: "",
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	req := newActivateReq("validtoken123")
	rec := httptest.NewRecorder()
	srv.handleActivate(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d (StatusServiceUnavailable when botUsername is empty)",
			rec.Code, http.StatusServiceUnavailable)
	}
	if loc := rec.Header().Get("Location"); loc != "" {
		t.Errorf("Location = %q, want empty (no redirect on misconfigured bot)", loc)
	}
	// Body should mention the bot-username issue so an operator reading the
	// response can diagnose it.
	if body := rec.Body.String(); !strings.Contains(strings.ToLower(body), "bot") {
		t.Errorf("body = %q, want it to mention the bot-username issue", body)
	}
}
