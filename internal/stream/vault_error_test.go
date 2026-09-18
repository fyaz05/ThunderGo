package stream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// These tests pin the resolve/pipeline error→HTTP mapping (plan §8): every
// failure lands on a fixed status with fixed headers and a constant body —
// floods are surfaced as values (Retry-After), permanents self-heal,
// transients never delete records, and unknowns carry only a support id.

func errorTestHandler(t *testing.T) (*Handler, *tgutil.FakeTransport, *fakeStore) {
	t.Helper()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := &Handler{
		Pool:        pool.NewForTests(ft),
		Store:       st,
		Log:         quietStreamLogger(),
		Ingester:    &ingest.Ingester{},
		concurrency: 4,
		bufferCount: 8,
		timeout:     5 * time.Second,
		maxRetries:  1,
		vault:       newVaultCache(),
	}
	return h, ft, st
}

func requestFor(rec *store.FileRecord, canceled bool) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	ctx := WithToken(req.Context(), rec.Hash)
	if canceled {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		ctx = cctx
	}
	return req.WithContext(ctx)
}

func TestResolveError_FloodMapsTo503WithWaitValue(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		seconds int
		wantRA  string
	}{
		{"tiny wait floors to 2", 1, "2"},
		{"exact wait passes through", 30, "30"},
		{"huge wait caps at 600", 5000, "600"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, ft, _ := errorTestHandler(t)
			ft.SetResolve(func(_ context.Context, _ int64, _ int) (tgutil.FileHandle, error) {
				return tgutil.FileHandle{}, tgutil.NewFloodWaitTestError(tc.seconds)
			})
			rec := testRecorder()
			rr := httptest.NewRecorder()
			h.serveResolveError(rr, requestFor(rec, false), rec.Hash, rec, mustLease(t, h), tgutil.NewFloodWaitTestError(tc.seconds))
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", rr.Code)
			}
			if ra := rr.Header().Get("Retry-After"); ra != tc.wantRA {
				t.Fatalf("Retry-After = %q, want %q", ra, tc.wantRA)
			}
			if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
				t.Fatal("missing no-store")
			}
		})
	}
}

func TestResolveError_PermanentSentinelsSelfHeal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
	}{
		{"stale media", tgutil.ErrStaleMedia},
		{"record mismatch", tgutil.ErrRecordMismatch},
		{"media missing", tgutil.ErrMediaMissing},
		{"stale wrapped", fmtWrap{tgutil.ErrStaleMedia}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _, st := errorTestHandler(t)
			rec := testRecorder()
			rr := httptest.NewRecorder()
			h.serveResolveError(rr, requestFor(rec, false), rec.Hash, rec, mustLease(t, h), tc.err)
			if rr.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", rr.Code)
			}
			if deleted := st.deletedHashes(); len(deleted) != 1 || deleted[0] != rec.Hash {
				t.Fatalf("deleted = %v, want [%s]", deleted, rec.Hash)
			}
		})
	}
}

func TestResolveError_TransientNeverDeletes(t *testing.T) {
	t.Parallel()
	h, _, st := errorTestHandler(t)
	rec := testRecorder()
	rr := httptest.NewRecorder()
	h.serveResolveError(rr, requestFor(rec, false), rec.Hash, rec, mustLease(t, h),
		errors.Join(tgutil.ErrTransient, errors.New("dial timeout")))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if ra := rr.Header().Get("Retry-After"); ra != strconv.Itoa(overloadRetryAfterSeconds) {
		t.Fatalf("Retry-After = %q", ra)
	}
	if deleted := st.deletedHashes(); len(deleted) != 0 {
		t.Fatalf("records deleted on transient: %v", deleted)
	}
}

func TestResolveError_UnknownBecomes500SupportID(t *testing.T) {
	t.Parallel()
	h, _, _ := errorTestHandler(t)
	rec := testRecorder()
	rr := httptest.NewRecorder()
	h.serveResolveError(rr, requestFor(rec, false), rec.Hash, rec, mustLease(t, h), errors.New("???"))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "support id: ") {
		t.Fatalf("body = %q, want support id", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "???") {
		t.Fatalf("body reflects the error text: %q", rr.Body.String())
	}
}

func TestResolveError_CanceledContextWritesNothing(t *testing.T) {
	t.Parallel()
	h, _, _ := errorTestHandler(t)
	rec := testRecorder()
	rr := httptest.NewRecorder()
	h.serveResolveError(rr, requestFor(rec, true), rec.Hash, rec, mustLease(t, h), tgutil.ErrStaleMedia)
	if rr.Code != http.StatusOK { // recorder default; handler wrote nothing
		t.Fatalf("status = %d, want untouched 200", rr.Code)
	}
	if rr.Body.Len() != 0 {
		t.Fatalf("body = %q, want empty", rr.Body.String())
	}
}

func TestPipelineError_FloodMapsTo429WithCappedWait(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		seconds int
		wantRA  string
	}{
		{"tiny wait floors to 2", 1, "2"},
		{"exact wait passes through", 45, "45"},
		{"huge wait caps at 60", 5000, "60"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _, _ := errorTestHandler(t)
			rec := testRecorder()
			rr := httptest.NewRecorder()
			h.servePipelineError(rr, requestFor(rec, false), rec.Hash, rec, mustLease(t, h),
				&tgutil.FloodWait{Seconds: tc.seconds})
			if rr.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429", rr.Code)
			}
			if ra := rr.Header().Get("Retry-After"); ra != tc.wantRA {
				t.Fatalf("Retry-After = %q, want %q", ra, tc.wantRA)
			}
			if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
				t.Fatal("missing no-store")
			}
		})
	}
}

func TestPipelineError_PermanentSelfHealsAndInvalidatesCache(t *testing.T) {
	t.Parallel()
	h, _, st := errorTestHandler(t)
	rec := testRecorder()
	h.vault.put(rec.Hash, testHandle(), time.Now())
	rr := httptest.NewRecorder()
	h.servePipelineError(rr, requestFor(rec, false), rec.Hash, rec, mustLease(t, h), tgutil.ErrStaleMedia)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if _, ok := h.vault.get(rec.Hash, time.Now()); ok {
		t.Fatal("vault cache entry survived permanent error")
	}
	if deleted := st.deletedHashes(); len(deleted) != 1 {
		t.Fatalf("deleted = %v, want self-heal", deleted)
	}
}

func TestPipelineError_TransientMapsTo503(t *testing.T) {
	t.Parallel()
	h, _, st := errorTestHandler(t)
	rec := testRecorder()
	rr := httptest.NewRecorder()
	h.servePipelineError(rr, requestFor(rec, false), rec.Hash, rec, mustLease(t, h), tgutil.ErrTransient)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if deleted := st.deletedHashes(); len(deleted) != 0 {
		t.Fatalf("records deleted on transient: %v", deleted)
	}
}

func TestPipelineError_UnknownBecomes500(t *testing.T) {
	t.Parallel()
	h, _, _ := errorTestHandler(t)
	rec := testRecorder()
	rr := httptest.NewRecorder()
	h.servePipelineError(rr, requestFor(rec, false), rec.Hash, rec, mustLease(t, h), errors.New("mystery"))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
}

func TestServeUnavailable_CapacityVsBrownout(t *testing.T) {
	t.Parallel()
	h, ft, _ := errorTestHandler(t)

	// Capacity: fixed Retry-After 2.
	rr := httptest.NewRecorder()
	h.serveUnavailable(rr, pool.ErrPoolCapacity)
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") != "2" {
		t.Fatalf("capacity: status=%d RA=%q", rr.Code, rr.Header().Get("Retry-After"))
	}

	// Brownout: floor 5s, pool estimate, cap 600s.
	h.Pool.ReportFlood(ft, 600*time.Second)
	rr = httptest.NewRecorder()
	h.serveUnavailable(rr, pool.ErrPoolBrownout)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("brownout: status = %d", rr.Code)
	}
	ra, err := strconv.Atoi(rr.Header().Get("Retry-After"))
	if err != nil || ra < 5 || ra > 600 {
		t.Fatalf("brownout Retry-After = %q, want [5,600]", rr.Header().Get("Retry-After"))
	}
}

// fmtWrap wraps an error non-transparantly to prove errors.Is-based
// classification (not string matching) drives the permanent path.
type fmtWrap struct{ err error }

func (w fmtWrap) Error() string { return "wrapped: " + w.err.Error() }
func (w fmtWrap) Is(target error) bool {
	return errors.Is(w.err, target)
}

// mustLease acquires one lease for handlers that need one in scope.
func mustLease(t *testing.T, h *Handler) *pool.Lease {
	t.Helper()
	lease, err := h.Pool.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest: %v", err)
	}
	t.Cleanup(lease.Release)
	return lease
}
