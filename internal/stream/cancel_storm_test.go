package stream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// Phase 4 characterization (plan §5): slot accounting under cancel storms and
// the stall-timeout behavior. These guard every future mtgo bump.

// TestSlotAccountingUnderCancelStorms slams the handler with concurrent
// requests whose contexts are cancelled at random early moments — the shape
// produced by browsers aborting range requests and by flaky mobile clients.
// The invariant that matters: every lease is released exactly once and the
// pool drains to zero in-flight after the storm (no slot leaks, no deadlock).
func TestSlotAccountingUnderCancelStorms(t *testing.T) {
	if testing.Short() {
		t.Skip("storm test skipped in -short mode")
	}
	t.Parallel()

	storm := func(t *testing.T, workers int) {
		t.Helper()
		ft := tgutil.NewFakeTransport()
		ft.DefaultHandle = testHandle()
		h := &Handler{
			Pool:        pool.NewForTests(ft, tgutil.NewFakeTransport()), // 2 slots
			Store:       &fakeStore{rec: testRecorder()},
			Log:         quietStreamLogger(),
			Ingester:    &ingest.Ingester{},
			concurrency: 4,
			bufferCount: 8,
			timeout:     2 * time.Second,
			maxRetries:  2,
			vault:       newVaultCache(),
		}
		// Slow chunks so cancellations land mid-pipeline, plus a small tail
		// delay jitter to shuffle completion order.
		ft.SetFetchChunk(func(ctx context.Context, fh tgutil.FileHandle, offset int64, limit int32) ([]byte, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(15 * time.Millisecond):
			}
			return fakeTestChunk(fh, offset, limit), nil
		})

		const n = 200
		codes := make([]int, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ctx, cancel := context.WithCancel(context.Background())
				// Cancel a quarter of the requests almost immediately
				// (mid-resolve / mid-pipeline); let the rest complete.
				if i%4 == 0 {
					go func() {
						time.Sleep(time.Duration(i%7) * time.Millisecond)
						cancel()
					}()
				} else {
					defer cancel()
				}
				req := httptest.NewRequest(http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
				req = req.WithContext(WithToken(ctx, testToken16))
				if i%2 == 0 {
					req.Header.Set("Range", "bytes=1000000-2000000")
				}
				rr := httptest.NewRecorder()
				h.ServeHTTP(rr, req)
				codes[i] = rr.Code
			}(i)
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Logf("=== STORM WATCHDOG STACK DUMP ===\n%s", buf[:n])
			t.Fatal("storm deadlocked")
		}

		// THE pin: no slot leaks after the storm.
		if got := h.Pool.TotalInflight(); got != 0 {
			t.Fatalf("TotalInflight after storm = %d, want 0 (slot leak)", got)
		}
		for i, c := range codes {
			if c == 0 {
				t.Fatalf("request %d never produced a response", i)
			}
		}
		// The pool must still be usable afterwards: a clean request succeeds.
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
		req = req.WithContext(WithToken(req.Context(), testToken16))
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("post-storm healthy request = %d, want 200", rr.Code)
		}
	}

	t.Run("one wave", func(t *testing.T) { storm(t, 1) })
}

// TestFetchChunkStallTimeout pins the stall-timeout discipline: every
// FetchChunk call runs under a deadline (StreamTimeout), a wedged fetch is
// treated as transient, and the request surfaces a 503 with Retry-After
// instead of hanging.
func TestFetchChunkStallTimeout(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	ft.DefaultHandle = testHandle()
	h := &Handler{
		Pool:        pool.NewForTests(ft),
		Store:       &fakeStore{rec: testRecorder()},
		Log:         quietStreamLogger(),
		Ingester:    &ingest.Ingester{},
		concurrency: 1,
		bufferCount: 4,
		timeout:     60 * time.Millisecond,
		maxRetries:  1,
		vault:       newVaultCache(),
	}

	var deadlines []time.Duration
	ft.SetFetchChunk(func(ctx context.Context, fh tgutil.FileHandle, offset int64, limit int32) ([]byte, error) {
		if dl, ok := ctx.Deadline(); ok {
			deadlines = append(deadlines, time.Until(dl))
		}
		<-ctx.Done() // wedge until the stall timeout cuts us off
		return nil, ctx.Err()
	})

	start := time.Now()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	req = req.WithContext(WithToken(req.Context(), testToken16))
	h.ServeHTTP(rr, req)
	elapsed := time.Since(start)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 after stall timeout (body %q)", rr.Code, rr.Body.String())
	}
	if ra := rr.Header().Get("Retry-After"); ra == "" {
		t.Fatal("503 missing Retry-After")
	}
	// One chunk, two attempts (maxRetries=1): each bounded by the 60ms stall
	// timeout, separated by the 100ms base backoff.
	if elapsed < 2*60*time.Millisecond {
		t.Fatalf("returned in %v, want at least two stalled attempts (120ms)", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("returned in %v — retries did not bound the stall", elapsed)
	}
	// At least one deadline per attempt of the probed chunk (2 with
	// maxRetries=1); a racing worker may add one more before the abort lands —
	// the pinned property is that EVERY fetch runs under a deadline.
	if len(deadlines) < 2 {
		t.Fatalf("captured %d deadlines, want >= 2 (one per attempt)", len(deadlines))
	}
	for i, d := range deadlines {
		if d > 200*time.Millisecond {
			t.Fatalf("attempt %d deadline %v exceeds StreamTimeout window", i, d)
		}
	}
}

// TestFloodCooldownSkipsCoolingSlot pins that a transport which reported a
// FLOOD_WAIT stops receiving new leases while alternatives exist (flood
// mapping on every client, plan §4) — complementary to the pool suite.
func TestFloodCooldownSkipsCoolingSlot(t *testing.T) {
	t.Parallel()
	ftA := tgutil.NewFakeTransport()
	ftB := tgutil.NewFakeTransport()
	p := pool.NewForTests(ftA, ftB)

	p.ReportFlood(ftA, 120*time.Second)
	seen := map[any]int{}
	for i := 0; i < 20; i++ {
		lease, err := p.AcquireBest()
		if err != nil {
			t.Fatalf("AcquireBest %d: %v", i, err)
		}
		seen[lease.Transport()]++
		lease.Release()
	}
	if seen[ftA] != 0 {
		t.Fatalf("cooling slot leased %d times, want 0", seen[ftA])
	}
	if seen[ftB] != 20 {
		t.Fatalf("healthy slot leased %d times, want 20", seen[ftB])
	}
}

// TestPoolExhaustionIsImmediate pins the no-queue admission rule: when every
// slot is at the hard cap, AcquireBest fails fast (the HTTP layer maps that
// to 503 + Retry-After) instead of growing an unbounded wait queue.
func TestPoolExhaustionIsImmediate(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	p := pool.NewForTests(ft)

	leases := make([]*pool.Lease, 0, 8)
	for i := 0; i < 8; i++ {
		lease, err := p.AcquireBest()
		if err != nil {
			t.Fatalf("lease %d: %v", i, err)
		}
		leases = append(leases, lease)
	}

	done := make(chan error, 1)
	go func() {
		_, err := p.AcquireBest()
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, pool.ErrPoolCapacity) {
			t.Fatalf("overflow error = %v, want ErrPoolCapacity", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AcquireBest blocked past the hard cap — queuing regression")
	}
	for _, l := range leases {
		l.Release()
	}
}
