package pool

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// These tests exercise the in-flight counter logic of the pool without
// connecting to Telegram. We construct a *Pool directly (white-box, same
// package) with fake Clients whose embedded *telegram.Client is nil. The
// counter methods (Pick, Acquire, TotalInflight, PerClientInflight, Len,
// All, Primary) never touch the embedded telegram.Client, so a nil
// pointer is safe. Stop is exercised on an empty pool to avoid the
// nil-pointer dereference that would occur if client.Stop() were invoked
// on a fake Client.

// fakePool builds a Pool with n fake Clients (nil *telegram.Client). The
// Inflight counters start at zero. maxConcurrent defaults to 8 (matching
// production); tests may override it after construction.
func fakePool(n int) *Pool {
	p := &Pool{
		stopped:       make(chan struct{}),
		maxConcurrent: 8,
	}
	for i := 0; i < n; i++ {
		p.all = append(p.all, &Client{})
	}
	if n > 0 {
		p.primary = p.all[0]
	}
	return p
}

// quietLogger returns a logger that discards all output, keeping the test
// run free of flood-wait warning spam.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// indexOf returns the index of c in s, or -1 if c is nil / not found.
func indexOf(s []*Client, c *Client) int {
	if c == nil {
		return -1
	}
	for i, e := range s {
		if e == c {
			return i
		}
	}
	return -1
}

// ---------- Pick ----------

func TestPickLeastLoaded(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	p.all[0].Inflight.Store(5)
	p.all[1].Inflight.Store(2)
	p.all[2].Inflight.Store(8)

	got := p.Pick()
	if got != p.all[1] {
		t.Errorf("Pick() = client[%d] (inflight=%d); want client[1] (inflight=2)",
			indexOf(p.all, got), got.Inflight.Load())
	}
}

func TestPickEmptyPool(t *testing.T) {
	t.Parallel()
	p := fakePool(0)
	if c := p.Pick(); c != nil {
		t.Errorf("Pick() on empty pool = %v; want nil", c)
	}
}

func TestPickAllAtCapacityReturnsNil(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	p.maxConcurrent = 2
	p.all[0].Inflight.Store(2)
	p.all[1].Inflight.Store(3)

	if got := p.Pick(); got != nil {
		t.Errorf("Pick() = client[%d]; want nil when every client is at capacity", indexOf(p.all, got))
	}
}

func TestAcquireAllAtCapacityReturnsNil(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	p.maxConcurrent = 1
	p.all[0].Inflight.Store(1)
	p.all[1].Inflight.Store(1)

	client, release := p.AcquireBest()
	if client != nil {
		t.Fatalf("AcquireBest() = %v, want nil when every client is at cap", client)
	}
	// The no-client release remains safe for uniform deferred cleanup.
	release()
}

func TestPickPrefersUnderCap(t *testing.T) {
	t.Parallel()
	// One client under cap (1 < 8), another far lower but at cap.
	// Pass 1 must pick the under-cap client even though its raw count
	// is higher than the at-cap client's.
	p := fakePool(2)
	p.maxConcurrent = 8
	p.all[0].Inflight.Store(0) // at cap? no — 0 < 8
	p.all[1].Inflight.Store(8) // exactly at cap

	got := p.Pick()
	if got != p.all[0] {
		t.Errorf("Pick() = client[%d]; want client[0] (under cap, picked in pass 1)",
			indexOf(p.all, got))
	}
}

// ---------- Acquire / Release ----------

func TestAcquireBumpsCounter(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	c, release := p.Acquire()
	if c == nil {
		t.Fatalf("Acquire() = nil; want client")
	}
	if got := c.Inflight.Load(); got != 1 {
		t.Errorf("after Acquire, Inflight = %d; want 1", got)
	}
	release()
	if got := c.Inflight.Load(); got != 0 {
		t.Errorf("after release, Inflight = %d; want 0", got)
	}
}

func TestAcquirePicksLeastLoaded(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	p.all[0].Inflight.Store(5)
	p.all[1].Inflight.Store(2)
	p.all[2].Inflight.Store(8)

	c, release := p.Acquire()
	defer release()
	if c != p.all[1] {
		t.Fatalf("Acquire() picked client[%d]; want client[1] (least loaded)",
			indexOf(p.all, c))
	}
	if got := p.all[1].Inflight.Load(); got != 3 {
		t.Errorf("after Acquire, all[1].Inflight = %d; want 3 (was 2, +1)", got)
	}
}

func TestAcquireReleaseIdempotent(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	c, release := p.Acquire()
	if c == nil {
		t.Fatalf("Acquire() = nil")
	}
	release()
	release() // must NOT drive counter negative
	release() // belt and suspenders
	if got := c.Inflight.Load(); got != 0 {
		t.Errorf("after triple release, Inflight = %d; want 0 (idempotent)", got)
	}
}

func TestAcquireEmptyPool(t *testing.T) {
	t.Parallel()
	p := fakePool(0)
	c, release := p.Acquire()
	if c != nil {
		t.Errorf("Acquire() on empty pool = %v; want nil", c)
	}
	// The returned release must be a safe no-op.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("release() on empty pool panicked: %v", r)
		}
	}()
	release()
}

func TestAcquireMultipleConcurrentReleases(t *testing.T) {
	t.Parallel()
	// Concurrent release calls must collapse via sync.Once — counter
	// lands at exactly 0, never negative.
	p := fakePool(1)
	c, release := p.Acquire()
	if c == nil {
		t.Fatalf("Acquire() = nil")
	}
	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			release()
		}()
	}
	wg.Wait()
	if got := c.Inflight.Load(); got != 0 {
		t.Errorf("after %d concurrent releases, Inflight = %d; want 0", n, got)
	}
}

// ---------- TotalInflight / PerClientInflight ----------

func TestTotalInflight(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	p.all[0].Inflight.Store(1)
	p.all[1].Inflight.Store(2)
	p.all[2].Inflight.Store(3)
	if got := p.TotalInflight(); got != 6 {
		t.Errorf("TotalInflight() = %d; want 6", got)
	}
}

func TestTotalInflightEmpty(t *testing.T) {
	t.Parallel()
	p := fakePool(0)
	if got := p.TotalInflight(); got != 0 {
		t.Errorf("TotalInflight() on empty pool = %d; want 0", got)
	}
}

func TestPerClientInflight(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	p.all[0].Inflight.Store(10)
	p.all[1].Inflight.Store(20)
	p.all[2].Inflight.Store(30)
	got := p.PerClientInflight()
	if len(got) != 3 {
		t.Fatalf("PerClientInflight() len = %d; want 3", len(got))
	}
	want := []int64{10, 20, 30}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("PerClientInflight()[%d] = %d; want %d", i, got[i], want[i])
		}
	}
}

func TestPerClientInflightEmpty(t *testing.T) {
	t.Parallel()
	p := fakePool(0)
	got := p.PerClientInflight()
	if len(got) != 0 {
		t.Errorf("PerClientInflight() on empty pool len = %d; want 0", len(got))
	}
}

// ---------- Len / All / Primary ----------

func TestLenAndAllAndPrimary(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	if got := p.Len(); got != 3 {
		t.Errorf("Len() = %d; want 3", got)
	}
	all := p.All()
	if len(all) != 3 {
		t.Fatalf("All() len = %d; want 3", len(all))
	}
	for i, c := range all {
		if c != p.all[i] {
			t.Errorf("All()[%d] != all[%d]", i, i)
		}
	}
	if p.Primary() == nil {
		t.Fatalf("Primary() = nil; want non-nil")
	}
	if p.Primary() != p.all[0] {
		t.Errorf("Primary() != all[0]")
	}
}

func TestLenAndPrimaryEmpty(t *testing.T) {
	t.Parallel()
	p := fakePool(0)
	if got := p.Len(); got != 0 {
		t.Errorf("Len() on empty pool = %d; want 0", got)
	}
	if got := p.All(); len(got) != 0 {
		t.Errorf("All() on empty pool len = %d; want 0", len(got))
	}
	if p.Primary() != nil {
		t.Errorf("Primary() on empty pool = %v; want nil", p.Primary())
	}
}

// ---------- Vault lookup limiter ----------

func TestAcquireLookupHonorsContextAndRelease(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	p.all[0].lookupSem = make(chan struct{}, 1)

	release, ok := p.all[0].AcquireLookup(context.Background())
	if !ok {
		t.Fatal("first AcquireLookup() = false, want true")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := p.all[0].AcquireLookup(ctx); ok {
		t.Fatal("AcquireLookup() succeeded with a cancelled context while slot is held")
	}

	release()
	release() // release is idempotent
	release2, ok := p.all[0].AcquireLookup(context.Background())
	if !ok {
		t.Fatal("AcquireLookup() = false after release, want true")
	}
	release2()
}

// ---------- makeFloodHandler ----------

func TestFloodHandlerNilError(t *testing.T) {
	t.Parallel()
	stopped := make(chan struct{})
	h := makeFloodHandler(quietLogger(), 0, stopped)
	if h(nil) != false {
		t.Errorf("flood handler with nil err = true; want false")
	}
}

func TestFloodHandlerNonFloodError(t *testing.T) {
	t.Parallel()
	stopped := make(chan struct{})
	h := makeFloodHandler(quietLogger(), 0, stopped)
	err := fmt.Errorf("rpc error: 500 INTERNAL")
	if h(err) != false {
		t.Errorf("flood handler with non-flood err = true; want false")
	}
}

func TestFloodHandlerInterruptible(t *testing.T) {
	t.Parallel()
	// Close the stopped channel first. The handler's select must fire on
	// <-stopped and return false WITHOUT sleeping for 60 seconds.
	stopped := make(chan struct{})
	close(stopped)
	h := makeFloodHandler(quietLogger(), 0, stopped)

	err := fmt.Errorf("rpc error: 420 FLOOD_WAIT_60")
	start := time.Now()
	got := h(err)
	elapsed := time.Since(start)

	if got != false {
		t.Errorf("flood handler with closed stopped = %v; want false (interrupted)", got)
	}
	if elapsed > time.Second {
		t.Errorf("flood handler slept %v; want <1s (interrupted by stopped)", elapsed)
	}
}

func TestFloodHandlerOverMaxWait(t *testing.T) {
	t.Parallel()
	// Wait > maxFloodWaitSecs (600) → handler refuses to retry, returns
	// false immediately, without sleeping (and without depending on the
	// stopped channel being closed).
	stopped := make(chan struct{}) // intentionally left open
	h := makeFloodHandler(quietLogger(), 0, stopped)
	err := fmt.Errorf("FLOOD_WAIT_3600")

	start := time.Now()
	got := h(err)
	elapsed := time.Since(start)

	if got != false {
		t.Errorf("flood handler with 3600s wait = true; want false (over max)")
	}
	if elapsed > time.Second {
		t.Errorf("flood handler took %v; want <1s (should not sleep)", elapsed)
	}
}

func TestFloodHandlerZeroWait(t *testing.T) {
	t.Parallel()
	// GetFloodWait returns 0 for an unparseable error → handler returns
	// false immediately (covers the wait <= 0 branch).
	stopped := make(chan struct{})
	h := makeFloodHandler(quietLogger(), 0, stopped)
	err := fmt.Errorf("FLOOD_WAIT_0") // parses to 0
	if h(err) != false {
		t.Errorf("flood handler with FLOOD_WAIT_0 = true; want false (wait <= 0)")
	}
}

// ---------- Stop ----------

func TestStopIdempotent(t *testing.T) {
	t.Parallel()
	// Use an empty pool: Stop only closes `stopped` and iterates over
	// (no) clients. Calling twice must not panic on double-close
	// (guarded by stopOnce). A non-empty pool with nil telegram.Client
	// is unsafe here because client.Stop() would dereference nil.
	p := fakePool(0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p.Stop(ctx)
	p.Stop(ctx) // must not panic

	select {
	case <-p.stopped:
		// good — closed
	default:
		t.Errorf("stopped channel not closed after Stop")
	}
}

func TestStopContextCancelled(t *testing.T) {
	t.Parallel()
	// Even with an already-cancelled context, Stop on an empty pool
	// must return promptly and close `stopped`.
	p := fakePool(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p.Stop(ctx) // must not block / panic

	select {
	case <-p.stopped:
	default:
		t.Errorf("stopped channel not closed after Stop with cancelled ctx")
	}
}

// ---------- Concurrency (run under -race) ----------

func TestConcurrentAcquireRelease(t *testing.T) {
	t.Parallel()
	// 100 goroutines × 100 Acquire/release cycles across 5 clients.
	// After all goroutines finish, every Inflight counter must be 0
	// (idempotent release + matching Acquire/release pairs).
	p := fakePool(5)
	const goroutines = 100
	const cycles = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < cycles; j++ {
				c, release := p.Acquire()
				if c == nil {
					t.Errorf("Acquire() = nil; want client")
					return
				}
				// Exercise a read under the race detector.
				_ = c.Inflight.Load()
				release()
			}
		}()
	}
	wg.Wait()

	if got := p.TotalInflight(); got != 0 {
		t.Errorf("TotalInflight after concurrent test = %d; want 0", got)
	}
	for i, n := range p.PerClientInflight() {
		if n != 0 {
			t.Errorf("PerClientInflight[%d] = %d; want 0", i, n)
		}
	}
}

func TestConcurrentPick(t *testing.T) {
	t.Parallel()
	// Pure Pick() stress — verifies concurrent reads of Inflight are
	// race-free. Counters are set once before the goroutines start, so
	// the expected pick (all[0]) is stable.
	p := fakePool(8)
	for i := range p.all {
		p.all[i].Inflight.Store(int64(i)) // 0..7 → all[0] is least loaded
	}
	const goroutines = 50
	const iterations = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				c := p.Pick()
				if c == nil {
					t.Errorf("Pick() = nil")
					return
				}
			}
		}()
	}
	wg.Wait()
}
