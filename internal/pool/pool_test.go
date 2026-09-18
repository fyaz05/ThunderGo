package pool

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// These tests exercise the fleet logic (least-loaded acquisition, hard cap,
// flood cooldowns, counters, lifecycle) on tgutil.FakeTransport — no network,
// no mtgo, no gogram. Pool is constructed white-box (same package): slots are
// built with the production newSlot helper so counters/semaphores/DC caches
// match what New would produce.

// quietLogger returns a logger that discards all output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakePool builds a Pool with n FakeTransport slots. maxConcurrent defaults
// to 8 (matching production); tests may override it after construction.
func fakePool(n int) *Pool {
	p := &Pool{
		maxConcurrent: 8,
		log:           quietLogger(),
	}
	for i := 0; i < n; i++ {
		ft := tgutil.NewFakeTransport()
		ft.FakeDC = i + 2 // distinct DCs for PerClientDC assertions
		p.slots = append(p.slots, newSlot(ft, i, 8))
	}
	return p
}

// transportOf returns slot i's transport as *tgutil.FakeTransport.
func transportOf(t *testing.T, p *Pool, i int) *tgutil.FakeTransport {
	t.Helper()
	ft, ok := p.Transport(i).(*tgutil.FakeTransport)
	if !ok || ft == nil {
		t.Fatalf("Transport(%d) is not a *tgutil.FakeTransport", i)
	}
	return ft
}

// ---------- AcquireBest: least-loaded selection ----------

func TestAcquireBestLeastLoaded(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	p.slots[0].inflight.Store(5)
	p.slots[1].inflight.Store(2)
	p.slots[2].inflight.Store(8)

	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() error: %v", err)
	}
	defer lease.Release()
	if lease.slot != p.slots[1] {
		t.Errorf("AcquireBest() = slot[%d], want slot[1] (least loaded)", lease.slot.index)
	}
	if got := p.slots[1].inflight.Load(); got != 3 {
		t.Errorf("after acquire, inflight = %d, want 3 (was 2, +1)", got)
	}
}

func TestAcquireBestSkipsCoolingSlots(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	// Slot 0 has the lowest load but is flood-cooling; slot 1 must win.
	p.slots[0].inflight.Store(0)
	p.slots[0].floodUntil.Store(time.Now().Add(time.Hour).Unix())
	p.slots[1].inflight.Store(4)

	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() error: %v", err)
	}
	defer lease.Release()
	if lease.slot != p.slots[1] {
		t.Errorf("AcquireBest() = slot[%d], want slot[1] (slot 0 cooling)", lease.slot.index)
	}
}

// ---------- AcquireBest: hard cap + typed errors ----------

func TestAcquireBestCapacityError(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	p.maxConcurrent = 1
	p.slots[0].inflight.Store(1)
	p.slots[1].inflight.Store(1)

	lease, err := p.AcquireBest()
	if lease != nil {
		t.Fatalf("AcquireBest() = %v, want nil at capacity", lease)
	}
	if !errors.Is(err, ErrPoolCapacity) {
		t.Errorf("err = %v, want ErrPoolCapacity", err)
	}
	var pe *poolError
	if !errors.As(err, &pe) || pe.RetryAfter() != 0 {
		t.Errorf("RetryAfter on capacity = %v, want 0", err)
	}
}

func TestAcquireBestBrownoutError(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	p.slots[0].floodUntil.Store(time.Now().Add(90 * time.Second).Unix())
	p.slots[1].floodUntil.Store(time.Now().Add(30 * time.Second).Unix())

	lease, err := p.AcquireBest()
	if lease != nil {
		t.Fatalf("AcquireBest() = %v, want nil during brownout", lease)
	}
	if !errors.Is(err, ErrPoolBrownout) {
		t.Errorf("err = %v, want ErrPoolBrownout", err)
	}
	// The error carries the minimum remaining wait (30s), and the pool
	// remembers it for BrownoutRetryAfter.
	var pe *poolError
	if !errors.As(err, &pe) || pe.RetryAfter() != 30 {
		t.Errorf("RetryAfter on brownout = %v, want 30", err)
	}
	if got := p.BrownoutRetryAfter(); got != 30 {
		t.Errorf("BrownoutRetryAfter() = %d, want 30", got)
	}
}

func TestAcquireBestEmptyPoolIsCapacity(t *testing.T) {
	t.Parallel()
	p := fakePool(0)
	lease, err := p.AcquireBest()
	if lease != nil {
		t.Fatalf("AcquireBest() on empty pool = %v, want nil", lease)
	}
	if !errors.Is(err, ErrPoolCapacity) {
		t.Errorf("err = %v, want ErrPoolCapacity (an empty fleet is not a brownout)", err)
	}
}

func TestAcquireBestNeverBlocks(t *testing.T) {
	t.Parallel()
	// Saturate every slot; AcquireBest must return promptly with a typed
	// error rather than queueing behind the cap.
	p := fakePool(3)
	p.maxConcurrent = 2
	for _, s := range p.slots {
		s.inflight.Store(2)
	}
	done := make(chan struct{})
	go func() {
		_, err := p.AcquireBest()
		if !errors.Is(err, ErrPoolCapacity) {
			t.Errorf("err = %v, want ErrPoolCapacity", err)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("AcquireBest() blocked at capacity; must fail fast")
	}
}

// ---------- Lease.Release ----------

func TestReleaseBumpsCounterDown(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() error: %v", err)
	}
	if got := p.TotalInflight(); got != 1 {
		t.Fatalf("TotalInflight after acquire = %d, want 1", got)
	}
	lease.Release()
	if got := p.TotalInflight(); got != 0 {
		t.Errorf("TotalInflight after release = %d, want 0", got)
	}
}

func TestReleaseIdempotent(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() error: %v", err)
	}
	lease.Release()
	lease.Release() // must NOT drive counter negative
	lease.Release() // belt and suspenders
	if got := p.slots[0].inflight.Load(); got != 0 {
		t.Errorf("after triple release, inflight = %d, want 0 (idempotent)", got)
	}
	// Releasing a nil lease is also safe.
	var nilLease *Lease
	nilLease.Release()
}

func TestReleaseConcurrentIsIdempotent(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() error: %v", err)
	}
	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			lease.Release()
		}()
	}
	wg.Wait()
	if got := p.slots[0].inflight.Load(); got != 0 {
		t.Errorf("after %d concurrent releases, inflight = %d, want 0", n, got)
	}
}

// ---------- Lease.Transport / DC / AcquireLookup ----------

func TestLeaseTransportReturnsSlotTransport(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() error: %v", err)
	}
	defer lease.Release()
	if lease.Transport() != p.slots[0].t {
		t.Errorf("Transport() != slot 0 transport")
	}
}

func TestLeaseDCCachedFromOwnDC(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() error: %v", err)
	}
	defer lease.Release()
	if got := lease.DC(); got != 2 {
		t.Errorf("DC() = %d, want 2 (FakeTransport default after ownDC cache)", got)
	}
}

func TestLeaseAcquireLookup(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() error: %v", err)
	}
	defer lease.Release()

	release, ok := lease.AcquireLookup(context.Background())
	if !ok {
		t.Fatal("first AcquireLookup() = false, want true")
	}

	// The semaphore has capacity 1: a cancelled context must not steal it.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := lease.AcquireLookup(ctx); ok {
		t.Fatal("AcquireLookup() succeeded with cancelled context while slot held")
	}

	release()
	release() // idempotent
	release2, ok := lease.AcquireLookup(context.Background())
	if !ok {
		t.Fatal("AcquireLookup() = false after release, want true")
	}
	release2()
}

// ---------- ReportFlood + sweep ----------

func TestReportFloodSetsCooldownAndSkips(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	ft := transportOf(t, p, 0)
	p.ReportFlood(ft, 60*time.Second)

	if got := p.slots[0].floodUntil.Load(); got == 0 {
		t.Fatal("floodUntil not set after ReportFlood")
	}
	if got := p.slots[0].consecutiveFloods.Load(); got != 1 {
		t.Errorf("consecutiveFloods = %d, want 1", got)
	}
	// Slot 0 cooling → acquisition must land on slot 1.
	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() error: %v", err)
	}
	defer lease.Release()
	if lease.slot != p.slots[1] {
		t.Errorf("AcquireBest() = slot[%d], want slot[1] (slot 0 cooling)", lease.slot.index)
	}
}

func TestReportFloodCapsWaitAt600s(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	p.ReportFlood(transportOf(t, p, 0), time.Hour)
	remaining := p.slots[0].floodUntil.Load() - time.Now().Unix()
	if remaining > maxFloodCooldownSecs {
		t.Errorf("cooldown = %ds, want capped at %ds", remaining, maxFloodCooldownSecs)
	}
}

func TestReportFloodUnknownTransportIsNoop(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	p.ReportFlood(tgutil.NewFakeTransport(), time.Minute) // not a pool member
	p.ReportFlood(nil, time.Minute)
	if p.slots[0].floodUntil.Load() != 0 {
		t.Errorf("floodUntil set by unknown transport report")
	}
}

func TestFloodSweepExpiresCooldown(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.StartFloodSweep(ctx, 10*time.Millisecond)

	// A 1-second cooldown would normally outlive the test; the sweep
	// must not clear it early.
	p.ReportFlood(transportOf(t, p, 0), time.Second)
	time.Sleep(30 * time.Millisecond)
	if p.slots[0].floodUntil.Load() == 0 {
		t.Fatal("sweep cleared a still-active cooldown")
	}

	// An EXPIRED cooldown must be cleared (and the consecutive counter
	// reset) by the sweep.
	p.slots[1].floodUntil.Store(time.Now().Add(-time.Second).Unix())
	p.slots[1].consecutiveFloods.Store(3)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.slots[1].floodUntil.Load() == 0 && p.slots[1].consecutiveFloods.Load() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if p.slots[1].floodUntil.Load() != 0 || p.slots[1].consecutiveFloods.Load() != 0 {
		t.Errorf("sweep did not reset expired cooldown: until=%d floods=%d",
			p.slots[1].floodUntil.Load(), p.slots[1].consecutiveFloods.Load())
	}
}

func TestBrownoutRecoversAfterCooldownExpiry(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	p.slots[0].floodUntil.Store(time.Now().Add(10 * time.Second).Unix())
	if _, err := p.AcquireBest(); !errors.Is(err, ErrPoolBrownout) {
		t.Fatalf("err = %v, want ErrPoolBrownout while cooling", err)
	}
	// Cooldown expires (simulating a sweep without changing the counter
	// discipline) → acquisition works again.
	p.slots[0].floodUntil.Store(time.Now().Add(-time.Second).Unix())
	lease, err := p.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest() after expiry: %v", err)
	}
	lease.Release()
}

// ---------- Counters ----------

func TestTotalInflight(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	p.slots[0].inflight.Store(1)
	p.slots[1].inflight.Store(2)
	p.slots[2].inflight.Store(3)
	if got := p.TotalInflight(); got != 6 {
		t.Errorf("TotalInflight() = %d, want 6", got)
	}
}

func TestPerClientInflight(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	p.slots[0].inflight.Store(10)
	p.slots[1].inflight.Store(20)
	p.slots[2].inflight.Store(30)
	got := p.PerClientInflight()
	if len(got) != 3 {
		t.Fatalf("PerClientInflight() len = %d, want 3", len(got))
	}
	want := []int64{10, 20, 30}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("PerClientInflight()[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestPerClientDC(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	got := p.PerClientDC()
	if len(got) != 3 {
		t.Fatalf("PerClientDC() len = %d, want 3", len(got))
	}
	for i, want := range []int{2, 3, 4} {
		if got[i] != want {
			t.Errorf("PerClientDC()[%d] = %d, want %d", i, got[i], want)
		}
	}
}

// Zero-value Pool must be safe for the stats surfaces the bot reads
// (bot_test.go builds &pool.Pool{} directly).
func TestZeroValuePoolStats(t *testing.T) {
	t.Parallel()
	var p Pool
	if got := p.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}
	if got := p.TotalInflight(); got != 0 {
		t.Errorf("TotalInflight() = %d, want 0", got)
	}
	if got := len(p.PerClientInflight()); got != 0 {
		t.Errorf("PerClientInflight() len = %d, want 0", got)
	}
	if got := len(p.PerClientDC()); got != 0 {
		t.Errorf("PerClientDC() len = %d, want 0", got)
	}
	if p.Primary() != nil {
		t.Errorf("Primary() on empty pool = %v, want nil", p.Primary())
	}
	// Stop on a zero-value pool is a no-op (no panic).
	p.Stop(context.Background())
}

// ---------- Primary / Transport ----------

func TestPrimaryReturnsBotBackend(t *testing.T) {
	t.Parallel()
	p := fakePool(2)
	primary := p.Primary()
	if primary == nil {
		t.Fatal("Primary() = nil, want the slot-0 backend")
	}
	if _, ok := primary.(*tgutil.FakeTransport); !ok {
		t.Errorf("Primary() = %T, want *tgutil.FakeTransport", primary)
	}
}

func TestTransportOutOfRange(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	if p.Transport(-1) != nil || p.Transport(1) != nil || p.Transport(0) == nil {
		t.Errorf("Transport() out-of-range handling wrong")
	}
}

// ---------- Stop ----------

func TestStopStopsAllTransportsIdempotent(t *testing.T) {
	t.Parallel()
	p := fakePool(3)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p.StartFloodSweep(ctx, time.Hour) // sweep must be stopped by Stop
	p.Stop(ctx)
	p.Stop(ctx) // must not panic (idempotent)

	for i := range p.slots {
		ft := transportOf(t, p, i)
		if len(ft.CallsOf(tgutil.CallStop)) != 1 {
			t.Errorf("slot %d stop calls = %d, want exactly 1", i, len(ft.CallsOf(tgutil.CallStop)))
		}
	}
}

func TestStopBoundedByContext(t *testing.T) {
	t.Parallel()
	p := fakePool(1)
	ft := transportOf(t, p, 0)
	// A transport whose Stop never returns must not hang pool.Stop past
	// the caller's deadline.
	ft.StopErr = errors.New("stop blocked") // Stop returns the error but a slow transport needs a ctx bound:
	// FakeTransport.Stop returns immediately; simulate a stuck transport
	// by wrapping it.
	slow := &stuckTransport{inner: ft}
	p.slots[0].t = slow
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	p.Stop(ctx)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Stop took %v, want bounded by the 100ms ctx", elapsed)
	}
}

// stuckTransport wraps a transport with a Stop that never returns.
type stuckTransport struct{ inner tgutil.Transport }

func (s *stuckTransport) Start(ctx context.Context) error { return s.inner.Start(ctx) }

// Stop blocks forever (testing the ctx bound in Pool.Stop).
func (s *stuckTransport) Stop() error {
	<-make(chan struct{})
	return nil
}
func (s *stuckTransport) OnCommand(cmd string, h func(ctx context.Context, m tgutil.IncomingMsg) error) {
	s.inner.OnCommand(cmd, h)
}
func (s *stuckTransport) OnCallback(prefix string, h func(ctx context.Context, q tgutil.CallbackQuery) error) {
	s.inner.OnCallback(prefix, h)
}
func (s *stuckTransport) SendText(ctx context.Context, chatID int64, text string, markup any) error {
	return s.inner.SendText(ctx, chatID, text, markup)
}
func (s *stuckTransport) ResolveMedia(ctx context.Context, chatID int64, msgID int) (tgutil.FileHandle, error) {
	return s.inner.ResolveMedia(ctx, chatID, msgID)
}
func (s *stuckTransport) FetchChunk(ctx context.Context, fh tgutil.FileHandle, offset int64, limit int32) ([]byte, error) {
	return s.inner.FetchChunk(ctx, fh, offset, limit)
}
func (s *stuckTransport) RefreshFileRef(ctx context.Context, fh *tgutil.FileHandle) error {
	return s.inner.RefreshFileRef(ctx, fh)
}

// ---------- Concurrency (run under -race) ----------

func TestConcurrentAcquireRelease(t *testing.T) {
	t.Parallel()
	p := fakePool(5)
	const goroutines = 100
	const cycles = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < cycles; j++ {
				lease, err := p.AcquireBest()
				if err != nil {
					t.Errorf("AcquireBest() error: %v", err)
					return
				}
				_ = lease.Transport()
				lease.Release()
			}
		}()
	}
	wg.Wait()

	if got := p.TotalInflight(); got != 0 {
		t.Errorf("TotalInflight after concurrent test = %d, want 0", got)
	}
	for i, n := range p.PerClientInflight() {
		if n != 0 {
			t.Errorf("PerClientInflight[%d] = %d, want 0", i, n)
		}
	}
}

func TestConcurrentAcquireRespectsHardCap(t *testing.T) {
	t.Parallel()
	// Hammer AcquireBest from many goroutines with cap 2: at no point may
	// a slot's inflight exceed the cap.
	p := fakePool(3)
	p.maxConcurrent = 2

	var mu sync.Mutex
	overflow := 0
	var wg sync.WaitGroup
	wg.Add(64)
	for i := 0; i < 64; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				lease, err := p.AcquireBest()
				if err != nil {
					continue // capacity is fine under contention
				}
				load := lease.slot.inflight.Load()
				if load > int64(p.maxConcurrent) {
					mu.Lock()
					overflow++
					mu.Unlock()
				}
				lease.Release()
			}
		}()
	}
	wg.Wait()
	if overflow > 0 {
		t.Errorf("hard cap exceeded %d times", overflow)
	}
}
