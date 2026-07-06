package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLimiter_Disabled(t *testing.T) {
	// A limiter with cap <= 0 is disabled — everything passes.
	l := New(0, time.Minute)
	for i := 0; i < 100; i++ {
		if !l.Allow(1) {
			t.Fatalf("disabled limiter should always Allow, rejected at i=%d", i)
		}
	}
}

func TestLimiter_UnderCap(t *testing.T) {
	l := New(5, time.Minute)
	for i := 0; i < 5; i++ {
		if !l.Allow(1) {
			t.Fatalf("user under cap should be allowed, rejected at i=%d", i)
		}
	}
}

func TestLimiter_OverCap(t *testing.T) {
	l := New(3, time.Minute)
	for i := 0; i < 3; i++ {
		l.Allow(1)
	}
	if l.Allow(1) {
		t.Fatal("user over cap should be rejected")
	}
}

func TestLimiter_DifferentUsers(t *testing.T) {
	l := New(2, time.Minute)
	// User 1 uses their allowance.
	l.Allow(1)
	l.Allow(1)
	// User 2 should still be allowed — caps are per-user.
	if !l.Allow(2) {
		t.Fatal("different user should not be affected by another user's cap")
	}
}

func TestLimiter_WindowReset(t *testing.T) {
	// GCRA: after the window elapses, the TAT is stale and the user gets a
	// fresh allowance.
	l := New(2, 50*time.Millisecond)
	l.Allow(1)
	l.Allow(1)
	if l.Allow(1) {
		t.Fatal("should be rejected before window resets")
	}
	time.Sleep(60 * time.Millisecond)
	if !l.Allow(1) {
		t.Fatal("should be allowed after window resets")
	}
}

func TestLimiter_Reset(t *testing.T) {
	l := New(2, time.Minute)
	l.Allow(1)
	l.Allow(1)
	if l.Allow(1) {
		t.Fatal("should be rejected at cap")
	}
	l.Reset(1)
	if !l.Allow(1) {
		t.Fatal("should be allowed after Reset")
	}
}

func TestLimiter_Sweep(t *testing.T) {
	l := New(5, 50*time.Millisecond)
	l.Allow(1)
	l.Allow(2)
	l.Allow(3)
	if got := l.Len(); got != 3 {
		t.Fatalf("expected 3 entries, got %d", got)
	}
	time.Sleep(60 * time.Millisecond)
	l.Sweep()
	if got := l.Len(); got != 0 {
		t.Fatalf("expected 0 entries after sweep, got %d", got)
	}
}

func TestLimiter_NilSafe(t *testing.T) {
	// All methods should be nil-safe (no panics).
	var l *Limiter
	if l.Allow(1) != true {
		t.Fatal("nil limiter should Allow")
	}
	l.Sweep()
	l.Reset(1)
}

// TestLimiter_AllowN tests batched consumption (D-022).
func TestLimiter_AllowN(t *testing.T) {
	l := New(10, time.Minute)
	// Consume 5 atomically.
	if ok, _ := l.AllowN(1, 5); !ok {
		t.Fatal("AllowN(5) should succeed with cap 10")
	}
	// 5 more should work.
	if ok, _ := l.AllowN(1, 5); !ok {
		t.Fatal("AllowN(5) should succeed (5+5=10)")
	}
	// 1 more should fail.
	if ok, _ := l.AllowN(1, 1); ok {
		t.Fatal("AllowN(1) should fail at cap")
	}
	// 11 should never work.
	if ok, _ := l.AllowN(1, 11); ok {
		t.Fatal("AllowN(11) should fail with cap 10")
	}
}

// TestLimiter_RetryAfter tests that AllowN returns a non-zero retry-after
// when rejected (D-026).
func TestLimiter_RetryAfter(t *testing.T) {
	l := New(2, 100*time.Millisecond)
	l.Allow(1)
	l.Allow(1)
	ok, retry := l.AllowN(1, 1)
	if ok {
		t.Fatal("should be rejected at cap")
	}
	if retry <= 0 {
		t.Fatal("retry-after should be positive when rejected")
	}
}

// TestLimiter_Concurrent stress-tests the limiter under concurrent access
// with the race detector (D-015). Run with: go test -race -count=10.
func TestLimiter_Concurrent(t *testing.T) {
	l := New(100, time.Minute)
	var wg sync.WaitGroup
	const goroutines = 100
	const perGoroutine = 100
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(uid int64) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				l.Allow(uid)
			}
		}(int64(i))
	}
	wg.Wait()
	// Each user should have been allowed at most `cap` times.
	// We can't assert exact counts because GCRA allows bursts up to cap,
	// but we can assert no panics and no negative TATs.
}

// TestLimiter_NoBoundaryBurst verifies GCRA eliminates the 2× boundary
// burst that fixed-window suffers from (D-003).
func TestLimiter_NoBoundaryBurst(t *testing.T) {
	// With cap=5 and window=100ms, a fixed-window limiter would allow 5 at
	// t=99ms and 5 at t=101ms (10 in 2ms). GCRA should reject the 6th
	// request even at the boundary because the TAT has advanced.
	l := New(5, 100*time.Millisecond)
	for i := 0; i < 5; i++ {
		if !l.Allow(1) {
			t.Fatalf("first %d should be allowed", i+1)
		}
	}
	// 6th should be rejected immediately — no 2× burst.
	if l.Allow(1) {
		t.Fatal("6th request should be rejected (no boundary burst)")
	}
}

// --- GlobalLimiter tests (audit 7.1-7.4) ---

// TestGlobalLimiter_Disabled verifies NewGlobal returns nil when rps<=0
// (audit 7.4 — rate.Limit(0) would create a never-allow limiter, so we
// short-circuit instead).
func TestGlobalLimiter_Disabled(t *testing.T) {
	if g := NewGlobal(0, 0); g != nil {
		t.Fatalf("NewGlobal(0, 0) should return nil, got %v", g)
	}
	if g := NewGlobal(-1, 0); g != nil {
		t.Fatalf("NewGlobal(-1, 0) should return nil, got %v", g)
	}
}

// TestGlobalLimiter_NilSafe verifies the Allow and RetryAfter methods
// don't panic on a nil receiver (defensive — callers should nil-check,
// but the methods must still be safe).
func TestGlobalLimiter_NilSafe(t *testing.T) {
	var g *GlobalLimiter
	if !g.Allow() {
		t.Fatal("nil GlobalLimiter should Allow")
	}
	if d := g.RetryAfter(); d != 0 {
		t.Fatalf("nil GlobalLimiter RetryAfter should be 0, got %v", d)
	}
}

// TestGlobalLimiter_BurstDefault verifies the burst defaults to rps*2
// when burst<=0 (audit 7.3). With rps=5, burst=10: the first 10 requests
// in a tight loop should all pass.
func TestGlobalLimiter_BurstDefault(t *testing.T) {
	g := NewGlobal(5, 0) // burst defaults to 10
	if g == nil {
		t.Fatal("NewGlobal(5, 0) should not return nil")
	}
	for i := 0; i < 10; i++ {
		if !g.Allow() {
			t.Fatalf("burst request %d should be allowed (burst=10)", i+1)
		}
	}
	// 11th request in the same instant should be rejected (burst exhausted).
	if g.Allow() {
		t.Fatal("11th request should be rejected — burst exhausted")
	}
}

// TestGlobalLimiter_BurstExplicit verifies an explicitly-passed burst is
// honored (the config layer may compute rps*2 itself and pass it in).
func TestGlobalLimiter_BurstExplicit(t *testing.T) {
	g := NewGlobal(100, 3)
	if g == nil {
		t.Fatal("NewGlobal(100, 3) should not return nil")
	}
	for i := 0; i < 3; i++ {
		if !g.Allow() {
			t.Fatalf("burst request %d should be allowed (burst=3)", i+1)
		}
	}
	if g.Allow() {
		t.Fatal("4th request should be rejected — burst exhausted")
	}
}

// TestGlobalLimiter_Refills verifies that after the rate interval
// elapses, tokens are refilled. With rps=10 and burst=1, after rejecting
// the 2nd request, waiting 100ms (1/rps) should allow one more.
func TestGlobalLimiter_Refills(t *testing.T) {
	g := NewGlobal(10, 1) // 10 tokens/sec, burst of 1
	// First request consumes the only burst token.
	if !g.Allow() {
		t.Fatal("first request should be allowed (burst=1)")
	}
	// 2nd immediately should fail.
	if g.Allow() {
		t.Fatal("second request should be rejected (no tokens yet)")
	}
	// Wait for one token to refill (~100ms at 10 rps).
	time.Sleep(150 * time.Millisecond)
	if !g.Allow() {
		t.Fatal("request after refill interval should be allowed")
	}
}

// TestGlobalLimiter_RetryAfter verifies RetryAfter returns a positive
// duration when the limiter is exhausted (audit 7.2) AND that the call
// does NOT consume a token (the immediate next Allow() is still
// rejected).
func TestGlobalLimiter_RetryAfter(t *testing.T) {
	g := NewGlobal(1, 1) // 1 token/sec, burst of 1
	if !g.Allow() {
		t.Fatal("first request should be allowed (burst=1)")
	}
	// Now the bucket is empty — RetryAfter should return a positive
	// duration (close to 1 second for 1 rps).
	d := g.RetryAfter()
	if d <= 0 {
		t.Fatalf("RetryAfter when exhausted should be positive, got %v", d)
	}
	// RetryAfter must NOT have consumed a token — the next Allow()
	// should still be rejected (the reservation was cancelled).
	if g.Allow() {
		t.Fatal("Allow() after RetryAfter() should still be rejected — RetryAfter must not consume a token")
	}
}

// TestGlobalLimiter_NonBlocking verifies Allow() returns immediately
// (audit 7.1 — Wait() would block). We measure the call time and assert
// it's well under the refill interval.
func TestGlobalLimiter_NonBlocking(t *testing.T) {
	g := NewGlobal(1, 1) // 1 token/sec, burst of 1
	g.Allow()            // exhaust burst
	start := time.Now()
	// This should return false immediately, not wait ~1 second.
	allowed := g.Allow()
	elapsed := time.Since(start)
	if allowed {
		t.Fatal("exhausted limiter should not Allow")
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("Allow() on exhausted limiter took %v — should be non-blocking", elapsed)
	}
}

// TestGlobalLimiter_Concurrent stress-tests the global limiter under
// concurrent access with the race detector. With rps=100 and burst=50,
// 100 goroutines × 10 requests = 1000 calls — most should be rejected
// but none should panic.
func TestGlobalLimiter_Concurrent(t *testing.T) {
	g := NewGlobal(100, 50)
	if g == nil {
		t.Fatal("NewGlobal(100, 50) should not return nil")
	}
	var wg sync.WaitGroup
	const goroutines = 100
	const perGoroutine = 10
	var allowed atomic.Int64
	var rejected atomic.Int64
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				if g.Allow() {
					allowed.Add(1)
				} else {
					rejected.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	total := allowed.Load() + rejected.Load()
	if total != goroutines*perGoroutine {
		t.Fatalf("expected %d total calls, got %d", goroutines*perGoroutine, total)
	}
	// We can't assert exact counts (timing-dependent), but allowed
	// should be roughly burst + a few refills, definitely < total.
	if allowed.Load() == 0 {
		t.Fatal("expected at least one allowed request")
	}
}
