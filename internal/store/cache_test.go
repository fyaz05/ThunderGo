package store

// White-box tests for the lazy-LRU cache in cache.go.
//
// These tests exercise the lazy-LRU eviction semantics (oldest lastAccess
// wins, not list-back), TTL lazy expiration on read, background sweeping,
// nil-cache safety, and concurrent access under the race detector.

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// cacheLen returns the number of live entries under a read lock. Used to
// verify internal eviction/expiry state without relying on get() (which has
// its own lazy-expiry side effects).
func cacheLen[V any](c *cache[V]) int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ll.Len()
}

// assertHit fails the test if get(key) does not return (wantVal, true).
func assertHit[V comparable](tb testing.TB, c *cache[V], key string, wantVal V) {
	tb.Helper()
	v, ok := c.get(key)
	if !ok {
		tb.Fatalf("get(%q): expected hit, got miss", key)
	}
	if v != wantVal {
		tb.Fatalf("get(%q): val = %v, want %v", key, v, wantVal)
	}
}

// assertMiss fails the test if get(key) returns a hit.
func assertMiss[V any](tb testing.TB, c *cache[V], key string) {
	tb.Helper()
	if _, ok := c.get(key); ok {
		tb.Fatalf("get(%q): expected miss, got hit", key)
	}
}

// ---------------------------------------------------------------------------
// 1. Basic get / set / invalidate + nil-cache safety
// ---------------------------------------------------------------------------

func TestCacheBasicGetSetInvalidate(t *testing.T) {
	t.Parallel()
	c := newCache[int](10, time.Minute)

	// Missing key → (zero, false).
	if v, ok := c.get("nope"); ok || v != 0 {
		t.Fatalf("get missing key: got (%d, %v), want (0, false)", v, ok)
	}

	// Set + get round-trip.
	c.set("a", 42)
	assertHit(t, c, "a", 42)

	// Invalidate removes the entry.
	c.invalidate("a")
	assertMiss(t, c, "a")

	// Invalidate on a missing key is a no-op (must not panic).
	c.invalidate("ghost")
}

func TestCacheNilSafe(t *testing.T) {
	t.Parallel()
	var c *cache[int] // nil cache

	// get on nil → (zero, false), no panic.
	if v, ok := c.get("x"); ok || v != 0 {
		t.Fatalf("nil get: got (%d, %v), want (0, false)", v, ok)
	}

	// set / invalidate on nil → no-op, no panic.
	c.set("x", 1)
	c.invalidate("x")

	// StartSweep on nil → returns a no-op stop func, no goroutine, no panic.
	stop := c.StartSweep(time.Millisecond)
	if stop == nil {
		t.Fatal("nil StartSweep returned nil stop func")
	}
	stop() // idempotent, safe to call
}

// ---------------------------------------------------------------------------
// 2. TTL expiration (lazy on get)
// ---------------------------------------------------------------------------

func TestCacheTTLLazyExpiration(t *testing.T) {
	t.Parallel()
	c := newCache[int](10, 50*time.Millisecond)

	c.set("k", 7)

	// Before expiry → hit.
	assertHit(t, c, "k", 7)

	// Poll up to 200ms for the entry to expire (avoid flaky fixed sleep).
	deadline := time.Now().Add(200 * time.Millisecond)
	var expired bool
	for time.Now().Before(deadline) {
		if _, ok := c.get("k"); !ok {
			expired = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !expired {
		t.Fatal("entry did not expire within deadline")
	}

	if n := cacheLen(c); n != 0 {
		t.Fatalf("entry not removed after lazy expiry: cache len = %d", n)
	}
}

// ---------------------------------------------------------------------------
// 3. LRU eviction — oldest lastAccess evicted, NOT list-back
// ---------------------------------------------------------------------------

func TestCacheLRUEvictionOldestLastAccess(t *testing.T) {
	t.Parallel()
	c := newCache[int](3, time.Minute)

	c.set("a", 1)
	c.set("b", 2)
	c.set("c", 3)

	// Touch "a" so its lastAccess becomes the newest. Without this, "a"
	// would be the oldest (inserted first) and would be evicted — but the
	// lazy-LRU policy keys off lastAccess, not insertion order.
	time.Sleep(5 * time.Millisecond)
	assertHit(t, c, "a", 1)

	// Inserting "d" overflows (len becomes 4 > maxSize 3) → evictBack
	// should evict "b" (oldest lastAccess), NOT "a" (just touched).
	c.set("d", 4)

	assertHit(t, c, "a", 1) // "a" must survive
	assertMiss(t, c, "b")   // "b" must be evicted
	assertHit(t, c, "c", 3)
	assertHit(t, c, "d", 4)

	if n := cacheLen(c); n != 3 {
		t.Errorf("cache len = %d, want 3", n)
	}
}

// ---------------------------------------------------------------------------
// 4. Update existing key — value replaced, no size growth
// ---------------------------------------------------------------------------

func TestCacheUpdateExistingKey(t *testing.T) {
	t.Parallel()
	c := newCache[int](10, time.Minute)

	c.set("k", 1)
	c.set("k", 2)
	c.set("k", 3)

	assertHit(t, c, "k", 3)

	if n := cacheLen(c); n != 1 {
		t.Errorf("cache len = %d, want 1 (update must not grow cache)", n)
	}
}

// ---------------------------------------------------------------------------
// 5. Background sweep (StartSweep)
// ---------------------------------------------------------------------------

func TestCacheStartSweep(t *testing.T) {
	// Not parallel — timing-sensitive background goroutine.
	c := newCache[int](10, 50*time.Millisecond)

	c.set("x", 1)
	c.set("y", 2)
	c.set("z", 3)

	stop := c.StartSweep(20 * time.Millisecond)
	t.Cleanup(stop)

	// TTL is 50ms; sweep ticks every 20ms. Poll up to 500ms for all
	// entries to be reaped by the background sweeper (avoids flaky sleep).
	deadline := time.Now().Add(500 * time.Millisecond)
	allGone := false
	for time.Now().Before(deadline) {
		_, xok := c.get("x")
		_, yok := c.get("y")
		_, zok := c.get("z")
		if !xok && !yok && !zok {
			allGone = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !allGone {
		t.Fatal("sweep did not reap all entries within deadline")
	}
	if n := cacheLen(c); n != 0 {
		t.Errorf("cache len = %d after sweep, want 0", n)
	}
}

// ---------------------------------------------------------------------------
// 6. Concurrent access (run under -race)
// ---------------------------------------------------------------------------

func TestCacheConcurrentAccess(t *testing.T) {
	t.Parallel()
	c := newCache[int](20, 100*time.Millisecond)

	const goroutines = 100
	const opsPerG = 100
	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func(seed int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed)))
			<-start // release all goroutines at once for max contention
			for i := 0; i < opsPerG; i++ {
				k := keys[r.Intn(len(keys))]
				switch r.Intn(3) {
				case 0:
					c.get(k)
				case 1:
					c.set(k, r.Intn(1000))
				case 2:
					c.invalidate(k)
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()

	// Reaching this point without panic/deadlock is the primary assertion.
	// Sanity-check: cache must never exceed maxSize.
	if n := cacheLen(c); n > 20 {
		t.Errorf("cache len = %d, exceeds maxSize 20", n)
	}
}

// ---------------------------------------------------------------------------
// 7. Cache at capacity, all gets — no eviction should happen
// ---------------------------------------------------------------------------

func TestCacheAtCapacityAllGetsNoEviction(t *testing.T) {
	t.Parallel()
	c := newCache[int](3, time.Minute)

	c.set("a", 1)
	c.set("b", 2)
	c.set("c", 3)

	for i := 0; i < 50; i++ {
		c.get("a")
		c.get("b")
		c.get("c")
	}

	if n := cacheLen(c); n != 3 {
		t.Errorf("cache len = %d after repeated gets, want 3 (gets must not evict)", n)
	}
}

// ---------------------------------------------------------------------------
// 8. evictBack picks oldest lastAccess, not oldest insertion
// ---------------------------------------------------------------------------

func TestCacheEvictBackPicksOldestLastAccess(t *testing.T) {
	t.Parallel()
	c := newCache[int](2, time.Minute)

	c.set("a", 1)
	time.Sleep(10 * time.Millisecond)
	c.set("b", 2)

	// Touch "a" so its lastAccess is newer than "b"'s.
	time.Sleep(5 * time.Millisecond)
	assertHit(t, c, "a", 1)

	// Insert "c" → overflow → evictBack should evict "b" (oldest lastAccess),
	// even though "a" was inserted first.
	c.set("c", 3)

	assertHit(t, c, "a", 1) // "a" survives (was recently accessed)
	assertMiss(t, c, "b")   // "b" evicted (oldest lastAccess)
	assertHit(t, c, "c", 3)
}

// ---------------------------------------------------------------------------
// 9. Setting the same key repeatedly doesn't grow the cache
// ---------------------------------------------------------------------------

func TestCacheRepeatedSetSameKeyNoGrowth(t *testing.T) {
	t.Parallel()
	c := newCache[int](5, time.Minute)

	for i := 0; i < 10; i++ {
		c.set("k", i)
	}

	if n := cacheLen(c); n != 1 {
		t.Errorf("cache len = %d, want 1 (repeated sets must not grow cache)", n)
	}
	assertHit(t, c, "k", 9) // last write wins
}

// ---------------------------------------------------------------------------
// Bonus: evictBack directly — verify O(n) scan picks oldest lastAccess
// ---------------------------------------------------------------------------

func TestCacheEvictBackDirect(t *testing.T) {
	t.Parallel()
	c := newCache[int](10, time.Minute) // maxSize large enough that set won't auto-evict

	c.set("a", 1)
	c.set("b", 2)
	c.set("c", 3)

	// Touch "a" → "a" now has newest lastAccess. Among untouched entries,
	// "b" was set before "c", so "b" has the oldest lastAccess.
	time.Sleep(5 * time.Millisecond)
	assertHit(t, c, "a", 1)

	// evictBack requires the caller to hold the write lock.
	c.mu.Lock()
	c.evictBack()
	c.mu.Unlock()

	assertHit(t, c, "a", 1) // survived (recently touched)
	assertMiss(t, c, "b")   // evicted (oldest lastAccess)
	assertHit(t, c, "c", 3) // survived

	if n := cacheLen(c); n != 2 {
		t.Errorf("cache len = %d, want 2 after one evictBack", n)
	}
}

// ---------------------------------------------------------------------------
// Bonus: sweep removes only expired entries, leaves live ones intact
// ---------------------------------------------------------------------------

func TestCacheSweepRemovesExpiredOnly(t *testing.T) {
	t.Parallel()
	c := newCache[int](10, 50*time.Millisecond)

	c.set("fresh", 1)
	// Poll until "fresh" expires without calling get() (to test sweep path).
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	c.set("newer", 2)

	// Manually invoke sweep (same code path StartSweep calls).
	c.sweep()

	assertMiss(t, c, "fresh")   // expired → reaped
	assertHit(t, c, "newer", 2) // live → untouched

	if n := cacheLen(c); n != 1 {
		t.Errorf("cache len = %d after sweep, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Bonus: StartSweep stop func is idempotent
// ---------------------------------------------------------------------------

func TestCacheStartSweepStopIdempotent(t *testing.T) {
	t.Parallel()
	c := newCache[int](10, time.Minute)
	stop := c.StartSweep(10 * time.Millisecond)
	t.Cleanup(stop)

	// Calling stop multiple times must not panic (sync.Once guards close).
	stop()
	stop()
	stop()
}
