package store

import (
	"container/list"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// cache is a bounded TTL cache with lazy-LRU eviction.
//
// V is stored by value; if V contains pointers, mutations outside the cache
// will affect cached entries. For immutable V or value types this is safe.
//
// Eviction: when full, evicts the entry with the oldest lastAccess. Reads
// update lastAccess atomically under RLock (no MoveToFront on reads), avoiding
// write-lock contention on cache hits.
//
// Expiration: TTL per entry; removed lazily on get/set and actively by StartSweep.
type cache[V any] struct {
	mu      sync.RWMutex
	entries map[string]*list.Element // key → list element
	ll      *list.List               // live entries; front = most recently inserted/updated
	maxSize int
	ttl     time.Duration
}

type cacheEntry[V any] struct {
	key        string
	value      V
	expires    time.Time
	lastAccess atomic.Int64 // unix-nano; updated on get under RLock (lazy LRU)
}

func newCache[V any](maxSize int, ttl time.Duration) *cache[V] {
	return &cache[V]{
		entries: make(map[string]*list.Element),
		ll:      list.New(),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

// get returns the cached value for key. Expired entries are removed lazily.
// Read lock only — lastAccess is updated atomically so cache hits don't
// serialize behind a write lock.
func (c *cache[V]) get(key string) (V, bool) {
	var zero V
	if c == nil {
		return zero, false
	}
	c.mu.RLock()
	el, ok := c.entries[key]
	if !ok {
		c.mu.RUnlock()
		return zero, false
	}
	e := el.Value.(*cacheEntry[V])
	if time.Now().After(e.expires) {
		c.mu.RUnlock()
		// Lazy expiration: remove under write lock; re-check for concurrent removal/refresh.
		c.mu.Lock()
		if el2, ok := c.entries[key]; ok && el2 == el {
			c.removeElement(el2)
		}
		c.mu.Unlock()
		return zero, false
	}
	e.lastAccess.Store(time.Now().UnixNano())
	val := e.value
	c.mu.RUnlock()
	return val, true
}

// set adds or updates an entry, marking it most-recently-used. Evicts the
// entry with the oldest lastAccess if over capacity (lazy LRU).
func (c *cache[V]) set(key string, value V) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if el, ok := c.entries[key]; ok {
		e := el.Value.(*cacheEntry[V])
		e.value = value
		e.expires = now.Add(c.ttl)
		e.lastAccess.Store(now.UnixNano())
		c.ll.MoveToFront(el)
		return
	}
	e := &cacheEntry[V]{
		key:     key,
		value:   value,
		expires: now.Add(c.ttl),
	}
	e.lastAccess.Store(now.UnixNano())
	c.entries[key] = c.ll.PushFront(e)
	if c.ll.Len() > c.maxSize {
		c.evictBack()
	}
}

// invalidate removes a single entry by key.
func (c *cache[V]) invalidate(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if el, ok := c.entries[key]; ok {
		c.removeElement(el)
	}
	c.mu.Unlock()
}

// removeElement unlinks a list element and drops its map entry. Caller must hold the write lock.
func (c *cache[V]) removeElement(el *list.Element) {
	e := el.Value.(*cacheEntry[V])
	delete(c.entries, e.key)
	c.ll.Remove(el)
}

// evictBack removes the entry with the oldest lastAccess. Caller must hold the
// write lock. O(n) but only runs on insertion over capacity — never on a read.
func (c *cache[V]) evictBack() {
	var oldestEl *list.Element
	var oldestTime int64 = math.MaxInt64
	for el := c.ll.Front(); el != nil; el = el.Next() {
		la := el.Value.(*cacheEntry[V]).lastAccess.Load()
		if la <= oldestTime {
			oldestTime = la
			oldestEl = el
		}
	}
	if oldestEl != nil {
		c.removeElement(oldestEl)
	}
}

// StartSweep launches a background goroutine that periodically removes expired
// entries. Default interval is 5 min; pass a positive duration to override.
// Returns an idempotent stop function (safe to call multiple times).
func (c *cache[V]) StartSweep(interval time.Duration) (stop func()) {
	if c == nil {
		return func() {}
	}
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				c.sweep()
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

// sweep removes all expired entries under a single write lock.
func (c *cache[V]) sweep() {
	if c == nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.ll.Front(); el != nil; {
		next := el.Next()
		if now.After(el.Value.(*cacheEntry[V]).expires) {
			c.removeElement(el)
		}
		el = next
	}
}
