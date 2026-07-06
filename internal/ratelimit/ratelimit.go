// Package ratelimit implements a per-user sharded GCRA rate limiter and a
// global token-bucket limiter. Owner and authorized users bypass both
// (enforced by the caller).
//
// In-memory, single-process. Swap for a Redis-backed limiter if horizontally
// scaled.
package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// numShards controls lock contention. Must be a power of two so the modulo
// is a bitmask AND.
const numShards = 16

// Limiter is a sharded GCRA rate limiter. A zero-value Limiter (or one
// constructed with cap ≤ 0) is a no-op — all requests pass.
type Limiter struct {
	cap    int
	window time.Duration

	shards [numShards]shard
}

// shard holds a subset of users' TAT (theoretical arrival time) values.
type shard struct {
	mu   sync.Mutex
	tats map[int64]*atomic.Int64 // user ID → TAT in nanoseconds since epoch
}

// New returns a Limiter with the given cap and window. If cap ≤ 0, the
// limiter is disabled.
func New(cap int, window time.Duration) *Limiter {
	l := &Limiter{cap: cap, window: window}
	for i := range l.shards {
		l.shards[i].tats = make(map[int64]*atomic.Int64)
	}
	return l
}

// Allow returns true if the user is under the cap. Calling Allow consumes
// one slot. No-op (always true) when the limiter is disabled.
func (l *Limiter) Allow(userID int64) bool {
	allowed, _ := l.AllowN(userID, 1)
	return allowed
}

// AllowN returns true if the user can consume n slots atomically, plus the
// retry-after duration (zero if allowed). No-op when disabled.
//
// GCRA: period = window / cap; advance TAT by n*period; reject if TAT-now
// exceeds one window. Eliminates the 2× boundary burst of fixed-window.
func (l *Limiter) AllowN(userID int64, n int) (allowed bool, retryAfter time.Duration) {
	if l == nil || l.cap <= 0 {
		return true, 0
	}
	if n <= 0 {
		return false, 0
	}
	if n > l.cap {
		return false, l.window
	}

	shardIdx := userID & (numShards - 1) // bitmask hash, always in [0, 15]
	s := &l.shards[shardIdx]

	s.mu.Lock()
	tatPtr, ok := s.tats[userID]
	if !ok {
		tatPtr = new(atomic.Int64)
		s.tats[userID] = tatPtr
	}

	period := l.window.Nanoseconds() / int64(l.cap)
	burstAllowance := l.window.Nanoseconds()
	now := time.Now().UnixNano()

	oldTAT := tatPtr.Load()
	newTAT := oldTAT
	if newTAT < now {
		newTAT = now
	}
	newTAT += period * int64(n)
	if newTAT-now > burstAllowance {
		s.mu.Unlock()
		retry := time.Duration(newTAT - burstAllowance - now)
		if retry < 0 {
			retry = 0
		}
		return false, retry
	}
	tatPtr.Store(newTAT)
	s.mu.Unlock()
	return true, 0
}

// Reset clears a user's counter. Used when an authorized user is added so
// they get a fresh window immediately.
func (l *Limiter) Reset(userID int64) {
	if l == nil || l.cap <= 0 {
		return
	}
	s := &l.shards[userID&(numShards-1)]
	s.mu.Lock()
	delete(s.tats, userID)
	s.mu.Unlock()
}

// Sweep removes expired entries. An entry is expired if its TAT is more
// than one window in the past. Call periodically to bound memory.
func (l *Limiter) Sweep() {
	if l == nil || l.cap <= 0 {
		return
	}
	cutoff := time.Now().Add(-l.window).UnixNano()
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		for uid, tatPtr := range s.tats {
			if tatPtr.Load() < cutoff {
				delete(s.tats, uid)
			}
		}
		s.mu.Unlock()
	}
}

// Len returns the total number of tracked users across all shards.
func (l *Limiter) Len() int {
	if l == nil {
		return 0
	}
	var n int
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		n += len(s.tats)
		s.mu.Unlock()
	}
	return n
}

// StartSweep launches a background goroutine that calls Sweep at the given
// interval. The returned stop function is idempotent (sync.Once) and blocks
// until the goroutine has exited.
func (l *Limiter) StartSweep(interval time.Duration) (stop func()) {
	if l == nil || interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				l.Sweep()
			}
		}
	}()
	return func() {
		once.Do(func() { close(done) })
		wg.Wait()
	}
}

// GlobalLimiter caps total requests-per-second across ALL users (a
// circuit-breaker guarding against Telegram FLOOD_WAITs under load). It wraps
// golang.org/x/time/rate.Limiter (token-bucket). A nil GlobalLimiter is
// disabled; Allow and RetryAfter are nil-safe.
type GlobalLimiter struct {
	limiter *rate.Limiter
}

// NewGlobal creates a GlobalLimiter. Returns nil (disabled) if rps ≤ 0.
// If burst ≤ 0 and rps > 0, burst defaults to rps*2.
func NewGlobal(rps, burst int) *GlobalLimiter {
	if rps <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = rps * 2
	}
	return &GlobalLimiter{limiter: rate.NewLimiter(rate.Limit(rps), burst)}
}

// Allow returns true if the request is permitted. Non-blocking — never
// consumes a slot when none is available and never waits. A nil receiver
// (disabled) always returns true.
func (g *GlobalLimiter) Allow() bool {
	if g == nil {
		return true
	}
	return g.limiter.Allow()
}

// RetryAfter returns the wait time before the next Allow() would succeed.
// Uses Reserve().Delay() then cancels the reservation so the token is not
// consumed. A nil receiver returns 0. Returns time.Hour if the limiter is
// permanently disabled (should not happen given NewGlobal guards rps>0).
func (g *GlobalLimiter) RetryAfter() time.Duration {
	if g == nil {
		return 0
	}
	r := g.limiter.Reserve()
	if !r.OK() {
		return time.Hour
	}
	delay := r.Delay()
	r.Cancel() // don't consume the token — we're rejecting
	return delay
}
