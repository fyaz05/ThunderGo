// Package pool manages a fleet of independent Telegram bot transports and
// spreads file transfers across them by picking the least-loaded available
// transport per request. Every slot carries an atomic in-flight counter, a
// hard per-slot stream cap, a vault-lookup semaphore, and a flood-wait
// cooldown; the pool itself never queues work — when nothing is available it
// returns a typed capacity/brownout error so the HTTP layer can answer 503
// immediately.
//
// Phase 3: the fleet is built from tgutil.MTGOTransport (mtgo) clients, one
// per bot token. The gogram client pool is gone.
package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fyaz05/ThunderGo/internal/config"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// Compile-time gate: the mtgo transport satisfies the full BotBackend seam,
// so the pool's primary slot can be handed to the bot/ingest layers as-is.
var _ tgutil.BotBackend = (*tgutil.MTGOTransport)(nil)

// Typed acquire errors. Both are sentinel-wrapped (errors.Is-able) and carry
// a RetryAfter hint for the HTTP layer.
var (
	// ErrPoolCapacity means every healthy slot is at its hard stream cap.
	// The HTTP layer answers 503 with a short Retry-After rather than
	// queueing work on an already saturated Telegram session.
	ErrPoolCapacity = errors.New("pool: all clients at stream capacity")
	// ErrPoolBrownout means every slot is in flood-wait cooldown. The HTTP
	// layer answers 503 with the minimum remaining wait (capped).
	ErrPoolBrownout = errors.New("pool: every client in flood cooldown")
)

// poolError wraps one of the sentinels above with a Retry-After hint in
// seconds. Unexported on purpose: the stream layer reads the hint through
// Pool.BrownoutRetryAfter(), and errors.Is works through Unwrap.
type poolError struct {
	sentinel   error
	retryAfter int // seconds; 0 for capacity (HTTP uses its own default)
}

func (e *poolError) Error() string {
	if e.retryAfter > 0 {
		return fmt.Sprintf("%v (retry after %ds)", e.sentinel, e.retryAfter)
	}
	return e.sentinel.Error()
}

// Unwrap makes errors.Is(err, ErrPoolCapacity/ErrPoolBrownout) work.
func (e *poolError) Unwrap() error { return e.sentinel }

// RetryAfter returns the hint in seconds.
func (e *poolError) RetryAfter() int { return e.retryAfter }

// slot is one fleet member: a transport plus its accounting.
type slot struct {
	t        tgutil.Transport
	index    int
	inflight atomic.Int64

	// lookupSem bounds vault lookups per slot (cap 1). A cancelled HTTP
	// request cannot stop an in-flight ResolveMedia instantly, so its
	// permit is held until the call actually returns instead of allowing
	// unbounded concurrent lookups on one Telegram session.
	lookupSem chan struct{}

	// floodUntil is a unix-seconds timestamp: while now < floodUntil the
	// slot is skipped by AcquireBest. 0 = healthy.
	floodUntil atomic.Int64

	// consecutiveFloods counts flood reports since the last healthy sweep.
	consecutiveFloods atomic.Int32

	// dc is the transport's own DC, cached right after Start (no live RPC
	// on the streaming path). Only written before the pool is published.
	dc int
}

// Lease is one admission on a slot. The caller MUST Release it (defer-safe;
// idempotent).
type Lease struct {
	slot     *slot
	p        *Pool
	released atomic.Bool
}

// Transport returns the leased transport.
func (l *Lease) Transport() tgutil.Transport {
	if l == nil || l.slot == nil {
		return nil
	}
	return l.slot.t
}

// DC returns the leased slot's cached own-DC (0 when unknown). The stream
// pipeline uses it for cross-DC pacing.
func (l *Lease) DC() int {
	if l == nil || l.slot == nil {
		return 0
	}
	return l.slot.dc
}

// Release returns the admission. Idempotent — only the first call decrements
// the counter.
func (l *Lease) Release() {
	if l == nil || l.slot == nil {
		return
	}
	if l.released.CompareAndSwap(false, true) {
		l.slot.inflight.Add(-1)
	}
}

// AcquireLookup reserves one bounded vault-lookup slot on the leased client.
// A lease without a semaphore (constructed outside New) behaves as an
// unlimited no-op.
func (l *Lease) AcquireLookup(ctx context.Context) (release func(), ok bool) {
	if l == nil || l.slot == nil || l.slot.lookupSem == nil {
		return func() {}, true
	}
	// Fast-path: an already-cancelled context must never take a semaphore
	// token (select is unbiased when both sides are ready).
	if err := ctx.Err(); err != nil {
		return func() {}, false
	}
	select {
	case l.slot.lookupSem <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-l.slot.lookupSem }) }, true
	case <-ctx.Done():
		return func() {}, false
	}
}

// Pool is the fleet manager.
type Pool struct {
	slots []*slot // immutable after New; slot[0] is the primary

	// maxConcurrent is a hard simultaneous-stream cap per slot. Requests
	// are rejected by the HTTP layer when every slot is at this cap.
	maxConcurrent int

	// acquireMu serialises AcquireBest so slot selection and reservation
	// happen atomically.
	acquireMu sync.Mutex

	log *slog.Logger

	brownoutRetryAfter atomic.Int64 // seconds; set when a brownout error is produced

	sweepMu     sync.Mutex
	sweepCancel context.CancelFunc

	stopOnce sync.Once
}

// New creates and connects the transport fleet: one primary (receives
// updates) plus one secondary per extra token (download-only). Primary
// failure is fatal; secondary failures are logged and skipped (partial pool
// usable). Duplicate tokens anywhere in the fleet are a configuration error
// and fail fast — two sessions on one bot account corrupt each other's auth
// state.
//
// Each transport is started with the caller-supplied startup ctx (app.go's
// 30s budget). Sessions persist in MongoDB (database "thundergo") when a
// Mongo URI is configured; otherwise mtgo's in-memory storage is used.
func New(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Pool, error) {
	if log == nil {
		log = slog.Default()
	}
	maxConcurrent := 8
	if cfg.MaxConcurrentPerClient > 0 {
		maxConcurrent = cfg.MaxConcurrentPerClient
	}
	p := &Pool{
		maxConcurrent: maxConcurrent,
		log:           log,
	}

	// Session storage: one shared MongoDB-backed adapter (keyed per
	// session by SessionName) so every bot keeps its auth keys across
	// restarts. nil → mtgo in-memory storage (fresh bot login per boot).
	var storage any
	if cfg.MongoURI != "" {
		st, err := tgutil.MongoStorage(cfg.MongoURI, "thundergo")
		if err != nil {
			return nil, fmt.Errorf("pool: session storage: %w", err)
		}
		storage = st
	}

	// Duplicate-token check across primary + extras. tgutil.NewMTGOTransport
	// would happily build two clients on one token and Telegram would
	// terminate them alternately; fail the build instead.
	seen := map[string]int{cfg.BotToken: 0}
	for i, tok := range cfg.ExtraBots {
		if first, dup := seen[tok]; dup {
			return nil, fmt.Errorf("pool: duplicate bot token (sessions bot-%02d and bot-%02d)", first, i+1)
		}
		seen[tok] = i + 1
	}

	build := func(token, session string) (tgutil.Transport, error) {
		t, err := tgutil.NewMTGOTransport(tgutil.MTGOConfig{
			APIID:       cfg.APIID,
			APIHash:     cfg.APIHash,
			BotToken:    token,
			SessionName: session,
			Storage:     storage,
			Log:         log.With("component", session),
		})
		if err != nil {
			return nil, err
		}
		if err := t.Start(ctx); err != nil {
			return nil, err
		}
		return t, nil
	}

	// Primary (session bot-00): failure is fatal.
	primary, err := build(cfg.BotToken, "bot-00")
	if err != nil {
		return nil, fmt.Errorf("starting primary transport: %w", err)
	}
	p.slots = append(p.slots, newSlot(primary, 0, maxConcurrent))

	// Secondary fleet (sessions bot-01..): download-only; skip-on-error.
	for i, tok := range cfg.ExtraBots {
		t, err := build(tok, fmt.Sprintf("bot-%02d", i+1))
		if err != nil {
			log.Error("failed to start secondary transport; skipping", "index", i+1, "error", err)
			continue
		}
		p.slots = append(p.slots, newSlot(t, len(p.slots), maxConcurrent))
	}

	dcs := make([]int, len(p.slots))
	for i, s := range p.slots {
		dcs[i] = s.dc
	}
	log.Info("telegram transport fleet ready",
		"total_transports", len(p.slots),
		"max_concurrent_per_transport", maxConcurrent,
		"primary_dc", p.slots[0].dc,
		"slot_dcs", dcs,
	)
	return p, nil
}

// NewForTests builds a pool around pre-built transports instead of dialing
// Telegram. Cross-package suites (internal/stream) exercise admission,
// brownout and lease semantics on tgutil.FakeTransport through it; production
// code must use New. Mirrors the exported test-seam pattern in tgutil
// (NewFloodWaitTestError & co).
//
//nolint:revive // exported test seam; see doc comment
func NewForTests(ts ...tgutil.Transport) *Pool {
	p := &Pool{maxConcurrent: 8, log: slog.Default()}
	for i, t := range ts {
		p.slots = append(p.slots, newSlot(t, i, 8))
	}
	return p
}

// newSlot builds a slot and caches the transport's own DC right after Start
// (OwnDC reads the live session without an RPC; only BotBackend carries it).
func newSlot(t tgutil.Transport, index, lookupCap int) *slot {
	lookupCap = max(lookupCap, 1)
	s := &slot{
		t:         t,
		index:     index,
		lookupSem: make(chan struct{}, lookupCap),
	}
	if bb, ok := t.(tgutil.BotBackend); ok {
		s.dc = bb.OwnDC(context.Background())
	}
	return s
}

// Primary returns the primary slot's transport as a BotBackend (nil for an
// empty pool). The bot and ingest layers run on it.
func (p *Pool) Primary() tgutil.BotBackend {
	if len(p.slots) == 0 {
		return nil
	}
	bb, _ := p.slots[0].t.(tgutil.BotBackend)
	return bb
}

// Transport returns slot i's transport (nil when out of range). Test seam.
func (p *Pool) Transport(i int) tgutil.Transport {
	if i < 0 || i >= len(p.slots) {
		return nil
	}
	return p.slots[i].t
}

// Len returns the number of live slots.
func (p *Pool) Len() int { return len(p.slots) }

// AcquireBest finds the least-loaded non-cooling slot under the hard cap and
// reserves it (charge-at-exec). Never queues: when nothing qualifies it
// returns ErrPoolCapacity (all healthy slots full) or ErrPoolBrownout (every
// slot flood-cooling; check Pool.BrownoutRetryAfter for the hint).
func (p *Pool) AcquireBest() (*Lease, error) {
	p.acquireMu.Lock()
	defer p.acquireMu.Unlock()

	now := time.Now().Unix()
	var best *slot
	var bestLoad int64
	cooling := 0
	for _, s := range p.slots {
		if fu := s.floodUntil.Load(); fu > now {
			cooling++
			continue
		}
		n := s.inflight.Load()
		if n >= int64(p.maxConcurrent) {
			continue
		}
		if best == nil || n < bestLoad {
			best = s
			bestLoad = n
		}
	}
	if best != nil {
		best.inflight.Add(1)
		return &Lease{slot: best, p: p}, nil
	}

	if len(p.slots) > 0 && cooling == len(p.slots) {
		// Every slot is flood-cooling → brownout. Report the minimum
		// remaining wait so the HTTP layer can honour Retry-After.
		minWait := int64(-1)
		for _, s := range p.slots {
			w := s.floodUntil.Load() - now
			if w < 0 {
				w = 0
			}
			if minWait < 0 || w < minWait {
				minWait = w
			}
		}
		wait := int(minWait)
		if wait < 1 {
			wait = 1
		}
		p.brownoutRetryAfter.Store(int64(wait))
		return nil, &poolError{sentinel: ErrPoolBrownout, retryAfter: wait}
	}
	return nil, &poolError{sentinel: ErrPoolCapacity}
}

// BrownoutRetryAfter returns the minimum remaining flood-wait (seconds) from
// the most recent brownout error, or 0 if none was produced yet.
func (p *Pool) BrownoutRetryAfter() int {
	return int(p.brownoutRetryAfter.Load())
}

// maxFloodCooldownSecs bounds flood cooldowns. Waits above this are treated
// as rate limits needing operator intervention rather than something the
// pool sleeps through.
const maxFloodCooldownSecs = 600

// ReportFlood records a FLOOD_WAIT against the slot owning t: the slot stops
// receiving new admissions until now+min(wait, 600s) elapses (or the sweep
// resets it). Safe to call with a transport the pool doesn't own (no-op).
func (p *Pool) ReportFlood(t tgutil.Transport, wait time.Duration) {
	if t == nil {
		return
	}
	for _, s := range p.slots {
		if s.t == t {
			secs := int(wait / time.Second)
			if secs < 1 {
				secs = 1
			}
			if secs > maxFloodCooldownSecs {
				secs = maxFloodCooldownSecs
			}
			s.floodUntil.Store(time.Now().Unix() + int64(secs))
			s.consecutiveFloods.Add(1)
			p.log.Warn("flood cooldown",
				"slot", s.index,
				"wait_secs", secs,
				"consecutive_floods", s.consecutiveFloods.Load(),
			)
			return
		}
	}
}

// StartFloodSweep launches the background goroutine that clears expired
// flood cooldowns (production cadence: 5 minutes). Stopping the pool (or
// cancelling ctx) stops the sweep. Call once, right after New.
func (p *Pool) StartFloodSweep(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = 5 * time.Minute
	}
	ctx, cancel := context.WithCancel(ctx)
	p.sweepMu.Lock()
	p.sweepCancel = cancel
	p.sweepMu.Unlock()
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now().Unix()
				for _, s := range p.slots {
					if fu := s.floodUntil.Load(); fu != 0 && fu <= now {
						s.floodUntil.Store(0)
						s.consecutiveFloods.Store(0)
						p.log.Debug("flood cooldown expired", "slot", s.index)
					}
				}
			}
		}
	}()
}

func (p *Pool) stopSweep() {
	p.sweepMu.Lock()
	cancel := p.sweepCancel
	p.sweepCancel = nil
	p.sweepMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// TotalInflight sums every slot's in-flight counter.
func (p *Pool) TotalInflight() int64 {
	var total int64
	for _, s := range p.slots {
		total += s.inflight.Load()
	}
	return total
}

// PerClientInflight returns the in-flight counters, indexed like the fleet
// (slot 0 = primary). Used by the /status endpoint and the bot's /stats.
func (p *Pool) PerClientInflight() []int64 {
	out := make([]int64, len(p.slots))
	for i, s := range p.slots {
		out[i] = s.inflight.Load()
	}
	return out
}

// PerClientDC returns each slot's cached own-DC (0 when unknown), indexed the
// same way as PerClientInflight. Used by the bot's /status command.
func (p *Pool) PerClientDC() []int {
	out := make([]int, len(p.slots))
	for i, s := range p.slots {
		out[i] = s.dc
	}
	return out
}

// Stop stops the flood sweep and every transport in parallel, context-aware
// so a stuck transport Stop cannot hang shutdown past the caller's deadline.
// Idempotent via sync.Once.
func (p *Pool) Stop(ctx context.Context) {
	p.stopOnce.Do(func() {
		p.stopSweep()
		var wg sync.WaitGroup
		for _, s := range p.slots {
			wg.Add(1)
			go func(s *slot) {
				defer wg.Done()
				done := make(chan struct{})
				go func() {
					defer close(done)
					_ = s.t.Stop()
				}()
				select {
				case <-done:
				case <-ctx.Done():
					// Give up waiting; Stop will finish in background.
				}
			}(s)
		}
		wg.Wait()
	})
}
