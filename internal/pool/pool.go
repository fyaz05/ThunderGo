// Package pool manages a set of independent Telegram bot clients and spreads
// file transfers across them by picking the least-loaded available client
// per transfer. Each client has an atomic in-flight counter; the pool installs
// a FloodHandler on every client that sleeps and retries.
package pool

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amarnathcjd/gogram/telegram"

	"github.com/fyaz05/ThunderGo/internal/config"
)

type Client struct {
	*telegram.Client
	Inflight atomic.Int64

	// lookupSem bounds non-context GetMessages calls. A cancelled HTTP request
	// cannot stop gogram's lookup immediately, so its permit is held until the
	// detached call actually returns instead of allowing unbounded goroutines.
	lookupSem chan struct{}
}

// AcquireLookup reserves one bounded vault-lookup slot. A zero-value test
// Client has no semaphore and intentionally behaves as an unlimited no-op.
func (c *Client) AcquireLookup(ctx context.Context) (release func(), ok bool) {
	if c == nil || c.lookupSem == nil {
		return func() {}, c != nil
	}
	select {
	case c.lookupSem <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-c.lookupSem }) }, true
	case <-ctx.Done():
		return func() {}, false
	}
}

type Pool struct {
	primary *Client
	all     []*Client

	// maxConcurrent is a hard simultaneous-stream cap per client. Requests are
	// rejected by the HTTP layer when every client is at this cap.
	maxConcurrent int

	stopped  chan struct{} // closed by Stop() to interrupt flood handler sleeps
	stopOnce sync.Once     // guards close(stopped) against double-close panic

	// acquireMu serialises AcquireBest so slot selection and reservation
	// happen atomically.
	acquireMu sync.Mutex
}

// New creates and connects every client in the pool: one primary (receives
// updates) plus one secondary per extra token (download-only). Primary failure
// is fatal; secondary failures are logged and skipped (partial pool usable).
func New(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Pool, error) {
	p := &Pool{
		stopped: make(chan struct{}),
	}
	if cfg.MaxConcurrentPerClient > 0 {
		p.maxConcurrent = cfg.MaxConcurrentPerClient
	} else {
		p.maxConcurrent = 8
	}

	// dispatcher must be initialized.
	primary, err := startClient(ctx, cfg, log, cfg.BotToken, 0, false, p.stopped)
	if err != nil {
		return nil, fmt.Errorf("starting primary client: %w", err)
	}
	p.primary = primary
	p.all = append(p.all, primary)

	// Secondary clients: download-only (no updates, lighter on resources).
	for i, tok := range cfg.ExtraBots {
		c, err := startClient(ctx, cfg, log, tok, i+1, true, p.stopped)
		if err != nil {
			log.Error("failed to start secondary client; skipping", "index", i+1, "error", err)
			continue
		}
		p.all = append(p.all, c)
	}

	log.Info("telegram client pool ready", "total_clients", len(p.all), "primary_dc", p.primary.GetDC())

	return p, nil
}

func startClient(ctx context.Context, cfg *config.Config, log *slog.Logger, token string, idx int, noUpdates bool, stopped chan struct{}) (*Client, error) {
	// Honor TG_DATA_DIR so session files land in /app/data (writable by
	// appuser) instead of CWD. Fall back to CWD for local dev.
	dataDir := os.Getenv("TG_DATA_DIR")
	if dataDir == "" {
		dataDir = "."
	} else {
		info, err := os.Stat(dataDir)
		if err != nil {
			return nil, fmt.Errorf("TG_DATA_DIR %q: %w", dataDir, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("TG_DATA_DIR %q is not a directory", dataDir)
		}
		if f, err := os.CreateTemp(dataDir, ".tg_write_test"); err != nil {
			return nil, fmt.Errorf("TG_DATA_DIR %q is not writable: %w", dataDir, err)
		} else {
			f.Close()
			os.Remove(f.Name())
		}
	}
	sessionFile := filepath.Join(dataDir, fmt.Sprintf("bot_%02d.session", idx))

	clientCfg := telegram.ClientConfig{
		AppID:            cfg.APIID,
		AppHash:          cfg.APIHash,
		Session:          sessionFile,
		SessionName:      fmt.Sprintf("bot-%02d", idx),
		ParseMode:        "HTML",
		LogLevel:         telegram.LogInfo,
		NoUpdates:        noUpdates,
		SleepThresholdMs: 10000,
		FloodHandler:     makeFloodHandler(log, idx, stopped),
		CacheSenders:     true, // cache per-DC download senders for speed
	}
	c, err := telegram.NewClient(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("creating client %d: %w", idx, err)
	}
	if err := c.Connect(); err != nil {
		return nil, fmt.Errorf("connecting client %d: %w", idx, err)
	}

	// Tighten session file permissions before login for defense-in-depth.
	// gogram creates the session file lazily; if the file doesn't exist
	// yet, Chmod returns ENOENT which we skip.
	if err := os.Chmod(sessionFile, 0o600); err != nil && !os.IsNotExist(err) { //nosec G703
		log.Warn("chmod session file failed", "client", idx, "file", sessionFile, "error", err)
	}

	if err := c.LoginBot(token); err != nil {
		return nil, fmt.Errorf("login client %d: %w", idx, err)
	}

	// UpdatesGetState activates MTProto push updates; without it, updates queue on the getUpdates side.
	if !noUpdates {
		if _, err := c.UpdatesGetState(); err != nil {
			log.Warn("UpdatesGetState failed; update receiving may be impaired", "client", idx, "error", err)
		} else {
			log.Info("update state synced", "client", idx)
		}
		// Set "/" as the only command prefix (gogram default is "/!").
		c.SetCommandPrefixes("/")
	}

	lookupCap := cfg.MaxConcurrentPerClient
	if lookupCap < 1 {
		lookupCap = 8
	}
	return &Client{Client: c, lookupSem: make(chan struct{}, lookupCap)}, nil
}

// maxFloodWaitSecs is the upper bound on flood-wait retries. Waits above
// this are treated as rate limits that need operator intervention rather
// than something to sleep through.
const maxFloodWaitSecs = 600

// makeFloodHandler returns a FloodHandler that sleeps for the parsed wait and
// returns true to retry. Waits over 10 min are not retried. Sleep is interruptible
// via stopped so shutdown isn't delayed.
func makeFloodHandler(log *slog.Logger, idx int, stopped <-chan struct{}) func(err error) bool {
	return func(err error) bool {
		if err == nil {
			return false
		}
		wait := telegram.GetFloodWait(err)
		if wait <= 0 {
			return false
		}
		if wait > maxFloodWaitSecs {
			log.Warn("flood wait too long; not retrying", "client", idx, "wait_secs", wait)
			return false
		}
		log.Warn("flood wait; sleeping", "client", idx, "wait_secs", wait)
		select {
		case <-time.After(time.Duration(wait) * time.Second):
			return true
		case <-stopped:
			return false
		}
	}
}

func (p *Pool) Primary() *Client { return p.primary }

func (p *Pool) All() []*Client { return p.all }

func (p *Pool) Len() int { return len(p.all) }

// AcquireBest finds the least-loaded client under the cap and returns it with
// an idempotent release callback (caller MUST defer it).
func (p *Pool) AcquireBest() (c *Client, release func()) {
	p.acquireMu.Lock()
	defer p.acquireMu.Unlock()

	if len(p.all) == 0 {
		return nil, func() {}
	}

	// Pass 1: find least-loaded client under the cap.
	var best *Client
	var bestCount int64
	for _, cl := range p.all {
		n := cl.Inflight.Load()
		if n >= int64(p.maxConcurrent) {
			continue
		}
		if best == nil || n < bestCount {
			best = cl
			bestCount = n
		}
	}
	if best == nil {
		// All clients are at their hard stream cap. The HTTP handler turns
		// this into a short Retry-After response rather than queueing work on
		// an already saturated Telegram session.
		return nil, func() {}
	}

	best.Inflight.Add(1)
	var once sync.Once
	return best, func() { once.Do(func() { best.Inflight.Add(-1) }) }
}

// Deprecated: prefer AcquireBest for atomic pick+reserve.
func (p *Pool) Pick() *Client {
	if len(p.all) == 0 {
		return nil
	}

	// Pass 1: find least-loaded client under the cap.
	var best *Client
	var bestCount int64
	for _, c := range p.all {
		n := c.Inflight.Load()
		if n >= int64(p.maxConcurrent) {
			continue
		}
		if best == nil || n < bestCount {
			best = c
			bestCount = n
		}
	}
	if best != nil {
		return best
	}

	return nil
}

// Deprecated: prefer AcquireBest.
func (p *Pool) Acquire() (c *Client, release func()) {
	return p.AcquireBest()
}

func (p *Pool) TotalInflight() int64 {
	var total int64
	for _, c := range p.all {
		total += c.Inflight.Load()
	}
	return total
}

// Used by the /status endpoint.
func (p *Pool) PerClientInflight() []int64 {
	out := make([]int64, len(p.all))
	for i, c := range p.all {
		out[i] = c.Inflight.Load()
	}
	return out
}

// Stop stops every client in parallel, context-aware so a stuck client.Stop()
// cannot hang shutdown past the caller's deadline. Idempotent via sync.Once.
func (p *Pool) Stop(ctx context.Context) {
	p.stopOnce.Do(func() {
		close(p.stopped)
		var wg sync.WaitGroup
		for _, c := range p.all {
			wg.Add(1)
			go func(client *Client) {
				defer wg.Done()
				done := make(chan struct{})
				go func() {
					_ = client.Terminate()
					close(done)
				}()
				select {
				case <-done:
				case <-ctx.Done():
					// Give up waiting; client.Terminate will finish in background.
				}
			}(c)
		}
		wg.Wait()
	})
}
