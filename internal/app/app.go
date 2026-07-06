// Package app wires together all subsystems and manages their lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fyaz05/ThunderGo/internal/bot"
	"github.com/fyaz05/ThunderGo/internal/config"
	tghttp "github.com/fyaz05/ThunderGo/internal/http"
	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/log"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/ratelimit"
	"github.com/fyaz05/ThunderGo/internal/shortener"
	"github.com/fyaz05/ThunderGo/internal/store"
)

// App is the running application.
type App struct {
	Cfg      *config.Config
	Log      *slog.Logger
	Store    *store.Store
	Pool     *pool.Pool
	Ingester *ingest.Ingester
	Bot      *bot.Bot
	HTTP     *tghttp.Server
	Limiter  *ratelimit.Limiter

	// touchBuffer batches seen_count increments. Created in New(), stopped
	// in Run() BEFORE Store.Close() so the final BulkWrite flush doesn't
	// race with mongo client disconnect (audit 9.3).
	touchBuffer *store.TouchBuffer

	// stopLimiterSweep stops the background rate-limiter sweep goroutine
	// on shutdown.
	stopLimiterSweep func()
	stopDedupCleanup func()

	// restartCtx / restartCancel / restartWg track the EditRestartMarkerIfPending
	// goroutine so it can be cancelled and waited on during shutdown (A-001).
	restartCtx    context.Context
	restartCancel context.CancelFunc
	restartWg     sync.WaitGroup
}

// New constructs the application: loads config, connects to MongoDB,
// and wires up all subsystems.
func New() (*App, error) {
	cfg, err := config.Load(".env", ".env.local")
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}

	if err := log.Init("", cfg.LogLevel); err != nil {
		return nil, fmt.Errorf("initializing log: %w", err)
	}
	logger := log.L()
	logger.Info("starting ThunderGo",
		"version", tghttp.Version,
		"base_url", cfg.BaseURL,
		"private_mode", cfg.PrivateMode,
		"client_count", 1+len(cfg.ExtraBots),
	)

	// Startup timeout is hardcoded at 30s — sufficient for Mongo connect +
	// Telegram pool warmup on any reasonable network.
	const startupTimeout = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()

	st, err := store.New(ctx, cfg.MongoURI, cfg.FileTTLDays)
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB: %w", err)
	}
	logger.Info("MongoDB connected")

	// Wire the batched seen-count buffer. Stream accesses are buffered
	// and flushed via BulkWrite every 1 second — a 10-100x reduction in
	// DB writes under load. Stopped in Run() BEFORE Store.Close().
	touchBuffer := store.NewTouchBuffer(1*time.Second, st.FilesCollection())
	st.SetTouchBuffer(touchBuffer)

	p, err := pool.New(ctx, cfg, logger)
	if err != nil {
		touchBuffer.Stop() // drain flush goroutine before mongo disconnect
		_ = st.Close(ctx)
		return nil, fmt.Errorf("starting telegram pool: %w", err)
	}

	ingester := ingest.New(p, st, cfg.VaultChannelID, logger)
	stopDedupCleanup := ingester.StartDedupCleanup(10 * time.Minute)

	limiter := ratelimit.New(cfg.RateLimit, 60*time.Second) // window hardcoded to 60s = "per minute"
	// Sweep expired rate-limiter entries every 5 minutes to keep memory
	// bounded under sustained load from many distinct users.
	stopSweep := limiter.StartSweep(5 * time.Minute)

	sh, _ := shortener.New(cfg.ShortenerAPIKey, cfg.ShortenerSite, logger)

	b := bot.New(cfg, p, st, ingester, sh, limiter, logger)
	if err := b.Start(ctx); err != nil {
		// RA-A-009: stop background sweeps BEFORE the pool so they
		// don't race against pool teardown.
		stopDedupCleanup()
		stopSweep()
		b.Stop() // RA-A-002: drain handlers even though Start reported failure
		p.Stop(context.Background())
		touchBuffer.Stop() // drain flush goroutine before mongo disconnect
		_ = st.Close(ctx)
		return nil, fmt.Errorf("starting bot: %w", err)
	}

	httpServer, err := tghttp.New(cfg, p, st, ingester, logger)
	if err != nil {
		// Correct cleanup ordering (A-003): stop pool/bot FIRST, then close DB.
		// RA-A-009: stop background sweeps BEFORE the pool so they
		// don't race against pool teardown.
		stopDedupCleanup()
		stopSweep()
		b.Stop()
		p.Stop(context.Background())
		touchBuffer.Stop() // drain flush goroutine before mongo disconnect
		_ = st.Close(ctx)
		return nil, fmt.Errorf("building HTTP server: %w", err)
	}

	a := &App{
		Cfg:              cfg,
		Log:              logger,
		Store:            st,
		Pool:             p,
		Ingester:         ingester,
		Bot:              b,
		HTTP:             httpServer,
		Limiter:          limiter,
		touchBuffer:      touchBuffer,
		stopLimiterSweep: stopSweep,
		stopDedupCleanup: stopDedupCleanup,
	}

	// On startup, check for a pending restart marker. Track with a cancellable
	// context + WaitGroup so shutdown can cancel and wait (A-001).
	a.restartCtx, a.restartCancel = context.WithCancel(context.Background())
	a.restartWg.Add(1)
	go func() {
		defer a.restartWg.Done()
		defer func() {
			if r := recover(); r != nil {
				logger.Warn("panic in EditRestartMarkerIfPending", "panic", r)
			}
		}()
		_ = b.EditRestartMarkerIfPending(a.restartCtx)
	}()

	return a, nil
}

// Run starts the HTTP server and blocks until SIGINT or SIGTERM.
func (a *App) Run() error {
	a.HTTP.StartKeepalive()

	// Install signal handler BEFORE starting the HTTP server goroutine so a
	// SIGTERM during the bind window triggers graceful shutdown (A-011).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// Run the HTTP server in a goroutine; the main goroutine waits for a
	// signal.
	errCh := make(chan error, 1)
	go func() {
		if err := a.HTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case sig := <-sigCh:
		a.Log.Info("shutdown signal received", "signal", sig.String())
	case err := <-errCh:
		if err != nil {
			a.Log.Error("HTTP server error", "error", err)
			return err
		}
	}

	// Graceful shutdown: cancel restart goroutine, drain HTTP, stop bot/pool,
	// close DB. Aggregate errors so a failed shutdown is visible to the
	// supervisor (A-010).
	a.restartCancel()
	a.restartWg.Wait()

	// Graceful-shutdown timeout mirrors the startup timeout (30s) — enough
	// to drain in-flight HTTP handlers, stop the bot/pool, and close Mongo
	// without hanging the supervisor.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var shutdownErr error
	if err := a.HTTP.Shutdown(shutdownCtx); err != nil {
		a.Log.Warn("HTTP shutdown error", "error", err)
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("http shutdown: %w", err))
	}
	// RA-A-009: stop background sweeps BEFORE the pool so they don't
	// race against pool teardown.
	if a.stopDedupCleanup != nil {
		a.stopDedupCleanup()
	}
	if a.stopLimiterSweep != nil {
		a.stopLimiterSweep()
	}
	// RA-A-003: Bot.Stop waits unboundedly on in-flight handlers. Wrap it
	// in a goroutine and bound it by shutdownCtx so a stuck handler can't
	// hang the whole shutdown past the operator's deadline.
	botDone := make(chan struct{})
	go func() { a.Bot.Stop(); close(botDone) }()
	select {
	case <-botDone:
	case <-shutdownCtx.Done():
		a.Log.Warn("bot stop timed out; some handlers may still be running")
	}
	a.Pool.Stop(shutdownCtx)
	// Stop the touch buffer BEFORE closing the store so the final BulkWrite
	// flush completes against a live mongo client (audit 9.3). Stop() closes
	// the channel and blocks on <-done until the flush goroutine drains +
	// flushes, so by the time Store.Close() disconnects the client there are
	// no in-flight writes.
	a.touchBuffer.Stop()
	if err := a.Store.Close(shutdownCtx); err != nil {
		a.Log.Warn("MongoDB close error", "error", err)
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("mongo close: %w", err))
	}
	a.Log.Info("shutdown complete")
	return shutdownErr
}
