// Package http is the HTTP gateway: streaming handler, HTML player,
// health endpoints, and CORS preflight.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/fyaz05/ThunderGo/internal/config"
	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/stream"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
	"github.com/fyaz05/ThunderGo/web"
)

// Version is the application version, overridable at link time. Defaults to "dev".
var Version = "1.0"

// Server is the HTTP gateway.
type Server struct {
	Cfg   *config.Config
	Pool  *pool.Pool
	Store *store.Store
	Log   *slog.Logger

	StartedAt time.Time
	Router    http.Handler
	Templates *template.Template

	streamHandler *stream.Handler
	httpServer    *http.Server
	keepaliveStop chan struct{}
	keepaliveOnce sync.Once // guards close(keepaliveStop) against double-close

	botUsername string
}

// New wires up the routes and returns a Server ready to ListenAndServe.
func New(cfg *config.Config, p *pool.Pool, s *store.Store, in *ingest.Ingester, log *slog.Logger) (*Server, error) {
	tmpl := template.New("player.html")
	tmpl, err := tmpl.ParseFS(web.AssetsFS, "templates/player.html")
	if err != nil {
		// Fallback: parse from filesystem (dev mode).
		tmpl, err = template.New("player.html").ParseFiles("web/templates/player.html")
		if err != nil {
			return nil, fmt.Errorf("parsing player template: %w", err)
		}
	}

	srv := &Server{
		Cfg:           cfg,
		Pool:          p,
		Store:         s,
		Log:           log,
		StartedAt:     time.Now(),
		Templates:     tmpl,
		streamHandler: stream.New(p, s, in, log),
		keepaliveStop: make(chan struct{}),
	}

	if primary := p.Primary(); primary != nil {
		if me, err := primary.GetMe(); err == nil && me != nil {
			srv.botUsername = me.Username
		}
	}

	srv.Router = srv.buildRouter()
	return srv, nil
}

func (s *Server) buildRouter() http.Handler {
	r := chi.NewRouter()

	r.Use(s.corsMiddleware)
	r.Use(s.logMiddleware)

	// Health & meta routes.
	r.Get("/", s.handleRoot)
	r.Get("/health", s.handleHealth)
	r.Get("/status", s.handleStatus)

	// Activation route: 302 to Telegram deep link. Token consumed in /start so
	// the web redirect can't burn it before the user opens Telegram.
	r.Get("/activate/{token}", s.handleActivate)

	// Register HEAD on the player page route so HEAD probes get the same
	// headers as GET instead of a 405.
	r.Get("/f/{token}/{filename}", s.handlePlayerPage)
	r.Head("/f/{token}/{filename}", s.handlePlayerPage)
	r.Head("/f/{token}/{filename}/raw", s.handleStream)
	r.Get("/f/{token}/{filename}/raw", s.handleStream)

	return r
}

// ListenAndServe starts the HTTP gateway on the configured bind address.
// Operators behind a reverse proxy should bind 127.0.0.1 only.
func (s *Server) ListenAndServe() error {
	addr := fmt.Sprintf("%s:%d", s.Cfg.BindAddress, s.Cfg.ListenPort())
	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           s.Router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       0, // streaming downloads can take a long time
		WriteTimeout:      0, // streaming uploads can take a long time
		IdleTimeout:       120 * time.Second,
	}
	s.Log.Info("HTTP gateway listening", "addr", addr, "base_url", s.Cfg.BaseURL)
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops the HTTP gateway. Safe to call multiple times.
func (s *Server) Shutdown(ctx context.Context) error {
	s.keepaliveOnce.Do(func() { close(s.keepaliveStop) })
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

// StartKeepalive pings /health at the configured interval to keep the process
// awake on PaaS providers that sleep idle processes. TG_KEEPALIVE_SECS=0 disables.
func (s *Server) StartKeepalive() {
	interval := time.Duration(s.Cfg.KeepaliveSecs) * time.Second
	if interval <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		client := &http.Client{Timeout: 10 * time.Second}
		for {
			select {
			case <-s.keepaliveStop:
				return
			case <-t.C:
				resp, err := client.Get(s.Cfg.BaseURL + "/health")
				if err != nil {
					s.Log.Warn("keepalive ping failed", "error", err)
					continue
				}
				if resp.StatusCode != http.StatusOK {
					s.Log.Warn("keepalive ping returned non-200", "status", resp.StatusCode)
				}
				// Drain the body before Close so the TCP connection can be
				// reused by the keep-alive client.
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}
	}()
}

// --- middleware ---

// corsMiddleware handles OPTIONS preflight and adds wildcard CORS headers so
// browser players on third-party sites can embed file links (the link IS the
// secret; 128-bit token entropy defends against enumeration).
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Range, Content-Type, Accept, Authorization")
		h.Set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Content-Disposition")
		h.Set("Access-Control-Max-Age", "86400")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(ww, r)
		s.Log.Info("http",
			"method", r.Method,
			"path", redactPath(r.URL.Path),
			"status", ww.status,
			"bytes", ww.bytes,
			"duration_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
		)
	})
}

// redactPath masks the token in /f/{token}/... and /activate/{token} paths
// so the access log never persists the bearer credential.
func redactPath(p string) string {
	if !strings.HasPrefix(p, "/f/") && !strings.HasPrefix(p, "/activate/") {
		return p
	}
	// /f/{token}/{filename} → 4 segments; /activate/{token} → 3 segments.
	parts := strings.SplitN(p, "/", 4)
	if len(parts) < 3 || parts[2] == "" {
		return p
	}
	parts[2] = tgutil.TokenHash(parts[2])
	return strings.Join(parts, "/")
}

// statusRecorder wraps ResponseWriter to capture status and bytes for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Flush delegates to the underlying ResponseWriter so streaming flushes work.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// --- routes ---

// handleRoot redirects to the project homepage.
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "https://github.com/fyaz05/ThunderGo", http.StatusFound)
}

// handleActivate 302-redirects to the Telegram deep link that fires /start.
// Token is consumed in /start so it's only burned when the user reaches the bot.
func (s *Server) handleActivate(w http.ResponseWriter, r *http.Request) {
	token := tgutil.NormalizeBase32(chi.URLParam(r, "token"))
	if token == "" {
		http.NotFound(w, r)
		return
	}
	if s.botUsername == "" {
		// Bot username empty at startup → misconfigured; 503 so monitoring catches it.
		s.Log.Warn("activate: bot username not cached; cannot redirect", "path", r.URL.Path)
		http.Error(w, "bot username not available", http.StatusServiceUnavailable)
		return
	}
	target := fmt.Sprintf("tg://%s?start=%s", s.botUsername, token)
	http.Redirect(w, r, target, http.StatusFound)
}

// handleHealth is a lightweight health check — 200 OK with no DB or API calls.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// StatusResponse is the JSON health document returned by /status.
type StatusResponse struct {
	Status        string         `json:"status"`
	Version       string         `json:"version"`
	Uptime        string         `json:"uptime"`
	UptimeSecs    int64          `json:"uptime_secs"`
	BotUsername   string         `json:"bot_username"`
	ClientCount   int            `json:"client_count"`
	TotalInflight int64          `json:"total_inflight"`
	Clients       []ClientStatus `json:"clients"`
}

type ClientStatus struct {
	Index    int   `json:"index"`
	Inflight int64 `json:"inflight"`
	DC       int   `json:"dc"`
}

// handleStatus returns operational telemetry (clients, inflight, uptime).
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	perClient := s.Pool.PerClientInflight()
	all := s.Pool.All()
	clients := make([]ClientStatus, len(perClient))
	for i, n := range perClient {
		dc := 0
		if i < len(all) && all[i] != nil {
			dc = all[i].GetDC()
		}
		clients[i] = ClientStatus{Index: i, Inflight: n, DC: dc}
	}
	resp := StatusResponse{
		Status:        "ok",
		Version:       Version,
		Uptime:        time.Since(s.StartedAt).Round(time.Second).String(),
		UptimeSecs:    int64(time.Since(s.StartedAt).Seconds()),
		BotUsername:   s.botUsername,
		ClientCount:   s.Pool.Len(),
		TotalInflight: s.Pool.TotalInflight(),
		Clients:       clients,
	}
	// Status is operational telemetry — never cache.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.Log.Debug("status encode error", "error", err)
	}
}

// handlePlayerPage renders the HTML player page for a file.
func (s *Server) handlePlayerPage(w http.ResponseWriter, r *http.Request) {
	token := tgutil.NormalizeBase32(chi.URLParam(r, "token"))
	if token == "" {
		http.NotFound(w, r)
		return
	}

	rec, err := s.Store.FindFileByHash(r.Context(), token)
	if err != nil {
		s.Log.Error("file lookup failed", "token", tgutil.TokenHash(token), "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if rec == nil || rec.Size <= 0 {
		// 128-bit token entropy makes online enumeration infeasible.
		http.NotFound(w, r)
		return
	}

	rawURL := s.Cfg.FileRawURL(token, rec.FileName) + "?disposition=inline"
	downloadURL := s.Cfg.FileRawURL(token, rec.FileName)

	pageData := playerPageData{
		FileName:      rec.FileName,
		MimeType:      rec.MimeType,
		Size:          rec.Size,
		SizeFormatted: formatBytes(rec.Size),
		StreamURL:     rawURL,
		DownloadURL:   downloadURL,
		IsVideo:       strings.HasPrefix(rec.MimeType, "video/"),
		IsAudio:       strings.HasPrefix(rec.MimeType, "audio/"),
		IsImage:       strings.HasPrefix(rec.MimeType, "image/"),
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.Templates.ExecuteTemplate(w, "player.html", pageData); err != nil {
		s.Log.Error("rendering player page", "error", err)
	}
}

type playerPageData struct {
	FileName      string
	MimeType      string
	Size          int64
	SizeFormatted string
	StreamURL     string
	DownloadURL   string
	IsVideo       bool
	IsAudio       bool
	IsImage       bool
}

// handleStream extracts {token}, stashes it in the request context, then
// delegates to stream.Handler.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	ctx := stream.WithToken(r.Context(), chi.URLParam(r, "token"))
	r = r.WithContext(ctx)
	s.streamHandler.ServeHTTP(w, r)
}

// formatBytes returns a human-readable byte size (e.g. "1.5 MiB").
func formatBytes(n int64) string {
	return tgutil.FormatBytes(n)
}
