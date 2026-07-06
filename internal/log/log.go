// Package log initializes a structured slog logger writing JSON to stdout
// and a lumberjack-rotated file.
package log

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	defaultLogger atomic.Pointer[slog.Logger]
	initMu        sync.Mutex
	initialized   bool
	initErr       error
)

// Init initializes the global default logger: JSON to stdout + lumberjack-rotated file.
// If filename is empty, falls back to TG_LOG_FILE, then "thundergo.log".
// Rotation: 100 MiB max, 5 backups, 14-day retention, uncompressed. Re-calling
// is safe — a successful call is idempotent; a failed call may be retried.
func Init(filename, level string) error {
	initMu.Lock()
	defer initMu.Unlock()

	if initialized && initErr == nil {
		return nil // already successfully initialized
	}

	// Reset state so a retry starts clean.
	initErr = nil

	if filename == "" {
		// Undocumented escape hatch for operators who need a custom path.
		filename = os.Getenv("TG_LOG_FILE")
		if filename == "" {
			filename = "/var/log/thundergo/thundergo.log"
		}
	}
	if dir := filepath.Dir(filename); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			initErr = fmt.Errorf("creating log directory %s: %w", dir, err)
			return initErr
		}
		fmt.Fprintf(os.Stderr, "log directory created: %s\n", dir)
	}

	fileRotator := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    100, // MiB
		MaxBackups: 5,
		MaxAge:     14,    // days
		Compress:   false, // true stalls the caller's goroutine on gzip during rotation
	}

	tee := io.MultiWriter(os.Stdout, fileRotator)

	slogLevel := parseLevel(level)
	handler := slog.NewJSONHandler(tee, &slog.HandlerOptions{Level: slogLevel})
	defaultLogger.Store(slog.New(handler))
	slog.SetDefault(defaultLogger.Load())

	initialized = true
	initErr = nil
	return nil
}

// L returns the package-level default logger. If Init has not been called,
// returns slog.Default() (stdout at Info level).
func L() *slog.Logger {
	if l := defaultLogger.Load(); l != nil {
		return l
	}
	return slog.Default()
}

// parseLevel maps a case-insensitive level string to slog.Level. Unknown values fall back to Info.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		if s != "" {
			fmt.Fprintf(os.Stderr, "WARNING: Unknown log level %q; defaulting to \"info\"\n", s)
		}
		return slog.LevelInfo
	}
}
