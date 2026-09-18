// Package config loads and validates runtime configuration from environment
// variables, optionally via .env files.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config holds all runtime configuration. Fields are exported for env parsing
// but MUST NOT be mutated after Load(). TG_URL is the single source of truth
// for scheme/host/port; TG_HTTP_PORT overrides the URL-derived port (for
// reverse-proxy setups where the URL points at 443 but the bot listens locally).
type Config struct {
	// Required
	APIID          int32  `env:"TG_API_ID,required"`
	APIHash        string `env:"TG_API_HASH,required,notEmpty"`
	BotToken       string `env:"TG_BOT_TOKEN,required,notEmpty"`
	VaultChannelID int64  `env:"TG_VAULT_CHANNEL_ID,required"`
	OwnerUserID    int64  `env:"TG_OWNER_USER_ID"`
	MongoURI       string `env:"TG_MONGO_URI,required,notEmpty"`
	BaseURL        string `env:"TG_URL,required,notEmpty"`

	// ExtraBots is populated manually in Load() by scanning TG_EXTRA_BOTS1,
	// TG_EXTRA_BOTS2, ... (caarlos0/env can't express indexed-scan semantics).
	ExtraBots []string

	// Optional — access control
	PrivateMode        bool    `env:"TG_PRIVATE_MODE"`
	ForceSubChannelID  int64   `env:"TG_FORCE_SUB_CHANNEL_ID"`
	BannedChannelIDs   []int64 `env:"TG_BANNED_CHANNEL_IDS" envSeparator:","`
	ChannelAutoProcess bool    `env:"TG_CHANNEL_AUTO_PROCESS"`

	// Optional — rate limiting
	RateLimit int `env:"TG_RATE_LIMIT"` // per-user files per minute (0 = disabled)
	GlobalRPS int `env:"TG_GLOBAL_RPS"` // global requests/sec cap (0 = disabled, burst = 2x RPS)

	// Optional — URL shortener
	ShortenerAPIKey string `env:"TG_SHORTENER_API_KEY"`
	ShortenerSite   string `env:"TG_SHORTENER_SITE"`

	// Token activation gate. When enabled, unauthorized users must activate via
	// a one-time link before using the bot. Owner and authorized users bypass.
	// File URLs themselves remain public — the gate is on bot command access only.
	TokenEnabled  bool `env:"TG_TOKEN_ENABLED"`                   // gate off by default (bot is open to everyone)
	TokenTTLHours int  `env:"TG_TOKEN_TTL_HOURS" envDefault:"24"` // activation duration in hours

	// Optional — tuning knobs (with sensible defaults)
	MaxConcurrentPerClient int    `env:"TG_MAX_CONCURRENT_PER_CLIENT" envDefault:"8"`       // max simultaneous downloads per bot client
	FileTTLDays            int    `env:"TG_FILE_TTL_DAYS"             envDefault:"0"`       // 0 = never expire; >0 = auto-expire files unseen for N days
	LogLevel               string `env:"TG_LOG_LEVEL"                 envDefault:"info"`    // debug|info|warn|error
	KeepaliveSecs          int    `env:"TG_KEEPALIVE_SECS"            envDefault:"300"`     // self-ping interval to keep PaaS processes awake (0 = disabled)
	BindAddress            string `env:"TG_BIND_ADDRESS"              envDefault:"0.0.0.0"` // interface to listen on
	HTTPPort               int    `env:"TG_HTTP_PORT"                 envDefault:"0"`       // 0 = derive from TG_URL scheme+port; >0 = override
	BatchCap               int    `env:"TG_BATCH_CAP"                 envDefault:"50"`      // max files per /link N batch

	// Auto-update (non-Docker)
	UpstreamRepo   string `env:"UPSTREAM_REPO"`                                // git URL for auto-update on restart
	UpstreamBranch string `env:"UPSTREAM_BRANCH"            envDefault:"main"` // branch to track

	// --- Stream pipeline knobs (Phase 3) ---
	//
	// STREAM_PROFILE selects one of the named presets (basic|medium|high)
	// for the adaptive windowed download pipeline. The four individual
	// knobs (CONCURRENCY, BUFFER_COUNT, TIMEOUT_SEC, MAX_RETRIES) are
	// parsed manually in Load() so that "explicitly set" can be detected
	// with os.LookupEnv (caarlos0/env cannot express that for ints).
	// Precedence per knob: explicit env > profile preset > basic defaults.
	StreamConcurrency int
	StreamBufferCount int
	StreamTimeout     time.Duration
	StreamMaxRetries  int

	// EnableLegacyLinks serves the pre-revival URL shapes (/watch/* and
	// 2-segment /f/{token} links). When false (default) those routes
	// answer 410 Gone so old links fail loudly instead of leaking tokens
	// into logs and search engines.
	EnableLegacyLinks bool `env:"ENABLE_LEGACY_LINKS"`
}

// StreamPresetSpec describes one named stream profile preset.
type StreamPresetSpec struct {
	Name        string
	Concurrency int
	BufferCount int
	TimeoutSecs int
	MaxRetries  int
}

// streamPresets is the named-preset table for the stream pipeline. "basic"
// doubles as the default when STREAM_PROFILE is unset or when an individual
// knob needs a fallback.
var streamPresets = map[string]StreamPresetSpec{
	"basic":  {Name: "basic", Concurrency: 4, BufferCount: 8, TimeoutSecs: 30, MaxRetries: 3},
	"medium": {Name: "medium", Concurrency: 6, BufferCount: 12, TimeoutSecs: 45, MaxRetries: 3},
	"high":   {Name: "high", Concurrency: 8, BufferCount: 16, TimeoutSecs: 60, MaxRetries: 5},
}

// StreamPreset returns the named preset spec (ok=false for unknown names).
// Exposed for tests and documentation; production parsing happens in Load.
func StreamPreset(name string) (StreamPresetSpec, bool) {
	p, ok := streamPresets[name]
	return p, ok
}

// Stream pipeline env-var names, parsed manually in loadStreamProfile.
const (
	envStreamConcurrency = "CONCURRENCY"
	envStreamBufferCount = "BUFFER_COUNT"
	envStreamTimeoutSec  = "TIMEOUT_SEC"
	envStreamMaxRetries  = "MAX_RETRIES"
	envStreamProfile     = "STREAM_PROFILE"
)

// Stream knob validation bounds. Values below the floor are clamped up,
// values above the cap are clamped down (with a stderr warning, matching the
// TG_BATCH_CAP style) so a fat-fingered env cannot wedge the pipeline.
const (
	streamConcurrencyFloor = 1
	streamConcurrencyCap   = 64
	streamBufferFloor      = 2
	streamBufferCap        = 128
	streamTimeoutFloorSecs = 5
	streamTimeoutCapSecs   = 120
	streamRetriesFloor     = 0
	streamRetriesCap       = 10
)

// Load reads .env files (best-effort), then parses environment variables into a Config.
func Load(filenames ...string) (*Config, error) {
	for _, f := range filenames {
		if err := loadDotenv(f); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("loading %s: %w", f, err)
		}
	}

	c := &Config{}
	if err := env.Parse(c); err != nil {
		return nil, fmt.Errorf("parsing environment: %w", err)
	}

	// BaseURL must be a full URL; strip trailing slash for clean FileURL/FileRawURL paths.
	rawURL := strings.TrimSpace(c.BaseURL)
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("TG_URL must be a full URL like https://bot.herokuapp.com (got: %s)", rawURL)
	}
	c.BaseURL = strings.TrimRight(rawURL, "/")

	// Indexed scan for extra bot tokens: TG_EXTRA_BOTS1 .. TG_EXTRA_BOTS50
	for i := 1; i <= 50; i++ {
		if tok := os.Getenv(fmt.Sprintf("TG_EXTRA_BOTS%d", i)); tok != "" {
			c.ExtraBots = append(c.ExtraBots, tok)
		}
	}

	if err := c.loadStreamProfile(); err != nil {
		return nil, err
	}

	if err := c.validate(); err != nil {
		return nil, err
	}

	return c, nil
}

// loadStreamProfile resolves the stream pipeline knobs.
//
// Precedence per knob: explicit env (CONCURRENCY / BUFFER_COUNT /
// TIMEOUT_SEC / MAX_RETRIES) beats the STREAM_PROFILE preset, which beats
// the "basic" preset values (also the default when STREAM_PROFILE is unset).
// STREAM_PROFILE is validated fail-fast at Load; the numeric knobs are
// clamped into their documented bounds.
func (c *Config) loadStreamProfile() error {
	profile := "basic"
	if raw, ok := os.LookupEnv(envStreamProfile); ok && strings.TrimSpace(raw) != "" {
		profile = strings.ToLower(strings.TrimSpace(raw))
	}
	preset, ok := streamPresets[profile]
	if !ok {
		return fmt.Errorf("%s must be one of basic|medium|high, got %q", envStreamProfile, profile)
	}

	if v, ok, err := lookupStreamInt(envStreamConcurrency); err != nil {
		return err
	} else if ok {
		c.StreamConcurrency = clampStream(v, streamConcurrencyFloor, streamConcurrencyCap, envStreamConcurrency)
	} else {
		c.StreamConcurrency = preset.Concurrency
	}
	if v, ok, err := lookupStreamInt(envStreamBufferCount); err != nil {
		return err
	} else if ok {
		c.StreamBufferCount = clampStream(v, streamBufferFloor, streamBufferCap, envStreamBufferCount)
	} else {
		c.StreamBufferCount = preset.BufferCount
	}
	if v, ok, err := lookupStreamInt(envStreamTimeoutSec); err != nil {
		return err
	} else if ok {
		c.StreamTimeout = time.Duration(clampStream(v, streamTimeoutFloorSecs, streamTimeoutCapSecs, envStreamTimeoutSec)) * time.Second
	} else {
		c.StreamTimeout = time.Duration(preset.TimeoutSecs) * time.Second
	}
	if v, ok, err := lookupStreamInt(envStreamMaxRetries); err != nil {
		return err
	} else if ok {
		c.StreamMaxRetries = clampStream(v, streamRetriesFloor, streamRetriesCap, envStreamMaxRetries)
	} else {
		c.StreamMaxRetries = preset.MaxRetries
	}
	return nil
}

// lookupStreamInt reads an integer env var. "Present but empty" counts as
// unset so `.env` files can carry placeholder "KEY=" lines. A present but
// unparseable value fails fast at Load.
func lookupStreamInt(key string) (int, bool, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return 0, false, nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, true, fmt.Errorf("%s must be an integer, got %q", key, raw)
	}
	return v, true, nil
}

// clampStream clamps v into [floor, ceiling] and warns when clamping occurred.
func clampStream(v, floor, ceiling int, key string) int {
	if v < floor {
		fmt.Fprintf(os.Stderr, "WARNING: %s=%d is below the minimum %d; clamping to %d\n", key, v, floor, floor)
		return floor
	}
	if v > ceiling {
		fmt.Fprintf(os.Stderr, "WARNING: %s=%d is above the maximum %d; clamping to %d\n", key, v, ceiling, ceiling)
		return ceiling
	}
	return v
}

func (c *Config) validate() error {
	if c.APIID <= 0 {
		return fmt.Errorf("TG_API_ID must be a positive integer")
	}
	// Channel IDs must carry the -100 prefix Telegram uses for channels/supergroups.
	const minChannelID = int64(-1000000000000)
	if c.VaultChannelID > minChannelID {
		return fmt.Errorf("TG_VAULT_CHANNEL_ID must be a -100-prefixed channel ID (<= -1000000000000), got %d", c.VaultChannelID)
	}
	if c.ForceSubChannelID != 0 && c.ForceSubChannelID > minChannelID {
		return fmt.Errorf("TG_FORCE_SUB_CHANNEL_ID must be a -100-prefixed channel ID (<= -1000000000000) or 0 (unset), got %d", c.ForceSubChannelID)
	}
	if c.OwnerUserID == 0 {
		return fmt.Errorf("TG_OWNER_USER_ID is required")
	}
	if c.TokenEnabled && c.TokenTTLHours <= 0 {
		return fmt.Errorf("TG_TOKEN_TTL_HOURS must be positive when TG_TOKEN_ENABLED is true, got %d", c.TokenTTLHours)
	}
	if c.MaxConcurrentPerClient < 1 {
		return fmt.Errorf("TG_MAX_CONCURRENT_PER_CLIENT must be >= 1, got %d", c.MaxConcurrentPerClient)
	}
	if c.FileTTLDays < 0 {
		return fmt.Errorf("TG_FILE_TTL_DAYS must be >= 0 (0 = never expire), got %d", c.FileTTLDays)
	}
	if c.KeepaliveSecs < 0 {
		return fmt.Errorf("TG_KEEPALIVE_SECS must be >= 0 (0 = disabled), got %d", c.KeepaliveSecs)
	}
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
		c.LogLevel = strings.ToLower(c.LogLevel)
	default:
		fmt.Fprintf(os.Stderr, "WARNING: Unknown TG_LOG_LEVEL=%q; defaulting to \"info\"\n", c.LogLevel)
		c.LogLevel = "info"
	}
	if c.HTTPPort < 0 || c.HTTPPort > 65535 {
		return fmt.Errorf("TG_HTTP_PORT must be 0 (auto-derive from TG_URL) or in [1, 65535], got %d", c.HTTPPort)
	}
	if c.BatchCap < 1 {
		fmt.Fprintf(os.Stderr, "WARNING: TG_BATCH_CAP=%d is invalid (must be >= 1); clamping to 50\n", c.BatchCap)
		c.BatchCap = 50
	}
	return nil
}

// ListenPort returns the HTTP listen port: TG_HTTP_PORT if >0 (override for
// reverse proxies), otherwise derived from TG_URL (explicit port wins, else
// scheme default: 443 for https, 8080 for http).
func (c *Config) ListenPort() int {
	if c.HTTPPort > 0 {
		return c.HTTPPort // explicit override
	}
	// Fallback: if the platform (Heroku, etc.) sets PORT, use it.
	if p := os.Getenv("PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 && n <= 65535 {
			return n
		}
	}
	parsed, err := url.Parse(c.BaseURL)
	if err != nil || parsed == nil {
		return 8080
	}
	return defaultPort(parsed)
}

// FileURL builds the public URL for a file's player page. File URLs are PUBLIC —
// no per-file credential.
func (c *Config) FileURL(hash, filename string) string {
	return fmt.Sprintf("%s/f/%s/%s", c.BaseURL, hash, escapeFileName(filename))
}

// FileRawURL builds the public URL for the raw-bytes endpoint.
func (c *Config) FileRawURL(hash, filename string) string {
	return fmt.Sprintf("%s/f/%s/%s/raw", c.BaseURL, hash, escapeFileName(filename))
}

// escapeFileName mirrors FileToLink's link-safe filename behavior. A slash
// must be replaced before PathEscape: net/http unescapes %2F into URL.Path
// before chi matches route segments, so escaping the slash alone can create a
// broken link with an extra path segment.
func escapeFileName(filename string) string {
	return url.PathEscape(strings.ReplaceAll(filename, "/", "_"))
}

// IsOwner reports whether the given user ID is the configured owner.
func (c *Config) IsOwner(userID int64) bool {
	return c.OwnerUserID != 0 && userID == c.OwnerUserID
}

// ChannelIDToRaw converts a -100-prefixed channel ID to the raw positive ID
// used in t.me/c/{id} URLs.
func ChannelIDToRaw(channelID int64) int64 {
	if channelID >= 0 {
		return channelID
	}
	const offset = int64(1000000000000)
	if channelID <= -offset {
		return -channelID - offset
	}
	return -channelID
}

// --- helpers ---

func defaultPort(u *url.URL) int {
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			return n
		}
	}
	if u.Scheme == "https" {
		return 443
	}
	return 8080
}

var loadDotenvOnce sync.Once

// loadDotenv parses a .env file and sets env vars without overriding existing ones.
//
//gosec:disable G304 // filename is a hardcoded literal (".env", ".env.local") passed by Load, not user input
func loadDotenv(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	loadDotenvOnce.Do(func() {
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			// Strip trailing `# ...` comments outside any quote pair (preserves KEY="a#b").
			line = stripInlineComment(line)
			kv := strings.SplitN(line, "=", 2)
			if len(kv) != 2 {
				continue
			}
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])
			if len(val) >= 2 {
				quote := val[0]
				if quote == '"' && val[len(val)-1] == '"' {
					inner := val[1 : len(val)-1]
					inner = strings.NewReplacer(`\\`, `\`, `\"`, `"`).Replace(inner)
					val = inner
				} else if quote == '\'' && val[len(val)-1] == '\'' {
					inner := val[1 : len(val)-1]
					inner = strings.NewReplacer(`\\`, `\`, `\'`, `'`).Replace(inner)
					val = inner
				} else {
					val = strings.Trim(val, "\"'")
				}
			} else {
				val = strings.Trim(val, "\"'")
			}
			if _, exists := os.LookupEnv(key); !exists {
				os.Setenv(key, val)
			}
		}
	})
	return nil
}

// stripInlineComment removes a trailing `# ...` comment from a dotenv line.
// '#' is a comment only when preceded by whitespace and outside any quote pair.
func stripInlineComment(line string) string {
	inSingle := false
	inDouble := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch c {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '#':
			if !inSingle && !inDouble && i > 0 {
				prev := line[i-1]
				if prev == ' ' || prev == '\t' {
					return strings.TrimRight(line[:i], " \t")
				}
			}
		}
	}
	return line
}
