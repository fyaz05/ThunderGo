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
}

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

	if err := c.validate(); err != nil {
		return nil, err
	}

	return c, nil
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
