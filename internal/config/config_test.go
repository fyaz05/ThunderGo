package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestChannelIDToRaw(t *testing.T) {
	t.Parallel()
	cases := map[int64]int64{
		0:              0,
		1234567890:     1234567890,
		-1001234567890: 1234567890,
		-42:            42,
	}
	for in, want := range cases {
		if got := ChannelIDToRaw(in); got != want {
			t.Errorf("ChannelIDToRaw(%d) = %d, want %d", in, got, want)
		}
	}
}

// unsetEnvVar unsets an environment variable for the duration of the test
// and restores its original value (or un-set state) on cleanup. Unlike
// t.Setenv(key, ""), which sets the variable to an empty string, this
// actually unsets it — the caarlos0/env library distinguishes between
// "empty" and "unset" for `required` int fields (an empty string fails
// with a parse error rather than a missing-required error), so the only
// honest way to test "missing required var" is to unset it (D-025).
func unsetEnvVar(t *testing.T, key string) {
	t.Helper()
	orig, hadOrig := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("os.Unsetenv(%q): %v", key, err)
	}
	t.Cleanup(func() {
		if hadOrig {
			if err := os.Setenv(key, orig); err != nil {
				t.Errorf("restore %q: %v", key, err)
			}
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

// TestLoad_EssentialMissing verifies that each required field, when unset
// (not merely set to ""), causes Load to fail with an error mentioning
// that field's env-var name. A helper sets ALL required fields to valid
// values, then a single field is unset per subtest (D-025).
func TestLoad_EssentialMissing(t *testing.T) {
	cases := []struct {
		name   string
		envVar string
		errSub string
	}{
		{"API_ID", "TG_API_ID", "TG_API_ID"},
		{"API_HASH", "TG_API_HASH", "TG_API_HASH"},
		{"BOT_TOKEN", "TG_BOT_TOKEN", "TG_BOT_TOKEN"},
		{"VAULT_CHANNEL_ID", "TG_VAULT_CHANNEL_ID", "TG_VAULT_CHANNEL_ID"},
		{"MONGO_URI", "TG_MONGO_URI", "TG_MONGO_URI"},
		{"URL", "TG_URL", "TG_URL"},
		{"OWNER_USER_ID", "TG_OWNER_USER_ID", "TG_OWNER_USER_ID"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setTestEnv(t)            // set every required var to a valid value
			unsetEnvVar(t, c.envVar) // then unset just the one under test
			_, err := Load()
			if err == nil {
				t.Fatalf("Load should fail when %s is unset", c.envVar)
			}
			if !strings.Contains(err.Error(), c.errSub) {
				t.Errorf("error should mention %q: got %v", c.errSub, err)
			}
		})
	}
}

func TestLoad_Success(t *testing.T) {
	setTestEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.APIID != 1234567 {
		t.Errorf("APIID = %d, want 1234567", cfg.APIID)
	}
	if cfg.BaseURL != "https://example.com" {
		t.Errorf("BaseURL = %q, want https://example.com", cfg.BaseURL)
	}
}

func TestLoad_URLParsing(t *testing.T) {
	setTestEnv(t)
	t.Setenv("TG_URL", "https://bot.herokuapp.com")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.BaseURL != "https://bot.herokuapp.com" {
		t.Errorf("BaseURL = %q", cfg.BaseURL)
	}
}

func TestLoad_InvalidURL(t *testing.T) {
	setTestEnv(t)
	t.Setenv("TG_URL", "not-a-url")
	_, err := Load()
	if err == nil {
		t.Fatal("should fail with invalid URL")
	}
}

func TestFileURL(t *testing.T) {
	t.Parallel()
	cfg := &Config{BaseURL: "https://example.com"}
	got := cfg.FileURL("abc123", "movie.mp4")
	want := "https://example.com/f/abc123/movie.mp4"
	if got != want {
		t.Errorf("FileURL = %q, want %q", got, want)
	}
}

func TestFileURL_SpecialChars(t *testing.T) {
	t.Parallel()
	cfg := &Config{BaseURL: "https://example.com"}
	got := cfg.FileURL("abc123", "my file #1.mp4")
	if !strings.Contains(got, "my%20file") {
		t.Errorf("FileURL should percent-encode spaces: %q", got)
	}
}

func TestFileURL_NormalizesSlashBeforeEscaping(t *testing.T) {
	t.Parallel()
	cfg := &Config{BaseURL: "https://example.com"}
	got := cfg.FileURL("abc123", "dir/movie.mp4")
	want := "https://example.com/f/abc123/dir_movie.mp4"
	if got != want {
		t.Errorf("FileURL = %q, want %q", got, want)
	}
}

func setTestEnv(t *testing.T) {
	t.Helper()
	vars := map[string]string{
		"TG_API_ID":           "1234567",
		"TG_API_HASH":         "abcdef0123456789abcdef0123456789",
		"TG_BOT_TOKEN":        "1234567:ABC-DEF",
		"TG_VAULT_CHANNEL_ID": "-1001234567890",
		"TG_OWNER_USER_ID":    "111111111",
		"TG_MONGO_URI":        "mongodb+srv://user:pass@cluster.mongodb.net",
		"TG_URL":              "https://example.com",
	}
	for k, v := range vars {
		t.Setenv(k, v)
	}
}

// --- Stream profile presets (Phase 3) ---

// TestStreamPreset pins the named-preset table (plan §4: basic=4/8/30/3,
// medium=6/12/45/3, high=8/16/60/5) and the unknown-name miss.
func TestStreamPreset(t *testing.T) {
	t.Parallel()
	want := map[string]StreamPresetSpec{
		"basic":  {Name: "basic", Concurrency: 4, BufferCount: 8, TimeoutSecs: 30, MaxRetries: 3},
		"medium": {Name: "medium", Concurrency: 6, BufferCount: 12, TimeoutSecs: 45, MaxRetries: 3},
		"high":   {Name: "high", Concurrency: 8, BufferCount: 16, TimeoutSecs: 60, MaxRetries: 5},
	}
	for name, spec := range want {
		got, ok := StreamPreset(name)
		if !ok {
			t.Fatalf("StreamPreset(%q) = ok=false, want true", name)
		}
		if got != spec {
			t.Errorf("StreamPreset(%q) = %+v, want %+v", name, got, spec)
		}
	}
	if _, ok := StreamPreset("turbo"); ok {
		t.Errorf("StreamPreset(turbo) = ok=true, want false")
	}
}

// TestLoad_StreamDefaults verifies that with NO stream env vars set the
// config falls back to the basic preset (4/8/30s/3) and that
// ENABLE_LEGACY_LINKS defaults to false.
func TestLoad_StreamDefaults(t *testing.T) {
	setTestEnv(t)
	unsetStreamEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.StreamConcurrency != 4 || cfg.StreamBufferCount != 8 {
		t.Errorf("defaults = %d/%d, want 4/8 (basic preset)", cfg.StreamConcurrency, cfg.StreamBufferCount)
	}
	if cfg.StreamTimeout != 30*time.Second {
		t.Errorf("StreamTimeout = %v, want 30s", cfg.StreamTimeout)
	}
	if cfg.StreamMaxRetries != 3 {
		t.Errorf("StreamMaxRetries = %d, want 3", cfg.StreamMaxRetries)
	}
	if cfg.EnableLegacyLinks {
		t.Errorf("EnableLegacyLinks = true, want false by default")
	}
}

// unsetStreamEnv unsets every stream knob for the duration of the test so
// ambient env or .env values can't skew precedence tests.
func unsetStreamEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"STREAM_PROFILE", "CONCURRENCY", "BUFFER_COUNT", "TIMEOUT_SEC", "MAX_RETRIES", "ENABLE_LEGACY_LINKS"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("os.Unsetenv(%q): %v", key, err)
		}
	}
}

// TestLoad_StreamProfilePrecedence pins the documented precedence:
// explicit env > profile preset > basic defaults.
func TestLoad_StreamProfilePrecedence(t *testing.T) {
	cases := []struct {
		name        string
		profile     string
		explicitKey string // one knob set explicitly, "" = none
		explicitVal string
		wantConc    int
		wantBuffer  int
		wantTimeout time.Duration
		wantRetries int
	}{
		{name: "no_profile_basic_defaults", wantConc: 4, wantBuffer: 8, wantTimeout: 30 * time.Second, wantRetries: 3},
		{name: "medium", profile: "medium", wantConc: 6, wantBuffer: 12, wantTimeout: 45 * time.Second, wantRetries: 3},
		{name: "high", profile: "high", wantConc: 8, wantBuffer: 16, wantTimeout: 60 * time.Second, wantRetries: 5},
		// Explicit beats profile: CONCURRENCY=10 overrides medium's 6;
		// the other three knobs still come from the medium preset.
		{
			name: "explicit_beats_profile", profile: "medium",
			explicitKey: "CONCURRENCY", explicitVal: "10",
			wantConc: 10, wantBuffer: 12, wantTimeout: 45 * time.Second, wantRetries: 3,
		},
		{
			name: "explicit_beats_profile_timeout", profile: "high",
			explicitKey: "TIMEOUT_SEC", explicitVal: "90",
			wantConc: 8, wantBuffer: 16, wantTimeout: 90 * time.Second, wantRetries: 5,
		},
		// Explicit beats the default too.
		{
			name:        "explicit_beats_default_retries",
			explicitKey: "MAX_RETRIES", explicitVal: "0",
			wantConc: 4, wantBuffer: 8, wantTimeout: 30 * time.Second, wantRetries: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setTestEnv(t)
			unsetStreamEnv(t)
			if c.profile != "" {
				t.Setenv("STREAM_PROFILE", c.profile)
			}
			if c.explicitKey != "" {
				t.Setenv(c.explicitKey, c.explicitVal)
			}
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load failed: %v", err)
			}
			if cfg.StreamConcurrency != c.wantConc {
				t.Errorf("StreamConcurrency = %d, want %d", cfg.StreamConcurrency, c.wantConc)
			}
			if cfg.StreamBufferCount != c.wantBuffer {
				t.Errorf("StreamBufferCount = %d, want %d", cfg.StreamBufferCount, c.wantBuffer)
			}
			if cfg.StreamTimeout != c.wantTimeout {
				t.Errorf("StreamTimeout = %v, want %v", cfg.StreamTimeout, c.wantTimeout)
			}
			if cfg.StreamMaxRetries != c.wantRetries {
				t.Errorf("StreamMaxRetries = %d, want %d", cfg.StreamMaxRetries, c.wantRetries)
			}
		})
	}
}

// TestLoad_StreamProfileInvalid verifies fail-fast on an unknown profile.
func TestLoad_StreamProfileInvalid(t *testing.T) {
	setTestEnv(t)
	unsetStreamEnv(t)
	t.Setenv("STREAM_PROFILE", "turbo")
	_, err := Load()
	if err == nil {
		t.Fatal("Load should fail with an invalid STREAM_PROFILE")
	}
	if !strings.Contains(err.Error(), "STREAM_PROFILE") {
		t.Errorf("error should mention STREAM_PROFILE: %v", err)
	}
}

// TestLoad_StreamKnobInvalidValue verifies fail-fast on a non-integer knob.
func TestLoad_StreamKnobInvalidValue(t *testing.T) {
	setTestEnv(t)
	unsetStreamEnv(t)
	t.Setenv("CONCURRENCY", "many")
	_, err := Load()
	if err == nil {
		t.Fatal("Load should fail with a non-integer CONCURRENCY")
	}
	if !strings.Contains(err.Error(), "CONCURRENCY") {
		t.Errorf("error should mention CONCURRENCY: %v", err)
	}
}

// TestLoad_StreamKnobBounds pins the clamping bounds: CONCURRENCY [1,64],
// BUFFER_COUNT [2,128], TIMEOUT_SEC [5,120], MAX_RETRIES [0,10].
func TestLoad_StreamKnobBounds(t *testing.T) {
	cases := []struct {
		key, val    string
		wantConc    int
		wantBuffer  int
		wantTimeout time.Duration
		wantRetries int
	}{
		{key: "CONCURRENCY", val: "0", wantConc: 1, wantBuffer: 8, wantTimeout: 30 * time.Second, wantRetries: 3},
		{key: "CONCURRENCY", val: "1000", wantConc: 64, wantBuffer: 8, wantTimeout: 30 * time.Second, wantRetries: 3},
		{key: "BUFFER_COUNT", val: "1", wantConc: 4, wantBuffer: 2, wantTimeout: 30 * time.Second, wantRetries: 3},
		{key: "BUFFER_COUNT", val: "999", wantConc: 4, wantBuffer: 128, wantTimeout: 30 * time.Second, wantRetries: 3},
		{key: "TIMEOUT_SEC", val: "1", wantConc: 4, wantBuffer: 8, wantTimeout: 5 * time.Second, wantRetries: 3},
		{key: "TIMEOUT_SEC", val: "999", wantConc: 4, wantBuffer: 8, wantTimeout: 120 * time.Second, wantRetries: 3},
		{key: "MAX_RETRIES", val: "-1", wantConc: 4, wantBuffer: 8, wantTimeout: 30 * time.Second, wantRetries: 0},
		{key: "MAX_RETRIES", val: "99", wantConc: 4, wantBuffer: 8, wantTimeout: 30 * time.Second, wantRetries: 10},
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.val, func(t *testing.T) {
			setTestEnv(t)
			unsetStreamEnv(t)
			t.Setenv(c.key, c.val)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load failed: %v", err)
			}
			if cfg.StreamConcurrency != c.wantConc {
				t.Errorf("StreamConcurrency = %d, want %d", cfg.StreamConcurrency, c.wantConc)
			}
			if cfg.StreamBufferCount != c.wantBuffer {
				t.Errorf("StreamBufferCount = %d, want %d", cfg.StreamBufferCount, c.wantBuffer)
			}
			if cfg.StreamTimeout != c.wantTimeout {
				t.Errorf("StreamTimeout = %v, want %v", cfg.StreamTimeout, c.wantTimeout)
			}
			if cfg.StreamMaxRetries != c.wantRetries {
				t.Errorf("StreamMaxRetries = %d, want %d", cfg.StreamMaxRetries, c.wantRetries)
			}
		})
	}
}

// TestLoad_EnableLegacyLinks verifies the legacy-links flag parses.
func TestLoad_EnableLegacyLinks(t *testing.T) {
	t.Run("default_false", func(t *testing.T) {
		setTestEnv(t)
		unsetStreamEnv(t)
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if cfg.EnableLegacyLinks {
			t.Errorf("EnableLegacyLinks = true, want false by default")
		}
	})
	t.Run("explicit_true", func(t *testing.T) {
		setTestEnv(t)
		unsetStreamEnv(t)
		t.Setenv("ENABLE_LEGACY_LINKS", "true")
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if !cfg.EnableLegacyLinks {
			t.Errorf("EnableLegacyLinks = false, want true")
		}
	})
}
