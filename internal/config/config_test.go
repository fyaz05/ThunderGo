package config

import (
	"os"
	"strings"
	"testing"
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
