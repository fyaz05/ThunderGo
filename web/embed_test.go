package web

import (
	"testing"
)

// TestAssetsFSContainsPlayer verifies the embedded templates directory is
// populated at build time (RA-F-010 / `make check-embed` equivalent). If this
// test fails, the go:embed directive in embed.go is misconfigured and the
// player page would 500 in production.
func TestAssetsFSContainsPlayer(t *testing.T) {
	t.Parallel()
	b, err := AssetsFS.ReadFile("templates/player.html")
	if err != nil {
		t.Fatalf("read player.html: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("player.html is empty")
	}
}
