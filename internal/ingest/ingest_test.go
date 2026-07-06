// Package ingest — pure-function tests for deterministicHash.
//
// Per audit finding 13.1, the ingest package does NOT attempt a full gogram
// mock — the gogram API surface used by Ingester.Ingest (Forward,
// DownloadMedia, GetMe, etc., all on *telegram.Client via *pool.Pool) would
// require a multi-day mockable-interface refactor. The full Ingest pipeline
// is therefore deferred to manual / staging tests against a real Telegram
// connection (Phase 3).
//
// What we CAN test in isolation is the pure function at the heart of the
// dedup pipeline: `deterministicHash(fileKey)`. It takes a string and returns
// a hex digest, with no I/O — making it trivially testable without any mock.
//
// Tests:
//   - TestDeterministicHash_Length: output is exactly 32 hex chars (= 128 bits).
//   - TestDeterministicHash_Deterministic: same input → same output across calls.
//   - TestDeterministicHash_DifferentInputs: distinct inputs → distinct outputs.
//   - TestDeterministicHash_KnownVector: pin a known sha256(fileKey)[:16] vector.
//   - TestDeterministicHash_HexAlphabet: output is in [0-9a-f].
//   - TestDeterministicHash_EmptyInput: empty input is handled (sha256 of "").
//   - TestDeterministicHash_PackBotFileIDLike: realistic PackBotFileID strings work.
package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// TestDeterministicHash_Length verifies the output is exactly 32 hex chars
// (= 128 bits of entropy, per spec §3.3).
func TestDeterministicHash_Length(t *testing.T) {
	t.Parallel()
	cases := []string{
		"",
		"a",
		"some-file-key",
		"AQADBAATY28lAAdkAAGj2ciBvb3bXckBAAEC",
		strings.Repeat("x", 1024),
	}
	for _, in := range cases {
		got := deterministicHash(in)
		if len(got) != 32 {
			t.Errorf("deterministicHash(%q) length = %d, want 32", in, len(got))
		}
	}
}

// TestDeterministicHash_Deterministic verifies the same input always produces
// the same output. Determinism is what makes the hash usable as a URL-level
// dedup key: two ingests of the same file must produce the same URL.
func TestDeterministicHash_Deterministic(t *testing.T) {
	t.Parallel()
	const in = "AQADBAATY28lAAdkAAGj2ciBvb3bXckBAAEC"
	got1 := deterministicHash(in)
	got2 := deterministicHash(in)
	got3 := deterministicHash(in)
	if got1 != got2 || got2 != got3 {
		t.Errorf("deterministicHash is not deterministic: %q, %q, %q", got1, got2, got3)
	}
}

// TestDeterministicHash_DifferentInputs verifies distinct inputs produce
// distinct outputs. (A collision on 128-bit sha256 truncation is
// cryptographically infeasible, so any practical test input will not
// collide.)
func TestDeterministicHash_DifferentInputs(t *testing.T) {
	t.Parallel()
	cases := []string{
		"file-key-1",
		"file-key-2",
		"file-key-3",
		"AQADBAATY28lAAdkAAGj2ciBvb3bXckBAAEC",
		"AQADBAATY28lAAdkAAGj2ciBvb3bXckBAAED", // 1-bit diff at the end
	}
	seen := make(map[string]string, len(cases)) // hash → input
	for _, in := range cases {
		h := deterministicHash(in)
		if prev, ok := seen[h]; ok {
			t.Errorf("deterministicHash collision: inputs %q and %q both produced %q", prev, in, h)
		}
		seen[h] = in
	}
}

// TestDeterministicHash_KnownVector pins the hash to its documented
// definition: sha256(fileKey) truncated to the first 16 bytes, hex-encoded.
// If someone "improves" the hash (e.g. switches to sha512, or changes the
// truncation length), this test will catch it and force a deliberate review.
func TestDeterministicHash_KnownVector(t *testing.T) {
	t.Parallel()
	const in = "hello-world"
	want := func() string {
		sum := sha256.Sum256([]byte(in))
		return hex.EncodeToString(sum[:16])
	}()
	got := deterministicHash(in)
	if got != want {
		t.Errorf("deterministicHash(%q) = %q, want %q (sha256(input)[:16] hex-encoded)", in, got, want)
	}
}

// TestDeterministicHash_HexAlphabet verifies the output only contains
// lowercase hex characters [0-9a-f]. Mixed case or non-hex chars would break
// URL handling and DB lookups (the hash is used as a URL token and a BSON
// index key).
func TestDeterministicHash_HexAlphabet(t *testing.T) {
	t.Parallel()
	const hexAlpha = "0123456789abcdef"
	cases := []string{"a", "abc", "long-file-key-12345", ""}
	for _, in := range cases {
		got := deterministicHash(in)
		for i, c := range got {
			if !strings.ContainsRune(hexAlpha, c) {
				t.Errorf("deterministicHash(%q)[%d] = %q, not a lowercase hex char", in, i, c)
			}
		}
	}
}

// TestDeterministicHash_EmptyInput verifies the empty-string input is handled
// without panicking. sha256("") is a well-defined digest; we don't expect
// callers to pass "" (FileKey would be empty → Ingest returns early), but
// the function must not crash.
func TestDeterministicHash_EmptyInput(t *testing.T) {
	t.Parallel()
	got := deterministicHash("")
	if got == "" {
		t.Fatal("deterministicHash(\"\") = empty string, want 32-char hex digest")
	}
	if len(got) != 32 {
		t.Errorf("deterministicHash(\"\") length = %d, want 32", len(got))
	}
	// Cross-check against the direct sha256 computation.
	sum := sha256.Sum256(nil)
	want := hex.EncodeToString(sum[:16])
	if got != want {
		t.Errorf("deterministicHash(\"\") = %q, want %q", got, want)
	}
}

// TestDeterministicHash_PackBotFileIDLike verifies the hash works on
// realistic PackBotFileID strings (base64-ish, ~40-60 chars). This is the
// shape of input FileKey returns in production.
func TestDeterministicHash_PackBotFileIDLike(t *testing.T) {
	t.Parallel()
	cases := []string{
		"AQADBAATY28lAAdkAAGj2ciBvb3bXckBAAEC",
		"BQADBAATY28lAAdkAAGj2ciBvb3bXckBAAEC",
		"AQADBAATY28lAAdkAAGj2ciBvb3bXckBAAEC.1",
		"AQAAAAAAAAD//////////////8AAAAAAAAA=",
	}
	for _, in := range cases {
		got := deterministicHash(in)
		if len(got) != 32 {
			t.Errorf("deterministicHash(%q) length = %d, want 32", in, len(got))
		}
	}
}
