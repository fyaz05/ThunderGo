package tgutil

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestEncodeDecodeBase32(t *testing.T) {
	t.Parallel()
	cases := [][]byte{
		{0x00},
		{0xff},
		{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a},
		make([]byte, 16),
	}
	// 16 zero bytes — known answer: "00000000000000000000000000"
	encoded := EncodeBase32(make([]byte, 16))
	if encoded != "00000000000000000000000000" {
		t.Errorf("16 zero bytes: got %q, want 26 zeros", encoded)
	}
	if len(encoded) != 26 {
		t.Errorf("16 bytes should produce 26 base32 chars, got %d", len(encoded))
	}
	for _, in := range cases {
		out := EncodeBase32(in)
		dec, err := DecodeBase32(out)
		if err != nil {
			t.Errorf("decode(%q) error: %v", out, err)
			continue
		}
		if len(dec) != len(in) {
			t.Errorf("round-trip length mismatch: in=%d, decoded=%d", len(in), len(dec))
			continue
		}
		for i := range in {
			if in[i] != dec[i] {
				t.Errorf("round-trip mismatch at byte %d: in=%x, decoded=%x", i, in[i], dec[i])
				break
			}
		}
	}
}

func TestNormalizeBase32(t *testing.T) {
	t.Parallel()
	// Crockford normalization: O→0, I→1, L→1; case-insensitive.
	cases := map[string]string{
		// 32-char Crockford alphabet, with I/L/O mixed in to exercise the
		// normalization map.
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567": "abcdefgh1jk1mn0pqrstuvwxyz234567",
		// 3 Os + 4 Is + 3 Ls = 10 chars; each maps to 0 or 1.
		"OOOIIIILLL": "0001111111",
		"abcdef":     "abcdef",
		"ABCDEF":     "abcdef",
	}
	for in, want := range cases {
		got := NormalizeBase32(in)
		if got != want {
			t.Errorf("NormalizeBase32(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseRange_FullBody(t *testing.T) {
	t.Parallel()
	rng, hasRange, err := ParseRange("", 1000)
	if err != nil || hasRange {
		t.Errorf("empty Range should mean full body: rng=%v hasRange=%v err=%v", rng, hasRange, err)
	}
}

func TestParseRange_Simple(t *testing.T) {
	t.Parallel()
	rng, hasRange, err := ParseRange("bytes=0-1023", 2000)
	if err != nil || !hasRange {
		t.Fatalf("expected ok, got err=%v hasRange=%v", err, hasRange)
	}
	if rng.Start != 0 || rng.End != 1023 {
		t.Errorf("got Start=%d End=%d, want 0..1023", rng.Start, rng.End)
	}
}

func TestParseRange_Suffix(t *testing.T) {
	t.Parallel()
	rng, hasRange, err := ParseRange("bytes=-512", 2000)
	if err != nil || !hasRange {
		t.Fatalf("suffix range failed: err=%v hasRange=%v", err, hasRange)
	}
	// Last 512 bytes of a 2000-byte file: 1488..1999
	if rng.Start != 1488 || rng.End != 1999 {
		t.Errorf("suffix range: got %d..%d, want 1488..1999", rng.Start, rng.End)
	}
}

func TestParseRange_OpenEnded(t *testing.T) {
	t.Parallel()
	rng, hasRange, err := ParseRange("bytes=1000-", 2000)
	if err != nil || !hasRange {
		t.Fatalf("open-ended range failed: err=%v hasRange=%v", err, hasRange)
	}
	if rng.Start != 1000 || rng.End != 1999 {
		t.Errorf("open-ended: got %d..%d, want 1000..1999", rng.Start, rng.End)
	}
}

func TestParseRange_OutOfBounds(t *testing.T) {
	t.Parallel()
	_, _, err := ParseRange("bytes=5000-6000", 2000)
	if err != ErrUnsatisfiableRange {
		t.Errorf("out-of-bounds should be ErrUnsatisfiableRange, got %v", err)
	}
}

func TestParseRange_Malformed(t *testing.T) {
	t.Parallel()
	cases := []string{
		"chars=0-1", // wrong unit
		"bytes=",    // empty spec
		"bytes=a-b", // non-numeric
	}
	for _, c := range cases {
		_, _, err := ParseRange(c, 2000)
		if err != ErrMalformedRange {
			t.Errorf("ParseRange(%q): got %v, want ErrMalformedRange", c, err)
		}
	}
}

func TestParseRange_MultiRangeCoalesced(t *testing.T) {
	t.Parallel()
	// E-023: multi-range requests are coalesced to the first range (matching
	// http.ServeContent behavior). Previously rejected with ErrMalformedRange.
	rng, hasRange, err := ParseRange("bytes=0-1,2-3", 2000)
	if err != nil || !hasRange {
		t.Fatalf("multi-range should coalesce to first range: err=%v hasRange=%v", err, hasRange)
	}
	if rng.Start != 0 || rng.End != 1 {
		t.Errorf("multi-range: got %d..%d, want 0..1", rng.Start, rng.End)
	}

	// Multi-range with whitespace after the comma should also coalesce.
	rng2, hasRange2, err2 := ParseRange("bytes=100-199, 200-299", 2000)
	if err2 != nil || !hasRange2 {
		t.Fatalf("multi-range with space: err=%v hasRange=%v", err2, hasRange2)
	}
	if rng2.Start != 100 || rng2.End != 199 {
		t.Errorf("multi-range with space: got %d..%d, want 100..199", rng2.Start, rng2.End)
	}

	// Multi-range where the first range is empty after split → malformed.
	_, _, err3 := ParseRange("bytes=,0-1", 2000)
	if err3 != ErrMalformedRange {
		t.Errorf("multi-range with empty first range: got %v, want ErrMalformedRange", err3)
	}
}

func TestParseRange_Unsatisfiable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		header string
		size   int64
	}{
		{"bytes=10-5", 2000},      // end < start
		{"bytes=2000-2001", 2000}, // start >= size
	}
	for _, c := range cases {
		_, _, err := ParseRange(c.header, c.size)
		if err != ErrUnsatisfiableRange {
			t.Errorf("ParseRange(%q, %d): got %v, want ErrUnsatisfiableRange", c.header, c.size, err)
		}
	}
}

func TestQueryDisposition(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":           "attachment",
		"inline":     "inline",
		"INLINE":     "inline",
		"attachment": "attachment",
		"foo":        "attachment",
		"foo=bar":    "attachment", // "foo=bar" isn't a valid disposition value
	}
	for in, want := range cases {
		r, _ := http.NewRequest("GET", "/?disposition="+in, nil)
		got := QueryDisposition(r)
		if got != want {
			t.Errorf("QueryDisposition(disposition=%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSanitizeForDisposition(t *testing.T) {
	t.Parallel()
	// ASCII fallback: printable ASCII (except " and \) is preserved.
	// The RFC 6266 spec allows spaces within quoted filename values, so
	// "movie (2024).mp4" stays intact.
	ascii, ext := SanitizeForDisposition("movie (2024).mp4")
	if ascii != "movie (2024).mp4" {
		t.Errorf("ascii fallback: got %q, want %q", ascii, "movie (2024).mp4")
	}
	if ext == "" {
		t.Errorf("extended form should be non-empty")
	}
	// Extended form should start with UTF-8''
	if len(ext) < 7 || ext[:7] != "UTF-8''" {
		t.Errorf("extended form should start with UTF-8'': got %q", ext)
	}

	// Non-ASCII chars get percent-encoded in the extended form and replaced
	// with underscore in the ASCII fallback.
	ascii2, ext2 := SanitizeForDisposition("фильл.mp4")
	if ext2 == "" {
		t.Errorf("extended form for Cyrillic name should be non-empty")
	}
	// ASCII fallback should not contain any Cyrillic bytes.
	for _, r := range ascii2 {
		if r >= 0x80 {
			t.Errorf("ascii fallback should not contain non-ASCII: got %q", ascii2)
			break
		}
	}
}

// TestNilSafe verifies every exported extractor/key/type function is nil-safe:
// calling with a nil *telegram.NewMessage must not panic and must return a
// sensible zero value (D-017).
func TestNilSafe(t *testing.T) {
	t.Parallel()
	t.Run("ExtractFileName", func(t *testing.T) {
		name, ok := ExtractFileName(nil)
		if name != "" || ok {
			t.Errorf("ExtractFileName(nil) = (%q, %v), want (\"\", false)", name, ok)
		}
	})
	t.Run("ExtractMIME", func(t *testing.T) {
		if got := ExtractMIME(nil); got != "application/octet-stream" {
			t.Errorf("ExtractMIME(nil) = %q, want application/octet-stream", got)
		}
	})
	t.Run("ExtractSize", func(t *testing.T) {
		if got := ExtractSize(nil); got != 0 {
			t.Errorf("ExtractSize(nil) = %d, want 0", got)
		}
	})
	t.Run("ExtractDcID", func(t *testing.T) {
		if got := ExtractDcID(nil); got != 0 {
			t.Errorf("ExtractDcID(nil) = %d, want 0", got)
		}
	})
	t.Run("FileKey", func(t *testing.T) {
		if got := FileKey(nil); got != "" {
			t.Errorf("FileKey(nil) = %q, want empty", got)
		}
	})
	t.Run("MediaType", func(t *testing.T) {
		if got := MediaType(nil); got != "document" {
			t.Errorf("MediaType(nil) = %q, want document", got)
		}
	})
}

// TestEncodeBase32_RoundTrip is a table-driven round-trip test for
// EncodeBase32 / DecodeBase32 (D-017). Empty input is documented to
// produce empty output; all other inputs must survive a full round-trip.
func TestEncodeBase32_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"single_zero", []byte{0x00}},
		{"single_ff", []byte{0xff}},
		{"two_bytes", []byte{0x00, 0xff}},
		{"four_bytes", []byte{0xde, 0xad, 0xbe, 0xef}},
		{"eight_bytes", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}},
		{"ten_bytes", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}},
		{"sixteen_zeros", make([]byte, 16)},
		{"sixteen_random", []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			encoded := EncodeBase32(c.in)
			decoded, err := DecodeBase32(encoded)
			if err != nil {
				t.Fatalf("DecodeBase32(%q) error: %v", encoded, err)
			}
			if !bytes.Equal(decoded, c.in) {
				t.Errorf("round-trip mismatch: in=%x, encoded=%q, decoded=%x", c.in, encoded, decoded)
			}
		})
	}
}

// TestEncodeBase32_OutputLength verifies the documented output length: 16
// random bytes (128 bits) produce a 26-char Crockford base32 string.
func TestEncodeBase32_OutputLength(t *testing.T) {
	t.Parallel()
	if got := EncodeBase32(make([]byte, 16)); len(got) != 26 {
		t.Errorf("16-byte input: got %d chars, want 26", len(got))
	}
	// Empty input must produce empty output (not a panic).
	if got := EncodeBase32(nil); got != "" {
		t.Errorf("nil input: got %q, want empty", got)
	}
	if got := EncodeBase32([]byte{}); got != "" {
		t.Errorf("empty input: got %q, want empty", got)
	}
}

// TestDecodeBase32_Invalid is a table-driven test for invalid inputs that
// must produce an error (D-017).
func TestDecodeBase32_Invalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
	}{
		{"excluded_char_u", "uuuu"},
		{"special_char_at", "ab@c"},
		{"internal_space", "ab c"},
		{"dash", "a-b"},
		{"plus", "a+b"},
		{"non_ascii", "abcdé"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := DecodeBase32(c.input); err == nil {
				t.Errorf("DecodeBase32(%q) should return error", c.input)
			}
		})
	}
}

// TestDecodeBase32_Empty verifies empty input returns (nil, nil).
func TestDecodeBase32_Empty(t *testing.T) {
	t.Parallel()
	out, err := DecodeBase32("")
	if err != nil {
		t.Errorf("DecodeBase32(\"\") error: %v", err)
	}
	if out != nil {
		t.Errorf("DecodeBase32(\"\") = %v, want nil", out)
	}
}

// TestDecodeBase32_NonZeroTrailingBits verifies that DecodeBase32 rejects
// inputs whose final character carries non-zero trailing bits (D-019).
// EncodeBase32 always zero-pads the low bits of the final character, so any
// canonical encoded string has all-zero trailing bits. A non-zero value means
// the input was not produced by EncodeBase32 and could decode to multiple
// distinct byte sequences — we reject it for canonicalization.
func TestDecodeBase32_NonZeroTrailingBits(t *testing.T) {
	t.Parallel()
	// Crockford alphabet: ... 'w'=28 (11100), 'x'=29 (11101), 'y'=30 (11110), 'z'=31 (11111).
	// EncodeBase32([]byte{0xff}) == "zw" (the encoder pads the final char's
	// low 2 bits with zeros: 'z'=11111, 'w'=11100 → byte 0xff + 2 trailing zero bits).
	// Replacing 'w' with any char whose low 2 bits are non-zero yields a
	// non-canonical string that must be rejected.
	cases := []struct {
		name  string
		input string
	}{
		// 2-char input, 1 byte (8 bits) + 2 trailing bits — last char's low 2 bits must be 0.
		{"2char_zz", "zz"}, // 11111 11111 → trailing 2 bits = 11
		{"2char_zy", "zy"}, // 11111 11110 → trailing 2 bits = 10
		{"2char_zx", "zx"}, // 11111 11101 → trailing 2 bits = 01
		// 1-char input, 0 bytes + 5 trailing bits — all 5 bits must be 0.
		{"1char_1", "1"}, // 00001 → trailing 5 bits = 00001
		{"1char_v", "v"}, // 11111 → trailing 5 bits = 11111
		// 4-char input, 2 bytes (16 bits) + 4 trailing bits — last char's low 4 bits must be 0.
		{"4char_0001", "0001"}, // last char 00001 → trailing 4 bits = 0001
		{"4char_000z", "000z"}, // last char 11111 → trailing 4 bits = 1111
		// 7-char input, 4 bytes (32 bits) + 3 trailing bits — last char's low 3 bits must be 0.
		{"7char_0000001", "0000001"}, // last char 00001 → trailing 3 bits = 001
		{"7char_000000z", "000000z"}, // last char 11111 → trailing 3 bits = 111
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := DecodeBase32(c.input)
			if err == nil {
				t.Errorf("DecodeBase32(%q) should return error for non-zero trailing bits, got out=%v err=nil", c.input, out)
			}
		})
	}
}

// TestDecodeBase32_ZeroTrailingBits verifies that strings whose trailing bits
// are all zero (i.e. canonical encodings) decode without error, even when the
// byte length is not a multiple of 5 bits (D-019).
func TestDecodeBase32_ZeroTrailingBits(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
		want  []byte
	}{
		// 2-char input, 1 byte + 2 zero trailing bits.
		// "zw" == EncodeBase32([]byte{0xff}).
		{"2char_zw", "zw", []byte{0xff}},
		// 4-char input, 2 bytes + 4 zero trailing bits.
		// "03zg" == EncodeBase32([]byte{0x00, 0xff}).
		{"4char_03zg", "03zg", []byte{0x00, 0xff}},
		// 1-char input, 0 bytes + 5 zero trailing bits (canonical: only "0").
		{"1char_0", "0", []byte{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := DecodeBase32(c.input)
			if err != nil {
				t.Fatalf("DecodeBase32(%q) error: %v", c.input, err)
			}
			if !bytes.Equal(out, c.want) {
				t.Errorf("DecodeBase32(%q) = %x, want %x", c.input, out, c.want)
			}
		})
	}
}

// TestParseRange_TableDriven consolidates valid and invalid Range-header
// cases into a single table-driven test (D-017).
func TestParseRange_TableDriven(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		header    string
		size      int64
		wantStart int64
		wantEnd   int64
		wantHas   bool
		wantErr   error
	}{
		// No Range header → full body.
		{"empty_header", "", 1000, 0, 0, false, nil},
		// Valid ranges.
		{"simple", "bytes=0-1023", 2000, 0, 1023, true, nil},
		{"suffix", "bytes=-512", 2000, 1488, 1999, true, nil},
		{"open_ended", "bytes=1000-", 2000, 1000, 1999, true, nil},
		{"single_byte", "bytes=500-500", 2000, 500, 500, true, nil},
		{"end_clamped", "bytes=0-9999", 2000, 0, 1999, true, nil},
		{"suffix_clamped", "bytes=-5000", 2000, 0, 1999, true, nil},
		// Malformed.
		{"wrong_unit", "chars=0-1", 2000, 0, 0, false, ErrMalformedRange},
		{"empty_spec", "bytes=", 2000, 0, 0, false, ErrMalformedRange},
		{"non_numeric", "bytes=a-b", 2000, 0, 0, false, ErrMalformedRange},
		{"multi_range_coalesced", "bytes=0-1,2-3", 2000, 0, 1, true, nil}, // E-023: coalesces to first range
		{"missing_dash", "bytes=01000", 2000, 0, 0, false, ErrMalformedRange},
		{"suffix_zero", "bytes=-0", 2000, 0, 0, false, ErrMalformedRange},
		{"empty_suffix", "bytes=-", 2000, 0, 0, false, ErrMalformedRange},
		{"negative_start", "bytes=-5-10", 2000, 0, 0, false, ErrMalformedRange},
		// Unsatisfiable.
		{"out_of_bounds", "bytes=5000-6000", 2000, 0, 0, false, ErrUnsatisfiableRange},
		{"end_before_start", "bytes=10-5", 2000, 0, 0, false, ErrUnsatisfiableRange},
		{"start_at_size", "bytes=2000-2001", 2000, 0, 0, false, ErrUnsatisfiableRange},
		{"zero_size_with_range", "bytes=0-0", 0, 0, 0, false, ErrUnsatisfiableRange},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rng, has, err := ParseRange(c.header, c.size)
			if err != c.wantErr {
				t.Errorf("ParseRange(%q, %d) err = %v, want %v", c.header, c.size, err, c.wantErr)
				return
			}
			if err != nil {
				return
			}
			if has != c.wantHas {
				t.Errorf("ParseRange(%q, %d) has = %v, want %v", c.header, c.size, has, c.wantHas)
			}
			if has {
				if rng.Start != c.wantStart || rng.End != c.wantEnd {
					t.Errorf("ParseRange(%q, %d) = %d-%d, want %d-%d",
						c.header, c.size, rng.Start, rng.End, c.wantStart, c.wantEnd)
				}
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Tests for ContentRangeValue, UnsatisfiableContentRange, DispositionHeader,
// TokenHash, mimeToExt, toUpper (Task 2-C).
// ----------------------------------------------------------------------------

// TestContentRangeValue verifies the Content-Range header value formatting:
// "bytes N-M/total". Pure function — table-driven.
func TestContentRangeValue(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		r     Range
		total int64
		want  string
	}{
		{"first_1k_of_2k", Range{Start: 0, End: 1023}, 2048, "bytes 0-1023/2048"},
		{"middle_512_of_2k", Range{Start: 512, End: 1023}, 2048, "bytes 512-1023/2048"},
		{"single_byte", Range{Start: 0, End: 0}, 1, "bytes 0-0/1"},
		{"full_file_as_range", Range{Start: 0, End: 999}, 1000, "bytes 0-999/1000"},
		{"mid_range_large_total", Range{Start: 1048576, End: 2097151}, 10485760, "bytes 1048576-2097151/10485760"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ContentRangeValue(c.r, c.total)
			if got != c.want {
				t.Errorf("ContentRangeValue(%+v, %d) = %q, want %q", c.r, c.total, got, c.want)
			}
		})
	}
}

// TestUnsatisfiableContentRange verifies the 416 Content-Range formatting:
// "bytes */total". Pure function — table-driven.
func TestUnsatisfiableContentRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		total int64
		want  string
	}{
		{2048, "bytes */2048"},
		{1, "bytes */1"},
		{0, "bytes */0"},
		{9223372036854775807, "bytes */9223372036854775807"}, // max int64
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("total_%d", c.total), func(t *testing.T) {
			got := UnsatisfiableContentRange(c.total)
			if got != c.want {
				t.Errorf("UnsatisfiableContentRange(%d) = %q, want %q", c.total, got, c.want)
			}
		})
	}
}

// TestDispositionHeader verifies the Content-Disposition header construction
// for both ASCII and non-ASCII filenames, in both attachment and inline modes.
// Uses individual subtests because the expected output varies structurally.
func TestDispositionHeader(t *testing.T) {
	t.Parallel()
	t.Run("attachment_ascii", func(t *testing.T) {
		got := DispositionHeader("attachment", "movie.mp4")
		want := `attachment; filename="movie.mp4"; filename*=UTF-8''movie.mp4`
		if got != want {
			t.Errorf("DispositionHeader(attachment, movie.mp4) = %q, want %q", got, want)
		}
	})
	t.Run("inline_ascii", func(t *testing.T) {
		got := DispositionHeader("inline", "file.txt")
		want := `inline; filename="file.txt"; filename*=UTF-8''file.txt`
		if got != want {
			t.Errorf("DispositionHeader(inline, file.txt) = %q, want %q", got, want)
		}
	})
	t.Run("attachment_non_ascii", func(t *testing.T) {
		// Non-ASCII filename: ASCII fallback replaces each non-ASCII char with
		// '_'; the extended form carries the UTF-8 percent-encoded version.
		got := DispositionHeader("attachment", "фильм.mp4")
		prefix := `attachment; filename="`
		sep := `"; filename*=UTF-8''`
		if !strings.HasPrefix(got, prefix) {
			t.Fatalf("expected prefix %q, got %q", prefix, got)
		}
		idx := strings.Index(got, sep)
		if idx < 0 {
			t.Fatalf("missing separator %q in %q", sep, got)
		}
		ascii := got[len(prefix):idx]
		ext := got[idx+len(sep):]
		// ASCII fallback must be all-ASCII.
		for _, r := range ascii {
			if r >= 0x80 {
				t.Errorf("ASCII fallback contains non-ASCII byte 0x%X in %q", r, ascii)
				break
			}
		}
		// ASCII fallback: Cyrillic chars (5 of them) → 5 underscores + ".mp4".
		wantASCII := "_____.mp4"
		if ascii != wantASCII {
			t.Errorf("ASCII fallback = %q, want %q", ascii, wantASCII)
		}
		// Extended form must be non-empty and contain percent-encoded bytes.
		if ext == "" {
			t.Errorf("extended form should be non-empty")
		}
		if !strings.Contains(ext, "%") {
			t.Errorf("extended form for non-ASCII filename should contain percent-encoded bytes, got %q", ext)
		}
		// Extended form should NOT contain any raw non-ASCII bytes (everything
		// outside the attr-char set is %XX-encoded).
		for _, r := range ext {
			if r >= 0x80 {
				t.Errorf("extended form should be ASCII-only (percent-encoded), found 0x%X in %q", r, ext)
				break
			}
		}
	})
	t.Run("empty_filename", func(t *testing.T) {
		got := DispositionHeader("attachment", "")
		want := `attachment; filename=""; filename*=UTF-8''`
		if got != want {
			t.Errorf("DispositionHeader(attachment, \"\") = %q, want %q", got, want)
		}
	})
	t.Run("spaces_in_filename", func(t *testing.T) {
		// Spaces are preserved inside the quoted ASCII filename and
		// percent-encoded as %20 in the extended form.
		got := DispositionHeader("attachment", "a b c.txt")
		want := `attachment; filename="a b c.txt"; filename*=UTF-8''a%20b%20c.txt`
		if got != want {
			t.Errorf("DispositionHeader(attachment, \"a b c.txt\") = %q, want %q", got, want)
		}
	})
	t.Run("disposition_preserved", func(t *testing.T) {
		// The disposition value must be preserved exactly (no normalization).
		for _, disp := range []string{"attachment", "inline"} {
			got := DispositionHeader(disp, "x.txt")
			if !strings.HasPrefix(got, disp+`; filename="`) {
				t.Errorf("disposition %q not preserved as prefix in %q", disp, got)
			}
		}
	})
}

// TestTokenHash verifies TokenHash returns the first 16 hex chars of
// sha256(token), is deterministic, lowercase, and always 16 chars.
func TestTokenHash(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		token string
		want  string
	}{
		// sha256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
		// first 8 bytes (16 hex chars) = "ba7816bf8f01cfea"
		{"abc", "abc", "ba7816bf8f01cfea"},
		// sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
		// first 16 hex chars = "e3b0c44298fc1c14"
		{"empty", "", "e3b0c44298fc1c14"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := TokenHash(c.token)
			if got != c.want {
				t.Errorf("TokenHash(%q) = %q, want %q", c.token, got, c.want)
			}
			// Always exactly 16 hex chars (8 bytes × 2).
			if len(got) != 16 {
				t.Errorf("TokenHash(%q) length = %d, want 16", c.token, len(got))
			}
			// Must be lowercase hex.
			for _, r := range got {
				if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
					t.Errorf("TokenHash(%q) should be lowercase hex, got %q", c.token, got)
					break
				}
			}
		})
	}
	// Determinism: same input must always produce the same output.
	a := TokenHash("deterministic-input")
	b := TokenHash("deterministic-input")
	if a != b {
		t.Errorf("TokenHash should be deterministic: got %q then %q for same input", a, b)
	}
	// Different inputs should (overwhelmingly likely) produce different hashes.
	x := TokenHash("input-one")
	y := TokenHash("input-two")
	if x == y {
		t.Errorf("distinct inputs produced same hash (astronomically unlikely): %q", x)
	}
	// Long token should still yield exactly 16 chars (no length-dependence).
	long := TokenHash(strings.Repeat("a", 10_000))
	if len(long) != 16 {
		t.Errorf("TokenHash(10k chars) length = %d, want 16", len(long))
	}
}

// TestMimeToExt verifies the MIME-to-extension mapping, including the subtype
// fallback for unknown MIME types and the case/whitespace normalization.
// mimeToExt is unexported — white-box test.
func TestMimeToExt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		mime string
		want string
	}{
		// Known mappings from the switch.
		{"video_mp4", "video/mp4", "mp4"},
		{"video_webm", "video/webm", "webm"},
		{"video_mkv", "video/x-matroska", "mkv"},
		{"audio_mpeg", "audio/mpeg", "mp3"},
		{"audio_mp3", "audio/mp3", "mp3"},
		{"audio_ogg", "audio/ogg", "ogg"},
		{"audio_mp4_m4a", "audio/mp4", "m4a"},
		{"image_jpeg", "image/jpeg", "jpg"},
		{"image_png", "image/png", "png"},
		{"image_webp", "image/webp", "webp"},
		{"image_gif", "image/gif", "gif"},
		{"pdf", "application/pdf", "pdf"},
		{"zip", "application/zip", "zip"},
		// Subtype fallback: all-alphanumeric, length ≤ 6.
		{"text_plain_fallback", "text/plain", "plain"},
		{"custom_subtype_alnum", "foo/abc123", "abc123"},
		{"custom_subtype_6chars", "foo/abcdef", "abcdef"},
		// Unknown / unsupported → empty string.
		{"octet_stream_has_dash", "application/octet-stream", ""},
		{"empty_mime", "", ""},
		{"with_params_has_semicolon", "video/mp4; codecs=avc1", ""},
		{"subtype_too_long", "foo/abcdefg", ""}, // 7 chars > 6
		{"trailing_slash_empty_sub", "foo/", ""},
		{"no_slash", "foobar", ""},
		// Case-insensitive + whitespace-trimmed (function lowercases + trims).
		{"uppercase", "VIDEO/MP4", "mp4"},
		{"mixed_case", "Image/JPEG", "jpg"},
		{"whitespace_trimmed", "  video/mp4  ", "mp4"},
		{"whitespace_and_case", "\t AUDIO/MP3 \n", "mp3"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mimeToExt(c.mime)
			if got != c.want {
				t.Errorf("mimeToExt(%q) = %q, want %q", c.mime, got, c.want)
			}
		})
	}
}

// TestToUpper verifies ASCII uppercase conversion for lowercase letters and
// pass-through for non-letters (uppercase, digits, punctuation, non-ASCII).
// toUpper is unexported — white-box test.
func TestToUpper(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   rune
		want rune
	}{
		{"a_to_A", 'a', 'A'},
		{"z_to_Z", 'z', 'Z'},
		{"m_to_M", 'm', 'M'},
		{"A_unchanged", 'A', 'A'},
		{"Z_unchanged", 'Z', 'Z'},
		{"digit_0", '0', '0'},
		{"digit_9", '9', '9'},
		{"exclamation", '!', '!'},
		{"space", ' ', ' '},
		{"null_byte", 0, 0},
		{"tilde", '~', '~'},
		{"backslash", '\\', '\\'},
		{"non_ascii_passthrough", 'é', 'é'}, // pass-through for non-ASCII runes
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := toUpper(c.in)
			if got != c.want {
				t.Errorf("toUpper(%q (0x%X)) = %q (0x%X), want %q (0x%X)",
					c.in, c.in, got, got, c.want, c.want)
			}
		})
	}
	// Verify the full lowercase alphabet maps to uppercase.
	for r := rune('a'); r <= 'z'; r++ {
		got := toUpper(r)
		want := r - 32
		if got != want {
			t.Errorf("toUpper(%q) = %q, want %q", r, got, want)
		}
	}
}

// TestFormatBytes covers the binary-unit ladder (B → KiB → MiB → GiB → TiB →
// PiB → EiB) with 2-decimal precision, plus edge cases (zero, negative,
// exactly-on-boundary values, and the largest int64 values where the EiB
// tier is the only one that fits). 5-F added the PiB / EiB tiers so very
// large values (e.g. the 1<<62 case previously rendered as "4194304.00 TiB"
// by the http package's old copy) now format with a sensible unit.
func TestFormatBytes(t *testing.T) {
	t.Parallel()
	const (
		KiB = int64(1024)
		MiB = KiB * 1024
		GiB = MiB * 1024
		TiB = GiB * 1024
		PiB = TiB * 1024
		EiB = PiB * 1024
	)
	tests := []struct {
		name string
		n    int64
		want string
	}{
		{"zero", 0, "0 B"},
		{"one byte", 1, "1 B"},
		{"1023 bytes (boundary)", 1023, "1023 B"},
		{"1 KiB exactly", KiB, "1.00 KiB"},
		{"1.5 KiB", 1536, "1.50 KiB"},
		{"1 KiB below MiB (rounds cleanly)", MiB - 1024, "1023.00 KiB"},
		{"1 MiB exactly", MiB, "1.00 MiB"},
		{"3.5 MiB", 7 * MiB / 2, "3.50 MiB"},
		{"1 GiB exactly", GiB, "1.00 GiB"},
		{"2.25 GiB", 9 * GiB / 4, "2.25 GiB"},
		{"1 TiB exactly", TiB, "1.00 TiB"},
		{"1 byte above 1 TiB", 1 + TiB, "1.00 TiB"},
		{"1 PiB exactly", PiB, "1.00 PiB"},
		{"5 PiB", 5 * PiB, "5.00 PiB"},
		{"1 EiB exactly", EiB, "1.00 EiB"},
		{"1<<62 (4 EiB)", 1 << 62, "4.00 EiB"},
		{"max int64 (~8 EiB)", 1<<63 - 1, "8.00 EiB"},
		{"negative (fallback to %d B)", -1, "-1 B"},
		{"negative large", -1024, "-1024 B"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatBytes(tt.n)
			if got != tt.want {
				t.Errorf("FormatBytes(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}
