// Package util provides pure helpers: Crockford base32 tokens, HTTP Range
// parsing, Content-Disposition escaping, byte formatting, and token hashing.
// (The gogram-coupled message extractors were removed in Phase 3: the
// transport seam's FileHandle/MediaInfo types carry name/mime/size/DC now.)
package tgutil

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// Crockford base32 alphabet (excludes O/I/L/U to avoid visual confusion).
const crockford = "0123456789abcdefghjkmnpqrstvwxyz"

var (
	decodeMap     [256]byte
	decodeMapOnce sync.Once
)

func initDecodeMap() {
	for i := range decodeMap {
		decodeMap[i] = 0xFF
	}
	for i, c := range []byte(crockford) {
		decodeMap[c] = byte(i)
		decodeMap[byte(toUpper(rune(c)))] = byte(i) //nosec G115 // c is from a 32-char alphabet; i < 32, no overflow possible
	}
	// Crockford normalization: O→0, I/L→1.
	decodeMap['O'] = 0
	decodeMap['o'] = 0
	decodeMap['I'] = 1
	decodeMap['i'] = 1
	decodeMap['L'] = 1
	decodeMap['l'] = 1
}

func toUpper(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - 32
	}
	return r
}

// EncodeBase32 encodes data using Crockford base32. Caller must supply
// random input (e.g. crypto/rand) for the result to be unpredictable.
func EncodeBase32(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	out := make([]byte, 0, (len(data)*8+4)/5)
	var buf uint64
	var bits uint
	for _, b := range data {
		buf = (buf << 8) | uint64(b)
		bits += 8
		for bits >= 5 {
			bits -= 5
			out = append(out, crockford[(buf>>bits)&0x1F])
		}
	}
	if bits > 0 {
		out = append(out, crockford[(buf<<(5-bits))&0x1F])
	}
	return string(out)
}

// DecodeBase32 decodes a Crockford base32 string. Rejects invalid chars
// and non-zero trailing bits so Encode→Decode is a strict round-trip.
func DecodeBase32(s string) ([]byte, error) {
	decodeMapOnce.Do(initDecodeMap)
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var out []byte
	var buf uint64
	var bits uint
	for i := 0; i < len(s); i++ {
		c := s[i]
		v := decodeMap[c]
		if v == 0xFF {
			return nil, fmt.Errorf("invalid base32 character %q at index %d", c, i)
		}
		buf = (buf << 5) | uint64(v)
		bits += 5
		if bits >= 8 {
			bits -= 8
			out = append(out, byte(buf>>bits)) //nosec G115 // bits is 0-7, buf masked to 5-bit chunks, no overflow
		}
	}
	// Reject non-zero trailing bits so two strings can't decode to the same bytes.
	if bits > 0 {
		mask := uint64(1)<<bits - 1
		if buf&mask != 0 {
			return nil, fmt.Errorf("non-zero trailing bits in base32 input (got %d bits, value %b)", bits, buf&mask)
		}
	}
	return out, nil
}

// NormalizeBase32 returns the canonical lowercase form of a token. Tokens
// are case-insensitive on lookup; the DB stores the lowercase form.
func NormalizeBase32(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	// Apply Crockford normalization: O→0, I→1, L→1.
	var b strings.Builder
	b.Grow(len(s))
	for _, c := range s {
		switch c {
		case 'o':
			b.WriteByte('0')
		case 'i', 'l':
			b.WriteByte('1')
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}

// mimeToExt maps a MIME type to a canonical file extension ("" when
// unknown). Pure helper kept from the gogram era — the ingest layer
// synthesizes fallback names from MediaInfo now, but tests pin its behavior.
func mimeToExt(mime string) string {
	mime = strings.ToLower(strings.TrimSpace(mime))
	switch mime {
	case "video/mp4":
		return "mp4"
	case "video/webm":
		return "webm"
	case "video/x-matroska":
		return "mkv"
	case "audio/mpeg", "audio/mp3":
		return "mp3"
	case "audio/ogg":
		return "ogg"
	case "audio/mp4":
		return "m4a"
	case "image/jpeg":
		return "jpg"
	case "image/png":
		return "png"
	case "image/webp":
		return "webp"
	case "image/gif":
		return "gif"
	case "application/pdf":
		return "pdf"
	case "application/zip":
		return "zip"
	}
	if i := strings.IndexByte(mime, '/'); i > 0 {
		// Use the subtype as the extension if it's all alphanumeric.
		sub := mime[i+1:]
		ok := true
		for _, r := range sub {
			if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') {
				ok = false
				break
			}
		}
		if ok && len(sub) <= 6 {
			return sub
		}
	}
	return ""
}

// Range describes a byte window into a file. Start and End are inclusive.
type Range struct {
	Start int64
	End   int64
}

// ParseRange parses an HTTP Range header for a file of the given size.
// Returns (Range, true, nil) for a satisfiable range; (Range{}, false, nil)
// when no Range header is present; or an error for malformed/unsatisfiable.
var (
	ErrMalformedRange     = fmt.Errorf("malformed Range header")
	ErrUnsatisfiableRange = fmt.Errorf("range not satisfiable")
)

func ParseRange(header string, size int64) (Range, bool, error) {
	if header == "" {
		return Range{}, false, nil
	}
	if size <= 0 {
		return Range{}, false, ErrUnsatisfiableRange
	}
	const prefix = "bytes="
	if !strings.HasPrefix(header, prefix) {
		return Range{}, false, ErrMalformedRange
	}
	spec := strings.TrimSpace(header[len(prefix):])
	if spec == "" {
		return Range{}, false, ErrMalformedRange
	}
	// Coalesce multi-range requests to the first range (RFC 7233 permits
	// ignoring subsequent ranges).
	if i := strings.IndexByte(spec, ','); i >= 0 {
		spec = strings.TrimSpace(spec[:i])
		if spec == "" {
			return Range{}, false, ErrMalformedRange
		}
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return Range{}, false, ErrMalformedRange
	}
	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])

	if startStr == "" {
		// Suffix range: bytes=-N (last N bytes).
		if endStr == "" {
			return Range{}, false, ErrMalformedRange
		}
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return Range{}, false, ErrMalformedRange
		}
		if n > size {
			n = size
		}
		start := size - n
		return Range{Start: start, End: size - 1}, true, nil
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return Range{}, false, ErrMalformedRange
	}
	if start >= size {
		return Range{}, false, ErrUnsatisfiableRange
	}
	var end int64
	if endStr == "" {
		end = size - 1
	} else {
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil || end < start {
			// last-byte-pos < first-byte-pos is unsatisfiable (RFC 7233).
			return Range{}, false, ErrUnsatisfiableRange
		}
		if end >= size {
			end = size - 1
		}
	}
	return Range{Start: start, End: end}, true, nil
}

// ContentRangeValue formats a Content-Range header value: "bytes N-M/total".
func ContentRangeValue(r Range, total int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", r.Start, r.End, total)
}

// UnsatisfiableContentRange returns "bytes */{size}" for 416 responses.
func UnsatisfiableContentRange(total int64) string {
	return fmt.Sprintf("bytes */%d", total)
}

// SanitizeForDisposition escapes a filename for Content-Disposition (RFC 6266).
// Returns an ASCII fallback and an RFC 5987 percent-encoded filename* form.
func SanitizeForDisposition(name string) (ascii, extended string) {
	// ASCII fallback: replace non-printable or special chars with underscore.
	var ab strings.Builder
	ab.Grow(len(name))
	for _, r := range name {
		if r >= 0x20 && r < 0x7F && r != '"' && r != '\\' {
			ab.WriteRune(r)
		} else {
			ab.WriteByte('_')
		}
	}
	// Extended form: percent-encode per RFC 5987.
	var eb strings.Builder
	eb.Grow(len(name) + 16)
	eb.WriteString("UTF-8''")
	for _, b := range []byte(name) {
		// RFC 5987 attr-char; everything else is %xx-escaped.
		if isAttrChar(b) {
			eb.WriteByte(b)
		} else {
			eb.WriteByte('%')
			eb.WriteString(hex.EncodeToString([]byte{b}))
		}
	}
	return ab.String(), eb.String()
}

func isAttrChar(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z',
		b >= 'A' && b <= 'Z',
		b >= '0' && b <= '9':
		return true
	}
	switch b {
	case '!', '#', '$', '&', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}

// DispositionHeader returns the Content-Disposition header value with both
// ASCII and UTF-8 (filename*) forms.
func DispositionHeader(disposition, filename string) string {
	ascii, extended := SanitizeForDisposition(filename)
	return fmt.Sprintf(`%s; filename="%s"; filename*=%s`, disposition, ascii, extended)
}

// QueryDisposition parses the ?disposition= query param. Anything other than
// "inline" (case-insensitive) defaults to "attachment".
func QueryDisposition(r *http.Request) string {
	v := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("disposition")))
	if v == "inline" {
		return "inline"
	}
	return "attachment"
}

// FormatBytes returns a human-readable binary-unit size (KiB/MiB/.../EiB),
// 2 decimal places, or raw bytes for values < 1024.
func FormatBytes(n int64) string {
	const (
		KiB = 1 << 10
		MiB = KiB * 1024
		GiB = MiB * 1024
		TiB = GiB * 1024
		PiB = TiB * 1024
		EiB = PiB * 1024
	)
	switch {
	case n >= EiB:
		return fmt.Sprintf("%.2f EiB", float64(n)/EiB)
	case n >= PiB:
		return fmt.Sprintf("%.2f PiB", float64(n)/PiB)
	case n >= TiB:
		return fmt.Sprintf("%.2f TiB", float64(n)/TiB)
	case n >= GiB:
		return fmt.Sprintf("%.2f GiB", float64(n)/GiB)
	case n >= MiB:
		return fmt.Sprintf("%.2f MiB", float64(n)/MiB)
	case n >= KiB:
		return fmt.Sprintf("%.2f KiB", float64(n)/KiB)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// TokenHash returns the first 16 hex chars of sha256(token), for log
// correlation without leaking the URL credential.
func TokenHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:8]) // first 16 hex chars (64 bits)
}
