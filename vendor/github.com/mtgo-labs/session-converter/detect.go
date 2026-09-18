package tgconv

import (
	"encoding/base64"
	"strings"
)

// tryB64URL tries to decode a URL-safe base64 string (with or without padding).
func tryB64URL(s string) ([]byte, error) {
	if b, err := base64.URLEncoding.DecodeString(padBase64(s)); err == nil {
		return b, nil
	}
	return base64.RawURLEncoding.DecodeString(s)
}

// tryB64Std tries to decode a standard base64 string.
func tryB64Std(s string) ([]byte, error) {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.RawStdEncoding.DecodeString(s)
}

// isStdBase64 reports whether the string uses standard base64 charset
// (contains + or /) vs URL-safe (- or _).
func isStdBase64(s string) bool {
	return strings.ContainsAny(s, "+/")
}

// looksLikeJSON reports whether the data starts with '{' or '['.
func looksLikeJSON(data []byte) bool {
	for _, b := range data {
		if b == ' ' || b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		return b == '{' || b == '['
	}
	return false
}
