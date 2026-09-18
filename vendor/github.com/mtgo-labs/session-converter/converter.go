package tgconv

import "fmt"

// Decode auto-detects the session string format and decodes it.
func Decode(str string) (*Session, Format, error) {
	if str == "" {
		return nil, "", fmt.Errorf("empty session string")
	}

	// Try detected format first.
	if f := DetectFormat(str); f != "" {
		if s, err := decodeByFormat(str, f); err == nil {
			return s, f, nil
		}
	}

	// Brute-force: try all formats.
	for _, f := range AllFormats {
		if s, err := decodeByFormat(str, f); err == nil {
			return s, f, nil
		}
	}

	return nil, "", fmt.Errorf("unable to detect session format")
}

// DecodeFormat decodes a session string in the specified format.
func DecodeFormat(str string, format Format) (*Session, error) {
	return decodeByFormat(str, format)
}

// Encode encodes a session into the specified format.
func Encode(s *Session, format Format) (string, error) {
	switch format {
	case FormatTelethon:
		return EncodeTelethon(s)
	case FormatPyrogram:
		return EncodePyrogram(s)
	case FormatGramJS:
		return EncodeGramJS(s)
	case FormatMtcute:
		return EncodeMtcute(s)
	case FormatMTKruto:
		return EncodeMTKruto(s)
	case FormatGogram:
		return EncodeGogram(s)
	case FormatGotgproto:
		return EncodeGotgproto(s)
	case FormatMTGO:
		return EncodeSession(s)
	default:
		return "", fmt.Errorf("unsupported format: %s", format)
	}
}

// Convert decodes a session from any format and re-encodes it to the target.
func Convert(str string, to Format) (string, error) {
	s, _, err := Decode(str)
	if err != nil {
		return "", err
	}
	return Encode(s, to)
}

// ConvertFrom decodes from a known format and re-encodes.
func ConvertFrom(str string, from, to Format) (string, error) {
	s, err := decodeByFormat(str, from)
	if err != nil {
		return "", err
	}
	return Encode(s, to)
}

// DetectFormat attempts to identify the format without fully decoding.
func DetectFormat(str string) Format {
	if str == "" {
		return ""
	}

	// mtgo: "MTGO<version>." prefix. Check first — the header is unambiguous.
	if _, _, ok := splitMTGOPrefix(str); ok {
		return FormatMTGO
	}

	// gogram: prefix "1BvE" or "1BvX".
	if len(str) > 4 && (str[:4] == gogramPrefix || str[:4] == gogramPrefixLegacy) {
		return FormatGogram
	}

	// gotgproto: base64std JSON. Check early — JSON is unambiguous.
	if raw, err := tryB64Std(str); err == nil {
		if looksLikeJSON(raw) {
			return FormatGotgproto
		}
	}

	// Telethon or GramJS: start with "1".
	if len(str) > 1 && str[0] == '1' {
		// Telethon uses URL-safe base64 (contains - or _).
		// GramJS uses standard base64 (contains + or /).
		rest := str[1:]
		if isStdBase64(rest) {
			return FormatGramJS
		}
		return FormatTelethon
	}

	// Pyrogram: base64url, decodes to exactly 271 bytes.
	if payload, err := tryB64URL(str); err == nil {
		if len(payload) == 271 {
			return FormatPyrogram
		}
	}

	// mtcute: first decoded byte is 3 (version).
	if payload, err := tryB64URL(str); err == nil {
		if len(payload) >= 5 && payload[0] == mtcuteVersion {
			return FormatMtcute
		}
	}

	// MTKruto: RLE-encoded base64url. Last-resort heuristic.
	if payload, err := tryB64URL(str); err == nil {
		decoded := rleDecode(payload)
		if len(decoded) >= 4 {
			// Check if it starts with a valid TL string length.
			if _, _, err := readTLBytes(decoded, 0); err == nil {
				return FormatMTKruto
			}
		}
	}

	return ""
}

func decodeByFormat(str string, f Format) (*Session, error) {
	switch f {
	case FormatTelethon:
		return DecodeTelethon(str)
	case FormatPyrogram:
		return DecodePyrogram(str)
	case FormatGramJS:
		return DecodeGramJS(str)
	case FormatMtcute:
		return DecodeMtcute(str)
	case FormatMTKruto:
		return DecodeMTKruto(str)
	case FormatGogram:
		return DecodeGogram(str)
	case FormatGotgproto:
		return DecodeGotgproto(str)
	case FormatMTGO:
		return DecodeSession(str)
	default:
		return nil, fmt.Errorf("unsupported format: %s", f)
	}
}
