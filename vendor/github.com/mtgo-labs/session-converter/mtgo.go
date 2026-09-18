package tgconv

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MTGO session string format.
//
// Wire format:
//
//	MTGO<version>.<base64url(payload)>
//
// The version 1 payload is a compact binary sequence of varint-prefixed
// fields followed by the fixed-size authorization key:
//
//	version  u8     = 1
//	flags    u8     bit0 = test mode, bit1 = is bot; other bits must be 0
//	user_id  varint Telegram user or bot ID (> 0)
//	phone    varint byte length + UTF-8 bytes (0..64; empty for bots)
//	auth_key 256 bytes
//	api_id   varint (> 0)
//	api_hash varint byte length + 16 raw bytes (32 hex chars on the API)
//	dc_id    varint (1..255)
//
// Unknown flag bits and unknown versions fail closed so future revisions
// (e.g. MTGO2) can add fields without ambiguity.
const (
	mtgoPrefix      = "MTGO"
	mtgoVersion     = 1
	mtgoFlagTest    = 1 << 0
	mtgoFlagBot     = 1 << 1
	mtgoKnownFlags  = mtgoFlagTest | mtgoFlagBot
	mtgoMaxPhone    = 64
	mtgoMaxPayload  = 1024
	mtgoHashBytes   = 16
	mtgoHashHexSize = 32
	mtgoMaxInt53    = 1<<53 - 1
)

// EncodeSession encodes a session in the native mtgo MTGO1 string format.
//
// Unlike the legacy formats, the MTGO1 payload also carries the API hash and,
// for user accounts, the phone number, so the result is fully self-contained.
// APIHash must be the 32-character hex API hash from my.telegram.org.
func EncodeSession(s *Session) (string, error) {
	if s == nil {
		return "", fmt.Errorf("mtgo: nil session")
	}
	if len(s.AuthKey) != 256 {
		return "", fmt.Errorf("mtgo: auth_key must be 256 bytes, got %d", len(s.AuthKey))
	}
	if s.UserID <= 0 || s.UserID > mtgoMaxInt53 {
		return "", fmt.Errorf("mtgo: invalid user_id %d", s.UserID)
	}
	if s.DCID < 1 || s.DCID > 255 {
		return "", fmt.Errorf("mtgo: invalid dc_id %d", s.DCID)
	}
	if s.AppID <= 0 || s.AppID > math.MaxInt32 {
		return "", fmt.Errorf("mtgo: invalid api_id %d", s.AppID)
	}
	if len(s.APIHash) != mtgoHashHexSize {
		return "", fmt.Errorf("mtgo: api_hash must be %d hex characters, got %d", mtgoHashHexSize, len(s.APIHash))
	}
	apiHash, err := hex.DecodeString(s.APIHash)
	if err != nil {
		return "", fmt.Errorf("mtgo: invalid api_hash: %w", err)
	}
	if len(s.PhoneNumber) > mtgoMaxPhone {
		return "", fmt.Errorf("mtgo: phone number exceeds %d bytes", mtgoMaxPhone)
	}
	if !utf8.ValidString(s.PhoneNumber) {
		return "", fmt.Errorf("mtgo: phone number is not valid UTF-8")
	}

	flags := byte(0)
	if s.TestMode {
		flags |= mtgoFlagTest
	}
	if s.IsBot {
		flags |= mtgoFlagBot
	}

	buf := make([]byte, 0, mtgoMaxPayload)
	buf = append(buf, mtgoVersion, flags)
	buf = binary.AppendUvarint(buf, uint64(s.UserID))
	buf = binary.AppendUvarint(buf, uint64(len(s.PhoneNumber)))
	buf = append(buf, s.PhoneNumber...)
	buf = append(buf, s.AuthKey...)
	buf = binary.AppendUvarint(buf, uint64(s.AppID))
	buf = binary.AppendUvarint(buf, uint64(len(apiHash)))
	buf = append(buf, apiHash...)
	buf = binary.AppendUvarint(buf, uint64(s.DCID))
	if len(buf) > mtgoMaxPayload {
		return "", fmt.Errorf("mtgo: payload exceeds %d bytes", mtgoMaxPayload)
	}
	return mtgoPrefix + strconv.Itoa(mtgoVersion) + "." + base64.RawURLEncoding.EncodeToString(buf), nil
}

// DecodeSession decodes a native mtgo MTGO1 session string. The MTGO prefix
// and version are validated; unsupported versions fail closed.
func DecodeSession(str string) (*Session, error) {
	version, payload, ok := splitMTGOPrefix(str)
	if !ok {
		return nil, fmt.Errorf("mtgo: invalid session string: missing %s<version>. prefix", mtgoPrefix)
	}
	if version != mtgoVersion {
		return nil, fmt.Errorf("mtgo: unsupported session version %s%d (supported: %s%d)",
			mtgoPrefix, version, mtgoPrefix, mtgoVersion)
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("mtgo: base64 decode: %w", err)
	}
	if len(data) == 0 || len(data) > mtgoMaxPayload {
		return nil, fmt.Errorf("mtgo: invalid payload size %d", len(data))
	}

	d := mtgoDecoder{data: data}
	if v, err := d.byte(); err != nil || v != mtgoVersion {
		return nil, fmt.Errorf("mtgo: invalid version byte")
	}
	flags, err := d.byte()
	if err != nil {
		return nil, fmt.Errorf("mtgo: truncated payload: %w", err)
	}
	if flags&^mtgoKnownFlags != 0 {
		return nil, fmt.Errorf("mtgo: unknown flags 0x%02x", flags&^mtgoKnownFlags)
	}
	userID, err := d.uvarint()
	if err != nil {
		return nil, err
	}
	if userID == 0 || userID > mtgoMaxInt53 {
		return nil, fmt.Errorf("mtgo: invalid user_id %d", userID)
	}
	phone, err := d.bytes()
	if err != nil {
		return nil, err
	}
	if len(phone) > mtgoMaxPhone || !utf8.Valid(phone) {
		return nil, fmt.Errorf("mtgo: invalid phone number")
	}
	authKey, err := d.take(256)
	if err != nil {
		return nil, err
	}
	apiID, err := d.uvarint()
	if err != nil {
		return nil, err
	}
	if apiID == 0 || apiID > math.MaxInt32 {
		return nil, fmt.Errorf("mtgo: invalid api_id %d", apiID)
	}
	apiHash, err := d.bytes()
	if err != nil {
		return nil, err
	}
	if len(apiHash) != mtgoHashBytes {
		return nil, fmt.Errorf("mtgo: invalid api_hash length %d", len(apiHash))
	}
	dcID, err := d.uvarint()
	if err != nil {
		return nil, err
	}
	if dcID < 1 || dcID > 255 {
		return nil, fmt.Errorf("mtgo: invalid dc_id %d", dcID)
	}
	if d.off != len(data) {
		return nil, fmt.Errorf("mtgo: trailing data after payload (%d bytes)", len(data)-d.off)
	}

	s := &Session{
		DCID:        int(dcID),
		AuthKey:     authKey,
		AppID:       int32(apiID),
		TestMode:    flags&mtgoFlagTest != 0,
		UserID:      int64(userID),
		IsBot:       flags&mtgoFlagBot != 0,
		APIHash:     hex.EncodeToString(apiHash),
		PhoneNumber: string(phone),
	}
	s.FillDefaults()
	return s, nil
}

// splitMTGOPrefix parses the "MTGO<digits>." header and returns the version
// and the remaining payload. It reports ok=false unless the string starts
// with "MTGO" followed by a canonical (no leading zeros) decimal version
// number and a dot.
func splitMTGOPrefix(s string) (version int, payload string, ok bool) {
	if !strings.HasPrefix(s, mtgoPrefix) {
		return 0, "", false
	}
	rest := s[len(mtgoPrefix):]
	dot := strings.IndexByte(rest, '.')
	if dot <= 0 {
		return 0, "", false
	}
	digits := rest[:dot]
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, "", false
		}
	}
	version, err := strconv.Atoi(digits)
	if err != nil || strconv.Itoa(version) != digits {
		return 0, "", false
	}
	return version, rest[dot+1:], true
}

type mtgoDecoder struct {
	data []byte
	off  int
}

func (d *mtgoDecoder) byte() (byte, error) {
	if d.off >= len(d.data) {
		return 0, fmt.Errorf("mtgo: truncated payload")
	}
	v := d.data[d.off]
	d.off++
	return v, nil
}

func (d *mtgoDecoder) uvarint() (uint64, error) {
	v, n := binary.Uvarint(d.data[d.off:])
	if n <= 0 {
		return 0, fmt.Errorf("mtgo: invalid varint")
	}
	d.off += n
	return v, nil
}

func (d *mtgoDecoder) take(n int) ([]byte, error) {
	if n < 0 || len(d.data)-d.off < n {
		return nil, fmt.Errorf("mtgo: truncated payload")
	}
	v := d.data[d.off : d.off+n]
	d.off += n
	return v, nil
}

// bytes reads a varint length-prefixed byte string.
func (d *mtgoDecoder) bytes() ([]byte, error) {
	n, err := d.uvarint()
	if err != nil {
		return nil, err
	}
	if n > mtgoMaxPayload {
		return nil, fmt.Errorf("mtgo: field length %d exceeds maximum", n)
	}
	return d.take(int(n))
}
