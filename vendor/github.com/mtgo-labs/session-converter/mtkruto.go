package tgconv

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// EncodeMTKruto encodes a session in MTKruto string format.
//
// base64url(rleEncode(TL_string(dc) + TL_bytes(authkey) + int32(apiId) LE + byte(isBot) + int64(userId) LE))
//
// The dc string is formatted as "<dcID>" or "<dcID>-test" for test mode.
func EncodeMTKruto(s *Session) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}

	dc := strconv.Itoa(s.DCID)
	if s.TestMode {
		dc += "-test"
	}

	buf := make([]byte, 0, 512)
	buf = append(buf, tlBytes([]byte(dc))...)
	buf = append(buf, tlBytes(s.AuthKey)...)

	apiIDBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(apiIDBuf, uint32(s.AppID))
	buf = append(buf, apiIDBuf...)

	if s.IsBot {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	uidBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(uidBuf, uint64(s.UserID))
	buf = append(buf, uidBuf...)

	encoded := rleEncode(buf)
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

// DecodeMTKruto decodes an MTKruto session string.
func DecodeMTKruto(str string) (*Session, error) {
	raw, err := base64.RawURLEncoding.DecodeString(str)
	if err != nil {
		raw, err = base64.URLEncoding.DecodeString(padBase64(str))
		if err != nil {
			return nil, fmt.Errorf("mtkruto: base64 decode: %w", err)
		}
	}

	data := rleDecode(raw)

	off := 0

	// DC string.
	dcBytes, n, err := readTLBytes(data, off)
	if err != nil {
		return nil, fmt.Errorf("mtkruto: reading dc: %w", err)
	}
	off = n
	dcStr := string(dcBytes)

	// Auth key.
	authKeyBytes, n, err := readTLBytes(data, off)
	if err != nil {
		return nil, fmt.Errorf("mtkruto: reading auth key: %w", err)
	}
	off = n

	if len(authKeyBytes) != 256 {
		return nil, fmt.Errorf("mtkruto: auth_key must be 256 bytes, got %d", len(authKeyBytes))
	}
	authKey := make([]byte, 256)
	copy(authKey, authKeyBytes)

	var session Session
	session.AuthKey = authKey

	// Parse DC string: "2" or "2-test".
	testMode := false
	dcNum := dcStr
	if idx := strings.Index(dcStr, "-test"); idx >= 0 {
		testMode = true
		dcNum = dcStr[:idx]
	}
	// Also handle "-Test" or negative-style.
	dcNum = strings.TrimSuffix(dcNum, "-Test")
	dcID, err := strconv.Atoi(dcNum)
	if err != nil {
		return nil, fmt.Errorf("mtkruto: invalid DC string %q: %w", dcStr, err)
	}
	session.DCID = dcID
	session.TestMode = testMode

	// apiId (4 bytes LE) — may be absent in older sessions.
	if off+4 <= len(data) {
		session.AppID = int32(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
	}

	// isBot (1 byte).
	if off < len(data) {
		session.IsBot = data[off] != 0
		off++
	}

	// userId (8 bytes LE).
	if off+8 <= len(data) {
		session.UserID = int64(binary.LittleEndian.Uint64(data[off : off+8]))
	}

	session.FillDefaults()

	return &session, nil
}
