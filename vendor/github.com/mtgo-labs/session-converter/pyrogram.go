package tgconv

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

// EncodePyrogram encodes a session in Pyrogram string format.
//
// base64url(dc[1] + api_id[4 BE] + test_mode[1] + authkey[256] + user_id[8 BE] + is_bot[1])
// No version prefix. Total payload: 271 bytes.
func EncodePyrogram(s *Session) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}

	buf := make([]byte, 0, 271)
	buf = append(buf, byte(s.DCID))

	appIDBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(appIDBuf, uint32(s.AppID))
	buf = append(buf, appIDBuf...)

	if s.TestMode {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	buf = append(buf, s.AuthKey...)

	uidBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(uidBuf, uint64(s.UserID))
	buf = append(buf, uidBuf...)

	if s.IsBot {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	encoded := base64.URLEncoding.EncodeToString(buf)
	return trimBase64Pad(encoded), nil
}

// DecodePyrogram decodes a Pyrogram session string.
//
// Format: base64url(dc[1] + api_id[4 BE] + test_mode[1] + authkey[256] + user_id[8 BE] + is_bot[1])
// Total payload: 271 bytes. No version prefix, no legacy variants.
func DecodePyrogram(str string) (*Session, error) {
	payload, err := base64.URLEncoding.DecodeString(padBase64(str))
	if err != nil {
		return nil, fmt.Errorf("pyrogram: base64 decode: %w", err)
	}

	if len(payload) != 271 {
		return nil, fmt.Errorf("pyrogram: unexpected payload length %d (expected 271)", len(payload))
	}

	authKey := make([]byte, 256)
	copy(authKey, payload[6:262])

	s := &Session{
		DCID:     int(payload[0]),
		AppID:    int32(binary.BigEndian.Uint32(payload[1:5])),
		TestMode: payload[5] != 0,
		AuthKey:  authKey,
		UserID:   int64(binary.BigEndian.Uint64(payload[262:270])),
		IsBot:    payload[270] != 0,
	}
	s.FillDefaults()
	return s, nil
}


// padBase64 adds padding to a base64 string.
func padBase64(s string) string {
	switch len(s) % 4 {
	case 2:
		return s + "=="
	case 3:
		return s + "="
	default:
		return s
	}
}

// trimBase64Pad removes trailing = characters.
func trimBase64Pad(s string) string {
	for len(s) > 0 && s[len(s)-1] == '=' {
		s = s[:len(s)-1]
	}
	return s
}
