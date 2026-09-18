package tgconv

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
)

// EncodeTelethon encodes a session in Telethon string format.
//
// Telethon's StringSession has a single version (prefix "1") and stores only
// what's needed to connect — it never serializes api_id:
//
//	"1" + base64url(dc[1] + ip[4|16] + port[2 BE] + authkey[256])
//
// Mirrors Telethon v1 sessions/string.py (struct ">B{}sH256s", CURRENT_VERSION
// = "1"). AppID is dropped from the wire format on purpose.
func EncodeTelethon(s *Session) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}

	ip := net.ParseIP(s.ServerAddress)
	if ip == nil {
		ip = net.IPv4(0, 0, 0, 0)
	}

	var ipBytes []byte
	if ip4 := ip.To4(); ip4 != nil {
		ipBytes = ip4
	} else {
		ipBytes = ip.To16()
	}

	buf := make([]byte, 0, 1+len(ipBytes)+2+256)
	buf = append(buf, byte(s.DCID))
	buf = append(buf, ipBytes...)

	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, uint16(s.Port))
	buf = append(buf, portBuf...)

	buf = append(buf, s.AuthKey...)

	return "1" + base64.URLEncoding.EncodeToString(buf), nil
}

// DecodeTelethon decodes a Telethon session string. Telethon has a single
// version (prefix "1"), storing dc + ip + port + auth_key. No api_id.
func DecodeTelethon(str string) (*Session, error) {
	if len(str) < 2 || str[0] != '1' {
		return nil, fmt.Errorf("not a Telethon session string")
	}

	payload, err := base64.URLEncoding.DecodeString(str[1:])
	if err != nil {
		payload, err = base64.RawURLEncoding.DecodeString(str[1:])
		if err != nil {
			return nil, fmt.Errorf("telethon: base64 decode: %w", err)
		}
	}

	if len(payload) < 1+4+2+256 {
		return nil, fmt.Errorf("telethon: payload too short (%d bytes)", len(payload))
	}

	off := 0
	dcID := int(payload[off])
	off++

	// IP: 4 bytes (IPv4) or 16 bytes (IPv6). Determine by payload length.
	var ipLen int
	remaining := len(payload) - off
	for _, candidate := range []int{4, 16} {
		if remaining == candidate+2+256 {
			ipLen = candidate
			break
		}
	}
	if ipLen == 0 {
		ipLen = 4 // default to IPv4
	}

	if off+ipLen > len(payload) {
		return nil, fmt.Errorf("telethon: IP exceeds payload")
	}

	ip := net.IP(payload[off : off+ipLen])
	off += ipLen
	serverAddr := ip.String()

	if off+2 > len(payload) {
		return nil, fmt.Errorf("telethon: port out of range")
	}
	port := int(binary.BigEndian.Uint16(payload[off : off+2]))
	off += 2


	if off+256 > len(payload) {
		return nil, fmt.Errorf("telethon: auth_key out of range")
	}
	authKey := make([]byte, 256)
	copy(authKey, payload[off:off+256])

	return &Session{
		DCID:          dcID,
		ServerAddress: serverAddr,
		Port:          port,
		AuthKey:       authKey,
	}, nil
}
