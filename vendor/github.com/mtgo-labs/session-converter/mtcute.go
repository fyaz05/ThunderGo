package tgconv

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

// mtcute format constants.
const (
	mtcuteVersion  = 3 // session version byte
	mtcuteDCVer    = 2 // DC option version byte (current mtcute)
	mtcuteDCVerAlt = 1 // legacy DC option version (older implementations)

	mtcuteFlagHasSelf  uint32 = 1 << 0
	mtcuteFlagTestMode uint32 = 1 << 1
	mtcuteFlagMedia    uint32 = 1 << 2

	mtcuteDCFlagIPv6      byte = 1 << 0
	mtcuteDCFlagMediaOnly byte = 1 << 1
	mtcuteDCFlagTestMode  byte = 1 << 2
)

// EncodeMtcute encodes a session in mtcute string format.
//
// base64url(version[1]=3 + flags[4 LE] + dc_option(TL) + [media_dc(TL)] + user_id[8 LE] + is_bot(TL bool) + authkey(TL bytes))
func EncodeMtcute(s *Session) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}

	buf := make([]byte, 0, 512)
	buf = append(buf, mtcuteVersion)

	var flags uint32
	if s.UserID != 0 {
		flags |= mtcuteFlagHasSelf
	}
	if s.TestMode {
		flags |= mtcuteFlagTestMode
	}
	flagsBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(flagsBuf, flags)
	buf = append(buf, flagsBuf...)

	// Primary DC option.
	buf = append(buf, tlBytes(serializeMtcuteDC(s.DCID, s.ServerAddress, s.Port, s.TestMode))...)

	// User fields.
	if s.UserID != 0 {
		uidBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(uidBuf, uint64(s.UserID))
		buf = append(buf, uidBuf...)
		buf = append(buf, tlBool(s.IsBot)...)
	}

	// Auth key.
	buf = append(buf, tlBytes(s.AuthKey)...)

	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// DecodeMtcute decodes an mtcute session string.
func DecodeMtcute(str string) (*Session, error) {
	payload, err := base64.RawURLEncoding.DecodeString(str)
	if err != nil {
		payload, err = base64.URLEncoding.DecodeString(padBase64(str))
		if err != nil {
			return nil, fmt.Errorf("mtcute: base64 decode: %w", err)
		}
	}

	if len(payload) < 5 {
		return nil, fmt.Errorf("mtcute: payload too short (%d bytes)", len(payload))
	}

	off := 0
	if payload[off] != mtcuteVersion {
		return nil, fmt.Errorf("mtcute: unexpected version %d (expected %d)", payload[off], mtcuteVersion)
	}
	off++

	flags := binary.LittleEndian.Uint32(payload[off : off+4])
	off += 4

	hasSelf := flags&mtcuteFlagHasSelf != 0
	hasMedia := flags&mtcuteFlagMedia != 0

	// Primary DC option.
	dcData, n, err := readTLBytes(payload, off)
	if err != nil {
		return nil, fmt.Errorf("mtcute: reading DC option: %w", err)
	}
	off = n

	// Skip media DC option if present.
	if hasMedia {
		_, n, err = readTLBytes(payload, off)
		if err != nil {
			return nil, fmt.Errorf("mtcute: reading media DC option: %w", err)
		}
		off = n
	}

	dcID, addr, port, testMode, err := parseMtcuteDC(dcData)
	if err != nil {
		return nil, fmt.Errorf("mtcute: parsing DC option: %w", err)
	}

	var userID int64
	var isBot bool

	if hasSelf {
		if off+8 > len(payload) {
			return nil, fmt.Errorf("mtcute: payload too short for user_id")
		}
		userID = int64(binary.LittleEndian.Uint64(payload[off : off+8]))
		off += 8

		if off+4 > len(payload) {
			return nil, fmt.Errorf("mtcute: payload too short for is_bot")
		}
		isBot = readTLBool(payload[off : off+4])
		off += 4
	}

	// Auth key.
	authKeyData, _, err := readTLBytes(payload, off)
	if err != nil {
		return nil, fmt.Errorf("mtcute: reading auth key: %w", err)
	}

	if len(authKeyData) != 256 {
		return nil, fmt.Errorf("mtcute: auth_key must be 256 bytes, got %d", len(authKeyData))
	}

	authKey := make([]byte, 256)
	copy(authKey, authKeyData)

	return &Session{
		DCID:          dcID,
		ServerAddress: addr,
		Port:          port,
		AuthKey:       authKey,
		TestMode:      testMode,
		UserID:        userID,
		IsBot:         isBot,
	}, nil
}

func serializeMtcuteDC(dcID int, ip string, port int, testMode bool) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, mtcuteDCVer) // version
	buf = append(buf, byte(dcID)) // dcId

	var dcFlags byte
	if isIPv6(ip) {
		dcFlags |= mtcuteDCFlagIPv6
	}
	if testMode {
		dcFlags |= mtcuteDCFlagTestMode
	}
	buf = append(buf, dcFlags)
	buf = append(buf, tlBytes([]byte(ip))...)

	portBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(portBuf, uint32(port))
	buf = append(buf, portBuf...)

	return buf
}

func parseMtcuteDC(data []byte) (dcID int, addr string, port int, testMode bool, err error) {
	if len(data) < 3 {
		return 0, "", 0, false, fmt.Errorf("DC option too short (%d bytes)", len(data))
	}

	off := 0
	ver := data[off]
	if ver != mtcuteDCVer && ver != mtcuteDCVerAlt {
		return 0, "", 0, false, fmt.Errorf("unexpected DC option version %d", ver)
	}
	off++

	dcID = int(data[off])
	off++

	dcFlags := data[off]
	off++
	testMode = dcFlags&mtcuteDCFlagTestMode != 0

	ipData, n, err := readTLBytes(data, off)
	if err != nil {
		return 0, "", 0, false, fmt.Errorf("reading IP: %w", err)
	}
	addr = string(ipData)
	off = n

	if off+4 > len(data) {
		return 0, "", 0, false, fmt.Errorf("DC option too short for port")
	}
	port = int(binary.LittleEndian.Uint32(data[off : off+4]))

	return dcID, addr, port, testMode, nil
}
