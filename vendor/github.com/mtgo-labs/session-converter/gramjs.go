package tgconv

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
)

// EncodeGramJS encodes a session in GramJS string format.
//
// "1" + base64std(dc[1] + addr_len[2 BE] + addr_bytes[variable] + port[2 BE] + authkey[256])
func EncodeGramJS(s *Session) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}

	addr := []byte(s.ServerAddress)
	if len(addr) > 253 {
		return "", fmt.Errorf("gramjs: address too long (%d bytes)", len(addr))
	}

	buf := make([]byte, 0, 1+2+len(addr)+2+256)
	buf = append(buf, byte(s.DCID))

	addrLen := make([]byte, 2)
	binary.BigEndian.PutUint16(addrLen, uint16(len(addr)))
	buf = append(buf, addrLen...)
	buf = append(buf, addr...)

	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, uint16(s.Port))
	buf = append(buf, portBuf...)

	buf = append(buf, s.AuthKey...)

	return "1" + base64.StdEncoding.EncodeToString(buf), nil
}

// DecodeGramJS decodes a GramJS session string.
func DecodeGramJS(str string) (*Session, error) {
	if len(str) < 2 || str[0] != '1' {
		return nil, fmt.Errorf("not a GramJS session string")
	}

	payload, err := base64.StdEncoding.DecodeString(str[1:])
	if err != nil {
		return nil, fmt.Errorf("gramjs: base64 decode: %w", err)
	}

	if len(payload) < 5+256 {
		return nil, fmt.Errorf("gramjs: payload too short (%d bytes)", len(payload))
	}

	dcID := int(payload[0])
	addrLen := int(binary.BigEndian.Uint16(payload[1:3]))

	if len(payload) < 3+addrLen+2+256 {
		return nil, fmt.Errorf("gramjs: payload too short for address length %d", addrLen)
	}

	// Standard GramJS stores the server address as a UTF-8 string. Some
	// converters/older variants store it as a 4-byte binary IPv4 instead —
	// render that as dotted-quad rather than decoding the raw bytes as text.
	addrBytes := payload[3 : 3+addrLen]
	var addr string
	if addrLen == 4 {
		addr = fmt.Sprintf("%d.%d.%d.%d", addrBytes[0], addrBytes[1], addrBytes[2], addrBytes[3])
	} else {
		addr = string(addrBytes)
	}
	port := int(binary.BigEndian.Uint16(payload[3+addrLen : 3+addrLen+2]))
	authKey := make([]byte, 256)
	copy(authKey, payload[3+addrLen+2:])

	return &Session{
		DCID:          dcID,
		ServerAddress: addr,
		Port:          port,
		AuthKey:       authKey,
	}, nil
}
