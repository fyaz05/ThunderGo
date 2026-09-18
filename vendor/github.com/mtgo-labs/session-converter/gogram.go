package tgconv

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// gogram session format constants.
const (
	gogramPrefix       = "1BvE"
	gogramPrefixLegacy = "1BvX"
	gogramSeparator    = ":_:"
	gogramLegacySep    = "::"
)

// gogramSession is the JSON structure of a modern gogram string session.
type gogramSession struct {
	AuthKey     []byte `json:"key,omitempty"`
	AuthKeyHash []byte `json:"hash,omitempty"`
	DcID        int    `json:"dc_id,omitempty"`
	IpAddr      string `json:"ip_addr,omitempty"`
	AppID       int32  `json:"app_id,omitempty"`
}

// EncodeGogram encodes a session in gogram string format.
//
// Modern: "1BvE" + base64RawURL(json({key, hash, dc_id, ip_addr, app_id}))
func EncodeGogram(s *Session) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}

	// Compute auth key hash (SHA-1 of auth key, first 8 bytes used as ID).
	hash := authKeyHash(s.AuthKey)

	gs := gogramSession{
		AuthKey:     s.AuthKey,
		AuthKeyHash: hash,
		DcID:        s.DCID,
		IpAddr:      s.ServerAddress,
		AppID:       s.AppID,
	}

	data, err := json.Marshal(gs)
	if err != nil {
		return "", fmt.Errorf("gogram: json marshal: %w", err)
	}

	return gogramPrefix + base64.RawURLEncoding.EncodeToString(data), nil
}

// DecodeGogram decodes a gogram session string (modern and legacy).
func DecodeGogram(str string) (*Session, error) {
	if strings.HasPrefix(str, gogramPrefix) {
		return decodeGogramModern(str)
	}
	if strings.HasPrefix(str, gogramPrefixLegacy) {
		return decodeGogramLegacy(str)
	}
	return nil, fmt.Errorf("not a gogram session string")
}

func decodeGogramModern(str string) (*Session, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(str[len(gogramPrefix):])
	if err != nil {
		return nil, fmt.Errorf("gogram: base64 decode: %w", err)
	}

	var gs gogramSession
	if err := json.Unmarshal(decoded, &gs); err != nil {
		return nil, fmt.Errorf("gogram: json unmarshal: %w", err)
	}

	if len(gs.AuthKey) != 256 {
		return nil, fmt.Errorf("gogram: auth_key must be 256 bytes, got %d", len(gs.AuthKey))
	}

	s := &Session{
		AuthKey:       gs.AuthKey,
		DCID:          gs.DcID,
		ServerAddress: gs.IpAddr,
		AppID:         gs.AppID,
	}
	s.FillDefaults()
	return s, nil
}

func decodeGogramLegacy(str string) (*Session, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(str[len(gogramPrefixLegacy):])
	if err != nil {
		return nil, fmt.Errorf("gogram: legacy base64 decode: %w", err)
	}

	decodedStr := string(decoded)
	split := strings.Split(decodedStr, gogramSeparator)
	if len(split) != 5 {
		split = strings.Split(decodedStr, gogramLegacySep)
	}
	if len(split) != 5 {
		return nil, fmt.Errorf("gogram: legacy session has %d fields (expected 5)", len(split))
	}

	authKey := []byte(split[0])
	if len(authKey) != 256 {
		return nil, fmt.Errorf("gogram: auth_key must be 256 bytes, got %d", len(authKey))
	}

	dcID, err := strconv.Atoi(split[3])
	if err != nil {
		return nil, fmt.Errorf("gogram: invalid DC ID %q: %w", split[3], err)
	}

	appID, err := strconv.ParseInt(split[4], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("gogram: invalid app ID %q: %w", split[4], err)
	}

	s := &Session{
		AuthKey:       authKey,
		ServerAddress: split[2],
		DCID:          dcID,
		AppID:         int32(appID),
	}
	s.FillDefaults()
	return s, nil
}
