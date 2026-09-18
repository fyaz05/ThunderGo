package tgconv

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
)

// authKeyHash returns the SHA-1 hash of the auth key (used by gogram/gotd).
func authKeyHash(authKey []byte) []byte {
	h := sha1.Sum(authKey) //nolint:gosec // Telegram auth key hash, not security-critical
	return h[:]
}

// EncodeGotgproto encodes a session in gotgproto string format.
//
// The format is base64std(json(gotgprotoSession)), where gotgprotoSession
// wraps gotd's session data:
//
//	{
//	  "Version": 1,
//	  "Data": base64(json({Version: 1, Data: {DC, Addr, AuthKey, AuthKeyID, Salt, Config}}))
//	}
func EncodeGotgproto(s *Session) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}

	addr := s.ServerAddress
	if s.Port > 0 {
		addr = net.JoinHostPort(s.ServerAddress, strconv.Itoa(s.Port))
	}

	keyID := authKeyHash(s.AuthKey)[12:] // last 8 bytes of SHA-1

	// Inner: gotd session.Data JSON.
	innerData := map[string]any{
		"Config": map[string]any{
			"TestMode": s.TestMode,
		},
		"DC":        s.DCID,
		"Addr":      addr,
		"AuthKey":   s.AuthKey,
		"AuthKeyID": keyID,
		"Salt":      0,
	}

	innerJSON, err := json.Marshal(map[string]any{
		"Version": 1,
		"Data":    innerData,
	})
	if err != nil {
		return "", fmt.Errorf("gotgproto: marshal inner: %w", err)
	}

	// Outer: gotgproto storage.Session. Data is raw nested JSON, not
	// base64 — json.RawMessage prevents []byte from being base64-encoded.
	outer, err := json.Marshal(map[string]any{
		"Version": 1,
		"Data":    json.RawMessage(innerJSON),
	})
	if err != nil {
		return "", fmt.Errorf("gotgproto: marshal outer: %w", err)
	}

	return base64.StdEncoding.EncodeToString(outer), nil
}

// DecodeGotgproto decodes a gotgproto session string.
//
// Handles both the current format (Version + Data wrapping gotd JSON) and
// older formats where the JSON directly contains DCID, AuthKey, etc.
func DecodeGotgproto(str string) (*Session, error) {
	raw, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		raw, err = base64.RawURLEncoding.DecodeString(str)
		if err != nil {
			return nil, fmt.Errorf("gotgproto: base64 decode: %w", err)
		}
	}

	// Parse the outer JSON as a generic map to handle multiple versions.
	var outer map[string]json.RawMessage
	if err := json.Unmarshal(raw, &outer); err != nil {
		return nil, fmt.Errorf("gotgproto: json unmarshal: %w", err)
	}

	// Try to extract the inner data. Supports multiple formats:
	//   1. Data is a base64 string → decode → unwrap inner Version/Data.
	//   2. Data is a raw JSON object with Version/Data envelope → unwrap.
	//   3. Data is a raw JSON object with direct fields → use as-is.
	//   4. Fields are directly in the outer JSON (very old format).
	var dataMap map[string]json.RawMessage

	if dataRaw, ok := outer["Data"]; ok {
		// Case 1: Data is a base64-encoded JSON string.
		var dataB64 string
		if err := json.Unmarshal(dataRaw, &dataB64); err == nil {
			innerBytes, err := base64.StdEncoding.DecodeString(dataB64)
			if err != nil {
				return nil, fmt.Errorf("gotgproto: decode inner data: %w", err)
			}

			var innerWrapper map[string]json.RawMessage
			if err := json.Unmarshal(innerBytes, &innerWrapper); err != nil {
				return nil, fmt.Errorf("gotgproto: unmarshal inner wrapper: %w", err)
			}

			if innerData, ok := innerWrapper["Data"]; ok {
				if err := json.Unmarshal(innerData, &dataMap); err != nil {
					return nil, fmt.Errorf("gotgproto: unmarshal inner Data: %w", err)
				}
			} else {
				dataMap = innerWrapper
			}
		} else {
			// Case 2/3: Data is a raw JSON object.
			var innerWrapper map[string]json.RawMessage
			if err := json.Unmarshal(dataRaw, &innerWrapper); err != nil {
				dataMap = outer // Case 4: fall back to outer map.
			} else if innerData, ok := innerWrapper["Data"]; ok {
				// Case 2: unwrap the Version/Data envelope.
				if err := json.Unmarshal(innerData, &dataMap); err != nil {
					dataMap = innerWrapper
				}
			} else {
				// Case 3: direct fields in Data object.
				dataMap = innerWrapper
			}
		}
	} else {
		dataMap = outer
	}

	s := &Session{}

	// DC.
	if v, ok := dataMap["DC"]; ok {
		json.Unmarshal(v, &s.DCID)
	}
	if v, ok := dataMap["DcId"]; ok {
		json.Unmarshal(v, &s.DCID)
	}
	if v, ok := dataMap["dc"]; ok {
		json.Unmarshal(v, &s.DCID)
	}

	// Addr (may be "host:port" or separate fields).
	if v, ok := dataMap["Addr"]; ok {
		var addr string
		json.Unmarshal(v, &addr)
		s.ServerAddress, s.Port = splitAddr(addr)
	}
	if v, ok := dataMap["ServerAddress"]; ok {
		var addr string
		json.Unmarshal(v, &addr)
		s.ServerAddress, s.Port = splitAddr(addr)
	}

	// AuthKey.
	if v, ok := dataMap["AuthKey"]; ok {
		json.Unmarshal(v, &s.AuthKey)
	}
	if v, ok := dataMap["authKey"]; ok {
		json.Unmarshal(v, &s.AuthKey)
	}

	// TestMode.
	if v, ok := dataMap["Config"]; ok {
		var cfg struct {
			TestMode bool `json:"TestMode"`
		}
		json.Unmarshal(v, &cfg)
		s.TestMode = cfg.TestMode
	}

	// UserID.
	if v, ok := dataMap["UserID"]; ok {
		json.Unmarshal(v, &s.UserID)
	}
	if v, ok := dataMap["UserId"]; ok {
		json.Unmarshal(v, &s.UserID)
	}

	s.FillDefaults()
	if s.AuthKey == nil || len(s.AuthKey) != 256 {
		return nil, fmt.Errorf("gotgproto: auth_key must be 256 bytes, got %d", len(s.AuthKey))
	}

	return s, nil
}

// splitAddr splits a "host:port" address into its components.
func splitAddr(addr string) (host string, port int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	p, _ := strconv.Atoi(portStr)
	return host, p
}

