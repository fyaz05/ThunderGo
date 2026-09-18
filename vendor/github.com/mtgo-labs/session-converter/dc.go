package tgconv

import "fmt"

// dcAddresses maps Telegram DC IDs to their default server addresses.
// Used as a fallback when the source format does not include a server
// address (e.g., Pyrogram string sessions omit the IP).
var dcAddresses = map[int]string{
	1:  "149.154.175.50",
	2:  "149.154.167.51",
	3:  "149.154.175.100",
	4:  "149.154.167.91",
	5:  "91.108.56.165",
	// Test DCs
	-1: "149.154.175.50",
	-2: "149.154.167.51",
	-3: "149.154.175.100",
	-4: "149.154.167.91",
	-5: "91.108.56.165",
}

// defaultPort is the standard Telegram MTProto port.
const defaultPort = 443

// dcAddr returns the default server address for a DC ID, or empty if unknown.
func dcAddr(dcID int) string {
	return dcAddresses[dcID]
}

// FillDefaults populates ServerAddress and Port from DC defaults if they are
// zero. This is called after decoding formats that omit these fields, and
// should be called after constructing a Session manually (e.g., from SQLite).
func (s *Session) FillDefaults() {
	if s.ServerAddress == "" {
		if addr := dcAddr(s.DCID); addr != "" {
			s.ServerAddress = addr
		}
	}
	if s.Port == 0 {
		s.Port = defaultPort
	}
}

// validate checks that the session has the minimum required fields.
func (s *Session) validate() error {
	if len(s.AuthKey) != 256 {
		return fmt.Errorf("auth_key must be 256 bytes, got %d", len(s.AuthKey))
	}
	if s.DCID == 0 {
		return fmt.Errorf("dc_id is zero")
	}
	return nil
}
