// Package storage defines the interfaces and domain types used by mtgo
// Telegram clients to persist session data, peer cache entries, and
// conversation state.
//
// The primary interface is [Adapter], which combines [SessionStore] and
// [PeerStore]. Implementations that also support the conversations plugin
// should satisfy [ConversationStore] as well.
//
// Built-in adapters are provided as sub-modules:
//
//   - [github.com/mtgo-labs/storage/sqlite]     — SQLite (pure-Go, no CGO)
//   - [github.com/mtgo-labs/storage/postgres]    — PostgreSQL
//   - [github.com/mtgo-labs/storage/mongodb]     — MongoDB
//
// To create a custom adapter, implement the [Adapter] interface (and
// optionally [ConversationStore]). See the ExampleCustomStorage type in this
// package for a minimal in-memory implementation.
//
// # Wiring an adapter into a client
//
//	store, _ := sqlite.Open("session.db")
//	client, _ := telegram.NewClient(apiID, apiHash, telegram.WithStorage(store))
//
// # Implementing a custom adapter
//
// A custom adapter must implement [SessionStore], [PeerStore], and [Close].
// To support the conversations plugin, also implement [ConversationStore].
// Use the test suite in [github.com/mtgo-labs/storage/internal/suite] to verify
// correctness:
//
//	func TestMyAdapter(t *testing.T) {
//	    a := NewMyAdapter()
//	    suite.Run(t, a)
//	}
package storage

import (
	"encoding/json"
)

// PeerType represents the type of a Telegram peer.
type PeerType int

const (
	// PeerTypeUser identifies a Telegram user.
	PeerTypeUser PeerType = 0
	// PeerTypeChat identifies a basic group chat.
	PeerTypeChat PeerType = 1
	// PeerTypeChannel identifies a channel or supergroup.
	PeerTypeChannel PeerType = 2
)

func (p PeerType) String() string {
	switch p {
	case PeerTypeUser:
		return "user"
	case PeerTypeChat:
		return "chat"
	case PeerTypeChannel:
		return "channel"
	default:
		return "unknown"
	}
}

// Session holds MTProto session data.
type Session struct {
	SessionID string `json:"session_id"`
	DC        int    `json:"dc"`
	APIID     int32  `json:"api_id"`
	APIHash   string `json:"api_hash"`
	TestMode  bool   `json:"test_mode"`
	AuthKey   []byte `json:"auth_key"`
	State     []byte `json:"state"`
	UserID    int64  `json:"user_id"`
	IsBot     bool   `json:"is_bot"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Username  string `json:"username"`
	Date      int    `json:"date"`
}

// Peer holds cached peer information.
type Peer struct {
	ID          int64    `json:"id"`
	Type        PeerType `json:"type"`
	AccessHash  int64    `json:"access_hash"`
	Username    string   `json:"username"`
	Usernames   string   `json:"usernames,omitempty"`
	FirstName   string   `json:"first_name"`
	LastName    string   `json:"last_name"`
	PhoneNumber string   `json:"phone_number,omitempty"`
	IsBot       bool     `json:"is_bot,omitempty"`
	PhotoID     int64    `json:"photo_id,omitempty"`
	Language    string   `json:"language,omitempty"`
	LastUpdated int64    `json:"last_updated"`
}

// Conversation holds plugin conversation state.
type Conversation struct {
	ChatID    int64  `json:"chat_id"`
	UserID    int64  `json:"user_id"`
	Name      string `json:"name"`
	Step      int    `json:"step"`
	Data      []byte `json:"data,omitempty"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

// SessionStore persists MTProto session data.
type SessionStore interface {
	LoadSession() (*Session, error)
	SaveSession(s *Session) error
}

// PeerStore persists peer cache entries.
type PeerStore interface {
	SavePeer(p *Peer) error
	GetPeer(id int64) (*Peer, error)
	GetPeerByUsername(username string) (*Peer, error)
	LoadPeers() ([]*Peer, error)
	DeletePeer(id int64) error
}

// ConversationStore persists plugin conversation state.
// Implementations should create their backing table lazily on first use.
type ConversationStore interface {
	SaveConversation(c *Conversation) error
	LoadConversation(chatID, userID int64) (*Conversation, error)
	DeleteConversation(chatID, userID int64) error
}

// StateEntry holds a single FSM/state plugin key-value entry.
type StateEntry struct {
	Scope     int    `json:"scope"`      // 0=chat+user, 1=user, 2=chat
	ChatID    int64  `json:"chat_id"`
	UserID    int64  `json:"user_id"`
	Key       string `json:"key"`
	Value     []byte `json:"value"`      // JSON-encoded
	ExpiresAt int64  `json:"expires_at"` // unix seconds, 0 = never
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

// StateStore persists FSM/state plugin entries.
// Implementations should create their backing table lazily on first use.
type StateStore interface {
	SaveState(entry *StateEntry) error
	LoadState(scope int, chatID, userID int64, key string) (*StateEntry, error)
	DeleteState(scope int, chatID, userID int64, key string) error
	ClearState(scope int, chatID, userID int64) error
}

// DeviceEntry holds a persisted device profile.
type DeviceEntry struct {
	Key            string `json:"key"`              // session name or custom identifier
	DeviceModel    string `json:"device_model"`
	SystemVersion  string `json:"system_version"`
	AppVersion     string `json:"app_version"`
	LangCode       string `json:"lang_code"`
	SystemLangCode string `json:"system_lang_code"`
	LangPack       string `json:"lang_pack"`
	Platform       string `json:"platform"`         // types.ClientPlatform as string
	CreatedAt      int64  `json:"created_at"`
	UpdatedAt      int64  `json:"updated_at"`
}

// DeviceStore persists device profiles.
// Implementations should create their backing table lazily on first use.
type DeviceStore interface {
	SaveDevice(entry *DeviceEntry) error
	LoadDevice(key string) (*DeviceEntry, error)
	DeleteDevice(key string) error
}

// Adapter combines session and peer storage. ConversationStore is optional;
// adapters that support it should also implement ConversationStore.
type Adapter interface {
	SessionStore
	PeerStore
	Close() error
}

type DCAuthEntry struct {
	DCID       int    `json:"dc_id"`
	AuthKey    []byte `json:"auth_key"`
	ServerSalt int64  `json:"server_salt"`
	Port       int    `json:"port"`
	ServerAddr string `json:"server_addr"`
}

type DCAuthStore interface {
	SaveDCAuth(entry DCAuthEntry) error
	LoadDCAuth(dcID int) (DCAuthEntry, error)
}

// Storage is the interface the Telegram client uses to read and write
// session fields. Implementations must also implement [PeerCache] for
// peer persistence and [Close] for cleanup.
//
// Built-in implementations:
//   - [MemoryStorage] — in-memory, for testing
//   - [SQLiteStorage] — SQLite-backed, created via [NewSQLiteStorage]
//
// Third-party adapters (sqlite, mongodb, postgres) can be bridged via [NewAdapter]:
//
//	store, _ := sqlite.Open("bot.db")
//	client, _ := tg.NewClient(apiID, apiHash, &tg.Config{
//	    Storage: storage.NewAdapter(store),
//	})
type Storage interface {
	SessionID() (string, error)
	SetSessionID(string) error

	DCID() (int, error)
	SetDCID(int) error

	APIID() (int32, error)
	SetAPIID(int32) error

	APIHash() (string, error)
	SetAPIHash(string) error

	TestMode() (bool, error)
	SetTestMode(bool) error

	AuthKey() ([]byte, error)
	SetAuthKey([]byte) error

	UserID() (int64, error)
	SetUserID(int64) error

	IsBot() (bool, error)
	SetIsBot(bool) error

	FirstName() (string, error)
	SetFirstName(string) error

	LastName() (string, error)
	SetLastName(string) error

	Username() (string, error)
	SetUsername(string) error

	Date() (int, error)
	SetDate(int) error

	State() ([]byte, error)
	SetState([]byte) error

	// ExportSessionString encodes the session state into a portable string
	// using the Telethon/Pyrogram format:
	//
	//	"1" + base64url( dc_id[1B] + ip[4B|16B] + port[2B big-endian] + auth_key[256B] )
	//
	// WARNING: the returned string contains the full 256-byte authorization key
	// in plaintext. Anyone who obtains this string can fully impersonate the
	// session.
	ExportSessionString() (string, error)

	Close() error
}

// PeerCache is an optional interface that Storage implementations can
// satisfy to persist peer entries for the Telegram client's peer cache.
type PeerCache interface {
	SavePeers([]*Peer) error
	LoadPeers() ([]*Peer, error)
	SavePeer(*Peer) error
	DeletePeer(id int64) error
}

// MergePeer returns incoming with empty metadata fields filled from existing.
// It prevents minimal cache writes from erasing richer peer data saved earlier.
func MergePeer(existing, incoming *Peer) *Peer {
	if incoming == nil {
		return existing
	}
	if existing == nil {
		cp := *incoming
		return &cp
	}
	merged := *incoming
	if merged.AccessHash == 0 {
		merged.AccessHash = existing.AccessHash
	}
	if merged.Username == "" {
		merged.Username = existing.Username
	}
	if merged.Usernames == "" {
		merged.Usernames = existing.Usernames
	}
	if merged.FirstName == "" {
		merged.FirstName = existing.FirstName
	}
	if merged.LastName == "" {
		merged.LastName = existing.LastName
	}
	if merged.PhoneNumber == "" {
		merged.PhoneNumber = existing.PhoneNumber
	}
	if merged.PhotoID == 0 {
		merged.PhotoID = existing.PhotoID
	}
	if merged.Language == "" {
		merged.Language = existing.Language
	}
	if merged.LastUpdated == 0 {
		merged.LastUpdated = existing.LastUpdated
	}
	merged.IsBot = incoming.IsBot || existing.IsBot
	return &merged
}

type UpdateState struct {
	SessionID string `json:"session_id"`
	Pts       int32  `json:"pts"`
	Qts       int32  `json:"qts"`
	Date      int32  `json:"date"`
	Seq       int32  `json:"seq"`
}

type ChannelUpdateState struct {
	SessionID string `json:"session_id"`
	ChannelID int64  `json:"channel_id"`
	Pts       int32  `json:"pts"`
}

type DurableUpdate struct {
	SessionID string `json:"session_id"`
	ID        string `json:"id"`
	Payload   []byte `json:"payload"`
	Attempts  int    `json:"attempts"`
	LastError string `json:"last_error"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type UpdateStateStore interface {
	LoadUpdateState(sessionID string) (*UpdateState, error)
	SaveUpdateState(state *UpdateState) error
	LoadChannelUpdateState(sessionID string, channelID int64) (*ChannelUpdateState, error)
	LoadAllChannelUpdateStates(sessionID string) ([]*ChannelUpdateState, error)
	SaveChannelUpdateState(state *ChannelUpdateState) error
	SaveUpdateDedupKey(sessionID string, key string) (bool, error)
	UpdateDedupKeyExists(sessionID string, key string) (bool, error)
	EnqueueDurableUpdate(update *DurableUpdate) error
	DeleteDurableUpdate(sessionID string, id string) error
	LoadDurableUpdates(sessionID string, limit int) ([]*DurableUpdate, error)
	MarkDurableUpdateFailed(sessionID string, id string, attempts int, lastErr string) error
}

// SessionIDAware is an optional interface that adapters can implement to
// receive the session name from the client. The client calls SetSessionName
// early in connectTransport, before any data access, so the adapter can
// scope its queries accordingly.
type SessionIDAware interface {
	SetSessionName(name string)
}

// MarshalJSON marshals v to JSON bytes.
func MarshalJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// UnmarshalJSON unmarshals data into v. If data is empty, it is a no-op.
func UnmarshalJSON(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, v)
}
