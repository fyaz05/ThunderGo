//go:build js

package sqlite

import (
	"fmt"

	"github.com/mtgo-labs/storage"
)

// SQLite is a no-op stub for js/wasm builds where modernc.org/sqlite
// is unavailable due to platform constraints.
type SQLite struct{}

// Open returns an error on wasm builds — use InMemory storage instead.
func Open(path string) (*SQLite, error) {
	return nil, fmt.Errorf("sqlite is not available in wasm builds; use InMemory storage")
}

// --- SessionStore ---

func (a *SQLite) LoadSession() (*storage.Session, error)   { return nil, nil }
func (a *SQLite) SaveSession(s *storage.Session) error      { return nil }

// --- PeerStore ---

func (a *SQLite) SavePeer(p *storage.Peer) error                         { return nil }
func (a *SQLite) GetPeer(id int64) (*storage.Peer, error)                 { return nil, nil }
func (a *SQLite) GetPeerByUsername(username string) (*storage.Peer, error) { return nil, nil }
func (a *SQLite) LoadPeers() ([]*storage.Peer, error)                     { return nil, nil }
func (a *SQLite) DeletePeer(id int64) error                               { return nil }

// --- ConversationStore ---

func (a *SQLite) SaveConversation(c *storage.Conversation) error          { return nil }
func (a *SQLite) LoadConversation(chatID, userID int64) (*storage.Conversation, error) {
	return nil, nil
}
func (a *SQLite) DeleteConversation(chatID, userID int64) error { return nil }

// --- Close ---

func (a *SQLite) Close() error { return nil }

// --- UpdateStateStore (stubs) ---

func (a *SQLite) LoadUpdateState(string) (*storage.UpdateState, error)                { return nil, nil }
func (a *SQLite) SaveUpdateState(*storage.UpdateState) error                         { return nil }
func (a *SQLite) LoadChannelUpdateState(string, int64) (*storage.ChannelUpdateState, error) { return nil, nil }
func (a *SQLite) LoadAllChannelUpdateStates(string) ([]*storage.ChannelUpdateState, error)  { return nil, nil }
func (a *SQLite) SaveChannelUpdateState(*storage.ChannelUpdateState) error           { return nil }
func (a *SQLite) SaveUpdateDedupKey(string, string) (bool, error)                    { return true, nil }
func (a *SQLite) UpdateDedupKeyExists(string, string) (bool, error)                  { return false, nil }
func (a *SQLite) EnqueueDurableUpdate(*storage.DurableUpdate) error                  { return nil }
func (a *SQLite) DeleteDurableUpdate(string, string) error                           { return nil }
func (a *SQLite) LoadDurableUpdates(string, int) ([]*storage.DurableUpdate, error)   { return nil, nil }
func (a *SQLite) MarkDurableUpdateFailed(string, string, int, string) error          { return nil }

// --- ExportSessionString ---

func ExportSessionString(s *storage.Session) (string, error) {
	return "", fmt.Errorf("sqlite: not available in wasm builds")
}
