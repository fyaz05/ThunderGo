package storage

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

type memoryAdapter struct {
	mu          sync.RWMutex
	sessionName string

	sess *Session

	peers          map[int64]*Peer
	peerByUsername map[string]int64

	updateStates   map[string]*UpdateState
	channelStates  map[string]map[int64]*ChannelUpdateState
	updateDedup    map[string]map[string]struct{}
	durableUpdates map[string]map[string]*DurableUpdate

	conversations map[conversationKey]*Conversation
	dcAuth        map[int]DCAuthEntry
	stateEntries  map[stateKey]*StateEntry
	deviceEntries map[string]*DeviceEntry
}

type conversationKey struct {
	ChatID int64
	UserID int64
}

type stateKey struct {
	Scope  int
	ChatID int64
	UserID int64
	Key    string
}


var (
	_ Adapter           = (*memoryAdapter)(nil)
	_ ConversationStore = (*memoryAdapter)(nil)
	_ UpdateStateStore  = (*memoryAdapter)(nil)
	_ DCAuthStore       = (*memoryAdapter)(nil)
	_ StateStore        = (*memoryAdapter)(nil)
	_ DeviceStore        = (*memoryAdapter)(nil)
	_ SessionIDAware    = (*memoryAdapter)(nil)
)

func NewMemory() Storage {
	a := &memoryAdapter{
		peers:          make(map[int64]*Peer),
		peerByUsername: make(map[string]int64),
		updateStates:   make(map[string]*UpdateState),
		channelStates:  make(map[string]map[int64]*ChannelUpdateState),
		updateDedup:    make(map[string]map[string]struct{}),
		durableUpdates: make(map[string]map[string]*DurableUpdate),
		conversations:  make(map[conversationKey]*Conversation),
		dcAuth:         make(map[int]DCAuthEntry),
		stateEntries:   make(map[stateKey]*StateEntry),
		deviceEntries: make(map[string]*DeviceEntry),
	}
	return NewAdapter(a)
}

func (m *memoryAdapter) SetSessionName(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessionName = name
}

// --- SessionStore ---

func (m *memoryAdapter) LoadSession() (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.sess == nil {
		return nil, nil
	}
	cp := *m.sess
	return &cp, nil
}

func (m *memoryAdapter) SaveSession(s *Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *s
	m.sess = &cp
	return nil
}

// --- PeerStore ---

func (m *memoryAdapter) SavePeer(p *Peer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *MergePeer(m.peers[p.ID], p)
	m.peers[p.ID] = &cp
	if p.Username != "" {
		m.peerByUsername[p.Username] = p.ID
	}
	return nil
}

func (m *memoryAdapter) GetPeer(id int64) (*Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[id]
	if !ok {
		return nil, nil
	}
	cp := *p
	return &cp, nil
}

func (m *memoryAdapter) GetPeerByUsername(username string) (*Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, ok := m.peerByUsername[username]
	if !ok {
		return nil, nil
	}
	p, ok := m.peers[id]
	if !ok {
		return nil, nil
	}
	cp := *p
	return &cp, nil
}

func (m *memoryAdapter) LoadPeers() ([]*Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Peer, 0, len(m.peers))
	for _, p := range m.peers {
		cp := *p
		out = append(out, &cp)
	}
	return out, nil
}

func (m *memoryAdapter) DeletePeer(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.peers[id]; ok {
		if p.Username != "" {
			delete(m.peerByUsername, p.Username)
		}
		delete(m.peers, id)
	}
	return nil
}

// --- ConversationStore ---

func (m *memoryAdapter) SaveConversation(c *Conversation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := c.UpdatedAt
	if now == 0 {
		now = time.Now().Unix()
	}
	createdAt := c.CreatedAt
	if createdAt == 0 {
		createdAt = now
	}
	cp := *c
	cp.CreatedAt = createdAt
	cp.UpdatedAt = now
	m.conversations[conversationKey{ChatID: c.ChatID, UserID: c.UserID}] = &cp
	return nil
}

func (m *memoryAdapter) LoadConversation(chatID, userID int64) (*Conversation, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.conversations[conversationKey{ChatID: chatID, UserID: userID}]
	if !ok {
		return nil, nil
	}
	cp := *c
	return &cp, nil
}

func (m *memoryAdapter) DeleteConversation(chatID, userID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conversations, conversationKey{ChatID: chatID, UserID: userID})
	return nil
}

// --- StateStore ---

func (m *memoryAdapter) SaveState(entry *StateEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := entry.UpdatedAt
	if now == 0 {
		now = time.Now().Unix()
	}
	createdAt := entry.CreatedAt
	if createdAt == 0 {
		createdAt = now
	}
	cp := *entry
	cp.CreatedAt = createdAt
	cp.UpdatedAt = now
	if cp.Value != nil {
		cp.Value = append([]byte(nil), cp.Value...)
	}
	m.stateEntries[stateKey{Scope: entry.Scope, ChatID: entry.ChatID, UserID: entry.UserID, Key: entry.Key}] = &cp
	return nil
}

func (m *memoryAdapter) LoadState(scope int, chatID, userID int64, key string) (*StateEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.stateEntries[stateKey{Scope: scope, ChatID: chatID, UserID: userID, Key: key}]
	if !ok {
		return nil, nil
	}
	cp := *e
	if cp.Value != nil {
		cp.Value = append([]byte(nil), cp.Value...)
	}
	return &cp, nil
}

func (m *memoryAdapter) DeleteState(scope int, chatID, userID int64, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.stateEntries, stateKey{Scope: scope, ChatID: chatID, UserID: userID, Key: key})
	return nil
}

func (m *memoryAdapter) ClearState(scope int, chatID, userID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for sk := range m.stateEntries {
		if sk.Scope == scope && sk.ChatID == chatID && sk.UserID == userID {
			delete(m.stateEntries, sk)
		}
	}
	return nil
}

// --- DeviceStore ---

func (m *memoryAdapter) SaveDevice(entry *DeviceEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := entry.UpdatedAt
	if now == 0 {
		now = time.Now().Unix()
	}
	createdAt := entry.CreatedAt
	if createdAt == 0 {
		createdAt = now
	}
	cp := *entry
	cp.CreatedAt = createdAt
	cp.UpdatedAt = now
	m.deviceEntries[entry.Key] = &cp
	return nil
}

func (m *memoryAdapter) LoadDevice(key string) (*DeviceEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.deviceEntries[key]
	if !ok {
		return nil, nil
	}
	cp := *e
	return &cp, nil
}

func (m *memoryAdapter) DeleteDevice(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.deviceEntries, key)
	return nil
}

// --- DCAuthStore ---

func (m *memoryAdapter) SaveDCAuth(entry DCAuthEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dcAuth[entry.DCID] = entry
	return nil
}

func (m *memoryAdapter) LoadDCAuth(dcID int) (DCAuthEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.dcAuth[dcID]
	if !ok {
		return DCAuthEntry{}, fmt.Errorf("dc auth not found: %d", dcID)
	}
	return entry, nil
}

// --- UpdateStateStore ---

func (m *memoryAdapter) LoadUpdateState(sessionID string) (*UpdateState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.updateStates[sessionID]
	if !ok {
		return nil, nil
	}
	cp := *s
	return &cp, nil
}

func (m *memoryAdapter) SaveUpdateState(s *UpdateState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *s
	m.updateStates[s.SessionID] = &cp
	return nil
}

func (m *memoryAdapter) LoadChannelUpdateState(sessionID string, channelID int64) (*ChannelUpdateState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	channels := m.channelStates[sessionID]
	if channels == nil {
		return nil, nil
	}
	s, ok := channels[channelID]
	if !ok {
		return nil, nil
	}
	cp := *s
	return &cp, nil
}

func (m *memoryAdapter) LoadAllChannelUpdateStates(sessionID string) ([]*ChannelUpdateState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	channels := m.channelStates[sessionID]
	if channels == nil {
		return nil, nil
	}
	out := make([]*ChannelUpdateState, 0, len(channels))
	for _, s := range channels {
		cp := *s
		out = append(out, &cp)
	}
	return out, nil
}

func (m *memoryAdapter) SaveChannelUpdateState(s *ChannelUpdateState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.channelStates[s.SessionID] == nil {
		m.channelStates[s.SessionID] = make(map[int64]*ChannelUpdateState)
	}
	cp := *s
	m.channelStates[s.SessionID][s.ChannelID] = &cp
	return nil
}

func (m *memoryAdapter) SaveUpdateDedupKey(sessionID string, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.updateDedup[sessionID] == nil {
		m.updateDedup[sessionID] = make(map[string]struct{})
	}
	if _, ok := m.updateDedup[sessionID][key]; ok {
		return false, nil
	}
	m.updateDedup[sessionID][key] = struct{}{}
	return true, nil
}

func (m *memoryAdapter) UpdateDedupKeyExists(sessionID string, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.updateDedup[sessionID][key]
	return ok, nil
}

func (m *memoryAdapter) EnqueueDurableUpdate(u *DurableUpdate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.durableUpdates[u.SessionID] == nil {
		m.durableUpdates[u.SessionID] = make(map[string]*DurableUpdate)
	}
	cp := *u
	m.durableUpdates[u.SessionID][u.ID] = &cp
	return nil
}

func (m *memoryAdapter) DeleteDurableUpdate(sessionID string, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.durableUpdates[sessionID], id)
	return nil
}

func (m *memoryAdapter) LoadDurableUpdates(sessionID string, limit int) ([]*DurableUpdate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*DurableUpdate
	for _, item := range m.durableUpdates[sessionID] {
		cp := *item
		out = append(out, &cp)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *memoryAdapter) MarkDurableUpdateFailed(sessionID string, id string, attempts int, lastErr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	item := m.durableUpdates[sessionID][id]
	if item == nil {
		return nil
	}
	item.Attempts = attempts
	item.LastError = lastErr
	return nil
}

// --- Close ---

func (m *memoryAdapter) Close() error { return nil }

// --- Export helpers ---

// ExportSessionString encodes session data into a portable string.
// This is a convenience function for memory-based adapters.
func ExportSessionString(sess *Session) (string, error) {
	if len(sess.AuthKey) == 0 {
		return "", nil
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(uint8(sess.DC))
	buf.Write([]byte{0, 0, 0, 0})
	_ = binary.Write(buf, binary.BigEndian, uint16(443))
	buf.Write(sess.AuthKey)
	return "1" + base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}
