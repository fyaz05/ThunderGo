package storage

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
)

// adapterWrapper wraps an [Adapter] to satisfy the [Storage] and [PeerCache]
// interfaces used by the Telegram client.
//
// Call [NewAdapter] to create one:
//
//	store, _ := sqlite.Open("bot.db")
//	client, _ := tg.NewClient(apiID, apiHash, &tg.Config{
//	    Storage: storage.NewAdapter(store),
//	})
type adapterWrapper struct {
	ext   Adapter
	sess  *Session
	dirty bool
}

// NewAdapter wraps a [github.com/mtgo-labs/storage.Adapter] so it can be used
// as Config.Storage in the Telegram client.
func NewAdapter(a Adapter) *adapterWrapper {
	return &adapterWrapper{ext: a}
}

func (a *adapterWrapper) load() error {
	if a.sess != nil {
		return nil
	}
	s, err := a.ext.LoadSession()
	if err != nil {
		return err
	}
	if s == nil {
		s = &Session{}
	}
	a.sess = s
	return nil
}

func (a *adapterWrapper) flush() error {
	if !a.dirty || a.sess == nil {
		return nil
	}
	if err := a.ext.SaveSession(a.sess); err != nil {
		return err
	}
	a.dirty = false
	return nil
}

// Sync flushes any pending session changes to the underlying adapter.
// Call this after critical lifecycle operations (auth key creation, login)
// to ensure session data survives process crashes.
func (a *adapterWrapper) Sync() error {
	return a.flush()
}

func (a *adapterWrapper) SessionID() (string, error) {
	if err := a.load(); err != nil {
		return "", err
	}
	return a.sess.SessionID, nil
}

func (a *adapterWrapper) SetSessionID(v string) error {
	if sa, ok := a.ext.(SessionIDAware); ok {
		sa.SetSessionName(v)
	}
	a.sess = nil
	if err := a.load(); err != nil {
		return err
	}
	a.sess.SessionID = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) DCID() (int, error) {
	if err := a.load(); err != nil {
		return 0, err
	}
	return a.sess.DC, nil
}
func (a *adapterWrapper) SetDCID(v int) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.DC = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) APIID() (int32, error) {
	if err := a.load(); err != nil {
		return 0, err
	}
	return a.sess.APIID, nil
}
func (a *adapterWrapper) SetAPIID(v int32) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.APIID = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) APIHash() (string, error) {
	if err := a.load(); err != nil {
		return "", err
	}
	return a.sess.APIHash, nil
}
func (a *adapterWrapper) SetAPIHash(v string) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.APIHash = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) TestMode() (bool, error) {
	if err := a.load(); err != nil {
		return false, err
	}
	return a.sess.TestMode, nil
}
func (a *adapterWrapper) SetTestMode(v bool) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.TestMode = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) AuthKey() ([]byte, error) {
	if err := a.load(); err != nil {
		return nil, err
	}
	return a.sess.AuthKey, nil
}
func (a *adapterWrapper) SetAuthKey(v []byte) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.AuthKey = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) UserID() (int64, error) {
	if err := a.load(); err != nil {
		return 0, err
	}
	return a.sess.UserID, nil
}
func (a *adapterWrapper) SetUserID(v int64) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.UserID = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) IsBot() (bool, error) {
	if err := a.load(); err != nil {
		return false, err
	}
	return a.sess.IsBot, nil
}
func (a *adapterWrapper) SetIsBot(v bool) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.IsBot = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) FirstName() (string, error) {
	if err := a.load(); err != nil {
		return "", err
	}
	return a.sess.FirstName, nil
}
func (a *adapterWrapper) SetFirstName(v string) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.FirstName = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) LastName() (string, error) {
	if err := a.load(); err != nil {
		return "", err
	}
	return a.sess.LastName, nil
}
func (a *adapterWrapper) SetLastName(v string) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.LastName = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) Username() (string, error) {
	if err := a.load(); err != nil {
		return "", err
	}
	return a.sess.Username, nil
}
func (a *adapterWrapper) SetUsername(v string) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.Username = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) Date() (int, error) {
	if err := a.load(); err != nil {
		return 0, err
	}
	return a.sess.Date, nil
}
func (a *adapterWrapper) SetDate(v int) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.Date = v
	a.dirty = true
	return nil
}
func (a *adapterWrapper) State() ([]byte, error) {
	if err := a.load(); err != nil {
		return nil, err
	}
	return a.sess.State, nil
}
func (a *adapterWrapper) SetState(v []byte) error {
	if err := a.load(); err != nil {
		return err
	}
	a.sess.State = v
	a.dirty = true
	return nil
}

func (a *adapterWrapper) ExportSessionString() (string, error) {
	if err := a.flush(); err != nil {
		return "", err
	}
	if err := a.load(); err != nil {
		return "", err
	}
	if len(a.sess.AuthKey) == 0 {
		return "", nil
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(uint8(a.sess.DC))
	buf.Write([]byte{0, 0, 0, 0})
	_ = binary.Write(buf, binary.BigEndian, uint16(443))
	buf.Write(a.sess.AuthKey)
	return "1" + base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}

func (a *adapterWrapper) Close() error {
	if err := a.flush(); err != nil {
		return err
	}
	return a.ext.Close()
}

func (a *adapterWrapper) SavePeer(p *Peer) error {
	return a.ext.SavePeer(p)
}

func (a *adapterWrapper) SavePeers(peers []*Peer) error {
	for _, p := range peers {
		if err := a.ext.SavePeer(p); err != nil {
			return err
		}
	}
	return nil
}

func (a *adapterWrapper) LoadPeers() ([]*Peer, error) {
	return a.ext.LoadPeers()
}

func (a *adapterWrapper) DeletePeer(id int64) error {
	return a.ext.DeletePeer(id)
}

func (a *adapterWrapper) GetPeer(id int64) (*Peer, error) {
	return a.ext.GetPeer(id)
}

func (a *adapterWrapper) GetPeerByUsername(username string) (*Peer, error) {
	return a.ext.GetPeerByUsername(username)
}

func (a *adapterWrapper) conversationStore() ConversationStore {
	if s, ok := a.ext.(ConversationStore); ok {
		return s
	}
	return nil
}

func (a *adapterWrapper) SaveConversation(conv *Conversation) error {
	if s := a.conversationStore(); s != nil {
		return s.SaveConversation(conv)
	}
	return nil
}

func (a *adapterWrapper) LoadConversation(chatID, userID int64) (*Conversation, error) {
	if s := a.conversationStore(); s != nil {
		return s.LoadConversation(chatID, userID)
	}
	return nil, nil
}

func (a *adapterWrapper) DeleteConversation(chatID, userID int64) error {
	if s := a.conversationStore(); s != nil {
		return s.DeleteConversation(chatID, userID)
	}
	return nil
}

func (a *adapterWrapper) stateStore() StateStore {
	if s, ok := a.ext.(StateStore); ok {
		return s
	}
	return nil
}

func (a *adapterWrapper) SaveState(entry *StateEntry) error {
	if s := a.stateStore(); s != nil {
		return s.SaveState(entry)
	}
	return nil
}

func (a *adapterWrapper) LoadState(scope int, chatID, userID int64, key string) (*StateEntry, error) {
	if s := a.stateStore(); s != nil {
		return s.LoadState(scope, chatID, userID, key)
	}
	return nil, nil
}

func (a *adapterWrapper) DeleteState(scope int, chatID, userID int64, key string) error {
	if s := a.stateStore(); s != nil {
		return s.DeleteState(scope, chatID, userID, key)
	}
	return nil
}

func (a *adapterWrapper) ClearState(scope int, chatID, userID int64) error {
	if s := a.stateStore(); s != nil {
		return s.ClearState(scope, chatID, userID)
	}
	return nil
}

func (a *adapterWrapper) updateStateStore() UpdateStateStore {
	if s, ok := a.ext.(UpdateStateStore); ok {
		return s
	}
	return nil
}

func (a *adapterWrapper) LoadUpdateState(sessionID string) (*UpdateState, error) {
	if s := a.updateStateStore(); s != nil {
		return s.LoadUpdateState(sessionID)
	}
	return nil, nil
}

func (a *adapterWrapper) SaveUpdateState(state *UpdateState) error {
	if s := a.updateStateStore(); s != nil {
		return s.SaveUpdateState(state)
	}
	return nil
}

func (a *adapterWrapper) LoadChannelUpdateState(sessionID string, channelID int64) (*ChannelUpdateState, error) {
	if s := a.updateStateStore(); s != nil {
		return s.LoadChannelUpdateState(sessionID, channelID)
	}
	return nil, nil
}

func (a *adapterWrapper) LoadAllChannelUpdateStates(sessionID string) ([]*ChannelUpdateState, error) {
	if s := a.updateStateStore(); s != nil {
		return s.LoadAllChannelUpdateStates(sessionID)
	}
	return nil, nil
}

func (a *adapterWrapper) SaveChannelUpdateState(state *ChannelUpdateState) error {
	if s := a.updateStateStore(); s != nil {
		return s.SaveChannelUpdateState(state)
	}
	return nil
}

func (a *adapterWrapper) SaveUpdateDedupKey(sessionID string, key string) (bool, error) {
	if s := a.updateStateStore(); s != nil {
		return s.SaveUpdateDedupKey(sessionID, key)
	}
	return true, nil
}

func (a *adapterWrapper) UpdateDedupKeyExists(sessionID string, key string) (bool, error) {
	if s := a.updateStateStore(); s != nil {
		return s.UpdateDedupKeyExists(sessionID, key)
	}
	return false, nil
}

func (a *adapterWrapper) EnqueueDurableUpdate(update *DurableUpdate) error {
	if s := a.updateStateStore(); s != nil {
		return s.EnqueueDurableUpdate(update)
	}
	return nil
}

func (a *adapterWrapper) DeleteDurableUpdate(sessionID string, id string) error {
	if s := a.updateStateStore(); s != nil {
		return s.DeleteDurableUpdate(sessionID, id)
	}
	return nil
}

func (a *adapterWrapper) LoadDurableUpdates(sessionID string, limit int) ([]*DurableUpdate, error) {
	if s := a.updateStateStore(); s != nil {
		return s.LoadDurableUpdates(sessionID, limit)
	}
	return nil, nil
}

func (a *adapterWrapper) MarkDurableUpdateFailed(sessionID string, id string, attempts int, lastErr string) error {
	if s := a.updateStateStore(); s != nil {
		return s.MarkDurableUpdateFailed(sessionID, id, attempts, lastErr)
	}
	return nil
}

func (a *adapterWrapper) deviceStore() DeviceStore {
	if s, ok := a.ext.(DeviceStore); ok {
		return s
	}
	return nil
}

func (a *adapterWrapper) SaveDevice(entry *DeviceEntry) error {
	if s := a.deviceStore(); s != nil {
		return s.SaveDevice(entry)
	}
	return nil
}

func (a *adapterWrapper) LoadDevice(key string) (*DeviceEntry, error) {
	if s := a.deviceStore(); s != nil {
		return s.LoadDevice(key)
	}
	return nil, nil
}

func (a *adapterWrapper) DeleteDevice(key string) error {
	if s := a.deviceStore(); s != nil {
		return s.DeleteDevice(key)
	}
	return nil
}
