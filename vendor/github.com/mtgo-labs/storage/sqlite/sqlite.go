//go:build !js

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mtgo-labs/storage"
	_ "modernc.org/sqlite"
)

type SQLite struct {
	db        *sql.DB
	q         *Queries
	path      string
	initOnce  sync.Once
	initErr   error
	convOnce  sync.Once
	convReady bool
}

var (
	_ storage.Adapter           = (*SQLite)(nil)
	_ storage.ConversationStore = (*SQLite)(nil)
	_ storage.UpdateStateStore  = (*SQLite)(nil)
	_ storage.SessionIDAware    = (*SQLite)(nil)
)

func Open(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLite{db: db, q: NewSqlcQueries(), path: path}, nil
}

func New(path ...string) storage.Storage {
	p := ""
	if len(path) > 0 {
		p = path[0]
	}
	return storage.NewAdapter(&SQLite{path: p, q: NewSqlcQueries()})
}

func (a *SQLite) SetSessionName(name string) {
	if a.path == "" {
		a.path = name
	}
}

func (a *SQLite) init() error {
	a.initOnce.Do(func() {
		db, err := sql.Open("sqlite", a.path)
		if err != nil {
			a.initErr = fmt.Errorf("sql.Open(%q): %w", a.path, err)
			return
		}
		if err := initSchema(db); err != nil {
			db.Close()
			a.initErr = fmt.Errorf("initSchema(%q): %w", a.path, err)
			return
		}
		a.db = db
	})
	return a.initErr
}

func initSchema(db *sql.DB) error {
	pragmas := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
		`PRAGMA foreign_keys=ON`,
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("sqlite pragma: %w", err)
		}
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS sessions (
			session_id     TEXT PRIMARY KEY,
			dc             INTEGER DEFAULT 0,
			api_id         INTEGER DEFAULT 0,
			api_hash       TEXT DEFAULT '',
			test_mode      INTEGER DEFAULT 0,
			auth_key       BLOB,
			state          BLOB,
			user_id        INTEGER DEFAULT 0,
			is_bot         INTEGER DEFAULT 0,
			first_name     TEXT DEFAULT '',
			last_name      TEXT DEFAULT '',
			username       TEXT DEFAULT '',
			date           INTEGER DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS peers (
			id           INTEGER PRIMARY KEY,
			type         INTEGER NOT NULL,
			access_hash  INTEGER DEFAULT 0,
			username     TEXT DEFAULT '',
			usernames    TEXT DEFAULT '',
			first_name   TEXT DEFAULT '',
			last_name    TEXT DEFAULT '',
			phone_number TEXT DEFAULT '',
			is_bot       INTEGER DEFAULT 0,
			photo_id     INTEGER DEFAULT 0,
			language     TEXT DEFAULT '',
			last_updated INTEGER DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_peers_username ON peers(username)`,
		`CREATE INDEX IF NOT EXISTS idx_peers_phone ON peers(phone_number)`,
		`CREATE TABLE IF NOT EXISTS update_state (
			session_id TEXT PRIMARY KEY,
			pts INTEGER NOT NULL DEFAULT 0,
			qts INTEGER NOT NULL DEFAULT 0,
			date INTEGER NOT NULL DEFAULT 0,
			seq INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS channel_update_state (
			session_id TEXT NOT NULL,
			channel_id INTEGER NOT NULL,
			pts INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (session_id, channel_id)
		)`,
		`CREATE TABLE IF NOT EXISTS update_dedup (
			session_id TEXT NOT NULL,
			dedup_key TEXT NOT NULL,
			created_at INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (session_id, dedup_key)
		)`,
		`CREATE TABLE IF NOT EXISTS durable_updates (
			session_id TEXT NOT NULL,
			id TEXT NOT NULL,
			payload BLOB NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (session_id, id)
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("sqlite init: %w", err)
		}
	}
	return nil
}

func (a *SQLite) ensureConvTable() error {
	if err := a.init(); err != nil {
		return err
	}
	if a.convReady {
		return nil
	}
	_, err := a.db.Exec(`CREATE TABLE IF NOT EXISTS conversations (
		chat_id    INTEGER NOT NULL,
		user_id    INTEGER NOT NULL,
		name       TEXT    NOT NULL,
		step       INTEGER DEFAULT 0,
		data       BLOB,
		created_at INTEGER DEFAULT 0,
		updated_at INTEGER DEFAULT 0,
		PRIMARY KEY (chat_id, user_id)
	)`)
	if err == nil {
		a.convReady = true
	}
	return err
}

func (a *SQLite) Close() error {
	if err := a.init(); err != nil {
		return err
	}
	if _, err := a.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		a.db.Close()
		return err
	}
	if err := a.db.Close(); err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(a.path + suffix); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// --- SessionStore ---

func (a *SQLite) LoadSession() (*storage.Session, error) {
	if err := a.init(); err != nil {
		return nil, err
	}
	row, err := a.q.GetSession(context.Background(), a.db)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.Session{
		SessionID: row.SessionID, DC: int(row.Dc), APIID: int32(row.ApiID), APIHash: row.ApiHash,
		TestMode: row.TestMode != 0, AuthKey: row.AuthKey, State: row.State,
		UserID: row.UserID, IsBot: row.IsBot != 0,
		FirstName: row.FirstName, LastName: row.LastName, Username: row.Username,
		Date: int(row.Date),
	}, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (a *SQLite) SaveSession(s *storage.Session) error {
	if err := a.init(); err != nil {
		return err
	}
	ctx := context.Background()
	if err := a.q.DeleteAllSessions(ctx, a.db); err != nil {
		return err
	}
	return a.q.UpsertSession(ctx, a.db, UpsertSessionParams{
		SessionID: s.SessionID, ApiID: int64(s.APIID),
		ApiHash: s.APIHash, TestMode: boolToInt(s.TestMode),
		AuthKey: s.AuthKey, State: s.State, UserID: s.UserID,
		IsBot: boolToInt(s.IsBot), FirstName: s.FirstName, LastName: s.LastName,
		Username: s.Username, Date: int64(s.Date), Dc: int64(s.DC),
	})
}

// --- PeerStore ---

func (a *SQLite) SavePeer(p *storage.Peer) error {
	if err := a.init(); err != nil {
		return err
	}
	existing, err := a.GetPeer(p.ID)
	if err != nil {
		return err
	}
	p = storage.MergePeer(existing, p)
	return a.q.UpsertPeer(context.Background(), a.db, UpsertPeerParams{
		ID: p.ID, Type: int64(p.Type), AccessHash: p.AccessHash,
		Username: p.Username, Usernames: p.Usernames, FirstName: p.FirstName,
		LastName: p.LastName, PhoneNumber: p.PhoneNumber, IsBot: boolToInt(p.IsBot),
		PhotoID: p.PhotoID, Language: p.Language, LastUpdated: p.LastUpdated,
	})
}

func (a *SQLite) GetPeer(id int64) (*storage.Peer, error) {
	if err := a.init(); err != nil {
		return nil, err
	}
	row, err := a.q.GetPeer(context.Background(), a.db, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.Peer{
		ID: row.ID, Type: storage.PeerType(row.Type), AccessHash: row.AccessHash,
		Username: row.Username, Usernames: row.Usernames, FirstName: row.FirstName,
		LastName: row.LastName, PhoneNumber: row.PhoneNumber, IsBot: row.IsBot != 0,
		PhotoID: row.PhotoID, Language: row.Language, LastUpdated: row.LastUpdated,
	}, nil
}

func (a *SQLite) GetPeerByUsername(username string) (*storage.Peer, error) {
	if err := a.init(); err != nil {
		return nil, err
	}
	row, err := a.q.GetPeerByUsername(context.Background(), a.db, username)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.Peer{
		ID: row.ID, Type: storage.PeerType(row.Type), AccessHash: row.AccessHash,
		Username: row.Username, Usernames: row.Usernames, FirstName: row.FirstName,
		LastName: row.LastName, PhoneNumber: row.PhoneNumber, IsBot: row.IsBot != 0,
		PhotoID: row.PhotoID, Language: row.Language, LastUpdated: row.LastUpdated,
	}, nil
}

func (a *SQLite) LoadPeers() ([]*storage.Peer, error) {
	if err := a.init(); err != nil {
		return nil, err
	}
	rows, err := a.q.ListPeers(context.Background(), a.db)
	if err != nil {
		return nil, err
	}
	out := make([]*storage.Peer, len(rows))
	for i, r := range rows {
		out[i] = &storage.Peer{
			ID: r.ID, Type: storage.PeerType(r.Type), AccessHash: r.AccessHash,
			Username: r.Username, Usernames: r.Usernames, FirstName: r.FirstName,
			LastName: r.LastName, PhoneNumber: r.PhoneNumber, IsBot: r.IsBot != 0,
			PhotoID: r.PhotoID, Language: r.Language, LastUpdated: r.LastUpdated,
		}
	}
	return out, nil
}

func (a *SQLite) DeletePeer(id int64) error {
	if err := a.init(); err != nil {
		return err
	}
	return a.q.DeletePeer(context.Background(), a.db, id)
}

// --- ConversationStore (lazy) ---

func (a *SQLite) SaveConversation(c *storage.Conversation) error {
	if err := a.ensureConvTable(); err != nil {
		return err
	}
	now := c.UpdatedAt
	if now == 0 {
		now = time.Now().Unix()
	}
	createdAt := c.CreatedAt
	if createdAt == 0 {
		createdAt = now
	}
	return a.q.UpsertConversation(context.Background(), a.db, UpsertConversationParams{
		ChatID: c.ChatID, UserID: c.UserID, Name: c.Name, Step: int64(c.Step),
		Data: c.Data, CreatedAt: createdAt, UpdatedAt: now,
	})
}

func (a *SQLite) LoadConversation(chatID, userID int64) (*storage.Conversation, error) {
	if err := a.ensureConvTable(); err != nil {
		return nil, err
	}
	row, err := a.q.GetConversation(context.Background(), a.db, GetConversationParams{
		ChatID: chatID, UserID: userID,
	})
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.Conversation{
		ChatID: row.ChatID, UserID: row.UserID, Name: row.Name,
		Step: int(row.Step), Data: row.Data, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
	}, nil
}

func (a *SQLite) DeleteConversation(chatID, userID int64) error {
	if err := a.ensureConvTable(); err != nil {
		return err
	}
	return a.q.DeleteConversation(context.Background(), a.db, DeleteConversationParams{
		ChatID: chatID, UserID: userID,
	})
}

// --- UpdateStateStore ---

func (a *SQLite) LoadUpdateState(sessionID string) (*storage.UpdateState, error) {
	if err := a.init(); err != nil {
		return nil, err
	}
	row, err := a.q.GetUpdateState(context.Background(), a.db, sessionID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.UpdateState{
		SessionID: row.SessionID, Pts: int32(row.Pts), Qts: int32(row.Qts),
		Date: int32(row.Date), Seq: int32(row.Seq),
	}, nil
}

func (a *SQLite) SaveUpdateState(s *storage.UpdateState) error {
	if err := a.init(); err != nil {
		return err
	}
	return a.q.UpsertUpdateState(context.Background(), a.db, UpsertUpdateStateParams{
		SessionID: s.SessionID, Pts: int64(s.Pts), Qts: int64(s.Qts),
		Date: int64(s.Date), Seq: int64(s.Seq), UpdatedAt: time.Now().Unix(),
	})
}

func (a *SQLite) LoadChannelUpdateState(sessionID string, channelID int64) (*storage.ChannelUpdateState, error) {
	if err := a.init(); err != nil {
		return nil, err
	}
	row, err := a.q.GetChannelUpdateState(context.Background(), a.db, GetChannelUpdateStateParams{
		SessionID: sessionID, ChannelID: channelID,
	})
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.ChannelUpdateState{
		SessionID: row.SessionID, ChannelID: row.ChannelID, Pts: int32(row.Pts),
	}, nil
}

func (a *SQLite) LoadAllChannelUpdateStates(sessionID string) ([]*storage.ChannelUpdateState, error) {
	if err := a.init(); err != nil {
		return nil, err
	}
	rows, err := a.q.ListChannelUpdateStates(context.Background(), a.db, sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]*storage.ChannelUpdateState, len(rows))
	for i, r := range rows {
		out[i] = &storage.ChannelUpdateState{
			SessionID: r.SessionID, ChannelID: r.ChannelID, Pts: int32(r.Pts),
		}
	}
	return out, nil
}

func (a *SQLite) SaveChannelUpdateState(s *storage.ChannelUpdateState) error {
	if err := a.init(); err != nil {
		return err
	}
	return a.q.UpsertChannelUpdateState(context.Background(), a.db, UpsertChannelUpdateStateParams{
		SessionID: s.SessionID, ChannelID: s.ChannelID, Pts: int64(s.Pts),
		UpdatedAt: time.Now().Unix(),
	})
}

func (a *SQLite) SaveUpdateDedupKey(sessionID string, key string) (bool, error) {
	if err := a.init(); err != nil {
		return false, err
	}
	n, err := a.q.InsertDedupKey(context.Background(), a.db, InsertDedupKeyParams{
		SessionID: sessionID, DedupKey: key, CreatedAt: time.Now().Unix(),
	})
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (a *SQLite) UpdateDedupKeyExists(sessionID string, key string) (bool, error) {
	if err := a.init(); err != nil {
		return false, err
	}
	count, err := a.q.ExistsDedupKey(context.Background(), a.db, ExistsDedupKeyParams{
		SessionID: sessionID, DedupKey: key,
	})
	return count > 0, err
}

func (a *SQLite) EnqueueDurableUpdate(u *storage.DurableUpdate) error {
	if err := a.init(); err != nil {
		return err
	}
	return a.q.UpsertDurableUpdate(context.Background(), a.db, UpsertDurableUpdateParams{
		SessionID: u.SessionID, ID: u.ID, Payload: u.Payload,
		Attempts: int64(u.Attempts), LastError: u.LastError,
		CreatedAt: u.CreatedAt, UpdatedAt: time.Now().Unix(),
	})
}

func (a *SQLite) DeleteDurableUpdate(sessionID string, id string) error {
	if err := a.init(); err != nil {
		return err
	}
	return a.q.DeleteDurableUpdate(context.Background(), a.db, DeleteDurableUpdateParams{
		SessionID: sessionID, ID: id,
	})
}

func (a *SQLite) LoadDurableUpdates(sessionID string, limit int) ([]*storage.DurableUpdate, error) {
	if err := a.init(); err != nil {
		return nil, err
	}
	rows, err := a.q.ListDurableUpdates(context.Background(), a.db, ListDurableUpdatesParams{
		SessionID: sessionID, Limit: int64(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]*storage.DurableUpdate, len(rows))
	for i, r := range rows {
		out[i] = &storage.DurableUpdate{
			SessionID: r.SessionID, ID: r.ID, Payload: r.Payload,
			Attempts: int(r.Attempts), LastError: r.LastError,
			CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
		}
	}
	return out, nil
}

func (a *SQLite) MarkDurableUpdateFailed(sessionID string, id string, attempts int, lastErr string) error {
	if err := a.init(); err != nil {
		return err
	}
	return a.q.MarkDurableUpdateFailed(context.Background(), a.db, MarkDurableUpdateFailedParams{
		Attempts: int64(attempts), LastError: lastErr, UpdatedAt: time.Now().Unix(),
		SessionID: sessionID, ID: id,
	})
}

// --- ExportSessionString ---

// ExportSessionString encodes session data into a portable string using the
// Telethon/Pyrogram format:
//
//	"1" + base64url( dc_id[1B] + ip[4B|16B] + port[2B big-endian] + auth_key[256B] )
func ExportSessionString(s *storage.Session) (string, error) {
	if len(s.AuthKey) == 0 {
		return "", nil
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(uint8(s.DC))
	buf.Write([]byte{0, 0, 0, 0})
	_ = binary.Write(buf, binary.BigEndian, uint16(443))
	buf.Write(s.AuthKey)
	return "1" + base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}
