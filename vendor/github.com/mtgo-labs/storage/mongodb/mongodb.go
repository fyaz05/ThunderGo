// Package mongodb provides a storage adapter backed by a MongoDB database.
//
// It implements [storage.Adapter] and [storage.ConversationStore]. Collections
// and indexes are created implicitly on first use.
//
// Basic usage:
//
//	store, err := mongodb.Open(ctx, mongodb.Config{
//	    URI:      "mongodb://localhost:27017",
//	    Database: "mtgo",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer store.Close()
package mongodb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mtgo-labs/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoDB is a storage adapter backed by a MongoDB database.
type MongoDB struct {
	client   *mongo.Client
	db       *mongo.Database
	cfg      Config
	initOnce sync.Once
	initErr  error

	convOnce  sync.Once
	convReady bool
}

var (
	_ storage.Adapter           = (*MongoDB)(nil)
	_ storage.ConversationStore = (*MongoDB)(nil)
	_ storage.UpdateStateStore  = (*MongoDB)(nil)
)

// Config holds MongoDB connection parameters.
type Config struct {
	URI      string
	Database string
}

// Open connects to a MongoDB cluster and verifies connectivity.
// Collections are created implicitly on first use.
func Open(ctx context.Context, cfg Config) (*MongoDB, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return nil, err
	}
	db := client.Database(cfg.Database)

	// Create indexes for peer lookups.
	_, _ = db.Collection("peers").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "username", Value: 1}},
	})
	_, _ = db.Collection("conversations").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "chat_id", Value: 1}, {Key: "user_id", Value: 1}},
	})
	_, _ = db.Collection("channel_update_state").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "session_id", Value: 1}, {Key: "channel_id", Value: 1}},
	})
	_, _ = db.Collection("update_dedup").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "session_id", Value: 1}, {Key: "dedup_key", Value: 1}},
	})

	return &MongoDB{client: client, db: db}, nil
}

func New(cfg Config) storage.Storage {
	return storage.NewAdapter(&MongoDB{cfg: cfg})
}

func (m *MongoDB) init() error {
	m.initOnce.Do(func() {
		ctx := context.Background()
		client, err := mongo.Connect(options.Client().ApplyURI(m.cfg.URI))
		if err != nil {
			m.initErr = err
			return
		}
		if err := client.Ping(ctx, nil); err != nil {
			client.Disconnect(ctx)
			m.initErr = err
			return
		}
		db := client.Database(m.cfg.Database)
		_, _ = db.Collection("peers").Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: "username", Value: 1}},
		})
		_, _ = db.Collection("conversations").Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: "chat_id", Value: 1}, {Key: "user_id", Value: 1}},
		})
		_, _ = db.Collection("channel_update_state").Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: "session_id", Value: 1}, {Key: "channel_id", Value: 1}},
		})
		_, _ = db.Collection("update_dedup").Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: "session_id", Value: 1}, {Key: "dedup_key", Value: 1}},
		})
		m.client = client
		m.db = db
	})
	return m.initErr
}

func (m *MongoDB) ensureConvColl() {
	m.convOnce.Do(func() {
		m.convReady = true
	})
}

// --- SessionStore ---

func (m *MongoDB) LoadSession() (*storage.Session, error) {
	if err := m.init(); err != nil {
		return nil, err
	}
	coll := m.db.Collection("sessions")
	var doc struct {
		DC        int    `bson:"dc"`
		APIID     int32  `bson:"api_id"`
		APIHash   string `bson:"api_hash"`
		TestMode  int    `bson:"test_mode"`
		AuthKey   []byte `bson:"auth_key"`
		State     []byte `bson:"state"`
		UserID    int64  `bson:"user_id"`
		IsBot     int    `bson:"is_bot"`
		FirstName string `bson:"first_name"`
		LastName  string `bson:"last_name"`
		Username  string `bson:"username"`
		Date      int    `bson:"date"`
	}
	err := coll.FindOne(context.Background(), bson.M{}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.Session{
		DC: doc.DC, APIID: doc.APIID, APIHash: doc.APIHash, TestMode: doc.TestMode != 0,
		AuthKey: doc.AuthKey, State: doc.State, UserID: doc.UserID, IsBot: doc.IsBot != 0,
		FirstName: doc.FirstName, LastName: doc.LastName, Username: doc.Username,
		Date: doc.Date,
	}, nil
}

func (m *MongoDB) SaveSession(s *storage.Session) error {
	if err := m.init(); err != nil {
		return err
	}
	coll := m.db.Collection("sessions")
	tm := 0
	if s.TestMode {
		tm = 1
	}
	ib := 0
	if s.IsBot {
		ib = 1
	}
	doc := bson.M{
		"dc": s.DC, "api_id": s.APIID, "api_hash": s.APIHash, "test_mode": tm,
		"auth_key": s.AuthKey, "state": s.State, "user_id": s.UserID, "is_bot": ib,
		"first_name": s.FirstName, "last_name": s.LastName, "username": s.Username,
		"date": s.Date,
	}
	_, err := coll.ReplaceOne(context.Background(), bson.M{}, doc, options.Replace().SetUpsert(true))
	return err
}

// --- PeerStore ---

func (m *MongoDB) SavePeer(p *storage.Peer) error {
	if err := m.init(); err != nil {
		return err
	}
	existing, err := m.GetPeer(p.ID)
	if err != nil {
		return err
	}
	p = storage.MergePeer(existing, p)
	coll := m.db.Collection("peers")
	ib := 0
	if p.IsBot {
		ib = 1
	}
	doc := bson.M{
		"type": p.Type, "access_hash": p.AccessHash, "username": p.Username,
		"usernames": p.Usernames, "first_name": p.FirstName, "last_name": p.LastName,
		"phone_number": p.PhoneNumber, "is_bot": ib, "photo_id": p.PhotoID,
		"language": p.Language, "last_updated": p.LastUpdated,
	}
	_, err = coll.ReplaceOne(context.Background(), bson.M{"_id": p.ID}, doc, options.Replace().SetUpsert(true))
	return err
}

func (m *MongoDB) GetPeer(id int64) (*storage.Peer, error) {
	if err := m.init(); err != nil {
		return nil, err
	}
	coll := m.db.Collection("peers")
	var doc struct {
		ID          int64  `bson:"_id"`
		Type        int    `bson:"type"`
		AccessHash  int64  `bson:"access_hash"`
		Username    string `bson:"username"`
		Usernames   string `bson:"usernames"`
		FirstName   string `bson:"first_name"`
		LastName    string `bson:"last_name"`
		PhoneNumber string `bson:"phone_number"`
		IsBot       int    `bson:"is_bot"`
		PhotoID     int64  `bson:"photo_id"`
		Language    string `bson:"language"`
		LastUpdated int64  `bson:"last_updated"`
	}
	err := coll.FindOne(context.Background(), bson.M{"_id": id}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.Peer{
		ID: doc.ID, Type: storage.PeerType(doc.Type), AccessHash: doc.AccessHash,
		Username: doc.Username, Usernames: doc.Usernames, FirstName: doc.FirstName,
		LastName: doc.LastName, PhoneNumber: doc.PhoneNumber, IsBot: doc.IsBot != 0,
		PhotoID: doc.PhotoID, Language: doc.Language, LastUpdated: doc.LastUpdated,
	}, nil
}

func (m *MongoDB) GetPeerByUsername(username string) (*storage.Peer, error) {
	if err := m.init(); err != nil {
		return nil, err
	}
	coll := m.db.Collection("peers")
	var doc struct {
		ID          int64  `bson:"_id"`
		Type        int    `bson:"type"`
		AccessHash  int64  `bson:"access_hash"`
		Username    string `bson:"username"`
		Usernames   string `bson:"usernames"`
		FirstName   string `bson:"first_name"`
		LastName    string `bson:"last_name"`
		PhoneNumber string `bson:"phone_number"`
		IsBot       int    `bson:"is_bot"`
		PhotoID     int64  `bson:"photo_id"`
		Language    string `bson:"language"`
		LastUpdated int64  `bson:"last_updated"`
	}
	err := coll.FindOne(context.Background(), bson.M{"username": username}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.Peer{
		ID: doc.ID, Type: storage.PeerType(doc.Type), AccessHash: doc.AccessHash,
		Username: doc.Username, Usernames: doc.Usernames, FirstName: doc.FirstName,
		LastName: doc.LastName, PhoneNumber: doc.PhoneNumber, IsBot: doc.IsBot != 0,
		PhotoID: doc.PhotoID, Language: doc.Language, LastUpdated: doc.LastUpdated,
	}, nil
}

func (m *MongoDB) LoadPeers() ([]*storage.Peer, error) {
	if err := m.init(); err != nil {
		return nil, err
	}
	coll := m.db.Collection("peers")
	cursor, err := coll.Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	var peers []*storage.Peer
	for cursor.Next(context.Background()) {
		var doc struct {
			ID          int64  `bson:"_id"`
			Type        int    `bson:"type"`
			AccessHash  int64  `bson:"access_hash"`
			Username    string `bson:"username"`
			Usernames   string `bson:"usernames"`
			FirstName   string `bson:"first_name"`
			LastName    string `bson:"last_name"`
			PhoneNumber string `bson:"phone_number"`
			IsBot       int    `bson:"is_bot"`
			PhotoID     int64  `bson:"photo_id"`
			Language    string `bson:"language"`
			LastUpdated int64  `bson:"last_updated"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		peers = append(peers, &storage.Peer{
			ID: doc.ID, Type: storage.PeerType(doc.Type), AccessHash: doc.AccessHash,
			Username: doc.Username, Usernames: doc.Usernames, FirstName: doc.FirstName,
			LastName: doc.LastName, PhoneNumber: doc.PhoneNumber, IsBot: doc.IsBot != 0,
			PhotoID: doc.PhotoID, Language: doc.Language, LastUpdated: doc.LastUpdated,
		})
	}
	return peers, nil
}

func (m *MongoDB) DeletePeer(id int64) error {
	if err := m.init(); err != nil {
		return err
	}
	coll := m.db.Collection("peers")
	_, err := coll.DeleteOne(context.Background(), bson.M{"_id": id})
	return err
}

// --- ConversationStore (lazy) ---

func (m *MongoDB) SaveConversation(c *storage.Conversation) error {
	if err := m.init(); err != nil {
		return err
	}
	m.ensureConvColl()
	coll := m.db.Collection("conversations")
	now := c.UpdatedAt
	if now == 0 {
		now = time.Now().Unix()
	}
	createdAt := c.CreatedAt
	if createdAt == 0 {
		createdAt = now
	}
	doc := bson.M{
		"name": c.Name, "step": c.Step, "data": c.Data,
		"created_at": createdAt, "updated_at": now,
	}
	filter := bson.M{"chat_id": c.ChatID, "user_id": c.UserID}
	_, err := coll.ReplaceOne(context.Background(), filter, doc, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("mongodb save conversation: %w", err)
	}
	return nil
}

func (m *MongoDB) LoadConversation(chatID, userID int64) (*storage.Conversation, error) {
	if err := m.init(); err != nil {
		return nil, err
	}
	m.ensureConvColl()
	coll := m.db.Collection("conversations")
	var doc struct {
		ChatID    int64  `bson:"chat_id"`
		UserID    int64  `bson:"user_id"`
		Name      string `bson:"name"`
		Step      int    `bson:"step"`
		Data      []byte `bson:"data"`
		CreatedAt int64  `bson:"created_at"`
		UpdatedAt int64  `bson:"updated_at"`
	}
	err := coll.FindOne(context.Background(), bson.M{"chat_id": chatID, "user_id": userID}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.Conversation{
		ChatID: doc.ChatID, UserID: doc.UserID, Name: doc.Name,
		Step: doc.Step, Data: doc.Data, CreatedAt: doc.CreatedAt, UpdatedAt: doc.UpdatedAt,
	}, nil
}

func (m *MongoDB) DeleteConversation(chatID, userID int64) error {
	if err := m.init(); err != nil {
		return err
	}
	m.ensureConvColl()
	coll := m.db.Collection("conversations")
	_, err := coll.DeleteOne(context.Background(), bson.M{"chat_id": chatID, "user_id": userID})
	return err
}

// --- UpdateStateStore ---

func (m *MongoDB) LoadUpdateState(sessionID string) (*storage.UpdateState, error) {
	var doc struct {
		SessionID string `bson:"session_id"`
		Pts       int32  `bson:"pts"`
		Qts       int32  `bson:"qts"`
		Date      int32  `bson:"date"`
		Seq       int32  `bson:"seq"`
	}
	err := m.db.Collection("update_state").FindOne(context.Background(), bson.M{"session_id": sessionID}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.UpdateState{SessionID: doc.SessionID, Pts: doc.Pts, Qts: doc.Qts, Date: doc.Date, Seq: doc.Seq}, nil
}

func (m *MongoDB) SaveUpdateState(s *storage.UpdateState) error {
	doc := bson.M{"pts": s.Pts, "qts": s.Qts, "date": s.Date, "seq": s.Seq, "updated_at": time.Now().Unix()}
	_, err := m.db.Collection("update_state").ReplaceOne(context.Background(), bson.M{"session_id": s.SessionID}, doc, options.Replace().SetUpsert(true))
	return err
}

func (m *MongoDB) LoadChannelUpdateState(sessionID string, channelID int64) (*storage.ChannelUpdateState, error) {
	var doc struct {
		SessionID string `bson:"session_id"`
		ChannelID int64  `bson:"channel_id"`
		Pts       int32  `bson:"pts"`
	}
	err := m.db.Collection("channel_update_state").FindOne(context.Background(), bson.M{"session_id": sessionID, "channel_id": channelID}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storage.ChannelUpdateState{SessionID: doc.SessionID, ChannelID: doc.ChannelID, Pts: doc.Pts}, nil
}

func (m *MongoDB) LoadAllChannelUpdateStates(sessionID string) ([]*storage.ChannelUpdateState, error) {
	cursor, err := m.db.Collection("channel_update_state").Find(context.Background(), bson.M{"session_id": sessionID})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	var out []*storage.ChannelUpdateState
	for cursor.Next(context.Background()) {
		var doc struct {
			SessionID string `bson:"session_id"`
			ChannelID int64  `bson:"channel_id"`
			Pts       int32  `bson:"pts"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		out = append(out, &storage.ChannelUpdateState{SessionID: doc.SessionID, ChannelID: doc.ChannelID, Pts: doc.Pts})
	}
	return out, nil
}

func (m *MongoDB) SaveChannelUpdateState(s *storage.ChannelUpdateState) error {
	doc := bson.M{"pts": s.Pts, "updated_at": time.Now().Unix()}
	_, err := m.db.Collection("channel_update_state").ReplaceOne(context.Background(), bson.M{"session_id": s.SessionID, "channel_id": s.ChannelID}, doc, options.Replace().SetUpsert(true))
	return err
}

func (m *MongoDB) SaveUpdateDedupKey(sessionID string, key string) (bool, error) {
	doc := bson.M{"created_at": time.Now().Unix()}
	res, err := m.db.Collection("update_dedup").ReplaceOne(context.Background(), bson.M{"session_id": sessionID, "dedup_key": key}, doc, options.Replace().SetUpsert(true))
	if err != nil {
		return false, err
	}
	return res.UpsertedCount > 0, nil
}

func (m *MongoDB) UpdateDedupKeyExists(sessionID string, key string) (bool, error) {
	count, err := m.db.Collection("update_dedup").CountDocuments(context.Background(), bson.M{"session_id": sessionID, "dedup_key": key})
	return count > 0, err
}

func (m *MongoDB) EnqueueDurableUpdate(u *storage.DurableUpdate) error {
	doc := bson.M{"payload": u.Payload, "attempts": u.Attempts, "last_error": u.LastError, "created_at": u.CreatedAt, "updated_at": time.Now().Unix()}
	_, err := m.db.Collection("durable_updates").ReplaceOne(context.Background(), bson.M{"session_id": u.SessionID, "id": u.ID}, doc, options.Replace().SetUpsert(true))
	return err
}

func (m *MongoDB) DeleteDurableUpdate(sessionID string, id string) error {
	_, err := m.db.Collection("durable_updates").DeleteOne(context.Background(), bson.M{"session_id": sessionID, "id": id})
	return err
}

func (m *MongoDB) LoadDurableUpdates(sessionID string, limit int) ([]*storage.DurableUpdate, error) {
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}}).SetLimit(int64(limit))
	cursor, err := m.db.Collection("durable_updates").Find(context.Background(), bson.M{"session_id": sessionID}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	var out []*storage.DurableUpdate
	for cursor.Next(context.Background()) {
		var doc struct {
			SessionID string `bson:"session_id"`
			ID        string `bson:"id"`
			Payload   []byte `bson:"payload"`
			Attempts  int    `bson:"attempts"`
			LastError string `bson:"last_error"`
			CreatedAt int64  `bson:"created_at"`
			UpdatedAt int64  `bson:"updated_at"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		out = append(out, &storage.DurableUpdate{
			SessionID: doc.SessionID, ID: doc.ID, Payload: doc.Payload,
			Attempts: doc.Attempts, LastError: doc.LastError,
			CreatedAt: doc.CreatedAt, UpdatedAt: doc.UpdatedAt,
		})
	}
	return out, nil
}

func (m *MongoDB) MarkDurableUpdateFailed(sessionID string, id string, attempts int, lastErr string) error {
	_, err := m.db.Collection("durable_updates").UpdateOne(context.Background(),
		bson.M{"session_id": sessionID, "id": id},
		bson.M{"$set": bson.M{"attempts": attempts, "last_error": lastErr, "updated_at": time.Now().Unix()}})
	return err
}

// --- Close ---

func (m *MongoDB) Close() error {
	return m.client.Disconnect(context.Background())
}
