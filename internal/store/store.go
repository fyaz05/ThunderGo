// Package store is the MongoDB persistence layer with in-memory caches for hot reads.
package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Store wraps a *mongo.Database with typed methods and in-memory caches.
type Store struct {
	client   *mongo.Client
	database *mongo.Database

	users          *mongo.Collection
	bannedUsers    *mongo.Collection
	bannedChannels *mongo.Collection
	files          *mongo.Collection
	authorized     *mongo.Collection
	restart        *mongo.Collection

	// Both collections use TTL indexes on expires_at (absolute expiry).
	activationTokens *mongo.Collection
	activatedUsers   *mongo.Collection

	// Cross-process ingest locks keyed by file_key, reaped by a TTL index.
	// Prevents two processes racing to forward + insert the same file (the
	// unique file_key index only blocks duplicate rows, not duplicate messages).
	fileIngestLocks *mongo.Collection

	// Caches for hot read paths. Writes invalidate (not set) so a concurrent
	// reader cannot overwrite a fresh value with a stale one.
	fileByHash          *cache[FileRecord] // hash → FileRecord (value copy)
	fileByKey           *cache[FileRecord] // file_key → FileRecord (value copy)
	bannedUsersCache    *cache[bool]       // user_id → bool
	bannedChansCache    *cache[bool]       // channel_id → bool
	authUsers           *cache[bool]       // user_id → bool
	activatedUsersCache *cache[bool]       // user_id → bool (5-min TTL)

	// Stop funcs for cache sweepers, called from Close to prevent leaks.
	sweepStops []func()

	// Optional batched seen-count buffer; nil = synchronous fallback.
	touchBuffer atomic.Pointer[TouchBuffer]
}

// New connects to MongoDB, creates indexes, and returns a Store.
// fileTTLDays > 0 adds a TTL index on files.created_at; 0 = never expire.
// The caller must call Close on shutdown.
func New(ctx context.Context, uri string, fileTTLDays int) (*Store, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	db := client.Database("thundergo")
	s := &Store{
		client:              client,
		database:            db,
		users:               db.Collection("users"),
		bannedUsers:         db.Collection("banned_users"),
		bannedChannels:      db.Collection("banned_channels"),
		files:               db.Collection("files"),
		authorized:          db.Collection("authorized"),
		restart:             db.Collection("restart_markers"),
		activationTokens:    db.Collection("activation_tokens"),
		activatedUsers:      db.Collection("activated_users"),
		fileIngestLocks:     db.Collection("file_ingest_locks"),
		fileByHash:          newCache[FileRecord](50000, 30*time.Minute),
		fileByKey:           newCache[FileRecord](50000, 30*time.Minute),
		bannedUsersCache:    newCache[bool](10000, 10*time.Minute),
		bannedChansCache:    newCache[bool](1000, 10*time.Minute),
		authUsers:           newCache[bool](10000, 10*time.Minute),
		activatedUsersCache: newCache[bool](10000, 5*time.Minute),
	}
	// 5-min interval balances reaping vs lock contention.
	s.sweepStops = []func(){
		s.fileByHash.StartSweep(5 * time.Minute),
		s.fileByKey.StartSweep(5 * time.Minute),
		s.bannedUsersCache.StartSweep(5 * time.Minute),
		s.bannedChansCache.StartSweep(5 * time.Minute),
		s.authUsers.StartSweep(5 * time.Minute),
		s.activatedUsersCache.StartSweep(5 * time.Minute),
	}

	if err := s.ensureIndexes(ctx, fileTTLDays); err != nil {
		for _, stop := range s.sweepStops {
			stop()
		}
		_ = client.Disconnect(ctx)
		return nil, err
	}
	return s, nil
}

func (s *Store) Close(ctx context.Context) error {
	for _, stop := range s.sweepStops {
		stop()
	}
	return s.client.Disconnect(ctx)
}

func (s *Store) ensureIndexes(ctx context.Context, fileTTLDays int) error {
	type idx struct {
		coll string
		keys bson.D
		opts *options.IndexOptionsBuilder
	}
	all := []idx{
		{"users", bson.D{{Key: "user_id", Value: 1}}, options.Index().SetUnique(true)},
		{"banned_users", bson.D{{Key: "user_id", Value: 1}}, options.Index().SetUnique(true)},
		{"banned_channels", bson.D{{Key: "channel_id", Value: 1}}, options.Index().SetUnique(true)},
		{"files", bson.D{{Key: "file_key", Value: 1}}, options.Index().SetUnique(true)},
		{"files", bson.D{{Key: "hash", Value: 1}}, options.Index().SetUnique(true)},
		{"files", bson.D{{Key: "vault_msg_id", Value: 1}}, options.Index().SetUnique(true)},
		{"authorized", bson.D{{Key: "user_id", Value: 1}}, options.Index().SetUnique(true)},
		// TTL: restart_markers expire after 1h.
		{"restart_markers", bson.D{{Key: "created_at", Value: 1}}, options.Index().SetExpireAfterSeconds(3600)},
		// Activation tokens: unique on token, TTL on expires_at (absolute expiry).
		{"activation_tokens", bson.D{{Key: "token", Value: 1}}, options.Index().SetUnique(true)},
		{"activation_tokens", bson.D{{Key: "expires_at", Value: 1}}, options.Index().SetExpireAfterSeconds(0)},
		// Activated users: unique on user_id, TTL on expires_at (absolute expiry).
		{"activated_users", bson.D{{Key: "user_id", Value: 1}}, options.Index().SetUnique(true)},
		{"activated_users", bson.D{{Key: "expires_at", Value: 1}}, options.Index().SetExpireAfterSeconds(0)},
		// Ingest locks: TTL on expires_at reaps locks from crashed processes;
		// implicit unique _id makes the lock exclusive.
		{"file_ingest_locks", bson.D{{Key: "expires_at", Value: 1}}, options.Index().SetExpireAfterSeconds(0)},
	}
	// files.created_at TTL: expire N days after ingestion. Cap at 24855 to avoid int32 overflow.
	if fileTTLDays > 0 {
		const maxTTLDays = 24855 // math.MaxInt32 / 86400
		if fileTTLDays > maxTTLDays {
			fileTTLDays = maxTTLDays
		}
		all = append(all, idx{
			"files",
			bson.D{{Key: "created_at", Value: 1}},
			options.Index().SetExpireAfterSeconds(safeExpireSeconds(fileTTLDays)),
		})
	}
	for _, i := range all {
		c := s.database.Collection(i.coll)
		if _, err := c.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: i.keys, Options: i.opts}); err != nil {
			// Code 86 (IndexKeySpecsConflict) is fatal: existing index has wrong spec.
			// Must be checked before the code-85 short-circuit below.
			var ce mongo.CommandError
			if errors.As(err, &ce) && ce.Code == 86 {
				return fmt.Errorf("index %s key spec conflict (code 86): existing index differs from expected; drop and recreate the index: %w", i.coll, err)
			}
			if !isIndexExistsErr(err) {
				return err
			}
			// Code 85 (IndexAlreadyExists) with identical spec is harmless.
		}
	}
	return nil
}

func isIndexExistsErr(err error) bool {
	if err == nil {
		return false
	}
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code == 85 // IndexAlreadyExists
	}
	return false
}

// safeExpireSeconds converts days to seconds as int32 with overflow
// protection. fileTTLDays is bounded to 24855 days (see ensureIndexes), so
// overflow cannot occur in practice — the clamp is a defense-in-depth measure.
func safeExpireSeconds(days int) int32 {
	secs := int64(days) * 86400
	if secs > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(secs) // #nosec G115 — bounded to MaxInt32 above
}

// ----------------------------------------------------------------------------
// User entity
// ----------------------------------------------------------------------------

// User is a person who has started the bot. Lookup by Telegram user ID.
type User struct {
	UserID    int64     `bson:"user_id"`
	FirstName string    `bson:"first_name,omitempty"`
	LastName  string    `bson:"last_name,omitempty"`
	Username  string    `bson:"username,omitempty"`
	JoinedAt  time.Time `bson:"joined_at"`
}

// UpsertUser records a user as having started the bot. Returns true if newly inserted.
func (s *Store) UpsertUser(ctx context.Context, u User) (bool, error) {
	filter := bson.M{"user_id": u.UserID}
	update := bson.M{
		"$set": bson.M{
			"first_name": u.FirstName,
			"last_name":  u.LastName,
			"username":   u.Username,
		},
		"$setOnInsert": bson.M{
			"user_id":   u.UserID,
			"joined_at": u.JoinedAt,
		},
	}
	res, err := s.users.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return false, err
	}
	return res.UpsertedCount > 0, nil
}

// CountUsers returns the total number of users who have started the bot.
func (s *Store) CountUsers(ctx context.Context) (int64, error) {
	return s.users.CountDocuments(ctx, bson.M{})
}

// DeleteUser removes a user. Used by /broadcast to prune unreachable users.
func (s *Store) DeleteUser(ctx context.Context, userID int64) error {
	_, err := s.users.DeleteOne(ctx, bson.M{"user_id": userID})
	return err
}

// HasUser reports whether a user has started the bot.
func (s *Store) HasUser(ctx context.Context, userID int64) (bool, error) {
	count, err := s.users.CountDocuments(ctx, bson.M{"user_id": userID})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// StreamUsers iterates every user via cursor, calling fn for each.
// Streaming avoids materializing the full collection — important for /broadcast at scale.
func (s *Store) StreamUsers(ctx context.Context, fn func(User) error) error {
	// BatchSize=100 balances round-trips vs memory.
	cur, err := s.users.Find(ctx, bson.M{}, options.Find().SetBatchSize(100))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var u User
		if err := cur.Decode(&u); err != nil {
			return err
		}
		if err := fn(u); err != nil {
			return err
		}
	}
	return cur.Err()
}

// ----------------------------------------------------------------------------
// Banned user entity
// ----------------------------------------------------------------------------

type BannedUser struct {
	UserID   int64     `bson:"user_id"`
	BannedBy int64     `bson:"banned_by"`
	BannedAt time.Time `bson:"banned_at"`
	Reason   string    `bson:"reason,omitempty"`
}

func (s *Store) BanUser(ctx context.Context, b BannedUser) error {
	filter := bson.M{"user_id": b.UserID}
	update := bson.M{
		"$set": bson.M{
			"user_id":   b.UserID,
			"banned_by": b.BannedBy,
			"banned_at": b.BannedAt,
			"reason":    b.Reason,
		},
	}
	_, err := s.bannedUsers.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if err == nil {
		// Invalidate (not set): avoids a stale false overwriting a fresh true.
		s.bannedUsersCache.invalidate(strconv.FormatInt(b.UserID, 10))
	}
	return err
}

func (s *Store) UnbanUser(ctx context.Context, userID int64) error {
	_, err := s.bannedUsers.DeleteOne(ctx, bson.M{"user_id": userID})
	if err == nil {
		s.bannedUsersCache.invalidate(strconv.FormatInt(userID, 10))
	}
	return err
}

func (s *Store) IsUserBanned(ctx context.Context, userID int64) (bool, error) {
	key := strconv.FormatInt(userID, 10)
	if v, ok := s.bannedUsersCache.get(key); ok {
		return v, nil
	}
	count, err := s.bannedUsers.CountDocuments(ctx, bson.M{"user_id": userID})
	if err != nil {
		return false, err
	}
	banned := count > 0
	s.bannedUsersCache.set(key, banned)
	return banned, nil
}

// ----------------------------------------------------------------------------
// Banned channel entity
// ----------------------------------------------------------------------------

type BannedChannel struct {
	ChannelID int64     `bson:"channel_id"`
	BannedBy  int64     `bson:"banned_by"`
	BannedAt  time.Time `bson:"banned_at"`
	Reason    string    `bson:"reason,omitempty"`
}

func (s *Store) BanChannel(ctx context.Context, b BannedChannel) error {
	filter := bson.M{"channel_id": b.ChannelID}
	update := bson.M{
		"$set": bson.M{
			"channel_id": b.ChannelID,
			"banned_by":  b.BannedBy,
			"banned_at":  b.BannedAt,
			"reason":     b.Reason,
		},
	}
	_, err := s.bannedChannels.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if err == nil {
		// Invalidate (not set): avoids a stale false overwriting a fresh true.
		s.bannedChansCache.invalidate(strconv.FormatInt(b.ChannelID, 10))
	}
	return err
}

func (s *Store) UnbanChannel(ctx context.Context, channelID int64) error {
	_, err := s.bannedChannels.DeleteOne(ctx, bson.M{"channel_id": channelID})
	if err == nil {
		s.bannedChansCache.invalidate(strconv.FormatInt(channelID, 10))
	}
	return err
}

func (s *Store) IsChannelBanned(ctx context.Context, channelID int64) (bool, error) {
	key := strconv.FormatInt(channelID, 10)
	if v, ok := s.bannedChansCache.get(key); ok {
		return v, nil
	}
	count, err := s.bannedChannels.CountDocuments(ctx, bson.M{"channel_id": channelID})
	if err != nil {
		return false, err
	}
	banned := count > 0
	s.bannedChansCache.set(key, banned)
	return banned, nil
}

// ----------------------------------------------------------------------------
// File record entity (dedup)
// ----------------------------------------------------------------------------

type FileRecord struct {
	FileKey    string    `bson:"file_key"`     // stable identifier (PackBotFileID or doc ID + access hash hex)
	Hash       string    `bson:"hash"`         // deterministic: hex(sha256(fileKey)[:16]) = 128-bit prefix
	FileName   string    `bson:"file_name"`    // original or synthesized
	MimeType   string    `bson:"mime_type"`    // best-effort MIME
	Size       int64     `bson:"size"`         // byte count
	MediaType  string    `bson:"media_type"`   // "document"|"video"|"audio"|"photo"|"voice"|"animation"|"video_note"|"sticker"
	VaultMsgID int32     `bson:"vault_msg_id"` // message ID in the vault channel
	DCID       int32     `bson:"dc_id"`        // Telegram data center of the file
	CreatedAt  time.Time `bson:"created_at"`
	LastSeenAt time.Time `bson:"last_seen_at"`

	// Advisory tracking fields (power /stats; do not affect dedup or stream).
	SeenCount         int64 `bson:"seen_count"`                     // incremented on each stream access
	ReuseCount        int64 `bson:"reuse_count"`                    // incremented on each dedup hit
	FirstSourceChatID int64 `bson:"first_source_chat_id,omitempty"` // provenance: where the file was first seen
	FirstSourceMsgID  int32 `bson:"first_source_msg_id,omitempty"`  // provenance: message ID of first source
}

func (s *Store) InsertFile(ctx context.Context, f FileRecord) error {
	_, err := s.files.InsertOne(ctx, f)
	if err == nil {
		// Cache value copies so callers can't mutate cached entries.
		s.fileByHash.set(f.Hash, f)
		s.fileByKey.set(f.FileKey, f)
	}
	return err
}

// FindFileByKey looks up a file by dedup key. Cached. No projection: callers
// (e.g. stream handler) need all fields. Returns a fresh value copy.
func (s *Store) FindFileByKey(ctx context.Context, key string) (*FileRecord, error) {
	if v, ok := s.fileByKey.get(key); ok {
		rec := v
		return &rec, nil
	}
	var f FileRecord
	if err := s.files.FindOne(ctx, bson.M{"file_key": key}).Decode(&f); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	s.fileByKey.set(key, f)
	return &f, nil
}

// FindFileByHash looks up a file by its deterministic URL hash. Cached.
// Returns a fresh value copy.
func (s *Store) FindFileByHash(ctx context.Context, hash string) (*FileRecord, error) {
	if v, ok := s.fileByHash.get(hash); ok {
		rec := v
		return &rec, nil
	}
	var f FileRecord
	if err := s.files.FindOne(ctx, bson.M{"hash": hash}).Decode(&f); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	s.fileByHash.set(hash, f)
	return &f, nil
}

func (s *Store) DeleteFileByHash(ctx context.Context, hash string) error {
	// Read fileKey from cache BEFORE invalidating (fast path).
	var fileKey string
	if v, ok := s.fileByHash.get(hash); ok {
		fileKey = v.FileKey
	}

	// Double-invalidate: before the DB delete (so a concurrent reader can't
	// re-cache a soon-to-be-deleted row) and after (to evict any racing entry).
	s.fileByHash.invalidate(hash)

	// Fall back to DB lookup if the cache was cold.
	if fileKey == "" {
		var f FileRecord
		if err := s.files.FindOne(ctx, bson.M{"hash": hash},
			options.FindOne().SetProjection(bson.M{"file_key": 1})).Decode(&f); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				s.fileByHash.invalidate(hash)
				return nil
			}
			return err
		}
		fileKey = f.FileKey
	}
	if fileKey != "" {
		s.fileByKey.invalidate(fileKey)
	}
	_, err := s.files.DeleteOne(ctx, bson.M{"hash": hash})
	if err == nil {
		// Post-delete invalidate (second half of the double-invalidate).
		s.fileByHash.invalidate(hash)
		if fileKey != "" {
			s.fileByKey.invalidate(fileKey)
		}
	}
	return err
}

// IncrementReuseCount atomically bumps reuse_count on dedup hits. Best-effort:
// does not touch last_seen_at (a dedup hit is a submission, not a stream).
func (s *Store) IncrementReuseCount(ctx context.Context, fileKey string) error {
	_, err := s.files.UpdateOne(ctx, bson.M{"file_key": fileKey}, bson.M{"$inc": bson.M{"reuse_count": 1}})
	return err
}

// IncrementSeenCount bumps seen_count and last_seen_at. Caches are not
// invalidated (advisory field, content unchanged). With a TouchBuffer wired up
// the call is non-blocking and the ctx is ignored; without one it falls back
// to a synchronous UpdateOne.
func (s *Store) IncrementSeenCount(ctx context.Context, fileKey string) error {
	if tb := s.touchBuffer.Load(); tb != nil {
		tb.Increment(fileKey)
		return nil
	}
	_, err := s.files.UpdateOne(ctx,
		bson.M{"file_key": fileKey},
		bson.M{
			"$inc": bson.M{"seen_count": 1},
			"$set": bson.M{"last_seen_at": time.Now()},
		},
	)
	return err
}

// SetTouchBuffer wires the batched-seen-count buffer. Must be called before
// the stream handler starts. Not safe to call concurrently with IncrementSeenCount.
func (s *Store) SetTouchBuffer(tb *TouchBuffer) {
	s.touchBuffer.Store(tb)
}

// FilesCollection exposes the underlying files collection for TouchBuffer's BulkWrite.
func (s *Store) FilesCollection() *mongo.Collection {
	return s.files
}

// ----------------------------------------------------------------------------
// TouchBuffer — batched seen-count increments
// ----------------------------------------------------------------------------
//
// Stream accesses are write-heavy on the files collection (one IncrementSeenCount
// per byte-range request). Batching coalesces N accesses across M distinct files
// into ceil(M/batch) BulkWrite ops. Increment is non-blocking (drops on full
// channel — seen_count is advisory); Stop() drains and flushes before Close.

const touchChannelCapacity = 1000

// TouchBuffer batches seen-count increments. Stream accesses send file keys to
// a channel; a background goroutine flushes via BulkWrite every interval
// (default 1s, configurable via TG_TOUCH_FLUSH_INTERVAL_MS).
type TouchBuffer struct {
	ch      chan string
	files   *mongo.Collection
	done    chan struct{}
	dropped atomic.Int64
}

// NewTouchBuffer launches the flush goroutine. Caller MUST call Stop()
// during shutdown before Store.Close() so the final flush completes.
// Non-positive interval is clamped to 1s.
func NewTouchBuffer(interval time.Duration, files *mongo.Collection) *TouchBuffer {
	if interval <= 0 {
		interval = time.Second
	}
	tb := &TouchBuffer{
		ch:    make(chan string, touchChannelCapacity),
		files: files,
		done:  make(chan struct{}),
	}
	go tb.flushLoop(interval)
	return tb
}

// Increment records a file access. Non-blocking: drops on full channel
// (seen_count is advisory, so a dropped increment is acceptable).
func (tb *TouchBuffer) Increment(fileKey string) {
	select {
	case tb.ch <- fileKey:
	default:
		tb.dropped.Add(1)
	}
}

// Dropped returns the count of increments dropped because the channel was full.
func (tb *TouchBuffer) Dropped() int64 {
	return tb.dropped.Load()
}

// Stop closes the channel and waits for the final flush. Must be called
// BEFORE Store.Close() so the BulkWrite doesn't race with disconnect.
// Safe to call at most once.
func (tb *TouchBuffer) Stop() {
	close(tb.ch)
	select {
	case <-tb.done:
	case <-time.After(5 * time.Second):
	}
}

// flushLoop drains the channel into a counts map and flushes on the ticker.
// On channel close (Stop), drains remaining values, flushes, then signals done.
func (tb *TouchBuffer) flushLoop(interval time.Duration) {
	defer close(tb.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	counts := make(map[string]int)
	flush := func() {
		if len(counts) == 0 {
			return
		}
		models := make([]mongo.WriteModel, 0, len(counts))
		now := time.Now()
		for key, n := range counts {
			models = append(models, mongo.NewUpdateOneModel().
				SetFilter(bson.M{"file_key": key}).
				SetUpdate(bson.M{"$inc": bson.M{"seen_count": n}, "$set": bson.M{"last_seen_at": now}}))
		}
		// Best-effort: seen_count is advisory; BulkWrite failures surface via mongo client logging.
		_, _ = tb.files.BulkWrite(context.Background(), models, options.BulkWrite().SetOrdered(false))
		counts = make(map[string]int)
	}
	for {
		select {
		case key, ok := <-tb.ch:
			if !ok {
				// Channel closed (Stop) — drain remaining values, then final flush.
				for remaining := range tb.ch {
					counts[remaining]++
				}
				flush()
				return
			}
			counts[key]++
		case <-ticker.C:
			flush()
		}
	}
}

// ingestLockDoc is stored in file_ingest_locks. The _id (file key) plus the
// implicit unique _id index make the lock exclusive.
type ingestLockDoc struct {
	ID        string    `bson:"_id"`        // = fileKey
	ExpiresAt time.Time `bson:"expires_at"` // TTL index removes the doc once this passes
	CreatedAt time.Time `bson:"created_at"` // for debugging
}

// AcquireIngestLock tries to acquire a cross-process ingest lock for a file key.
// Returns (true, nil) if acquired, (false, nil) if held by another process,
// (false, err) on a genuine DB error. Implementation: InsertOne with _id=fileKey;
// duplicate-key (code 11000) means held. The TTL index reaps locks from crashed
// processes. Caller MUST ReleaseIngestLock (typically via defer).
func (s *Store) AcquireIngestLock(ctx context.Context, fileKey string, ttl time.Duration) (bool, error) {
	now := time.Now()
	doc := ingestLockDoc{
		ID:        fileKey,
		ExpiresAt: now.Add(ttl),
		CreatedAt: now,
	}
	_, err := s.fileIngestLocks.InsertOne(ctx, doc)
	if err == nil {
		return true, nil
	}
	if isDuplicateKeyErr(err) {
		var existing ingestLockDoc
		if findErr := s.fileIngestLocks.FindOne(ctx, bson.M{"_id": fileKey}).Decode(&existing); findErr == nil && existing.ExpiresAt.Before(now) {
			if _, delErr := s.fileIngestLocks.DeleteOne(ctx, bson.M{"_id": fileKey}); delErr == nil {
				_, retryErr := s.fileIngestLocks.InsertOne(ctx, doc)
				if retryErr == nil {
					return true, nil
				}
				if isDuplicateKeyErr(retryErr) {
					return false, nil
				}
				return false, retryErr
			}
		}
		return false, nil
	}
	return false, err
}

// ReleaseIngestLock releases an ingest lock. Idempotent (deleting a
// non-existent _id is a no-op) — safe to call from defer even if Acquire failed.
func (s *Store) ReleaseIngestLock(ctx context.Context, fileKey string) error {
	_, err := s.fileIngestLocks.DeleteOne(ctx, bson.M{"_id": fileKey})
	return err
}

// isDuplicateKeyErr reports whether err is a MongoDB duplicate-key write
// error (code 11000).
func isDuplicateKeyErr(err error) bool {
	if err == nil {
		return false
	}
	var we mongo.WriteException
	if errors.As(err, &we) {
		for _, e := range we.WriteErrors {
			if e.Code == 11000 {
				return true
			}
		}
	}
	return false
}

// ----------------------------------------------------------------------------
// Authorized user entity (allowlist)
// ----------------------------------------------------------------------------

type AuthorizedUser struct {
	UserID    int64     `bson:"user_id"`
	FirstName string    `bson:"first_name,omitempty"`
	AddedBy   int64     `bson:"added_by"`
	AddedAt   time.Time `bson:"added_at"`
}

func (s *Store) Authorize(ctx context.Context, a AuthorizedUser) error {
	filter := bson.M{"user_id": a.UserID}
	update := bson.M{
		"$set": bson.M{
			"user_id":    a.UserID,
			"first_name": a.FirstName,
			"added_by":   a.AddedBy,
			"added_at":   a.AddedAt,
		},
	}
	_, err := s.authorized.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if err == nil {
		// Invalidate (not set): avoids a stale false overwriting a fresh true.
		s.authUsers.invalidate(strconv.FormatInt(a.UserID, 10))
	}
	return err
}

func (s *Store) Deauthorize(ctx context.Context, userID int64) error {
	_, err := s.authorized.DeleteOne(ctx, bson.M{"user_id": userID})
	if err == nil {
		s.authUsers.invalidate(strconv.FormatInt(userID, 10))
	}
	return err
}

func (s *Store) IsAuthorized(ctx context.Context, userID int64) (bool, error) {
	key := strconv.FormatInt(userID, 10)
	if v, ok := s.authUsers.get(key); ok {
		return v, nil
	}
	count, err := s.authorized.CountDocuments(ctx, bson.M{"user_id": userID})
	if err != nil {
		return false, err
	}
	auth := count > 0
	s.authUsers.set(key, auth)
	return auth, nil
}

func (s *Store) ListAuthorized(ctx context.Context) ([]AuthorizedUser, error) {
	cur, err := s.authorized.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	// cur.All closes the cursor at EOF; no defer Close needed.
	var out []AuthorizedUser
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// StreamAuthorized iterates every authorized user via cursor, calling fn for
// each. Streaming avoids materializing the full collection in memory.
func (s *Store) StreamAuthorized(ctx context.Context, fn func(AuthorizedUser) error) error {
	cur, err := s.authorized.Find(ctx, bson.M{}, options.Find().SetBatchSize(100))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var a AuthorizedUser
		if err := cur.Decode(&a); err != nil {
			return err
		}
		if err := fn(a); err != nil {
			return err
		}
	}
	return cur.Err()
}

// ----------------------------------------------------------------------------
// Restart marker
// ----------------------------------------------------------------------------

type RestartMarker struct {
	ID        string    `bson:"_id"`
	ChatID    int64     `bson:"chat_id"`
	MessageID int32     `bson:"message_id"`
	CreatedAt time.Time `bson:"created_at"`
}

func (s *Store) SaveRestartMarker(ctx context.Context, m RestartMarker) error {
	_, err := s.restart.InsertOne(ctx, m)
	return err
}

func (s *Store) PopRestartMarker(ctx context.Context) (*RestartMarker, error) {
	// Atomically find+delete the most recent marker (TTL index expires old ones after 1h).
	opts := options.FindOneAndDelete().SetSort(bson.D{{Key: "created_at", Value: -1}})
	var m RestartMarker
	if err := s.restart.FindOneAndDelete(ctx, bson.M{"created_at": bson.M{"$gte": time.Now().Add(-1 * time.Hour)}}, opts).Decode(&m); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	return &m, nil
}

// ----------------------------------------------------------------------------
// Activation token + activated user entities (token-gate)
// ----------------------------------------------------------------------------

// ActivationToken is a one-time use token for bot access activation.
// TTL: 10 minutes (via MongoDB TTL index on expires_at).
type ActivationToken struct {
	Token     string    `bson:"token"` // Crockford base32 of a 128-bit random value
	CreatedAt time.Time `bson:"created_at"`
	ExpiresAt time.Time `bson:"expires_at"` // = CreatedAt + ttl (10min default)
}

// ActivatedUser is a user with bot access for a limited time.
// TTL: TG_TOKEN_TTL_HOURS (default 24h, via MongoDB TTL index on expires_at).
type ActivatedUser struct {
	UserID      int64     `bson:"user_id"`
	ActivatedAt time.Time `bson:"activated_at"`
	ExpiresAt   time.Time `bson:"expires_at"` // = ActivatedAt + TG_TOKEN_TTL_HOURS
}

// SaveActivationToken stores a one-time activation token with the given TTL
// (10 min by convention). At 128 bits the collision probability is negligible.
func (s *Store) SaveActivationToken(ctx context.Context, token string, ttl time.Duration) error {
	now := time.Now()
	doc := ActivationToken{
		Token:     token,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
	_, err := s.activationTokens.InsertOne(ctx, doc)
	return err
}

// ConsumeActivationToken validates and atomically deletes a one-time token.
// FindOneAndDelete prevents token-reuse attacks: two concurrent /start calls
// with the same token cannot both succeed.
func (s *Store) ConsumeActivationToken(ctx context.Context, token string) error {
	res := s.activationTokens.FindOneAndDelete(ctx, bson.M{"token": token, "expires_at": bson.M{"$gt": time.Now()}})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("activation token not found or expired")
		}
		return err
	}
	return nil
}

// ActivateUser creates or refreshes an activated user record with the given TTL.
// Cache is invalidated (not set) to avoid a stale false overwriting a fresh true.
func (s *Store) ActivateUser(ctx context.Context, userID int64, ttl time.Duration) error {
	now := time.Now()
	filter := bson.M{"user_id": userID}
	update := bson.M{
		"$set": bson.M{
			"user_id":      userID,
			"activated_at": now,
			"expires_at":   now.Add(ttl),
		},
	}
	_, err := s.activatedUsers.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if err == nil {
		// Invalidate (not set): avoids a stale false overwriting a fresh true.
		s.activatedUsersCache.invalidate(strconv.FormatInt(userID, 10))
	}
	return err
}

// IsUserActivated reports whether the user has an active (non-expired) record.
// Cached for 5 min — a hot user issues dozens of file requests per minute.
func (s *Store) IsUserActivated(ctx context.Context, userID int64) (bool, error) {
	key := strconv.FormatInt(userID, 10)
	if v, ok := s.activatedUsersCache.get(key); ok {
		return v, nil
	}
	count, err := s.activatedUsers.CountDocuments(ctx, bson.M{"user_id": userID, "expires_at": bson.M{"$gt": time.Now()}})
	if err != nil {
		return false, err
	}
	activated := count > 0
	s.activatedUsersCache.set(key, activated)
	return activated, nil
}

// InvalidateActivatedUser removes a user's activation record. Used by
// /deauthorize for immediate revocation.
func (s *Store) InvalidateActivatedUser(ctx context.Context, userID int64) error {
	_, err := s.activatedUsers.DeleteOne(ctx, bson.M{"user_id": userID})
	if err == nil {
		s.activatedUsersCache.invalidate(strconv.FormatInt(userID, 10))
	}
	return err
}
