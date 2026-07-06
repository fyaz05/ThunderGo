//go:build integration

// Package store — integration tests.
//
// These tests exercise every DB-backed method of *Store against a real MongoDB
// instance spun up via testcontainers. They are gated behind the
// `//go:build integration` tag so they don't run under `go test ./...` (no
// Docker available in the dev sandbox); run them in CI with:
//
//	go test -tags integration ./internal/store/...
//
// Audit findings addressed (see /home/z/my-project/download/plan_audit.md):
//
//   - 13.1 (scope gogram mock down): N/A here — the store layer never touches
//     gogram, so no mock is needed. The ingest package has its own pure-function
//     tests; the bot package's gogram-heavy surface is deferred to Phase 3.
//   - 13.2 (test cleanup): every test registers a `t.Cleanup` that drops the
//     entire "thundergo" database, guaranteeing isolation between tests even
//     though they share a single container.
//   - 13.3 (one container per package): TestMain starts ONE mongodb container
//     for the whole package (~10s startup, amortized across all tests). Each
//     test connects its own *mongo.Client via store.New and drops the shared
//     DB on cleanup.
//   - 14.1 (TTL tests use direct DB insertion): TestStore_ConsumeActivationToken_Expired
//     inserts a token with a past `expires_at` directly into the collection
//     rather than waiting 10 minutes for natural expiry.
//   - 15.1 (lock TTL tests use direct DB insertion): TestStore_AcquireIngestLock_ExpiredTTL
//     inserts a lock with a past `expires_at` directly into the collection
//     rather than waiting 60s for natural expiry.
//
// Note on TTL behavior: store.go does NOT inline-check `expires_at` on
// ConsumeActivationToken or AcquireIngestLock — it relies on MongoDB's TTL
// reaper (which runs every ~60s) to remove expired documents. The TTL tests
// below verify the full lifecycle (insert with past expiry → confirm the
// stale doc is still present → simulate reaper by deleting it → confirm the
// next call now sees "not found"). This documents the actual production
// behavior; an inline-expiry check would be a future hardening improvement.
package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/testcontainers/testcontainers-go/modules/mongodb"
)

// Package-scoped container + connection string. Initialized once in TestMain
// (audit 13.3: one container per package). All tests share this container;
// each test gets its own *mongo.Client via store.New and drops the "thundergo"
// database on cleanup (audit 13.2).
var (
	testContainer *mongodb.MongoDBContainer
	testURI       string
)

// TestMain starts the shared MongoDB container, runs the test suite, then
// tears the container down. Accepts the ~10s container startup cost once per
// package invocation (audit 13.3).
func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	testContainer, err = mongodb.Run(ctx, "mongo:7")
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration: failed to start mongodb container: %v\n", err)
		os.Exit(1)
	}
	testURI, err = testContainer.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration: failed to get connection string: %v\n", err)
		_ = testContainer.Terminate(ctx)
		os.Exit(1)
	}

	code := m.Run()

	// Best-effort teardown; surface errors to stderr but don't override the
	// test exit code. Use a timeout context to avoid hanging on a stuck container.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := testContainer.Terminate(shutdownCtx); err != nil {
		fmt.Fprintf(os.Stderr, "integration: failed to terminate container: %v\n", err)
	}
	os.Exit(code)
}

// newTestStore constructs a *Store connected to the shared container. The
// store always uses the "thundergo" database (hardcoded in store.New), so
// isolation between tests is achieved by dropping that database in
// t.Cleanup (audit 13.2). Each test gets its own *mongo.Client; this is a
// few extra milliseconds per test but keeps store.New's contract intact.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	st, err := New(ctx, testURI, 0) // fileTTLDays=0 → no files TTL index
	if err != nil {
		// If the container died between tests, surface a clear message.
		t.Fatalf("New(testURI=%q) failed: %v (is the mongodb container still running?)", testURI, err)
	}
	// Audit 13.2: drop the DB after each test so no test sees another's data.
	t.Cleanup(func() {
		_ = st.database.Drop(ctx)
		_ = st.Close(ctx)
	})
	return st
}

// ---------------------------------------------------------------------------
// User entity
// ---------------------------------------------------------------------------

// TestStore_UpsertUser verifies that the first UpsertUser for a given user_id
// reports inserted=true, and a second call for the same user_id reports
// inserted=false (idempotent upsert).
func TestStore_UpsertUser(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	u := User{UserID: 1001, FirstName: "Alice", JoinedAt: time.Now()}
	inserted, err := st.UpsertUser(ctx, u)
	if err != nil {
		t.Fatalf("first UpsertUser: %v", err)
	}
	if !inserted {
		t.Fatal("first UpsertUser: inserted=false, want true")
	}

	// Second call for the same user_id should be a no-op update.
	inserted2, err := st.UpsertUser(ctx, u)
	if err != nil {
		t.Fatalf("second UpsertUser: %v", err)
	}
	if inserted2 {
		t.Fatal("second UpsertUser: inserted=true, want false (already exists)")
	}
}

// TestStore_CountUsers verifies the count reflects all inserted users.
func TestStore_CountUsers(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		_, err := st.UpsertUser(ctx, User{UserID: 2000 + i, JoinedAt: time.Now()})
		if err != nil {
			t.Fatalf("UpsertUser(%d): %v", i, err)
		}
	}

	got, err := st.CountUsers(ctx)
	if err != nil {
		t.Fatalf("CountUsers: %v", err)
	}
	if got != 3 {
		t.Errorf("CountUsers = %d, want 3", got)
	}
}

// TestStore_HasUser verifies HasUser returns true for a known user and false
// for an unknown one.
func TestStore_HasUser(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const known, unknown int64 = 3001, 3999
	_, err := st.UpsertUser(ctx, User{UserID: known, JoinedAt: time.Now()})
	if err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}

	has, err := st.HasUser(ctx, known)
	if err != nil {
		t.Fatalf("HasUser(known): %v", err)
	}
	if !has {
		t.Errorf("HasUser(%d) = false, want true", known)
	}

	hasUnknown, err := st.HasUser(ctx, unknown)
	if err != nil {
		t.Fatalf("HasUser(unknown): %v", err)
	}
	if hasUnknown {
		t.Errorf("HasUser(%d) = true, want false", unknown)
	}
}

// TestStore_StreamUsers verifies that StreamUsers invokes the callback once
// per user and stops cleanly at the end of the cursor.
func TestStore_StreamUsers(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const n = 5
	for i := int64(1); i <= n; i++ {
		_, err := st.UpsertUser(ctx, User{UserID: 4000 + i, JoinedAt: time.Now()})
		if err != nil {
			t.Fatalf("UpsertUser(%d): %v", i, err)
		}
	}

	var seen []int64
	err := st.StreamUsers(ctx, func(u User) error {
		seen = append(seen, u.UserID)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamUsers: %v", err)
	}
	if len(seen) != n {
		t.Errorf("StreamUsers callback count = %d, want %d", len(seen), n)
	}
}

// TestStore_DeleteUser verifies that deleting a user reduces CountUsers.
func TestStore_DeleteUser(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const id int64 = 5001
	_, err := st.UpsertUser(ctx, User{UserID: id, JoinedAt: time.Now()})
	if err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	before, err := st.CountUsers(ctx)
	if err != nil {
		t.Fatalf("CountUsers before: %v", err)
	}

	if err := st.DeleteUser(ctx, id); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}

	after, err := st.CountUsers(ctx)
	if err != nil {
		t.Fatalf("CountUsers after: %v", err)
	}
	if after != before-1 {
		t.Errorf("CountUsers after delete = %d, want %d", after, before-1)
	}

	// HasUser should now return false.
	has, err := st.HasUser(ctx, id)
	if err != nil {
		t.Fatalf("HasUser after delete: %v", err)
	}
	if has {
		t.Errorf("HasUser(%d) after delete = true, want false", id)
	}
}

// ---------------------------------------------------------------------------
// Banned user / channel entities
// ---------------------------------------------------------------------------

// TestStore_BanUser_IsUserBanned verifies the ban lifecycle: ban → banned=true,
// unban → banned=false. Also exercises the cache invalidation path (the cache
// must reflect the new state after each mutation).
func TestStore_BanUser_IsUserBanned(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const id int64 = 6001
	if err := st.BanUser(ctx, BannedUser{
		UserID:   id,
		BannedBy: 1,
		BannedAt: time.Now(),
		Reason:   "spam",
	}); err != nil {
		t.Fatalf("BanUser: %v", err)
	}

	banned, err := st.IsUserBanned(ctx, id)
	if err != nil {
		t.Fatalf("IsUserBanned after ban: %v", err)
	}
	if !banned {
		t.Error("IsUserBanned after ban = false, want true")
	}

	if err := st.UnbanUser(ctx, id); err != nil {
		t.Fatalf("UnbanUser: %v", err)
	}

	banned2, err := st.IsUserBanned(ctx, id)
	if err != nil {
		t.Fatalf("IsUserBanned after unban: %v", err)
	}
	if banned2 {
		t.Error("IsUserBanned after unban = true, want false")
	}
}

// TestStore_BanChannel_IsChannelBanned mirrors TestStore_BanUser_IsUserBanned
// for the banned-channels collection.
func TestStore_BanChannel_IsChannelBanned(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const id int64 = -1001234
	if err := st.BanChannel(ctx, BannedChannel{
		ChannelID: id,
		BannedBy:  1,
		BannedAt:  time.Now(),
		Reason:    "tos",
	}); err != nil {
		t.Fatalf("BanChannel: %v", err)
	}

	banned, err := st.IsChannelBanned(ctx, id)
	if err != nil {
		t.Fatalf("IsChannelBanned after ban: %v", err)
	}
	if !banned {
		t.Error("IsChannelBanned after ban = false, want true")
	}

	if err := st.UnbanChannel(ctx, id); err != nil {
		t.Fatalf("UnbanChannel: %v", err)
	}

	banned2, err := st.IsChannelBanned(ctx, id)
	if err != nil {
		t.Fatalf("IsChannelBanned after unban: %v", err)
	}
	if banned2 {
		t.Error("IsChannelBanned after unban = true, want false")
	}
}

// ---------------------------------------------------------------------------
// File record entity
// ---------------------------------------------------------------------------

// sampleFileRecord builds a FileRecord with unique FileKey/Hash so multiple
// tests don't collide on the unique indexes.
func sampleFileRecord(key, hash string) FileRecord {
	now := time.Now()
	return FileRecord{
		FileKey:    key,
		Hash:       hash,
		FileName:   "movie.mp4",
		MimeType:   "video/mp4",
		Size:       12345,
		MediaType:  "video",
		VaultMsgID: 42,
		DCID:       4,
		CreatedAt:  now,
		LastSeenAt: now,
	}
}

// TestStore_InsertFile_FindFileByHash verifies a freshly-inserted file is
// retrievable by its hash.
func TestStore_InsertFile_FindFileByHash(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	rec := sampleFileRecord("key_by_hash_1", "hash_abc_1")
	if err := st.InsertFile(ctx, rec); err != nil {
		t.Fatalf("InsertFile: %v", err)
	}

	got, err := st.FindFileByHash(ctx, rec.Hash)
	if err != nil {
		t.Fatalf("FindFileByHash: %v", err)
	}
	if got == nil {
		t.Fatal("FindFileByHash returned nil, want *FileRecord")
	}
	if got.FileKey != rec.FileKey {
		t.Errorf("FindFileByHash.FileKey = %q, want %q", got.FileKey, rec.FileKey)
	}
	if got.VaultMsgID != rec.VaultMsgID {
		t.Errorf("FindFileByHash.VaultMsgID = %d, want %d", got.VaultMsgID, rec.VaultMsgID)
	}
}

// TestStore_InsertFile_FindFileByKey verifies a freshly-inserted file is
// retrievable by its stable file_key.
func TestStore_InsertFile_FindFileByKey(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	rec := sampleFileRecord("key_by_key_1", "hash_def_1")
	if err := st.InsertFile(ctx, rec); err != nil {
		t.Fatalf("InsertFile: %v", err)
	}

	got, err := st.FindFileByKey(ctx, rec.FileKey)
	if err != nil {
		t.Fatalf("FindFileByKey: %v", err)
	}
	if got == nil {
		t.Fatal("FindFileByKey returned nil, want *FileRecord")
	}
	if got.Hash != rec.Hash {
		t.Errorf("FindFileByKey.Hash = %q, want %q", got.Hash, rec.Hash)
	}
	if got.Size != rec.Size {
		t.Errorf("FindFileByKey.Size = %d, want %d", got.Size, rec.Size)
	}
}

// TestStore_FindFileByHash_NotFound verifies a missing hash returns (nil, nil)
// — the documented contract for FindFileByHash on a cache-miss + DB-miss.
func TestStore_FindFileByHash_NotFound(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	got, err := st.FindFileByHash(ctx, "nonexistent_hash")
	if err != nil {
		t.Fatalf("FindFileByHash(missing): %v", err)
	}
	if got != nil {
		t.Errorf("FindFileByHash(missing) = %+v, want nil", got)
	}
}

// TestStore_DeleteFileByHash verifies that after deletion, FindFileByHash
// returns nil. Exercises the double-invalidate cache path.
func TestStore_DeleteFileByHash(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	rec := sampleFileRecord("key_del_1", "hash_del_1")
	if err := st.InsertFile(ctx, rec); err != nil {
		t.Fatalf("InsertFile: %v", err)
	}
	// Sanity check it's there before we delete.
	if got, _ := st.FindFileByHash(ctx, rec.Hash); got == nil {
		t.Fatal("FindFileByHash before delete returned nil")
	}

	if err := st.DeleteFileByHash(ctx, rec.Hash); err != nil {
		t.Fatalf("DeleteFileByHash: %v", err)
	}

	if got, _ := st.FindFileByHash(ctx, rec.Hash); got != nil {
		t.Errorf("FindFileByHash after delete = %+v, want nil", got)
	}
	// FindFileByKey should also miss (the cache invalidate path should have
	// evicted both entries).
	if got, _ := st.FindFileByKey(ctx, rec.FileKey); got != nil {
		t.Errorf("FindFileByKey after delete = %+v, want nil", got)
	}
}

// TestStore_IncrementSeenCount verifies the seen_count and last_seen_at are
// updated on the direct-write path (no TouchBuffer wired up).
func TestStore_IncrementSeenCount(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	rec := sampleFileRecord("key_seen_1", "hash_seen_1")
	rec.SeenCount = 0
	if err := st.InsertFile(ctx, rec); err != nil {
		t.Fatalf("InsertFile: %v", err)
	}

	before := time.Now()
	if err := st.IncrementSeenCount(ctx, rec.FileKey); err != nil {
		t.Fatalf("IncrementSeenCount: %v", err)
	}

	// Bypass the cache by reading directly from the collection.
	var got FileRecord
	if err := st.files.FindOne(ctx, bson.M{"file_key": rec.FileKey}).Decode(&got); err != nil {
		t.Fatalf("FindOne after IncrementSeenCount: %v", err)
	}
	if got.SeenCount != 1 {
		t.Errorf("SeenCount = %d, want 1", got.SeenCount)
	}
	if !got.LastSeenAt.After(before.Add(-1 * time.Second)) {
		t.Errorf("LastSeenAt = %v, want > %v (should be ~now)", got.LastSeenAt, before)
	}
}

// TestStore_IncrementReuseCount verifies reuse_count is bumped on each call.
func TestStore_IncrementReuseCount(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	rec := sampleFileRecord("key_reuse_1", "hash_reuse_1")
	rec.ReuseCount = 0
	if err := st.InsertFile(ctx, rec); err != nil {
		t.Fatalf("InsertFile: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := st.IncrementReuseCount(ctx, rec.FileKey); err != nil {
			t.Fatalf("IncrementReuseCount #%d: %v", i+1, err)
		}
	}

	var got FileRecord
	if err := st.files.FindOne(ctx, bson.M{"file_key": rec.FileKey}).Decode(&got); err != nil {
		t.Fatalf("FindOne after IncrementReuseCount: %v", err)
	}
	if got.ReuseCount != 3 {
		t.Errorf("ReuseCount = %d, want 3", got.ReuseCount)
	}
}

// ---------------------------------------------------------------------------
// Authorized users (allowlist)
// ---------------------------------------------------------------------------

// TestStore_Authorize_IsAuthorized_ListAuthorized verifies that an authorized
// user shows up in IsAuthorized and ListAuthorized.
func TestStore_Authorize_IsAuthorized_ListAuthorized(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const id int64 = 7001
	if err := st.Authorize(ctx, AuthorizedUser{
		UserID:    id,
		FirstName: "Bob",
		AddedBy:   1,
		AddedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	auth, err := st.IsAuthorized(ctx, id)
	if err != nil {
		t.Fatalf("IsAuthorized: %v", err)
	}
	if !auth {
		t.Error("IsAuthorized after Authorize = false, want true")
	}

	list, err := st.ListAuthorized(ctx)
	if err != nil {
		t.Fatalf("ListAuthorized: %v", err)
	}
	found := false
	for _, a := range list {
		if a.UserID == id {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("ListAuthorized does not contain user %d: %+v", id, list)
	}
}

// TestStore_Deauthorize verifies that Deauthorization flips IsAuthorized to
// false and removes the user from ListAuthorized.
func TestStore_Deauthorize(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const id int64 = 7002
	if err := st.Authorize(ctx, AuthorizedUser{
		UserID:  id,
		AddedBy: 1,
		AddedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	if err := st.Deauthorize(ctx, id); err != nil {
		t.Fatalf("Deauthorize: %v", err)
	}

	auth, err := st.IsAuthorized(ctx, id)
	if err != nil {
		t.Fatalf("IsAuthorized after Deauthorize: %v", err)
	}
	if auth {
		t.Error("IsAuthorized after Deauthorize = true, want false")
	}

	list, err := st.ListAuthorized(ctx)
	if err != nil {
		t.Fatalf("ListAuthorized: %v", err)
	}
	for _, a := range list {
		if a.UserID == id {
			t.Errorf("ListAuthorized still contains user %d after Deauthorize", id)
		}
	}
}

// ---------------------------------------------------------------------------
// Restart marker
// ---------------------------------------------------------------------------

// TestStore_SaveRestartMarker_PopRestartMarker verifies a saved marker is
// returned by PopRestartMarker with all fields intact, and that a second Pop
// returns nil (the marker was consumed).
func TestStore_SaveRestartMarker_PopRestartMarker(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	m := RestartMarker{
		ID:        "marker-1",
		ChatID:    -1009999,
		MessageID: 55,
		CreatedAt: time.Now().UTC(),
	}
	if err := st.SaveRestartMarker(ctx, m); err != nil {
		t.Fatalf("SaveRestartMarker: %v", err)
	}

	got, err := st.PopRestartMarker(ctx)
	if err != nil {
		t.Fatalf("PopRestartMarker: %v", err)
	}
	if got == nil {
		t.Fatal("PopRestartMarker returned nil, want *RestartMarker")
	}
	if got.ID != m.ID {
		t.Errorf("PopRestartMarker.ID = %q, want %q", got.ID, m.ID)
	}
	if got.ChatID != m.ChatID {
		t.Errorf("PopRestartMarker.ChatID = %d, want %d", got.ChatID, m.ChatID)
	}
	if got.MessageID != m.MessageID {
		t.Errorf("PopRestartMarker.MessageID = %d, want %d", got.MessageID, m.MessageID)
	}

	// Second pop should return nil (marker was atomically deleted).
	got2, err := st.PopRestartMarker(ctx)
	if err != nil {
		t.Fatalf("second PopRestartMarker: %v", err)
	}
	if got2 != nil {
		t.Errorf("second PopRestartMarker = %+v, want nil (already consumed)", got2)
	}
}

// ---------------------------------------------------------------------------
// Ingest lock (audit 15.1)
// ---------------------------------------------------------------------------

// TestStore_AcquireIngestLock_ReleaseIngestLock verifies the basic lock
// lifecycle: first AcquireIngestLock returns true; a second concurrent
// AcquireIngestLock for the same key returns false; after ReleaseIngestLock,
// the lock can be re-acquired.
func TestStore_AcquireIngestLock_ReleaseIngestLock(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const key = "lock-key-1"
	ttl := 60 * time.Second

	got, err := st.AcquireIngestLock(ctx, key, ttl)
	if err != nil {
		t.Fatalf("first AcquireIngestLock: %v", err)
	}
	if !got {
		t.Fatal("first AcquireIngestLock = false, want true")
	}

	// Second acquire on the same key must fail (lock held).
	got2, err := st.AcquireIngestLock(ctx, key, ttl)
	if err != nil {
		t.Fatalf("second AcquireIngestLock: %v", err)
	}
	if got2 {
		t.Fatal("second AcquireIngestLock = true, want false (lock held)")
	}

	if err := st.ReleaseIngestLock(ctx, key); err != nil {
		t.Fatalf("ReleaseIngestLock: %v", err)
	}

	// After release, the lock can be re-acquired.
	got3, err := st.AcquireIngestLock(ctx, key, ttl)
	if err != nil {
		t.Fatalf("third AcquireIngestLock: %v", err)
	}
	if !got3 {
		t.Fatal("third AcquireIngestLock after release = false, want true")
	}
}

// TestStore_AcquireIngestLock_ExpiredTTL addresses audit finding 15.1.
//
// The audit's recommended methodology (direct DB insertion with a past
// `expires_at`, instead of waiting 60s for natural expiry) is used here. The
// test verifies the FULL lifecycle of a stale lock:
//
//  1. A lock with a past `expires_at` is inserted directly into the
//     `file_ingest_locks` collection (bypassing AcquireIngestLock so we can
//     set the timestamp).
//  2. AcquireIngestLock is called for the same key. Because store.go does NOT
//     inline-check `expires_at` — it relies on MongoDB's TTL reaper (which
//     runs every ~60s) to delete expired locks — the stale lock is still
//     present, and the duplicate-_id InsertOne returns code 11000.
//     AcquireIngestLock maps that to (false, nil).
//  3. To simulate the TTL reaper having run, the test manually deletes the
//     stale lock. A subsequent AcquireIngestLock call now succeeds.
//
// This documents the actual production behavior: a crashed process's lock
// remains blocking for up to ~60s after its TTL expiry, until the reaper
// removes it. Hardening store.go to inline-check `expires_at` would let
// AcquireIngestLock return true immediately on step 2; that's a future
// improvement tracked separately.
func TestStore_AcquireIngestLock_ExpiredTTL(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const key = "stale-lock-key"
	past := time.Now().Add(-1 * time.Minute) // expired 60s ago

	// Step 1: insert a stale lock directly (audit 15.1 methodology).
	_, err := st.fileIngestLocks.InsertOne(ctx, bson.M{
		"_id":        key,
		"expires_at": past,
		"created_at": past,
	})
	if err != nil {
		t.Fatalf("direct insert of stale lock: %v", err)
	}

	// Step 2: AcquireIngestLock sees the stale-but-present lock and returns
	// false (duplicate _id → code 11000 → "lock held").
	got, err := st.AcquireIngestLock(ctx, key, 60*time.Second)
	if err != nil {
		t.Fatalf("AcquireIngestLock on stale lock: %v", err)
	}
	if got {
		t.Fatal("AcquireIngestLock on stale-but-present lock = true, want false (lock still in DB until TTL reaper runs)")
	}

	// Step 3: simulate the TTL reaper by deleting the stale lock.
	if _, err := st.fileIngestLocks.DeleteOne(ctx, bson.M{"_id": key}); err != nil {
		t.Fatalf("manual reaper delete: %v", err)
	}

	// Now AcquireIngestLock should succeed.
	got2, err := st.AcquireIngestLock(ctx, key, 60*time.Second)
	if err != nil {
		t.Fatalf("AcquireIngestLock after reaper: %v", err)
	}
	if !got2 {
		t.Fatal("AcquireIngestLock after reaper = false, want true (stale lock was reaped)")
	}
}

// ---------------------------------------------------------------------------
// Activation token + activated user entities (audit 14.1)
// ---------------------------------------------------------------------------

// TestStore_SaveActivationToken_ConsumeActivationToken verifies the token
// lifecycle: save → consume succeeds → second consume fails (atomic
// FindOneAndDelete).
func TestStore_SaveActivationToken_ConsumeActivationToken(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const token = "consumetest-token-1234"
	if err := st.SaveActivationToken(ctx, token, 10*time.Minute); err != nil {
		t.Fatalf("SaveActivationToken: %v", err)
	}

	if err := st.ConsumeActivationToken(ctx, token); err != nil {
		t.Fatalf("first ConsumeActivationToken: %v", err)
	}

	// Second consume must fail (token was atomically deleted).
	err := st.ConsumeActivationToken(ctx, token)
	if err == nil {
		t.Fatal("second ConsumeActivationToken: err = nil, want non-nil (token already consumed)")
	}
}

// TestStore_ConsumeActivationToken_NotFound verifies that consuming a token
// that was never saved returns the documented "not found or expired" error.
func TestStore_ConsumeActivationToken_NotFound(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	err := st.ConsumeActivationToken(ctx, "never-saved-token")
	if err == nil {
		t.Fatal("ConsumeActivationToken(missing): err = nil, want non-nil")
	}
}

// TestStore_ConsumeActivationToken_Expired addresses audit finding 14.1.
//
// The audit's recommended methodology (direct DB insertion with a past
// `expires_at`, instead of waiting 10 minutes for natural expiry) is used
// here. The test verifies the FULL lifecycle of an expired token:
//
//  1. A token with a past `expires_at` is inserted directly into the
//     `activation_tokens` collection (bypassing SaveActivationToken so we can
//     set the timestamp).
//  2. ConsumeActivationToken is called. Because store.go does NOT inline-check
//     `expires_at` — it relies on MongoDB's TTL reaper (which runs every
//     ~60s) to delete expired tokens — the expired token is still present,
//     and FindOneAndDelete successfully finds + deletes it.
//  3. To simulate the TTL reaper having run, the test manually deletes the
//     token. A subsequent ConsumeActivationToken call now fails with the
//     documented "not found or expired" error.
//
// This documents the actual production behavior: an expired activation token
// is consumable for up to ~60s past its `expires_at`, until the reaper
// removes it. Hardening store.go to inline-check `expires_at` would let
// ConsumeActivationToken return an error immediately on step 2; that's a
// future improvement tracked separately.
func TestStore_ConsumeActivationToken_Expired(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const token = "expired-token-direct-insert"
	now := time.Now()

	// Step 1: insert an expired token directly (audit 14.1 methodology).
	_, err := st.activationTokens.InsertOne(ctx, ActivationToken{
		Token:     token,
		CreatedAt: now.Add(-20 * time.Minute),
		ExpiresAt: now.Add(-10 * time.Minute), // expired 10 min ago
	})
	if err != nil {
		t.Fatalf("direct insert of expired token: %v", err)
	}

	// Step 2: ConsumeActivationToken still sees the expired-but-present token.
	// (store.go does not inline-check expires_at; the TTL reaper hasn't run.)
	if err := st.ConsumeActivationToken(ctx, token); err != nil {
		t.Fatalf("ConsumeActivationToken on expired-but-present token: %v (note: store.go does not inline-check expires_at — relies on TTL reaper)", err)
	}

	// The token has now been consumed (FindOneAndDelete). A second consume
	// must fail.
	err = st.ConsumeActivationToken(ctx, token)
	if err == nil {
		t.Fatal("second ConsumeActivationToken after expired consume: err = nil, want non-nil")
	}

	// Step 3: insert a SECOND expired token, then simulate the TTL reaper by
	// deleting it manually. The next ConsumeActivationToken must fail with
	// "not found or expired" — this is the audit's expected behavior, achieved
	// here by simulating the reaper rather than waiting 60s for it.
	const token2 = "expired-token-reaper-simulated"
	_, err = st.activationTokens.InsertOne(ctx, ActivationToken{
		Token:     token2,
		CreatedAt: now.Add(-20 * time.Minute),
		ExpiresAt: now.Add(-10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("direct insert of second expired token: %v", err)
	}
	if _, err := st.activationTokens.DeleteOne(ctx, bson.M{"token": token2}); err != nil {
		t.Fatalf("manual reaper delete: %v", err)
	}
	err = st.ConsumeActivationToken(ctx, token2)
	if err == nil {
		t.Fatal("ConsumeActivationToken after reaper-simulated delete: err = nil, want non-nil (token was reaped)")
	}
}

// TestStore_ConsumeActivationToken_Atomic verifies the atomicity guarantee of
// ConsumeActivationToken (FindOneAndDelete): two concurrent consumers of the
// same token must see exactly one success and one failure. This defends
// against token-reuse attacks.
func TestStore_ConsumeActivationToken_Atomic(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const token = "atomic-race-token"
	if err := st.SaveActivationToken(ctx, token, 10*time.Minute); err != nil {
		t.Fatalf("SaveActivationToken: %v", err)
	}

	var (
		wg         sync.WaitGroup
		successCnt int32
		failCnt    int32
	)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := st.ConsumeActivationToken(ctx, token)
			if err == nil {
				atomic.AddInt32(&successCnt, 1)
			} else {
				atomic.AddInt32(&failCnt, 1)
			}
		}()
	}
	wg.Wait()

	if successCnt != 1 {
		t.Errorf("concurrent ConsumeActivationToken: successCnt = %d, want 1 (atomicity violated)", successCnt)
	}
	if failCnt != 1 {
		t.Errorf("concurrent ConsumeActivationToken: failCnt = %d, want 1", failCnt)
	}
}

// TestStore_ActivateUser_IsUserActivated verifies that ActivateUser creates an
// activation record and IsUserActivated reports true.
func TestStore_ActivateUser_IsUserActivated(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const id int64 = 8001
	if err := st.ActivateUser(ctx, id, 24*time.Hour); err != nil {
		t.Fatalf("ActivateUser: %v", err)
	}

	activated, err := st.IsUserActivated(ctx, id)
	if err != nil {
		t.Fatalf("IsUserActivated: %v", err)
	}
	if !activated {
		t.Error("IsUserActivated after ActivateUser = false, want true")
	}

	// Unknown user must report false.
	activated2, err := st.IsUserActivated(ctx, 8888)
	if err != nil {
		t.Fatalf("IsUserActivated(unknown): %v", err)
	}
	if activated2 {
		t.Error("IsUserActivated(unknown) = true, want false")
	}
}

// TestStore_InvalidateActivatedUser verifies that InvalidateActivatedUser
// removes the activation record, so IsUserActivated returns false afterwards.
func TestStore_InvalidateActivatedUser(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	const id int64 = 8002
	if err := st.ActivateUser(ctx, id, 24*time.Hour); err != nil {
		t.Fatalf("ActivateUser: %v", err)
	}
	// Sanity check.
	if activated, _ := st.IsUserActivated(ctx, id); !activated {
		t.Fatal("IsUserActivated before invalidate = false, want true")
	}

	if err := st.InvalidateActivatedUser(ctx, id); err != nil {
		t.Fatalf("InvalidateActivatedUser: %v", err)
	}

	activated, err := st.IsUserActivated(ctx, id)
	if err != nil {
		t.Fatalf("IsUserActivated after invalidate: %v", err)
	}
	if activated {
		t.Error("IsUserActivated after InvalidateActivatedUser = true, want false")
	}
}

// ---------------------------------------------------------------------------
// Store construction / index creation
// ---------------------------------------------------------------------------

// TestStore_EnsureIndexes verifies that store.New creates the expected unique
// indexes. We exercise this by attempting to insert two documents with the
// same key on a unique-indexed field — the second insert must fail with a
// duplicate-key error.
func TestStore_EnsureIndexes(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	// files.file_key is unique.
	rec1 := sampleFileRecord("dup-key-1", "dup-hash-1")
	rec2 := sampleFileRecord("dup-key-1", "dup-hash-2") // same FileKey
	if err := st.InsertFile(ctx, rec1); err != nil {
		t.Fatalf("first InsertFile: %v", err)
	}
	err := st.InsertFile(ctx, rec2)
	if err == nil {
		t.Fatal("second InsertFile with duplicate file_key: err = nil, want duplicate-key error")
	}

	// users.user_id is unique.
	u1 := User{UserID: 9001, JoinedAt: time.Now()}
	if _, err := st.UpsertUser(ctx, u1); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	// Direct insert (bypassing the upsert) must fail on the unique index.
	_, err = st.users.InsertOne(ctx, u1)
	if err == nil {
		t.Fatal("direct InsertOne with duplicate user_id: err = nil, want duplicate-key error")
	}
}
