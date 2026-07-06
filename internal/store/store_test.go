package store

// White-box tests for store.go entity structs and pure helpers.
//
// store.New() connects to MongoDB, so the DB-backed methods (UpsertUser,
// FindFileByHash DB fallback, BanUser, IsUserBanned, InsertFile,
// DeleteFileByHash double-invalidate against a real collection, Authorize,
// SaveRestartMarker, PopRestartMarker, ensureIndexes, etc.) cannot be
// unit-tested without a running MongoDB instance. These tests cover what we
// CAN test in isolation:
//
//  1. Entity struct construction and BSON tag correctness for all 6 types
//     (FileRecord, User, BannedUser, BannedChannel, AuthorizedUser,
//     RestartMarker). This catches silent field renames / tag typos that
//     would break persistence — a misnamed bson tag silently drops data.
//  2. StreamUsers signature verification at compile time (catches renames
//     or signature drift).
//  3. Cache fast-path of FindFileByHash / FindFileByKey — the cache-hit
//     branch returns the cached value WITHOUT touching s.files, so it can
//     be exercised with a nil mongo client. Verifies the value-copy
//     semantics documented in the method comments (CWE-662 / CWE-704).
//  4. Pure helper isIndexExistsErr (no DB needed).
//
// MongoDB-dependent integration tests require a live MongoDB instance and
// are NOT covered here. They should be added under a `//go:build integration`
// tag once a test MongoDB harness (testcontainers / local mongod) is
// available. The methods that remain untested without such a harness:
//
//   - New / Close / ensureIndexes       (require a live client + database)
//   - UpsertUser / CountUsers / HasUser (s.users)
//   - StreamUsers (DB path)                    (s.users.Find)
//   - BanUser / UnbanUser / IsUserBanned (DB path)
//   - BanChannel / UnbanChannel / IsChannelBanned (DB path)
//   - InsertFile / TouchFile / DeleteFileByHash (DB path)
//   - FindFileByKey / FindFileByHash (DB-miss path)
//   - Authorize / Deauthorize / IsAuthorized / ListAuthorized (DB path)
//   - SaveRestartMarker / PopRestartMarker (DB path)
//
// Note: Store is NOT nil-safe by design — every method dereferences the
// *Store receiver and accesses a *mongo.Collection field (s.users, s.files,
// ...). A nil *Store will panic on the first collection access. This is
// intentional: Store is always constructed via store.New, which returns a
// fully-initialized *Store or an error. Do NOT add a "nil-safe Store" test
// — it would assert behavior the implementation deliberately does not
// provide. (The caches inside Store ARE nil-safe — see cache_test.go.)

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ---------------------------------------------------------------------------
// helper: assertBSONTags
// ---------------------------------------------------------------------------

// assertBSONTags verifies that the struct type T has the expected BSON tags
// on the named Go fields. The expected map is Go-field-name → full bson tag
// (e.g. "UserID" → "user_id,omitempty"). A missing or mismatched tag fails
// the test, as does an expected field name that does not exist on the struct.
func assertBSONTags[T any](tb testing.TB, expected map[string]string) {
	tb.Helper()
	var zero T
	rt := reflect.TypeOf(zero)
	if rt.Kind() != reflect.Struct {
		tb.Fatalf("assertBSONTags: %s is not a struct", rt)
	}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		want, listed := expected[f.Name]
		if !listed {
			continue // caller did not list this field — skip
		}
		got := f.Tag.Get("bson")
		if got != want {
			tb.Errorf("%s.%s: bson tag = %q, want %q", rt.Name(), f.Name, got, want)
		}
	}
	// Verify every expected field name actually exists on the struct so a
	// typo in the test's expected map is caught loudly.
	for name := range expected {
		if _, ok := rt.FieldByName(name); !ok {
			tb.Errorf("%s: expected field %q not found on struct", rt.Name(), name)
		}
	}
}

// ---------------------------------------------------------------------------
// 1. FileRecord struct + BSON tags
// ---------------------------------------------------------------------------

func TestFileRecordStruct(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	f := FileRecord{
		FileKey:    "key_abc",
		Hash:       "hash_xyz",
		FileName:   "movie.mp4",
		MimeType:   "video/mp4",
		Size:       12345,
		MediaType:  "video",
		VaultMsgID: 42,
		DCID:       4,
		CreatedAt:  now,
		LastSeenAt: now,

		// Wave 2 tracking fields (populated by ingest/stream).
		SeenCount:         7,
		ReuseCount:        3,
		FirstSourceChatID: -1001234567890,
		FirstSourceMsgID:  555,
	}
	if f.FileKey != "key_abc" || f.Hash != "hash_xyz" || f.FileName != "movie.mp4" {
		t.Fatalf("FileRecord string field wrong: %+v", f)
	}
	if f.MimeType != "video/mp4" || f.MediaType != "video" {
		t.Fatalf("FileRecord media field wrong: %+v", f)
	}
	if f.Size != 12345 {
		t.Errorf("FileRecord.Size = %d, want 12345", f.Size)
	}
	if f.VaultMsgID != 42 {
		t.Errorf("FileRecord.VaultMsgID = %d, want 42", f.VaultMsgID)
	}
	if f.DCID != 4 {
		t.Errorf("FileRecord.DCID = %d, want 4", f.DCID)
	}
	if !f.CreatedAt.Equal(now) {
		t.Errorf("FileRecord.CreatedAt = %v, want %v", f.CreatedAt, now)
	}
	if !f.LastSeenAt.Equal(now) {
		t.Errorf("FileRecord.LastSeenAt = %v, want %v", f.LastSeenAt, now)
	}

	// Tracking fields (Wave 2).
	if f.SeenCount != 7 {
		t.Errorf("FileRecord.SeenCount = %d, want 7", f.SeenCount)
	}
	if f.ReuseCount != 3 {
		t.Errorf("FileRecord.ReuseCount = %d, want 3", f.ReuseCount)
	}
	if f.FirstSourceChatID != -1001234567890 {
		t.Errorf("FileRecord.FirstSourceChatID = %d, want -1001234567890", f.FirstSourceChatID)
	}
	if f.FirstSourceMsgID != 555 {
		t.Errorf("FileRecord.FirstSourceMsgID = %d, want 555", f.FirstSourceMsgID)
	}

	assertBSONTags[FileRecord](t, map[string]string{
		"FileKey":           "file_key",
		"Hash":              "hash",
		"FileName":          "file_name",
		"MimeType":          "mime_type",
		"Size":              "size",
		"MediaType":         "media_type",
		"VaultMsgID":        "vault_msg_id",
		"DCID":              "dc_id",
		"CreatedAt":         "created_at",
		"LastSeenAt":        "last_seen_at",
		"SeenCount":         "seen_count",
		"ReuseCount":        "reuse_count",
		"FirstSourceChatID": "first_source_chat_id,omitempty",
		"FirstSourceMsgID":  "first_source_msg_id,omitempty",
	})
}

// ---------------------------------------------------------------------------
// 2. User struct + BSON tags
// ---------------------------------------------------------------------------

func TestUserStruct(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	u := User{
		UserID:    100,
		FirstName: "Alice",
		LastName:  "Smith",
		Username:  "alice",
		JoinedAt:  now,
	}
	if u.UserID != 100 {
		t.Errorf("User.UserID = %d, want 100", u.UserID)
	}
	if u.FirstName != "Alice" || u.LastName != "Smith" || u.Username != "alice" {
		t.Errorf("User string fields wrong: %+v", u)
	}
	if !u.JoinedAt.Equal(now) {
		t.Errorf("User.JoinedAt = %v, want %v", u.JoinedAt, now)
	}

	assertBSONTags[User](t, map[string]string{
		"UserID":    "user_id",
		"FirstName": "first_name,omitempty",
		"LastName":  "last_name,omitempty",
		"Username":  "username,omitempty",
		"JoinedAt":  "joined_at",
	})
}

// ---------------------------------------------------------------------------
// 3. BannedUser struct + BSON tags
// ---------------------------------------------------------------------------

func TestBannedUserStruct(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	b := BannedUser{
		UserID:   200,
		BannedBy: 100,
		BannedAt: now,
		Reason:   "spam",
	}
	if b.UserID != 200 || b.BannedBy != 100 || b.Reason != "spam" {
		t.Errorf("BannedUser fields wrong: %+v", b)
	}
	if !b.BannedAt.Equal(now) {
		t.Errorf("BannedUser.BannedAt = %v, want %v", b.BannedAt, now)
	}

	assertBSONTags[BannedUser](t, map[string]string{
		"UserID":   "user_id",
		"BannedBy": "banned_by",
		"BannedAt": "banned_at",
		"Reason":   "reason,omitempty",
	})
}

// ---------------------------------------------------------------------------
// 4. BannedChannel struct + BSON tags
// ---------------------------------------------------------------------------

func TestBannedChannelStruct(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	b := BannedChannel{
		ChannelID: -1001234,
		BannedBy:  100,
		BannedAt:  now,
		Reason:    "tos",
	}
	if b.ChannelID != -1001234 || b.BannedBy != 100 || b.Reason != "tos" {
		t.Errorf("BannedChannel fields wrong: %+v", b)
	}
	if !b.BannedAt.Equal(now) {
		t.Errorf("BannedChannel.BannedAt = %v, want %v", b.BannedAt, now)
	}

	assertBSONTags[BannedChannel](t, map[string]string{
		"ChannelID": "channel_id",
		"BannedBy":  "banned_by",
		"BannedAt":  "banned_at",
		"Reason":    "reason,omitempty",
	})
}

// ---------------------------------------------------------------------------
// 5. AuthorizedUser struct + BSON tags
// ---------------------------------------------------------------------------

func TestAuthorizedUserStruct(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	a := AuthorizedUser{
		UserID:    300,
		FirstName: "Bob",
		AddedBy:   100,
		AddedAt:   now,
	}
	if a.UserID != 300 || a.FirstName != "Bob" || a.AddedBy != 100 {
		t.Errorf("AuthorizedUser fields wrong: %+v", a)
	}
	if !a.AddedAt.Equal(now) {
		t.Errorf("AuthorizedUser.AddedAt = %v, want %v", a.AddedAt, now)
	}

	assertBSONTags[AuthorizedUser](t, map[string]string{
		"UserID":    "user_id",
		"FirstName": "first_name,omitempty",
		"AddedBy":   "added_by",
		"AddedAt":   "added_at",
	})
}

// ---------------------------------------------------------------------------
// 6. RestartMarker struct + BSON tags
// ---------------------------------------------------------------------------

func TestRestartMarkerStruct(t *testing.T) {
	t.Parallel()
	now := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	m := RestartMarker{
		ID:        "marker-1",
		ChatID:    -1009999,
		MessageID: 55,
		CreatedAt: now,
	}
	if m.ID != "marker-1" || m.ChatID != -1009999 || m.MessageID != 55 {
		t.Errorf("RestartMarker fields wrong: %+v", m)
	}
	if !m.CreatedAt.Equal(now) {
		t.Errorf("RestartMarker.CreatedAt = %v, want %v", m.CreatedAt, now)
	}

	// Note: ID uses bson:"_id" — the MongoDB canonical primary key.
	assertBSONTags[RestartMarker](t, map[string]string{
		"ID":        "_id",
		"ChatID":    "chat_id",
		"MessageID": "message_id",
		"CreatedAt": "created_at",
	})
}

// ---------------------------------------------------------------------------
// 7. StreamUsers signature check (compile-time)
// ---------------------------------------------------------------------------

// TestStreamUsersSignature verifies that (*Store).StreamUsers exists and has
// the signature `func(context.Context, func(User) error) error`. The
// assignment below is evaluated at compile time — if someone renames
// StreamUsers, changes the parameter type, or alters the return type, this
// file will fail to compile. The nil receiver is safe because we only take
// the method value; we never call it.
func TestStreamUsersSignature(t *testing.T) {
	t.Parallel()
	// Compile-time signature assertion.
	var fn func(context.Context, func(User) error) error = (*Store)(nil).StreamUsers
	if fn == nil {
		t.Fatal("StreamUsers method value is nil (method does not exist or signature mismatch)")
	}
}

// ---------------------------------------------------------------------------
// 8. All entity types usable as map/slice values (no panics)
// ---------------------------------------------------------------------------

func TestAllEntityTypes(t *testing.T) {
	t.Parallel()
	users := []User{{UserID: 1}, {UserID: 2}}
	bannedUsers := []BannedUser{{UserID: 1}, {UserID: 2}}
	bannedChans := []BannedChannel{{ChannelID: 1}, {ChannelID: 2}}
	files := []FileRecord{{FileKey: "a"}, {FileKey: "b"}}
	auth := []AuthorizedUser{{UserID: 1}, {UserID: 2}}
	markers := []RestartMarker{{ID: "a"}, {ID: "b"}}

	// Maps keyed by each entity's natural ID (mirrors how callers use them).
	userMap := make(map[int64]User, len(users))
	for _, u := range users {
		userMap[u.UserID] = u
	}
	bannedMap := make(map[int64]BannedUser, len(bannedUsers))
	for _, b := range bannedUsers {
		bannedMap[b.UserID] = b
	}
	chanMap := make(map[int64]BannedChannel, len(bannedChans))
	for _, c := range bannedChans {
		chanMap[c.ChannelID] = c
	}
	fileMap := make(map[string]FileRecord, len(files))
	for _, f := range files {
		fileMap[f.FileKey] = f
	}
	authMap := make(map[int64]AuthorizedUser, len(auth))
	for _, a := range auth {
		authMap[a.UserID] = a
	}
	markerMap := make(map[string]RestartMarker, len(markers))
	for _, m := range markers {
		markerMap[m.ID] = m
	}

	// Reaching this point without a panic is the primary assertion.
	// Sanity-check that each map has the expected size.
	if len(userMap) != 2 {
		t.Errorf("userMap len = %d, want 2", len(userMap))
	}
	if len(bannedMap) != 2 {
		t.Errorf("bannedMap len = %d, want 2", len(bannedMap))
	}
	if len(chanMap) != 2 {
		t.Errorf("chanMap len = %d, want 2", len(chanMap))
	}
	if len(fileMap) != 2 {
		t.Errorf("fileMap len = %d, want 2", len(fileMap))
	}
	if len(authMap) != 2 {
		t.Errorf("authMap len = %d, want 2", len(authMap))
	}
	if len(markerMap) != 2 {
		t.Errorf("markerMap len = %d, want 2", len(markerMap))
	}
}

// ---------------------------------------------------------------------------
// 9. Cache fast-path of FindFileByHash / FindFileByKey (no DB needed)
// ---------------------------------------------------------------------------
//
// The cache-hit branch of FindFileByHash / FindFileByKey returns the cached
// value WITHOUT touching s.files, so it can be exercised with a nil mongo
// client by pre-populating the relevant cache. This verifies the value-copy
// semantics documented in the method comments (callers cannot mutate the
// cached entry — CWE-662 / CWE-704).

func TestFindFileByHashCacheFastPath(t *testing.T) {
	t.Parallel()
	s := &Store{
		fileByHash: newCache[FileRecord](10, time.Minute),
	}
	rec := FileRecord{
		FileKey:    "key_abc",
		Hash:       "hash_xyz",
		FileName:   "movie.mp4",
		MimeType:   "video/mp4",
		Size:       12345,
		MediaType:  "video",
		VaultMsgID: 42,
		DCID:       4,
	}
	s.fileByHash.set("hash_xyz", rec)

	got, err := s.FindFileByHash(context.Background(), "hash_xyz")
	if err != nil {
		t.Fatalf("FindFileByHash cache hit: unexpected err %v", err)
	}
	if got == nil {
		t.Fatal("FindFileByHash cache hit: got nil, want *FileRecord")
	}
	if got.FileKey != "key_abc" || got.Hash != "hash_xyz" || got.FileName != "movie.mp4" {
		t.Fatalf("FindFileByHash cache hit: returned wrong record: %+v", got)
	}
	if got.Size != 12345 || got.VaultMsgID != 42 || got.DCID != 4 || got.MediaType != "video" {
		t.Fatalf("FindFileByHash cache hit: numeric/media fields wrong: %+v", got)
	}

	// Value-copy semantics: mutating the returned *FileRecord must NOT affect
	// the cached entry. If it did, a caller mutating the returned pointer
	// would corrupt the cache for subsequent readers (CWE-662).
	got.FileName = "MUTATED"
	got.Size = -999
	got2, err := s.FindFileByHash(context.Background(), "hash_xyz")
	if err != nil {
		t.Fatalf("second FindFileByHash: unexpected err %v", err)
	}
	if got2.FileName != "movie.mp4" {
		t.Errorf("cache returned mutated value: FileName = %q, want %q (value-copy violated)",
			got2.FileName, "movie.mp4")
	}
	if got2.Size != 12345 {
		t.Errorf("cache returned mutated value: Size = %d, want 12345 (value-copy violated)",
			got2.Size)
	}
}

func TestFindFileByKeyCacheFastPath(t *testing.T) {
	t.Parallel()
	s := &Store{
		fileByKey: newCache[FileRecord](10, time.Minute),
	}
	rec := FileRecord{
		FileKey:    "key_abc",
		Hash:       "hash_xyz",
		FileName:   "movie.mp4",
		Size:       999,
		VaultMsgID: 7,
	}
	s.fileByKey.set("key_abc", rec)

	got, err := s.FindFileByKey(context.Background(), "key_abc")
	if err != nil {
		t.Fatalf("FindFileByKey cache hit: unexpected err %v", err)
	}
	if got == nil {
		t.Fatal("FindFileByKey cache hit: got nil, want *FileRecord")
	}
	if got.FileKey != "key_abc" || got.Hash != "hash_xyz" || got.Size != 999 || got.VaultMsgID != 7 {
		t.Fatalf("FindFileByKey cache hit: returned wrong record: %+v", got)
	}

	// Value-copy semantics: mutating the returned pointer must not affect cache.
	got.Size = -1
	got.VaultMsgID = 0
	got2, err := s.FindFileByKey(context.Background(), "key_abc")
	if err != nil {
		t.Fatalf("second FindFileByKey: unexpected err %v", err)
	}
	if got2.Size != 999 {
		t.Errorf("cache returned mutated value: Size = %d, want 999 (value-copy violated)",
			got2.Size)
	}
	if got2.VaultMsgID != 7 {
		t.Errorf("cache returned mutated value: VaultMsgID = %d, want 7 (value-copy violated)",
			got2.VaultMsgID)
	}
}

// ---------------------------------------------------------------------------
// 10. isIndexExistsErr — pure helper, no DB needed
// ---------------------------------------------------------------------------

func TestIsIndexExistsErr(t *testing.T) {
	t.Parallel()
	// nil → false.
	if isIndexExistsErr(nil) {
		t.Error("isIndexExistsErr(nil) = true, want false")
	}
	// Arbitrary non-mongo error → false.
	if isIndexExistsErr(errors.New("some other error")) {
		t.Error("isIndexExistsErr(arbitrary error) = true, want false")
	}
	// A real mongo.CommandError with code 85 (IndexAlreadyExists) → true.
	// This is the harmless "index already exists with identical spec" case.
	ce85 := mongo.CommandError{Code: 85, Message: "IndexAlreadyExists"}
	if !isIndexExistsErr(ce85) {
		t.Error("isIndexExistsErr(code 85) = false, want true")
	}
	// A wrapped code-85 error should also be detected via errors.As.
	if !isIndexExistsErr(fmt.Errorf("wrapped: %w", ce85)) {
		t.Error("isIndexExistsErr(wrapped code 85) = false, want true")
	}
	// Code 86 (IndexKeySpecsConflict) is NOT an "exists" error — it must be
	// surfaced as a hard error by the caller (C-007). Only code 85 is
	// harmless. The previous code path checked `!isIndexExistsErr(err)` first
	// and would have wrongly swallowed a code 86 as "exists" — this test
	// pins the correct behavior.
	ce86 := mongo.CommandError{Code: 86, Message: "IndexKeySpecsConflict"}
	if isIndexExistsErr(ce86) {
		t.Error("isIndexExistsErr(code 86) = true, want false (code 86 is a hard error)")
	}
	// Code 11000 (DuplicateKey) is also NOT an "exists" error.
	ce11000 := mongo.CommandError{Code: 11000, Message: "DuplicateKey"}
	if isIndexExistsErr(ce11000) {
		t.Error("isIndexExistsErr(code 11000) = true, want false")
	}
}
