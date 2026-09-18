package stream

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// The serving contract is pinned here on tgutil.FakeTransport + a fake
// FileStore: byte-exact output, resume-at-offset, range semantics, error
// mapping (plan §8) and the windowed pipeline ordering (plan §4). No network,
// no MongoDB, no mtgo.

const (
	testFileKB  = 3 << 20 // 3 MiB test file
	testToken16 = "0123456789abcdef"
)

func quietStreamLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeStore is a Mongo-free FileStore returning one record (or nil).
type fakeStore struct {
	mu      sync.Mutex
	rec     *store.FileRecord
	seen    []string
	deleted []string
}

func (f *fakeStore) FindFileByHash(_ context.Context, hash string) (*store.FileRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rec == nil || f.rec.Hash != hash {
		return nil, nil
	}
	cp := *f.rec
	return &cp, nil
}

func (f *fakeStore) IncrementSeenCount(_ context.Context, fileKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, fileKey)
	return nil
}

func (f *fakeStore) DeleteFileByHash(_ context.Context, hash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, hash)
	if f.rec != nil && f.rec.Hash == hash {
		f.rec = nil
	}
	return nil
}

func (f *fakeStore) deletedHashes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

func (f *fakeStore) seenKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

// testRecorder is the record used across the serving tests.
func testRecorder() *store.FileRecord {
	return &store.FileRecord{
		FileKey:    "doc:111:222",
		Hash:       testToken16,
		FileName:   "video.mp4",
		MimeType:   "video/mp4",
		Size:       testFileKB,
		MediaType:  "video",
		VaultMsgID: 7,
		DCID:       2,
	}
}

// testHandle matches the record and the fake's deterministic byte generator.
func testHandle() tgutil.FileHandle {
	return tgutil.FileHandle{
		ChatID:   -1001234567890,
		MsgID:    7,
		DC:       2,
		Location: tgutil.NewTestDocumentLocation(111, 222),
		Size:     testFileKB,
		Mime:     "video/mp4",
		Name:     "video.mp4",
	}
}

// expectedFakeFile reproduces tgutil.FakeTransport's default chunk generator
// over the whole file: byte i = i & 0xFF.
func expectedFakeFile(size int64) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(int64(i) & 0xFF)
	}
	return out
}

func newTestHandler(ft *tgutil.FakeTransport, st FileStore) *Handler {
	p := pool.NewForTests(ft)
	return &Handler{
		Pool:        p,
		Store:       st,
		Log:         quietStreamLogger(),
		Ingester:    &ingest.Ingester{},
		concurrency: 4,
		bufferCount: 8,
		timeout:     5 * time.Second,
		maxRetries:  3,
		vault:       newVaultCache(),
	}
}

func serveWithToken(h *Handler, method, target string, setHeaders func(*http.Request)) *httptest.ResponseRecorder {
	ft := h.Pool.Transport(0).(*tgutil.FakeTransport)
	ft.DefaultHandle = testHandle()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, nil)
	req = req.WithContext(WithToken(req.Context(), testToken16))
	if setHeaders != nil {
		setHeaders(req)
	}
	h.ServeHTTP(rr, req)
	return rr
}

// ---------- pure helpers ----------

func TestChunkSizeFor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		remaining int64
		want      int32
	}{
		{0, 64 << 10},
		{1, 64 << 10},
		{512<<10 - 1, 64 << 10},
		{512 << 10, 256 << 10},
		{4<<20 - 1, 256 << 10},
		{4 << 20, 512 << 10},
		{32<<20 - 1, 512 << 10},
		{32 << 20, 1 << 20},
		{1 << 30, 1 << 20},
	}
	for _, tc := range cases {
		if got := chunkSizeFor(tc.remaining); got != tc.want {
			t.Errorf("chunkSizeFor(%d) = %d, want %d", tc.remaining, got, tc.want)
		}
		if got := chunkSizeFor(tc.remaining); got%4096 != 0 || got > 1<<20 {
			t.Errorf("chunkSizeFor(%d) = %d violates 4KiB-alignment/1MiB cap", tc.remaining, got)
		}
	}
}

func TestBuildChunkPlan(t *testing.T) {
	t.Parallel()
	// 3 MiB span: tiers shrink as the remaining count shrinks — 11 × 256 KiB
	// while remaining ≥ 512 KiB, then 4 × 64 KiB for the tail.
	plan := buildChunkPlan(0, 3<<20-1)
	if len(plan) != 15 {
		t.Fatalf("plan length = %d, want 15", len(plan))
	}
	var offset int64
	for i, e := range plan {
		if e.offset != offset {
			t.Fatalf("plan[%d].offset = %d, want %d (contiguity broken)", i, e.offset, offset)
		}
		if e.size <= 0 || int64(e.size)%4096 != 0 {
			t.Fatalf("plan[%d].size = %d violates alignment", i, e.size)
		}
		offset += int64(e.size)
	}
	if offset != 3<<20 {
		t.Fatalf("plan covers %d bytes, want %d", offset, int64(3<<20))
	}
	for _, e := range plan[:11] {
		if e.size != 256<<10 {
			t.Fatalf("early window size = %d, want 256KiB", e.size)
		}
	}
	for _, e := range plan[11:] {
		if e.size != 64<<10 {
			t.Fatalf("tail window size = %d, want 64KiB (tier shrank with remaining)", e.size)
		}
	}

	// Sub-range mid-file: window is clamped to the remainder at the end.
	plan = buildChunkPlan(1000, 1000+65536)
	if len(plan) != 2 {
		t.Fatalf("plan length = %d, want 2", len(plan))
	}
	if plan[0].offset != 1000 || plan[0].size != 64<<10 {
		t.Fatalf("plan[0] = %+v, want offset 1000 size 65536", plan[0])
	}
	if plan[1].offset != 1000+65536 || plan[1].size != 1 {
		t.Fatalf("plan[1] = %+v, want offset 66536 size 1 (clamped)", plan[1])
	}

	if plan := buildChunkPlan(5, 4); len(plan) != 0 {
		t.Fatalf("empty range produced plan %+v", plan)
	}
}

func TestNormalizeStreamTuning(t *testing.T) {
	t.Parallel()
	c, b, r, to := normalizeStreamTuning(0, 0, -1, 0)
	if c != defaultStreamConcurrency || b != defaultStreamBufferCount ||
		r != defaultStreamMaxRetries || to != defaultStreamTimeout {
		t.Fatalf("defaults: got %d/%d/%d/%v", c, b, r, to)
	}
	c, b, r, to = normalizeStreamTuning(7, 3, 5, 45*time.Second)
	if c != 7 || b != 3 || r != 5 || to != 45*time.Second {
		t.Fatalf("passthrough: got %d/%d/%d/%v", c, b, r, to)
	}
}

func TestSupportIDFormat(t *testing.T) {
	t.Parallel()
	re := regexp.MustCompile(`^[0-9a-f]{16}$`)
	for i := 0; i < 8; i++ {
		if !re.MatchString(supportID()) {
			t.Fatalf("supportID %q does not match 16-hex shape", supportID())
		}
	}
}

func TestVaultCacheLifecycle(t *testing.T) {
	t.Parallel()
	now := time.Now()
	c := newVaultCache()
	fh := testHandle()

	if _, ok := c.get("k", now); ok {
		t.Fatal("empty cache returned a hit")
	}
	c.put("k", fh, now)
	if got, ok := c.get("k", now.Add(time.Second)); !ok || got.MsgID != fh.MsgID {
		t.Fatalf("get after put = (%v, %v)", got, ok)
	}
	if _, ok := c.get("k", now.Add(vaultCacheTTL+time.Second)); ok {
		t.Fatal("entry survived its TTL")
	}

	// Evict-oldest on overflow.
	c2 := newVaultCache()
	for i := 0; i < vaultCacheCapacity+1; i++ {
		c2.put(strconv.Itoa(i), fh, now)
	}
	if _, ok := c2.get("0", now); ok {
		t.Fatal("oldest entry survived eviction")
	}
	if _, ok := c2.get(strconv.Itoa(vaultCacheCapacity), now); !ok {
		t.Fatal("newest entry missing")
	}

	c2.invalidate(strconv.Itoa(vaultCacheCapacity))
	if _, ok := c2.get(strconv.Itoa(vaultCacheCapacity), now); ok {
		t.Fatal("invalidate did not remove the entry")
	}

	// nil-safe receivers.
	var nilCache *vaultCache
	nilCache.put("k", fh, now)
	nilCache.invalidate("k")
	if _, ok := nilCache.get("k", now); ok {
		t.Fatal("nil cache returned a hit")
	}
}

// ---------- context plumbing & early guards (ported) ----------

func TestWithTokenTokenFromContext(t *testing.T) {
	t.Parallel()
	ctx := WithToken(context.Background(), "ABC-123")
	if got := tokenFromContext(ctx); got != "abc-123" { // lowercased Crockford form (O→0, I/L→1)
		t.Fatalf("tokenFromContext = %q, want %q", got, "abc-123")
	}
	if got := tokenFromContext(WithToken(context.Background(), "OIL-5")); got != "011-5" {
		t.Fatalf("Crockford mapping: got %q, want %q", got, "011-5")
	}
	if got := tokenFromContext(context.Background()); got != "" {
		t.Fatalf("empty ctx token = %q", got)
	}
}

func TestServeHTTP_MissingToken(t *testing.T) {
	t.Parallel()
	h := newTestHandler(tgutil.NewFakeTransport(), &fakeStore{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/f/x/y/raw", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}
}

func TestServeHTTP_RejectsUnsupportedMethodBeforeStoreAccess(t *testing.T) {
	t.Parallel()
	st := &fakeStore{}
	h := newTestHandler(tgutil.NewFakeTransport(), st)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/f/x/y/raw", nil)
	req = req.WithContext(WithToken(req.Context(), testToken16))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if allow := rr.Header().Get("Allow"); allow != "GET, HEAD, OPTIONS" {
		t.Fatalf("Allow = %q", allow)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}
}

func TestServeHTTP_NilStoreWithTokenDoesNotPanic(t *testing.T) {
	t.Parallel()
	h := newTestHandler(tgutil.NewFakeTransport(), nil)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/f/x/y/raw", nil)
	req = req.WithContext(WithToken(req.Context(), testToken16))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
}

func TestServeHTTP_UnknownHashOrZeroSize(t *testing.T) {
	t.Parallel()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(tgutil.NewFakeTransport(), st)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/f/missing/raw", nil)
	req = req.WithContext(WithToken(req.Context(), "ffffffffffffffff"))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown hash: status = %d, want 404", rr.Code)
	}

	zero := testRecorder()
	zero.Size = 0
	st.rec = zero
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/f/zero/raw", nil)
	req = req.WithContext(WithToken(req.Context(), testToken16))
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("zero-size record: status = %d, want 404", rr.Code)
	}
}

// ---------- byte-exactness (plan §4 gate, hermetic edition) ----------

func TestServeHTTP_ByteExactFullFile(t *testing.T) {
	t.Parallel()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(tgutil.NewFakeTransport(), st)

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rr.Code, rr.Body.String())
	}
	want := expectedFakeFile(testFileKB)
	got := rr.Body.Bytes()
	if len(got) != len(want) {
		t.Fatalf("body length = %d, want %d", len(got), len(want))
	}
	sumGot := sha256.Sum256(got)
	sumWant := sha256.Sum256(want)
	if sumGot != sumWant {
		t.Fatal("sha256 mismatch on full-file stream")
	}
	if keys := st.seenKeys(); len(keys) != 1 || keys[0] != "doc:111:222" {
		t.Fatalf("seen keys = %v, want exactly one increment for doc:111:222", keys)
	}
}

func TestServeHTTP_ByteExactMidRange(t *testing.T) {
	t.Parallel()
	st := &fakeStore{rec: testRecorder()}
	ft := tgutil.NewFakeTransport()
	h := newTestHandler(ft, st)

	var firstOffset int64
	var fetches int
	var mu sync.Mutex
	ft.SetFetchChunk(func(_ context.Context, fh tgutil.FileHandle, offset int64, limit int32) ([]byte, error) {
		mu.Lock()
		if fetches == 0 {
			firstOffset = offset
		}
		fetches++
		mu.Unlock()
		return fakeTestChunk(fh, offset, limit), nil
	})

	const start, end = 1000000, 1000099
	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw",
		func(r *http.Request) { r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end)) })

	if rr.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rr.Code)
	}
	want := expectedFakeFile(testFileKB)[start : end+1]
	if string(rr.Body.Bytes()) != string(want) {
		t.Fatalf("range body mismatch: got %d bytes", len(rr.Body.Bytes()))
	}
	if cr := rr.Header().Get("Content-Range"); cr != fmt.Sprintf("bytes %d-%d/%d", start, end, testFileKB) {
		t.Fatalf("Content-Range = %q", cr)
	}
	if cl := rr.Header().Get("Content-Length"); cl != strconv.Itoa(end-start+1) {
		t.Fatalf("Content-Length = %q", cl)
	}
	// Resume-at-offset: the first network fetch starts at the 64 KiB-aligned
	// window boundary, never at 0.
	tier := int64(chunkSizeFor(end - start + 1))
	wantOffset := (start / tier) * tier
	if firstOffset != wantOffset {
		t.Fatalf("first fetch offset = %d, want %d (resume-at-offset)", firstOffset, wantOffset)
	}
}

func TestServeHTTP_ByteExactSuffixRange(t *testing.T) {
	t.Parallel()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(tgutil.NewFakeTransport(), st)

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw",
		func(r *http.Request) { r.Header.Set("Range", "bytes=-4096") })
	if rr.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rr.Code)
	}
	want := expectedFakeFile(testFileKB)[testFileKB-4096:]
	if string(rr.Body.Bytes()) != string(want) {
		t.Fatalf("suffix body mismatch: got %d bytes, want 4096", len(rr.Body.Bytes()))
	}
}

func TestServeHTTP_FullRangeCollapsesTo200(t *testing.T) {
	t.Parallel()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(tgutil.NewFakeTransport(), st)

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw",
		func(r *http.Request) { r.Header.Set("Range", fmt.Sprintf("bytes=0-%d", testFileKB-1)) })
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for full-range request", rr.Code)
	}
	if cr := rr.Header().Get("Content-Range"); cr != "" {
		t.Fatalf("Content-Range = %q on a collapsed 200", cr)
	}
}

func TestServeHTTP_HEADMetadataOnly(t *testing.T) {
	t.Parallel()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(tgutil.NewFakeTransport(), st)

	rr := serveWithToken(h, http.MethodHead, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if cl := rr.Header().Get("Content-Length"); cl != strconv.Itoa(testFileKB) {
		t.Fatalf("Content-Length = %q", cl)
	}
	if rr.Body.Len() != 0 {
		t.Fatalf("HEAD produced %d body bytes", rr.Body.Len())
	}
	if cd := rr.Header().Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment") {
		t.Fatalf("Content-Disposition = %q", cd)
	}
}

// ---------- range errors (constant bodies, no fetches) ----------

func TestServeHTTP_Range416(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw",
		func(r *http.Request) { r.Header.Set("Range", "bytes=99999999-") })
	if rr.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", rr.Code)
	}
	if cr := rr.Header().Get("Content-Range"); cr != fmt.Sprintf("bytes */%d", testFileKB) {
		t.Fatalf("Content-Range = %q", cr)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatal("416 response missing Cache-Control: no-store (plan §8)")
	}
}

func TestServeHTTP_Range400NeverReflectsInput(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)

	garbage := "bytes=SECRET-INPUT"
	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw",
		func(r *http.Request) { r.Header.Set("Range", garbage) })
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatal("400 response missing Cache-Control: no-store")
	}
	if strings.Contains(rr.Body.String(), "SECRET") {
		t.Fatalf("400 body reflects request input: %q", rr.Body.String())
	}
}

// ---------- admission mapping (503 capacity / brownout) ----------

func TestServeHTTP_503CapacityRetryAfter2(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)

	for i := 0; i < 8; i++ { // hard cap = 8 in NewForTests
		lease, err := h.Pool.AcquireBest()
		if err != nil {
			t.Fatalf("lease %d: unexpected error %v", i, err)
		}
		defer lease.Release()
	}

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if ra := rr.Header().Get("Retry-After"); ra != "2" {
		t.Fatalf("Retry-After = %q, want 2 (capacity)", ra)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatal("503 response missing Cache-Control: no-store")
	}
}

func TestServeHTTP_503BrownoutRetryAfterEstimate(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)

	h.Pool.ReportFlood(ft, 600*time.Second) // whole fleet flood-cooling

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	ra, err := strconv.Atoi(rr.Header().Get("Retry-After"))
	if err != nil || ra < 5 || ra > 600 {
		t.Fatalf("Retry-After = %q, want within [5,600] (brownout)", rr.Header().Get("Retry-After"))
	}
}

// ---------- resolve error mapping (plan §8) ----------

func TestServeHTTP_ResolvePermanentSelfHealsTo404(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)
	ft.SetResolve(func(_ context.Context, _ int64, _ int) (tgutil.FileHandle, error) {
		return tgutil.FileHandle{}, tgutil.ErrStaleMedia
	})

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatal("404 response missing Cache-Control: no-store")
	}
	if deleted := st.deletedHashes(); len(deleted) != 1 || deleted[0] != testToken16 {
		t.Fatalf("self-heal deleted %v, want exactly [%s]", deleted, testToken16)
	}
}

func TestServeHTTP_ResolveTransient503(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)
	ft.SetResolve(func(_ context.Context, _ int64, _ int) (tgutil.FileHandle, error) {
		return tgutil.FileHandle{}, tgutil.ErrTransient
	})

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if ra := rr.Header().Get("Retry-After"); ra != "2" {
		t.Fatalf("Retry-After = %q, want 2", ra)
	}
	if deleted := st.deletedHashes(); len(deleted) != 0 {
		t.Fatalf("transient failure deleted records: %v (never delete on transient)", deleted)
	}
}

func TestServeHTTP_ResolveFlood503WithCooldown(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)
	ft.SetResolve(func(_ context.Context, _ int64, _ int) (tgutil.FileHandle, error) {
		return tgutil.FileHandle{}, tgutil.NewFloodWaitTestError(30)
	})

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if ra := rr.Header().Get("Retry-After"); ra != "30" {
		t.Fatalf("Retry-After = %q, want 30 (flood wait surfaced as a value)", ra)
	}

	// The slot must now be flood-cooling: the next admission is a brownout.
	if _, err := h.Pool.AcquireBest(); !errors.Is(err, pool.ErrPoolBrownout) {
		t.Fatalf("post-flood AcquireBest error = %v, want ErrPoolBrownout", err)
	}
}

func TestServeHTTP_ResolveUnknownError500SupportID(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)
	ft.SetResolve(func(_ context.Context, _ int64, _ int) (tgutil.FileHandle, error) {
		return tgutil.FileHandle{}, errors.New("boom: unclassified")
	})

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	re := regexp.MustCompile(`^internal error \(support id: [0-9a-f]{16}\)\n$`)
	if !re.MatchString(rr.Body.String()) {
		t.Fatalf("500 body = %q, want constant shape with support id", rr.Body.String())
	}
	if acao := rr.Header().Get("Access-Control-Allow-Origin"); acao != "*" {
		t.Fatal("500 response missing CORS header")
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatal("500 response missing Cache-Control: no-store")
	}
}

// ---------- pipeline behavior ----------

func TestServeHTTP_429FloodBeforeFirstByte(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)
	h.maxRetries = 1 // bounded: attempt0 + 1 retry, each sleeping min(1s, 15s)
	ft.SetFetchChunk(func(_ context.Context, fh tgutil.FileHandle, offset int64, limit int32) ([]byte, error) {
		return nil, &tgutil.FloodWait{Seconds: 1}
	})

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 (body %q)", rr.Code, rr.Body.String())
	}
	if ra := rr.Header().Get("Retry-After"); ra != "2" { // wait 1 floored to the 2s minimum
		t.Fatalf("Retry-After = %q, want 2", ra)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatal("429 response missing Cache-Control: no-store")
	}
}

func TestServeHTTP_MidStreamFailureTruncates(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	st := &fakeStore{rec: testRecorder()}
	h := newTestHandler(ft, st)

	// Position-aware scripting (NOT call-count aware): worker scheduling
	// may interleave chunk fetches, but offset 0 is unambiguously chunk 0.
	ft.SetFetchChunk(func(_ context.Context, fh tgutil.FileHandle, offset int64, limit int32) ([]byte, error) {
		if offset == 0 {
			return fakeTestChunk(fh, offset, limit), nil
		}
		return nil, tgutil.ErrTransient
	})

	rr := serveWithToken(h, http.MethodGet, "/f/0123456789abcdef/video.mp4/raw", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (failure happened mid-stream)", rr.Code)
	}
	if len(rr.Body.Bytes()) == 0 || int64(len(rr.Body.Bytes())) >= testFileKB {
		t.Fatalf("body = %d bytes, want a truncated prefix", len(rr.Body.Bytes()))
	}
	if strings.Contains(rr.Body.String(), "internal error") {
		t.Fatal("error body written after stream started")
	}
}

func TestPipelineFileRefRefreshBudget(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	h := newTestHandler(ft, &fakeStore{})
	h.concurrency = 1 // single worker, single chunk → deterministic counts
	h.maxRetries = 5  // deep enough for the refresh budget (3) to be the limiter

	var fetches, refreshes int
	var mu sync.Mutex
	ft.SetFetchChunk(func(_ context.Context, fh tgutil.FileHandle, offset int64, limit int32) ([]byte, error) {
		mu.Lock()
		fetches++
		mu.Unlock()
		return nil, tgutil.ErrFileRefExpired
	})
	ft.SetRefresh(func(_ context.Context, _ *tgutil.FileHandle) error {
		mu.Lock()
		refreshes++
		mu.Unlock()
		return nil
	})

	lease, err := h.Pool.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest: %v", err)
	}
	defer lease.Release()

	// One 100-byte chunk → one plan entry → one fetchWithRetry loop.
	fh := testHandle()
	fh.Size = 100
	p := h.newPipeline(context.Background(), lease, fh, 0, 99)
	defer p.close()

	res, ok := p.next()
	if !ok {
		t.Fatal("pipeline produced no result")
	}
	if res.err == nil {
		t.Fatal("expected the chunk to fail after the refresh budget was spent")
	}
	// ErrFileRefExpired classifies as transient → 503 via servePipelineError;
	// the pinned semantics here are the budget: 3 refreshes then give up.
	mu.Lock()
	defer mu.Unlock()
	if refreshes != maxRefreshesPerRequest {
		t.Fatalf("refreshes = %d, want %d (budget)", refreshes, maxRefreshesPerRequest)
	}
	if fetches != maxRefreshesPerRequest+1 {
		t.Fatalf("fetches = %d, want %d", fetches, maxRefreshesPerRequest+1)
	}
}

func TestPipelineOrderedEmit(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	h := newTestHandler(ft, &fakeStore{})
	h.bufferCount = 4
	h.concurrency = 8

	lease, err := h.Pool.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest: %v", err)
	}
	defer lease.Release()

	// Three 64 KiB chunks; make completion order the REVERSE of plan order.
	ft.SetFetchChunk(func(_ context.Context, fh tgutil.FileHandle, offset int64, limit int32) ([]byte, error) {
		if offset == 0 {
			time.Sleep(30 * time.Millisecond) // first chunk finishes last
		}
		return fakeTestChunk(fh, offset, limit), nil
	})

	const span = 3 * 64 << 10
	fh := testHandle()
	fh.Size = span
	p := h.newPipeline(context.Background(), lease, fh, 0, span-1)
	defer p.close()

	var got []byte
	for {
		res, ok := p.next()
		if !ok {
			break
		}
		if res.err != nil {
			t.Fatalf("chunk error: %v", res.err)
		}
		got = append(got, res.data...)
	}
	want := expectedFakeFile(span)
	if string(got) != string(want) {
		t.Fatalf("ordered emit mismatch: got %d bytes, want %d in strict plan order", len(got), len(want))
	}
}

func TestPipelineCrossDCPacing(t *testing.T) {
	t.Parallel()
	ft := tgutil.NewFakeTransport()
	h := newTestHandler(ft, &fakeStore{})
	h.bufferCount = 4
	h.concurrency = 1 // serialize so pacing accumulates

	lease, err := h.Pool.AcquireBest()
	if err != nil {
		t.Fatalf("AcquireBest: %v", err)
	}
	defer lease.Release()

	fh := testHandle()
	fh.DC = 1 // slot cached DC is 2 (FakeDC default) → cross-DC

	start := time.Now()
	p := h.newPipeline(context.Background(), lease, fh, 0, 2*64<<10-1)
	for {
		res, ok := p.next()
		if !ok {
			break
		}
		if res.err != nil {
			t.Fatalf("chunk error: %v", res.err)
		}
	}
	p.close()
	elapsed := time.Since(start)
	// 2 chunks × 25 ms pacing (worker-serialized). Pin the floor only —
	// ceilings flake under the race scheduler's slower goroutine wakeups.
	if elapsed < 40*time.Millisecond {
		t.Fatalf("cross-DC pacing missing: 2 fetches in %v", elapsed)
	}
}

// ---------- common headers (ported byte-exact assertions) ----------

func TestSetCommonHeaders(t *testing.T) {
	t.Parallel()
	h := &Handler{Log: quietStreamLogger()}
	rr := httptest.NewRecorder()
	rec := testRecorder()
	h.setCommonHeaders(rr, rec, "attachment")

	want := map[string]string{
		"Content-Type":           "video/mp4",
		"Content-Disposition":    `attachment; filename="video.mp4"; filename*=UTF-8''video.mp4`,
		"Accept-Ranges":          "bytes",
		"Cache-Control":          "public, max-age=31536000",
		"Connection":             "keep-alive",
		"X-Content-Type-Options": "nosniff",
	}
	for k, v := range want {
		if got := rr.Header().Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if _, ok := rr.Header()["Access-Control-Allow-Origin"]; ok {
		t.Error("handler must not set CORS headers (gateway middleware owns them)")
	}
}

func TestSetCommonHeaders_NonASCIIFilename(t *testing.T) {
	t.Parallel()
	h := &Handler{Log: quietStreamLogger()}
	rr := httptest.NewRecorder()
	rec := testRecorder()
	rec.FileName = ".Report - रिपोर्ट.pdf"
	h.setCommonHeaders(rr, rec, "attachment")

	cd := rr.Header().Get("Content-Disposition")
	// ASCII fallback replaces non-ASCII runes with underscores; the RFC 5987
	// extended parameter carries the percent-encoded original.
	if !strings.HasPrefix(cd, `attachment; filename=".Report - _______.pdf"`) {
		t.Fatalf("ASCII fallback wrong in %q", cd)
	}
	if !strings.Contains(cd, "filename*=UTF-8''") || !strings.Contains(cd, "%e0%a4%b0") {
		t.Fatalf("RFC 5987 filename* missing/incorrect in %q", cd)
	}
}

func TestSetCommonHeaders_FallbackMime(t *testing.T) {
	t.Parallel()
	h := &Handler{Log: quietStreamLogger()}
	rr := httptest.NewRecorder()
	rec := testRecorder()
	rec.MimeType = ""
	h.setCommonHeaders(rr, rec, "inline")
	if got := rr.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want application/octet-stream fallback", got)
	}
}

// fakeTestChunk mirrors tgutil.FakeTransport's default generator (it is
// unexported there): byte (offset+i) & 0xFF, clamped to the handle size.
func fakeTestChunk(fh tgutil.FileHandle, offset int64, limit int32) []byte {
	n := int64(limit)
	if rem := fh.Size - offset; rem < n {
		n = rem
	}
	if n < 0 {
		n = 0
	}
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((offset + int64(i)) & 0xFF)
	}
	return out
}
