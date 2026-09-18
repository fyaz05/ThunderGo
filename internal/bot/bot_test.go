package bot

// Bot-layer smoke tests for the mtgo-replatform port.
//
// The suite drives the real gate chain (dispatch → banned → private-mode →
// activation → force-sub → rate-limit → handler) against tgutil.FakeTransport,
// with the botStore test seam (Bot.db) standing in for MongoDB. No mtgo
// imports, no network, no sleeps: dispatch is asynchronous but tracked by
// Bot.handlerWg, which injectAndWait uses as its synchronization point.
//
// Assertions quote the messages.go constants wherever possible so the test
// breaks if the user-facing strings drift.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fyaz05/ThunderGo/internal/config"
	"github.com/fyaz05/ThunderGo/internal/ingest"
	"github.com/fyaz05/ThunderGo/internal/pool"
	"github.com/fyaz05/ThunderGo/internal/ratelimit"
	"github.com/fyaz05/ThunderGo/internal/store"
	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// Fixed identities used across the suite.
const (
	testOwnerID    int64 = 777000
	testUserID     int64 = 424242
	testBotID      int64 = 606060
	testBotName          = "thundergo_test_bot"
	testForceSubID       = int64(-1001234567890)
	testVaultID          = int64(-1001111111111)
	testBaseURL          = "https://thundergo.example.com"
	testUserMsgID        = 100 // MsgID stamped on injected commands
)

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

// testEnv carries everything a testOpt may tweak before Bot.Start runs.
// Force-sub resolution happens inside Start, so ft.Chats must be seeded via
// an option, not after construction.
type testEnv struct {
	cfg *config.Config
	ft  *tgutil.FakeTransport
	log *slog.Logger
}

type testOpt func(*testEnv)

// newTestBot builds a Bot exactly like app.go does (cfg, backend, pool, store,
// ingester, shortener, limiter, log) with Mongo-free substitutes:
//
//   - store: nil *store.Store + stubStore installed into the Bot.db seam
//   - pool: zero-value shell (the smoke tests only touch /stats, which reads
//     Len/TotalInflight — both nil-safe)
//   - ingester: constructed via ingest.New (pure field assignment, no I/O);
//     it is never invoked by the paths under test (PostVaultLog only runs
//     after a successful vault ingest, /start notifies the vault through
//     Backend.SendHTML directly)
//   - shortener: nil, the same shape app.go produces when TG_SHORTENER_* is
//     unset (shortener.New("", "") returns nil, nil), so activation links are
//     never shortened
//
// After Start it resets the recorded calls (Start itself records SetCommands)
// so every test asserts on its own traffic only, and registers a cleanup that
// drains in-flight handlers.
func newTestBot(t *testing.T, opts ...testOpt) (*Bot, *tgutil.FakeTransport, *stubStore) {
	t.Helper()

	cfg := &config.Config{
		APIID:          12345,
		APIHash:        "test-api-hash",
		BotToken:       "test-bot-token",
		VaultChannelID: testVaultID,
		OwnerUserID:    testOwnerID,
		MongoURI:       "mongodb://127.0.0.1:27017/unused",
		BaseURL:        testBaseURL,
		BatchCap:       50,
		TokenTTLHours:  24,
	}

	ft := tgutil.NewFakeTransport()
	ft.FakeMe = tgutil.BotInfo{ID: testBotID, Username: testBotName, DC: 2}
	ft.Users = map[int64]tgutil.UserInfo{}
	ft.Usernames = map[string]tgutil.UserInfo{}
	ft.MemberStatus = map[int64]map[int64]string{}
	ft.Chats = map[string]tgutil.ChatInfo{}
	ft.Messages = map[int64]map[int]tgutil.IncomingMsg{}

	env := &testEnv{cfg: cfg, ft: ft, log: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(env)
	}

	st := newStubStore()
	b := New(cfg, ft, &pool.Pool{}, nil,
		ingest.New(ft, nil, cfg.VaultChannelID, env.log),
		nil, ratelimit.New(cfg.RateLimit, time.Minute), env.log)
	b.db = st // install the Mongo-free store seam (nil in production)

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("bot.Start: %v", err)
	}
	ft.Reset() // drop Start's own SetCommands record
	t.Cleanup(b.Stop)
	return b, ft, st
}

// injectCommand delivers a private-chat command from senderID and waits for
// the asynchronous dispatch to finish (dispatch spawns a goroutine tracked by
// handlerWg; Wait is deterministic and sleep-free).
func injectCommand(t *testing.T, b *Bot, ft *tgutil.FakeTransport, senderID int64, text string) {
	t.Helper()
	_ = ft.InjectMessage(context.Background(), tgutil.IncomingMsg{
		ChatID:    senderID,
		SenderID:  senderID,
		MsgID:     testUserMsgID,
		Text:      text,
		IsPrivate: true,
	})
	b.handlerWg.Wait()
}

// injectCallback delivers an inline-button press (callback handlers run
// synchronously inside the backend, no wait needed).
func injectCallback(t *testing.T, ft *tgutil.FakeTransport, q tgutil.CallbackQuery) {
	t.Helper()
	_ = ft.InjectCallback(context.Background(), q)
}

// sends returns every recorded outbound HTML send (SendHTML and the ID-returning
// SendHTMLMsg both record under CallSendHTML).
func sends(ft *tgutil.FakeTransport) []tgutil.FakeCall {
	return ft.CallsOf(tgutil.CallSendHTML)
}

// requireSingleSend asserts exactly one HTML send happened and returns it.
func requireSingleSend(t *testing.T, ft *tgutil.FakeTransport) tgutil.FakeCall {
	t.Helper()
	snd := sends(ft)
	if len(snd) != 1 {
		t.Fatalf("got %d SendHTML calls (%+v), want exactly 1", len(snd), ft.Calls)
	}
	return snd[0]
}

// assertNoGateLeak fails if any recorded call carries one of the gate messages
// (all verb-free constants, so Contains against formatted output is sound).
func assertNoGateLeak(t *testing.T, ft *tgutil.FakeTransport, needles ...string) {
	t.Helper()
	for _, c := range ft.Calls {
		for _, leak := range needles {
			if strings.Contains(c.HTML, leak) || strings.Contains(c.Text, leak) {
				t.Errorf("gate message leaked into outbound traffic: %q in %s call", leak, c.Method)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// stubStore — the botStore test seam (MongoDB-free)
// ---------------------------------------------------------------------------

// stubStore answers the bot's store queries from plain maps with fail-closed
// defaults: nobody is banned, authorized, or activated unless a test says so.
// All counters are mutex-guarded because dispatch runs handlers concurrently.
type stubStore struct {
	mu          sync.Mutex
	banned      map[int64]bool
	bannedChans map[int64]bool
	authorized  map[int64]bool
	activated   map[int64]bool
	users       map[int64]store.User
	tokens      map[string]bool // outstanding activation tokens

	banUserCalls []store.BannedUser // /ban audit trail (owner-only guard)
	tokensSaved  []string           // promptActivation audit trail

	lookups struct {
		banned, bannedChans, authorized, activated int
	}
}

func newStubStore() *stubStore {
	return &stubStore{
		banned:      map[int64]bool{},
		bannedChans: map[int64]bool{},
		authorized:  map[int64]bool{},
		activated:   map[int64]bool{},
		users:       map[int64]store.User{},
		tokens:      map[string]bool{},
	}
}

// Compile-time gate: the stub must satisfy the exact botStore seam.
var _ botStore = (*stubStore)(nil)

func (s *stubStore) IsUserBanned(_ context.Context, userID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups.banned++
	return s.banned[userID], nil
}

func (s *stubStore) IsChannelBanned(_ context.Context, channelID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups.bannedChans++
	return s.bannedChans[channelID], nil
}

func (s *stubStore) IsAuthorized(_ context.Context, userID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups.authorized++
	return s.authorized[userID], nil
}

func (s *stubStore) IsUserActivated(_ context.Context, userID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups.activated++
	return s.activated[userID], nil
}

func (s *stubStore) UpsertUser(_ context.Context, u store.User) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.users[u.UserID]
	s.users[u.UserID] = u
	return !existed, nil
}

func (s *stubStore) SaveActivationToken(_ context.Context, token string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[token] = true
	s.tokensSaved = append(s.tokensSaved, token)
	return nil
}

func (s *stubStore) ConsumeActivationToken(_ context.Context, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.tokens[token] {
		return errors.New("stubStore: unknown activation token")
	}
	delete(s.tokens, token)
	return nil
}

func (s *stubStore) ActivateUser(_ context.Context, userID int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activated[userID] = true
	return nil
}

func (s *stubStore) HasUser(_ context.Context, userID int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.users[userID]
	return ok, nil
}

func (s *stubStore) StreamUsers(_ context.Context, fn func(u store.User) error) error {
	s.mu.Lock()
	all := make([]store.User, 0, len(s.users))
	for _, u := range s.users {
		all = append(all, u)
	}
	s.mu.Unlock()
	for _, u := range all {
		if err := fn(u); err != nil {
			return err
		}
	}
	return nil
}

func (s *stubStore) DeleteUser(_ context.Context, userID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.users, userID)
	return nil
}

func (s *stubStore) CountUsers(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.users)), nil
}

func (s *stubStore) BanUser(_ context.Context, bu store.BannedUser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.banUserCalls = append(s.banUserCalls, bu)
	s.banned[bu.UserID] = true
	return nil
}

func (s *stubStore) BanChannel(_ context.Context, bc store.BannedChannel) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bannedChans[bc.ChannelID] = true
	return nil
}

func (s *stubStore) UnbanUser(_ context.Context, userID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.banned, userID)
	return nil
}

func (s *stubStore) UnbanChannel(_ context.Context, channelID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.bannedChans, channelID)
	return nil
}

func (s *stubStore) Authorize(_ context.Context, a store.AuthorizedUser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authorized[a.UserID] = true
	return nil
}

func (s *stubStore) Deauthorize(_ context.Context, userID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.authorized, userID)
	return nil
}

func (s *stubStore) ListAuthorized(_ context.Context) ([]store.AuthorizedUser, error) {
	return nil, nil
}

func (s *stubStore) InvalidateActivatedUser(_ context.Context, userID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.activated, userID)
	return nil
}

func (s *stubStore) SaveRestartMarker(_ context.Context, _ store.RestartMarker) error {
	return nil
}

func (s *stubStore) PopRestartMarker(_ context.Context) (*store.RestartMarker, error) {
	return nil, nil
}

// --- test-side mutators / readouts ---

func (s *stubStore) ban(userID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.banned[userID] = true
}

func (s *stubStore) counts() (banned, activated, authorized int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookups.banned, s.lookups.activated, s.lookups.authorized
}

func (s *stubStore) banUserCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.banUserCalls)
}

func (s *stubStore) tokensSavedSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.tokensSaved...)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestDispatchOwnerBypassesGates: with every gate hostile (private-mode on,
// token gate on, force-sub configured and the owner NOT a member) the owner's
// /stats reaches the handler untouched — no preflight store probes, no gate
// message, no leave.
func TestDispatchOwnerBypassesGates(t *testing.T) {
	t.Parallel()

	b, ft, st := newTestBot(t, func(e *testEnv) {
		e.cfg.PrivateMode = true
		e.cfg.TokenEnabled = true
		e.cfg.ForceSubChannelID = testForceSubID
		e.ft.Chats[fmt.Sprint(testForceSubID)] = tgutil.ChatInfo{ID: testForceSubID, Title: "Sub Channel", Username: "subchannel"}
		e.ft.MemberStatus[testForceSubID] = map[int64]string{testOwnerID: "left"} // hostile
	})

	injectCommand(t, b, ft, testOwnerID, "/stats")

	snd := requireSingleSend(t, ft)
	// handleStats has no messages.go constant; assert its stable markers.
	if !strings.Contains(snd.HTML, "Runtime Statistics") || !strings.Contains(snd.HTML, "Uptime") {
		t.Errorf("owner /stats response missing runtime/uptime markers:\n%s", snd.HTML)
	}
	if snd.ChatID != testOwnerID {
		t.Errorf("stats sent to chat %d, want %d", snd.ChatID, testOwnerID)
	}

	// No preflight interference: the owner path never consults the gate chain.
	banned, activated, authorized := st.counts()
	if banned != 0 || activated != 0 {
		t.Errorf("owner dispatch consulted gate stores (banned=%d activated=%d); preflight must be skipped entirely", banned, activated)
	}
	if authorized != 1 {
		t.Errorf("IsAuthorized lookups = %d, want 1 (dispatch resolves owner/authorized once)", authorized)
	}
	assertNoGateLeak(t, ft, msgBannedNotice, msgPrivateMode, msgActivationRequired)
	if n := len(ft.CallsOf(tgutil.CallLeaveChat)); n != 0 {
		t.Errorf("LeaveChat called %d times for the owner", n)
	}
}

// TestDispatchPrivateModeBlocksNonOwner: a non-owner in a private chat gets
// the private-mode notice verbatim and the command handler never runs.
func TestDispatchPrivateModeBlocksNonOwner(t *testing.T) {
	t.Parallel()

	b, ft, _ := newTestBot(t, func(e *testEnv) {
		e.cfg.PrivateMode = true
	})

	injectCommand(t, b, ft, testUserID, "/ping")

	snd := requireSingleSend(t, ft)
	if snd.HTML != msgPrivateMode {
		t.Errorf("response = %q, want msgPrivateMode %q", snd.HTML, msgPrivateMode)
	}
	if n := len(ft.CallsOf(tgutil.CallEditHTML)); n != 0 {
		t.Errorf("ping handler executed despite private-mode block (%d edits)", n)
	}
}

// TestDispatchUnknownCommandSilent: unregistered commands produce zero
// outbound traffic and zero handler errors.
func TestDispatchUnknownCommandSilent(t *testing.T) {
	t.Parallel()

	b, ft, _ := newTestBot(t)

	injectCommand(t, b, ft, testUserID, "/nosuchcmd")

	if n := len(ft.Calls); n != 0 {
		t.Errorf("unknown command produced %d outbound calls (%+v), want 0", n, ft.Calls)
	}
	if errs := ft.HandlerErrs(); len(errs) != 0 {
		t.Errorf("handler errors recorded: %v", errs)
	}
}

// TestDispatchOwnerOnlySilentForNonOwner: /ban from a non-owner is dropped
// before preflight — no reply, no store write.
func TestDispatchOwnerOnlySilentForNonOwner(t *testing.T) {
	t.Parallel()

	b, ft, st := newTestBot(t)

	injectCommand(t, b, ft, testUserID, "/ban 123 spam")

	if n := len(ft.Calls); n != 0 {
		t.Errorf("owner-only command from non-owner produced %d outbound calls (%+v), want 0", n, ft.Calls)
	}
	if n := st.banUserCount(); n != 0 {
		t.Errorf("store.BanUser called %d times for a non-owner /ban, want 0", n)
	}
	if errs := ft.HandlerErrs(); len(errs) != 0 {
		t.Errorf("handler errors recorded: %v", errs)
	}
}

// TestStartBypassesActivationGate: with the token gate on and the user not
// activated, /start still delivers the welcome message (and the vault
// new-user notice) instead of the activation prompt.
func TestStartBypassesActivationGate(t *testing.T) {
	t.Parallel()

	b, ft, st := newTestBot(t, func(e *testEnv) {
		e.cfg.TokenEnabled = true
		e.ft.Users[testUserID] = tgutil.UserInfo{ID: testUserID, FirstName: "Testy"}
	})

	injectCommand(t, b, ft, testUserID, "/start")

	snd := sends(ft)
	if len(snd) != 2 {
		t.Fatalf("got %d sends (%+v), want 2 (vault notice + welcome)", len(snd), ft.Calls)
	}
	if snd[0].ChatID != testVaultID {
		t.Errorf("first send went to %d, want the vault channel %d (new-user notice)", snd[0].ChatID, testVaultID)
	}
	wantWelcome := fmt.Sprintf(msgWelcome, "Testy", b.Cfg.BatchCap)
	welcome := snd[1]
	if welcome.HTML != wantWelcome {
		t.Errorf("welcome = %q, want msgWelcome rendered as %q", welcome.HTML, wantWelcome)
	}
	if welcome.ChatID != testUserID {
		t.Errorf("welcome sent to %d, want %d", welcome.ChatID, testUserID)
	}
	if !reflect.DeepEqual(welcome.Markup, buildStartMarkup(false, "", "")) {
		t.Errorf("welcome markup = %+v, want %+v", welcome.Markup, buildStartMarkup(false, "", ""))
	}
	assertNoGateLeak(t, ft, msgActivationRequired)

	// Order proof: the ban gate still ran, the activation gate did not.
	banned, activated, _ := st.counts()
	if banned != 1 {
		t.Errorf("ban gate lookups = %d, want 1 (ban check applies to /start too)", banned)
	}
	if activated != 0 {
		t.Errorf("activation gate lookups = %d, want 0 (/start bypasses activation)", activated)
	}
	if toks := st.tokensSavedSnapshot(); len(toks) != 0 {
		t.Errorf("activation token saved for /start: %v", toks)
	}
}

// TestNonStartTriggersActivationPrompt: with the token gate on, a
// non-activated user's /help is answered with the activation prompt and a
// one-time token button pointing at BaseURL/activate/<token>.
func TestNonStartTriggersActivationPrompt(t *testing.T) {
	t.Parallel()

	b, ft, st := newTestBot(t, func(e *testEnv) {
		e.cfg.TokenEnabled = true
	})

	injectCommand(t, b, ft, testUserID, "/help")

	snd := requireSingleSend(t, ft)
	if snd.HTML != msgActivationRequired {
		t.Errorf("response = %q, want msgActivationRequired %q", snd.HTML, msgActivationRequired)
	}
	markup, ok := snd.Markup.(tgutil.Markup)
	if !ok {
		t.Fatalf("activation markup type = %T, want tgutil.Markup", snd.Markup)
	}
	var buttonURL string
	for _, row := range markup.Rows {
		for _, btn := range row {
			if btn.URL != "" {
				buttonURL = btn.URL
				if btn.Text != msgActivationButton {
					t.Errorf("activation button text = %q, want %q", btn.Text, msgActivationButton)
				}
			}
		}
	}
	wantPrefix := b.Cfg.BaseURL + "/activate/"
	if !strings.HasPrefix(buttonURL, wantPrefix) {
		t.Errorf("activation URL = %q, want prefix %q", buttonURL, wantPrefix)
	}

	toks := st.tokensSavedSnapshot()
	if len(toks) != 1 {
		t.Fatalf("saved activation tokens = %v, want exactly 1", toks)
	}
	if !strings.HasSuffix(buttonURL, toks[0]) {
		t.Errorf("button URL %q does not end with the saved token %q", buttonURL, toks[0])
	}
	if _, activated, _ := st.counts(); activated != 1 {
		t.Errorf("IsUserActivated lookups = %d, want 1 (fail-closed gate consult)", activated)
	}
}

// TestForceSubGate: a configured force-sub channel rejects non-members with
// the join prompt (carrying the resolved channel title and join link); valid
// member statuses pass through to the handler.
func TestForceSubGate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		status  string
		blocked bool
	}{
		{"left is rejected", "left", true},
		{"kicked is rejected", "kicked", true},
		{"restricted is rejected", "restricted", true},
		{"member passes", "member", false},
		{"administrator passes", "administrator", false},
		{"creator passes", "creator", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, ft, _ := newTestBot(t, func(e *testEnv) {
				e.cfg.ForceSubChannelID = testForceSubID
				e.ft.Chats[fmt.Sprint(testForceSubID)] = tgutil.ChatInfo{ID: testForceSubID, Title: "Sub Channel", Username: "subchannel"}
				e.ft.MemberStatus[testForceSubID] = map[int64]string{testUserID: tc.status}
			})

			injectCommand(t, b, ft, testUserID, "/help")

			if !tc.blocked {
				snd := requireSingleSend(t, ft)
				if snd.HTML != b.buildHelpText() {
					t.Errorf("member got %q, want the help text (gate must pass)", snd.HTML)
				}
				return
			}

			snd := requireSingleSend(t, ft)
			if snd.HTML != fmt.Sprintf(msgForceSub, "Sub Channel") {
				t.Errorf("response = %q, want msgForceSub rendered with the resolved title", snd.HTML)
			}
			markup, ok := snd.Markup.(tgutil.Markup)
			if !ok {
				t.Fatalf("force-sub markup type = %T, want tgutil.Markup", snd.Markup)
			}
			var joinURL string
			for _, row := range markup.Rows {
				for _, btn := range row {
					if btn.URL != "" {
						joinURL = btn.URL
						if btn.Text != msgForceSubButton {
							t.Errorf("join button text = %q, want %q", btn.Text, msgForceSubButton)
						}
					}
				}
			}
			if want := "https://t.me/subchannel"; joinURL != want {
				t.Errorf("join URL = %q, want %q", joinURL, want)
			}
		})
	}
}

// TestPingRepliesThenEdits: /ping first replies with the pinging notice
// (quoting the user), then edits that same message with the pong text.
func TestPingRepliesThenEdits(t *testing.T) {
	t.Parallel()

	b, ft, _ := newTestBot(t)

	injectCommand(t, b, ft, testUserID, "/ping")

	snd := sends(ft)
	if len(snd) != 1 {
		t.Fatalf("got %d sends, want 1 (the ping placeholder)", len(snd))
	}
	if snd[0].HTML != msgPinging {
		t.Errorf("first response = %q, want msgPinging %q", snd[0].HTML, msgPinging)
	}
	if snd[0].ReplyTo != testUserMsgID {
		t.Errorf("ping reply-to = %d, want %d (quotes the user's message)", snd[0].ReplyTo, testUserMsgID)
	}

	edits := ft.CallsOf(tgutil.CallEditHTML)
	if len(edits) != 1 {
		t.Fatalf("got %d edits, want 1 (the pong)", len(edits))
	}
	if edits[0].MsgID != snd[0].MsgID {
		t.Errorf("edited message %d, want the ping placeholder id %d", edits[0].MsgID, snd[0].MsgID)
	}
	// msgPong has a single %.2f verb: match the template around the measured
	// latency instead of the whole string.
	pct := strings.IndexByte(msgPong, '%')
	if pct < 0 {
		t.Fatalf("msgPong lost its latency placeholder: %q", msgPong)
	}
	prefix, suffix := msgPong[:pct], msgPong[pct+len("%.2f"):]
	if !strings.HasPrefix(edits[0].HTML, prefix) || !strings.HasSuffix(edits[0].HTML, suffix) {
		t.Errorf("pong edit = %q, want msgPong template %q with a number in place", edits[0].HTML, msgPong)
	}

	sendIdx, editIdx := -1, -1
	for i, c := range ft.Calls {
		switch c.Method {
		case tgutil.CallSendHTML:
			if sendIdx < 0 {
				sendIdx = i
			}
		case tgutil.CallEditHTML:
			editIdx = i
		}
	}
	if sendIdx < 0 || editIdx < 0 || sendIdx > editIdx {
		t.Errorf("call order violated: send index %d, edit index %d (want reply-then-edit)", sendIdx, editIdx)
	}
}

// TestHelpCallback: the "help" inline button answers the query and edits the
// host message to the help text; unknown button data gets the unsupported
// alert from the catch-all route and no edit.
func TestHelpCallback(t *testing.T) {
	t.Parallel()

	b, ft, _ := newTestBot(t)

	injectCallback(t, ft, tgutil.CallbackQuery{
		ID: 1, ChatID: testUserID, SenderID: testUserID, MessageID: 55, Data: "help",
	})

	ans, ok := ft.LastCall(tgutil.CallAnswerCallback)
	if !ok {
		t.Fatal("help callback produced no AnswerCallback")
	}
	if ans.Text != msgCallbackHelpAnswered || ans.Alert {
		t.Errorf("answer = (%q, alert=%v), want (%q, alert=false)", ans.Text, ans.Alert, msgCallbackHelpAnswered)
	}
	edits := ft.CallsOf(tgutil.CallCallbackEdit)
	if len(edits) != 1 {
		t.Fatalf("got %d CallbackEditHTML calls, want 1", len(edits))
	}
	if edits[0].HTML != b.buildHelpText() {
		t.Errorf("help edit body drifted from buildHelpText():\n%s", edits[0].HTML)
	}
	wantMarkup := buildHelpMarkup(b.Cfg.ForceSubChannelID != 0, b.forceSubLink, b.forceSubTitle)
	if !reflect.DeepEqual(edits[0].Markup, wantMarkup) {
		t.Errorf("help edit markup = %+v, want %+v", edits[0].Markup, wantMarkup)
	}

	ft.Reset() // scope the second phase's assertions

	injectCallback(t, ft, tgutil.CallbackQuery{
		ID: 2, ChatID: testUserID, SenderID: testUserID, MessageID: 55, Data: "legacy_button",
	})

	ans2, ok := ft.LastCall(tgutil.CallAnswerCallback)
	if !ok {
		t.Fatal("unknown callback produced no AnswerCallback")
	}
	if ans2.Text != msgCallbackUnsupported || !ans2.Alert {
		t.Errorf("answer = (%q, alert=%v), want (%q, alert=true)", ans2.Text, ans2.Alert, msgCallbackUnsupported)
	}
	if n := len(ft.CallsOf(tgutil.CallCallbackEdit)); n != 0 {
		t.Errorf("unknown callback edited a message %d times", n)
	}
}

// TestBroadcastCancelOwnerOnly: broadcast_cancel presses are answered per the
// ported handleBroadcastCancel behavior —
//   - non-owner: msgCallbackBroadcastCancelDenied alert (the port does NOT
//     reuse msgCallbackCloseDenied here),
//   - owner without an active broadcast: msgCallbackBroadcastNotFound alert,
//   - owner with a registered cancel: the cancel func fires and the answer is
//     msgCallbackBroadcastCancelled (no alert), with no handler error.
func TestBroadcastCancelOwnerOnly(t *testing.T) {
	t.Parallel()

	b, ft, _ := newTestBot(t)

	// Non-owner press → denial alert.
	injectCallback(t, ft, tgutil.CallbackQuery{
		ID: 1, ChatID: testUserID, SenderID: testUserID, MessageID: 9, Data: "broadcast_cancel",
	})
	ans, ok := ft.LastCall(tgutil.CallAnswerCallback)
	if !ok {
		t.Fatal("non-owner cancel produced no AnswerCallback")
	}
	if ans.Text != msgCallbackBroadcastCancelDenied || !ans.Alert {
		t.Errorf("non-owner answer = (%q, alert=%v), want (%q, alert=true)", ans.Text, ans.Alert, msgCallbackBroadcastCancelDenied)
	}

	ft.Reset()

	// Owner press, nothing registered → not-found alert.
	injectCallback(t, ft, tgutil.CallbackQuery{
		ID: 2, ChatID: testOwnerID, SenderID: testOwnerID, MessageID: 9, Data: "broadcast_cancel",
	})
	ans2, ok := ft.LastCall(tgutil.CallAnswerCallback)
	if !ok {
		t.Fatal("owner cancel (no broadcast) produced no AnswerCallback")
	}
	if ans2.Text != msgCallbackBroadcastNotFound || !ans2.Alert {
		t.Errorf("owner not-found answer = (%q, alert=%v), want (%q, alert=true)", ans2.Text, ans2.Alert, msgCallbackBroadcastNotFound)
	}

	ft.Reset()

	// Owner press with a live broadcast → the registered cancel fires.
	cancelled := make(chan struct{})
	b.registerBroadcastCancel(9, func() { close(cancelled) })
	injectCallback(t, ft, tgutil.CallbackQuery{
		ID: 3, ChatID: testOwnerID, SenderID: testOwnerID, MessageID: 9, Data: "broadcast_cancel",
	})
	select {
	case <-cancelled:
	default:
		t.Error("registered broadcast cancel func was not invoked for the owner")
	}
	ans3, ok := ft.LastCall(tgutil.CallAnswerCallback)
	if !ok {
		t.Fatal("owner cancel (live broadcast) produced no AnswerCallback")
	}
	if ans3.Text != msgCallbackBroadcastCancelled || ans3.Alert {
		t.Errorf("cancelled answer = (%q, alert=%v), want (%q, alert=false)", ans3.Text, ans3.Alert, msgCallbackBroadcastCancelled)
	}
	if errs := ft.HandlerErrs(); len(errs) != 0 {
		t.Errorf("handler errors recorded: %v", errs)
	}
}

// TestGateOrder: banned + private-mode + token gate all hostile at once.
// The FIRST (and only) response is the banned notice — the ban check precedes
// private-mode and activation, which must never be consulted.
func TestGateOrder(t *testing.T) {
	t.Parallel()

	b, ft, st := newTestBot(t, func(e *testEnv) {
		e.cfg.PrivateMode = true
		e.cfg.TokenEnabled = true
	})
	st.ban(testUserID)

	injectCommand(t, b, ft, testUserID, "/help")

	snd := requireSingleSend(t, ft)
	if snd.HTML != msgBannedNotice {
		t.Errorf("first response = %q, want msgBannedNotice %q", snd.HTML, msgBannedNotice)
	}
	// Order proof: after the ban rejection, nothing downstream ran.
	banned, activated, _ := st.counts()
	if banned != 1 {
		t.Errorf("ban gate lookups = %d, want 1", banned)
	}
	if activated != 0 {
		t.Errorf("activation gate consulted after ban rejection (%d lookups) — gate order violation", activated)
	}
	if toks := st.tokensSavedSnapshot(); len(toks) != 0 {
		t.Errorf("activation token saved despite ban: %v", toks)
	}
	assertNoGateLeak(t, ft, msgPrivateMode, msgActivationRequired)
}

// TestAnonymousSenderDenied: SenderID 0 (anonymous) is an ordinary non-owner
// as far as dispatch is concerned — with a gate active the command is denied
// and the handler never runs. (No gate configured, the dispatch chain has no
// dedicated anonymous rejection: the gate IS the denial.)
func TestAnonymousSenderDenied(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		opt      testOpt
		wantHTML string // the gate message the anonymous sender receives
	}{
		{
			name:     "private mode blocks anonymous",
			opt:      func(e *testEnv) { e.cfg.PrivateMode = true },
			wantHTML: msgPrivateMode,
		},
		{
			name:     "token gate prompts anonymous",
			opt:      func(e *testEnv) { e.cfg.TokenEnabled = true },
			wantHTML: msgActivationRequired,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, ft, st := newTestBot(t, tc.opt)

			injectCommand(t, b, ft, 0 /*anonymous sender*/, "/ping")

			snd := requireSingleSend(t, ft)
			if snd.HTML != tc.wantHTML {
				t.Errorf("anonymous response = %q, want %q", snd.HTML, tc.wantHTML)
			}
			if n := len(ft.CallsOf(tgutil.CallEditHTML)); n != 0 {
				t.Errorf("ping handler executed for anonymous sender (%d edits)", n)
			}
			if _, _, authorized := st.counts(); authorized != 1 {
				t.Errorf("IsAuthorized lookups = %d, want 1 (dispatch resolves the anonymous sender)", authorized)
			}
		})
	}
}
