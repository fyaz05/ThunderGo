package bot

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fyaz05/ThunderGo/internal/tgutil"
)

// Phase 4 characterization (plan §5): activation tokens are consume-once, and
// the failure modes (unknown token, replayed token) are distinct from the
// success path. Pinned hermetically against the stub store whose
// ConsumeActivationToken deletes the token — the same FindOneAndDelete
// atomicity the real store implements (the integration-tagged suite covers
// the Mongo semantics).

func activationReplies(t *testing.T, ft *tgutil.FakeTransport) []string {
	t.Helper()
	var out []string
	for _, c := range sends(ft) {
		out = append(out, c.HTML)
	}
	return out
}

func TestActivationConsumeOnce(t *testing.T) {
	t.Parallel()
	b, ft, st := newTestBot(t)
	b.Cfg.TokenEnabled = true
	if err := st.SaveActivationToken(context.Background(), "TOKEN1234", 0); err != nil {
		t.Fatalf("seeding token: %v", err)
	}

	// First /start with the token: consumed, user activated, success reply.
	injectCommand(t, b, ft, testUserID, "/start TOKEN1234")
	replies := activationReplies(t, ft)
	// msgActivated is a Sprintf template (%s = formatted TTL); assert on the
	// rendered form the handler produces.
	wantActivated := fmt.Sprintf(msgActivated, formatDuration(24*time.Hour))
	if len(replies) == 0 || !strings.Contains(replies[len(replies)-1], wantActivated) {
		t.Fatalf("first consume replies = %v, want %q", replies, wantActivated)
	}
	st.mu.Lock()
	activated := st.activated[testUserID]
	outstanding := st.tokens["TOKEN1234"]
	st.mu.Unlock()
	if !activated {
		t.Fatal("user not activated after successful consume")
	}
	if outstanding {
		t.Fatal("token still outstanding after consume (not consume-once)")
	}

	// Replay: the same token again → invalid, activation state untouched.
	ft.Reset()
	injectCommand(t, b, ft, testUserID, "/start TOKEN1234")
	replies = activationReplies(t, ft)
	if len(replies) == 0 || !strings.Contains(replies[len(replies)-1], msgActivationInvalid) {
		t.Fatalf("replayed consume replies = %v, want %q", replies, msgActivationInvalid)
	}
}

func TestActivationUnknownTokenDistinctFromSuccess(t *testing.T) {
	t.Parallel()
	b, ft, _ := newTestBot(t)
	b.Cfg.TokenEnabled = true

	injectCommand(t, b, ft, testUserID, "/start NOSUCHTOKEN0")
	replies := activationReplies(t, ft)
	if len(replies) == 0 || !strings.Contains(replies[len(replies)-1], msgActivationInvalid) {
		t.Fatalf("unknown token replies = %v, want %q", replies, msgActivationInvalid)
	}
}

func TestActivationPlainStartNeverConsumes(t *testing.T) {
	t.Parallel()
	b, ft, st := newTestBot(t)
	b.Cfg.TokenEnabled = true
	if err := st.SaveActivationToken(context.Background(), "TOKEN9999", 0); err != nil {
		t.Fatalf("seeding token: %v", err)
	}
	st.mu.Lock()
	savedBefore := len(st.tokensSaved)
	st.mu.Unlock()

	// Plain /start (no args): welcome path; the token is NOT consumed (the
	// burn happens only in the /start-with-token flow, never on plain start).
	injectCommand(t, b, ft, testUserID, "/start")
	replies := activationReplies(t, ft)
	wantWelcome := fmt.Sprintf(msgWelcome, ".*", b.Cfg.BatchCap) // name is HTML-escaped at runtime; prefix match below
	if len(replies) == 0 || !strings.Contains(replies[len(replies)-1], strings.Split(wantWelcome, "%")[0]) {
		if len(replies) == 0 || !strings.HasPrefix(replies[len(replies)-1], "<b>⚡ Welcome to ThunderGo,") {
			t.Fatalf("plain start replies = %v, want welcome", replies)
		}
	}
	st.mu.Lock()
	outstanding := st.tokens["TOKEN9999"]
	saved := len(st.tokensSaved)
	st.mu.Unlock()
	if !outstanding {
		t.Fatal("plain /start consumed an activation token")
	}
	if saved != savedBefore {
		t.Fatalf("plain /start minted %d new tokens (audit %d -> %d), want 0", saved-savedBefore, savedBefore, saved)
	}
}
