// Package tgutil provides ThunderGo's helper utilities and — in this file —
// the MTProto transport seam that decouples the rest of the codebase from any
// single Telegram library.
//
// # The one-import rule
//
// github.com/mtgo-labs/mtgo (and its satellite module github.com/mtgo-labs/storage)
// are imported by EXACTLY ONE source file in this repository: this one
// (internal/tgutil/transport.go). The companion file transport_fake.go and the
// gate tests in transport_test.go must stay mtgo-free. Every other package
// (bot, ingest, stream, pool, http, ...) depends only on the Transport and
// BotBackend interfaces plus the plain-Go types defined here. This is the
// cornerstone of the revival plan: swapping or upgrading the MTProto backend
// must never touch more than this file.
//
// Concretely, this file defines:
//
//   - The seam types (FileHandle, IncomingMsg, MediaInfo, CallbackQuery) and
//     the Transport / BotBackend interfaces every backend implements.
//   - A tgutil-native keyboard builder converted to mtgo markup inside this
//     file only.
//   - MessageFilter combinators for the bot layer.
//   - The error taxonomy (sentinels, FloodWait, RPCError, MapFloodWait,
//     Classify) that the stream and http layers branch on.
//   - DeriveFileKey — the stable media identity used for dedup (replaces
//     gogram's PackBotFileID).
//   - MTGOTransport, the production implementation backed by one
//     *telegram.Client from github.com/mtgo-labs/mtgo.
package tgutil

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/mtgo/telegram/params"
	"github.com/mtgo-labs/mtgo/telegram/types"
	"github.com/mtgo-labs/mtgo/tg"
	"github.com/mtgo-labs/mtgo/tgerr"
	"github.com/mtgo-labs/storage"
	mongostorage "github.com/mtgo-labs/storage/mongodb"
	sqlitestorage "github.com/mtgo-labs/storage/sqlite"
)

// ---------------------------------------------------------------------------
// Seam types
// ---------------------------------------------------------------------------

// FileHandle identifies one resolvable media item on the grid. It is produced
// by Transport.ResolveMedia and consumed by Transport.FetchChunk. Location is
// opaque on purpose: consumers never touch it, and only tgutil/transport.go
// may type-assert it into the backend's location type.
type FileHandle struct {
	ChatID   int64
	MsgID    int
	DC       int
	Location any // opaque (mtgo tg.InputFileLocationClass); consumers never touch it
	Size     int64
	Mime     string
	Name     string
}

// IncomingMsg is the backend-agnostic view of an inbound message handed to
// bot-layer handlers. Command detection is performed by the transport (from
// BotCommand entities); mtgo's own Message.Command/Matches fields are dead in
// v0.21.0 and are deliberately not relied upon.
type IncomingMsg struct {
	ChatID       int64
	SenderID     int64
	MsgID        int
	Text         string
	IsPrivate    bool
	IsGroup      bool
	IsChannel    bool
	IsCommand    bool
	CommandName  string // without slash/bot-suffix, e.g. "start"
	Args         string // text after the command word, trimmed
	ReplyToMsgID int
	Media        *MediaInfo // nil if none
	Raw          any        // escape hatch (*types.Message); only tgutil may type-assert
}

// MediaInfo describes the media attached to an IncomingMsg or discovered by
// ResolveMedia. Location is the same opaque handle used by FileHandle.
type MediaInfo struct {
	Kind     string // "photo","video","audio","voice","animation","sticker","document"
	Name     string
	Mime     string
	Size     int64
	DC       int
	MsgID    int
	ChatID   int64
	Location any    // opaque tg.InputFileLocationClass
	FileKey  string // stable dedup identity, see DeriveFileKey
}

// CallbackQuery is the backend-agnostic view of an inline-button press.
type CallbackQuery struct {
	ID        int64
	ChatID    int64
	SenderID  int64
	MessageID int
	Data      string
	Raw       any
}

// BotInfo is the authenticated bot's own identity.
type BotInfo struct {
	ID       int64
	Username string
	DC       int
}

// UserInfo is the backend-agnostic view of a Telegram user.
type UserInfo struct {
	ID        int64
	FirstName string
	LastName  string
	Username  string
	PhotoDC   int // DC hosting the profile photo; 0 when unknown
	IsBot     bool
}

// BotCommand is one entry of the bot's public command menu.
type BotCommand struct {
	Command     string // without leading slash
	Description string
}

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

// Transport is the minimal seam the streaming path needs: lifecycle, inbound
// routing, sending, and chunked media fetch. It is implemented by
// MTGOTransport (production) and FakeTransport (tests).
type Transport interface {
	Start(ctx context.Context) error
	Stop() error
	OnCommand(cmd string, h func(ctx context.Context, m IncomingMsg) error)
	OnCallback(prefix string, h func(ctx context.Context, q CallbackQuery) error)
	SendText(ctx context.Context, chatID int64, text string, markup any) error
	ResolveMedia(ctx context.Context, chatID int64, msgID int) (FileHandle, error)
	FetchChunk(ctx context.Context, fh FileHandle, offset int64, limit int32) ([]byte, error)
	RefreshFileRef(ctx context.Context, fh *FileHandle) error
}

// BotBackend extends Transport with the operations the ThunderGo bot and
// ingester need. Implemented by the mtgo transport and by FakeTransport.
//
// Signature conventions:
//   - markup any accepts nil (no markup field sent) or a value produced by
//     NewKeyboard().Build() / InlineURL / Markup — never a backend type.
//   - Message IDs and chat IDs are plain ints/int64s in Bot API marked form
//     (users positive, basic groups negative, channels/supergroups
//     -100-prefixed), exactly what IncomingMsg.ChatID carries.
type BotBackend interface {
	Transport
	OnAnyMessage(h func(ctx context.Context, m IncomingMsg) error) // catch-all dispatcher hook
	Me(ctx context.Context) (BotInfo, error)                       // id, username, DC
	SetCommands(ctx context.Context, cmds []BotCommand) error
	SendHTML(ctx context.Context, chatID int64, html string, markup any, replyTo int) error
	EditHTML(ctx context.Context, chatID int64, msgID int, html string, markup any) error
	DeleteMessages(ctx context.Context, chatID int64, msgIDs ...int) error
	// ForwardMessages copies (with hidden author when hideAuthor is set) the
	// given messages from fromChatID into destChatID and returns the new
	// message IDs in input order. (The destination parameter is required —
	// a forward without a target chat is meaningless.)
	ForwardMessages(ctx context.Context, destChatID, fromChatID int64, msgIDs []int, hideAuthor bool) ([]int, error)
	GetMessagesBulk(ctx context.Context, chatID int64, minID, maxID, limit int) ([]IncomingMsg, error) // ordered chronological
	GetReplyMessage(ctx context.Context, chatID int64, msgID int) (IncomingMsg, error)
	SendChatAction(ctx context.Context, chatID int64, action string) error
	GetUser(ctx context.Context, userID int64) (UserInfo, error) // id, names, username, photo DC
	ResolveUsername(ctx context.Context, username string) (UserInfo, error)
	// GetChatMemberStatus returns "creator", "administrator", "member",
	// "restricted", "left" or "kicked". Non-members map to "left".
	GetChatMemberStatus(ctx context.Context, chatID, userID int64) (string, error)
	LeaveChat(ctx context.Context, chatID int64) error
	SendFileDocument(ctx context.Context, chatID int64, localPath, caption string, forceDocument bool) error
	AnswerCallback(ctx context.Context, q CallbackQuery, text string, alert bool, url string) error
	CallbackEditHTML(ctx context.Context, q CallbackQuery, html string, markup any) error
	OwnDC(ctx context.Context) int
}

// ---------------------------------------------------------------------------
// Error taxonomy
// ---------------------------------------------------------------------------

// Sentinel errors used by the stream and http layers for response mapping.
// All classification funnels through Classify; errors from the backend are
// always translated into one of these (or an RPCError) by the transport.
var (
	// ErrStaleMedia is permanent: the message was deleted or the peer is
	// invalid → the file record should self-heal to a 404.
	ErrStaleMedia = errors.New("tgutil: media no longer available (deleted or invalid)")
	// ErrMediaMissing is permanent: the record points at a message without
	// downloadable media.
	ErrMediaMissing = errors.New("tgutil: message has no downloadable media")
	// ErrRecordMismatch is permanent: the retrieved media does not match the
	// stored record (identity drift).
	ErrRecordMismatch = errors.New("tgutil: file record does not match retrieved media")
	// ErrTransient covers network/DC issues → http maps to 503.
	ErrTransient = errors.New("tgutil: transient transport failure")
	// ErrFloodWait is the base sentinel for flood-limited calls; the concrete
	// wait is carried by *FloodWait.
	ErrFloodWait = errors.New("tgutil: telegram flood wait")
	// ErrFileRefExpired means the file reference went stale; RefreshFileRef
	// + retry heals it.
	ErrFileRefExpired = errors.New("tgutil: file reference expired")
	// ErrMediaUnsupported is permanent: the media kind/location cannot be
	// served by this transport (or the caller passed invalid chunk args).
	ErrMediaUnsupported = errors.New("tgutil: media unsupported")
)

// FloodWait reports a Telegram FLOOD_WAIT / FLOOD_PREMIUM_WAIT with the
// required wait in seconds. Compare behavior with errors.As, and consult
// MapFloodWait for the numeric value.
type FloodWait struct {
	Seconds int
}

// Error implements the error interface.
func (e *FloodWait) Error() string {
	return fmt.Sprintf("tgutil: flood wait: retry after %ds", e.Seconds)
}

// RPCError is an unclassified Telegram RPC failure. Type is the symbolic
// Telegram error (e.g. "CHAT_WRITE_FORBIDDEN"), Message the raw server string
// and Argument any numeric suffix (e.g. the X in FLOOD_WAIT_X).
type RPCError struct {
	Code     int
	Type     string
	Message  string
	Argument int
}

// Error implements the error interface.
func (e *RPCError) Error() string {
	if e.Type != "" && e.Type != e.Message {
		return fmt.Sprintf("tgutil: rpc error code %d: %s (%d)", e.Code, e.Type, e.Argument)
	}
	return fmt.Sprintf("tgutil: rpc error code %d: %s", e.Code, e.Message)
}

// NewFloodWaitTestError builds an error shaped exactly like the ones mtgo's
// invoker surfaces for FLOOD_WAIT_X (tgerr.New(420, "FLOOD_WAIT_X")). It lives
// in this mtgo-importing file so that tests elsewhere can exercise MapFloodWait
// without importing mtgo themselves.
//
//nolint:revive // exported test seam; see doc comment
func NewFloodWaitTestError(seconds int) error {
	return tgerr.New(420, fmt.Sprintf("FLOOD_WAIT_%d", seconds))
}

// NewFloodPremiumWaitTestError is the FLOOD_PREMIUM_WAIT twin of
// NewFloodWaitTestError.
//
//nolint:revive // exported test seam; see doc comment
func NewFloodPremiumWaitTestError(seconds int) error {
	return tgerr.New(420, fmt.Sprintf("FLOOD_PREMIUM_WAIT_%d", seconds))
}

// NewTestDocumentLocation builds an opaque document file location for
// mtgo-free tests (FakeTransport consumers, characterization tests) so they can
// exercise DeriveFileKey and FileHandle plumbing without importing mtgo. Same
// test-seam pattern as NewFloodWaitTestError above.
//
//nolint:revive // exported test seam; see doc comment
func NewTestDocumentLocation(id, accessHash int64) any {
	return &tg.InputDocumentFileLocation{ID: id, AccessHash: accessHash}
}

// NewTestPhotoLocation is the photo twin of NewTestDocumentLocation.
//
//nolint:revive // exported test seam; see doc comment
func NewTestPhotoLocation(id, accessHash int64) any {
	return &tg.InputPhotoFileLocation{ID: id, AccessHash: accessHash}
}

// floodTypeNames lists every Telegram error type treated as a flood wait
// (mirrors tgerr.FloodWaitErrors).
var floodTypeNames = []string{"FLOOD_WAIT", "FLOOD_PREMIUM_WAIT"}

// MapFloodWait maps an RPC error to (waitSeconds, true) when it is a
// FLOOD_WAIT / FLOOD_PREMIUM_WAIT. It understands mtgo's *tgerr.Error, this
// package's *FloodWait, and *RPCError.
func MapFloodWait(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	var fw *FloodWait
	if errors.As(err, &fw) {
		return fw.Seconds, true
	}
	if d, ok := tgerr.AsFloodWait(err); ok {
		return int(d.Seconds()), true
	}
	var rpc *RPCError
	if errors.As(err, &rpc) && slices.Contains(floodTypeNames, rpc.Type) {
		return rpc.Argument, true
	}
	return 0, false
}

// ErrClass is the coarse classification used by the stream and http layers.
type ErrClass int

const (
	// ErrClassOther is unknown or nil — http maps it to 500.
	ErrClassOther ErrClass = iota
	// ErrClassPermanent — http maps it to 404/410-class self-heal.
	ErrClassPermanent
	// ErrClassTransient — http maps it to 503 with Retry-After.
	ErrClassTransient
	// ErrClassFlood — http maps it to 429 with Retry-After.
	ErrClassFlood
)

// String returns a human-readable class name.
func (c ErrClass) String() string {
	switch c {
	case ErrClassPermanent:
		return "permanent"
	case ErrClassTransient:
		return "transient"
	case ErrClassFlood:
		return "flood"
	default:
		return "other"
	}
}

// permanentRPCTypes lists Telegram error types that indicate the media record
// can never be served again (deleted message, dead peer, corrupt file entry).
var permanentRPCTypes = []string{
	"MESSAGE_ID_INVALID",
	"MESSAGE_DELETED",
	"MESSAGE_EMPTY",
	"CHANNEL_INVALID",
	"CHANNEL_PRIVATE",
	"CHANNEL_DELETED",
	"CHAT_INVALID",
	"CHAT_ID_INVALID",
	"PEER_ID_INVALID",
	"FILE_PARTS_INVALID",
	"FILE_PART_INVALID",
	"FILE_INVALID",
	"FILE_ID_INVALID",
	"DOCUMENT_INVALID",
	"PHOTO_INVALID",
	"MEDIA_EMPTY",
	"PHOTO_INVALID_DIMENSIONS",
}

// transientRPCTypes lists Telegram error types worth retrying after a delay.
var transientRPCTypes = []string{
	"TIMEOUT",
	"CLIENT_DISCONNECT",
	"CONNECTION_RESET",
	"SESSION_PASSWORD_NEEDED", // recovered by re-auth, not by retry — but never permanent-stale
}

// Classify maps err onto the sentinel taxonomy: flood → ErrClassFlood;
// permanent sentinels / permanent RPC types → ErrClassPermanent; transient
// sentinels, network timeouts, 5xx codes and DC migration failures →
// ErrClassTransient; everything else → ErrClassOther. Classify(nil) returns
// ErrClassOther (there is nothing to classify — callers should check err
// first).
func Classify(err error) ErrClass {
	if err == nil {
		return ErrClassOther
	}
	if _, ok := MapFloodWait(err); ok {
		return ErrClassFlood
	}
	switch {
	case errors.Is(err, ErrStaleMedia),
		errors.Is(err, ErrMediaMissing),
		errors.Is(err, ErrRecordMismatch),
		errors.Is(err, ErrMediaUnsupported):
		return ErrClassPermanent
	case errors.Is(err, ErrFileRefExpired):
		// Healable (RefreshFileRef + retry), so retry-class rather than
		// permanent. The stream layer refreshes and retries before giving up.
		return ErrClassTransient
	case errors.Is(err, ErrTransient):
		return ErrClassTransient
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrClassTransient
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrClassTransient
	}
	var mig *telegram.MigrationError
	if errors.As(err, &mig) {
		return ErrClassTransient
	}
	var rpc *RPCError
	if errors.As(err, &rpc) {
		if rpc.Code >= 500 {
			return ErrClassTransient
		}
		if slices.Contains(transientRPCTypes, rpc.Type) {
			return ErrClassTransient
		}
		if slices.Contains(permanentRPCTypes, rpc.Type) {
			return ErrClassPermanent
		}
		return ErrClassOther
	}
	if tgErr, ok := tgerr.As(err); ok {
		if tgErr.Code >= 500 {
			return ErrClassTransient
		}
		if tgErr.IsOneOf(transientRPCTypes...) {
			return ErrClassTransient
		}
		if tgErr.IsOneOf(permanentRPCTypes...) {
			return ErrClassPermanent
		}
	}
	return ErrClassOther
}

// ---------------------------------------------------------------------------
// Keyboard builder (tgutil-native; converted to mtgo markup below)
// ---------------------------------------------------------------------------

// MarkupButton is one inline button. Exactly one of URL / CallbackData is
// honored (URL wins if both are set).
type MarkupButton struct {
	Text         string
	URL          string
	CallbackData string
}

// Markup is a tgutil-native inline keyboard. An empty Markup (no rows) sent
// through EditHTML/CallbackEditHTML clears the existing keyboard; a nil
// markup argument leaves the keyboard untouched.
type Markup struct {
	Rows [][]MarkupButton
}

// InlineURL returns a URL button for use with KeyboardBuilder.AddRow.
func InlineURL(text, url string) MarkupButton {
	return MarkupButton{Text: text, URL: url}
}

// InlineCallback returns a callback button for use with KeyboardBuilder.AddRow.
func InlineCallback(text, data string) MarkupButton {
	return MarkupButton{Text: text, CallbackData: data}
}

// KeyboardBuilder fluently assembles a Markup. Callback/URL append to the
// current row; Next() closes the row; AddRow appends a complete row; Build
// closes any open row and returns the Markup.
type KeyboardBuilder struct {
	rows [][]MarkupButton
	row  []MarkupButton
}

// NewKeyboard starts a new keyboard builder.
func NewKeyboard() *KeyboardBuilder {
	return &KeyboardBuilder{}
}

// AddRow appends a complete row of buttons.
func (b *KeyboardBuilder) AddRow(buttons ...MarkupButton) *KeyboardBuilder {
	if len(buttons) > 0 {
		b.rows = append(b.rows, buttons)
	}
	return b
}

// Callback adds a callback-data button to the current row. Data is truncated
// to Telegram's 64-byte limit.
func (b *KeyboardBuilder) Callback(text, data string) *KeyboardBuilder {
	if len(data) > 64 {
		data = data[:64]
	}
	b.row = append(b.row, MarkupButton{Text: text, CallbackData: data})
	return b
}

// URL adds an open-URL button to the current row.
func (b *KeyboardBuilder) URL(text, url string) *KeyboardBuilder {
	b.row = append(b.row, MarkupButton{Text: text, URL: url})
	return b
}

// Next closes the current row and starts a new one (no-op when empty).
func (b *KeyboardBuilder) Next() *KeyboardBuilder {
	if len(b.row) > 0 {
		b.rows = append(b.rows, b.row)
		b.row = nil
	}
	return b
}

// Build finalizes the keyboard (closing any open row) and returns it.
func (b *KeyboardBuilder) Build() Markup {
	b.Next()
	return Markup{Rows: b.rows}
}

// buildMarkup converts a tgutil markup value into the mtgo reply-markup type.
// It is the ONLY place mtgo markup types are constructed.
//
//	nil            → nil, nil            (no markup field sent)
//	nil *Markup    → nil, nil
//	Markup{}       → empty inline markup (clears buttons on edit)
//	*Markup / Markup → built inline markup
func buildMarkup(v any) (tg.ReplyMarkupClass, error) {
	switch m := v.(type) {
	case nil:
		return nil, nil
	case Markup:
		return markupToTL(&m), nil
	case *Markup:
		if m == nil {
			return nil, nil
		}
		return markupToTL(m), nil
	default:
		return nil, fmt.Errorf("tgutil: unsupported markup type %T (use tgutil.Markup or nil)", v)
	}
}

func markupToTL(m *Markup) tg.ReplyMarkupClass {
	out := &tg.ReplyInlineMarkup{Rows: make([]*tg.KeyboardInlineButtonRow, 0, len(m.Rows))}
	for _, row := range m.Rows {
		if len(row) == 0 {
			continue
		}
		buttons := make([]*tg.KeyboardInlineButton, 0, len(row))
		for _, btn := range row {
			switch {
			case btn.URL != "":
				buttons = append(buttons, &tg.KeyboardInlineButton{
					Text: btn.Text,
					Type: &tg.InlineButtonTypeURL{URL: btn.URL},
				})
			default:
				data := []byte(btn.CallbackData)
				if len(data) > 64 {
					data = data[:64]
				}
				buttons = append(buttons, &tg.KeyboardInlineButton{
					Text: btn.Text,
					Type: &tg.InlineButtonTypeCallback{Data: data},
				})
			}
		}
		out.Rows = append(out.Rows, &tg.KeyboardInlineButtonRow{Buttons: buttons})
	}
	return out
}

// ---------------------------------------------------------------------------
// Message filters (bot-layer combinators over IncomingMsg)
// ---------------------------------------------------------------------------

// MessageFilter predicates over the backend-agnostic IncomingMsg.
type MessageFilter func(IncomingMsg) bool

// Command matches IsCommand with the exact (case-insensitive) command name.
func Command(name string) MessageFilter {
	name = strings.ToLower(strings.TrimPrefix(name, "/"))
	return func(m IncomingMsg) bool {
		return m.IsCommand && strings.EqualFold(m.CommandName, name)
	}
}

// Regex matches the message Text against re.
func Regex(re *regexp.Regexp) MessageFilter {
	return func(m IncomingMsg) bool {
		return re != nil && re.MatchString(m.Text)
	}
}

// MediaOnly matches messages carrying any media.
func MediaOnly() MessageFilter {
	return func(m IncomingMsg) bool { return m.Media != nil }
}

// Photo matches photo media.
func Photo() MessageFilter { return kindIs("photo") }

// Document matches document media.
func Document() MessageFilter { return kindIs("document") }

// Video matches video media (including video notes).
func Video() MessageFilter { return kindIs("video") }

// Audio matches audio media.
func Audio() MessageFilter { return kindIs("audio") }

// Voice matches voice-note media.
func Voice() MessageFilter { return kindIs("voice") }

// Animation matches GIF-style animation media.
func Animation() MessageFilter { return kindIs("animation") }

// Sticker matches sticker media.
func Sticker() MessageFilter { return kindIs("sticker") }

func kindIs(kind string) MessageFilter {
	return func(m IncomingMsg) bool {
		return m.Media != nil && m.Media.Kind == kind
	}
}

// And composes two filters conjunctively.
func (f MessageFilter) And(g MessageFilter) MessageFilter {
	return func(m IncomingMsg) bool { return f(m) && g(m) }
}

// Or composes two filters disjunctively.
func (f MessageFilter) Or(g MessageFilter) MessageFilter {
	return func(m IncomingMsg) bool { return f(m) || g(m) }
}

// Not negates a filter.
func (f MessageFilter) Not() MessageFilter {
	return func(m IncomingMsg) bool { return !f(m) }
}

// ---------------------------------------------------------------------------
// FileKey
// ---------------------------------------------------------------------------

// DeriveFileKey returns the stable dedup identity for a media item, replacing
// gogram's PackBotFileID. Documents key on (document id, access hash); photos
// on (photo id, access hash). Old FileRecords keep their legacy keys in Mongo;
// only new ingests produce keys in this format (no schema change).
func DeriveFileKey(m MediaInfo) string {
	if m.Location == nil {
		return ""
	}
	switch loc := m.Location.(type) {
	case *tg.InputDocumentFileLocation:
		return fmt.Sprintf("doc:%d:%d", loc.ID, loc.AccessHash)
	case *tg.InputPhotoFileLocation:
		return fmt.Sprintf("photo:%d:%d", loc.ID, loc.AccessHash)
	default:
		return ""
	}
}

// ---------------------------------------------------------------------------
// MTGO transport
// ---------------------------------------------------------------------------

// MTGOConfig configures the production mtgo-backed transport.
type MTGOConfig struct {
	APIID       int32
	APIHash     string
	BotToken    string
	SessionName string // storage key; scopes the session inside a shared backend

	// Storage is either nil (mtgo in-memory session storage), a value built by
	// MongoStorage / SQLiteStorage (i.e. a storage.Storage from
	// github.com/mtgo-labs/storage wrapped for mtgo), or any other
	// storage.Storage implementation.
	Storage any

	// Log receives transport lifecycle and handler diagnostics. nil defaults
	// to slog.Default().
	Log *slog.Logger
}

// MongoStorage opens the mtgo-labs/storage MongoDB adapter and wraps it for
// use as MTGOConfig.Storage. The connection is pinged eagerly (15s budget) so
// a bad DSN fails at boot, not on first RPC.
func MongoStorage(uri, database string) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ext, err := mongostorage.Open(ctx, mongostorage.Config{URI: uri, Database: database})
	if err != nil {
		return nil, fmt.Errorf("tgutil: mongodb session storage: %w", err)
	}
	return storage.NewAdapter(ext), nil
}

// SQLiteStorage opens the mtgo-labs/storage SQLite adapter at path and wraps
// it for use as MTGOConfig.Storage. The database file is created (if needed)
// and explicitly chmod'ed 0600 — the adapter itself never tightens perms.
func SQLiteStorage(path string) (any, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("tgutil: sqlite session storage: %w", err)
	}
	_ = f.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("tgutil: sqlite session storage chmod: %w", err)
	}
	ext, err := sqlitestorage.Open(path)
	if err != nil {
		return nil, fmt.Errorf("tgutil: sqlite session storage: %w", err)
	}
	return storage.NewAdapter(ext), nil
}

// Chunk geometry enforced by FetchChunk (Telegram server constraints).
const (
	chunkAlign  = 4096        // offset and limit must be multiples of 4 KiB
	maxChunkLen = 1 << 20     // 1 MiB maximum per upload.getFile call
	dialTimeout = time.Minute // matches mtgo's DefaultConfig.Timeout
)

type msgHandler = func(context.Context, IncomingMsg) error
type cbHandler = func(context.Context, CallbackQuery) error

type callbackRoute struct {
	prefix string
	h      cbHandler
}

// MTGOTransport implements Transport + BotBackend on top of one
// *telegram.Client. It is the only place in ThunderGo that touches mtgo.
type MTGOTransport struct {
	log    *slog.Logger
	client *telegram.Client
	cfg    MTGOConfig

	mu        sync.Mutex
	started   bool
	stopped   bool
	anyMsg    []msgHandler
	commands  map[string]msgHandler // exact, lowercase command names
	callbacks []callbackRoute       // matched longest-prefix first

	cdnMu sync.Mutex
	cdn   map[string]*cdnState // per-file CDN redirect session state
}

// Compile-time gates: the production transport must satisfy the full seam.
var (
	_ Transport  = (*MTGOTransport)(nil)
	_ BotBackend = (*MTGOTransport)(nil)
)

// NewMTGOTransport builds the transport and registers the single pair of mtgo
// dispatcher hooks (messages + callback queries). No network I/O happens
// until Start.
func NewMTGOTransport(cfg MTGOConfig) (*MTGOTransport, error) {
	if cfg.APIID == 0 {
		return nil, errors.New("tgutil: APIID is required")
	}
	if cfg.APIHash == "" {
		return nil, errors.New("tgutil: APIHash is required")
	}
	if cfg.BotToken == "" {
		return nil, errors.New("tgutil: BotToken is required")
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	mc := &telegram.Config{
		BotToken:    cfg.BotToken,
		SessionName: cfg.SessionName,
		// CRITICAL: surface FLOOD_WAIT(_X) as a value instead of sleeping and
		// retrying inside the library. ThunderGo's rate limiters and the
		// ingest retry policy own that decision.
		SleepThreshold: -1,
		// Bound handler runtime: handlers perform RPCs (ingest retries with
		// backoff can span ~10s), but must never wedge a dispatch worker.
		HandlerTimeout: 120 * time.Second,
	}
	if cfg.Storage != nil {
		st, ok := cfg.Storage.(storage.Storage)
		if !ok {
			return nil, fmt.Errorf("tgutil: Storage must be nil or a storage.Storage (use MongoStorage/SQLiteStorage), got %T", cfg.Storage)
		}
		mc.Storage = st
	}

	client, err := telegram.NewClient(cfg.APIID, cfg.APIHash, mc)
	if err != nil {
		return nil, fmt.Errorf("tgutil: new mtgo client: %w", err)
	}

	t := &MTGOTransport{
		log:      log,
		client:   client,
		cfg:      cfg,
		commands: make(map[string]msgHandler),
		cdn:      make(map[string]*cdnState),
	}

	// ONE catch-all message hook and ONE callback hook: all tgutil-level
	// routing (OnAnyMessage / OnCommand / OnCallback) fans out from here.
	// Handlers are registered pre-Connect — the dispatcher starts delivering
	// as soon as the session inside Connect comes up.
	client.OnMessage(t.onMtgoMessage)
	client.OnCallbackQuery(t.onMtgoCallback)
	return t, nil
}

// Start connects the client and imports bot authorization. It returns as soon
// as the client is ready; inbound handlers are live from that moment.
//
// Verified mtgo behavior (v0.21.0): updates are dispatched from the session's
// reader goroutine — startSession() installs the update handler
// (client.go:2196) before sess.Connect() brings the encrypted session up, and
// completeConnect→postConnect fetches the update state. Client.Start() and
// Client.Idle() are pure blocking conveniences around a stop channel; they
// are NOT required for updates to flow. Therefore Start(ctx) simply calls
// Connect(timeout) and returns.
//
// Connect has no context parameter, so the timeout is derived from ctx: a
// deadline on ctx (if any) becomes the connect budget, otherwise 60s. Cancellation
// of ctx after Connect returns is ignored — use Stop for lifecycle control.
func (t *MTGOTransport) Start(ctx context.Context) error {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return errors.New("tgutil: transport already stopped")
	}
	if t.started {
		t.mu.Unlock()
		return errors.New("tgutil: transport already started")
	}
	t.started = true
	t.mu.Unlock()

	timeout := dialTimeout
	if d, ok := ctx.Deadline(); ok {
		if until := time.Until(d); until > 0 {
			timeout = until
		}
	}
	if err := t.client.Connect(timeout); err != nil {
		return fmt.Errorf("tgutil: mtgo connect: %w", err)
	}
	u := t.client.Me()
	t.log.Info("mtgo transport started",
		"bot_id", userIDOf(u),
		"dc", t.OwnDC(ctx),
	)
	return nil
}

// Stop disconnects and tears down the client. Idempotent.
func (t *MTGOTransport) Stop() error {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return nil
	}
	t.stopped = true
	t.mu.Unlock()

	// Disconnect closes sessions + flushes storage; Close additionally kills
	// the reconnect/health goroutines (terminal — the client is unusable
	// after). Double-Stop is safe via the stopped flag.
	if err := t.client.Disconnect(); err != nil {
		t.log.Debug("mtgo disconnect", "err", err)
	}
	t.client.Close()
	return nil
}

// OnCommand registers a handler for an exact command name (case-insensitive,
// without slash). Unknown commands are left to OnAnyMessage handlers.
func (t *MTGOTransport) OnCommand(cmd string, h func(ctx context.Context, m IncomingMsg) error) {
	cmd = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(cmd), "/"))
	t.mu.Lock()
	defer t.mu.Unlock()
	t.commands[cmd] = h
}

// OnCallback registers a handler for callback data with the given prefix.
// Longest prefix wins; at most one handler fires per query.
func (t *MTGOTransport) OnCallback(prefix string, h func(ctx context.Context, q CallbackQuery) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.callbacks = append(t.callbacks, callbackRoute{prefix: prefix, h: h})
}

// OnAnyMessage registers a catch-all message handler. Handlers run in
// registration order for every message (commands included) before command
// routing.
func (t *MTGOTransport) OnAnyMessage(h func(ctx context.Context, m IncomingMsg) error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.anyMsg = append(t.anyMsg, h)
}

// onMtgoMessage is the single mtgo message hook.
func (t *MTGOTransport) onMtgoMessage(mtgoCtx *telegram.Context, msg *types.Message) {
	if msg == nil {
		return
	}
	in := convertMessage(msg)
	ctx := mtgoCtx.Ctx

	t.mu.Lock()
	anyMsg := slices.Clone(t.anyMsg)
	var cmdH msgHandler
	if in.IsCommand {
		cmdH = t.commands[in.CommandName]
	}
	t.mu.Unlock()

	for _, h := range anyMsg {
		if err := h(ctx, in); err != nil {
			t.log.Warn("tgutil: OnAnyMessage handler failed", "chat_id", in.ChatID, "msg_id", in.MsgID, "err", err)
		}
	}
	if cmdH != nil {
		if err := cmdH(ctx, in); err != nil {
			t.log.Warn("tgutil: OnCommand handler failed", "command", in.CommandName, "chat_id", in.ChatID, "err", err)
		}
	}
}

// onMtgoCallback is the single mtgo callback hook. Longest-prefix match wins.
func (t *MTGOTransport) onMtgoCallback(mtgoCtx *telegram.Context, cb *types.CallbackQuery) {
	if cb == nil {
		return
	}
	q := convertCallback(cb)
	ctx := mtgoCtx.Ctx

	t.mu.Lock()
	best := -1
	bestLen := -1
	for i, r := range t.callbacks {
		if len(r.prefix) > bestLen && strings.HasPrefix(q.Data, r.prefix) {
			best = i
			bestLen = len(r.prefix)
		}
	}
	var h cbHandler
	if best >= 0 {
		h = t.callbacks[best].h
	}
	t.mu.Unlock()

	if h == nil {
		t.log.Debug("tgutil: no handler for callback", "data", q.Data)
		return
	}
	if err := h(ctx, q); err != nil {
		t.log.Warn("tgutil: OnCallback handler failed", "data", q.Data, "chat_id", q.ChatID, "err", err)
	}
}

// ---------------------------------------------------------------------------
// Sending / editing / message ops
// ---------------------------------------------------------------------------

// SendText sends a plain-text message. markup may be nil or a tgutil Markup.
func (t *MTGOTransport) SendText(ctx context.Context, chatID int64, text string, markup any) error {
	mk, err := buildMarkup(markup)
	if err != nil {
		return err
	}
	_, err = t.client.SendMessage(ctx, chatID, text, &params.SendMessage{ReplyMarkup: mk})
	if err != nil {
		return t.mapErr(err, "send text")
	}
	return nil
}

// SendHTML sends an HTML-formatted message, optionally as a reply.
func (t *MTGOTransport) SendHTML(ctx context.Context, chatID int64, html string, markup any, replyTo int) error {
	mk, err := buildMarkup(markup)
	if err != nil {
		return err
	}
	opt := &params.SendMessage{ParseMode: params.HTML, ReplyMarkup: mk}
	if replyTo > 0 {
		opt.ReplyToMessageID = int32(replyTo) //nosec G115 // message IDs fit int32
	}
	if _, err := t.client.SendMessage(ctx, chatID, html, opt); err != nil {
		return t.mapErr(err, "send html")
	}
	return nil
}

// EditHTML edits an existing message's text with HTML formatting. An empty
// Markup clears the keyboard; nil leaves it untouched.
func (t *MTGOTransport) EditHTML(ctx context.Context, chatID int64, msgID int, html string, markup any) error {
	mk, err := buildMarkup(markup)
	if err != nil {
		return err
	}
	_, err = t.client.EditMessageText(ctx, chatID, int32(msgID), html, &params.EditMessage{ParseMode: params.HTML, ReplyMarkup: mk}) //nosec G115 // message IDs fit int32
	if err != nil {
		return t.mapErr(err, "edit html")
	}
	return nil
}

// DeleteMessages deletes messages for everyone (Revoke).
func (t *MTGOTransport) DeleteMessages(ctx context.Context, chatID int64, msgIDs ...int) error {
	if len(msgIDs) == 0 {
		return nil
	}
	ids := make([]int32, len(msgIDs))
	for i, id := range msgIDs {
		ids[i] = int32(id) //nosec G115 // message IDs fit int32
	}
	if _, err := t.client.DeleteMessages(ctx, chatID, ids, &params.DeleteMessages{Revoke: true}); err != nil {
		return t.mapErr(err, "delete messages")
	}
	return nil
}

// ForwardMessages forwards messages from one chat into another and returns
// the new message IDs in input order. hideAuthor drops the forward header
// (mtgo DropAuthor — the mtgo equivalent of gogram's HideAuthor).
func (t *MTGOTransport) ForwardMessages(ctx context.Context, destChatID, fromChatID int64, msgIDs []int, hideAuthor bool) ([]int, error) {
	if len(msgIDs) == 0 {
		return nil, nil
	}
	ids := make([]int32, len(msgIDs))
	for i, id := range msgIDs {
		ids[i] = int32(id) //nosec G115 // message IDs fit int32
	}
	msgs, err := t.client.ForwardMessages(ctx, destChatID, fromChatID, ids, &params.ForwardMessages{DropAuthor: hideAuthor})
	if err != nil {
		return nil, t.mapErr(err, "forward messages")
	}
	out := make([]int, 0, len(msgs))
	for _, m := range msgs {
		if m != nil {
			out = append(out, int(m.ID))
		}
	}
	return out, nil
}

// GetMessagesBulk returns up to limit messages of a chat with
// minID < id <= maxID in chronological order. maxID == 0 means "from the
// newest". (mtgo GetChatHistory returns newest-first and treats offsetID as
// exclusive, so we query from maxID+1 and reverse.)
func (t *MTGOTransport) GetMessagesBulk(ctx context.Context, chatID int64, minID, maxID, limit int) ([]IncomingMsg, error) {
	if limit <= 0 {
		return nil, nil
	}
	offsetID := 0
	if maxID > 0 {
		offsetID = maxID + 1
	}
	msgs, err := t.client.GetChatHistory(ctx, chatID, limit, int32(offsetID)) //nosec G115 // message IDs fit int32
	if err != nil {
		return nil, t.mapErr(err, "get history")
	}
	out := make([]IncomingMsg, 0, len(msgs))
	for _, m := range msgs {
		if m == nil || int(m.ID) <= minID {
			continue
		}
		out = append(out, convertMessage(m))
	}
	slices.Reverse(out) // newest-first → chronological
	return out, nil
}

// GetReplyMessage resolves the message that msgID replies to.
func (t *MTGOTransport) GetReplyMessage(ctx context.Context, chatID int64, msgID int) (IncomingMsg, error) {
	base, err := t.fetchOneMessage(ctx, chatID, msgID)
	if err != nil {
		return IncomingMsg{}, err
	}
	if base.ReplyToID == 0 {
		return IncomingMsg{}, fmt.Errorf("tgutil: message %d in chat %d has no reply", msgID, chatID)
	}
	reply, err := t.fetchOneMessage(ctx, chatID, int(base.ReplyToID))
	if err != nil {
		return IncomingMsg{}, err
	}
	return convertMessage(reply), nil
}

func (t *MTGOTransport) fetchOneMessage(ctx context.Context, chatID int64, msgID int) (*types.Message, error) {
	msgs, err := t.client.GetMessages(ctx, chatID, []int32{int32(msgID)}) //nosec G115 // message IDs fit int32
	if err != nil {
		return nil, t.mapErr(err, "get message")
	}
	if len(msgs) == 0 || msgs[0] == nil || msgs[0].Empty {
		return nil, fmt.Errorf("%w: message %d in chat %d", ErrStaleMedia, msgID, chatID)
	}
	return msgs[0], nil
}

// chatActionMap maps Bot-API-style action names to mtgo action constructors.
var chatActionMap = map[string]func() tg.SendMessageActionClass{
	"typing":          func() tg.SendMessageActionClass { return &tg.SendMessageTypingAction{} },
	"upload_document": func() tg.SendMessageActionClass { return &tg.SendMessageUploadDocumentAction{Progress: 0} },
	"upload_photo":    func() tg.SendMessageActionClass { return &tg.SendMessageUploadPhotoAction{} },
	"upload_video":    func() tg.SendMessageActionClass { return &tg.SendMessageUploadVideoAction{} },
	"record_video":    func() tg.SendMessageActionClass { return &tg.SendMessageRecordVideoAction{} },
	"record_voice":    func() tg.SendMessageActionClass { return &tg.SendMessageRecordAudioAction{} },
}

// SendChatAction raises a chat action ("typing", "upload_document", ...).
func (t *MTGOTransport) SendChatAction(ctx context.Context, chatID int64, action string) error {
	mk, ok := chatActionMap[action]
	if !ok {
		return fmt.Errorf("tgutil: unknown chat action %q", action)
	}
	if err := t.client.SendChatAction(ctx, chatID, mk()); err != nil {
		return t.mapErr(err, "chat action")
	}
	return nil
}

// Me returns the authenticated bot's identity (cached by mtgo after Connect).
func (t *MTGOTransport) Me(ctx context.Context) (BotInfo, error) {
	u := t.client.Me()
	if u == nil {
		var err error
		u, err = t.client.GetMe(ctx)
		if err != nil {
			return BotInfo{}, t.mapErr(err, "get me")
		}
	}
	return BotInfo{
		ID:       u.ID,
		Username: u.Username,
		DC:       t.OwnDC(ctx),
	}, nil
}

// SetCommands publishes the bot's global command menu.
func (t *MTGOTransport) SetCommands(ctx context.Context, cmds []BotCommand) error {
	if len(cmds) == 0 {
		return nil
	}
	list := make([]*tg.BotCommand, len(cmds))
	for i, c := range cmds {
		list[i] = &tg.BotCommand{Command: c.Command, Description: c.Description}
	}
	if err := t.client.SetBotCommands(ctx, &tg.BotCommandScopeDefault{}, "", list); err != nil {
		return t.mapErr(err, "set commands")
	}
	return nil
}

// GetUser resolves a user by ID (access-hash repair for bots is handled
// inside mtgo).
func (t *MTGOTransport) GetUser(ctx context.Context, userID int64) (UserInfo, error) {
	u, err := t.client.GetUser(ctx, userID)
	if err != nil {
		return UserInfo{}, t.mapErr(err, "get user")
	}
	return userInfoFrom(u), nil
}

// ResolveUsername resolves a @username. Usernames backed by a user return the
// full profile; channel/supergroup usernames return a stub UserInfo with the
// marked chat ID (the seam's UserInfo is user-shaped).
func (t *MTGOTransport) ResolveUsername(ctx context.Context, username string) (UserInfo, error) {
	username = strings.TrimPrefix(strings.TrimSpace(username), "@")
	peer, err := t.client.ResolveUsername(ctx, username)
	if err != nil {
		return UserInfo{}, t.mapErr(err, "resolve username")
	}
	switch p := peer.(type) {
	case *tg.InputPeerUser:
		return t.GetUser(ctx, p.UserID)
	case *tg.InputPeerChannel:
		return UserInfo{
			ID:       markedChannelID(p.ChannelID),
			Username: username,
		}, nil
	default:
		return UserInfo{}, fmt.Errorf("tgutil: username %q resolved to unsupported peer %T", username, peer)
	}
}

// GetChatMemberStatus returns the Bot-API-style membership status. Non-member
// errors (USER_NOT_PARTICIPANT) map to "left"; mtgo's "owner" maps to
// "creator" and "banned" to "kicked".
func (t *MTGOTransport) GetChatMemberStatus(ctx context.Context, chatID, userID int64) (string, error) {
	member, err := t.client.GetChatMember(ctx, chatID, userID)
	if err != nil {
		if tgerr.Is(err, "USER_NOT_PARTICIPANT", "USER_NOT_MUTUAL_CONTACT") {
			return "left", nil
		}
		return "", t.mapErr(err, "get chat member")
	}
	if member == nil {
		return "left", nil
	}
	switch member.Status {
	case types.ChatMemberStatusOwner:
		return "creator", nil
	case types.ChatMemberStatusAdministrator:
		return "administrator", nil
	case types.ChatMemberStatusMember:
		return "member", nil
	case types.ChatMemberStatusRestricted:
		return "restricted", nil
	case types.ChatMemberStatusBanned:
		return "kicked", nil
	default:
		return "left", nil
	}
}

// LeaveChat leaves a group or channel.
func (t *MTGOTransport) LeaveChat(ctx context.Context, chatID int64) error {
	if err := t.client.LeaveChat(ctx, chatID); err != nil {
		return t.mapErr(err, "leave chat")
	}
	return nil
}

// SendFileDocument uploads a local file and sends it as a document.
func (t *MTGOTransport) SendFileDocument(ctx context.Context, chatID int64, localPath, caption string, forceDocument bool) error {
	name := filepath.Base(localPath)
	file := types.Path(localPath)
	if _, err := t.client.SendDocument(ctx, chatID, file, caption, &params.SendDocument{
		FileName:      name,
		ForceDocument: forceDocument,
		ParseMode:     params.HTML,
	}); err != nil {
		return t.mapErr(err, "send document")
	}
	return nil
}

// AnswerCallback acknowledges a callback query (toast, alert, or URL open).
func (t *MTGOTransport) AnswerCallback(ctx context.Context, q CallbackQuery, text string, alert bool, url string) error {
	if err := t.client.AnswerCallbackQuery(ctx, q.ID, text, alert, url, 0); err != nil {
		return t.mapErr(err, "answer callback")
	}
	return nil
}

// CallbackEditHTML edits the message a callback query originated from.
func (t *MTGOTransport) CallbackEditHTML(ctx context.Context, q CallbackQuery, html string, markup any) error {
	return t.EditHTML(ctx, q.ChatID, q.MessageID, html, markup)
}

// OwnDC reports the DC the client's primary session is attached to (0 when
// not connected and no DC was pinned in config).
func (t *MTGOTransport) OwnDC(_ context.Context) int {
	if sess := t.client.Session(); sess != nil {
		if dc := sess.DC(); dc.ID > 0 {
			return dc.ID
		}
	}
	return t.client.DC()
}

// ---------------------------------------------------------------------------
// Media resolution
// ---------------------------------------------------------------------------

// ResolveMedia fetches one message and extracts a resolvable FileHandle.
//
// FILE_MIGRATE note: GetMessages travels through mtgo's client invoker, which
// transparently follows FILE_MIGRATE_X (code 303, export-import re-invoke on
// the target DC — client.go handleMigrationError → migrateExportImport), so a
// message that moved DCs resolves successfully here and the returned
// location/file reference are valid on the right DC. A migration failure
// surfaces as *telegram.MigrationError and is classified transient.
func (t *MTGOTransport) ResolveMedia(ctx context.Context, chatID int64, msgID int) (FileHandle, error) {
	msg, err := t.fetchOneMessage(ctx, chatID, msgID)
	if err != nil {
		return FileHandle{}, err
	}
	mi := extractMediaInfo(msg, chatID)
	if mi == nil {
		return FileHandle{}, fmt.Errorf("%w: message %d in chat %d", ErrMediaMissing, msgID, chatID)
	}
	return FileHandle{
		ChatID:   chatID,
		MsgID:    msgID,
		DC:       mi.DC,
		Location: mi.Location,
		Size:     mi.Size,
		Mime:     mi.Mime,
		Name:     mi.Name,
	}, nil
}

// RefreshFileRef re-resolves the backing message and swaps the (stale) file
// reference inside fh, updating Size/DC if they changed.
func (t *MTGOTransport) RefreshFileRef(ctx context.Context, fh *FileHandle) error {
	if fh == nil {
		return errors.New("tgutil: nil file handle")
	}
	fresh, err := t.ResolveMedia(ctx, fh.ChatID, fh.MsgID)
	if err != nil {
		return err
	}
	fh.Location = fresh.Location
	fh.Size = fresh.Size
	fh.DC = fresh.DC
	return nil
}

// convertMessage maps mtgo's message wrapper onto the seam type.
func convertMessage(msg *types.Message) IncomingMsg {
	if msg == nil {
		return IncomingMsg{}
	}
	in := IncomingMsg{
		ChatID:       msg.ChatID,
		SenderID:     msg.FromID,
		MsgID:        int(msg.ID),
		Text:         msg.Text,
		ReplyToMsgID: int(msg.ReplyToID),
		Raw:          msg,
	}
	// Chat-type flags. mtgo's Chat may be nil when the peer is not cached;
	// fall back to the Bot API marked-ID convention (users positive, basic
	// groups small-negative, channels/supergroups -100-prefixed).
	switch {
	case msg.Chat == nil:
		in.IsPrivate = msg.ChatID > 0
		in.IsChannel = msg.ChatID <= markedChannelBasis
		in.IsGroup = !in.IsPrivate && !in.IsChannel
	case msg.Chat.Type == types.ChatTypeChannel:
		in.IsChannel = true
	case msg.Chat.Type == types.ChatTypePrivate || msg.Chat.Type == types.ChatTypeBot:
		in.IsPrivate = true
	default: // group, supergroup, forum
		in.IsGroup = true
	}
	in.IsCommand, in.CommandName, in.Args = parseCommand(msg)
	in.Media = extractMediaInfo(msg, msg.ChatID)
	return in
}

// convertCallback maps mtgo's callback wrapper onto the seam type.
func convertCallback(cb *types.CallbackQuery) CallbackQuery {
	if cb == nil {
		return CallbackQuery{}
	}
	return CallbackQuery{
		ID:        cb.ID,
		ChatID:    cb.ChatID,
		SenderID:  cb.UserID,
		MessageID: int(cb.MessageID),
		Data:      string(cb.Data),
		Raw:       cb,
	}
}

// markedChannelBasis is the Bot API marked-ID basis for channels/supergroups.
const markedChannelBasis = -1000000000000

func markedChannelID(bare int64) int64 { return markedChannelBasis - bare }

// parseCommand detects a BotCommand entity at offset 0 (the mtgo wrapper's
// Command/Matches fields are declared but never populated in v0.21.0).
// Returns (ok, name-without-@bot-suffix, args-after-command).
func parseCommand(msg *types.Message) (ok bool, name, args string) {
	if msg == nil || len(msg.Entities) == 0 {
		return false, "", ""
	}
	for _, e := range msg.Entities {
		if e == nil || e.Type != types.MessageEntityTypeBotCommand {
			continue
		}
		if e.Offset != 0 || e.Length <= 1 || e.Length > len(msg.Text) {
			continue // not a leading command (or "/")
		}
		word := msg.Text[0:e.Length]
		if !strings.HasPrefix(word, "/") {
			return false, "", ""
		}
		name = strings.TrimPrefix(word, "/")
		if i := strings.IndexByte(name, '@'); i > 0 {
			name = name[:i]
		}
		args = strings.TrimSpace(msg.Text[e.Length:])
		return true, name, args
	}
	return false, "", ""
}

// extractMediaInfo converts the parsed media of a message into a MediaInfo
// with an mtgo file location. Returns nil when the message carries no
// downloadable media. Mirrors the extraction patterns of tgutil/util.go but
// against mtgo's tg types (which is legal here: this file owns mtgo).
func extractMediaInfo(msg *types.Message, chatID int64) *MediaInfo {
	if msg == nil {
		return nil
	}

	// Photos: resolve the largest size variant (progressive photos report the
	// largest entry as the true byte size).
	if p := msg.Photo; p != nil {
		size, sizeType := largestPhotoSize(p.Sizes)
		if sizeType == "" {
			return nil
		}
		loc := &tg.InputPhotoFileLocation{
			ID:            p.ID,
			AccessHash:    p.AccessHash,
			FileReference: p.FileReference,
			ThumbSize:     sizeType,
		}
		return &MediaInfo{
			Kind:     "photo",
			Name:     "photo.jpg",
			Mime:     "image/jpeg",
			Size:     size,
			DC:       int(p.DCID),
			MsgID:    int(msg.ID),
			ChatID:   chatID,
			Location: loc,
			FileKey:  fmt.Sprintf("photo:%d:%d", p.ID, p.AccessHash),
		}
	}

	// Document family: sticker/animation carry *tg.Document in .Raw, the rest
	// in .RawDocument.
	doc, kind := documentOf(msg)
	if doc == nil {
		return nil
	}
	name := docName(doc, kind)
	loc := &tg.InputDocumentFileLocation{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}
	return &MediaInfo{
		Kind:     kind,
		Name:     name,
		Mime:     doc.MimeType,
		Size:     doc.Size,
		DC:       int(doc.DCID),
		MsgID:    int(msg.ID),
		ChatID:   chatID,
		Location: loc,
		FileKey:  fmt.Sprintf("doc:%d:%d", doc.ID, doc.AccessHash),
	}
}

// documentOf extracts the underlying *tg.Document for the supported media
// kinds and maps them onto the seam's Kind vocabulary. Video notes are served
// as "video" (they are streamable MP4 circles).
func documentOf(msg *types.Message) (*tg.Document, string) {
	switch {
	case msg.Sticker != nil && msg.Sticker.Raw != nil:
		return msg.Sticker.Raw, "sticker"
	case msg.Animation != nil && msg.Animation.Raw != nil:
		return msg.Animation.Raw, "animation"
	case msg.Video != nil && msg.Video.RawDocument != nil:
		return msg.Video.RawDocument, "video"
	case msg.VideoNote != nil && msg.VideoNote.RawDocument != nil:
		return msg.VideoNote.RawDocument, "video"
	case msg.Voice != nil && msg.Voice.RawDocument != nil:
		return msg.Voice.RawDocument, "voice"
	case msg.Audio != nil && msg.Audio.RawDocument != nil:
		return msg.Audio.RawDocument, "audio"
	case msg.Document != nil && msg.Document.RawDocument != nil:
		return msg.Document.RawDocument, "document"
	default:
		return nil, ""
	}
}

func docName(doc *tg.Document, kind string) string {
	for _, attr := range doc.Attributes {
		if fn, ok := attr.(*tg.DocumentAttributeFilename); ok && fn.FileName != "" {
			return fn.FileName
		}
	}
	if ext := mimeToExt(doc.MimeType); ext != "" {
		return fmt.Sprintf("file_%d.%s", doc.ID, ext)
	}
	switch kind {
	case "video":
		return "video.mp4"
	case "audio":
		return "audio.mp3"
	case "voice":
		return "voice.ogg"
	case "animation":
		return "animation.mp4"
	case "sticker":
		return "sticker.webp"
	default:
		return "file.bin"
	}
}

func largestPhotoSize(sizes []types.PhotoSize) (int64, string) {
	var (
		bestSize int64
		bestType string
	)
	for _, s := range sizes {
		if s.Size > int(bestSize) {
			bestSize = int64(s.Size)
			bestType = s.Type
		}
	}
	return bestSize, bestType
}

func userInfoFrom(u *types.User) UserInfo {
	if u == nil {
		return UserInfo{}
	}
	info := UserInfo{
		ID:        u.ID,
		FirstName: u.FirstName,
		LastName:  u.LastName,
		Username:  u.Username,
		IsBot:     u.IsBot,
	}
	if u.Photo != nil {
		info.PhotoDC = int(u.Photo.DcID)
	}
	return info
}

func userIDOf(u *types.User) int64 {
	if u == nil {
		return 0
	}
	return u.ID
}

// ---------------------------------------------------------------------------
// Chunk fetching
// ---------------------------------------------------------------------------

// FetchChunk downloads [offset, offset+limit) of a resolved file via the raw
// upload.getFile RPC.
//
// Chunk geometry (Telegram server rules): offset must be a multiple of 4096;
// limit must be a multiple of 4096 in (0, 1 MiB]. Invalid arguments return an
// error wrapping ErrMediaUnsupported — these are caller bugs, not media
// faults.
//
// Error mapping: FLOOD_WAIT(_X) → *FloodWait; FILE_REFERENCE_EXPIRED →
// ErrFileRefExpired (pair with RefreshFileRef); MESSAGE_ID_INVALID /
// MESSAGE_DELETED / CHANNEL_* → ErrStaleMedia; DC migration / 5xx / network
// timeouts → ErrTransient; other RPC errors → *RPCError.
//
// FILE_MIGRATE note: client.Raw() wraps the client invoker, whose raw path
// (RPCInvokeRaw → handleRawMigrationError) transparently follows
// FILE_MIGRATE_X via an export-import side session on the target DC, exactly
// like the high-level path. Only the FIRST chunk can trigger it (Telegram
// rule), and the returned bytes are from the correct DC either way.
//
// CDN note: the request deliberately does NOT set CDNSupported. Per the
// MTProto schema, upload.getFile returns upload.fileCdnRedirect only when the
// client advertises CDN support on that request, so plain chunks are always
// served by Telegram DCs. The defensive CDN branch (decrypt + hash check)
// below still handles a redirect should the server send one regardless.
func (t *MTGOTransport) FetchChunk(ctx context.Context, fh FileHandle, offset int64, limit int32) ([]byte, error) {
	if offset < 0 || offset%chunkAlign != 0 {
		return nil, fmt.Errorf("%w: offset %d must be a multiple of %d", ErrMediaUnsupported, offset, chunkAlign)
	}
	if limit <= 0 || limit%chunkAlign != 0 || limit > maxChunkLen {
		return nil, fmt.Errorf("%w: limit %d must be a multiple of %d in (0, %d]", ErrMediaUnsupported, limit, chunkAlign, maxChunkLen)
	}
	loc, ok := fh.Location.(tg.InputFileLocationClass)
	if !ok || loc == nil {
		return nil, fmt.Errorf("%w: file handle has no mtgo location (got %T)", ErrMediaUnsupported, fh.Location)
	}

	res, err := t.client.Raw().UploadGetFile(ctx, &tg.UploadGetFileRequest{
		Location: loc,
		Offset:   offset,
		Limit:    limit,
	})
	if err != nil {
		return nil, t.mapErr(err, "upload.getFile")
	}
	switch f := res.(type) {
	case *tg.UploadFile:
		return f.Bytes, nil // empty Bytes = EOF
	case *tg.UploadFileCDNRedirect:
		return t.fetchCDNChunk(ctx, fh, offset, limit, f)
	default:
		return nil, fmt.Errorf("tgutil: unexpected upload.getFile result %T", res)
	}
}

// cdnState carries the per-file CDN redirect session: decryption material,
// hash coverage and the sequential-cursor used by the hash checker.
type cdnState struct {
	mu      sync.Mutex
	redir   *tg.UploadFileCDNRedirect
	hashes  []*tg.FileHash // accumulated hash coverage (redirect.FileHashes + extensions)
	idx     int            // first hash not fully consumed
	buf     []byte         // partial hash bytes buffered across sequential chunks
	nextOff int64          // expected offset of the next sequential chunk
}

func cdnKey(fh FileHandle) string {
	return fmt.Sprintf("%d:%d", fh.ChatID, fh.MsgID)
}

// fetchCDNChunk serves one chunk from a CDN redirect. The decrypt + hash
// verification follows mtgo's internal approach (download.go downloadCDNToWriter):
// AES-CTR with the redirect IV advanced by offset/16 blocks, and SHA-256
// verification over the authenticated hash ranges (extended on demand via
// upload.getCDNFileHashes).
//
// Scope limitation, documented for the record: mtgo reaches the redirect DC
// through its unexported dcRPC side-session pool. From outside the library we
// can only issue CDN RPCs on the home invoker, which is correct when
// redirect.DCID matches the home DC. Because FetchChunk never sets
// CDNSupported, a redirect should not occur at all; a foreign-DC redirect
// therefore surfaces as a descriptive transient-class error rather than a
// silent failure.
func (t *MTGOTransport) fetchCDNChunk(ctx context.Context, fh FileHandle, offset int64, limit int32, redirect *tg.UploadFileCDNRedirect) ([]byte, error) {
	key := cdnKey(fh)
	t.cdnMu.Lock()
	st := t.cdn[key]
	if st == nil {
		st = &cdnState{redir: redirect, hashes: slices.Clone(redirect.FileHashes)}
		t.cdn[key] = st
	}
	t.cdnMu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()

	if st.redir.DCID != 0 && int(st.redir.DCID) != t.OwnDC(ctx) {
		return nil, fmt.Errorf("%w: cdn redirect from foreign dc %d (cdn support was not requested)",
			ErrTransient, st.redir.DCID)
	}
	rpc := t.client.Raw()

	// Extend hash coverage for this offset when the redirect's own hashes do
	// not reach it (mirrors mtgo's cdnHashChecker.ensureCoverage).
	if err := st.ensureCoverage(ctx, rpc, offset); err != nil {
		return nil, t.mapErr(err, "cdn hashes")
	}
	// Cap the read at the authenticated range; Telegram only serves CDN data
	// inside hashes it has attested.
	avail := st.coverageEndLocked(offset) - offset
	n := int64(limit)
	if avail > 0 && avail < n {
		n = avail
	}

	var data []byte
	var reuploads int
	for {
		res, err := rpc.UploadGetCDNFile(ctx, &tg.UploadGetCDNFileRequest{
			FileToken: st.redir.FileToken,
			Offset:    offset,
			Limit:     int32(n), //nosec G115 // n <= limit which fits int32
		})
		if err != nil {
			return nil, t.mapErr(err, "upload.getCDNFile")
		}
		switch v := res.(type) {
		case *tg.UploadCDNFile:
			data = v.Bytes
		case *tg.UploadCDNFileReuploadNeeded:
			reuploads++
			if reuploads > 3 {
				return nil, fmt.Errorf("%w: cdn reupload loop at offset %d", ErrTransient, offset)
			}
			if _, err := rpc.UploadReuploadCDNFile(ctx, &tg.UploadReuploadCDNFileRequest{
				FileToken:    st.redir.FileToken,
				RequestToken: v.RequestToken,
			}); err != nil {
				return nil, t.mapErr(err, "upload.reuploadCDNFile")
			}
			continue
		default:
			return nil, fmt.Errorf("tgutil: unexpected upload.getCDNFile result %T", res)
		}
		break
	}
	if len(data) == 0 {
		return nil, nil // EOF inside the authenticated range
	}
	plain, err := cdnDecrypt(data, st.redir.EncryptionKey, st.redir.EncryptionIv, offset)
	if err != nil {
		return nil, err
	}
	if err := st.verify(plain, offset, t.log); err != nil {
		return nil, fmt.Errorf("%w: cdn hash mismatch: %s", ErrTransient, err)
	}
	st.nextOff = offset + int64(len(plain))
	return plain, nil
}

// ensureCoverage extends the hash list until some authenticated range covers
// offset (ported from mtgo's cdnHashChecker.ensureCoverage).
func (st *cdnState) ensureCoverage(ctx context.Context, rpc *tg.RPCClient, offset int64) error {
	for attempt := 0; attempt < 3; attempt++ {
		if st.validFromLocked() && st.coverageEndLocked(offset) > offset {
			return nil
		}
		result, err := rpc.Invoke(ctx, &tg.UploadGetCDNFileHashesRequest{
			FileToken: st.redir.FileToken,
			Offset:    offset,
		}, decodeFileHashVector)
		if err != nil {
			return fmt.Errorf("tgutil: upload.getCDNFileHashes: %w", err)
		}
		fhr, ok := result.(*fileHashVector)
		if !ok || len(fhr.items) == 0 {
			return fmt.Errorf("tgutil: cdn: server returned no hashes at offset %d", offset)
		}
		st.hashes = append(st.hashes, fhr.items...)
		sort.Slice(st.hashes[st.idx:], func(i, j int) bool {
			return st.hashes[st.idx+i].Offset < st.hashes[st.idx+j].Offset
		})
	}
	return fmt.Errorf("tgutil: cdn: no authenticated range at offset %d", offset)
}

func (st *cdnState) validFromLocked() bool {
	for _, h := range st.hashes {
		if h == nil || h.Offset < 0 || h.Limit <= 0 || len(h.Hash) != sha256.Size {
			return false
		}
	}
	return true
}

// coverageEndLocked returns the end of the contiguous authenticated range
// starting at (or containing) offset.
func (st *cdnState) coverageEndLocked(offset int64) int64 {
	for st.idx < len(st.hashes) {
		h := st.hashes[st.idx]
		if h.Offset+int64(h.Limit) <= offset {
			st.idx++
			continue
		}
		break
	}
	end := offset
	for i := st.idx; i < len(st.hashes); i++ {
		h := st.hashes[i]
		start := h.Offset
		if i == st.idx && st.nextOff > offset {
			// Continuing a sequential stream across a chunk boundary: the
			// in-progress hash range started before this chunk.
			start = min64(start, st.nextOff)
		}
		if start > end {
			break // gap: only up to `end` is attested
		}
		if hashEnd := h.Offset + int64(h.Limit); hashEnd > end {
			end = hashEnd
		}
	}
	return end
}

// verify checks every hash range that is FULLY contained in [offset,
// offset+len(data)) against the plaintext. Ranges straddling the edges are
// verified when a later sequential chunk completes them (buffered via
// pendingStart), matching mtgo's cross-chunk checker semantics; a seek into
// the middle of a range leaves that one range unverified.
func (st *cdnState) verify(data []byte, offset int64, log *slog.Logger) error {
	for i := st.idx; i < len(st.hashes); i++ {
		h := st.hashes[i]
		hashEnd := h.Offset + int64(h.Limit)
		if h.Offset >= offset+int64(len(data)) {
			break // beyond this chunk
		}
		if hashEnd > offset+int64(len(data)) {
			break // incomplete: buffered continuation on a later chunk
		}
		from := h.Offset
		if from < offset {
			// Range started in a previous chunk (or in a seeked-over gap).
			// We can only verify when the stream was sequential.
			if st.nextOff != offset {
				log.Debug("tgutil: cdn hash range skipped after seek",
					"hash_offset", h.Offset, "chunk_offset", offset)
				st.idx = i + 1
				continue
			}
			if len(st.buf) == 0 {
				// No buffered bytes (e.g. state rebuilt) — cannot verify.
				st.idx = i + 1
				continue
			}
			sum := sha256.Sum256(append(st.buf, data[:hashEnd-offset]...))
			if !slices.Equal(sum[:], h.Hash) {
				return fmt.Errorf("hash mismatch at offset %d", h.Offset)
			}
			st.buf = nil
			st.idx = i + 1
			continue
		}
		relStart := h.Offset - offset
		relEnd := hashEnd - offset
		sum := sha256.Sum256(data[relStart:relEnd])
		if !slices.Equal(sum[:], h.Hash) {
			return fmt.Errorf("hash mismatch at offset %d", h.Offset)
		}
		st.idx = i + 1
	}
	// Buffer the tail of an in-progress hash for the next sequential chunk.
	for i := st.idx; i < len(st.hashes); i++ {
		h := st.hashes[i]
		if h.Offset > offset {
			break
		}
		if h.Offset+int64(h.Limit) > offset+int64(len(data)) {
			rel := int(h.Offset - offset)
			if rel < 0 {
				rel = 0
			}
			st.buf = append(st.buf[:0], data[rel:]...)
			break
		}
	}
	return nil
}

// cdnDecrypt decrypts an upload.getCDNFile response in place of mtgo's
// unexported cdnDecryptChunk: AES-128-CTR whose 128-bit counter is the
// redirect IV advanced by offset/16 blocks. mtgo's counter increments the
// last byte first (little-endian carry, download.go incrementIV), which is
// byte-for-byte the increment order of crypto/cipher's CTR, so a pre-advanced
// counter plus cipher.NewCTR reproduces the exact keystream.
func cdnDecrypt(data, key, iv []byte, offset int64) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("tgutil: cdn key: %w", err)
	}
	if len(iv) < 16 {
		return nil, fmt.Errorf("tgutil: cdn iv too short: %d", len(iv))
	}
	counter := make([]byte, 16)
	copy(counter, iv[:16])
	addToIV(counter, offset/16)
	out := make([]byte, len(data))
	cipher.NewCTR(block, counter).XORKeyStream(out, data)
	return out, nil
}

// addToIV adds n to the 16-byte counter treating byte 15 as least significant
// (same carry direction as mtgo's incrementIV/addToIVLE).
func addToIV(iv []byte, n int64) {
	if n <= 0 {
		return
	}
	carry := uint64(n)
	for i := 15; i >= 0 && carry != 0; i-- {
		sum := uint64(iv[i]) + (carry & 0xff)
		iv[i] = byte(sum)
		carry = (sum >> 8) + (carry >> 8)
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// fileHashVector wraps a bare TL vector<fileHash> response from
// upload.getCDNFileHashes (mirrors mtgo's internal decodeFileHashVector).
type fileHashVector struct {
	items []*tg.FileHash
}

func (*fileHashVector) ConstructorID() uint32        { return tg.VectorTypeID }
func (*fileHashVector) Encode(_ *bytes.Buffer) error { return nil }

// decodeFileHashVector decodes a bare TL vector<fileHash> payload.
func decodeFileHashVector(r *tg.Reader) (tg.TLObject, error) {
	hdr, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	if hdr != tg.VectorTypeID {
		return nil, fmt.Errorf("tgutil: expected fileHash vector, got constructor 0x%x", hdr)
	}
	count, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	if err := tg.CheckVectorCount(count); err != nil {
		return nil, err
	}
	items := make([]*tg.FileHash, count)
	for i := range items {
		h, err := tg.DecodeFileHash(r)
		if err != nil {
			return nil, err
		}
		items[i] = h
	}
	return &fileHashVector{items: items}, nil
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

// mapErr translates a backend error into the tgutil taxonomy. args is used
// only for diagnostics on unmapped errors.
func (t *MTGOTransport) mapErr(err error, op string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err // caller's own cancellation — pass through untouched
	}

	if wait, ok := tgerr.AsFloodWait(err); ok {
		return &FloodWait{Seconds: int(wait.Seconds())}
	}
	if tgerr.Is(err, tgerr.ErrFileReferenceExpired) {
		return fmt.Errorf("%w: %s", ErrFileRefExpired, op)
	}
	if tgErr, ok := tgerr.As(err); ok {
		if slices.Contains(permanentRPCTypes, tgErr.Type) {
			return fmt.Errorf("%w: %s: %s", ErrStaleMedia, op, tgErr.Type)
		}
		if tgErr.Code >= 500 || tgErr.Code == 303 {
			return fmt.Errorf("%w: %s: rpc %d %s", ErrTransient, op, tgErr.Code, tgErr.Message)
		}
		return &RPCError{Code: tgErr.Code, Type: tgErr.Type, Message: tgErr.Message, Argument: tgErr.Argument}
	}

	var mig *telegram.MigrationError
	if errors.As(err, &mig) {
		return fmt.Errorf("%w: %s: dc migration to %d: %v", ErrTransient, op, mig.TargetDC, mig.Err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %s: %v", ErrTransient, op, err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("%w: %s: %v", ErrTransient, op, err)
	}
	return err // unknown — leave intact for Classify → ErrClassOther
}
