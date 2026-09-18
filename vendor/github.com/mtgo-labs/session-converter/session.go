// Package tgconv converts Telegram session strings between library formats.
//
// A session string encapsulates the data needed to connect to Telegram
// without re-authenticating: the data center ID, server address, port, and
// the 256-byte authorization key (plus optional fields like user ID and
// API ID).
//
// Supported formats:
//
//   - Telethon
//   - Pyrogram
//   - GramJS
//   - mtcute
//   - MTKruto
//   - gogram
//   - gotgproto (gotd/td)
//   - mtgo (native MTGO1 format; see EncodeSession)
//
// SQLite session files from Telethon and Pyrogram are also supported via
// ReadSQLite.
package tgconv

// Format identifies a session string encoding format.
type Format string

const (
	FormatTelethon  Format = "telethon"
	FormatPyrogram  Format = "pyrogram"
	FormatGramJS    Format = "gramjs"
	FormatMtcute    Format = "mtcute"
	FormatMTKruto   Format = "mtkruto"
	FormatGogram    Format = "gogram"
	FormatGotgproto Format = "gotgproto"
	FormatMTGO      Format = "mtgo"
)

// AllFormats lists every supported format.
var AllFormats = []Format{
	FormatTelethon,
	FormatPyrogram,
	FormatGramJS,
	FormatMtcute,
	FormatMTKruto,
	FormatGogram,
	FormatGotgproto,
	FormatMTGO,
}

// Session holds the common fields extracted from any session format. Not
// every field is populated by every format — the auth key and DC ID are
// always present; the rest depend on the source.
type Session struct {
	DCID          int    // data center ID (1-5)
	ServerAddress string // IP address or hostname
	Port          int    // server port
	AuthKey       []byte // 256-byte MTProto authorization key
	AppID         int32  // API ID from my.telegram.org
	TestMode      bool   // connected to test servers
	UserID        int64  // authenticated user/bot ID
	IsBot         bool   // whether the account is a bot
	// APIHash is the 32-hex-char API hash from my.telegram.org. Only the
	// native mtgo format carries it.
	APIHash string
	// PhoneNumber is the account phone in international format. Only the
	// native mtgo format carries it; it is empty for bot sessions.
	PhoneNumber string
}
