package telegram

import (
	"github.com/mtgo-labs/mtgo/tg"
)

// KeyboardBuilder provides a fluent interface for constructing Telegram
// keyboards. Add buttons to the current row, call Next() to start a new row,
// then Build() for inline or BuildReply() for a reply keyboard.
//
// Inline:
//
//	markup := telegram.Keyboard().
//	    Callback("Yes", "yes").Primary().
//	    Callback("No", "no").Danger().
//	    Next().
//	    URL("Docs", "https://example.com").
//	    Build()
//
// Reply:
//
//	markup := telegram.Keyboard().
//	    Text("Option A").
//	    Text("Option B").
//	    BuildReply(telegram.ReplyOpts{Resize: true, OneTime: true})
type KeyboardBuilder struct {
	rows [][]builtButton
	row  []builtButton
}

// builtButton holds one button in its wire form. Inline-button methods fill
// the inline form, reply-button methods the reply form; the missing form is
// derived at build time because layer 229 splits the two button kinds into
// separate constructor hierarchies (keyboardButton#ButtonType vs
// keyboardInlineButton#InlineButtonType).
type builtButton struct {
	text   string
	inline *tg.KeyboardInlineButton
	reply  *tg.KeyboardButton
}

// Keyboard returns a new keyboard builder.
func Keyboard() *KeyboardBuilder {
	return &KeyboardBuilder{
		rows: make([][]builtButton, 0, 4),
		row:  make([]builtButton, 0, 6),
	}
}

func (b *KeyboardBuilder) addInline(btn *tg.KeyboardInlineButton) *KeyboardBuilder {
	b.row = append(b.row, builtButton{text: btn.Text, inline: btn})
	return b
}

func (b *KeyboardBuilder) addReply(btn *tg.KeyboardButton) *KeyboardBuilder {
	b.row = append(b.row, builtButton{text: btn.Text, reply: btn})
	return b
}

// Next finalizes the current row and starts a new one. No-op if the row is empty.
func (b *KeyboardBuilder) Next() *KeyboardBuilder {
	if len(b.row) > 0 {
		b.rows = append(b.rows, b.row)
		b.row = make([]builtButton, 0, cap(b.row))
	}
	return b
}

// InlineRow appends a pre-built row of inline buttons.
func (b *KeyboardBuilder) InlineRow(buttons ...*tg.KeyboardInlineButton) *KeyboardBuilder {
	if len(buttons) > 0 {
		row := make([]builtButton, len(buttons))
		for i, btn := range buttons {
			row[i] = builtButton{text: btn.Text, inline: btn}
		}
		b.rows = append(b.rows, row)
	}
	return b
}

// ReplyRow appends a pre-built row of reply-keyboard buttons.
func (b *KeyboardBuilder) ReplyRow(buttons ...*tg.KeyboardButton) *KeyboardBuilder {
	if len(buttons) > 0 {
		row := make([]builtButton, len(buttons))
		for i, btn := range buttons {
			row[i] = builtButton{text: btn.Text, reply: btn}
		}
		b.rows = append(b.rows, row)
	}
	return b
}

// ---------------------------------------------------------------------------
// Inline buttons
// ---------------------------------------------------------------------------

// Callback adds a callback button. data is truncated to 64 bytes (Telegram limit).
func (b *KeyboardBuilder) Callback(text, data string) *KeyboardBuilder {
	d := []byte(data)
	if len(d) > 64 {
		d = d[:64]
	}
	return b.addInline(&tg.KeyboardInlineButton{Text: text, Type: &tg.InlineButtonTypeCallback{Data: d}})
}

// URL adds a button that opens url when tapped.
func (b *KeyboardBuilder) URL(text, url string) *KeyboardBuilder {
	return b.addInline(&tg.KeyboardInlineButton{Text: text, Type: &tg.InlineButtonTypeURL{URL: url}})
}

// Switch adds a button that switches the user to inline mode.
// samePeer=true sends the query in the current chat; false lets the user pick.
func (b *KeyboardBuilder) Switch(text string, samePeer bool, query string) *KeyboardBuilder {
	return b.addInline(&tg.KeyboardInlineButton{
		Text: text,
		Type: &tg.InlineButtonTypeSwitchInline{Query: query, SamePeer: samePeer},
	})
}

// Copy adds a button that copies copyText to the user's clipboard.
func (b *KeyboardBuilder) Copy(text, copyText string) *KeyboardBuilder {
	return b.addInline(&tg.KeyboardInlineButton{Text: text, Type: &tg.InlineButtonTypeCopy{CopyText: copyText}})
}

// Game adds an HTML5 game button.
func (b *KeyboardBuilder) Game(text string) *KeyboardBuilder {
	return b.addInline(&tg.KeyboardInlineButton{Text: text, Type: &tg.InlineButtonTypeGame{}})
}

// Buy adds a payment button.
func (b *KeyboardBuilder) Buy(text string) *KeyboardBuilder {
	return b.addInline(&tg.KeyboardInlineButton{Text: text, Type: &tg.InlineButtonTypeBuy{}})
}

// WebApp adds a button that opens a Telegram Mini App.
func (b *KeyboardBuilder) WebApp(text, url string) *KeyboardBuilder {
	return b.addInline(&tg.KeyboardInlineButton{Text: text, Type: &tg.InlineButtonTypeWebView{URL: url}})
}

// ---------------------------------------------------------------------------
// Reply buttons
// ---------------------------------------------------------------------------

// Text adds a text button (sends its text as a message when tapped).
func (b *KeyboardBuilder) Text(text string) *KeyboardBuilder {
	return b.addReply(&tg.KeyboardButton{Text: text, Type: &tg.ButtonTypeDefault{}})
}

// RequestPhone adds a button that requests the user's phone number.
func (b *KeyboardBuilder) RequestPhone(text string) *KeyboardBuilder {
	return b.addReply(&tg.KeyboardButton{Text: text, Type: &tg.ButtonTypeRequestPhone{}})
}

// RequestPeer adds a button that lets the user share a chat, channel, or user.
// buttonID identifies which button was pressed in the response.
// peerType is one of &tg.RequestPeerTypeUser{}, &tg.RequestPeerTypeChat{},
// or &tg.RequestPeerTypeBroadcast{}.
// maxQuantity controls how many peers the user can share (for users only).
// PeerUserOpts configures optional filters when requesting a user peer.
type PeerUserOpts struct {
	Bot     bool
	Premium bool
}

// PeerGroupOpts controls optional filters when requesting a group peer.
type PeerGroupOpts struct {
	Creator        bool
	BotParticipant bool
	HasUsername    bool
	Forum          bool
}

// PeerChannelOpts controls optional filters when requesting a channel peer.
type PeerChannelOpts struct {
	Creator     bool
	HasUsername bool
}

func (b *KeyboardBuilder) RequestPeer(text string, buttonID int32, peerType tg.RequestPeerTypeClass, maxQuantity int32) *KeyboardBuilder {
	return b.addReply(&tg.KeyboardButton{
		Text: text,
		Type: &tg.InputButtonTypeRequestPeer{
			ButtonID:    buttonID,
			PeerType:    peerType,
			MaxQuantity: maxQuantity,
		},
	})
}

func (b *KeyboardBuilder) RequestUser(text string, buttonID int32, maxQuantity int32, opts ...PeerUserOpts) *KeyboardBuilder {
	o := getOptDef(PeerUserOpts{}, opts...)
	return b.RequestPeer(text, buttonID, &tg.RequestPeerTypeUser{
		Bot:     o.Bot,
		Premium: o.Premium,
	}, maxQuantity)
}

func (b *KeyboardBuilder) RequestGroup(text string, buttonID int32, opts ...PeerGroupOpts) *KeyboardBuilder {
	o := getOptDef(PeerGroupOpts{}, opts...)
	return b.RequestPeer(text, buttonID, &tg.RequestPeerTypeChat{
		Creator:        o.Creator,
		BotParticipant: o.BotParticipant,
		HasUsername:    o.HasUsername,
		Forum:          o.Forum,
	}, 1)
}

func (b *KeyboardBuilder) RequestChannel(text string, buttonID int32, opts ...PeerChannelOpts) *KeyboardBuilder {
	o := getOptDef(PeerChannelOpts{}, opts...)
	return b.RequestPeer(text, buttonID, &tg.RequestPeerTypeBroadcast{
		Creator:     o.Creator,
		HasUsername: o.HasUsername,
	}, 1)
}

// RequestGeo adds a button that requests the user's location.
func (b *KeyboardBuilder) RequestGeo(text string) *KeyboardBuilder {
	return b.addReply(&tg.KeyboardButton{Text: text, Type: &tg.ButtonTypeRequestGeoLocation{}})
}

// RequestPoll adds a button that prompts the user to create a poll or quiz.
func (b *KeyboardBuilder) RequestPoll(text string, quiz bool) *KeyboardBuilder {
	t := &tg.ButtonTypeRequestPoll{Quiz: quiz}
	// Always set the flag so quiz=false serializes as an explicit boolFalse.
	t.Flags.Set(0)
	return b.addReply(&tg.KeyboardButton{Text: text, Type: t})
}

// ---------------------------------------------------------------------------
// Style modifiers (applied to the last button in the current row)
// ---------------------------------------------------------------------------

// Primary highlights the last button with a primary background color.
func (b *KeyboardBuilder) Primary() *KeyboardBuilder { return b.applyStyle(primary) }

// Danger marks the last button as destructive (red background).
func (b *KeyboardBuilder) Danger() *KeyboardBuilder { return b.applyStyle(danger) }

// Success marks the last button as positive (green background).
func (b *KeyboardBuilder) Success() *KeyboardBuilder { return b.applyStyle(success) }

// Icon sets a custom emoji icon on the last button by custom emoji document ID.
func (b *KeyboardBuilder) Icon(docID int64) *KeyboardBuilder {
	return b.applyStyle(func(s *tg.KeyboardButtonStyle) { s.Icon = docID })
}

func primary(s *tg.KeyboardButtonStyle) { s.BgPrimary = true }
func danger(s *tg.KeyboardButtonStyle)  { s.BgDanger = true }
func success(s *tg.KeyboardButtonStyle) { s.BgSuccess = true }

// applyStyle modifies the Style field of the last button in the current row.
// No-op if the row is empty.
func (b *KeyboardBuilder) applyStyle(fn func(*tg.KeyboardButtonStyle)) *KeyboardBuilder {
	if len(b.row) == 0 {
		return b
	}
	if s := b.row[len(b.row)-1].style(); s != nil {
		fn(s)
	}
	return b
}

// style returns a pointer to the button's Style field, lazily creating it.
func (bb *builtButton) style() *tg.KeyboardButtonStyle {
	var p **tg.KeyboardButtonStyle
	switch {
	case bb.inline != nil:
		p = &bb.inline.Style
	case bb.reply != nil:
		p = &bb.reply.Style
	default:
		return nil
	}
	if *p == nil {
		*p = &tg.KeyboardButtonStyle{}
	}
	return *p
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

// ReplyOpts controls the appearance and behaviour of a reply keyboard.
type ReplyOpts struct {
	Resize      bool   // Shrink keyboard to fit fewer buttons.
	OneTime     bool   // Hide after first press.
	Selective   bool   // Show only to @mentioned or replied-to users.
	Persistent  bool   // Don't auto-hide when another keyboard is sent.
	Placeholder string // Hint text in the input field. Empty = default.
}

// finalize closes the current row and returns all accumulated rows, or nil.
func (b *KeyboardBuilder) finalize() [][]builtButton {
	b.Next()
	if len(b.rows) == 0 {
		return nil
	}
	return b.rows
}

// toInline returns the inline wire form of the button. Reply-only buttons
// degrade to a disabled text button (layer 229 has no plain inline button).
func (bb builtButton) toInline() *tg.KeyboardInlineButton {
	if bb.inline != nil {
		return bb.inline
	}
	return &tg.KeyboardInlineButton{Text: bb.text, Type: &tg.InlineButtonTypeDisabled{}}
}

// toReply returns the reply-keyboard wire form of the button. Inline-only
// buttons degrade to a plain text button.
func (bb builtButton) toReply() *tg.KeyboardButton {
	if bb.reply != nil {
		return bb.reply
	}
	return &tg.KeyboardButton{Text: bb.text, Type: &tg.ButtonTypeDefault{}}
}

// Build produces an inline keyboard (tg.ReplyInlineMarkup).
// Returns nil if no buttons were added.
func (b *KeyboardBuilder) Build() tg.ReplyMarkupClass {
	rows := b.finalize()
	if rows == nil {
		return nil
	}
	out := make([]*tg.KeyboardInlineButtonRow, len(rows))
	for i, row := range rows {
		buttons := make([]*tg.KeyboardInlineButton, len(row))
		for j, bb := range row {
			buttons[j] = bb.toInline()
		}
		out[i] = &tg.KeyboardInlineButtonRow{Buttons: buttons}
	}
	return &tg.ReplyInlineMarkup{Rows: out}
}

// BuildReply produces a reply keyboard (tg.ReplyKeyboardMarkup).
// Returns nil if no buttons were added.
func (b *KeyboardBuilder) BuildReply(opts ...ReplyOpts) tg.ReplyMarkupClass {
	rows := b.finalize()
	if rows == nil {
		return nil
	}

	var o ReplyOpts
	if len(opts) > 0 {
		o = opts[0]
	}

	out := make([]*tg.KeyboardButtonRow, len(rows))
	for i, row := range rows {
		buttons := make([]*tg.KeyboardButton, len(row))
		for j, bb := range row {
			buttons[j] = bb.toReply()
		}
		out[i] = &tg.KeyboardButtonRow{Buttons: buttons}
	}

	m := &tg.ReplyKeyboardMarkup{
		Rows:       out,
		Resize:     o.Resize,
		SingleUse:  o.OneTime,
		Selective:  o.Selective,
		Persistent: o.Persistent,
	}
	if o.Resize {
		m.Flags |= 1 << 0
	}
	if o.OneTime {
		m.Flags |= 1 << 1
	}
	if o.Selective {
		m.Flags |= 1 << 2
	}
	if o.Persistent {
		m.Flags |= 1 << 4
	}
	if o.Placeholder != "" {
		m.Placeholder = o.Placeholder
		m.Flags |= 1 << 3
	}
	return m
}

// ---------------------------------------------------------------------------
// Standalone markups
// ---------------------------------------------------------------------------

// ForceReplyMarkup returns a markup that forces the user to reply.
func ForceReplyMarkup(opts ...ReplyOpts) *tg.ReplyKeyboardForceReply {
	var o ReplyOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	m := &tg.ReplyKeyboardForceReply{
		SingleUse: o.OneTime,
		Selective: o.Selective,
	}
	if o.OneTime {
		m.Flags |= 1 << 1
	}
	if o.Selective {
		m.Flags |= 1 << 2
	}
	if o.Placeholder != "" {
		m.Placeholder = o.Placeholder
		m.Flags |= 1 << 3
	}
	return m
}

// RemoveKeyboard returns a markup that removes the bot's reply keyboard.
func RemoveKeyboard(opts ...ReplyOpts) *tg.ReplyKeyboardHide {
	var o ReplyOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	m := &tg.ReplyKeyboardHide{
		Selective: o.Selective,
	}
	if o.Selective {
		m.Flags |= 1 << 2
	}
	return m
}
