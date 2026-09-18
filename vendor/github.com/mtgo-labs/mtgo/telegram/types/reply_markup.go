package types

import "github.com/mtgo-labs/mtgo/tg"

// ReplyMarkup represents a keyboard or inline button layout attached to a
// message. Supports inline keyboards, reply keyboards, keyboard hiding, and
// forced reply modes.
//
// Example:
//
//	if msg.ReplyMarkup != nil {
//	    for _, row := range msg.ReplyMarkup.Rows {
//	        for _, btn := range row {
//	            fmt.Printf("Button: %s (type=%d)\n", btn.Text, btn.Type)
//	        }
//	    }
//	}
type ReplyMarkup struct {
	// Type identifies which kind of reply markup this is (inline, keyboard, hide, or force reply).
	Type ReplyMarkupType
	// Rows holds the button layout as a two-dimensional slice (rows × columns).
	Rows [][]Button
	// Resize indicates that the reply keyboard should be resized to fit fewer buttons.
	Resize bool
	// SingleUse indicates that the reply keyboard should be hidden after the first press.
	SingleUse bool
	// Selective indicates the keyboard is shown only for targeted users.
	Selective bool
	// Persistent indicates the reply keyboard should remain visible even when the
	// user scrolls down in the chat.
	Persistent bool
	// Placeholder is the hint text shown in the text input field when the keyboard is active.
	Placeholder string
}

// Button represents a single keyboard or inline button with its action type
// and associated data such as URL, callback payload, or inline query.
type Button struct {
	// Type identifies the kind of action this button triggers when pressed.
	Type ButtonType
	// Text is the visible label shown to the user on the button.
	Text string
	// URL is the HTTP or tg:// URL opened when the button is pressed.
	URL string
	// Data is the opaque callback payload delivered to the bot when the user
	// presses a callback button.
	Data []byte
	// Query is the inline query string pre-filled when the user presses a
	// switch-inline button.
	Query string
	// SamePeer indicates that the inline query should remain in the same chat
	// rather than prompting the user to pick one.
	SamePeer bool
	// UserID is the user ID associated with a URL-auth button for login flows.
	UserID int64
	// CopyText is the payload copied to the clipboard for copy buttons.
	CopyText string
	// Quiz indicates that a poll button requests a quiz rather than a regular poll.
	Quiz bool
	// ButtonID identifies a request-peer button in the resulting callback.
	ButtonID int32
}

// ReplyMarkupType enumerates the kinds of reply markup a message can carry.
type ReplyMarkupType int

const (
	// ReplyMarkupInline represents an inline keyboard with callback or URL buttons.
	ReplyMarkupInline ReplyMarkupType = iota
	// ReplyMarkupKeyboard represents a custom reply keyboard shown below the input field.
	ReplyMarkupKeyboard
	// ReplyMarkupHide instructs the client to remove the bot's reply keyboard.
	ReplyMarkupHide
	// ReplyMarkupForceReply forces the client to display a reply interface.
	ReplyMarkupForceReply
)

// ButtonType enumerates the kinds of actions a keyboard button can trigger.
type ButtonType int

const (
	// ButtonDefault is a plain text button that sends its text as a message.
	ButtonDefault ButtonType = iota
	// ButtonURL opens a URL in the user's browser or Telegram app.
	ButtonURL
	// ButtonCallback sends a callback query to the bot with the button's data.
	ButtonCallback
	// ButtonSwitchInline triggers an inline query, optionally in the same chat.
	ButtonSwitchInline
	// ButtonGame launches a Telegram game when pressed.
	ButtonGame
	// ButtonBuy initiates a payment flow for an invoice.
	ButtonBuy
	// ButtonURLAuth triggers a URL-based Telegram login flow.
	ButtonURLAuth
	// ButtonWebView opens a Telegram Mini App.
	ButtonWebView
	// ButtonCopy copies text to the clipboard.
	ButtonCopy
	// ButtonUserProfile opens a user's profile.
	ButtonUserProfile
	// ButtonRequestPhone requests the user's phone number.
	ButtonRequestPhone
	// ButtonRequestGeo requests the user's location.
	ButtonRequestGeo
	// ButtonRequestPoll prompts the user to create a poll or quiz.
	ButtonRequestPoll
	// ButtonRequestPeer lets the user share a chat, channel, or user.
	ButtonRequestPeer
)

// ParseReplyMarkup converts a TL reply markup object into a ReplyMarkup.
// Returns nil if raw is nil.
func ParseReplyMarkup(raw tg.ReplyMarkupClass) *ReplyMarkup {
	if raw == nil {
		return nil
	}
	rm := &ReplyMarkup{}
	switch r := raw.(type) {
	case *tg.ReplyInlineMarkup:
		rm.Type = ReplyMarkupInline
		rm.Rows = parseInlineButtonRows(r.Rows)
	case *tg.ReplyKeyboardMarkup:
		rm.Type = ReplyMarkupKeyboard
		rm.Rows = parseReplyButtonRows(r.Rows)
		rm.Resize = r.Resize
		rm.SingleUse = r.SingleUse
		rm.Selective = r.Selective
		rm.Persistent = r.Persistent
		if r.Placeholder != "" {
			rm.Placeholder = r.Placeholder
		}
	case *tg.ReplyKeyboardHide:
		rm.Type = ReplyMarkupHide
		rm.Selective = r.Selective
	case *tg.ReplyKeyboardForceReply:
		rm.Type = ReplyMarkupForceReply
		rm.SingleUse = r.SingleUse
		rm.Selective = r.Selective
		if r.Placeholder != "" {
			rm.Placeholder = r.Placeholder
		}
	}
	return rm
}

func parseInlineButtonRows(rows []*tg.KeyboardInlineButtonRow) [][]Button {
	if rows == nil {
		return nil
	}
	result := make([][]Button, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		buttons := make([]Button, 0, len(row.Buttons))
		for _, btn := range row.Buttons {
			if btn != nil {
				buttons = append(buttons, parseInlineButton(btn))
			}
		}
		result = append(result, buttons)
	}
	return result
}

func parseReplyButtonRows(rows []*tg.KeyboardButtonRow) [][]Button {
	if rows == nil {
		return nil
	}
	result := make([][]Button, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		buttons := make([]Button, 0, len(row.Buttons))
		for _, btn := range row.Buttons {
			if btn != nil {
				buttons = append(buttons, parseReplyButton(btn))
			}
		}
		result = append(result, buttons)
	}
	return result
}

// parseInlineButton converts a layer 229 keyboardInlineButton, whose action
// lives in the nested InlineButtonType constructor.
func parseInlineButton(btn *tg.KeyboardInlineButton) Button {
	b := Button{Text: btn.Text}
	switch t := btn.Type.(type) {
	case *tg.InlineButtonTypeURL:
		b.Type = ButtonURL
		b.URL = t.URL
	case *tg.InlineButtonTypeCallback:
		b.Type = ButtonCallback
		b.Data = t.Data
	case *tg.InlineButtonTypeSwitchInline:
		b.Type = ButtonSwitchInline
		b.Query = t.Query
		b.SamePeer = t.SamePeer
	case *tg.InlineButtonTypeGame:
		b.Type = ButtonGame
	case *tg.InlineButtonTypeBuy:
		b.Type = ButtonBuy
	case *tg.InlineButtonTypeURLAuth:
		b.Type = ButtonURLAuth
		b.URL = t.URL
	case *tg.InputInlineButtonTypeURLAuth:
		b.Type = ButtonURLAuth
		b.URL = t.URL
	case *tg.InlineButtonTypeWebView:
		b.Type = ButtonWebView
		b.URL = t.URL
	case *tg.InlineButtonTypeCopy:
		b.Type = ButtonCopy
		b.CopyText = t.CopyText
	case *tg.InlineButtonTypeUserProfile:
		b.Type = ButtonUserProfile
		b.UserID = t.UserID
	case *tg.InputInlineButtonTypeUserProfile:
		b.Type = ButtonUserProfile
	default:
		b.Type = ButtonDefault
	}
	return b
}

// parseReplyButton converts a layer 229 keyboardButton, whose action lives in
// the nested ButtonType constructor.
func parseReplyButton(btn *tg.KeyboardButton) Button {
	b := Button{Text: btn.Text}
	switch t := btn.Type.(type) {
	case *tg.ButtonTypeRequestPhone:
		b.Type = ButtonRequestPhone
	case *tg.ButtonTypeRequestGeoLocation:
		b.Type = ButtonRequestGeo
	case *tg.ButtonTypeRequestPoll:
		b.Type = ButtonRequestPoll
		b.Quiz = t.Quiz
	case *tg.ButtonTypeRequestPeer:
		b.Type = ButtonRequestPeer
		b.ButtonID = t.ButtonID
	case *tg.InputButtonTypeRequestPeer:
		b.Type = ButtonRequestPeer
		b.ButtonID = t.ButtonID
	case *tg.ButtonTypeSimpleWebView:
		b.Type = ButtonWebView
		b.URL = t.URL
	default:
		b.Type = ButtonDefault
	}
	return b
}
