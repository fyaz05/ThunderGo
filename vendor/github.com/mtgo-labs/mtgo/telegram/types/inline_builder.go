package types

import (
	"fmt"
	"strings"

	"github.com/mtgo-labs/mtgo/telegram/parser"
	"github.com/mtgo-labs/mtgo/tg"
)

// InlineArticle builds an inline query result of type "article" for sending
// text content with an optional thumbnail.
//
// Text is parsed into entities according to ParseMode (or used verbatim when
// pre-built Entities are supplied); parsing errors abort the answer.
//
// Example:
//
//	article := &types.InlineArticle{
//	    ID: "1", Title: "Hello", Text: "<b>World</b>", ParseMode: params.ParseModeHTML,
//	}
//	result, err := article.TL()
type InlineArticle struct {
	ID          string
	Title       string
	Description string
	URL         string
	ThumbURL    string
	ThumbMIME   string
	ThumbWidth  int32
	ThumbHeight int32
	Text        string
	ParseMode   ParseMode
	// Entities overrides ParseMode parsing when non-empty.
	Entities    []tg.MessageEntityClass
	NoWebpage   bool
	InvertMedia bool
	ReplyMarkup tg.ReplyMarkupClass
}

func (a *InlineArticle) TL() (tg.InputBotInlineResultClass, error) {
	var flags tg.Fields
	if a.Title != "" {
		flags.Set(1)
	}
	if a.Description != "" {
		flags.Set(2)
	}
	if a.URL != "" {
		flags.Set(3)
	}
	thumb, err := inlineThumb(a.ThumbURL, a.ThumbMIME, a.ThumbWidth, a.ThumbHeight)
	if err != nil {
		return nil, err
	}
	if thumb != nil {
		flags.Set(4)
	}
	text, entities, err := inlineParseText(a.ParseMode, a.Text, a.Entities)
	if err != nil {
		return nil, fmt.Errorf("article %q: %w", a.ID, err)
	}
	return &tg.InputBotInlineResult{
		Flags:       flags,
		ID:          a.ID,
		Type:        "article",
		Title:       a.Title,
		Description: a.Description,
		URL:         a.URL,
		Thumb:       thumb,
		SendMessage: &tg.InputBotInlineMessageText{
			NoWebpage:   a.NoWebpage,
			InvertMedia: a.InvertMedia,
			Message:     text,
			Entities:    entities,
			ReplyMarkup: a.ReplyMarkup,
		},
	}, nil
}

// InlinePhoto builds an inline query result referencing an existing photo by ID.
//
// Example:
//
//	photo := &types.InlinePhoto{ID: "2", PhotoID: docID, AccessHash: hash, Text: "caption"}
//	result, err := photo.TL()
type InlinePhoto struct {
	ID          string
	Title       string
	Description string
	PhotoID     int64
	AccessHash  int64
	FileRef     []byte
	Text        string
	ParseMode   ParseMode
	// Entities overrides ParseMode parsing when non-empty.
	Entities    []tg.MessageEntityClass
	InvertMedia bool
	ReplyMarkup tg.ReplyMarkupClass
}

func (p *InlinePhoto) TL() (tg.InputBotInlineResultClass, error) {
	text, entities, err := inlineParseText(p.ParseMode, p.Text, p.Entities)
	if err != nil {
		return nil, fmt.Errorf("photo %q: %w", p.ID, err)
	}
	return &tg.InputBotInlineResultPhoto{
		ID:   p.ID,
		Type: "photo",
		Photo: &tg.InputPhoto{
			ID:            p.PhotoID,
			AccessHash:    p.AccessHash,
			FileReference: p.FileRef,
		},
		SendMessage: &tg.InputBotInlineMessageMediaAuto{
			InvertMedia: p.InvertMedia,
			Message:     text,
			Entities:    entities,
			ReplyMarkup: p.ReplyMarkup,
		},
	}, nil
}

// InlineDocument builds an inline query result referencing an existing document
// by ID. Type selects the presentation: "document" (default), "gif", "video",
// "audio", "voice", "sticker".
//
// Example:
//
//	doc := &types.InlineDocument{ID: "3", DocumentID: docID, AccessHash: hash, Type: "gif"}
//	result, err := doc.TL()
type InlineDocument struct {
	ID          string
	Title       string
	Description string
	Type        string
	DocumentID  int64
	AccessHash  int64
	FileRef     []byte
	Text        string
	ParseMode   ParseMode
	// Entities overrides ParseMode parsing when non-empty.
	Entities    []tg.MessageEntityClass
	InvertMedia bool
	ReplyMarkup tg.ReplyMarkupClass
}

func (d *InlineDocument) TL() (tg.InputBotInlineResultClass, error) {
	var flags tg.Fields
	if d.Title != "" {
		flags.Set(1)
	}
	if d.Description != "" {
		flags.Set(2)
	}
	resultType := d.Type
	if resultType == "" {
		resultType = "document"
	}
	text, entities, err := inlineParseText(d.ParseMode, d.Text, d.Entities)
	if err != nil {
		return nil, fmt.Errorf("document %q: %w", d.ID, err)
	}
	return &tg.InputBotInlineResultDocument{
		Flags:       flags,
		ID:          d.ID,
		Type:        resultType,
		Title:       d.Title,
		Description: d.Description,
		Document: &tg.InputDocument{
			ID:            d.DocumentID,
			AccessHash:    d.AccessHash,
			FileReference: d.FileRef,
		},
		SendMessage: &tg.InputBotInlineMessageMediaAuto{
			InvertMedia: d.InvertMedia,
			Message:     text,
			Entities:    entities,
			ReplyMarkup: d.ReplyMarkup,
		},
	}, nil
}

// InlineGame builds an inline query result for a game. Games cannot carry text
// content; only the optional reply markup applies.
//
// Example:
//
//	game := &types.InlineGame{ID: "4", ShortName: "mygame"}
//	result, err := game.TL()
type InlineGame struct {
	ID          string
	ShortName   string
	ReplyMarkup tg.ReplyMarkupClass
}

func (g *InlineGame) TL() (tg.InputBotInlineResultClass, error) {
	return &tg.InputBotInlineResultGame{
		ID:          g.ID,
		ShortName:   g.ShortName,
		SendMessage: &tg.InputBotInlineMessageGame{ReplyMarkup: g.ReplyMarkup},
	}, nil
}

// InlineLocation builds an inline query result that sends a geolocation
// (Bot API InlineQueryResultLocation).
//
// Example:
//
//	loc := &types.InlineLocation{ID: "5", Title: "HQ", Latitude: 55.75, Longitude: 37.61}
//	result, err := loc.TL()
type InlineLocation struct {
	ID          string
	Title       string
	Description string
	ThumbURL    string
	ThumbMIME   string
	Latitude    float64
	Longitude   float64
	// Heading is the live location bearing in degrees (1-360), 0 when unset.
	Heading int32
	// Period is the live location validity period in seconds, 0 for an
	// indefinite one-shot location.
	Period          int32
	ProximityRadius int32
	ReplyMarkup     tg.ReplyMarkupClass
}

func (l *InlineLocation) TL() (tg.InputBotInlineResultClass, error) {
	var flags tg.Fields
	if l.Title != "" {
		flags.Set(1)
	}
	if l.Description != "" {
		flags.Set(2)
	}
	thumb, err := inlineThumb(l.ThumbURL, l.ThumbMIME, 0, 0)
	if err != nil {
		return nil, err
	}
	if thumb != nil {
		flags.Set(4)
	}
	return &tg.InputBotInlineResult{
		Flags:       flags,
		ID:          l.ID,
		Type:        "geo",
		Title:       l.Title,
		Description: l.Description,
		Thumb:       thumb,
		SendMessage: &tg.InputBotInlineMessageMediaGeo{
			GeoPoint:                    &tg.InputGeoPoint{Lat: l.Latitude, Long: l.Longitude},
			Heading:                     l.Heading,
			Period:                      l.Period,
			ProximityNotificationRadius: l.ProximityRadius,
			ReplyMarkup:                 l.ReplyMarkup,
		},
	}, nil
}

// InlineVenue builds an inline query result that sends a venue (Bot API
// InlineQueryResultVenue).
//
// Example:
//
//	v := &types.InlineVenue{ID: "6", Title: "Coffee", Latitude: 55.75, Longitude: 37.61,
//	    Address: "Tverskaya 1", Provider: "foursquare", VenueID: "abc"}
//	result, err := v.TL()
type InlineVenue struct {
	ID          string
	Title       string
	Description string
	ThumbURL    string
	ThumbMIME   string
	Latitude    float64
	Longitude   float64
	Address     string
	Provider    string
	VenueID     string
	VenueType   string
	ReplyMarkup tg.ReplyMarkupClass
}

func (v *InlineVenue) TL() (tg.InputBotInlineResultClass, error) {
	var flags tg.Fields
	if v.Title != "" {
		flags.Set(1)
	}
	if v.Description != "" {
		flags.Set(2)
	}
	thumb, err := inlineThumb(v.ThumbURL, v.ThumbMIME, 0, 0)
	if err != nil {
		return nil, err
	}
	if thumb != nil {
		flags.Set(4)
	}
	return &tg.InputBotInlineResult{
		Flags:       flags,
		ID:          v.ID,
		Type:        "venue",
		Title:       v.Title,
		Description: v.Description,
		Thumb:       thumb,
		SendMessage: &tg.InputBotInlineMessageMediaVenue{
			GeoPoint:    &tg.InputGeoPoint{Lat: v.Latitude, Long: v.Longitude},
			Title:       v.Title,
			Address:     v.Address,
			Provider:    v.Provider,
			VenueID:     v.VenueID,
			VenueType:   v.VenueType,
			ReplyMarkup: v.ReplyMarkup,
		},
	}, nil
}

// InlineContact builds an inline query result that sends a contact (Bot API
// InlineQueryResultContact).
//
// Example:
//
//	c := &types.InlineContact{ID: "7", FirstName: "Ada", LastName: "L", PhoneNumber: "+15550100"}
//	result, err := c.TL()
type InlineContact struct {
	ID          string
	FirstName   string
	LastName    string
	PhoneNumber string
	VCard       string
	ThumbURL    string
	ThumbMIME   string
	ReplyMarkup tg.ReplyMarkupClass
}

func (ct *InlineContact) TL() (tg.InputBotInlineResultClass, error) {
	var flags tg.Fields
	title := strings.TrimSpace(ct.FirstName + " " + ct.LastName)
	if title != "" {
		flags.Set(1)
	}
	thumb, err := inlineThumb(ct.ThumbURL, ct.ThumbMIME, 0, 0)
	if err != nil {
		return nil, err
	}
	if thumb != nil {
		flags.Set(4)
	}
	return &tg.InputBotInlineResult{
		Flags: flags,
		ID:    ct.ID,
		Type:  "contact",
		Title: title,
		Thumb: thumb,
		SendMessage: &tg.InputBotInlineMessageMediaContact{
			PhoneNumber: ct.PhoneNumber,
			FirstName:   ct.FirstName,
			LastName:    ct.LastName,
			Vcard:       ct.VCard,
			ReplyMarkup: ct.ReplyMarkup,
		},
	}, nil
}

// InlineRich builds an inline query result that sends a rich message (Bot API
// 10.1 InputRichMessageContent in inline query results). The rich message can
// be built with tg.InputRichMessageHTML, tg.InputRichMessageMarkdown, or
// tg.InputRichMessage blocks.
//
// Example:
//
//	r := &types.InlineRich{
//	    ID: "8", Title: "Docs",
//	    RichMessage: &tg.InputRichMessageMarkdown{Markdown: "**hello**"},
//	}
//	result, err := r.TL()
type InlineRich struct {
	ID          string
	Title       string
	Description string
	ThumbURL    string
	ThumbMIME   string
	RichMessage tg.InputRichMessageClass
	ReplyMarkup tg.ReplyMarkupClass
}

func (r *InlineRich) TL() (tg.InputBotInlineResultClass, error) {
	if r.RichMessage == nil {
		return nil, fmt.Errorf("inline rich result %q: rich message is nil", r.ID)
	}
	var flags tg.Fields
	if r.Title != "" {
		flags.Set(1)
	}
	if r.Description != "" {
		flags.Set(2)
	}
	thumb, err := inlineThumb(r.ThumbURL, r.ThumbMIME, 0, 0)
	if err != nil {
		return nil, err
	}
	if thumb != nil {
		flags.Set(4)
	}
	return &tg.InputBotInlineResult{
		Flags:       flags,
		ID:          r.ID,
		Type:        "rich",
		Title:       r.Title,
		Description: r.Description,
		Thumb:       thumb,
		SendMessage: &tg.InputBotInlineMessageRichMessage{
			RichMessage: r.RichMessage,
			ReplyMarkup: r.ReplyMarkup,
		},
	}, nil
}

// InlineResultBuilder is the interface for types that can produce a TL inline
// result for answering inline queries. TL reports construction errors such as
// invalid markup in ParseMode text.
type InlineResultBuilder interface {
	TL() (tg.InputBotInlineResultClass, error)
}

// BuildInlineResults converts builders into TL inline results, stopping at the
// first construction error.
func BuildInlineResults(results []InlineResultBuilder) ([]tg.InputBotInlineResultClass, error) {
	out := make([]tg.InputBotInlineResultClass, len(results))
	for i, r := range results {
		if r == nil {
			return nil, fmt.Errorf("inline result %d: nil builder", i)
		}
		res, err := r.TL()
		if err != nil {
			return nil, fmt.Errorf("inline result %d: %w", i, err)
		}
		out[i] = res
	}
	return out, nil
}

func buildInlineResults(results []InlineResultBuilder) ([]tg.InputBotInlineResultClass, error) {
	return BuildInlineResults(results)
}

// inlineParseText applies ParseMode parsing to inline result text, returning
// the plain text plus entities. Pre-built entities short-circuit parsing.
func inlineParseText(pm ParseMode, text string, entities []tg.MessageEntityClass) (string, []tg.MessageEntityClass, error) {
	if len(entities) > 0 {
		return text, entities, nil
	}
	switch strings.ToLower(strings.TrimSpace(string(pm))) {
	case "html":
		return parser.Parse(parser.ParseModeHTML, text)
	case "markdownv2":
		return parser.Parse(parser.ParseModeMarkdown, text)
	case "markdown":
		return parser.Parse(parser.ParseModeLegacyMarkdown, text)
	case "", "default", "disabled":
		return text, nil, nil
	default:
		return "", nil, fmt.Errorf("unknown parse mode %q (valid: html, markdown, markdownv2, disabled)", pm)
	}
}

// inlineThumb builds an InputWebDocument thumbnail from a URL, defaulting the
// MIME type to image/jpeg.
func inlineThumb(url, mime string, w, h int32) (*tg.InputWebDocument, error) {
	if url == "" {
		return nil, nil
	}
	if mime == "" {
		mime = "image/jpeg"
	}
	doc := &tg.InputWebDocument{URL: url, MimeType: mime}
	if w != 0 && h != 0 {
		doc.Attributes = []tg.DocumentAttributeClass{
			&tg.DocumentAttributeImageSize{W: w, H: h},
		}
	}
	return doc, nil
}
