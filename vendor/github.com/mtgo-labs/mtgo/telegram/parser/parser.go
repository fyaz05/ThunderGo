// Package parser converts Telegram HTML and Markdown markup into plain text
// accompanied by MessageEntity slices suitable for the MTProto API.
//
// Two parsers are provided:
//
//   - HTMLParser  — handles <b>, <strong>, <i>, <em>, <u>, <ins>, <s>, <strike>,
//     <del>, <code>, <pre>, <pre><code class="language-…">, <a href="…">,
//     <tg-spoiler>, <spoiler>, <span class="tg-spoiler">, <blockquote
//     collapsed/expandable>, <tg-emoji emoji-id="…">, <emoji emoji-id="…">, and
//     <tg-time unix="…" format="…"> tags. Dangerous URL protocols (javascript:,
//     data:, vbscript:, file:) are rejected. mailto: links produce
//     MessageEntityEmail. Numerical HTML entities are decoded.
//   - MarkdownParser — implements the official Bot API "MarkdownV2 style":
//     *bold*, _italic_, __underline__, ~strikethrough~, ||spoiler||, `code`,
//     ```pre```, [text](url) links, ![emoji](tg://emoji?id=…) custom emoji,
//     ![time](tg://time?unix=…&format=…) date-time entities, > blockquote /
//     **> expandable blockquote, and backslash escape sequences.
//
// LegacyMarkdownParser implements the deprecated Bot API "Markdown style"
// (*bold*, _italic_, code, pre, and links only).
//
// All parsers normalise Unicode and validate entity boundaries against UTF-8
// rune positions required by the Telegram API.
package parser

import (
	"fmt"

	tl "github.com/mtgo-labs/mtgo/tg"
)

// ParseMode represents the text formatting mode used to parse message content
// into Telegram-compatible entities.
type ParseMode int

const (
	// ParseModeDefault performs no parsing and returns the raw text with no entities.
	ParseModeDefault ParseMode = iota
	// ParseModeHTML interprets the input as Telegram HTML markup.
	ParseModeHTML
	// ParseModeMarkdown interprets the input as Telegram MarkdownV2
	// formatting (the Bot API parse_mode "MarkdownV2": *bold*, _italic_,
	// __underline__, ~strikethrough~, ||spoiler||, and friends).
	ParseModeMarkdown
	// ParseModeLegacyMarkdown interprets the input as the legacy Bot API
	// "Markdown" style (*bold*, _italic_, code, pre, links only).
	ParseModeLegacyMarkdown
	// ParseModeDisabled skips all parsing, returning the text unchanged.
	ParseModeDisabled
)

var (
	htmlParser           HTMLParser
	markdownParser       MarkdownParser
	legacyMarkdownParser LegacyMarkdownParser
)

// Parse parses text according to the given ParseMode and returns the plain text
// alongside the resulting Telegram message entities.
// It returns an error if the mode is not recognized.
func Parse(mode ParseMode, text string) (string, []tl.MessageEntityClass, error) {
	switch mode {
	case ParseModeHTML:
		return htmlParser.Parse(text)
	case ParseModeMarkdown:
		return markdownParser.Parse(text)
	case ParseModeLegacyMarkdown:
		return legacyMarkdownParser.Parse(text)
	case ParseModeDisabled, ParseModeDefault:
		return text, nil, nil
	default:
		return "", nil, fmt.Errorf("unsupported parse mode: %d", mode)
	}
}
