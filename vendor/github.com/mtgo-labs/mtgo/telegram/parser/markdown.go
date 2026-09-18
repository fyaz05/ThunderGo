package parser

import (
	"fmt"
	"regexp"
	"strings"

	tl "github.com/mtgo-labs/mtgo/tg"
)

// MarkdownParser parses Telegram MarkdownV2-formatted text into plain text
// and message entities by converting to HTML and delegating to HTMLParser.
//
// The implemented syntax follows the official Bot API "MarkdownV2 style"
// exactly: *bold*, _italic_, __underline__, ~strikethrough~, ||spoiler||,
// `code`, ```pre```, [link](url), ![emoji](tg://emoji?id=…),
// ![time](tg://time?unix=…&format=…), > blockquote, and **> expandable
// blockquote.
type MarkdownParser struct{}

// NewMarkdownParser returns a new MarkdownParser ready for use.
func NewMarkdownParser() *MarkdownParser {
	return &MarkdownParser{}
}

// Parse converts MarkdownV2-formatted text to HTML and delegates to HTMLParser.
// It returns the plain text and corresponding Telegram message entities.
func (p *MarkdownParser) Parse(md string) (string, []tl.MessageEntityClass, error) {
	html := mdToHTML(md, false)
	return htmlParser.Parse(html)
}

// LegacyMarkdownParser parses the legacy Bot API "Markdown style" (parse_mode
// Markdown): *bold*, _italic_, `code`, ```pre```, and [link](url). Underline,
// strikethrough, spoiler, blockquote, custom emoji, and date-time entities do
// not exist in this mode by specification.
type LegacyMarkdownParser struct{}

// NewLegacyMarkdownParser returns a new LegacyMarkdownParser ready for use.
func NewLegacyMarkdownParser() *LegacyMarkdownParser {
	return &LegacyMarkdownParser{}
}

// Parse converts legacy Markdown-formatted text to HTML and delegates to
// HTMLParser.
func (p *LegacyMarkdownParser) Parse(md string) (string, []tl.MessageEntityClass, error) {
	html := mdToHTML(md, true)
	return htmlParser.Parse(html)
}

// linkRe matches MarkdownV2 [text](url) links.
var linkRe = regexp.MustCompile(`\[([^\]]*)\]\(([^)]+)\)`)

// imageLinkRe matches MarkdownV2 ![alt](url) syntax used for custom emoji and
// date-time entities.
var imageLinkRe = regexp.MustCompile(`!\[([^\]]*)\]\(([^)]+)\)`)

// emojiLinkRe parses tg://emoji?id=… URLs inside image links.
var emojiLinkRe = regexp.MustCompile(`^tg://emoji\?id=(-?\d+)$`)

// timeLinkRe parses tg://time?unix=…&format=… URLs inside image links.
var timeLinkRe = regexp.MustCompile(`^tg://time\?unix=(\d+)(?:&format=([^&]*))?$`)

// mdToHTML converts MarkdownV2 (or legacy Markdown when legacy is true) to the
// HTML subset understood by HTMLParser.
func mdToHTML(md string, legacy bool) string {
	s := md

	// 1a. Pre-escape: handle \\ and \` before code extraction.
	// These two sequences affect code boundary detection — if
	// processed later, \` would be misread as a code delimiter and
	// \\ would prevent \` from being recognised as an escaped backtick.
	s = escapePre(s)

	// 2. Extract code spans (``` and `) and replace with numbered placeholders
	//    so that formatting delimiters inside code are not processed. Must run
	//    before blockquote processing so that > inside code blocks is literal.
	codeBlocks := make([]string, 0, 8)
	s = extractCodeBlocks(s, &codeBlocks)
	s = extractCodeSpans(s, &codeBlocks)

	// 1b. Post-escape: handle remaining escape sequences. These run after
	//     code extraction so that \* inside `code` is preserved literally.
	s = escapePost(s, legacy)

	if !legacy {
		// 3. Blockquotes — **>, >>> and > at line start. Must run after code
		//    extraction (so > inside code is protected) and before inline
		//    formatting (so that the ** of an expandable quote is not
		//    consumed as a bold delimiter).
		s = processBlockquotes(s)

		// 4. Image links ![alt](url) — custom emoji and date-time entities.
		//    Run before plain links and formatting delimiters.
		s = replaceImageLinks(s)
	}

	// 5. Links [text](url) — run before formatting delimiters so that * inside
	//    [text] sections is not prematurely consumed.
	s = replaceLinks(s)

	if legacy {
		// Legacy Markdown style: *bold*, _italic_ only. Doubled delimiters
		// are not entities in this mode — keep them literal instead of
		// producing empty delimiter pairs.
		s = strings.ReplaceAll(s, "__", "\uE020")
		s = strings.ReplaceAll(s, "**", "\uE021")
		s = replaceDelimited(s, "*", "<b>", "</b>")
		s = replaceDelimited(s, "_", "<i>", "</i>")
		s = strings.ReplaceAll(s, "\uE020", "__")
		s = strings.ReplaceAll(s, "\uE021", "**")
	} else {
		// 6. Formatting delimiters per the official MarkdownV2 style. Longer
		//    delimiters first (__ before _) so that underline wins over italic.
		s = replaceDelimited(s, "__", "<u>", "</u>")             // underline
		s = replaceDelimited(s, "||", "<spoiler>", "</spoiler>") // spoiler
		s = replaceDelimited(s, "*", "<b>", "</b>")              // bold
		s = replaceDelimited(s, "_", "<i>", "</i>")              // italic
		s = replaceDelimited(s, "~", "<s>", "</s>")              // strikethrough
	}

	// 7. Restore code spans from placeholders.
	s = restoreCodeBlocks(s, codeBlocks)

	// 8. Restore escaped characters, HTML-escaping <, >, and & so the
	//    HTML parser treats them as literal text. Handles both pre-escape
	//    and post-escape placeholders.
	s = unescapeAll(s)

	return s
}

// codeBlockPlaceholderFmt is the format string for code block placeholders.
const codeBlockPlaceholderFmt = "\uE100%04d\uE100"

// codeBlockPlaceholderRe matches code block placeholders.
var codeBlockPlaceholderRe = regexp.MustCompile("\uE100(\\d{4})\uE100")

// extractCodeBlocks replaces fenced code blocks (```) with placeholders
// and stores the original content (including delimiters converted to HTML).
func extractCodeBlocks(s string, blocks *[]string) string {
	return replaceDelimitedWithCollect(s, "```", func(content string) string {
		idx := len(*blocks)
		// Extract language hint from the first line if present.
		lang := ""
		rest := content
		if nl := strings.IndexByte(content, '\n'); nl >= 0 {
			firstLine := strings.TrimSpace(content[:nl])
			// A language hint is a single word without spaces.
			if !strings.ContainsAny(firstLine, " \t") && firstLine != "" {
				lang = firstLine
				rest = content[nl+1:]
			}
		}
		if lang != "" {
			*blocks = append(*blocks, "<pre language=\""+lang+"\">"+rest+"</pre>")
		} else {
			*blocks = append(*blocks, "<pre>"+rest+"</pre>")
		}
		return fmt.Sprintf(codeBlockPlaceholderFmt, idx)
	})
}

// extractCodeSpans replaces inline code (`) with placeholders.
func extractCodeSpans(s string, blocks *[]string) string {
	return replaceDelimitedWithCollect(s, "`", func(content string) string {
		idx := len(*blocks)
		*blocks = append(*blocks, "<code>"+content+"</code>")
		return fmt.Sprintf(codeBlockPlaceholderFmt, idx)
	})
}

// restoreCodeBlocks replaces code block placeholders with their stored HTML.
func restoreCodeBlocks(s string, blocks []string) string {
	return codeBlockPlaceholderRe.ReplaceAllStringFunc(s, func(match string) string {
		parts := codeBlockPlaceholderRe.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		var idx int
		fmt.Sscanf(parts[1], "%04d", &idx)
		if idx < 0 || idx >= len(blocks) {
			return match
		}
		return blocks[idx]
	})
}

// replaceDelimitedWithCollect is like replaceDelimited but collects the
// content between delimiter pairs and passes each to the collect function,
// replacing the entire delimited span with the function's return value.
func replaceDelimitedWithCollect(s, delim string, collect func(content string) string) string {
	var b strings.Builder
	b.Grow(len(s))
	for {
		idx := strings.Index(s, delim)
		if idx == -1 {
			b.WriteString(s)
			break
		}
		b.WriteString(s[:idx])
		s = s[idx+len(delim):]

		// Find the closing delimiter.
		end := strings.Index(s, delim)
		if end == -1 {
			// No closing delimiter — treat the rest as literal text.
			b.WriteString(delim)
			b.WriteString(s)
			break
		}
		content := s[:end]
		b.WriteString(collect(content))
		s = s[end+len(delim):]
	}
	return b.String()
}

// replaceLinks converts MarkdownV2 [text](url) syntax to HTML <a> tags.
// Characters that could break HTML parsing (" and >) are percent-encoded.
func replaceLinks(s string) string {
	return linkRe.ReplaceAllStringFunc(s, func(match string) string {
		parts := linkRe.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		text := parts[1]
		url := parts[2]
		// Prevent URL content from breaking the HTML attribute or tag
		// boundary. Per RFC 3986 these characters should already be
		// percent-encoded in valid URLs, but defensive escaping costs
		// nothing and prevents malformed-output edge cases.
		url = strings.ReplaceAll(url, `"`, `%22`)
		url = strings.ReplaceAll(url, `>`, `%3E`)
		return `<a href="` + url + `">` + text + `</a>`
	})
}

// replaceImageLinks converts the MarkdownV2 image-link syntax
// ![alt](tg://emoji?id=…) and ![alt](tg://time?unix=…&format=…) into the
// equivalent HTML tags. Other image URLs have no Bot API meaning and are left
// as literal text.
func replaceImageLinks(s string) string {
	return imageLinkRe.ReplaceAllStringFunc(s, func(match string) string {
		parts := imageLinkRe.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		alt, url := parts[1], parts[2]
		if m := emojiLinkRe.FindStringSubmatch(url); m != nil {
			return `<tg-emoji emoji-id="` + m[1] + `">` + alt + `</tg-emoji>`
		}
		if m := timeLinkRe.FindStringSubmatch(url); m != nil {
			if m[2] != "" {
				return `<tg-time unix="` + m[1] + `" format="` + m[2] + `">` + alt + `</tg-time>`
			}
			return `<tg-time unix="` + m[1] + `">` + alt + `</tg-time>`
		}
		return match
	})
}

// processBlockquotes converts MarkdownV2 line-prefix blockquote syntax to
// HTML <blockquote> tags. Consecutive quoted lines merge into a single
// blockquote entity, matching the documented multi-line examples:
//
//	>Block quotation started
//	>Block quotation continued
//
// **> at line start begins an expandable blockquote (the official syntax —
// the empty bold entity is the separator that keeps an expandable quote from
// merging into a preceding plain quote); subsequent > lines continue it.
// >>> is accepted as an mtgo extension for the same effect.
//
// Must run after code extraction so that > inside code spans is already
// placeholder-protected.
func processBlockquotes(s string) string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	var b strings.Builder
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		content, kind := quoteLine(line)
		if kind == quoteNone {
			out = append(out, line)
			continue
		}
		expandable := kind == quoteExpandable
		b.Reset()
		b.WriteString(content)
		// Plain runs continue only while plain-quote lines follow; an
		// expandable run absorbs any following quoted lines.
		for i+1 < len(lines) {
			next, nextKind := quoteLine(lines[i+1])
			if nextKind == quoteNone || (!expandable && nextKind == quoteExpandable) {
				break
			}
			b.WriteByte('\n')
			b.WriteString(next)
			i++
		}
		if expandable {
			out = append(out, "<blockquote expandable>"+b.String()+"</blockquote>")
		} else {
			out = append(out, "<blockquote>"+b.String()+"</blockquote>")
		}
	}
	return strings.Join(out, "\n")
}

type quoteKind int

const (
	quoteNone quoteKind = iota
	quotePlain
	quoteExpandable
)

// quoteLine classifies a blockquote line and returns its content with the
// prefix (and one optional following space) stripped.
func quoteLine(line string) (string, quoteKind) {
	if trimmed, ok := strings.CutPrefix(line, "**>"); ok {
		return strings.TrimPrefix(trimmed, " "), quoteExpandable
	}
	if trimmed, ok := strings.CutPrefix(line, ">>>"); ok {
		return strings.TrimPrefix(trimmed, " "), quoteExpandable
	}
	if trimmed, ok := strings.CutPrefix(line, ">"); ok {
		return strings.TrimPrefix(trimmed, " "), quotePlain
	}
	return line, quoteNone
}

// replaceDelimited performs a blind state-machine replacement: it alternates
// between open and close tags on each occurrence of delim in s. This is
// correct for non-nested delimiters; nested formatting is handled by the
// HTML-to-entity conversion layer (HTMLParser).
func replaceDelimited(s, delim, openTag, closeTag string) string {
	var b strings.Builder
	b.Grow(len(s))
	open := true
	for {
		idx := strings.Index(s, delim)
		if idx == -1 {
			b.WriteString(s)
			break
		}
		b.WriteString(s[:idx])
		if open {
			b.WriteString(openTag)
		} else {
			b.WriteString(closeTag)
		}
		s = s[idx+len(delim):]
		open = !open
	}
	return b.String()
}

// Escape sequences. Each backslash-character sequence is replaced with a
// Unicode private-use placeholder during the initial pass, then restored to
// its literal character after all formatting.
//
// Split into two groups:
//   - preEscapes: \\ and \` — processed before code extraction because
//     they affect code boundary detection.
//   - postEscapes: the remaining sequences — processed after code
//     extraction so that escapes inside code spans are preserved literally.
//
// Within each group \\ is ordered first so that the backslash in \\X is
// not consumed by the \X handler.
var preEscapes = []struct {
	escaped     string
	placeholder string
	literal     string
}{
	{`\\`, "\uE000", `\`},
	{"\\`", "\uE003", "`"},
}

// postEscapes covers every MarkdownV2 special character that must be escapable
// outside entities: '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+',
// '-', '=', '|', '{', '}', '.', '!'.
var postEscapes = []struct {
	escaped     string
	placeholder string
	literal     string
}{
	{`\*`, "\uE001", `*`},
	{`\_`, "\uE002", `_`},
	{`\~`, "\uE004", `~`},
	{`\|`, "\uE005", `|`},
	{`\[`, "\uE006", `[`},
	{`\]`, "\uE007", `]`},
	{`\(`, "\uE008", `(`},
	{`\)`, "\uE009", `)`},
	{`\{`, "\uE00A", `{`},
	{`\}`, "\uE00B", `}`},
	{`\<`, "\uE00C", `&lt;`},
	{`\>`, "\uE00D", `&gt;`},
	{`\!`, "\uE00E", `!`},
	{`\.`, "\uE00F", `.`},
	{`\-`, "\uE010", `-`},
	{`\=`, "\uE011", `=`},
	{`\+`, "\uE012", `+`},
	{`\#`, "\uE013", `#`},
}

// legacyPostEscapes covers the four characters escapable in the legacy
// Markdown style: '_', '*', '`' (handled pre-extraction), and '['.
var legacyPostEscapes = []struct {
	escaped     string
	placeholder string
	literal     string
}{
	{`\*`, "\uE001", `*`},
	{`\_`, "\uE002", `_`},
	{`\[`, "\uE006", `[`},
}

// allEscapes is the concatenation of preEscapes and postEscapes (all variants),
// used by unescapeAll to restore all placeholders in a single pass.
var allEscapes []struct {
	placeholder string
	literal     string
}

func init() {
	allEscapes = make([]struct {
		placeholder string
		literal     string
	}, len(preEscapes)+len(postEscapes)+len(legacyPostEscapes))
	i := 0
	for _, p := range preEscapes {
		allEscapes[i] = struct {
			placeholder string
			literal     string
		}{p.placeholder, p.literal}
		i++
	}
	for _, p := range postEscapes {
		allEscapes[i] = struct {
			placeholder string
			literal     string
		}{p.placeholder, p.literal}
		i++
	}
	for _, p := range legacyPostEscapes {
		allEscapes[i] = struct {
			placeholder string
			literal     string
		}{p.placeholder, p.literal}
		i++
	}
}

func escapePre(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	for _, p := range preEscapes {
		s = strings.ReplaceAll(s, p.escaped, p.placeholder)
	}
	return s
}

func escapePost(s string, legacy bool) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	escapes := postEscapes
	if legacy {
		escapes = legacyPostEscapes
	}
	for _, p := range escapes {
		s = strings.ReplaceAll(s, p.escaped, p.placeholder)
	}
	return s
}

func unescapeAll(s string) string {
	// Fast path: all placeholders share the UTF-8 0xEE 0x80 prefix.
	if !strings.Contains(s, "\xEE\x80") {
		return s
	}
	for _, p := range allEscapes {
		s = strings.ReplaceAll(s, p.placeholder, p.literal)
	}
	return s
}

// parseDateTimeFormat validates and converts a Bot API date-time format string
// (regex r|w?[dD]?[tT]?) into the messageEntityFormattedDate flag fields.
// Returns ok=false for malformed format strings.
func parseDateTimeFormat(format string) (relative, shortTime, longTime, shortDate, longDate, dayOfWeek bool, ok bool) {
	if format == "" {
		return false, false, false, false, false, false, true
	}
	if strings.EqualFold(format, "r") {
		return true, false, false, false, false, false, true
	}
	rest := format
	if len(rest) > 0 && (rest[0] == 'w' || rest[0] == 'W') {
		dayOfWeek = true
		rest = rest[1:]
	}
	if len(rest) > 0 && (rest[0] == 'd' || rest[0] == 'D') {
		longDate = rest[0] == 'D'
		shortDate = rest[0] == 'd'
		rest = rest[1:]
	}
	if len(rest) > 0 && (rest[0] == 't' || rest[0] == 'T') {
		longTime = rest[0] == 'T'
		shortTime = rest[0] == 't'
		rest = rest[1:]
	}
	if rest != "" {
		return false, false, false, false, false, false, false
	}
	return relative, shortTime, longTime, shortDate, longDate, dayOfWeek, true
}

// unixTimeRE validates the tg-time unix attribute.
var unixTimeRE = regexp.MustCompile(`^\d+$`)
