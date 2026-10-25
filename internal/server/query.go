package server

import (
	"strings"
)

// Lightweight lexical helpers used before the query reaches the engine:
// classifying a statement by its first keyword (readonly checks, INSERT
// detection) and locating where inline INSERT data starts in a large body,
// so the data can be streamed instead of being parsed as SQL.

// lexer walks SQL text, skipping whitespace and comments the way the
// ClickHouse lexer does (-- and # comments to end of line, /* */ blocks).
type lexer struct {
	s   string
	pos int
	// truncated is set when the text ended inside a comment, string or
	// quoted identifier.
	truncated bool
}

func (l *lexer) eof() bool { return l.pos >= len(l.s) }

func (l *lexer) skipSpace() {
	for l.pos < len(l.s) {
		c := l.s[l.pos]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			l.pos++
		case c == '-' && l.peek(1) == '-', c == '#' && (l.peek(1) == ' ' || l.peek(1) == '!'):
			nl := strings.IndexByte(l.s[l.pos:], '\n')
			if nl < 0 {
				l.pos = len(l.s)
				return
			}
			l.pos += nl + 1
		case c == '/' && l.peek(1) == '*':
			end := strings.Index(l.s[l.pos+2:], "*/")
			if end < 0 {
				l.pos, l.truncated = len(l.s), true
				return
			}
			l.pos += 2 + end + 2
		default:
			return
		}
	}
}

func (l *lexer) peek(off int) byte {
	if l.pos+off < len(l.s) {
		return l.s[l.pos+off]
	}
	return 0
}

func isWordByte(c byte) bool {
	return c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 0x80
}

// word consumes an unquoted identifier/keyword/number at pos.
func (l *lexer) word() string {
	start := l.pos
	for l.pos < len(l.s) && isWordByte(l.s[l.pos]) {
		l.pos++
	}
	return l.s[start:l.pos]
}

// quoted consumes a quoted token ('string', `ident` or "ident"), honouring
// backslash escapes and doubled quotes.
func (l *lexer) quoted() {
	q := l.s[l.pos]
	l.pos++
	for l.pos < len(l.s) {
		c := l.s[l.pos]
		switch {
		case c == '\\':
			l.pos += 2
		case c == q && l.peek(1) == q:
			l.pos += 2
		case c == q:
			l.pos++
			return
		default:
			l.pos++
		}
	}
	l.pos, l.truncated = len(l.s), true
}

// leadingKeyword returns the upper-cased first keyword of a statement,
// skipping comments and opening parentheses ("(SELECT 1)" -> "SELECT").
// It returns "" for an empty query.
func leadingKeyword(q string) string {
	l := lexer{s: q}
	for {
		l.skipSpace()
		if l.eof() || l.s[l.pos] != '(' {
			break
		}
		l.pos++
	}
	if l.eof() || !isWordByte(l.s[l.pos]) {
		return ""
	}
	return strings.ToUpper(l.word())
}

// isEmptyQuery reports whether q contains only whitespace and comments.
func isEmptyQuery(q string) bool {
	l := lexer{s: q}
	l.skipSpace()
	return l.eof()
}

// readonlyKeywords are statements allowed with readonly >= 1 (and therefore
// over HTTP GET). SET is allowed only with readonly = 2.
var readonlyKeywords = map[string]bool{
	"SELECT": true, "WITH": true, "SHOW": true, "DESCRIBE": true, "DESC": true,
	"EXISTS": true, "EXPLAIN": true, "USE": true, "SET": true,
}

// outputKeywords are statements that produce a result set (and may carry a
// FORMAT clause that selects the output format).
var outputKeywords = map[string]bool{
	"SELECT": true, "WITH": true, "SHOW": true, "DESCRIBE": true, "DESC": true,
	"EXISTS": true, "EXPLAIN": true,
}

// insertDataSplit locates inline data in the text of an INSERT statement.
// It returns the end of the SQL part (just after `FORMAT <name>` or after
// `VALUES`), whether the data follows a FORMAT clause (and therefore needs
// ClickHouse's leading-whitespace trimming), and ok=false when no inline-data
// boundary was found within text (INSERT ... SELECT, or text truncated).
func insertDataSplit(text string) (queryEnd int, isFormat bool, ok bool) {
	l := lexer{s: text}
	l.skipSpace()
	if l.eof() || !strings.EqualFold(l.word(), "INSERT") {
		return 0, false, false
	}
	depth := 0
	prev := "INSERT"
	for {
		l.skipSpace()
		if l.eof() || l.truncated {
			return 0, false, false
		}
		c := l.s[l.pos]
		switch {
		case c == '(':
			depth++
			l.pos++
			prev = "("
		case c == ')':
			depth--
			l.pos++
			prev = ")"
		case c == '\'' || c == '"' || c == '`':
			l.quoted()
			if l.truncated {
				return 0, false, false
			}
			prev = "q"
		case isWordByte(c):
			w := strings.ToUpper(l.word())
			if depth == 0 && prev != "INTO" && prev != "TABLE" && prev != "FUNCTION" && prev != "." {
				switch w {
				case "SELECT", "WITH":
					return 0, false, false
				case "VALUES":
					return l.pos, false, true
				case "FORMAT":
					l.skipSpace()
					if l.eof() || !isWordByte(l.s[l.pos]) {
						return 0, false, false
					}
					l.word()
					if l.eof() {
						// The name may continue past the end of text.
						return 0, false, false
					}
					return l.pos, true, true
				}
			}
			prev = w
		default:
			l.pos++
			prev = string(c)
		}
	}
}
