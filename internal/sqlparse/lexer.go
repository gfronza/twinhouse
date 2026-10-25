package sqlparse

import (
	"strings"
	"unicode/utf8"
)

type tokKind uint8

const (
	tEOF    tokKind = iota
	tIdent          // bare word (identifiers and keywords)
	tQIdent         // `quoted` or "quoted" identifier; val holds the unquoted name
	tNumber         // numeric literal, text as written
	tString         // 'string' literal; val holds the decoded value
	tOp             // punctuation and operators
	tError          // lexical error; val holds the message
)

type token struct {
	kind tokKind
	text string // exact source text
	val  string // decoded value (tString, tQIdent) or error message (tError)
	pos  int    // byte offset of the first byte
	end  int    // byte offset just past the last byte
	// identParam is the parameter name when the parser turned a
	// {name:Identifier} substitution into this tQIdent token; unbound is
	// set when no value is available for it.
	identParam string
	unbound    bool
}

// lexer produces tokens lazily so that INSERT ... FORMAT data is never
// tokenized. Lexical errors are returned as tError tokens and only become
// errors when the parser actually looks at them.
type lexer struct {
	src  string
	off  int
	prev token
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentChar(c byte) bool { return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$' }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isHexDigit(c byte) bool {
	return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// skipSpace skips whitespace and comments. It returns a non-empty message if
// a block comment is not closed.
func (l *lexer) skipSpace() string {
	s := l.src
	for l.off < len(s) {
		c := s[l.off]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			l.off++
		case c == '-' && l.off+1 < len(s) && s[l.off+1] == '-',
			c == '#' && l.off+1 < len(s) && (s[l.off+1] == ' ' || s[l.off+1] == '!'):
			for l.off < len(s) && s[l.off] != '\n' {
				l.off++
			}
		case c == '/' && l.off+1 < len(s) && s[l.off+1] == '*':
			depth := 0
			i := l.off
			for i < len(s) {
				if i+1 < len(s) && s[i] == '/' && s[i+1] == '*' {
					depth++
					i += 2
				} else if i+1 < len(s) && s[i] == '*' && s[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
			if depth != 0 {
				return "Multiline comment is not closed"
			}
			l.off = i
		default:
			return ""
		}
	}
	return ""
}

// dotIsOperator reports whether a '.' right after the previous token is a
// member-access dot (t.col, tup.1) rather than the start of a number (.5).
func (l *lexer) dotIsOperator() bool {
	p := l.prev
	switch p.kind {
	case tQIdent:
		return true
	case tIdent:
		return !dotNumberAfterKeyword[strings.ToUpper(p.text)]
	case tOp:
		return p.text == ")" || p.text == "]" || p.text == "}"
	}
	return false
}

// Keywords after which ".5" is a number rather than a member access.
var dotNumberAfterKeyword = map[string]bool{
	"SELECT": true, "WHERE": true, "PREWHERE": true, "HAVING": true, "AND": true, "OR": true,
	"NOT": true, "THEN": true, "ELSE": true, "WHEN": true, "BY": true, "IN": true, "LIKE": true,
	"ILIKE": true, "BETWEEN": true, "ON": true, "LIMIT": true, "OFFSET": true, "INTERVAL": true,
	"VALUES": true, "DISTINCT": true, "CASE": true, "IS": true, "DIV": true, "MOD": true,
}

var multiCharOps = []string{"<=>", "::", "->", "||", "==", "!=", "<>", "<=", ">="}

func (l *lexer) next() token {
	if msg := l.skipSpace(); msg != "" {
		t := token{kind: tError, text: l.src[l.off:], val: msg, pos: l.off, end: len(l.src)}
		l.off = len(l.src)
		return t
	}
	t := l.scan()
	l.prev = t
	return t
}

func (l *lexer) scan() token {
	s, start := l.src, l.off
	if start >= len(s) {
		return token{kind: tEOF, pos: len(s), end: len(s)}
	}
	c := s[start]
	mk := func(kind tokKind, end int) token {
		l.off = end
		return token{kind: kind, text: s[start:end], pos: start, end: end}
	}
	switch {
	case isDigit(c) || (c == '.' && start+1 < len(s) && isDigit(s[start+1]) && !l.dotIsOperator()):
		return l.scanNumber()
	case isIdentStart(c):
		i := start + 1
		for i < len(s) && isIdentChar(s[i]) {
			i++
		}
		return mk(tIdent, i)
	case c == '\'':
		return l.scanQuoted('\'', tString, "Single quoted string is not closed")
	case c == '`':
		return l.scanQuoted('`', tQIdent, "Back quoted string is not closed")
	case c == '"':
		return l.scanQuoted('"', tQIdent, "Double quoted string is not closed")
	}
	for _, op := range multiCharOps {
		if strings.HasPrefix(s[start:], op) {
			return mk(tOp, start+len(op))
		}
	}
	if strings.IndexByte("()[]{},;.*+-/%=<>?:@^", c) >= 0 {
		return mk(tOp, start+1)
	}
	_, size := utf8.DecodeRuneInString(s[start:])
	t := mk(tError, start+size)
	t.val = "Unrecognized token"
	return t
}

func (l *lexer) scanNumber() token {
	s, start := l.src, l.off
	i := start
	bad := func(end int) token {
		for end < len(s) && isIdentChar(s[end]) {
			end++
		}
		l.off = end
		return token{kind: tError, text: s[start:end], val: "Wrong number", pos: start, end: end}
	}
	if s[i] == '0' && i+2 < len(s) && (s[i+1] == 'x' || s[i+1] == 'X') && isHexDigit(s[i+2]) {
		i += 2
		for i < len(s) && (isHexDigit(s[i]) || s[i] == '_') {
			i++
		}
	} else if s[i] == '0' && i+2 < len(s) && (s[i+1] == 'b' || s[i+1] == 'B') && (s[i+2] == '0' || s[i+2] == '1') {
		i += 2
		for i < len(s) && (s[i] == '0' || s[i] == '1' || s[i] == '_') {
			i++
		}
	} else {
		for i < len(s) && (isDigit(s[i]) || (s[i] == '_' && i > start)) {
			i++
		}
		if i < len(s) && s[i] == '.' {
			i++
			for i < len(s) && isDigit(s[i]) {
				i++
			}
		}
		if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
			j := i + 1
			if j < len(s) && (s[j] == '+' || s[j] == '-') {
				j++
			}
			if j < len(s) && isDigit(s[j]) {
				for j < len(s) && isDigit(s[j]) {
					j++
				}
				i = j
			}
		}
	}
	if i < len(s) && isIdentChar(s[i]) {
		return bad(i)
	}
	l.off = i
	return token{kind: tNumber, text: s[start:i], pos: start, end: i}
}

// scanQuoted scans a quote-delimited token. The quote character is escaped
// by doubling it or with a backslash.
func (l *lexer) scanQuoted(q byte, kind tokKind, unclosed string) token {
	s, start := l.src, l.off
	i := start + 1
	for i < len(s) {
		switch s[i] {
		case '\\':
			i += 2
			continue
		case q:
			if i+1 < len(s) && s[i+1] == q {
				i += 2
				continue
			}
			l.off = i + 1
			t := token{kind: kind, text: s[start : i+1], pos: start, end: i + 1}
			v, msg := unescape(s[start+1:i], q)
			if msg != "" {
				t.kind, t.val = tError, msg
			} else {
				t.val = v
			}
			return t
		}
		i++
	}
	l.off = len(s)
	return token{kind: tError, text: s[start:], val: unclosed, pos: start, end: len(s)}
}

// unescape decodes the body of a quoted token using ClickHouse's escape
// rules. Unknown escapes keep the backslash (ClickHouse behaviour).
func unescape(body string, q byte) (string, string) {
	if strings.IndexByte(body, '\\') < 0 && strings.IndexByte(body, q) < 0 {
		return body, ""
	}
	var b strings.Builder
	b.Grow(len(body))
	for i := 0; i < len(body); i++ {
		c := body[i]
		if c == q { // doubled quote
			b.WriteByte(q)
			i++
			continue
		}
		if c != '\\' || i+1 >= len(body) {
			b.WriteByte(c)
			continue
		}
		i++
		switch e := body[i]; e {
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case '0':
			b.WriteByte(0)
		case 'a':
			b.WriteByte('\a')
		case 'v':
			b.WriteByte('\v')
		case 'e':
			b.WriteByte(0x1b)
		case 'N':
			// \N is the NULL marker in text formats; inside a literal it is dropped.
		case 'x', 'X':
			if i+2 >= len(body) || !isHexDigit(body[i+1]) || !isHexDigit(body[i+2]) {
				return "", "Incomplete escape sequence"
			}
			b.WriteByte(hexVal(body[i+1])<<4 | hexVal(body[i+2]))
			i += 2
		case 'u':
			if i+4 >= len(body) || !isHexDigit(body[i+1]) || !isHexDigit(body[i+2]) || !isHexDigit(body[i+3]) || !isHexDigit(body[i+4]) {
				b.WriteString("\\u")
				continue
			}
			r := rune(hexVal(body[i+1]))<<12 | rune(hexVal(body[i+2]))<<8 | rune(hexVal(body[i+3]))<<4 | rune(hexVal(body[i+4]))
			b.WriteRune(r)
			i += 4
		case '\\', '\'', '"', '`', '/', '=':
			b.WriteByte(e)
		default:
			b.WriteByte('\\')
			b.WriteByte(e)
		}
	}
	return b.String(), ""
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	default:
		return c - 'A' + 10
	}
}
