package format

import (
	"bufio"
	"errors"
	"io"
	"strings"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
)

// valuesDecoder reads the Values format: (v, ...), (v, ...)[;] where each
// v is a quoted literal, NULL or DEFAULT. Tuples may span lines.
type valuesDecoder struct {
	r    *bufio.Reader
	cols []Column
	s    inputSettings
	row  int
	buf  []byte
	done bool
}

func newValuesDecoder(r io.Reader, cols []Column, opts Options) *valuesDecoder {
	return &valuesDecoder{r: bufio.NewReaderSize(r, 64<<10), cols: cols, s: newInputSettings(opts)}
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// skipSeparators skips whitespace and the commas between rows; it returns
// the next significant byte without consuming it.
func (d *valuesDecoder) skipSeparators(allowComma bool) (byte, error) {
	for {
		c, err := d.r.ReadByte()
		if err != nil {
			return 0, err
		}
		if isSpace(c) || (allowComma && c == ',') {
			continue
		}
		return c, d.r.UnreadByte()
	}
}

// readTuple reads one parenthesised row, honouring quotes and nesting.
func (d *valuesDecoder) readTuple() (string, error) {
	d.buf = d.buf[:0]
	depth := 0
	var quote byte
	for {
		c, err := d.r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", cherr.New(cherr.CannotParseText, "unexpected end of Values input inside a row")
			}
			return "", err
		}
		d.buf = append(d.buf, c)
		if quote != 0 {
			switch c {
			case '\\':
				n, err := d.r.ReadByte()
				if err != nil {
					return "", cherr.New(cherr.CannotParseText, "unexpected end of Values input inside a string")
				}
				d.buf = append(d.buf, n)
			case quote:
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
			if depth == 0 {
				return string(d.buf), nil
			}
		}
	}
}

func (d *valuesDecoder) Next() ([]any, error) {
	if d.done {
		return nil, io.EOF
	}
	c, err := d.skipSeparators(d.row > 0)
	if err != nil {
		if errors.Is(err, io.EOF) {
			d.done = true
			return nil, io.EOF
		}
		return nil, err
	}
	if c == ';' {
		d.r.ReadByte()
		if c, err := d.skipSeparators(false); err == nil {
			return nil, cherr.New(cherr.CannotParseText, "unexpected data %q after ';' in Values input", c)
		}
		d.done = true
		return nil, io.EOF
	}
	if c != '(' {
		return nil, rowErr(cherr.New(cherr.CannotParseText, "expected '(' at the start of a Values row, got %q", c), d.row+1, "")
	}
	tuple, err := d.readTuple()
	if err != nil {
		return nil, rowErr(err, d.row+1, "")
	}
	d.row++
	return d.parseTuple(tuple)
}

func (d *valuesDecoder) parseTuple(s string) ([]any, error) {
	out := make([]any, len(d.cols))
	pos := 1 // after '('
	for i, col := range d.cols {
		pos = skipSpaces(s, pos)
		v, n, err := d.parseValue(col.Type, s[pos:])
		if err != nil {
			return nil, rowErr(err, d.row, col.Name)
		}
		out[i] = v
		pos = skipSpaces(s, pos+n)
		want := byte(',')
		if i == len(d.cols)-1 {
			want = ')'
		}
		if pos >= len(s) || s[pos] != want {
			if pos < len(s) && s[pos] == ',' && want == ')' {
				return nil, rowErr(cherr.New(cherr.CannotParseText, "too many values in Values row, expected %d", len(d.cols)), d.row, "")
			}
			if pos < len(s) && s[pos] == ')' {
				return nil, rowErr(cherr.New(cherr.CannotParseText, "too few values in Values row, expected %d", len(d.cols)), d.row, "")
			}
			return nil, rowErr(cherr.New(cherr.CannotParseText, "expected '%c' after value in Values row", want), d.row, col.Name)
		}
		pos++
	}
	if len(d.cols) == 0 && (skipSpaces(s, pos) >= len(s) || s[skipSpaces(s, pos)] != ')') {
		return nil, rowErr(cherr.New(cherr.CannotParseText, "expected empty Values row"), d.row, "")
	}
	return out, nil
}

func skipSpaces(s string, pos int) int {
	for pos < len(s) && isSpace(s[pos]) {
		pos++
	}
	return pos
}

// keywordAt reports whether s starts with the case-insensitive keyword kw
// followed by a non-identifier byte.
func keywordAt(s, kw string) bool {
	if len(s) < len(kw) || !strings.EqualFold(s[:len(kw)], kw) {
		return false
	}
	if len(s) == len(kw) {
		return true
	}
	c := s[len(kw)]
	return !(c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z')
}

func (d *valuesDecoder) parseValue(t chtype.Type, s string) (any, int, error) {
	switch {
	case keywordAt(s, "NULL"):
		v, err := d.s.null(t)
		return v, 4, err
	case keywordAt(s, "DEFAULT"):
		return Missing, 7, nil
	}
	if strings.HasPrefix(s, "'") && d.s.dtIn != dtInBasic {
		if _, isTime := bestEffortTarget(t); isTime {
			if sv, n, err := chtype.ParseQuoted(chtype.TString, s); err == nil {
				if str, _ := sv.(string); str != "" {
					if bv, ok, err := d.s.bestEffortValue(t, str); ok {
						return bv, n, err
					}
				}
			}
		}
	}
	v, n, err := chtype.ParseQuoted(t, s)
	if err != nil {
		if looksLikeExpression(s) {
			return nil, 0, cherr.New(cherr.NotImplemented, "expressions in Values input are not supported: %s", truncate(s, 40))
		}
		return nil, 0, err
	}
	return v, n, nil
}

// looksLikeExpression detects a function call or arithmetic, which
// ClickHouse would evaluate (input_format_values_interpret_expressions).
func looksLikeExpression(s string) bool {
	i := 0
	for i < len(s) && (s[i] == '_' || s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z' || i > 0 && s[i] >= '0' && s[i] <= '9') {
		i++
	}
	if i > 0 {
		j := skipSpaces(s, i)
		return j < len(s) && s[j] == '('
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
