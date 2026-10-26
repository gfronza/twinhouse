package format

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// jsonRowReader extracts one top-level JSON object or array at a time from
// a stream of rows that may be separated by whitespace or commas and may be
// wrapped in a single JSON array.
type jsonRowReader struct {
	r       *bufio.Reader
	open    byte // '{' for JSONEachRow, '[' for JSONCompactEachRow
	started bool
	wrapped bool
	done    bool
	buf     []byte
}

func (j *jsonRowReader) skip() (byte, error) {
	for {
		c, err := j.r.ReadByte()
		if err != nil {
			return 0, err
		}
		if isSpace(c) || c == ',' {
			continue
		}
		return c, nil
	}
}

// next returns the raw bytes of the next row value, or io.EOF.
func (j *jsonRowReader) next() ([]byte, error) {
	if j.done {
		return nil, io.EOF
	}
	c, err := j.skip()
	if err != nil {
		if errors.Is(err, io.EOF) {
			if j.wrapped {
				return nil, cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected ']' at end of stream")
			}
			j.done = true
			return nil, io.EOF
		}
		return nil, err
	}
	if !j.started {
		j.started = true
		// JSONEachRow rows may be wrapped in one JSON array.
		if c == '[' && j.open == '{' {
			j.wrapped = true
			return j.next()
		}
	}
	if j.wrapped && c == ']' {
		if c, err := j.skip(); err == nil {
			return nil, cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: %q after the JSON array of rows", c)
		}
		j.done = true
		return nil, io.EOF
	}
	if c != j.open {
		return nil, cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected '%c' at the start of a row, got %q", j.open, c)
	}
	return j.readBalanced(c)
}

func (j *jsonRowReader) readBalanced(first byte) ([]byte, error) {
	j.buf = append(j.buf[:0], first)
	depth := 1
	inStr := false
	for depth > 0 {
		c, err := j.r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, cherr.New(cherr.CannotReadAllData, "Unexpected end of stream inside a JSON row")
			}
			return nil, err
		}
		j.buf = append(j.buf, c)
		if inStr {
			switch c {
			case '\\':
				n, err := j.r.ReadByte()
				if err != nil {
					return nil, cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: unexpected end of input inside a JSON string")
				}
				j.buf = append(j.buf, n)
			case '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
		}
	}
	return j.buf, nil
}

func decodeJSON(raw []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: malformed JSON row: %v", err)
	}
	return nil
}

// jsonValue converts a decoded JSON value into column type t.
func (s *inputSettings) jsonValue(t chtype.Type, v any) (any, error) {
	if v == nil {
		return s.null(t)
	}
	if str, ok := v.(string); ok && str != "" {
		if bv, ok, err := s.bestEffortValue(t, str); ok {
			return bv, err
		}
	}
	return chtype.Normalize(t, s.jsonCoerce(t, v))
}

// jsonCoerce applies ClickHouse's lenient JSON reading rules that
// Normalize does not: numbers and bools read into String columns
// (input_format_json_read_numbers_as_strings) and bools into numeric
// columns (input_format_json_read_bools_as_numbers).
func (s *inputSettings) jsonCoerce(t chtype.Type, v any) any {
	base := unwrapAll(t)
	switch x := v.(type) {
	case json.Number:
		if isStringKind(base.Kind) && s.numbersAsStrings {
			return string(x)
		}
	case bool:
		if isNumericKind(base.Kind) && s.boolsAsNumbers {
			if x {
				return json.Number("1")
			}
			return json.Number("0")
		}
	case []any:
		if base.Kind == chtype.Array && base.Elem != nil {
			out := make([]any, len(x))
			for i, e := range x {
				if e == nil {
					out[i] = nil
				} else {
					out[i] = s.jsonCoerce(*base.Elem, e)
				}
			}
			return out
		}
	}
	return v
}

// jsonEachRowDecoder reads JSONEachRow: one object per row, keys mapped to
// columns by name.
type jsonEachRowDecoder struct {
	rr     jsonRowReader
	cols   []Column
	byName map[string]int
	s      inputSettings
	row    int
}

func newJSONEachRowDecoder(r io.Reader, cols []Column, opts Options) *jsonEachRowDecoder {
	d := &jsonEachRowDecoder{
		rr:     jsonRowReader{r: bufio.NewReaderSize(r, 64<<10), open: '{'},
		cols:   cols,
		byName: make(map[string]int, len(cols)),
		s:      newInputSettings(opts),
	}
	for i, c := range cols {
		d.byName[c.Name] = i
	}
	return d
}

func (d *jsonEachRowDecoder) Next() ([]any, error) {
	raw, err := d.rr.next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, rowErr(err, d.row+1, "")
	}
	d.row++
	var obj map[string]any
	if err := decodeJSON(raw, &obj); err != nil {
		return nil, rowErr(err, d.row, "")
	}
	out := make([]any, len(d.cols))
	for i := range out {
		out[i] = Missing
	}
	for k, v := range obj {
		i, ok := d.byName[k]
		if !ok {
			if d.s.skipUnknown {
				continue
			}
			return nil, rowErr(cherr.New(cherr.IncorrectData, "Unknown field found while parsing JSONEachRow format: %s", k), d.row, "")
		}
		val, err := d.s.jsonValue(d.cols[i].Type, v)
		if err != nil {
			return nil, rowErr(err, d.row, k)
		}
		out[i] = val
	}
	return out, nil
}

// jsonCompactEachRowDecoder reads JSONCompactEachRow: one array per row,
// values positional.
type jsonCompactEachRowDecoder struct {
	rr   jsonRowReader
	cols []Column
	s    inputSettings
	row  int
}

func newJSONCompactEachRowDecoder(r io.Reader, cols []Column, opts Options) *jsonCompactEachRowDecoder {
	return &jsonCompactEachRowDecoder{
		rr:   jsonRowReader{r: bufio.NewReaderSize(r, 64<<10), open: '['},
		cols: cols,
		s:    newInputSettings(opts),
	}
}

func (d *jsonCompactEachRowDecoder) Next() ([]any, error) {
	raw, err := d.rr.next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, rowErr(err, d.row+1, "")
	}
	d.row++
	var arr []any
	if err := decodeJSON(raw, &arr); err != nil {
		return nil, rowErr(err, d.row, "")
	}
	if len(arr) != len(d.cols) {
		return nil, rowErr(cherr.New(cherr.IncorrectData, "JSONCompactEachRow row has %d values, expected %d", len(arr), len(d.cols)), d.row, "")
	}
	out := make([]any, len(d.cols))
	for i, v := range arr {
		val, err := d.s.jsonValue(d.cols[i].Type, v)
		if err != nil {
			return nil, rowErr(err, d.row, d.cols[i].Name)
		}
		out[i] = val
	}
	return out, nil
}
