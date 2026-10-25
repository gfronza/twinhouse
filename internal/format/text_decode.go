package format

import (
	"bufio"
	"errors"
	"io"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
)

// delimitedReader yields rows of raw fields for TSV/CSV decoders.
type delimitedReader interface {
	readRow() ([]string, error) // io.EOF at end of input
}

// tsvReader splits TabSeparated input. Escape sequences are kept in the
// field (chtype.ParseText unescapes); a backslash protects the next byte,
// including a literal newline.
type tsvReader struct {
	r   *bufio.Reader
	raw bool
	buf []byte
}

func (t *tsvReader) readRow() ([]string, error) {
	var fields []string
	t.buf = t.buf[:0]
	started := false
	for {
		c, err := t.r.ReadByte()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			if !started {
				return nil, io.EOF
			}
			return append(fields, string(t.buf)), nil
		}
		started = true
		switch {
		case c == '\\' && !t.raw:
			n, err := t.r.ReadByte()
			if err != nil {
				return nil, cherr.New(cherr.CannotParseText, "unexpected end of TabSeparated input after backslash")
			}
			t.buf = append(t.buf, c, n)
		case c == '\t':
			fields = append(fields, string(t.buf))
			t.buf = t.buf[:0]
		case c == '\n':
			return append(fields, string(t.buf)), nil
		default:
			t.buf = append(t.buf, c)
		}
	}
}

// csvReader splits CSV input; quoted fields are returned unquoted and
// flagged so empty-vs-"" and \N-vs-"\N" can be told apart.
type csvReader struct {
	r      *bufio.Reader
	delim  byte
	buf    []byte
	quoted []bool
}

func (c *csvReader) readRow() ([]string, error) {
	var fields []string
	c.quoted = c.quoted[:0]
	if _, err := c.r.Peek(1); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	for {
		c.buf = c.buf[:0]
		ch, err := c.r.ReadByte()
		quoted := false
		if err == nil && ch == '"' {
			quoted = true
			for {
				ch, err = c.r.ReadByte()
				if err != nil {
					return nil, cherr.New(cherr.CannotParseText, "unexpected end of CSV input inside a quoted field")
				}
				if ch == '"' {
					next, err := c.r.ReadByte()
					if err == nil && next == '"' {
						c.buf = append(c.buf, '"')
						continue
					}
					ch = next
					if err != nil {
						ch = 0
					}
					// Allow spaces between the closing quote and the delimiter.
					for err == nil && (ch == ' ' || ch == '\t') && ch != c.delim {
						ch, err = c.r.ReadByte()
					}
					break
				}
				c.buf = append(c.buf, ch)
			}
		} else {
			for err == nil && ch != c.delim && ch != '\n' {
				c.buf = append(c.buf, ch)
				ch, err = c.r.ReadByte()
			}
			if n := len(c.buf); n > 0 && c.buf[n-1] == '\r' && (err != nil || ch == '\n') {
				c.buf = c.buf[:n-1]
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if quoted && err == nil && ch == '\r' {
			ch, err = c.r.ReadByte()
		}
		fields = append(fields, string(c.buf))
		c.quoted = append(c.quoted, quoted)
		switch {
		case err != nil: // EOF ends the row
			return fields, nil
		case ch == c.delim:
			continue
		case ch == '\n':
			return fields, nil
		default:
			return nil, cherr.New(cherr.CannotParseText, "unexpected character %q after a quoted CSV field", ch)
		}
	}
}

// delimitedDecoder is the shared TSV/CSV row → values logic.
type delimitedDecoder struct {
	rd      delimitedReader
	format  string
	cols    []Column
	s       inputSettings
	names   bool
	types   bool
	csv     bool
	raw     bool
	started bool
	row     int
	mapping []int         // input field -> target column (-1 skip)
	srcType []chtype.Type // input field type (header type or target type)
}

func newTSVDecoder(name string, r io.Reader, cols []Column, opts Options) *delimitedDecoder {
	raw := name == fTabSeparatedRaw || name == fTabSeparatedRawWithNames || name == fTabSeparatedRawWithNamesAndType
	d := &delimitedDecoder{
		rd:     &tsvReader{r: bufio.NewReaderSize(r, 64<<10), raw: raw},
		format: name,
		cols:   cols,
		s:      newInputSettings(opts),
		raw:    raw,
	}
	switch name {
	case fTabSeparatedWithNames, fTabSeparatedRawWithNames:
		d.names = true
	case fTabSeparatedWithNamesAndTypes, fTabSeparatedRawWithNamesAndType:
		d.names, d.types = true, true
	}
	return d
}

func newCSVDecoder(name string, r io.Reader, cols []Column, opts Options) (*delimitedDecoder, error) {
	delim, err := csvDelimiter(opts)
	if err != nil {
		return nil, err
	}
	return &delimitedDecoder{
		rd:     &csvReader{r: bufio.NewReaderSize(r, 64<<10), delim: delim},
		format: name,
		cols:   cols,
		s:      newInputSettings(opts),
		csv:    true,
		names:  name == fCSVWithNames || name == fCSVWithNamesAndTypes,
		types:  name == fCSVWithNamesAndTypes,
	}, nil
}

func (d *delimitedDecoder) readHeader() error {
	d.mapping = make([]int, len(d.cols))
	d.srcType = make([]chtype.Type, len(d.cols))
	for i, c := range d.cols {
		d.mapping[i], d.srcType[i] = i, c.Type
	}
	if !d.names {
		return nil
	}
	names, err := d.rd.readRow()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return err
	}
	d.row++
	for i, n := range names {
		names[i] = d.headerText(n)
	}
	var typeNames []string
	if d.types {
		if typeNames, err = d.rd.readRow(); err != nil {
			if errors.Is(err, io.EOF) {
				return cherr.New(cherr.CannotParseText, "%s input has a names header but no types header", d.format)
			}
			return err
		}
		d.row++
		if len(typeNames) != len(names) {
			return cherr.New(cherr.IncorrectData, "%s types header has %d fields, names header has %d", d.format, len(typeNames), len(names))
		}
	}
	if !d.s.useHeader {
		if len(names) != len(d.cols) {
			return cherr.New(cherr.IncorrectData, "%s header has %d fields, expected %d", d.format, len(names), len(d.cols))
		}
		return nil
	}
	if d.mapping, err = headerMap(names, d.cols, &d.s, d.format); err != nil {
		return err
	}
	d.srcType = make([]chtype.Type, len(names))
	for i, ti := range d.mapping {
		if ti < 0 {
			continue
		}
		d.srcType[i] = d.cols[ti].Type
		if d.types && d.s.useTypes {
			t, err := chtype.Parse(d.headerText(typeNames[i]))
			if err != nil {
				return cherr.Wrap(cherr.UnknownType, nil, "%s header type of column %s: %s", d.format, names[i], cherr.As(err).Msg)
			}
			if err := checkInputType(t); err != nil {
				return columnErr(err, names[i])
			}
			d.srcType[i] = t
		}
	}
	return nil
}

func (d *delimitedDecoder) headerText(s string) string {
	if d.csv || d.raw {
		return s
	}
	if v, err := chtype.ParseText(chtype.TString, s); err == nil {
		if str, ok := v.(string); ok {
			return str
		}
	}
	return s
}

func (d *delimitedDecoder) Next() ([]any, error) {
	if !d.started {
		d.started = true
		if err := d.readHeader(); err != nil {
			return nil, err
		}
	}
	fields, err := d.rd.readRow()
	if err != nil {
		return nil, err
	}
	d.row++
	if len(fields) != len(d.mapping) {
		return nil, rowErr(cherr.New(cherr.CannotParseText,
			"expected %d fields in %s row, got %d", len(d.mapping), d.format, len(fields)), d.row, "")
	}
	out := make([]any, len(d.cols))
	for i := range out {
		out[i] = Missing
	}
	var quoted []bool
	if cr, ok := d.rd.(*csvReader); ok {
		quoted = cr.quoted
	}
	for i, f := range fields {
		ti := d.mapping[i]
		if ti < 0 {
			continue
		}
		q := quoted != nil && quoted[i]
		v, err := d.value(f, q, d.srcType[i], d.cols[ti].Type)
		if err != nil {
			return nil, rowErr(err, d.row, d.cols[ti].Name)
		}
		out[ti] = v
	}
	return out, nil
}

// value converts one raw field into the target column's canonical value.
func (d *delimitedDecoder) value(f string, quoted bool, src, dst chtype.Type) (any, error) {
	nullRepr := d.s.tsvNull
	if d.csv {
		nullRepr = d.s.csvNull
	}
	if f == nullRepr && !quoted {
		return d.s.null(dst)
	}
	base := baseKind(src)
	var v any
	var err error
	if f != "" {
		if bv, ok, berr := d.s.bestEffortValue(src, f); ok {
			if berr != nil {
				return nil, berr
			}
			if !src.Equal(dst) {
				return chtype.Convert(bv, src, dst)
			}
			return bv, nil
		}
	}
	switch {
	case f == "" && d.csv && (!quoted || !isStringKind(base)) && d.s.csvEmptyAsDefault:
		return Missing, nil
	case f == "" && !d.csv && d.s.tsvEmptyAsDefault:
		return Missing, nil
	case f == "" && isNumericKind(base):
		// ClickHouse reads an empty field as 0 for numbers.
		v = chtype.Zero(unwrapAll(src))
	case (d.csv || d.raw) && isStringKind(base):
		v, err = chtype.Normalize(src, f)
	default:
		v, err = chtype.ParseText(src, f)
	}
	if err != nil {
		return nil, numberInputErr(err, base)
	}
	if !src.Equal(dst) {
		return chtype.Convert(v, src, dst)
	}
	return v, nil
}

// numberInputErr maps a malformed numeric field to ClickHouse's row-input
// code: its TSV/CSV readers parse the number prefix and then fail on the
// expected delimiter, CANNOT_PARSE_INPUT_ASSERTION_FAILED (27). Other codes
// (e.g. 72 for '-' in an unsigned field) are kept.
func numberInputErr(err error, base chtype.Kind) error {
	if !isNumericKind(base) {
		return err
	}
	ce := cherr.As(err)
	isFloat := base == chtype.Float32 || base == chtype.Float64
	if ce.Code == cherr.CannotParseText || isFloat && ce.Code == cherr.CannotParseNumber {
		return &cherr.Error{Code: cherr.CannotParseInputAssertionFailed, Msg: ce.Msg, Cause: ce.Cause}
	}
	return err
}

// unwrapAll strips Nullable / LowCardinality / SimpleAggregateFunction.
func unwrapAll(t chtype.Type) chtype.Type {
	for (t.Kind == chtype.Nullable || t.Kind == chtype.LowCardinality || t.Kind == chtype.SimpleAggregateFunction) && t.Elem != nil {
		t = *t.Elem
	}
	return t
}
