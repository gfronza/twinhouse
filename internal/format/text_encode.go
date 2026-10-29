package format

import (
	"bufio"
	"io"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

const outBufSize = 64 << 10

// nullText is how Pretty and the JSON*Strings formats render NULL.
const nullText = "ᴺᵁᴸᴸ"

// displayType returns the type used to render values of t as text: DateTime
// and DateTime64 without an explicit timezone are shown in the server
// timezone, as ClickHouse does.
func displayType(t chtype.Type, tz string) chtype.Type {
	if tz == "" {
		return t
	}
	switch t.Kind {
	case chtype.DateTime, chtype.DateTime64:
		if t.TZ == "" {
			t.TZ = tz
		}
	case chtype.Nullable, chtype.LowCardinality, chtype.Array, chtype.SimpleAggregateFunction, chtype.Map:
		if t.Elem != nil {
			e := displayType(*t.Elem, tz)
			t.Elem = &e
		}
		if t.Key != nil {
			k := displayType(*t.Key, tz)
			t.Key = &k
		}
	case chtype.Tuple:
		fs := make([]chtype.Field, len(t.Fields))
		for i, f := range t.Fields {
			fs[i] = chtype.Field{Name: f.Name, Type: displayType(f.Type, tz)}
		}
		t.Fields = fs
	}
	return t
}

func displayTypes(cols []Column, tz string) []chtype.Type {
	out := make([]chtype.Type, len(cols))
	for i, c := range cols {
		out[i] = displayType(c.Type, tz)
	}
	return out
}

// baseKind is t's kind with Nullable / LowCardinality / SimpleAggregateFunction stripped.
func baseKind(t chtype.Type) chtype.Kind { return unwrapAll(t).Kind }

// rawText renders a non-NULL value without escaping (TSVRaw, Pretty,
// JSON*Strings): strings and enum names verbatim, everything else in the
// TabSeparated text form.
func rawText(t chtype.Type, v any) string {
	switch baseKind(t) {
	case chtype.String, chtype.FixedString, chtype.Enum8, chtype.Enum16:
		if s, ok := v.(string); ok {
			return s
		}
	}
	return chtype.FormatText(t, v)
}

func checkRowWidth(rows [][]any, n int) error {
	for i, r := range rows {
		if len(r) != n {
			return cherr.New(cherr.LogicalError, "row %d has %d values, expected %d", i, len(r), n)
		}
	}
	return nil
}

// tsvEncoder implements the TabSeparated family.
type tsvEncoder struct {
	w           *bufio.Writer
	cols        []Column
	types       []chtype.Type
	raw         bool
	names       bool
	typesHeader bool
	eol         string
	null        string // format_tsv_null_representation
	headerDone  bool
}

func newTSVEncoder(name string, w io.Writer, cols []Column, opts Options) *tsvEncoder {
	e := &tsvEncoder{w: bufio.NewWriterSize(w, outBufSize), cols: cols, types: renderTypes(cols, opts), eol: "\n", null: tsvNullRepr(opts)}
	switch name {
	case fTabSeparatedRaw:
		e.raw = true
	case fTabSeparatedWithNames:
		e.names = true
	case fTabSeparatedWithNamesAndTypes:
		e.names, e.typesHeader = true, true
	case fTabSeparatedRawWithNames:
		e.raw, e.names = true, true
	case fTabSeparatedRawWithNamesAndType:
		e.raw, e.names, e.typesHeader = true, true, true
	}
	if settingBool(opts.Settings, "output_format_tsv_crlf_end_of_line", false) {
		e.eol = "\r\n"
	}
	return e
}

func (e *tsvEncoder) header() {
	if e.headerDone {
		return
	}
	e.headerDone = true
	if e.names {
		for i, c := range e.cols {
			if i > 0 {
				e.w.WriteByte('\t')
			}
			e.w.WriteString(e.headerField(c.Name))
		}
		e.w.WriteString(e.eol)
	}
	if e.typesHeader {
		for i, c := range e.cols {
			if i > 0 {
				e.w.WriteByte('\t')
			}
			e.w.WriteString(e.headerField(c.Type.String()))
		}
		e.w.WriteString(e.eol)
	}
}

func (e *tsvEncoder) headerField(s string) string {
	if e.raw {
		return s
	}
	return chtype.FormatText(chtype.TString, s)
}

func (e *tsvEncoder) WriteRows(rows [][]any) error {
	if err := checkRowWidth(rows, len(e.cols)); err != nil {
		return err
	}
	e.header()
	for _, r := range rows {
		for i, v := range r {
			if i > 0 {
				e.w.WriteByte('\t')
			}
			switch {
			case v == nil:
				e.w.WriteString(e.null)
			case e.raw:
				e.w.WriteString(rawText(e.types[i], v))
			default:
				e.w.WriteString(chtype.FormatText(e.types[i], v))
			}
		}
		if _, err := e.w.WriteString(e.eol); err != nil {
			return err
		}
	}
	return nil
}

func (e *tsvEncoder) Finish(Stats) error {
	e.header()
	return e.w.Flush()
}

func (e *tsvEncoder) Flush() error { return e.w.Flush() }

// csvEncoder implements the CSV family.
type csvEncoder struct {
	w          *bufio.Writer
	cols       []Column
	types      []chtype.Type
	delim      byte
	names      bool
	typesHdr   bool
	eol        string
	null       string // format_csv_null_representation
	headerDone bool
}

// tsvNullRepr / csvNullRepr return how NULL is written (and read) in the
// TabSeparated / CSV families: format_{tsv,csv}_null_representation,
// default \N. Only top-level fields (and the elements of a top-level CSV
// tuple, which CSV flattens) use it; NULL inside arrays stays NULL.
func tsvNullRepr(opts Options) string {
	if v, ok := opts.Settings["format_tsv_null_representation"]; ok {
		return v
	}
	return `\N`
}

func csvNullRepr(opts Options) string {
	if v, ok := opts.Settings["format_csv_null_representation"]; ok {
		return v
	}
	return `\N`
}

func csvDelimiter(opts Options) (byte, error) {
	d, ok := opts.Settings["format_csv_delimiter"]
	if !ok {
		return ',', nil
	}
	if len(d) != 1 {
		return 0, cherr.New(cherr.BadArguments, "A setting's value string has to be an exactly one character long")
	}
	return d[0], nil
}

func newCSVEncoder(name string, w io.Writer, cols []Column, opts Options) (*csvEncoder, error) {
	d, err := csvDelimiter(opts)
	if err != nil {
		return nil, err
	}
	e := &csvEncoder{w: bufio.NewWriterSize(w, outBufSize), cols: cols, types: renderTypes(cols, opts), delim: d, eol: "\n", null: csvNullRepr(opts)}
	e.names = name == fCSVWithNames || name == fCSVWithNamesAndTypes
	e.typesHdr = name == fCSVWithNamesAndTypes
	if settingBool(opts.Settings, "output_format_csv_crlf_end_of_line", false) {
		e.eol = "\r\n"
	}
	return e, nil
}

func (e *csvEncoder) header() {
	if e.headerDone {
		return
	}
	e.headerDone = true
	if e.names {
		for i, c := range e.cols {
			if i > 0 {
				e.w.WriteByte(e.delim)
			}
			e.w.WriteString(chtype.FormatCSV(chtype.TString, c.Name))
		}
		e.w.WriteString(e.eol)
	}
	if e.typesHdr {
		for i, c := range e.cols {
			if i > 0 {
				e.w.WriteByte(e.delim)
			}
			e.w.WriteString(chtype.FormatCSV(chtype.TString, c.Type.String()))
		}
		e.w.WriteString(e.eol)
	}
}

func (e *csvEncoder) WriteRows(rows [][]any) error {
	if err := checkRowWidth(rows, len(e.cols)); err != nil {
		return err
	}
	e.header()
	for _, r := range rows {
		for i, v := range r {
			if i > 0 {
				e.w.WriteByte(e.delim)
			}
			e.writeField(e.types[i], v)
		}
		if _, err := e.w.WriteString(e.eol); err != nil {
			return err
		}
	}
	return nil
}

// writeField writes one CSV field. A top-level Tuple is flattened into
// one field per element, separated by the CSV delimiter (as ClickHouse does).
func (e *csvEncoder) writeField(t chtype.Type, v any) {
	if v == nil {
		e.w.WriteString(e.null)
		return
	}
	if t.Kind == chtype.Tuple {
		if tup, ok := v.([]any); ok && len(tup) == len(t.Fields) {
			for i, f := range t.Fields {
				if i > 0 {
					e.w.WriteByte(e.delim)
				}
				e.writeField(f.Type, tup[i])
			}
			return
		}
	}
	e.w.WriteString(chtype.FormatCSV(t, v))
}

func (e *csvEncoder) Finish(Stats) error {
	e.header()
	return e.w.Flush()
}

func (e *csvEncoder) Flush() error { return e.w.Flush() }

// valuesEncoder implements Values: (v,...),(v,...) without a trailing newline.
type valuesEncoder struct {
	w     *bufio.Writer
	cols  []Column
	types []chtype.Type
	any   bool
}

func newValuesEncoder(w io.Writer, cols []Column, opts Options) *valuesEncoder {
	return &valuesEncoder{w: bufio.NewWriterSize(w, outBufSize), cols: cols, types: renderTypes(cols, opts)}
}

func (e *valuesEncoder) WriteRows(rows [][]any) error {
	if err := checkRowWidth(rows, len(e.cols)); err != nil {
		return err
	}
	for _, r := range rows {
		if e.any {
			e.w.WriteByte(',')
		}
		e.any = true
		e.w.WriteByte('(')
		for i, v := range r {
			if i > 0 {
				e.w.WriteByte(',')
			}
			if v == nil {
				e.w.WriteString("NULL")
			} else {
				e.w.WriteString(chtype.FormatQuoted(e.types[i], v))
			}
		}
		if err := e.w.WriteByte(')'); err != nil {
			return err
		}
	}
	return nil
}

func (e *valuesEncoder) Finish(Stats) error { return e.w.Flush() }

func (e *valuesEncoder) Flush() error { return e.w.Flush() }
