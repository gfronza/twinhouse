package format

import (
	"bufio"
	"io"
	"math"
	"strconv"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// jsonStyle holds the settings that shape JSON value rendering.
type jsonStyle struct {
	quote64        bool // output_format_json_quote_64bit_integers
	quote64Floats  bool // output_format_json_quote_64bit_floats
	quoteDenormals bool // output_format_json_quote_denormals
	namedTuples    bool // output_format_json_named_tuples_as_objects
	escapeSlash    bool // output_format_json_escape_forward_slashes
	strings        bool // JSON*Strings: every value rendered as a string
}

func newJSONStyle(opts Options, strings bool) jsonStyle {
	s := opts.Settings
	return jsonStyle{
		quote64:        settingBool(s, "output_format_json_quote_64bit_integers", false),
		quote64Floats:  settingBool(s, "output_format_json_quote_64bit_floats", false),
		quoteDenormals: settingBool(s, "output_format_json_quote_denormals", false),
		namedTuples:    settingBool(s, "output_format_json_named_tuples_as_objects", true),
		escapeSlash:    settingBool(s, "output_format_json_escape_forward_slashes", true),
		strings:        strings,
	}
}

const hexUpper = "0123456789ABCDEF"

// appendJSONString appends s as a JSON string literal the way ClickHouse
// writes it: control characters as \b \f \n \r \t or \u00XX (upper-case
// hex), U+2028/U+2029 escaped, '/' escaped unless disabled, invalid UTF-8
// passed through unchanged.
func (st *jsonStyle) appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b = append(b, '\\', '"')
		case '\\':
			b = append(b, '\\', '\\')
		case '/':
			if st.escapeSlash {
				b = append(b, '\\', '/')
			} else {
				b = append(b, '/')
			}
		case '\b':
			b = append(b, '\\', 'b')
		case '\f':
			b = append(b, '\\', 'f')
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		case 0xE2:
			if i+2 < len(s) && s[i+1] == 0x80 && (s[i+2] == 0xA8 || s[i+2] == 0xA9) {
				if s[i+2] == 0xA8 {
					b = append(b, ` `...)
				} else {
					b = append(b, ` `...)
				}
				i += 2
			} else {
				b = append(b, c)
			}
		default:
			if c < 0x20 {
				b = append(b, '\\', 'u', '0', '0', hexUpper[c>>4], hexUpper[c&0xF])
			} else {
				b = append(b, c)
			}
		}
	}
	return append(b, '"')
}

func appendIndent(b []byte, depth int) []byte {
	for range depth {
		b = append(b, '\t')
	}
	return b
}

// appendValue renders v of type t. With pretty set, non-empty arrays,
// tuples and maps are spread over lines indented by depth+1 tabs, matching
// ClickHouse's JSON format with output_format_json_pretty_print=1.
func (st *jsonStyle) appendValue(b []byte, t chtype.Type, v any, depth int, pretty bool) []byte {
	if v == nil {
		if st.strings {
			return st.appendJSONString(b, nullText)
		}
		return append(b, "null"...)
	}
	if st.strings {
		return st.appendJSONString(b, rawText(t, v))
	}
	t = unwrapAll(t)
	switch t.Kind {
	case chtype.Nothing:
		return append(b, "null"...)
	case chtype.Bool:
		if x, _ := v.(bool); x {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64:
		x, _ := v.(uint64)
		if st.quote64 && t.Kind == chtype.UInt64 {
			b = append(b, '"')
			b = strconv.AppendUint(b, x, 10)
			return append(b, '"')
		}
		return strconv.AppendUint(b, x, 10)
	case chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		x, _ := v.(int64)
		if st.quote64 && t.Kind == chtype.Int64 {
			b = append(b, '"')
			b = strconv.AppendInt(b, x, 10)
			return append(b, '"')
		}
		return strconv.AppendInt(b, x, 10)
	case chtype.Float32, chtype.Float64:
		x, _ := v.(float64)
		if math.IsNaN(x) || math.IsInf(x, 0) {
			if !st.quoteDenormals {
				return append(b, "null"...)
			}
			b = append(b, '"')
			if math.IsNaN(x) && math.Signbit(x) {
				// ClickHouse's JSON writer keeps the NaN sign bit
				// (text formats print every NaN as nan).
				b = append(b, '-')
			}
			b = append(b, chtype.FormatText(t, v)...)
			return append(b, '"')
		}
		if st.quote64Floats && t.Kind == chtype.Float64 {
			b = append(b, '"')
			b = append(b, chtype.FormatText(t, v)...)
			return append(b, '"')
		}
		return append(b, chtype.FormatText(t, v)...)
	case chtype.String, chtype.FixedString, chtype.Enum8, chtype.Enum16:
		s, _ := v.(string)
		return st.appendJSONString(b, s)
	case chtype.Array:
		arr, _ := v.([]any)
		return st.appendList(b, '[', ']', len(arr), depth, pretty, func(b []byte, i int) []byte {
			return st.appendValue(b, *t.Elem, arr[i], depth+1, pretty)
		})
	case chtype.Tuple:
		tup, _ := v.([]any)
		n := min(len(tup), len(t.Fields))
		if st.namedTuples && len(t.Fields) > 0 && t.Fields[0].Name != "" {
			return st.appendList(b, '{', '}', n, depth, pretty, func(b []byte, i int) []byte {
				b = st.appendJSONString(b, t.Fields[i].Name)
				b = appendColon(b, pretty)
				return st.appendValue(b, t.Fields[i].Type, tup[i], depth+1, pretty)
			})
		}
		return st.appendList(b, '[', ']', n, depth, pretty, func(b []byte, i int) []byte {
			return st.appendValue(b, t.Fields[i].Type, tup[i], depth+1, pretty)
		})
	case chtype.Map:
		pairs, _ := v.([]any)
		return st.appendList(b, '{', '}', len(pairs), depth, pretty, func(b []byte, i int) []byte {
			kv, _ := pairs[i].([]any)
			if len(kv) != 2 {
				return append(b, `"":null`...)
			}
			b = st.appendJSONString(b, mapKeyText(*t.Key, kv[0]))
			b = appendColon(b, pretty)
			return st.appendValue(b, *t.Elem, kv[1], depth+1, pretty)
		})
	}
	// UUID, Date, Date32, DateTime, DateTime64.
	return st.appendJSONString(b, chtype.FormatText(t, v))
}

func appendColon(b []byte, pretty bool) []byte {
	if pretty {
		return append(b, ':', ' ')
	}
	return append(b, ':')
}

func (st *jsonStyle) appendList(b []byte, open, close byte, n, depth int, pretty bool, elem func([]byte, int) []byte) []byte {
	b = append(b, open)
	if n == 0 {
		return append(b, close)
	}
	for i := range n {
		if i > 0 {
			b = append(b, ',')
		}
		if pretty {
			b = append(b, '\n')
			b = appendIndent(b, depth+1)
		}
		b = elem(b, i)
	}
	if pretty {
		b = append(b, '\n')
		b = appendIndent(b, depth)
	}
	return append(b, close)
}

// mapKeyText renders a Map key as the JSON object key text.
func mapKeyText(t chtype.Type, k any) string {
	if k == nil {
		return "null"
	}
	return rawText(t, k)
}

// jsonEncoder implements JSON, JSONCompact, JSONStrings and JSONCompactStrings.
type jsonEncoder struct {
	w          *bufio.Writer
	cols       []Column
	types      []chtype.Type
	st         jsonStyle
	compact    bool
	pretty     bool
	stats      bool
	rows       uint64
	headerDone bool
	buf        []byte
}

func newJSONEncoder(name string, w io.Writer, cols []Column, opts Options) *jsonEncoder {
	strs := name == fJSONStrings || name == fJSONCompactStrings
	return &jsonEncoder{
		w:       bufio.NewWriterSize(w, outBufSize),
		cols:    cols,
		types:   renderTypes(cols, opts),
		st:      newJSONStyle(opts, strs),
		compact: name == fJSONCompact || name == fJSONCompactStrings,
		pretty:  settingBool(opts.Settings, "output_format_json_pretty_print", true),
		stats:   settingBool(opts.Settings, "output_format_write_statistics", true),
	}
}

func (e *jsonEncoder) header() {
	if e.headerDone {
		return
	}
	e.headerDone = true
	b := append(e.buf[:0], "{\n\t\"meta\":\n\t[\n"...)
	for i, c := range e.cols {
		if i > 0 {
			b = append(b, ",\n"...)
		}
		b = append(b, "\t\t{\n\t\t\t\"name\": "...)
		b = e.st.appendJSONString(b, c.Name)
		b = append(b, ",\n\t\t\t\"type\": "...)
		b = e.st.appendJSONString(b, c.Type.String())
		b = append(b, "\n\t\t}"...)
	}
	b = append(b, "\n\t],\n\n\t\"data\":\n\t[\n"...)
	e.w.Write(b)
	e.buf = b
}

func (e *jsonEncoder) WriteRows(rows [][]any) error {
	if err := checkRowWidth(rows, len(e.cols)); err != nil {
		return err
	}
	e.header()
	for _, r := range rows {
		b := e.buf[:0]
		if e.rows > 0 {
			b = append(b, ",\n"...)
		}
		e.rows++
		if e.compact {
			b = append(b, "\t\t["...)
			for i, v := range r {
				if i > 0 {
					b = append(b, ", "...)
				}
				b = e.st.appendValue(b, e.types[i], v, 2, false)
			}
			b = append(b, ']')
		} else {
			b = append(b, "\t\t{"...)
			for i, v := range r {
				if i > 0 {
					b = append(b, ',')
				}
				b = append(b, "\n\t\t\t"...)
				b = e.st.appendJSONString(b, e.cols[i].Name)
				b = append(b, ": "...)
				b = e.st.appendValue(b, e.types[i], v, 3, e.pretty)
			}
			b = append(b, "\n\t\t}"...)
		}
		e.buf = b
		if _, err := e.w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (e *jsonEncoder) Finish(s Stats) error {
	e.header()
	b := append(e.buf[:0], "\n\t],\n\n\t\"rows\": "...)
	b = strconv.AppendUint(b, e.rows, 10)
	if s.HasRowsBeforeLimit {
		b = append(b, ",\n\n\t\"rows_before_limit_at_least\": "...)
		b = strconv.AppendUint(b, s.RowsBeforeLimit, 10)
	}
	if e.stats {
		b = append(b, ",\n\n\t\"statistics\":\n\t{\n\t\t\"elapsed\": "...)
		b = strconv.AppendFloat(b, s.Elapsed.Seconds(), 'f', -1, 64)
		b = append(b, ",\n\t\t\"rows_read\": "...)
		b = strconv.AppendUint(b, s.RowsRead, 10)
		b = append(b, ",\n\t\t\"bytes_read\": "...)
		b = strconv.AppendUint(b, s.BytesRead, 10)
		b = append(b, "\n\t}"...)
	}
	b = append(b, "\n}\n"...)
	e.buf = b
	if _, err := e.w.Write(b); err != nil {
		return err
	}
	return e.w.Flush()
}

func (e *jsonEncoder) Flush() error { return e.w.Flush() }

// WriteException closes the data written so far and adds the exception:
// ClickHouse leaves out rows_before_limit_at_least and statistics then.
func (e *jsonEncoder) WriteException(text string) error {
	e.header()
	b := append(e.buf[:0], "\n\t],\n\n\t\"rows\": "...)
	b = strconv.AppendUint(b, e.rows, 10)
	b = append(b, ",\n\n\t\"exception\": "...)
	b = e.st.appendJSONString(b, text)
	b = append(b, "\n}\n"...)
	e.buf = b
	if _, err := e.w.Write(b); err != nil {
		return err
	}
	return e.w.Flush()
}

// jsonEachRowEncoder implements JSONEachRow, JSONCompactEachRow and their
// Strings variants (one JSON value per line).
type jsonEachRowEncoder struct {
	w          *bufio.Writer
	cols       []Column
	types      []chtype.Type
	st         jsonStyle
	compact    bool
	arrayRows  bool // output_format_json_array_of_rows (JSONEachRow only)
	rows       uint64
	headerDone bool
	buf        []byte
}

func newJSONEachRowEncoder(name string, w io.Writer, cols []Column, opts Options) *jsonEachRowEncoder {
	strs := name == fJSONStringsEachRow || name == fJSONCompactStringsEachRow
	compact := name == fJSONCompactEachRow || name == fJSONCompactStringsEachRow
	return &jsonEachRowEncoder{
		w:         bufio.NewWriterSize(w, outBufSize),
		cols:      cols,
		types:     renderTypes(cols, opts),
		st:        newJSONStyle(opts, strs),
		compact:   compact,
		arrayRows: name == fJSONEachRow && settingBool(opts.Settings, "output_format_json_array_of_rows", false),
	}
}

func (e *jsonEachRowEncoder) header() {
	if e.headerDone {
		return
	}
	e.headerDone = true
	if e.arrayRows {
		e.w.WriteString("[\n")
	}
}

func (e *jsonEachRowEncoder) WriteRows(rows [][]any) error {
	if err := checkRowWidth(rows, len(e.cols)); err != nil {
		return err
	}
	e.header()
	for _, r := range rows {
		b := e.buf[:0]
		if e.arrayRows && e.rows > 0 {
			b = append(b, ",\n"...)
		}
		e.rows++
		if e.compact {
			b = append(b, '[')
			for i, v := range r {
				if i > 0 {
					b = append(b, ", "...)
				}
				b = e.st.appendValue(b, e.types[i], v, 0, false)
			}
			b = append(b, ']')
		} else {
			b = append(b, '{')
			for i, v := range r {
				if i > 0 {
					b = append(b, ',')
				}
				b = e.st.appendJSONString(b, e.cols[i].Name)
				b = append(b, ':')
				b = e.st.appendValue(b, e.types[i], v, 0, false)
			}
			b = append(b, '}')
		}
		if !e.arrayRows {
			b = append(b, '\n')
		}
		e.buf = b
		if _, err := e.w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (e *jsonEachRowEncoder) Finish(Stats) error {
	e.header()
	if e.arrayRows {
		e.w.WriteString("\n]\n")
	}
	return e.w.Flush()
}

func (e *jsonEachRowEncoder) Flush() error { return e.w.Flush() }

// WriteException adds the exception as one more value: an object
// {"exception": ...} or, for the compact formats, an array of it.
func (e *jsonEachRowEncoder) WriteException(text string) error {
	e.header()
	b := e.buf[:0]
	if e.arrayRows && e.rows > 0 {
		b = append(b, ",\n"...)
	}
	if e.compact {
		b = append(b, '[')
		b = e.st.appendJSONString(b, text)
		b = append(b, ']')
	} else {
		b = append(b, `{"exception": `...)
		b = e.st.appendJSONString(b, text)
		b = append(b, '}')
	}
	b = append(b, '\n')
	if e.arrayRows {
		b = append(b, "]\n"...)
	}
	e.buf = b
	if _, err := e.w.Write(b); err != nil {
		return err
	}
	return e.w.Flush()
}
