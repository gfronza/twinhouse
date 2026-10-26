package chtype

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// CheckFormattable returns a NOT_IMPLEMENTED error if values of t cannot be
// rendered by the text formatters (AggregateFunction states have a binary
// representation in ClickHouse that this server does not reproduce).
// Formatters cannot fail, so output formats should call this per column first.
func CheckFormattable(t Type) error {
	switch t.Kind {
	case AggregateFunction:
		return errf(cherr.NotImplemented, "Serialization of %s in text formats is not supported; use finalizeAggregation or -Merge", t.String())
	case Nullable, LowCardinality, Array, SimpleAggregateFunction:
		if t.Elem != nil {
			return CheckFormattable(*t.Elem)
		}
	case Map:
		if err := CheckFormattable(elemOrNothing(t.Key)); err != nil {
			return err
		}
		return CheckFormattable(elemOrNothing(t.Elem))
	case Tuple:
		for _, f := range t.Fields {
			if err := CheckFormattable(f.Type); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeEscaped writes s with ClickHouse's escaping for escaped/quoted text:
// \b \f \n \r \t \0 \\ and the quote character get a backslash, all other
// bytes (including invalid UTF-8) are written as is.
func writeEscaped(b *strings.Builder, s string, quote byte) {
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		var esc byte
		switch c {
		case '\b':
			esc = 'b'
		case '\f':
			esc = 'f'
		case '\n':
			esc = 'n'
		case '\r':
			esc = 'r'
		case '\t':
			esc = 't'
		case 0:
			esc = '0'
		case '\\':
			esc = '\\'
		default:
			if c != quote || quote == 0 {
				continue
			}
			esc = c
		}
		b.WriteString(s[start:i])
		b.WriteByte('\\')
		b.WriteByte(esc)
		start = i + 1
	}
	b.WriteString(s[start:])
}

// formatText implements FormatText (TabSeparated escaped form).
func formatText(t Type, v any) string {
	var b strings.Builder
	writeText(&b, t, v)
	return b.String()
}

func writeText(b *strings.Builder, t Type, v any) {
	switch t.Kind {
	case Nullable:
		if v == nil {
			b.WriteString(`\N`)
			return
		}
		writeText(b, *t.Elem, v)
	case LowCardinality, SimpleAggregateFunction:
		writeText(b, *t.Elem, v)
	case String, FixedString, Enum8, Enum16:
		writeEscaped(b, toStr(v), '\'')
	case Array, Tuple, Map:
		writeQuoted(b, t, v)
	case Nothing:
		if v == nil {
			b.WriteString(`\N`)
		}
	default:
		if v == nil {
			b.WriteString(`\N`)
			return
		}
		writeScalarPlain(b, t, v)
	}
}

// writeScalarPlain writes the unquoted, unescaped text of a scalar that
// never needs escaping (numbers, bool, UUID, dates).
func writeScalarPlain(b *strings.Builder, t Type, v any) {
	var buf [64]byte
	b.Write(appendScalarPlain(buf[:0], t, v))
}

func appendScalarPlain(dst []byte, t Type, v any) []byte {
	switch t.Kind {
	case Bool:
		if asBool(v) {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case UInt8, UInt16, UInt32, UInt64:
		return appendUintValue(dst, v)
	case Int8, Int16, Int32, Int64:
		return appendIntValue(dst, v)
	case Float32:
		return appendFloat(dst, asFloat(v), 32)
	case Float64:
		return appendFloat(dst, asFloat(v), 64)
	case UUID, String, FixedString, Enum8, Enum16:
		return append(dst, toStr(v)...)
	case Date, Date32:
		return asTime(v).UTC().AppendFormat(dst, "2006-01-02")
	case DateTime:
		return asTime(v).In(locationOf(t)).AppendFormat(dst, "2006-01-02 15:04:05")
	case DateTime64:
		return appendDateTime64(dst, asTime(v), t)
	case AggregateFunction:
		return dst
	}
	return fmt.Append(dst, v)
}

func appendDateTime64(dst []byte, tm time.Time, t Type) []byte {
	lt := tm.In(locationOf(t))
	dst = lt.AppendFormat(dst, "2006-01-02 15:04:05")
	if t.Precision > 0 {
		frac := int64(lt.Nanosecond()) / pow10[9-t.Precision]
		dst = append(dst, '.')
		s := strconv.FormatInt(frac, 10)
		for i := len(s); i < t.Precision; i++ {
			dst = append(dst, '0')
		}
		dst = append(dst, s...)
	}
	return dst
}

// formatQuoted implements FormatQuoted (VALUES / SQL literal form).
func formatQuoted(t Type, v any) string {
	var b strings.Builder
	writeQuoted(&b, t, v)
	return b.String()
}

func writeQuoted(b *strings.Builder, t Type, v any) {
	if v == nil {
		switch t.Kind {
		case Array:
			b.WriteString("[]")
		case Map:
			b.WriteString("{}")
		default:
			b.WriteString("NULL")
		}
		return
	}
	switch t.Kind {
	case Nullable, LowCardinality, SimpleAggregateFunction:
		writeQuoted(b, *t.Elem, v)
	case String, FixedString, Enum8, Enum16, UUID:
		b.WriteByte('\'')
		writeEscaped(b, toStr(v), '\'')
		b.WriteByte('\'')
	case Date, Date32, DateTime, DateTime64:
		b.WriteByte('\'')
		writeScalarPlain(b, t, v)
		b.WriteByte('\'')
	case Array:
		b.WriteByte('[')
		for i, e := range asSlice(v) {
			if i > 0 {
				b.WriteByte(',')
			}
			writeQuoted(b, *t.Elem, e)
		}
		b.WriteByte(']')
	case Tuple:
		vals := asSlice(v)
		b.WriteByte('(')
		for i, f := range t.Fields {
			if i > 0 {
				b.WriteByte(',')
			}
			var e any
			if i < len(vals) {
				e = vals[i]
			}
			writeQuoted(b, f.Type, e)
		}
		b.WriteByte(')')
	case Map:
		b.WriteByte('{')
		for i, kv := range asSlice(v) {
			if i > 0 {
				b.WriteByte(',')
			}
			k, val := mapPair(kv)
			writeQuoted(b, *t.Key, k)
			b.WriteByte(':')
			writeQuoted(b, *t.Elem, val)
		}
		b.WriteByte('}')
	default:
		writeScalarPlain(b, t, v)
	}
}

// formatCSV implements FormatCSV with the default ',' delimiter.
func formatCSV(t Type, v any) string {
	return string(AppendCSV(nil, t, v, ','))
}

// AppendCSV appends v as a CSV field (ClickHouse rules): strings, dates,
// UUIDs and enums double-quoted with "" escaping, numbers and Bool bare,
// NULL as \N, Array/Map as their quoted text in double quotes, and Tuple
// flattened into delim-separated fields.
func AppendCSV(dst []byte, t Type, v any, delim byte) []byte {
	switch t.Kind {
	case Nullable:
		if v == nil {
			return append(dst, `\N`...)
		}
		return AppendCSV(dst, *t.Elem, v, delim)
	case LowCardinality, SimpleAggregateFunction:
		return AppendCSV(dst, *t.Elem, v, delim)
	case Tuple:
		vals := asSlice(v)
		for i, f := range t.Fields {
			if i > 0 {
				dst = append(dst, delim)
			}
			var e any
			if i < len(vals) {
				e = vals[i]
			}
			dst = AppendCSV(dst, f.Type, e, delim)
		}
		return dst
	case Array, Map:
		var b strings.Builder
		writeQuoted(&b, t, v)
		return appendCSVQuoted(dst, b.String())
	case Bool, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64:
		if v == nil {
			return append(dst, `\N`...)
		}
		return appendScalarPlain(dst, t, v)
	case Nothing:
		return append(dst, `\N`...)
	}
	if v == nil {
		return append(dst, `\N`...)
	}
	return appendCSVQuoted(dst, string(appendScalarPlain(nil, t, v)))
}

func appendCSVQuoted(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			dst = append(dst, '"')
		}
		dst = append(dst, s[i])
	}
	return append(dst, '"')
}

// JSONOptions controls AppendJSONWith. The zero value matches ClickHouse
// 25.x defaults except Quote64/NamedTuplesAsObjects, which callers set from
// output_format_json_quote_64bit_integers (default 0 since 23.x) and
// output_format_json_named_tuples_as_objects (default 1).
type JSONOptions struct {
	Quote64              bool // quote (U)Int64 values as JSON strings
	NamedTuplesAsObjects bool // render named tuples as {"name": value}
	// NoEscapeForwardSlashes disables escaping '/' as "\/"
	// (output_format_json_escape_forward_slashes=0).
	NoEscapeForwardSlashes bool
	// ValidateUTF8 replaces invalid UTF-8 sequences with U+FFFD (the JSON
	// format does this; JSONEachRow passes bytes through by default).
	ValidateUTF8 bool
	// Pretty renders nested arrays/objects one element per line, indented
	// with tabs starting at Indent, like the JSON format's "data" section.
	Pretty bool
	Indent int
}

func appendJSON(dst []byte, t Type, v any, quote64, namedTuplesAsObjects bool) []byte {
	return AppendJSONWith(dst, t, v, JSONOptions{Quote64: quote64, NamedTuplesAsObjects: namedTuplesAsObjects})
}

// AppendJSONWith is AppendJSON with full control over ClickHouse's JSON
// output settings.
func AppendJSONWith(dst []byte, t Type, v any, o JSONOptions) []byte {
	return appendJSONValue(dst, t, v, &o, o.Indent)
}

func appendJSONValue(dst []byte, t Type, v any, o *JSONOptions, indent int) []byte {
	switch t.Kind {
	case Nullable, LowCardinality, SimpleAggregateFunction:
		if v == nil {
			return append(dst, "null"...)
		}
		return appendJSONValue(dst, *t.Elem, v, o, indent)
	}
	if v == nil {
		switch t.Kind {
		case Array:
			return append(dst, "[]"...)
		case Map:
			return append(dst, "{}"...)
		}
		return append(dst, "null"...)
	}
	switch t.Kind {
	case Bool, UInt8, UInt16, UInt32, Int8, Int16, Int32:
		return appendScalarPlain(dst, t, v)
	case UInt64, Int64:
		if o.Quote64 {
			dst = append(dst, '"')
			dst = appendScalarPlain(dst, t, v)
			return append(dst, '"')
		}
		return appendScalarPlain(dst, t, v)
	case Float32, Float64:
		f := asFloat(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return append(dst, "null"...)
		}
		return appendScalarPlain(dst, t, v)
	case Array:
		vals := asSlice(v)
		return appendJSONList(dst, len(vals), o, indent, '[', ']', func(dst []byte, i, ind int) []byte {
			return appendJSONValue(dst, *t.Elem, vals[i], o, ind)
		})
	case Tuple:
		vals := asSlice(v)
		elem := func(i int) any {
			if i < len(vals) {
				return vals[i]
			}
			return nil
		}
		if isNamedTuple(t) && o.NamedTuplesAsObjects {
			return appendJSONList(dst, len(t.Fields), o, indent, '{', '}', func(dst []byte, i, ind int) []byte {
				dst = appendJSONString(dst, t.Fields[i].Name, o)
				dst = append(dst, ':')
				if o.Pretty {
					dst = append(dst, ' ')
				}
				return appendJSONValue(dst, t.Fields[i].Type, elem(i), o, ind)
			})
		}
		if len(t.Fields) == 0 && o.Pretty {
			// ClickHouse prints an empty tuple as "[", blank line, "]".
			dst = append(dst, "[\n\n"...)
			dst = appendTabs(dst, indent)
			return append(dst, ']')
		}
		return appendJSONList(dst, len(t.Fields), o, indent, '[', ']', func(dst []byte, i, ind int) []byte {
			return appendJSONValue(dst, t.Fields[i].Type, elem(i), o, ind)
		})
	case Map:
		pairs := asSlice(v)
		return appendJSONList(dst, len(pairs), o, indent, '{', '}', func(dst []byte, i, ind int) []byte {
			k, val := mapPair(pairs[i])
			dst = appendJSONString(dst, mapKeyText(*t.Key, k), o)
			dst = append(dst, ':')
			if o.Pretty {
				dst = append(dst, ' ')
			}
			return appendJSONValue(dst, *t.Elem, val, o, ind)
		})
	case AggregateFunction:
		return append(dst, "null"...)
	}
	return appendJSONString(dst, string(appendScalarPlain(nil, t, v)), o)
}

// mapKeyText is the text a Map key takes as a JSON object key.
func mapKeyText(kt Type, k any) string {
	u := kt.Unwrap()
	if k == nil {
		return "null"
	}
	switch u.Kind {
	case Array, Tuple, Map:
		return formatQuoted(u, k)
	}
	return string(appendScalarPlain(nil, u, k))
}

func appendJSONList(dst []byte, n int, o *JSONOptions, indent int, open, close byte, elem func([]byte, int, int) []byte) []byte {
	dst = append(dst, open)
	if n == 0 {
		return append(dst, close)
	}
	for i := 0; i < n; i++ {
		if i > 0 {
			dst = append(dst, ',')
		}
		if o.Pretty {
			dst = append(dst, '\n')
			dst = appendTabs(dst, indent+1)
		}
		dst = elem(dst, i, indent+1)
	}
	if o.Pretty {
		dst = append(dst, '\n')
		dst = appendTabs(dst, indent)
	}
	return append(dst, close)
}

func appendTabs(dst []byte, n int) []byte {
	for i := 0; i < n; i++ {
		dst = append(dst, '\t')
	}
	return dst
}

const hexDigitsUpper = "0123456789ABCDEF"

// appendJSONString writes s as a JSON string with ClickHouse's escaping.
func appendJSONString(dst []byte, s string, o *JSONOptions) []byte {
	if o.ValidateUTF8 && !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "\uFFFD")
	}
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '"':
			dst = append(dst, '\\', '"')
		case '/':
			if o.NoEscapeForwardSlashes {
				dst = append(dst, '/')
			} else {
				dst = append(dst, '\\', '/')
			}
		default:
			switch {
			case c < 0x20:
				dst = append(dst, '\\', 'u', '0', '0', hexDigitsUpper[c>>4], hexDigitsUpper[c&0xf])
			case c == 0xE2 && i+2 < len(s) && s[i+1] == 0x80 && (s[i+2] == 0xA8 || s[i+2] == 0xA9):
				if s[i+2] == 0xA8 {
					dst = append(dst, "\\u2028"...)
				} else {
					dst = append(dst, "\\u2029"...)
				}
				i += 2
			default:
				dst = append(dst, c)
			}
		}
	}
	return append(dst, '"')
}

// ---------------------------------------------------------------------------
// Tolerant accessors: formatters accept canonical values and also the common
// non-canonical Go types so that a stray int never crashes a response.

func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case nil:
		return ""
	case fmt.Stringer:
		return x.String()
	}
	return fmt.Sprint(v)
}

func asBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case uint64:
		return x != 0
	case int64:
		return x != 0
	case int:
		return x != 0
	}
	return false
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case uint64:
		return float64(x)
	case int64:
		return float64(x)
	case int:
		return float64(x)
	}
	return math.NaN()
}

func appendUintValue(dst []byte, v any) []byte {
	switch x := v.(type) {
	case uint64:
		return strconv.AppendUint(dst, x, 10)
	case bool:
		if x {
			return append(dst, '1')
		}
		return append(dst, '0')
	}
	if i, ok := goInt(v); ok {
		return strconv.AppendInt(dst, i, 10)
	}
	if u, ok := goUint(v); ok {
		return strconv.AppendUint(dst, u, 10)
	}
	return fmt.Append(dst, v)
}

func appendIntValue(dst []byte, v any) []byte {
	if i, ok := goInt(v); ok {
		return strconv.AppendInt(dst, i, 10)
	}
	if u, ok := goUint(v); ok {
		return strconv.AppendInt(dst, int64(u), 10)
	}
	return fmt.Append(dst, v)
}

func goInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	}
	return 0, false
}

func goUint(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case uint:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	}
	return 0, false
}

func asTime(v any) time.Time {
	if t, ok := v.(time.Time); ok {
		return t
	}
	return Epoch
}

func asSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

func mapPair(kv any) (any, any) {
	p := asSlice(kv)
	if len(p) == 2 {
		return p[0], p[1]
	}
	return nil, nil
}
