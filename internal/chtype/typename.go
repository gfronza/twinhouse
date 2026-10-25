package chtype

import (
	"slices"
	"strconv"
	"strings"
)

var kindNames = [...]string{
	Invalid: "Invalid", Nothing: "Nothing", Bool: "Bool",
	UInt8: "UInt8", UInt16: "UInt16", UInt32: "UInt32", UInt64: "UInt64",
	Int8: "Int8", Int16: "Int16", Int32: "Int32", Int64: "Int64",
	Float32: "Float32", Float64: "Float64",
	String: "String", FixedString: "FixedString", UUID: "UUID",
	Date: "Date", Date32: "Date32", DateTime: "DateTime", DateTime64: "DateTime64",
	Enum8: "Enum8", Enum16: "Enum16",
	Nullable: "Nullable", LowCardinality: "LowCardinality",
	Array: "Array", Tuple: "Tuple", Map: "Map",
	AggregateFunction: "AggregateFunction", SimpleAggregateFunction: "SimpleAggregateFunction",
}

func kindName(k Kind) string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "Invalid"
}

// String returns the type-constructor name, e.g. "Array" or "UInt8".
func (k Kind) String() string { return kindName(k) }

func typeString(t Type) string {
	var b strings.Builder
	writeTypeName(&b, t, -1)
	return b.String()
}

// PrettyString renders t the way ClickHouse prints types in DESCRIBE /
// SHOW CREATE output (print_pretty_type_names=1): named tuples put each
// element on its own line, indented four spaces per nesting level. Every
// other type renders exactly like String().
func (t Type) PrettyString() string {
	var b strings.Builder
	writeTypeName(&b, t, 0)
	return b.String()
}

// writeTypeName appends t's name. indent < 0 selects the single-line form;
// otherwise it is the current pretty-printing nesting depth.
func writeTypeName(b *strings.Builder, t Type, indent int) {
	switch t.Kind {
	case FixedString:
		b.WriteString("FixedString(")
		b.WriteString(strconv.Itoa(t.N))
		b.WriteByte(')')
	case DateTime:
		b.WriteString("DateTime")
		if t.TZ != "" {
			b.WriteByte('(')
			b.WriteString(quoteSQLString(t.TZ))
			b.WriteByte(')')
		}
	case DateTime64:
		b.WriteString("DateTime64(")
		b.WriteString(strconv.Itoa(t.Precision))
		if t.TZ != "" {
			b.WriteString(", ")
			b.WriteString(quoteSQLString(t.TZ))
		}
		b.WriteByte(')')
	case Enum8, Enum16:
		b.WriteString(kindName(t.Kind))
		b.WriteByte('(')
		for i, ev := range sortedEnumValues(t.EnumValues) {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quoteSQLString(ev.Name))
			b.WriteString(" = ")
			b.WriteString(strconv.FormatInt(ev.Value, 10))
		}
		b.WriteByte(')')
	case Nullable, LowCardinality, Array:
		b.WriteString(kindName(t.Kind))
		b.WriteByte('(')
		writeTypeName(b, elemOrNothing(t.Elem), indent)
		b.WriteByte(')')
	case Map:
		b.WriteString("Map(")
		writeTypeName(b, elemOrNothing(t.Key), indent)
		b.WriteString(", ")
		writeTypeName(b, elemOrNothing(t.Elem), indent)
		b.WriteByte(')')
	case Tuple:
		writeTupleName(b, t, indent)
	case AggregateFunction:
		b.WriteString("AggregateFunction(")
		writeAggFunc(b, t)
		for _, a := range t.AggArgs {
			b.WriteString(", ")
			writeTypeName(b, a, -1)
		}
		b.WriteByte(')')
	case SimpleAggregateFunction:
		b.WriteString("SimpleAggregateFunction(")
		writeAggFunc(b, t)
		b.WriteString(", ")
		writeTypeName(b, elemOrNothing(t.Elem), -1)
		b.WriteByte(')')
	default:
		b.WriteString(kindName(t.Kind))
	}
}

func writeTupleName(b *strings.Builder, t Type, indent int) {
	named := isNamedTuple(t)
	b.WriteString("Tuple(")
	inner := indent
	if named && indent >= 0 {
		inner = indent + 1
	}
	for i, f := range t.Fields {
		if i > 0 {
			b.WriteByte(',')
			if !(named && indent >= 0) {
				b.WriteByte(' ')
			}
		}
		if named && indent >= 0 {
			b.WriteByte('\n')
			b.WriteString(strings.Repeat("    ", inner))
		}
		if named {
			b.WriteString(backQuoteIfNeed(f.Name))
			b.WriteByte(' ')
		}
		writeTypeName(b, f.Type, inner)
	}
	b.WriteByte(')')
}

func writeAggFunc(b *strings.Builder, t Type) {
	b.WriteString(t.AggFunc)
	if len(t.AggParams) > 0 {
		b.WriteByte('(')
		for i, p := range t.AggParams {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(formatLiteralParam(p))
		}
		b.WriteByte(')')
	}
}

func formatLiteralParam(p any) string {
	switch v := p.(type) {
	case uint64:
		return strconv.FormatUint(v, 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return formatFloat(v, 64)
	case string:
		return quoteSQLString(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case nil:
		return "NULL"
	}
	return "?"
}

func elemOrNothing(t *Type) Type {
	if t == nil {
		return TNothing
	}
	return *t
}

func isNamedTuple(t Type) bool {
	return t.Kind == Tuple && len(t.Fields) > 0 && t.Fields[0].Name != ""
}

func sortedEnumValues(vals []EnumValue) []EnumValue {
	if slices.IsSortedFunc(vals, cmpEnumValue) {
		return vals
	}
	s := slices.Clone(vals)
	slices.SortFunc(s, cmpEnumValue)
	return s
}

func cmpEnumValue(a, b EnumValue) int {
	switch {
	case a.Value < b.Value:
		return -1
	case a.Value > b.Value:
		return 1
	}
	return 0
}

// backQuoteIfNeed quotes an identifier with backquotes unless it is a plain
// [A-Za-z_][A-Za-z0-9_]* word (ClickHouse's backQuoteIfNeed; keywords are
// not quoted).
func backQuoteIfNeed(s string) string {
	plain := s != "" && isIdentStart(s[0])
	for i := 1; plain && i < len(s); i++ {
		plain = isIdentChar(s[i])
	}
	if plain {
		return s
	}
	var b strings.Builder
	b.WriteByte('`')
	writeEscaped(&b, s, '`')
	b.WriteByte('`')
	return b.String()
}

// quoteSQLString renders s as a single-quoted SQL string literal with
// ClickHouse's escaping.
func quoteSQLString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	writeEscaped(&b, s, '\'')
	b.WriteByte('\'')
	return b.String()
}
