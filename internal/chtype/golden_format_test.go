package chtype

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

type formatCase struct {
	expr, typ                              string
	values, tsv, csv                       string
	json, json64, jsonNoNamed, jsonNoSlash string
	jsonPretty                             string
}

// TestFormatGolden checks every formatter byte-for-byte against output
// captured from ClickHouse 25.8. The Go value is obtained by parsing
// ClickHouse's own Values rendering, so the test also covers ParseQuoted,
// and the TSV rendering is parsed back with ParseText.
func TestFormatGolden(t *testing.T) {
	for _, c := range formatGolden {
		t.Run(c.expr, func(t *testing.T) {
			typ, err := Parse(c.typ)
			if err != nil {
				t.Fatalf("Parse(%q): %v", c.typ, err)
			}
			if got := typ.String(); got != c.typ {
				t.Errorf("String() = %q, want %q", got, c.typ)
			}
			v, n, err := ParseQuoted(typ, c.values)
			if err != nil {
				t.Fatalf("ParseQuoted(%q): %v", c.values, err)
			}
			if n != len(c.values) {
				t.Errorf("ParseQuoted consumed %d of %d bytes", n, len(c.values))
			}
			checkCanonical(t, typ, v)
			check := func(what, got, want string) {
				t.Helper()
				if got != want {
					t.Errorf("%s:\n got %q\nwant %q", what, got, want)
				}
			}
			check("FormatQuoted", FormatQuoted(typ, v), c.values)
			check("FormatText", FormatText(typ, v), c.tsv)
			check("FormatCSV", FormatCSV(typ, v), c.csv)
			check("AppendJSON", string(AppendJSON(nil, typ, v, false, true)), c.json)
			check("AppendJSON quote64", string(AppendJSON(nil, typ, v, true, true)), c.json64)
			check("AppendJSON tuples as arrays", string(AppendJSON(nil, typ, v, false, false)), c.jsonNoNamed)
			check("AppendJSON no slash escape", string(AppendJSONWith(nil, typ, v, JSONOptions{NamedTuplesAsObjects: true, NoEscapeForwardSlashes: true})), c.jsonNoSlash)
			check("AppendJSON pretty", string(AppendJSONWith(nil, typ, v, JSONOptions{NamedTuplesAsObjects: true, Pretty: true, Indent: 3, ValidateUTF8: true})), c.jsonPretty)

			back, err := ParseText(typ, c.tsv)
			if err != nil {
				t.Fatalf("ParseText(%q): %v", c.tsv, err)
			}
			if !sameValue(back, v) {
				t.Errorf("ParseText round trip: got %#v, want %#v", back, v)
			}
		})
	}
}

// sameValue is strict equality of canonical values: identical Go types at
// every level and Compare == 0 (so NaN equals NaN).
func sameValue(a, b any) bool {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	if x, ok := a.([]any); ok {
		y := b.([]any)
		if len(x) != len(y) {
			return false
		}
		for i := range x {
			if !sameValue(x[i], y[i]) {
				return false
			}
		}
		return true
	}
	if ta, ok := a.(time.Time); ok {
		return ta.Equal(b.(time.Time)) && ta.Location() == time.UTC && b.(time.Time).Location() == time.UTC
	}
	return Compare(a, b) == 0
}

// checkCanonical asserts v uses exactly the representation type.go mandates.
func checkCanonical(t *testing.T, typ Type, v any) {
	t.Helper()
	if err := canonicalErr(typ, v); err != nil {
		t.Errorf("non-canonical value %#v for %s: %v", v, typ, err)
	}
}

func canonicalErr(typ Type, v any) error {
	bad := func() error { return fmt.Errorf("got Go type %T", v) }
	switch typ.Kind {
	case Nullable:
		if v == nil {
			return nil
		}
		return canonicalErr(*typ.Elem, v)
	case LowCardinality, SimpleAggregateFunction:
		return canonicalErr(*typ.Elem, v)
	case Nothing:
		if v != nil {
			return bad()
		}
	case Bool:
		if _, ok := v.(bool); !ok {
			return bad()
		}
	case UInt8, UInt16, UInt32, UInt64:
		if _, ok := v.(uint64); !ok {
			return bad()
		}
	case Int8, Int16, Int32, Int64:
		if _, ok := v.(int64); !ok {
			return bad()
		}
	case Float32, Float64:
		f, ok := v.(float64)
		if !ok {
			return bad()
		}
		if typ.Kind == Float32 && f == f && float64(float32(f)) != f {
			return fmt.Errorf("Float32 value %v is not float32-representable", f)
		}
	case String, UUID, Enum8, Enum16:
		if _, ok := v.(string); !ok {
			return bad()
		}
	case FixedString:
		s, ok := v.(string)
		if !ok {
			return bad()
		}
		if len(s) != typ.N {
			return fmt.Errorf("FixedString(%d) value has length %d", typ.N, len(s))
		}
	case Date, Date32, DateTime, DateTime64:
		tm, ok := v.(time.Time)
		if !ok {
			return bad()
		}
		if tm.Location() != time.UTC {
			return fmt.Errorf("time not in UTC: %v", tm.Location())
		}
		if (typ.Kind == Date || typ.Kind == Date32) && (tm.Hour() != 0 || tm.Minute() != 0 || tm.Second() != 0 || tm.Nanosecond() != 0) {
			return fmt.Errorf("date with time component %v", tm)
		}
	case Array:
		s, ok := v.([]any)
		if !ok {
			return bad()
		}
		for _, e := range s {
			if err := canonicalErr(*typ.Elem, e); err != nil {
				return err
			}
		}
	case Tuple:
		s, ok := v.([]any)
		if !ok || len(s) != len(typ.Fields) {
			return bad()
		}
		for i, e := range s {
			if err := canonicalErr(typ.Fields[i].Type, e); err != nil {
				return err
			}
		}
	case Map:
		s, ok := v.([]any)
		if !ok {
			return bad()
		}
		for _, kv := range s {
			p, ok := kv.([]any)
			if !ok || len(p) != 2 {
				return fmt.Errorf("map entry %T", kv)
			}
			if err := canonicalErr(*typ.Key, p[0]); err != nil {
				return err
			}
			if err := canonicalErr(*typ.Elem, p[1]); err != nil {
				return err
			}
		}
	case AggregateFunction:
		if _, ok := v.(*AggPartial); !ok {
			return bad()
		}
	}
	return nil
}
