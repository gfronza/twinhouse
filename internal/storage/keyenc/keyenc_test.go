package keyenc

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

func ptr(t chtype.Type) *chtype.Type { return &t }

var (
	tNullStr   = chtype.Type{Kind: chtype.Nullable, Elem: ptr(chtype.TString)}
	tNullInt   = chtype.Type{Kind: chtype.Nullable, Elem: ptr(chtype.TInt64)}
	tArrNullI  = chtype.Type{Kind: chtype.Array, Elem: ptr(tNullInt)}
	tArrStr    = chtype.Type{Kind: chtype.Array, Elem: ptr(chtype.TString)}
	tArrArrStr = chtype.Type{Kind: chtype.Array, Elem: ptr(tArrStr)}
	tTuple     = chtype.Type{Kind: chtype.Tuple, Fields: []chtype.Field{{Type: chtype.TString}, {Type: chtype.Type{Kind: chtype.Nullable, Elem: ptr(chtype.TUInt8)}}}}
	tMap       = chtype.Type{Kind: chtype.Map, Key: ptr(chtype.TString), Elem: ptr(chtype.TInt64)}
	tDT64      = chtype.NewDateTime64(9, "")
	tLCStr     = chtype.Type{Kind: chtype.LowCardinality, Elem: ptr(chtype.TString)}
	tArrTuple  = chtype.Type{Kind: chtype.Array, Elem: ptr(tTuple)}
)

// refCompare implements chtype.Compare's documented order for canonical
// values of one type, with the key encoding's normalisations (-0 == 0, NaN
// greatest and equal to itself).
func refCompare(a, b any) int {
	if a == nil || b == nil {
		switch {
		case a == nil && b == nil:
			return 0
		case a == nil:
			return -1
		}
		return 1
	}
	switch x := a.(type) {
	case bool:
		y := b.(bool)
		switch {
		case x == y:
			return 0
		case !x:
			return -1
		}
		return 1
	case uint64:
		return cmp3(x < b.(uint64), x > b.(uint64))
	case int64:
		return cmp3(x < b.(int64), x > b.(int64))
	case float64:
		y := b.(float64)
		xn, yn := math.IsNaN(x), math.IsNaN(y)
		switch {
		case xn && yn:
			return 0
		case xn:
			return 1
		case yn:
			return -1
		}
		return cmp3(x < y, x > y)
	case string:
		return cmp3(x < b.(string), x > b.(string))
	case time.Time:
		return x.Compare(b.(time.Time))
	case []any:
		y := b.([]any)
		for i := 0; i < len(x) && i < len(y); i++ {
			if c := refCompare(x[i], y[i]); c != 0 {
				return c
			}
		}
		return cmp3(len(x) < len(y), len(x) > len(y))
	}
	panic("unexpected value")
}

func cmp3(lt, gt bool) int {
	switch {
	case lt:
		return -1
	case gt:
		return 1
	}
	return 0
}

func sign(c int) int { return cmp3(c < 0, c > 0) }

type gen struct{ r *rand.Rand }

func (g gen) str() string {
	alphabet := []byte{0x00, 0x01, 'a', 'b', 0x7f, 0xfe, 0xff}
	n := g.r.IntN(5)
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[g.r.IntN(len(alphabet))]
	}
	return string(b)
}

func (g gen) value(t chtype.Type) any {
	switch t.Kind {
	case chtype.Nullable:
		if g.r.IntN(4) == 0 {
			return nil
		}
		return g.value(*t.Elem)
	case chtype.LowCardinality:
		return g.value(*t.Elem)
	case chtype.Bool:
		return g.r.IntN(2) == 1
	case chtype.UInt8:
		return uint64(g.r.IntN(256))
	case chtype.UInt64:
		switch g.r.IntN(4) {
		case 0:
			return uint64(g.r.IntN(3))
		case 1:
			return math.MaxUint64 - uint64(g.r.IntN(3))
		}
		return g.r.Uint64()
	case chtype.Int64:
		switch g.r.IntN(4) {
		case 0:
			return int64(g.r.IntN(5) - 2)
		case 1:
			return int64(math.MinInt64 + g.r.IntN(2))
		case 2:
			return int64(math.MaxInt64 - g.r.IntN(2))
		}
		return int64(g.r.Uint64())
	case chtype.Float64:
		specials := []float64{0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.NaN(),
			math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, math.MaxFloat64, -math.MaxFloat64, 1, -1}
		if g.r.IntN(3) == 0 {
			return specials[g.r.IntN(len(specials))]
		}
		return math.Float64frombits(g.r.Uint64())
	case chtype.String:
		return g.str()
	case chtype.DateTime64:
		return time.Unix(0, int64(g.r.Uint64())).UTC()
	case chtype.Array:
		n := g.r.IntN(4)
		out := make([]any, n)
		for i := range out {
			out[i] = g.value(*t.Elem)
		}
		return out
	case chtype.Tuple:
		out := make([]any, len(t.Fields))
		for i, f := range t.Fields {
			out[i] = g.value(f.Type)
		}
		return out
	case chtype.Map:
		n := g.r.IntN(3)
		out := make([]any, n)
		for i := range out {
			out[i] = []any{g.value(*t.Key), g.value(*t.Elem)}
		}
		return out
	}
	panic("gen: unsupported kind")
}

func mustEnc(t *testing.T, typ chtype.Type, v any) []byte {
	t.Helper()
	b, err := Append(nil, typ, v)
	if err != nil {
		t.Fatalf("encode %v: %v", v, err)
	}
	return b
}

func TestOrderMatchesCompare(t *testing.T) {
	types := map[string]chtype.Type{
		"Bool": chtype.TBool, "UInt8": chtype.TUInt8, "UInt64": chtype.TUInt64, "Int64": chtype.TInt64,
		"Float64": chtype.TFloat64, "String": chtype.TString, "Nullable(String)": tNullStr,
		"DateTime64": tDT64, "Array(Nullable(Int64))": tArrNullI, "Array(String)": tArrStr,
		"Array(Array(String))": tArrArrStr, "Tuple": tTuple, "Map": tMap, "LowCardinality": tLCStr,
		"Array(Tuple)": tArrTuple,
	}
	for name, typ := range types {
		t.Run(name, func(t *testing.T) {
			g := gen{rand.New(rand.NewPCG(1, uint64(len(name))))}
			for i := 0; i < 4000; i++ {
				a, b := g.value(typ), g.value(typ)
				ea, eb := mustEnc(t, typ, a), mustEnc(t, typ, b)
				want := sign(refCompare(a, b))
				if got := sign(bytes.Compare(ea, eb)); got != want {
					t.Fatalf("order mismatch for %#v vs %#v: bytes %d, ref %d\n%x\n%x", a, b, got, want, ea, eb)
				}
				// Nested NULLs are excluded: chtype.Compare follows
				// ClickHouse's comparison operators, where a NULL element
				// sorts after every value, while the key encoding keeps
				// NULL first at every depth (changing it would re-key
				// stored rows); the engine never derives ranges or row
				// order from nested key elements.
				if !hasNaN(a) && !hasNaN(b) && !hasNestedNull(a) && !hasNestedNull(b) {
					if c := sign(chtype.Compare(a, b)); c != want {
						t.Fatalf("chtype.Compare(%#v, %#v) = %d, encoding order %d", a, b, c, want)
					}
				}
				// Trailing data after a complete value (next key column or
				// a MergeTree uniquifier, whose first byte is < 0xFF) must not
				// change the order of distinct values.
				if want != 0 {
					ta := append(append([]byte{}, ea...), byte(g.r.IntN(0xFF)), 0xFF, 0xFF)
					if got := sign(bytes.Compare(ta, eb)); got != want {
						t.Fatalf("trailing-data order broken for %#v vs %#v", a, b)
					}
				}
			}
		})
	}
}

func hasNaN(v any) bool {
	switch x := v.(type) {
	case float64:
		return math.IsNaN(x)
	case []any:
		for _, e := range x {
			if hasNaN(e) {
				return true
			}
		}
	}
	return false
}

// hasNestedNull reports a NULL element inside an array, tuple or map.
func hasNestedNull(v any) bool {
	x, ok := v.([]any)
	if !ok {
		return false
	}
	for _, e := range x {
		if e == nil || hasNestedNull(e) {
			return true
		}
	}
	return false
}

func TestTupleOrderAndPrefix(t *testing.T) {
	ts := []chtype.Type{tNullStr, chtype.TInt64, tArrStr, chtype.TFloat64}
	g := gen{rand.New(rand.NewPCG(7, 7))}
	rows := make([][]any, 600)
	for i := range rows {
		rows[i] = make([]any, len(ts))
		for j, typ := range ts {
			rows[i][j] = g.value(typ)
		}
		// Small domains create shared prefixes.
		if g.r.IntN(2) == 0 {
			rows[i][0] = []any{nil, "", "a", "a\x00"}[g.r.IntN(4)]
			rows[i][1] = int64(g.r.IntN(3))
		}
	}
	for i := 0; i < len(rows); i++ {
		a := rows[i]
		ea, err := AppendTuple(nil, ts, a)
		if err != nil {
			t.Fatal(err)
		}
		for j := i; j < len(rows); j++ {
			b := rows[j]
			eb, _ := AppendTuple(nil, ts, b)
			if got, want := sign(bytes.Compare(ea, eb)), sign(refCompare(a, b)); got != want {
				t.Fatalf("tuple order %v vs %v: %d want %d", a, b, got, want)
			}
			for k := 0; k <= len(ts); k++ {
				pa, _ := AppendTuple(nil, ts[:k], a[:k])
				samePrefix := refCompare(a[:k], b[:k]) == 0
				inRange := bytes.Compare(eb, pa) >= 0 && bytes.Compare(eb, PrefixUpper(pa, 1024)) <= 0
				if inRange != samePrefix {
					t.Fatalf("enc(%q) in prefix range of %q[:%d] = %v, want %v", b, a, k, inRange, samePrefix)
				}
				if samePrefix && !bytes.HasPrefix(eb, pa) {
					t.Fatalf("begins_with must hold for equal prefixes")
				}
			}
		}
	}
}

func TestEncodingVectors(t *testing.T) {
	cases := []struct {
		name string
		typ  chtype.Type
		v    any
		want []byte
	}{
		{"null top", tNullStr, nil, []byte{0x00}},
		{"empty string", chtype.TString, "", []byte{0x02, 0x00}},
		{"string with nul", chtype.TString, "a\x00b", []byte{0x02, 'a', 0x00, 0xFF, 'b', 0x00}},
		{"uint", chtype.TUInt64, uint64(1), []byte{0x16, 0, 0, 0, 0, 0, 0, 0, 1}},
		{"int -1", chtype.TInt64, int64(-1), []byte{0x15, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
		{"int 0", chtype.TInt64, int64(0), []byte{0x15, 0x80, 0, 0, 0, 0, 0, 0, 0}},
		{"bool", chtype.TBool, true, []byte{0x27}},
		{"float +0", chtype.TFloat64, 0.0, []byte{0x21, 0x80, 0, 0, 0, 0, 0, 0, 0}},
		{"float -0 normalised", chtype.TFloat64, math.Copysign(0, -1), []byte{0x21, 0x80, 0, 0, 0, 0, 0, 0, 0}},
		{"epoch", chtype.TDateTime, chtype.Epoch, []byte{0x33, 0x80, 0, 0, 0, 0, 0, 0, 0}},
		{"array nested null", tArrNullI, []any{nil}, []byte{0x05, 0x00, 0xFF, 0x00}},
		{"empty array", tArrStr, []any{}, []byte{0x05, 0x00}},
		{"uuid lowercased", chtype.TUUID, "AB", []byte{0x02, 'a', 'b', 0x00}},
		{"int64 into UInt", chtype.TUInt64, int64(5), []byte{0x16, 0, 0, 0, 0, 0, 0, 0, 5}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := mustEnc(t, c.typ, c.v); !bytes.Equal(got, c.want) {
				t.Fatalf("got %x want %x", got, c.want)
			}
		})
	}
	if a, b := mustEnc(t, chtype.TFloat64, math.NaN()), mustEnc(t, chtype.TFloat64, -math.NaN()); !bytes.Equal(a, b) {
		t.Fatalf("NaN payloads encode differently: %x %x", a, b)
	}
}

func TestEncodeErrors(t *testing.T) {
	cases := []struct {
		name string
		typ  chtype.Type
		v    any
		code int
	}{
		{"null in non-nullable", chtype.TString, nil, cherr.TypeMismatch},
		{"wrong go type", chtype.TUInt64, "x", cherr.TypeMismatch},
		{"negative into uint", chtype.TUInt64, int64(-1), cherr.TypeMismatch},
		{"huge uint into int", chtype.TInt64, uint64(math.MaxUint64), cherr.TypeMismatch},
		{"int for float", chtype.TFloat64, int64(1), cherr.TypeMismatch},
		{"tuple arity", tTuple, []any{"a"}, cherr.TypeMismatch},
		{"time out of range", tDT64, time.Date(2290, 1, 1, 0, 0, 0, 0, time.UTC), cherr.BadArguments},
		{"aggregate", chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "sum"}, uint64(1), cherr.IllegalColumn},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Append(nil, c.typ, c.v)
			if !cherr.Is(err, c.code) {
				t.Fatalf("got %v, want code %d", err, c.code)
			}
		})
	}
}

func TestPrefixUpper(t *testing.T) {
	cases := []struct {
		p     []byte
		limit int
		want  []byte
	}{
		{[]byte{1}, 4, []byte{1, 0xFE, 0xFF, 0xFF}},
		{[]byte{1, 2, 3}, 4, []byte{1, 2, 3, 0xFE}},
		{[]byte{1, 2, 3, 4}, 4, []byte{1, 2, 3, 4}},
		{[]byte{1, 2, 3, 4, 5}, 4, []byte{1, 2, 3, 4}},
		{nil, 2, []byte{0xFE, 0xFF}},
	}
	for _, c := range cases {
		if got := PrefixUpper(c.p, c.limit); !bytes.Equal(got, c.want) {
			t.Errorf("PrefixUpper(%x, %d) = %x, want %x", c.p, c.limit, got, c.want)
		}
	}
}

func TestCheckTypeAndRangeSafe(t *testing.T) {
	enum := chtype.Type{Kind: chtype.Enum8, EnumValues: []chtype.EnumValue{{Name: "b", Value: 1}, {Name: "a", Value: 2}}}
	cases := []struct {
		typ       chtype.Type
		keyable   bool
		rangeSafe bool
	}{
		{chtype.TString, true, true},
		{tArrNullI, true, false}, // nested NULL: encodes first, compares last
		{chtype.NewArray(chtype.TInt64), true, true},
		{tMap, true, true},
		{enum, true, false},
		{chtype.NewArray(enum), true, false},
		{chtype.TUUID, true, false}, // text bytes are not UInt128 order
		{chtype.NewTuple(chtype.Field{Type: chtype.TUInt8}, chtype.Field{Type: chtype.TUUID}), true, false},
		{chtype.Type{Kind: chtype.Nullable, Elem: &chtype.TUUID}, true, false},
		{chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "uniq"}, false, false},
		{chtype.TNothing, false, false},
		{chtype.NewTuple(chtype.Field{Type: chtype.TUInt8}, chtype.Field{Type: chtype.Type{Kind: chtype.AggregateFunction}}), false, false},
	}
	for i, c := range cases {
		if got := CheckType(c.typ) == nil; got != c.keyable {
			t.Errorf("case %d: CheckType ok=%v want %v", i, got, c.keyable)
		}
		if got := RangeSafe(c.typ); got != c.rangeSafe {
			t.Errorf("case %d: RangeSafe=%v want %v", i, got, c.rangeSafe)
		}
	}
}
