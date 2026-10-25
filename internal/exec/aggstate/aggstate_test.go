package aggstate

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"

	"dynamodb-eventstream/internal/chtype"
)

func TestMergeAndFinalize(t *testing.T) {
	ctx := context.Background()
	mk := func(f string, mut func(*chtype.AggPartial)) *chtype.AggPartial {
		p := New(f, nil)
		mut(p)
		return p
	}
	cases := []struct {
		name string
		a, b *chtype.AggPartial
		rt   chtype.Type
		want any
	}{
		{"count", mk("count", func(p *chtype.AggPartial) { p.Count = 2 }), mk("count", func(p *chtype.AggPartial) { p.Count = 3 }), chtype.TUInt64, uint64(5)},
		{"sum_uint", mk("sum", func(p *chtype.AggPartial) { p.Sum = uint64(2) }), mk("sum", func(p *chtype.AggPartial) { p.Sum = uint64(3) }), chtype.TUInt64, uint64(5)},
		{"sum_int_wrap", mk("sum", func(p *chtype.AggPartial) { p.Sum = int64(math.MaxInt64) }), mk("sum", func(p *chtype.AggPartial) { p.Sum = int64(1) }), chtype.TInt64, int64(math.MinInt64)},
		{"sum_empty", New("sum", nil), New("sum", nil), chtype.TUInt64, uint64(0)},
		{"sum_empty_nullable", New("sum", nil), New("sum", nil), chtype.NewNullable(chtype.TUInt64), nil},
		{"avg", mk("avg", func(p *chtype.AggPartial) { p.Sum, p.Count = 3.0, 2 }), mk("avg", func(p *chtype.AggPartial) { p.Sum, p.Count = 3.0, 1 }), chtype.TFloat64, 2.0},
		{"min", mk("min", func(p *chtype.AggPartial) { p.Value, p.HasValue = "b", true }), mk("min", func(p *chtype.AggPartial) { p.Value, p.HasValue = "a", true }), chtype.TString, "a"},
		{"max_empty_side", mk("max", func(p *chtype.AggPartial) { p.Value, p.HasValue = int64(-5), true }), New("max", nil), chtype.TInt64, int64(-5)},
		{"max_nan_first", mk("max", func(p *chtype.AggPartial) { p.Value, p.HasValue = math.NaN(), true }), mk("max", func(p *chtype.AggPartial) { p.Value, p.HasValue = 1.0, true }), chtype.TFloat64, math.NaN()},
		{"max_nan_later", mk("max", func(p *chtype.AggPartial) { p.Value, p.HasValue = 1.0, true }), mk("max", func(p *chtype.AggPartial) { p.Value, p.HasValue = math.NaN(), true }), chtype.TFloat64, 1.0},
		{"any", mk("any", func(p *chtype.AggPartial) { p.Value, p.HasValue = "first", true }), mk("any", func(p *chtype.AggPartial) { p.Value, p.HasValue = "second", true }), chtype.TString, "first"},
		{"anyLast", mk("anyLast", func(p *chtype.AggPartial) { p.Value, p.HasValue = "first", true }), mk("anyLast", func(p *chtype.AggPartial) { p.Value, p.HasValue = "second", true }), chtype.TString, "second"},
		{"argMax", mk("argMax", func(p *chtype.AggPartial) { p.Value, p.Key, p.HasValue = "old", uint64(1), true }), mk("argMax", func(p *chtype.AggPartial) { p.Value, p.Key, p.HasValue = "new", uint64(2), true }), chtype.TString, "new"},
		{"argMax_tie_keeps_first", mk("argMax", func(p *chtype.AggPartial) { p.Value, p.Key, p.HasValue = "a", uint64(2), true }), mk("argMax", func(p *chtype.AggPartial) { p.Value, p.Key, p.HasValue = "b", uint64(2), true }), chtype.TString, "a"},
		{"argMin", mk("argMin", func(p *chtype.AggPartial) { p.Value, p.Key, p.HasValue = "old", uint64(1), true }), mk("argMin", func(p *chtype.AggPartial) { p.Value, p.Key, p.HasValue = "new", uint64(2), true }), chtype.TString, "old"},
		{"argMax_empty", New("argMax", nil), New("argMax", nil), chtype.TString, ""},
		{"uniqExact", mk("uniqExact", func(p *chtype.AggPartial) { p.Set = []any{"a", "b"} }), mk("uniqExact", func(p *chtype.AggPartial) { p.Set = []any{"b", "c"} }), chtype.TUInt64, uint64(3)},
		{"groupArray", mk("groupArray", func(p *chtype.AggPartial) { p.List = []any{1} }), mk("groupArray", func(p *chtype.AggPartial) { p.List = []any{2} }), chtype.NewArray(chtype.TUInt8), []any{1, 2}},
	}
	for _, c := range cases {
		if err := Merge(ctx, c.a, c.b); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got, err := Finalize(ctx, c.a, c.rt)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if f, ok := c.want.(float64); ok && math.IsNaN(f) {
			if g, ok := got.(float64); !ok || !math.IsNaN(g) {
				t.Errorf("%s: got %v, want NaN", c.name, got)
			}
			continue
		}
		if chtype.Compare(got, c.want) != 0 || (got == nil) != (c.want == nil) {
			t.Errorf("%s: got %#v, want %#v", c.name, got, c.want)
		}
	}
	if err := Merge(ctx, New("sum", nil), New("count", nil)); err == nil {
		t.Error("merging different functions must fail")
	}
	avg := New("avg", nil)
	if v, _ := Finalize(ctx, avg, chtype.TFloat64); !math.IsNaN(v.(float64)) {
		t.Errorf("avg of nothing is nan, got %v", v)
	}
}

func TestLazySets(t *testing.T) {
	ctx := context.Background()
	loads := 0
	lazy := func(members ...any) *chtype.AggPartial {
		return &chtype.AggPartial{Func: "uniqExact", SetSize: uint64(len(members)), LoadSet: func(context.Context) ([]any, error) {
			loads++
			return members, nil
		}}
	}
	p := lazy("a", "b", "c")
	eq := func(got, want any) {
		t.Helper()
		if chtype.Compare(got, want) != 0 {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	v, err := Finalize(ctx, p, chtype.TUInt64)
	if err != nil {
		t.Fatal(err)
	}
	eq(v, uint64(3))
	eq(loads, 0)
	dst := New("uniqExact", nil)
	if err := Merge(ctx, dst, lazy("a", "b")); err != nil {
		t.Fatal(err)
	}
	if err := Merge(ctx, dst, lazy("b", "z")); err != nil {
		t.Fatal(err)
	}
	eq(loads, 2)
	eq(SetCount(dst), uint64(3))
	m, _ := Members(ctx, dst)
	got := []string{}
	for _, x := range m {
		got = append(got, x.(string))
	}
	sort.Strings(got)
	eq(len(got), 3)
	if _, err := Members(ctx, &chtype.AggPartial{Func: "uniq", SetSize: 2}); err == nil {
		t.Error("an unloaded set without loader must fail")
	}
}

func TestKeyAndWrap(t *testing.T) {
	ts := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	same := [][2]any{
		{uint64(5), int64(5)}, {0.0, math.Copysign(0, -1)}, {math.NaN(), math.NaN()},
		{[]any{"a", uint64(1)}, []any{"a", int64(1)}}, {ts, ts.In(time.FixedZone("x", 3600))}, {nil, nil},
	}
	for _, p := range same {
		if Key(p[0]) != Key(p[1]) {
			t.Errorf("Key(%v) != Key(%v)", p[0], p[1])
		}
	}
	diff := [][2]any{
		{int64(-1), uint64(1<<64 - 1)}, {"a", []any{"a"}}, {[]any{"ab"}, []any{"a", "b"}}, {nil, ""}, {true, uint64(1)},
		{uint64(1), 1.0}, {ts, ts.Add(time.Nanosecond)},
	}
	for _, p := range diff {
		if Key(p[0]) == Key(p[1]) {
			t.Errorf("Key(%v) == Key(%v)", p[0], p[1])
		}
	}
	wraps := []struct {
		t    chtype.Type
		in   any
		want any
	}{
		{chtype.TUInt8, uint64(300), uint64(44)}, {chtype.TUInt8, int64(-1), uint64(255)}, {chtype.TInt8, int64(200), int64(-56)},
		{chtype.TInt16, uint64(40000), int64(-25536)}, {chtype.TUInt32, int64(-1), uint64(4294967295)}, {chtype.TInt64, uint64(1 << 63), int64(math.MinInt64)},
		{chtype.TFloat64, uint64(3), 3.0}, {chtype.TString, "x", "x"},
	}
	for _, w := range wraps {
		if got := Wrap(w.t, w.in); got != w.want {
			t.Errorf("Wrap(%s, %v) = %#v, want %#v", w.t.String(), w.in, got, w.want)
		}
	}
	if StateBase("sumIf") != "sum" || StateBase("uniqExactIf") != "uniqExact" || StateBase("sum") != "sum" || StateBase("noIf") != "noIf" {
		t.Error("StateBase")
	}
	if !Supported("countIf") || Supported("quantile") || !SimpleSupported("groupUniqArrayArray") || SimpleSupported("uniqExact") {
		t.Error("Supported/SimpleSupported")
	}
}
