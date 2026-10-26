package catalog

import (
	"strings"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// Canonical aggregate function names understood by the storage layer.
const (
	AggCount          = "count"
	AggSum            = "sum"
	AggAvg            = "avg"
	AggMin            = "min"
	AggMax            = "max"
	AggAny            = "any"
	AggAnyLast        = "anyLast"
	AggArgMin         = "argMin"
	AggArgMax         = "argMax"
	AggUniq           = "uniq"
	AggUniqExact      = "uniqExact"
	AggGroupUniqArray = "groupUniqArray"
	AggGroupArray     = "groupArray"
)

// AggregateSpec describes an AggregateFunction(f, args...) column the way
// storage persists it.
type AggregateSpec struct {
	// Func is the canonical, combinator-free function name (Agg* constants).
	Func string
	// Args are the aggregate's argument types (an -If condition removed).
	Args []chtype.Type
}

// IsSet reports whether the state is a set of distinct members kept in the
// members side table.
func (s AggregateSpec) IsSet() bool {
	return s.Func == AggUniq || s.Func == AggUniqExact || s.Func == AggGroupUniqArray
}

// MemberType returns the type of set members (one argument: its type;
// several: a tuple of them).
func (s AggregateSpec) MemberType() chtype.Type {
	if len(s.Args) == 1 {
		return s.Args[0]
	}
	fields := make([]chtype.Field, len(s.Args))
	for i, a := range s.Args {
		fields[i] = chtype.Field{Type: a}
	}
	return chtype.NewTuple(fields...)
}

// SumKind returns the Go kind of a sum/avg state's Sum: uint64, int64 or
// float64 (sum(UIntN) -> UInt64, sum(IntN) -> Int64, sum(Float) -> Float64,
// avg -> Float64).
func (s AggregateSpec) SumKind() chtype.Kind {
	if s.Func == AggAvg || len(s.Args) == 0 {
		return chtype.Float64
	}
	return SumResultKind(s.Args[0])
}

// SumResultKind returns UInt64, Int64 or Float64: the result kind of sum(t).
func SumResultKind(t chtype.Type) chtype.Kind {
	switch base(t).Kind {
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64, chtype.Bool:
		return chtype.UInt64
	case chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		return chtype.Int64
	}
	return chtype.Float64
}

// caseInsensitiveAggs are the functions ClickHouse resolves case-insensitively.
var caseInsensitiveAggs = map[string]string{
	"count": AggCount, "sum": AggSum, "avg": AggAvg, "min": AggMin, "max": AggMax, "any": AggAny,
}

var exactAggs = map[string]bool{
	AggAnyLast: true, AggArgMin: true, AggArgMax: true, AggUniq: true, AggUniqExact: true,
	AggGroupUniqArray: true, AggGroupArray: true,
}

func canonicalAgg(name string) (string, bool) {
	if c, ok := caseInsensitiveAggs[strings.ToLower(name)]; ok {
		return c, true
	}
	return name, exactAggs[name]
}

// ResolveAggregate validates an AggregateFunction type and returns its spec.
// Supported: count, sum, avg, min, max, any, anyLast, argMin, argMax, uniq,
// uniqExact, groupUniqArray, groupArray, each optionally with the -If
// combinator (whose state equals the base function's).
func ResolveAggregate(t chtype.Type) (AggregateSpec, error) {
	if t.Kind != chtype.AggregateFunction {
		return AggregateSpec{}, cherr.New(cherr.LogicalError, "not an AggregateFunction type")
	}
	name, args := t.AggFunc, t.AggArgs
	fn, ok := canonicalAgg(name)
	if !ok && strings.HasSuffix(name, "If") {
		if base, okBase := canonicalAgg(strings.TrimSuffix(name, "If")); okBase {
			if len(args) == 0 {
				return AggregateSpec{}, cherr.New(cherr.NumberOfArgsDoesntMatch, "aggregate function %s requires a condition argument", name)
			}
			fn, ok, args = base, true, args[:len(args)-1]
		}
	}
	if !ok {
		return AggregateSpec{}, cherr.New(cherr.NotImplemented,
			"AggregateFunction(%s, ...) columns are not supported by the DynamoDB storage", name)
	}
	if len(t.AggParams) > 0 {
		return AggregateSpec{}, cherr.New(cherr.NotImplemented, "parametric AggregateFunction(%s(...)) columns are not supported", name)
	}
	want := 1
	switch fn {
	case AggCount:
		want = -1
	case AggArgMin, AggArgMax:
		want = 2
	case AggUniq, AggUniqExact:
		if len(args) == 0 {
			return AggregateSpec{}, cherr.New(cherr.NumberOfArgsDoesntMatch, "aggregate function %s requires at least one argument", name)
		}
		want = -1
	}
	if want >= 0 && len(args) != want {
		return AggregateSpec{}, cherr.New(cherr.NumberOfArgsDoesntMatch,
			"aggregate function %s requires %d argument(s), got %d", name, want, len(args))
	}
	return AggregateSpec{Func: fn, Args: args}, nil
}

// ResolveSimpleAggregate validates a SimpleAggregateFunction(f, T) type and
// returns the canonical merge function: sum (also sumWithOverflow), min, max,
// any, anyLast, groupUniqArray (also groupUniqArrayArray; T must be an
// Array: the value is the union of the arrays) or groupArray
// (groupArrayArray: concatenation).
func ResolveSimpleAggregate(t chtype.Type) (string, error) {
	if t.Kind != chtype.SimpleAggregateFunction || t.Elem == nil {
		return "", cherr.New(cherr.LogicalError, "not a SimpleAggregateFunction type")
	}
	var fn string
	switch t.AggFunc {
	case "sum", "sumWithOverflow":
		fn = AggSum
		if k := base(*t.Elem).Kind; k != chtype.Bool && !numericKind(k) {
			return "", cherr.New(cherr.IllegalTypeOfArgument, "SimpleAggregateFunction(%s, ...) requires a numeric type", t.AggFunc)
		}
	case "min", "max", "any", "anyLast":
		fn = t.AggFunc
	case "groupUniqArray", "groupUniqArrayArray":
		fn = AggGroupUniqArray
	case "groupArrayArray":
		fn = AggGroupArray
	default:
		return "", cherr.New(cherr.NotImplemented,
			"SimpleAggregateFunction(%s, ...) columns are not supported by the DynamoDB storage", t.AggFunc)
	}
	if fn == AggGroupUniqArray || fn == AggGroupArray {
		if b := base(*t.Elem); b.Kind != chtype.Array || b.Elem == nil {
			return "", cherr.New(cherr.IllegalTypeOfArgument, "SimpleAggregateFunction(%s, ...) requires an Array type", t.AggFunc)
		}
	}
	return fn, nil
}

func numericKind(k chtype.Kind) bool {
	switch k {
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64,
		chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64, chtype.Float32, chtype.Float64:
		return true
	}
	return false
}

// base strips Nullable and LowCardinality wrappers (any nesting).
func base(t chtype.Type) chtype.Type {
	for (t.Kind == chtype.Nullable || t.Kind == chtype.LowCardinality) && t.Elem != nil {
		t = *t.Elem
	}
	return t
}
