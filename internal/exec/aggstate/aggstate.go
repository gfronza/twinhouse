// Package aggstate implements the merge and finalization rules of aggregate
// partial states (*chtype.AggPartial) and the canonical value hashing used for
// sets. It is shared by the query engine and storage implementations so that
// every component merges states identically.
package aggstate

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// Functions usable in AggregateFunction(f, ...) columns.
var stateFuncs = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
	"any": true, "anyLast": true, "argMin": true, "argMax": true,
	"uniq": true, "uniqExact": true, "groupUniqArray": true, "groupArray": true,
}

// Functions usable in SimpleAggregateFunction(f, T) columns.
var simpleFuncs = map[string]bool{
	"sum": true, "sumWithOverflow": true, "min": true, "max": true, "any": true, "anyLast": true,
	"groupUniqArrayArray": true, "groupArrayArray": true,
}

// Supported reports whether f can be stored as an AggregateFunction state.
func Supported(f string) bool { return stateFuncs[StateBase(f)] }

// StateBase returns the canonical function whose state an
// AggregateFunction(f, ...) column holds: an -If state is the state of the
// base function ("sumIf" -> "sum").
func StateBase(f string) string {
	if !stateFuncs[f] && strings.HasSuffix(f, "If") && stateFuncs[strings.TrimSuffix(f, "If")] {
		return strings.TrimSuffix(f, "If")
	}
	return f
}

// SimpleSupported reports whether f can be used in SimpleAggregateFunction.
func SimpleSupported(f string) bool { return simpleFuncs[f] }

// IsSetFunc reports whether f keeps a set of distinct members.
func IsSetFunc(f string) bool { return f == "uniq" || f == "uniqExact" || f == "groupUniqArray" }

// New returns an empty state for f.
func New(f string, argTypes []chtype.Type) *chtype.AggPartial {
	return &chtype.AggPartial{Func: f, ArgTypes: argTypes, SetLoaded: true}
}

// Clone returns a deep-enough copy of p (slices copied, values shared).
func Clone(p *chtype.AggPartial) *chtype.AggPartial {
	if p == nil {
		return nil
	}
	c := *p
	c.ArgTypes = append([]chtype.Type(nil), p.ArgTypes...)
	c.Set = append([]any(nil), p.Set...)
	c.List = append([]any(nil), p.List...)
	return &c
}

// Members returns the set members of p, loading them when necessary.
func Members(ctx context.Context, p *chtype.AggPartial) ([]any, error) {
	if p.SetLoaded || p.SetSize == 0 && len(p.Set) == 0 && p.LoadSet == nil {
		return p.Set, nil
	}
	if p.LoadSet == nil {
		return nil, cherr.New(cherr.LogicalError, "aggregate state %s has %d unloaded members and no loader", p.Func, p.SetSize)
	}
	m, err := p.LoadSet(ctx)
	if err != nil {
		return nil, err
	}
	p.Set, p.SetLoaded, p.SetSize = m, true, uint64(len(m))
	return m, nil
}

// SetCount returns the number of distinct members without loading them.
func SetCount(p *chtype.AggPartial) uint64 {
	if p.SetLoaded {
		return uint64(len(p.Set))
	}
	return p.SetSize
}

// Merge merges src into dst. Both must be states of the same function. Set
// members of both are loaded if necessary.
func Merge(ctx context.Context, dst, src *chtype.AggPartial) error {
	if src == nil {
		return nil
	}
	if dst.Func != src.Func {
		return cherr.New(cherr.TypeMismatch, "cannot merge aggregate state of %s into %s", src.Func, dst.Func)
	}
	switch dst.Func {
	case "count":
		dst.Count += src.Count
	case "sum":
		if src.Sum != nil {
			dst.Sum = AddNumbers(dst.Sum, src.Sum)
		}
		dst.HasValue = dst.HasValue || src.HasValue
	case "avg":
		if src.Sum != nil {
			dst.Sum = toFloat(dst.Sum) + toFloat(src.Sum)
		}
		dst.Count += src.Count
	case "min", "max":
		if src.HasValue && (!dst.HasValue || ReplacesTyped(src.Value, dst.Value, argType(dst, src, 0), dst.Func == "min")) {
			dst.Value, dst.HasValue = src.Value, true
		}
	case "any":
		if src.HasValue && !dst.HasValue {
			dst.Value, dst.HasValue = src.Value, true
		}
	case "anyLast":
		if src.HasValue {
			dst.Value, dst.HasValue = src.Value, true
		}
	case "argMin", "argMax":
		if src.HasValue && (!dst.HasValue || ReplacesTyped(src.Key, dst.Key, argType(dst, src, 1), dst.Func == "argMin")) {
			dst.Value, dst.Key, dst.HasValue = src.Value, src.Key, true
		}
	case "uniq", "uniqExact", "groupUniqArray":
		dm, err := Members(ctx, dst)
		if err != nil {
			return err
		}
		sm, err := Members(ctx, src)
		if err != nil {
			return err
		}
		dst.Set = Union(dm, sm)
		dst.SetSize = uint64(len(dst.Set))
		dst.SetLoaded = true
		dst.LoadSet = nil
	case "groupArray":
		dst.List = append(dst.List, src.List...)
	default:
		return cherr.New(cherr.NotImplemented, "aggregate function %s is not supported in AggregateFunction states", dst.Func)
	}
	return nil
}

// argType is the i-th argument type of a state (from dst, else src), or
// the zero Type when unknown.
func argType(dst, src *chtype.AggPartial, i int) chtype.Type {
	if i < len(dst.ArgTypes) {
		return dst.ArgTypes[i]
	}
	if i < len(src.ArgTypes) {
		return src.ArgTypes[i]
	}
	return chtype.Type{}
}

// Replaces reports whether v replaces the current extremum cur of a min
// (min=true) or max aggregate. Comparisons with a top-level NaN are false
// (IEEE semantics, like ClickHouse), so NaN only survives as the first
// value. Nested NULL/NaN elements (inside arrays and tuples) compare like
// ClickHouse's compareAt with nan_direction_hint +1 for min and -1 for max
// (chtype.CompareHint), so they never win the extremum:
// max([1,NULL],[1,2],[NULL],[1]) = [1,2], min(...) = [1].
func Replaces(v, cur any, min bool) bool {
	if isNaN(v) || isNaN(cur) {
		return false
	}
	if min {
		return chtype.CompareHint(v, cur, 1) < 0
	}
	return chtype.CompareHint(v, cur, -1) > 0
}

func isNaN(v any) bool {
	f, ok := v.(float64)
	return ok && math.IsNaN(f)
}

// Union returns a ∪ b keeping a's order and appending new members of b.
func Union(a, b []any) []any {
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]any, 0, len(a)+len(b))
	for _, s := range [][]any{a, b} {
		for _, v := range s {
			k := Key(v)
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}

// Finalize returns the aggregate's final value for result type rt (the type
// -Merge / finalizeAggregation return). Nullable result types yield NULL for
// empty states where ClickHouse does.
func Finalize(ctx context.Context, p *chtype.AggPartial, rt chtype.Type) (any, error) {
	nullable := rt.IsNullable()
	switch p.Func {
	case "count":
		return p.Count, nil
	case "sum":
		// A state that never saw a value has no Sum (stores may not keep
		// HasValue for sums).
		if p.Sum == nil {
			if nullable {
				return nil, nil
			}
			return chtype.Zero(rt), nil
		}
		return p.Sum, nil
	case "avg":
		if p.Count == 0 {
			if nullable {
				return nil, nil
			}
			return math.NaN(), nil
		}
		return toFloat(p.Sum) / float64(p.Count), nil
	case "min", "max", "any", "anyLast", "argMin", "argMax":
		if !p.HasValue {
			if nullable {
				return nil, nil
			}
			return chtype.Zero(rt), nil
		}
		return p.Value, nil
	case "uniq", "uniqExact":
		return SetCount(p), nil
	case "groupUniqArray":
		m, err := Members(ctx, p)
		if err != nil {
			return nil, err
		}
		return append([]any{}, m...), nil
	case "groupArray":
		return append([]any{}, p.List...), nil
	}
	return nil, cherr.New(cherr.NotImplemented, "cannot finalize aggregate state of %s", p.Func)
}

// AddNumbers adds two canonical numbers. nil acts as zero. Mixed kinds are
// widened (float64 > int64 > uint64); integer overflow wraps.
func AddNumbers(a, b any) any {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	switch x := a.(type) {
	case uint64:
		switch y := b.(type) {
		case uint64:
			return x + y
		case int64:
			return int64(x) + y
		case float64:
			return float64(x) + y
		}
	case int64:
		switch y := b.(type) {
		case uint64:
			return x + int64(y)
		case int64:
			return x + y
		case float64:
			return float64(x) + y
		}
	case float64:
		return x + toFloat(b)
	}
	return a
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case uint64:
		return float64(x)
	case int64:
		return float64(x)
	case float64:
		return x
	}
	return 0
}

// Key returns a canonical byte-string encoding of a value such that two
// canonical values of the same type are equal iff their keys are equal.
// Non-negative int64 and uint64 encode identically so that numeric equality
// holds across signedness.
func Key(v any) string { return string(AppendKey(nil, v)) }

// AppendKey appends the canonical encoding of v (see Key) to b.
func AppendKey(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, 0)
	case bool:
		if x {
			return append(b, 'T')
		}
		return append(b, 'F')
	case uint64:
		b = append(b, 'u')
		return binary.BigEndian.AppendUint64(b, x)
	case int64:
		if x >= 0 {
			b = append(b, 'u')
			return binary.BigEndian.AppendUint64(b, uint64(x))
		}
		b = append(b, 'i')
		return binary.BigEndian.AppendUint64(b, uint64(x))
	case float64:
		if x == 0 {
			x = 0 // fold -0
		}
		if math.IsNaN(x) {
			x = math.NaN()
		}
		b = append(b, 'f')
		return binary.BigEndian.AppendUint64(b, math.Float64bits(x))
	case string:
		b = append(b, 's')
		b = binary.AppendUvarint(b, uint64(len(x)))
		return append(b, x...)
	case time.Time:
		b = append(b, 't')
		b = binary.BigEndian.AppendUint64(b, uint64(x.Unix()))
		return binary.BigEndian.AppendUint32(b, uint32(x.Nanosecond()))
	case []any:
		b = append(b, 'a')
		b = binary.AppendUvarint(b, uint64(len(x)))
		for _, e := range x {
			b = AppendKey(b, e)
		}
		return b
	case *chtype.AggPartial:
		return append(b, fmt.Sprintf("p%p", x)...)
	}
	return append(b, fmt.Sprintf("?%T:%v", v, v)...)
}

// Wrap truncates an integer value to the width of integer type t (two's
// complement wrap-around like ClickHouse), and converts between uint64 and
// int64 representations as t requires. Non-integer types are returned as is.
func Wrap(t chtype.Type, v any) any {
	var u uint64
	switch x := v.(type) {
	case uint64:
		u = x
	case int64:
		u = uint64(x)
	case float64:
		if !t.IsInteger() {
			return x
		}
		if t.IsSigned() {
			u = uint64(int64(x))
		} else {
			u = uint64(x)
		}
	default:
		return v
	}
	switch t.Unwrap().Kind {
	case chtype.UInt8:
		return u & 0xff
	case chtype.UInt16:
		return u & 0xffff
	case chtype.UInt32:
		return u & 0xffffffff
	case chtype.UInt64:
		return u
	case chtype.Int8:
		return int64(int8(u))
	case chtype.Int16:
		return int64(int16(u))
	case chtype.Int32:
		return int64(int32(u))
	case chtype.Int64:
		return int64(u)
	case chtype.Float32, chtype.Float64:
		return toFloat(v)
	}
	return v
}

// OrderKey returns a function mapping values of type t to values that
// chtype.Compare orders like ClickHouse orders t, or nil when values of t
// already compare correctly (see chtype.OrderKey: enum names map to their
// numeric value and UUIDs to their UInt128 order key, at any depth).
func OrderKey(t chtype.Type) func(any) any { return chtype.OrderKey(t) }

// CompareTyped compares two values of type t like ClickHouse (enums by
// value, UUIDs as UInt128; see chtype.CompareTyped).
func CompareTyped(a, b any, t chtype.Type) int { return chtype.CompareTyped(a, b, t) }

// CompareTypedHint is CompareTyped with ClickHouse's nan_direction_hint
// for NULL and NaN (chtype.CompareTypedHint).
func CompareTypedHint(a, b any, t chtype.Type, hint int) int {
	return chtype.CompareTypedHint(a, b, t, hint)
}

// ReplacesTyped is Replaces for values of type t (enums compare by value,
// UUIDs as UInt128).
func ReplacesTyped(v, cur any, t chtype.Type, min bool) bool {
	if isNaN(v) || isNaN(cur) {
		return false
	}
	if min {
		return chtype.CompareTypedHint(v, cur, t, 1) < 0
	}
	return chtype.CompareTypedHint(v, cur, t, -1) > 0
}
