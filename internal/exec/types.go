package exec

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// intBytes returns the byte width of an integer kind (Bool counts as UInt8).
func intBytes(k chtype.Kind) int {
	switch k {
	case chtype.UInt8, chtype.Int8, chtype.Bool:
		return 1
	case chtype.UInt16, chtype.Int16:
		return 2
	case chtype.UInt32, chtype.Int32:
		return 4
	case chtype.UInt64, chtype.Int64:
		return 8
	}
	return 0
}

func isFloatKind(k chtype.Kind) bool { return k == chtype.Float32 || k == chtype.Float64 }

func isIntKind(k chtype.Kind) bool {
	return intBytes(k) > 0
}

func isSignedKind(k chtype.Kind) bool {
	switch k {
	case chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		return true
	}
	return false
}

// isNumKind reports numeric kinds usable in arithmetic (Bool acts as UInt8).
func isNumKind(k chtype.Kind) bool { return isIntKind(k) || isFloatKind(k) }

func isStringKind(k chtype.Kind) bool { return k == chtype.String || k == chtype.FixedString }

func isTimeKind(k chtype.Kind) bool {
	return k == chtype.Date || k == chtype.Date32 || k == chtype.DateTime || k == chtype.DateTime64
}

// intType constructs an integer type of the given signedness and width.
func intType(signed bool, size int) chtype.Type {
	switch {
	case size <= 1:
		if signed {
			return chtype.TInt8
		}
		return chtype.TUInt8
	case size == 2:
		if signed {
			return chtype.TInt16
		}
		return chtype.TUInt16
	case size <= 4:
		if signed {
			return chtype.TInt32
		}
		return chtype.TUInt32
	}
	if signed {
		return chtype.TInt64
	}
	return chtype.TUInt64
}

func nextSize(n int) int {
	if n >= 8 {
		return 8
	}
	return n * 2
}

// arithType returns ClickHouse's result type for a binary arithmetic
// function over unwrapped numeric argument types.
func arithType(op string, a, b chtype.Type) (chtype.Type, error) {
	ka, kb := a.Kind, b.Kind
	if !isNumKind(ka) || !isNumKind(kb) {
		return chtype.Type{}, cherr.New(cherr.IllegalTypeOfArgument, "Illegal types %s and %s of arguments of function %s", a.String(), b.String(), op)
	}
	fl := isFloatKind(ka) || isFloatKind(kb)
	sa, sb := isSignedKind(ka), isSignedKind(kb)
	na, nb := intBytes(ka), intBytes(kb)
	switch op {
	case "plus", "multiply":
		if fl {
			return chtype.TFloat64, nil
		}
		return intType(sa || sb, nextSize(max(na, nb))), nil
	case "minus":
		if fl {
			return chtype.TFloat64, nil
		}
		return intType(true, nextSize(max(na, nb))), nil
	case "divide":
		return chtype.TFloat64, nil
	case "intDiv", "intDivOrZero":
		if fl {
			return chtype.TInt64, nil
		}
		return intType(sa || sb, na), nil
	case "modulo", "moduloOrZero":
		if fl {
			return chtype.TFloat64, nil
		}
		// ResultOfModulo: the size of the divisor, widened when the dividend
		// is signed; signedness of the dividend.
		size := nb
		if sa {
			size = nextSize(nb)
		}
		return intType(sa, size), nil
	}
	return chtype.Type{}, cherr.New(cherr.LogicalError, "unknown arithmetic op %s", op)
}

// Numeric value accessors over canonical values.

func asFloat(v any) float64 {
	switch x := v.(type) {
	case uint64:
		return float64(x)
	case int64:
		return float64(x)
	case float64:
		return x
	case bool:
		if x {
			return 1
		}
	}
	return 0
}

func asInt(v any) int64 {
	switch x := v.(type) {
	case uint64:
		return int64(x)
	case int64:
		return x
	case float64:
		return floatToInt64(x)
	case bool:
		if x {
			return 1
		}
	}
	return 0
}

func asUint(v any) uint64 {
	switch x := v.(type) {
	case uint64:
		return x
	case int64:
		return uint64(x)
	case float64:
		if x < 0 {
			return uint64(floatToInt64(x))
		}
		if x >= 18446744073709551615 {
			return math.MaxUint64
		}
		return uint64(x)
	case bool:
		if x {
			return 1
		}
	}
	return 0
}

func floatToInt64(x float64) int64 {
	switch {
	case math.IsNaN(x):
		return math.MinInt64
	case x >= 9223372036854775807:
		return math.MaxInt64
	case x <= -9223372036854775808:
		return math.MinInt64
	}
	return int64(x)
}

// isNumber reports whether v is a canonical numeric value.
func isNumber(v any) bool {
	switch v.(type) {
	case uint64, int64, float64:
		return true
	}
	return false
}

func isNaN(v any) bool {
	f, ok := v.(float64)
	return ok && math.IsNaN(f)
}

// canBeInLowCardinality mirrors ClickHouse's rule for LowCardinality results.
func canBeInLowCardinality(t chtype.Type) bool {
	k := t.Kind
	if k == chtype.Nullable {
		k = t.Elem.Kind
	}
	return isNumKind(k) || isStringKind(k) || isTimeKind(k) || k == chtype.UUID
}

// nullableIf wraps t in Nullable when cond holds (and t can be nullable).
func nullableIf(t chtype.Type, cond bool) chtype.Type {
	if !cond || t.IsNullable() || t.Kind == chtype.Array || t.Kind == chtype.Tuple || t.Kind == chtype.Map ||
		t.Kind == chtype.AggregateFunction {
		return t
	}
	if t.Kind == chtype.LowCardinality {
		inner := chtype.NewNullable(*t.Elem)
		return chtype.NewLowCardinality(inner)
	}
	return chtype.NewNullable(t)
}

// isNothing reports whether t is Nothing or Nullable(Nothing).
func isNothing(t chtype.Type) bool {
	if t.Kind == chtype.Nothing {
		return true
	}
	return t.Kind == chtype.Nullable && t.Elem != nil && t.Elem.Kind == chtype.Nothing
}

// nullNothing is the type of the NULL literal.
var nullNothing = chtype.NewNullable(chtype.TNothing)

// exactConvert converts v from type from to type to only if the value is
// represented exactly (round-trips); ok is false otherwise. Used where a lossy
// conversion would change semantics (key pushdown, IN sets).
func exactConvert(v any, from, to chtype.Type) (any, bool) {
	if v == nil {
		return nil, true
	}
	fu, tu := from.Unwrap(), to.Unwrap()
	if fu.Equal(tu) {
		return v, true
	}
	if isNumKind(tu.Kind) && isNumber(v) {
		return exactNumber(v, tu)
	}
	if isNumKind(tu.Kind) {
		if b, ok := v.(bool); ok {
			return exactNumber(boolNum(b), tu)
		}
	}
	if s, ok := v.(string); ok && (fu.Kind == chtype.String || fu.Kind == chtype.FixedString) && tu.Kind == chtype.FixedString {
		// FixedString equality ignores trailing zero bytes (zero padding).
		s = strings.TrimRight(s, "\x00")
		if len(s) > tu.N {
			return nil, false
		}
		return s + strings.Repeat("\x00", tu.N-len(s)), true
	}
	if s, ok := v.(string); ok && isStringKind(fu.Kind) && !isStringKind(tu.Kind) {
		// Strings compare with other types by parsing them into that type
		// (see coerceCompare), so the parsed value is the exact key.
		if isNumKind(tu.Kind) {
			n, err := numericFromString(s, tu)
			if err != nil {
				return nil, false
			}
			return exactNumber(n, tu)
		}
		out, err := chtype.Convert(s, chtype.TString, tu)
		if err != nil {
			return nil, false
		}
		return out, true
	}
	out, err := chtype.Convert(v, fu, tu)
	if err != nil {
		return nil, false
	}
	back, err := chtype.Convert(out, tu, fu)
	if err != nil || chtype.Compare(back, v) != 0 {
		return nil, false
	}
	return out, true
}

func boolNum(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// exactNumber converts a canonical number to integer/float type t exactly.
func exactNumber(v any, t chtype.Type) (any, bool) {
	k := t.Kind
	if isFloatKind(k) {
		switch x := v.(type) {
		case float64:
			if k == chtype.Float32 && !math.IsNaN(x) && !math.IsInf(x, 0) && float64(float32(x)) != x {
				return nil, false
			}
			return x, true
		case uint64:
			return float64(x), uint64(float64(x)) == x
		case int64:
			return float64(x), int64(float64(x)) == x
		}
		return nil, false
	}
	if k == chtype.Bool {
		switch asFloat(v) {
		case 0:
			return false, true
		case 1:
			return true, true
		}
		return nil, false
	}
	var lo, hi float64
	n := intBytes(k)
	if isSignedKind(k) {
		lo, hi = -math.Pow(2, float64(8*n-1)), math.Pow(2, float64(8*n-1))-1
	} else {
		lo, hi = 0, math.Pow(2, float64(8*n))-1
	}
	switch x := v.(type) {
	case uint64:
		if k == chtype.UInt64 || float64(x) <= hi {
			if isSignedKind(k) {
				if x > math.MaxInt64 || float64(x) > hi {
					return nil, false
				}
				return int64(x), true
			}
			return x, true
		}
		return nil, false
	case int64:
		if x < 0 && !isSignedKind(k) {
			return nil, false
		}
		if float64(x) < lo || (k != chtype.Int64 && float64(x) > hi) {
			return nil, false
		}
		if isSignedKind(k) {
			return x, true
		}
		return uint64(x), true
	case float64:
		if x != math.Trunc(x) || x < lo || x > hi {
			return nil, false
		}
		if isSignedKind(k) {
			return int64(x), true
		}
		return uint64(x), true
	}
	return nil, false
}

// numericFromString parses s as a number for comparison with a column of
// numeric type t, like ClickHouse: integer types require integer syntax
// (and no sign for unsigned types); the value is not range-checked
// (out-of-range values simply never compare equal).
func numericFromString(s string, t chtype.Type) (any, error) {
	bad := func() error {
		return cherr.New(cherr.TypeMismatch, "Cannot convert string '%s' to type %s", s, t.String())
	}
	if isFloatKind(t.Kind) {
		switch strings.ToLower(s) {
		case "inf", "+inf", "infinity":
			return math.Inf(1), nil
		case "-inf", "-infinity":
			return math.Inf(-1), nil
		case "nan", "+nan", "-nan":
			return math.NaN(), nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return nil, bad()
		}
		return f, nil
	}
	if t.Kind == chtype.Bool {
		switch strings.ToLower(s) {
		case "true", "1":
			return uint64(1), nil
		case "false", "0":
			return uint64(0), nil
		}
		return nil, bad()
	}
	if isSignedKind(t.Kind) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, bad()
		}
		return n, nil
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(s, "+"), 10, 64)
	if err != nil {
		return nil, bad()
	}
	return n, nil
}

// zeroValue is ClickHouse's default value of t. It differs from chtype.Zero
// only for Date32, whose default is its minimum (1900-01-01).
func zeroValue(t chtype.Type) any {
	switch t.Kind {
	case chtype.Date32:
		return time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	case chtype.LowCardinality, chtype.SimpleAggregateFunction:
		return zeroValue(*t.Elem)
	case chtype.Tuple:
		out := make([]any, len(t.Fields))
		for i, f := range t.Fields {
			out[i] = zeroValue(f.Type)
		}
		return out
	}
	return chtype.Zero(t)
}

// supertypeError reports a chtype.Supertype failure. ClickHouse surfaces
// the supertype exception itself (NO_COMMON_TYPE, 386), so a coded error is
// returned unchanged; any other error is reported as NO_COMMON_TYPE.
func supertypeError(err error, format string, args ...any) error {
	var ce *cherr.Error
	if errors.As(err, &ce) && ce.Code != 0 && ce.Code != cherr.StdException {
		return ce
	}
	return cherr.Wrap(cherr.NoCommonType, err, format, args...)
}
