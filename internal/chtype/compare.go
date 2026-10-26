package chtype

import (
	"math"
	"strings"
	"time"
)

// Kind ranks for ordering values of incomparable kinds (NULL is handled
// separately by the callers of valueRank).
const (
	rankNumber = iota
	rankString
	rankTime
	rankList
	rankOther
)

func valueRank(v any) int {
	switch v.(type) {
	case bool, uint64, int64, float64, int, int32, int16, int8, uint, uint32, uint16, uint8, float32:
		return rankNumber
	case string, []byte:
		return rankString
	case time.Time:
		return rankTime
	case []any:
		return rankList
	}
	return rankOther
}

// compareTop implements Compare: a top-level NULL sorts first; below the top
// level (array elements, tuple fields, map entries) the ClickHouse
// comparison-operator order applies, i.e. compareValues with hint +1.
func compareTop(a, b any) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	return compareValues(a, b, 1)
}

// compareValues implements CompareHint with ClickHouse's IColumn::compareAt
// semantics at every depth: NULL equals NULL and, like NaN, compares as
// greater than every other value when hint > 0 and as less when hint <= 0,
// with NULL beyond NaN (hint > 0: numbers < NaN < NULL; hint <= 0: NULL <
// NaN < numbers). Numbers compare exactly across uint64/int64/float64 (and
// bool as 0/1); NaN equals NaN; -0 equals 0. Lists compare lexicographically,
// a proper prefix first.
func compareValues(a, b any, hint int) int {
	if hint <= 0 {
		hint = -1
	} else {
		hint = 1
	}
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return hint
	case b == nil:
		return -hint
	}
	ra, rb := valueRank(a), valueRank(b)
	if ra != rb {
		return cmpInt(ra, rb)
	}
	switch ra {
	case rankOther:
		return 0
	case rankNumber:
		return compareNumbers(a, b, hint)
	case rankString:
		return strings.Compare(toStr(a), toStr(b))
	case rankTime:
		return a.(time.Time).Compare(b.(time.Time))
	case rankList:
		x, y := a.([]any), b.([]any)
		for i := 0; i < len(x) && i < len(y); i++ {
			if c := compareValues(x[i], y[i], hint); c != 0 {
				return c
			}
		}
		return cmpInt(len(x), len(y))
	}
	return 0
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// numVal classifies a numeric value as an exact integer (neg, magnitude)
// or a float.
type numVal struct {
	isFloat bool
	f       float64
	neg     bool   // integer sign
	mag     uint64 // integer magnitude
}

func toNumVal(v any) numVal {
	switch x := v.(type) {
	case uint64:
		return numVal{mag: x}
	case int64:
		if x < 0 {
			return numVal{neg: true, mag: uint64(-x)}
		}
		return numVal{mag: uint64(x)}
	case float64:
		return numVal{isFloat: true, f: x}
	case float32:
		return numVal{isFloat: true, f: float64(x)}
	case bool:
		if x {
			return numVal{mag: 1}
		}
		return numVal{}
	}
	if i, ok := goInt(v); ok {
		return toNumVal(i)
	}
	if u, ok := goUint(v); ok {
		return toNumVal(u)
	}
	return numVal{isFloat: true, f: math.NaN()}
}

// compareNumbers compares two numbers; NaN equals NaN and sorts after every
// other number when hint > 0, before when hint < 0.
func compareNumbers(a, b any, hint int) int {
	x, y := toNumVal(a), toNumVal(b)
	xn, yn := x.isFloat && math.IsNaN(x.f), y.isFloat && math.IsNaN(y.f)
	switch {
	case xn && yn:
		return 0
	case xn:
		return hint
	case yn:
		return -hint
	case !x.isFloat && !y.isFloat:
		return cmpExactInts(x, y)
	case x.isFloat && y.isFloat:
		return cmpFloats(x.f, y.f)
	case x.isFloat:
		return -cmpIntFloat(y, x.f)
	default:
		return cmpIntFloat(x, y.f)
	}
}

func cmpExactInts(x, y numVal) int {
	if x.mag == 0 {
		x.neg = false
	}
	if y.mag == 0 {
		y.neg = false
	}
	switch {
	case x.neg && !y.neg:
		return -1
	case !x.neg && y.neg:
		return 1
	}
	c := 0
	switch {
	case x.mag < y.mag:
		c = -1
	case x.mag > y.mag:
		c = 1
	}
	if x.neg {
		return -c
	}
	return c
}

func cmpFloats(a, b float64) int {
	an, bn := math.IsNaN(a), math.IsNaN(b)
	switch {
	case an && bn:
		return 0
	case an:
		return 1
	case bn:
		return -1
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// cmpIntFloat compares an exact integer with a float without precision loss.
func cmpIntFloat(x numVal, f float64) int {
	if math.IsNaN(f) {
		return -1
	}
	if math.IsInf(f, 1) {
		return -1
	}
	if math.IsInf(f, -1) {
		return 1
	}
	fneg := f < 0
	if x.mag == 0 {
		x.neg = false
	}
	if f == 0 {
		if x.mag == 0 {
			return 0
		}
		if x.neg {
			return -1
		}
		return 1
	}
	if x.neg != fneg {
		if x.neg {
			return -1
		}
		return 1
	}
	af := math.Abs(f)
	var c int
	if af >= 18446744073709551616.0 {
		c = -1
	} else {
		ip, frac := math.Modf(af)
		fi := uint64(ip)
		switch {
		case x.mag < fi:
			c = -1
		case x.mag > fi:
			c = 1
		case frac > 0:
			c = -1
		}
	}
	if x.neg {
		return -c
	}
	return c
}
