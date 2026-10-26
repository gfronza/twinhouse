package dynamo

import (
	"math"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// compareValues orders two canonical values of one column type with
// chtype.Compare's documented semantics (nil first, numbers by value, strings
// bytewise, times by instant, bool false<true, []any lexicographically). NaN
// compares equal to everything so it never displaces a stored extremum.
func compareValues(a, b any) int {
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
		if y, ok := b.(bool); ok {
			switch {
			case x == y:
				return 0
			case !x:
				return -1
			}
			return 1
		}
	case uint64, int64, float64:
		return compareNumbers(a, b)
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y)
		}
	case time.Time:
		if y, ok := b.(time.Time); ok {
			return x.Compare(y)
		}
	case []any:
		if y, ok := b.([]any); ok {
			for i := 0; i < len(x) && i < len(y); i++ {
				if c := compareValues(x[i], y[i]); c != 0 {
					return c
				}
			}
			switch {
			case len(x) < len(y):
				return -1
			case len(x) > len(y):
				return 1
			}
			return 0
		}
	}
	return 0
}

func compareNumbers(a, b any) int {
	switch x := a.(type) {
	case uint64:
		switch y := b.(type) {
		case uint64:
			return cmpOrdered(x, y)
		case int64:
			if y < 0 {
				return 1
			}
			return cmpOrdered(x, uint64(y))
		}
	case int64:
		switch y := b.(type) {
		case int64:
			return cmpOrdered(x, y)
		case uint64:
			if x < 0 {
				return -1
			}
			return cmpOrdered(uint64(x), y)
		}
	}
	fa, fb := toFloat(a), toFloat(b)
	if math.IsNaN(fa) || math.IsNaN(fb) {
		return 0
	}
	return cmpOrdered(fa, fb)
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
	return math.NaN()
}

func cmpOrdered[T uint64 | int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// sameValue reports exact equality of two canonical values (floats by bits,
// all NaNs equal), used to match rows for deletion.
func sameValue(a, b any) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case float64:
		y, ok := b.(float64)
		return ok && (math.Float64bits(x) == math.Float64bits(y) || math.IsNaN(x) && math.IsNaN(y))
	case time.Time:
		y, ok := b.(time.Time)
		return ok && x.Equal(y)
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !sameValue(x[i], y[i]) {
				return false
			}
		}
		return true
	case *chtype.AggPartial:
		return true // states are not compared; keys identify merged rows
	case uint64, int64:
		return compareNumbers(a, b) == 0 && isInteger(b)
	}
	return a == b
}

func isInteger(v any) bool {
	switch v.(type) {
	case uint64, int64:
		return true
	}
	return false
}
