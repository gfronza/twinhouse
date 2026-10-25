package chtype

import (
	"strings"
)

func unwrapType(t Type) Type {
	for {
		switch t.Kind {
		case Nullable, LowCardinality, SimpleAggregateFunction:
			if t.Elem == nil {
				return t
			}
			t = *t.Elem
		default:
			return t
		}
	}
}

func isNullable(t Type) bool {
	switch t.Kind {
	case Nullable:
		return true
	case LowCardinality:
		return t.Elem != nil && t.Elem.Kind == Nullable
	}
	return false
}

// typeEqual is structural equality: same constructor, parameters, timezone,
// tuple element names and enum members (enum declaration order is
// irrelevant, as in ClickHouse which keeps members sorted by value).
func typeEqual(a, b Type) bool {
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case FixedString:
		return a.N == b.N
	case DateTime:
		return a.TZ == b.TZ
	case DateTime64:
		return a.Precision == b.Precision && a.TZ == b.TZ
	case Enum8, Enum16:
		ea, eb := sortedEnumValues(a.EnumValues), sortedEnumValues(b.EnumValues)
		if len(ea) != len(eb) {
			return false
		}
		for i := range ea {
			if ea[i] != eb[i] {
				return false
			}
		}
		return true
	case Nullable, LowCardinality, Array:
		return ptrTypeEqual(a.Elem, b.Elem)
	case Map:
		return ptrTypeEqual(a.Key, b.Key) && ptrTypeEqual(a.Elem, b.Elem)
	case Tuple:
		if len(a.Fields) != len(b.Fields) {
			return false
		}
		for i := range a.Fields {
			if a.Fields[i].Name != b.Fields[i].Name || !typeEqual(a.Fields[i].Type, b.Fields[i].Type) {
				return false
			}
		}
		return true
	case SimpleAggregateFunction:
		return a.AggFunc == b.AggFunc && ptrTypeEqual(a.Elem, b.Elem)
	case AggregateFunction:
		if a.AggFunc != b.AggFunc || len(a.AggArgs) != len(b.AggArgs) || len(a.AggParams) != len(b.AggParams) {
			return false
		}
		for i := range a.AggParams {
			if formatLiteralParam(a.AggParams[i]) != formatLiteralParam(b.AggParams[i]) {
				return false
			}
		}
		for i := range a.AggArgs {
			if !typeEqual(a.AggArgs[i], b.AggArgs[i]) {
				return false
			}
		}
		return true
	}
	return true
}

func ptrTypeEqual(a, b *Type) bool {
	if a == nil || b == nil {
		return a == b
	}
	return typeEqual(*a, *b)
}

func isNumeric(k Kind) bool {
	switch k {
	case UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64:
		return true
	}
	return false
}

func isInteger(k Kind) bool {
	switch k {
	case UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64:
		return true
	}
	return false
}

func isSigned(k Kind) bool {
	switch k {
	case Int8, Int16, Int32, Int64, Float32, Float64:
		return true
	}
	return false
}

// intBits returns the width of an integer kind (Bool counts as 8 bits).
func intBits(k Kind) int {
	switch k {
	case UInt8, Int8, Bool:
		return 8
	case UInt16, Int16:
		return 16
	case UInt32, Int32:
		return 32
	case UInt64, Int64:
		return 64
	}
	return 0
}

const zeroUUID = "00000000-0000-0000-0000-000000000000"

func zeroValue(t Type) any {
	switch t.Kind {
	case Bool:
		return false
	case UInt8, UInt16, UInt32, UInt64:
		return uint64(0)
	case Int8, Int16, Int32, Int64:
		return int64(0)
	case Float32, Float64:
		return float64(0)
	case String:
		return ""
	case FixedString:
		return strings.Repeat("\x00", t.N)
	case UUID:
		return zeroUUID
	case Date32:
		return minDate32 // defaultValueOfTypeName('Date32') = 1900-01-01
	case Date, DateTime, DateTime64:
		return Epoch
	case Enum8, Enum16:
		// ClickHouse's default enum value is the member with the smallest value.
		if s := sortedEnumValues(t.EnumValues); len(s) > 0 {
			return s[0].Name
		}
		return ""
	case Nullable, Nothing:
		return nil
	case LowCardinality, SimpleAggregateFunction:
		if t.Elem == nil {
			return nil
		}
		return zeroValue(*t.Elem)
	case Array, Map:
		return []any{}
	case Tuple:
		out := make([]any, len(t.Fields))
		for i, f := range t.Fields {
			out[i] = zeroValue(f.Type)
		}
		return out
	case AggregateFunction:
		return newAggPartial(t)
	}
	return nil
}

// aggCombinatorSuffixes are stripped (repeatedly) to find the base function
// whose state an AggregateFunction column holds: the -If combinator only
// filters rows, -State/-Merge/-MergeState produce/consume the same state.
var aggCombinatorSuffixes = []string{"MergeState", "Merge", "State", "If"}

// BaseAggFunc strips -If/-State/-Merge combinators from an aggregate name:
// "uniqExactIf" -> "uniqExact", "sumMergeState" -> "sum". Unknown suffixes
// are left alone.
func BaseAggFunc(name string) string {
	for {
		stripped := false
		for _, suf := range aggCombinatorSuffixes {
			if len(name) > len(suf) && strings.HasSuffix(name, suf) {
				name = name[:len(name)-len(suf)]
				stripped = true
				break
			}
		}
		if !stripped {
			return canonicalAggName(name)
		}
	}
}

func newAggPartial(t Type) *AggPartial {
	args := t.AggArgs
	base := BaseAggFunc(t.AggFunc)
	if strings.HasSuffix(t.AggFunc, "If") && len(args) > 0 {
		args = args[:len(args)-1] // drop the -If condition argument
	}
	p := &AggPartial{Func: base, ArgTypes: append([]Type(nil), args...), SetLoaded: true}
	switch base {
	case "sum":
		if len(args) > 0 {
			switch u := args[0].Unwrap(); {
			case u.Kind == Float32 || u.Kind == Float64:
				p.Sum = float64(0)
			case isSigned(u.Kind):
				p.Sum = int64(0)
			default:
				p.Sum = uint64(0)
			}
		} else {
			p.Sum = uint64(0)
		}
	case "avg":
		p.Sum = float64(0)
	}
	return p
}
