package chtype

import (
	"math"
	"strings"
)

// supertype implements ClickHouse's getLeastSupertype.
func supertype(ts ...Type) (Type, error) {
	switch len(ts) {
	case 0:
		return TNothing, nil
	case 1:
		return ts[0], nil
	}
	// Trivial case: all types equivalent (DateTime ignores the timezone,
	// Bool and UInt8 are interchangeable): ClickHouse returns the first.
	allEq := true
	for _, t := range ts[1:] {
		if !chEquivalent(ts[0], t) {
			allEq = false
			break
		}
	}
	if allEq {
		return ts[0], nil
	}

	// Nothing is the identity element.
	nonNothing := make([]Type, 0, len(ts))
	for _, t := range ts {
		if t.Kind != Nothing {
			nonNothing = append(nonNothing, t)
		}
	}
	if len(nonNothing) < len(ts) {
		return supertype(nonNothing...)
	}

	noCommon := func(why string) (Type, error) {
		names := make([]string, len(ts))
		for i, t := range ts {
			names[i] = t.String()
		}
		return Type{}, errf(codeNoCommonType, "There is no supertype for types %s because %s", strings.Join(names, ", "), why)
	}

	// Arrays.
	if n := countKind(ts, Array); n > 0 {
		if n != len(ts) {
			return noCommon("some of them are Array and some of them are not")
		}
		elems := make([]Type, len(ts))
		for i, t := range ts {
			elems[i] = *t.Elem
		}
		e, err := supertype(elems...)
		if err != nil {
			return Type{}, err
		}
		return NewArray(e), nil
	}

	// Tuples.
	if n := countKind(ts, Tuple); n > 0 {
		if n != len(ts) {
			return noCommon("some of them are Tuple and some of them are not")
		}
		size := len(ts[0].Fields)
		sameNames := true
		for _, t := range ts[1:] {
			if len(t.Fields) != size {
				return noCommon("Tuples have different sizes")
			}
			for i := range t.Fields {
				if t.Fields[i].Name != ts[0].Fields[i].Name {
					sameNames = false
				}
			}
		}
		out := Type{Kind: Tuple, Fields: make([]Field, size)}
		for i := 0; i < size; i++ {
			col := make([]Type, len(ts))
			for j, t := range ts {
				col[j] = t.Fields[i].Type
			}
			ft, err := supertype(col...)
			if err != nil {
				return Type{}, err
			}
			out.Fields[i].Type = ft
			if sameNames {
				out.Fields[i].Name = ts[0].Fields[i].Name
			}
		}
		return out, nil
	}

	// Maps.
	if n := countKind(ts, Map); n > 0 {
		if n != len(ts) {
			return noCommon("some of them are Maps and some of them are not")
		}
		keys := make([]Type, len(ts))
		vals := make([]Type, len(ts))
		for i, t := range ts {
			keys[i], vals[i] = *t.Key, *t.Elem
		}
		k, err := supertype(keys...)
		if err != nil {
			return Type{}, err
		}
		v, err := supertype(vals...)
		if err != nil {
			return Type{}, err
		}
		return Type{Kind: Map, Key: &k, Elem: &v}, nil
	}

	// LowCardinality: all LowCardinality keeps it, a mix drops it.
	if n := countKind(ts, LowCardinality); n > 0 {
		nested := make([]Type, len(ts))
		for i, t := range ts {
			if t.Kind == LowCardinality {
				nested[i] = *t.Elem
			} else {
				nested[i] = t
			}
		}
		st, err := supertype(nested...)
		if err != nil {
			return Type{}, err
		}
		if n == len(ts) && canBeInsideLowCardinality(unwrapNullable(st)) {
			return NewLowCardinality(st), nil
		}
		return st, nil
	}

	// Nullable.
	if n := countKind(ts, Nullable); n > 0 {
		nested := make([]Type, 0, len(ts))
		for _, t := range ts {
			if t.Kind == Nullable {
				nested = append(nested, *t.Elem)
			} else {
				nested = append(nested, t)
			}
		}
		st, err := supertype(nested...)
		if err != nil {
			return Type{}, err
		}
		if !canBeInsideNullable(st) {
			return noCommon("the common type " + st.String() + " cannot be inside Nullable")
		}
		return NewNullable(st), nil
	}

	// SimpleAggregateFunction behaves as its value type.
	if n := countKind(ts, SimpleAggregateFunction); n > 0 {
		nested := make([]Type, len(ts))
		for i, t := range ts {
			nested[i] = t.Unwrap()
		}
		return supertype(nested...)
	}

	// Strings (Enums join Strings as String).
	nStr := countKind(ts, String) + countKind(ts, FixedString)
	nEnum := countKind(ts, Enum8) + countKind(ts, Enum16)
	if nStr > 0 {
		if nStr+nEnum != len(ts) {
			return noCommon("some of them are String/FixedString/Enum and some of them are not")
		}
		return TString, nil
	}

	// Dates and times.
	nDate, nDate32 := countKind(ts, Date), countKind(ts, Date32)
	nDT, nDT64 := countKind(ts, DateTime), countKind(ts, DateTime64)
	if nDate+nDate32+nDT+nDT64 > 0 {
		if nDate+nDate32+nDT+nDT64 != len(ts) {
			return noCommon("some of them are Date/Date32/DateTime/DateTime64 and some of them are not")
		}
		if nDT64 == 0 && nDate32 == 0 {
			for _, t := range ts {
				if t.Kind == DateTime {
					return t, nil
				}
			}
			return TDate, nil
		}
		if nDT == 0 && nDT64 == 0 {
			return Type{Kind: Date32}, nil
		}
		best := Type{Kind: DateTime64}
		found := false
		for _, t := range ts {
			if t.Kind == DateTime64 && (!found || t.Precision > best.Precision) {
				best, found = t, true
			}
		}
		return best, nil // Date32 + DateTime -> DateTime64(0)
	}

	// Numbers (Bool counts as UInt8, Enum8/16 as Int8/16).
	maxSigned, maxUnsigned, maxMantissa := 0, 0, 0
	allNumbers := true
	for _, t := range ts {
		switch t.Kind {
		case Bool, UInt8, UInt16, UInt32, UInt64:
			maxUnsigned = max(maxUnsigned, intBits(t.Kind))
		case Int8, Enum8:
			maxSigned = max(maxSigned, 8)
		case Int16, Enum16:
			maxSigned = max(maxSigned, 16)
		case Int32:
			maxSigned = max(maxSigned, 32)
		case Int64:
			maxSigned = max(maxSigned, 64)
		case Float32:
			maxMantissa = max(maxMantissa, 24)
		case Float64:
			maxMantissa = max(maxMantissa, 53)
		default:
			allNumbers = false
		}
	}
	if maxSigned+maxUnsigned+maxMantissa > 0 {
		if !allNumbers {
			return noCommon("some of them are numbers and some of them are not")
		}
		minBits := max(maxSigned, maxUnsigned)
		if maxSigned > 0 && maxUnsigned >= maxSigned {
			if minBits == 64 {
				return noCommon("some of them are signed integers and some are unsigned integers, but there is no signed integer type, that can exactly represent all required unsigned integer values")
			}
			minBits++ // a signed type one bit wider is needed
		}
		if maxMantissa > 0 {
			switch m := max(minBits, maxMantissa); {
			case m <= 24:
				return TFloat32, nil
			case m <= 53:
				return TFloat64, nil
			}
			return noCommon("some of them are integers and some are floating point, but there is no floating point type, that can exactly represent all required integers")
		}
		if maxSigned > 0 {
			switch {
			case minBits <= 8:
				return TInt8, nil
			case minBits <= 16:
				return TInt16, nil
			case minBits <= 32:
				return TInt32, nil
			}
			return TInt64, nil
		}
		switch {
		case minBits <= 8:
			return TUInt8, nil
		case minBits <= 16:
			return TUInt16, nil
		case minBits <= 32:
			return TUInt32, nil
		}
		return TUInt64, nil
	}
	return noCommon("they are not compatible")
}

// chEquivalent mirrors ClickHouse IDataType::equals as used by
// getLeastSupertype's trivial case.
func chEquivalent(a, b Type) bool {
	// SimpleAggregateFunction is a custom name over its value type.
	if a.Kind == SimpleAggregateFunction {
		a = *a.Elem
	}
	if b.Kind == SimpleAggregateFunction {
		b = *b.Elem
	}
	isB8 := func(k Kind) bool { return k == Bool || k == UInt8 }
	if isB8(a.Kind) && isB8(b.Kind) {
		return true
	}
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case DateTime:
		return true
	case DateTime64:
		return a.Precision == b.Precision
	case Nullable, LowCardinality, Array:
		return chEquivalent(*a.Elem, *b.Elem)
	case Map:
		return chEquivalent(*a.Key, *b.Key) && chEquivalent(*a.Elem, *b.Elem)
	case Tuple:
		if len(a.Fields) != len(b.Fields) {
			return false
		}
		for i := range a.Fields {
			if a.Fields[i].Name != b.Fields[i].Name || !chEquivalent(a.Fields[i].Type, b.Fields[i].Type) {
				return false
			}
		}
		return true
	}
	return typeEqual(a, b)
}

func countKind(ts []Type, k Kind) int {
	n := 0
	for _, t := range ts {
		if t.Kind == k {
			n++
		}
	}
	return n
}

func unwrapNullable(t Type) Type {
	if t.Kind == Nullable {
		return *t.Elem
	}
	return t
}

// inferLiteral implements InferLiteral.
func inferLiteral(v any, isTuple bool) Type {
	switch x := v.(type) {
	case nil:
		return NewNullable(TNothing)
	case bool:
		return TBool
	case string, []byte:
		return TString
	case float64, float32:
		return TFloat64
	case uint64:
		return smallestUnsigned(x)
	case int64:
		return smallestSignedOrUnsigned(x)
	case []any:
		elems := make([]Type, len(x))
		for i, e := range x {
			elems[i] = inferLiteral(e, false)
		}
		if isTuple {
			fs := make([]Field, len(elems))
			for i, e := range elems {
				fs[i] = Field{Type: e}
			}
			return NewTuple(fs...)
		}
		st, err := supertype(elems...)
		if err != nil {
			// ClickHouse types array literals from their values: an unsigned
			// literal that fits Int64 joins negative literals as Int64.
			retyped := false
			for i, e := range x {
				if u, ok := e.(uint64); ok && u > math.MaxUint32 && u <= math.MaxInt64 {
					elems[i], retyped = TInt64, true
				}
			}
			if !retyped {
				return Type{}
			}
			if st, err = supertype(elems...); err != nil {
				return Type{}
			}
		}
		return NewArray(st)
	}
	if i, ok := goInt(v); ok {
		return smallestSignedOrUnsigned(i)
	}
	if u, ok := goUint(v); ok {
		return smallestUnsigned(u)
	}
	return Type{}
}

func smallestUnsigned(u uint64) Type {
	switch {
	case u <= math.MaxUint8:
		return TUInt8
	case u <= math.MaxUint16:
		return TUInt16
	case u <= math.MaxUint32:
		return TUInt32
	}
	return TUInt64
}

func smallestSignedOrUnsigned(i int64) Type {
	if i >= 0 {
		return smallestUnsigned(uint64(i))
	}
	switch {
	case i >= math.MinInt8:
		return TInt8
	case i >= math.MinInt16:
		return TInt16
	case i >= math.MinInt32:
		return TInt32
	}
	return TInt64
}
