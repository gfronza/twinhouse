package chtype

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// convert implements Convert (CAST(v AS to), v of type from).
func convert(v any, from, to Type) (any, error) {
	if v == nil {
		if to.IsNullable() || to.Kind == Nothing {
			return nil, nil
		}
		return nil, errf(cherr.CannotConvertType, "Cannot convert NULL value to non-Nullable type %s", to.String())
	}
	fu := from.Unwrap()
	if from.Kind == Invalid {
		fu = inferLiteral(v, false).Unwrap()
	}
	tu := to.Unwrap()
	switch tu.Kind {
	case Nothing:
		return nil, errf(cherr.CannotConvertType, "Cannot convert %s to Nothing", from.String())
	case AggregateFunction:
		if fu.Kind == AggregateFunction && typeEqual(fu, tu) {
			return v, nil
		}
		return nil, errf(cherr.TypeMismatch, "Conversion from %s to %s is not supported", from.String(), to.String())
	case String:
		return valueToString(fu, v)
	case FixedString:
		if fu.Kind != String && fu.Kind != FixedString {
			return nil, errf(cherr.NotImplemented, "CAST AS FixedString is only implemented for types String and FixedString")
		}
		s := toStr(v)
		if len(s) > tu.N {
			return nil, errf(codeTooLargeStringSize, "String too long for type %s", tu.String())
		}
		return padFixedString(s, tu.N)
	case Array:
		if fu.Kind == Map {
			// ClickHouse casts a Map through its nested Array(Tuple(K, V)).
			fu = mapAsArray(fu)
		}
		switch fu.Kind {
		case Array:
			if fu.Elem.Kind != Nothing && arrayDims(fu) != arrayDims(tu) {
				return nil, errf(cherr.TypeMismatch, "CAST AS Array can only be performed between same-dimensional array types")
			}
			in := asSlice(v)
			out := make([]any, len(in))
			for i, e := range in {
				c, err := convert(e, *fu.Elem, *tu.Elem)
				if err != nil {
					return nil, err
				}
				out[i] = c
			}
			return out, nil
		case String, FixedString:
			return parseCompositeString(toStr(v), tu)
		}
		return nil, errf(cherr.TypeMismatch, "CAST AS Array can only be performed between same-dimensional Array, Map or String types")
	case Tuple:
		switch fu.Kind {
		case Tuple:
			in := asSlice(v)
			if len(fu.Fields) != len(tu.Fields) || len(in) != len(tu.Fields) {
				return nil, errf(cherr.TypeMismatch, "CAST AS Tuple can only be performed between tuple types with the same number of elements or from String. Left type: %s, right type: %s", from.String(), to.String())
			}
			out := make([]any, len(in))
			for i, e := range in {
				c, err := convert(e, fu.Fields[i].Type, tu.Fields[i].Type)
				if err != nil {
					return nil, err
				}
				out[i] = c
			}
			return out, nil
		case String, FixedString:
			return parseCompositeString(toStr(v), tu)
		}
		return nil, errf(cherr.TypeMismatch, "CAST AS Tuple can only be performed between tuple types or from String. Left type: %s, right type: %s", from.String(), to.String())
	case Map:
		var in []any
		var fk, fv Type
		switch fu.Kind {
		case Map:
			in, fk, fv = asSlice(v), *fu.Key, *fu.Elem
		case Array:
			// Array(Tuple(K, V)) -> Map.
			e := fu.Elem.Unwrap()
			switch {
			case e.Kind == Nothing:
				fk, fv = TNothing, TNothing
			case e.Kind == Tuple && len(e.Fields) == 2:
				fk, fv = e.Fields[0].Type, e.Fields[1].Type
			default:
				return nil, errf(cherr.TypeMismatch, "CAST AS Map from array requires nested tuple of 2 elements. Left type: %s, right type: %s", from.String(), to.String())
			}
			in = asSlice(v)
		case Tuple:
			// Tuple(Array(K), Array(V)) -> Map.
			if len(fu.Fields) != 2 || fu.Fields[0].Type.Unwrap().Kind != Array || fu.Fields[1].Type.Unwrap().Kind != Array {
				return nil, errf(cherr.TypeMismatch, "CAST AS Map can only be performed from tuples of array. Got: %s", from.String())
			}
			parts := asSlice(v)
			keys, vals := asSlice(parts[0]), asSlice(parts[1])
			if len(keys) != len(vals) {
				return nil, errf(cherr.TypeMismatch, "CAST AS Map can only be performed from tuple of arrays with equal sizes")
			}
			in = make([]any, len(keys))
			for i := range keys {
				in[i] = []any{keys[i], vals[i]}
			}
			fk, fv = *fu.Fields[0].Type.Unwrap().Elem, *fu.Fields[1].Type.Unwrap().Elem
		default:
			return nil, errf(cherr.TypeMismatch, "Unsupported types to CAST AS Map. Left type: %s, right type: %s", from.String(), to.String())
		}
		out := make([]any, len(in))
		for i, kv := range in {
			k, val := mapPair(kv)
			ck, err := convert(k, fk, *tu.Key)
			if err != nil {
				return nil, err
			}
			cv, err := convert(val, fv, *tu.Elem)
			if err != nil {
				return nil, err
			}
			out[i] = []any{ck, cv}
		}
		return out, nil
	}

	// Scalar targets.
	switch fu.Kind {
	case String, FixedString:
		s := toStr(v)
		if fu.Kind == FixedString {
			s = strings.TrimRight(s, "\x00")
		}
		return convertFromString(s, tu)
	case Array, Tuple, Map, AggregateFunction:
		return nil, compositeToScalarErr(from, to, tu)
	case UUID:
		if tu.Kind == UUID {
			return toStr(v), nil
		}
		return nil, errf(cherr.NotImplemented, "Conversion between UUID and %s is not supported", to.String())
	case Enum8, Enum16:
		name := toStr(v)
		switch {
		case tu.Kind == Enum8 || tu.Kind == Enum16:
			// ClickHouse converts enums by value and refuses mappings in
			// which a shared element name would change its value.
			for _, ev := range fu.EnumValues {
				if tv, ok := enumValueOf(tu, ev.Name); ok && tv != ev.Value {
					return nil, errf(cherr.CannotConvertType, "Enum conversion changes value for element '%s' from %d to %d", ev.Name, ev.Value, tv)
				}
			}
			iv, ok := enumValueOf(fu, name)
			if !ok {
				return nil, errf(codeUnknownElementOfEnum, "Unknown element '%s' for enum", name)
			}
			return enumByValue(tu, iv)
		case isNumeric(tu.Kind) || tu.Kind == Bool:
			iv, ok := enumValueOf(fu, name)
			if !ok {
				return nil, errf(codeUnknownElementOfEnum, "Unknown element '%s' for enum", name)
			}
			return convertNumber(iv, tu.Kind)
		}
		return nil, errf(cherr.TypeMismatch, "Conversion from %s to %s is not supported", from.String(), to.String())
	case Date, Date32, DateTime, DateTime64:
		return convertFromTime(asTime(v), fu, tu)
	}

	// fu is numeric or Bool.
	num, err := numericValue(v)
	if err != nil {
		return nil, err
	}
	switch tu.Kind {
	case Bool, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64:
		return convertNumber(num, tu.Kind)
	case Enum8, Enum16:
		iv, ok := numberAsInt64(num)
		if !ok {
			return nil, errf(codeUnknownElementOfEnum, "Unexpected value %v in enum", num)
		}
		return enumByValue(tu, iv)
	case Date, Date32:
		return numberToDate(num, tu.Kind)
	case DateTime:
		return numberToDateTime(num)
	case DateTime64:
		return numberToDateTime64(num, tu.Precision)
	case UUID:
		return nil, errf(cherr.NotImplemented, "Conversion between numeric types and UUID is not supported. Probably the passed UUID is unquoted")
	}
	return nil, errf(cherr.TypeMismatch, "Conversion from %s to %s is not supported", from.String(), to.String())
}

// compositeToScalarErr is ClickHouse's error for CAST of an Array, Tuple,
// Map or AggregateFunction value to a scalar type (tu is to unwrapped):
// ILLEGAL_TYPE_OF_ARGUMENT from the toX conversion function, or
// CANNOT_CONVERT_TYPE for enums and DateTime64 (ClickHouse 25.8).
func compositeToScalarErr(from, to, tu Type) error {
	switch tu.Kind {
	case Enum8, Enum16, DateTime64:
		return errf(cherr.CannotConvertType, "Conversion from %s to %s is not supported", from.String(), to.String())
	case Bool:
		tu = TUInt8 // Bool is a custom name over UInt8 (toUInt8 in the message)
	}
	return errf(cherr.IllegalTypeOfArgument, "Illegal type %s of argument of function to%s", from.String(), kindName(tu.Kind))
}

// mapAsArray returns Map(K, V)'s nested representation Array(Tuple(K, V)).
func mapAsArray(m Type) Type {
	e := Type{Kind: Tuple, Fields: []Field{{Type: *m.Key}, {Type: *m.Elem}}}
	return Type{Kind: Array, Elem: &e}
}

// arrayDims counts directly nested Array levels (ClickHouse's
// getNumberOfDimensions).
func arrayDims(t Type) int {
	n := 0
	for t.Kind == Array && t.Elem != nil {
		n++
		t = *t.Elem
	}
	return n
}

func errCodeOf(err error) int {
	var ce *cherr.Error
	if errors.As(err, &ce) {
		return ce.Code
	}
	return 0
}

func parseCompositeString(s string, t Type) (any, error) {
	p := &quotedParser{s: s, saturate: true}
	v, err := p.parseValue(t)
	if err != nil {
		return nil, err
	}
	if p.pos != len(s) {
		return nil, errf(cherr.CannotParseText, "Cannot parse string '%s' as %s: syntax error at position %d", s, t.String(), p.pos)
	}
	return v, nil
}

// numericValue canonicalizes a numeric or Bool value for convertNumber.
func numericValue(v any) (any, error) {
	switch x := v.(type) {
	case uint64, int64, float64, bool:
		return x, nil
	case float32:
		return float64(x), nil
	}
	if i, ok := goInt(v); ok {
		return i, nil
	}
	if u, ok := goUint(v); ok {
		return u, nil
	}
	return nil, errf(cherr.CannotConvertType, "Cannot convert value of Go type %T", v)
}

// convertNumber converts a uint64/int64/float64/bool to numeric kind k with
// CAST semantics: integers wrap modulo 2^width; floats truncate toward zero
// and saturate at the target range (8/16-bit targets saturate at the 32-bit
// range and then wrap, matching ClickHouse's native conversion); NaN/Inf to
// an integer is an error; anything non-zero is true for Bool.
func convertNumber(v any, k Kind) (any, error) {
	if b, ok := v.(bool); ok {
		if b {
			v = uint64(1)
		} else {
			v = uint64(0)
		}
	}
	switch k {
	case Bool:
		switch x := v.(type) {
		case uint64:
			return x != 0, nil
		case int64:
			return x != 0, nil
		case float64:
			return x != 0, nil // NaN is true, like ClickHouse
		}
	case Float32, Float64:
		var f float64
		switch x := v.(type) {
		case uint64:
			f = float64(x)
		case int64:
			f = float64(x)
		case float64:
			f = x
		}
		if k == Float32 {
			f = float64(float32(f))
		}
		return f, nil
	case UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64:
		switch x := v.(type) {
		case uint64:
			return wrapInt(x, k), nil
		case int64:
			return wrapInt(uint64(x), k), nil
		case float64:
			return floatToInt(x, k)
		}
	}
	return nil, errf(cherr.CannotConvertType, "Cannot convert %v to %s", v, kindName(k))
}

func floatToInt(f float64, k Kind) (any, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, errf(cherr.CannotConvertType, "Unexpected inf or nan to integer conversion")
	}
	f = math.Trunc(f)
	switch k {
	case UInt64:
		switch {
		case f <= 0:
			return uint64(0), nil
		case f >= 18446744073709551616.0:
			return uint64(math.MaxUint64), nil
		}
		return uint64(f), nil
	case Int64:
		switch {
		case f >= 9223372036854775808.0:
			return int64(math.MaxInt64), nil
		case f < -9223372036854775808.0:
			return int64(math.MinInt64), nil
		}
		return int64(f), nil
	case UInt32:
		return uint64(math.Max(0, math.Min(f, math.MaxUint32))), nil
	case Int32:
		return int64(math.Max(math.MinInt32, math.Min(f, math.MaxInt32))), nil
	}
	i := int64(math.Max(math.MinInt32, math.Min(f, math.MaxInt32)))
	return wrapInt(uint64(i), k), nil
}

// convertFromString implements CAST(String AS to) for scalar targets.
func convertFromString(s string, to Type) (any, error) {
	switch to.Kind {
	case String:
		return s, nil
	case FixedString:
		if len(s) > to.N {
			return nil, errf(codeTooLargeStringSize, "String too long for type %s", to.String())
		}
		return padFixedString(s, to.N)
	case UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64:
		switch s {
		case "":
			return nil, errf(codeAttemptToReadAfterEOF, "Attempt to read after eof: Cannot parse %s from String, because value is too short", to.String())
		case "-", "+":
			return nil, errf(codeCannotParseNumber, "Cannot parse number without any digits: Cannot parse %s from String", to.String())
		}
		v, err := parseIntText(s, to.Kind, false)
		if err != nil {
			if errCodeOf(err) == codeCannotParseNumber {
				return nil, errf(codeCannotParseNumber, "Unsigned type must not contain '-' symbol: Cannot parse %s from String", to.String())
			}
			return nil, errf(cherr.CannotParseText, "Cannot parse string '%s' as %s", s, to.String())
		}
		return v, nil
	case Float32, Float64:
		if s == "" {
			return nil, errf(codeCannotParseNumber, "Cannot read floating point value: Cannot parse %s from String", to.String())
		}
		if s == "-" || s == "+" {
			return nil, errf(codeCannotParseNumber, "Cannot read floating point value: no digits read: Cannot parse %s from String", to.String())
		}
		v, err := parseFloatText(s, to.Kind)
		if err != nil {
			if danglingExponent(s) {
				return nil, errf(codeCannotParseNumber, "Cannot read floating point value: nothing after exponent: Cannot parse %s from String", to.String())
			}
			return nil, errf(cherr.CannotParseText, "Cannot parse string '%s' as %s", s, to.String())
		}
		return v, nil
	case Bool:
		return parseBoolText(s)
	case UUID:
		return parseUUID(s)
	case Enum8, Enum16:
		return enumFromText(to, s)
	case Date, Date32:
		v, err := parseDateText(s, to.Kind, true)
		return v, castParseErr(err, to)
	case DateTime:
		v, err := parseDateTimeTextSat(s, locationOf(to), true)
		return v, castParseErr(err, to)
	case DateTime64:
		if s == "" {
			return Epoch, nil // CAST('' AS DateTime64(p)) is the epoch in ClickHouse
		}
		v, err := parseDateTime64Text(s, to.Precision, locationOf(to))
		return v, castParseErr(err, to)
	case Array, Tuple, Map:
		if to.Kind == Map {
			return nil, errf(cherr.TypeMismatch, "Unsupported types to CAST AS Map. Left type: String, right type: %s", to.String())
		}
		return parseCompositeString(s, to)
	case Nullable, LowCardinality, SimpleAggregateFunction:
		return convertFromString(s, *to.Elem)
	}
	return nil, errf(cherr.TypeMismatch, "Conversion from String to %s is not supported", to.String())
}

// valueToString implements CAST(v AS String) for a value of (unwrapped) type t.
func valueToString(t Type, v any) (any, error) {
	switch t.Kind {
	case String:
		return toStr(v), nil
	case FixedString:
		return strings.TrimRight(toStr(v), "\x00"), nil
	case Array, Tuple, Map:
		return formatQuoted(t, v), nil
	case AggregateFunction:
		return nil, errf(cherr.NotImplemented, "Conversion of %s to String is not supported", t.String())
	case Invalid:
		return nil, errf(cherr.CannotConvertType, "Cannot convert value of Go type %T to String", v)
	}
	return string(appendScalarPlain(nil, t, v)), nil
}

// convertFromTime converts a Date/Date32/DateTime/DateTime64 value.
func convertFromTime(tm time.Time, from, to Type) (any, error) {
	switch to.Kind {
	case Date, Date32:
		d := tm
		if from.Kind == DateTime || from.Kind == DateTime64 {
			d = dateOf(tm, locationOf(from))
		}
		if err := checkDateRange(d, to.Kind); err != nil {
			return nil, err
		}
		return d, nil
	case DateTime:
		t := tm
		if from.Kind == Date || from.Kind == Date32 {
			t = midnightIn(tm, locationOf(to))
		}
		t = t.Truncate(time.Second)
		if err := checkDateTimeRange(t); err != nil {
			return nil, err
		}
		return t, nil
	case DateTime64:
		t := tm
		if from.Kind == Date || from.Kind == Date32 {
			t = midnightIn(tm, locationOf(to))
		}
		t = truncPrecision(t, to.Precision)
		if err := checkDateTime64Range(t); err != nil {
			return nil, err
		}
		return t, nil
	case UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64, Bool:
		switch from.Kind {
		case Date, Date32:
			days := int64(math.Floor(float64(tm.Unix()) / 86400))
			return convertNumber(days, to.Kind)
		case DateTime:
			return convertNumber(tm.Unix(), to.Kind)
		default:
			if to.Kind == Float32 || to.Kind == Float64 {
				f := float64(tm.Unix()) + float64(tm.Nanosecond())/1e9
				return convertNumber(f, to.Kind)
			}
			sec := tm.Unix()
			if sec < 0 && tm.Nanosecond() > 0 {
				sec++ // truncate toward zero
			}
			return convertNumber(sec, to.Kind)
		}
	}
	return nil, errf(cherr.TypeMismatch, "Conversion from %s to %s is not supported", from.String(), to.String())
}

// midnightIn returns 00:00 of the calendar date d in loc as an instant.
func midnightIn(d time.Time, loc *time.Location) time.Time {
	u := d.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, loc).UTC()
}

// danglingExponent reports a float mantissa followed by e/E (and optionally a
// sign) with no exponent digits, e.g. "1e" or "2.5E+": ClickHouse answers
// CANNOT_PARSE_NUMBER (72) for these rather than CANNOT_PARSE_TEXT.
func danglingExponent(s string) bool {
	l := strings.ToLower(s)
	l = strings.TrimSuffix(strings.TrimSuffix(l, "+"), "-")
	if !strings.HasSuffix(l, "e") {
		return false
	}
	m := strings.TrimPrefix(strings.TrimPrefix(l[:len(l)-1], "-"), "+")
	return m != "" && strings.Trim(m, "0123456789.") == "" && strings.Count(m, ".") <= 1 && strings.ContainsAny(m, "0123456789")
}
