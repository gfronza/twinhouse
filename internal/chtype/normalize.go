package chtype

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"dynamodb-eventstream/internal/cherr"
)

// normalize implements Normalize.
func normalize(t Type, v any) (any, error) {
	if v == nil {
		if t.IsNullable() || t.Kind == Nothing {
			return nil, nil
		}
		// input_format_null_as_default: NULL into a non-Nullable column
		// inserts the type's default.
		return zeroValue(t), nil
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer {
		if p, ok := v.(*AggPartial); ok {
			if t.Kind != AggregateFunction {
				return nil, normErr(t, v)
			}
			return p, nil
		}
		if rv.IsNil() {
			return normalize(t, nil)
		}
		return normalize(t, rv.Elem().Interface())
	}
	switch t.Kind {
	case Nullable, LowCardinality, SimpleAggregateFunction:
		return normalize(*t.Elem, v)
	case Nothing:
		return nil, normErr(t, v)
	case AggregateFunction:
		if p, ok := v.(AggPartial); ok {
			return &p, nil
		}
		return nil, normErr(t, v)
	case Bool:
		return normalizeBool(v)
	case UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64:
		return normalizeInt(t, v)
	case Float32, Float64:
		return normalizeFloat(t, v)
	case String, FixedString:
		var s string
		switch x := v.(type) {
		case string:
			s = x
		case []byte:
			s = string(x)
		case json.RawMessage:
			s = string(x)
		default:
			if st, ok := v.(fmt.Stringer); ok && isUUIDLike(v) {
				s = st.String()
				break
			}
			return nil, normErr(t, v)
		}
		if t.Kind == FixedString {
			return padFixedString(s, t.N)
		}
		return s, nil
	case UUID:
		switch x := v.(type) {
		case string:
			return parseUUID(x)
		case []byte:
			if len(x) == 16 {
				return uuidFromBytes(x), nil
			}
			return parseUUID(string(x))
		case [16]byte:
			return uuidFromBytes(x[:]), nil
		}
		if isUUIDLike(v) {
			b := reflect.ValueOf(v)
			raw := make([]byte, 16)
			for i := range raw {
				raw[i] = byte(b.Index(i).Uint())
			}
			return uuidFromBytes(raw), nil
		}
		return nil, normErr(t, v)
	case Enum8, Enum16:
		switch x := v.(type) {
		case string:
			if _, ok := enumValueOf(t, x); ok {
				return x, nil
			}
			return nil, errf(codeUnknownElementOfEnum, "Unknown element '%s' for enum", x)
		case json.Number:
			return enumFromText(t, x.String())
		}
		if i, ok := goInt(v); ok {
			return enumByValue(t, i)
		}
		if u, ok := goUint(v); ok && u <= math.MaxInt64 {
			return enumByValue(t, int64(u))
		}
		return nil, normErr(t, v)
	case Date, Date32, DateTime, DateTime64:
		return normalizeTime(t, v)
	case Array:
		items, ok := sliceItems(v)
		if !ok {
			if s, isStr := v.(string); isStr {
				return parseCompositeString(s, t)
			}
			return nil, normErr(t, v)
		}
		out := make([]any, len(items))
		for i, e := range items {
			n, err := normalize(*t.Elem, e)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	case Tuple:
		return normalizeTuple(t, v)
	case Map:
		return normalizeMap(t, v)
	}
	return nil, normErr(t, v)
}

func normErr(t Type, v any) error {
	return errf(cherr.TypeMismatch, "Cannot convert value %v of Go type %T to %s", truncForMsg(v), v, t.String())
}

func truncForMsg(v any) string {
	s := fmt.Sprint(v)
	if len(s) > 64 {
		s = s[:64] + "..."
	}
	return s
}

func isUUIDLike(v any) bool {
	rt := reflect.TypeOf(v)
	return rt.Kind() == reflect.Array && rt.Len() == 16 && rt.Elem().Kind() == reflect.Uint8
}

func uuidFromBytes(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, hexd[c>>4], hexd[c&0xf])
	}
	return string(out)
}

func normalizeBool(v any) (any, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case string:
		return parseBoolText(x)
	case json.Number:
		return parseBoolText(x.String())
	}
	if i, ok := goInt(v); ok && (i == 0 || i == 1) {
		return i == 1, nil
	}
	if u, ok := goUint(v); ok && u <= 1 {
		return u == 1, nil
	}
	return nil, normErr(TBool, v)
}

func intRange(k Kind) (lo int64, hi uint64) {
	switch k {
	case UInt8:
		return 0, math.MaxUint8
	case UInt16:
		return 0, math.MaxUint16
	case UInt32:
		return 0, math.MaxUint32
	case UInt64:
		return 0, math.MaxUint64
	case Int8:
		return math.MinInt8, math.MaxInt8
	case Int16:
		return math.MinInt16, math.MaxInt16
	case Int32:
		return math.MinInt32, math.MaxInt32
	}
	return math.MinInt64, math.MaxInt64
}

func outOfRange(t Type, v any) error {
	return errf(codeArgumentOutOfBound, "Value %v is out of range of %s", v, t.String())
}

// normalizeInt range-checks Go integers (no silent wrapping); strings and
// json.Number are parsed with CAST-from-String rules.
func normalizeInt(t Type, v any) (any, error) {
	lo, hi := intRange(t.Kind)
	signed := isSignedInt(t.Kind)
	fromInt := func(i int64) (any, error) {
		if i < lo || (i >= 0 && uint64(i) > hi) {
			return nil, outOfRange(t, i)
		}
		if signed {
			return i, nil
		}
		return uint64(i), nil
	}
	fromUint := func(u uint64) (any, error) {
		if u > hi {
			return nil, outOfRange(t, u)
		}
		if signed {
			return int64(u), nil
		}
		return u, nil
	}
	switch x := v.(type) {
	case bool:
		if x {
			return fromUint(1)
		}
		return fromUint(0)
	case float64:
		return floatIntoInt(t, x, fromInt, fromUint)
	case float32:
		return floatIntoInt(t, float64(x), fromInt, fromUint)
	case string:
		return asInputParseErr(convertFromString(x, t))(t, x)
	case json.Number:
		// Number text (JSON input). Error codes follow ClickHouse's
		// readIntText where it also fails: a '-' on an unsigned type is
		// CANNOT_PARSE_NUMBER and a non-integral number is
		// CANNOT_PARSE_INPUT_ASSERTION_FAILED. Integral fractional or
		// exponent forms (1.0, 1e2) are accepted leniently. Deliberate
		// deviation: an out-of-range integer is ARGUMENT_OUT_OF_BOUND where
		// ClickHouse silently wraps it modulo 2^width (300 -> 44).
		s := x.String()
		if strings.ContainsAny(s, ".eE") {
			f, err := x.Float64()
			if err != nil {
				return nil, normErr(t, v)
			}
			if f != math.Trunc(f) {
				return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected an integer for type %s, got '%s'", t.String(), s)
			}
			return floatIntoInt(t, f, fromInt, fromUint)
		}
		if strings.HasPrefix(s, "-") && !isSignedInt(t.Kind) {
			return nil, errf(codeCannotParseNumber, "Unsigned type must not contain '-' symbol")
		}
		n, err := parseNumberLiteral(s)
		if err != nil {
			return nil, normErr(t, v)
		}
		switch nv := n.(type) {
		case uint64:
			return fromUint(nv)
		case int64:
			return fromInt(nv)
		}
		return nil, outOfRange(t, s)
	}
	if i, ok := goInt(v); ok {
		return fromInt(i)
	}
	if u, ok := goUint(v); ok {
		return fromUint(u)
	}
	return nil, normErr(t, v)
}

func floatIntoInt(t Type, f float64, fromInt func(int64) (any, error), fromUint func(uint64) (any, error)) (any, error) {
	if f != math.Trunc(f) || math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, errf(cherr.CannotConvertType, "Cannot convert non-integral value %v to %s", f, t.String())
	}
	if f >= 0 && f < 18446744073709551616.0 {
		return fromUint(uint64(f))
	}
	if f < 0 && f >= -9223372036854775808.0 {
		return fromInt(int64(f))
	}
	return nil, outOfRange(t, f)
}

func normalizeFloat(t Type, v any) (any, error) {
	var f float64
	switch x := v.(type) {
	case float64:
		f = x
	case float32:
		f = float64(x)
	case bool:
		if x {
			f = 1
		}
	case string:
		return asInputParseErr(convertFromString(x, t))(t, x)
	case json.Number:
		return parseFloatText(x.String(), t.Kind)
	default:
		if i, ok := goInt(v); ok {
			f = float64(i)
		} else if u, ok := goUint(v); ok {
			f = float64(u)
		} else {
			return nil, normErr(t, v)
		}
	}
	if t.Kind == Float32 {
		f = float64(float32(f))
	}
	return f, nil
}

// timeFromInputString parses a date/time string from input data (JSON).
// Trailing data after a valid prefix is CANNOT_PARSE_INPUT_ASSERTION_FAILED
// (27) like ClickHouse's input formats (CAST reports 6 instead).
func timeFromInputString(s string, t Type) (any, error) {
	var v any
	var err error
	switch t.Kind {
	case Date, Date32:
		v, err = parseDateText(s, t.Kind, true)
	case DateTime:
		v, err = parseDateTimeText(s, locationOf(t))
	default:
		v, err = parseDateTime64Text(s, t.Precision, locationOf(t))
	}
	var td *trailingDataErr
	if errors.As(err, &td) && td.pos <= len(td.s) {
		return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected '\"' before: '%s'", td.s[td.pos:])
	}
	return v, plainErr(err)
}

// normalizeTime accepts time.Time, strings (parsed as text), and numbers
// (Date: day number; DateTime: unix seconds; DateTime64: unix seconds, or a
// scaled integer per the text rule).
func normalizeTime(t Type, v any) (any, error) {
	switch x := v.(type) {
	case time.Time:
		switch t.Kind {
		case Date, Date32:
			d := time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, time.UTC)
			if err := checkDateRange(d, t.Kind); err != nil {
				return nil, err
			}
			return d, nil
		case DateTime:
			u := x.UTC().Truncate(time.Second)
			if err := checkDateTimeRange(u); err != nil {
				return nil, err
			}
			return u, nil
		default:
			u := truncPrecision(x.UTC(), t.Precision)
			if err := checkDateTime64Range(u); err != nil {
				return nil, err
			}
			return u, nil
		}
	case string:
		return timeFromInputString(x, t)
	case json.Number:
		if t.Kind == DateTime64 {
			return parseDateTime64Text(x.String(), t.Precision, locationOf(t))
		}
		n, err := parseNumberLiteral(x.String())
		if err != nil {
			return nil, normErr(t, v)
		}
		return normalizeTime(t, n)
	}
	var num any
	if i, ok := goInt(v); ok {
		num = i
	} else if u, ok := goUint(v); ok {
		num = u
	} else if f, ok := v.(float64); ok {
		num = f
	} else if f, ok := v.(float32); ok {
		num = float64(f)
	} else {
		return nil, normErr(t, v)
	}
	switch t.Kind {
	case Date, Date32:
		iv, ok := numberAsInt64(num)
		if !ok {
			return nil, normErr(t, v)
		}
		d := Epoch.AddDate(0, 0, int(max(min(iv, 1<<20), -(1<<20))))
		if err := checkDateRange(d, t.Kind); err != nil {
			return nil, err
		}
		return d, nil
	case DateTime:
		iv, ok := numberAsInt64(num)
		if !ok || iv < 0 || iv > math.MaxUint32 {
			return nil, outOfRange(t, num)
		}
		return time.Unix(iv, 0).UTC(), nil
	default:
		if iv, ok := numberAsInt64(num); ok && iv >= dt64ScaledThreshold && t.Precision > 0 {
			// Integer already scaled by 10^precision (same rule as text input).
			scale := pow10[t.Precision]
			tm := time.Unix(iv/scale, (iv%scale)*pow10[9-t.Precision]).UTC()
			if err := checkDateTime64Range(tm); err != nil {
				return nil, err
			}
			return tm, nil
		}
		return numberToDateTime64(num, t.Precision)
	}
}

// sliceItems returns the elements of any Go slice/array value.
func sliceItems(v any) ([]any, bool) {
	switch x := v.(type) {
	case []any:
		return x, true
	case []byte:
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

func normalizeTuple(t Type, v any) (any, error) {
	if m, ok := v.(map[string]any); ok && isNamedTuple(t) {
		out := make([]any, len(t.Fields))
		for i, f := range t.Fields {
			fv, present := m[f.Name]
			if !present {
				out[i] = zeroValue(f.Type)
				continue
			}
			n, err := normalize(f.Type, fv)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		for k := range m {
			if !slices.ContainsFunc(t.Fields, func(f Field) bool { return f.Name == k }) {
				return nil, errf(cherr.TypeMismatch, "Unknown tuple element %q for %s", k, t.String())
			}
		}
		return out, nil
	}
	if s, ok := v.(string); ok {
		return parseCompositeString(s, t)
	}
	items, ok := sliceItems(v)
	if !ok {
		return nil, normErr(t, v)
	}
	if len(items) != len(t.Fields) {
		return nil, errf(cherr.TypeMismatch, "Tuple %s requires %d elements, got %d", t.String(), len(t.Fields), len(items))
	}
	out := make([]any, len(items))
	for i, e := range items {
		n, err := normalize(t.Fields[i].Type, e)
		if err != nil {
			return nil, err
		}
		out[i] = n
	}
	return out, nil
}

func normalizeMap(t Type, v any) (any, error) {
	if s, ok := v.(string); ok {
		p := &quotedParser{s: s}
		r, err := p.parseValue(t)
		if err != nil {
			return nil, err
		}
		if p.pos != len(s) {
			return nil, errf(cherr.CannotParseText, "Cannot parse %s from '%s'", t.String(), s)
		}
		return r, nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Map {
		keys := rv.MapKeys()
		pairs := make([][2]any, len(keys))
		for i, k := range keys {
			nk, err := normalize(*t.Key, k.Interface())
			if err != nil {
				return nil, err
			}
			nv, err := normalize(*t.Elem, rv.MapIndex(k).Interface())
			if err != nil {
				return nil, err
			}
			pairs[i] = [2]any{nk, nv}
		}
		// Go maps are unordered; sort by key for deterministic output.
		slices.SortFunc(pairs, func(a, b [2]any) int { return CompareTyped(a[0], b[0], *t.Key) })
		out := make([]any, len(pairs))
		for i, p := range pairs {
			out[i] = []any{p[0], p[1]}
		}
		return out, nil
	}
	items, ok := sliceItems(v)
	if !ok {
		return nil, normErr(t, v)
	}
	out := make([]any, len(items))
	for i, it := range items {
		kv, ok := sliceItems(it)
		if !ok || len(kv) != 2 {
			return nil, errf(cherr.TypeMismatch, "Map %s entries must be [key, value] pairs, got %T", t.String(), it)
		}
		nk, err := normalize(*t.Key, kv[0])
		if err != nil {
			return nil, err
		}
		nv, err := normalize(*t.Elem, kv[1])
		if err != nil {
			return nil, err
		}
		out[i] = []any{nk, nv}
	}
	return out, nil
}
