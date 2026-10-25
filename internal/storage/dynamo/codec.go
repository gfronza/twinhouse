package dynamo

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
)

// Column value <-> DynamoDB attribute codec (DESIGN.md §4 "Column value
// encoding"). Types are taken from the catalog, never inferred from items.

var (
	attrNull  = &types.AttributeValueMemberNULL{Value: true}
	attrTrue  = &types.AttributeValueMemberBOOL{Value: true}
	attrFalse = &types.AttributeValueMemberBOOL{Value: false}
)

// DynamoDB N magnitude limits (inclusive lower, exclusive upper).
const (
	minNumberMagnitude = 1e-130
	maxNumberMagnitude = 1e126
)

// baseType strips Nullable, LowCardinality and SimpleAggregateFunction.
func baseType(t chtype.Type) chtype.Type {
	for t.Elem != nil && (t.Kind == chtype.Nullable || t.Kind == chtype.LowCardinality || t.Kind == chtype.SimpleAggregateFunction) {
		t = *t.Elem
	}
	return t
}

func isNullable(t chtype.Type) bool {
	for t.Elem != nil && (t.Kind == chtype.LowCardinality || t.Kind == chtype.SimpleAggregateFunction) {
		t = *t.Elem
	}
	return t.Kind == chtype.Nullable || t.Kind == chtype.Nothing
}

// encodeValue converts a canonical value of type t to an attribute value.
func encodeValue(t chtype.Type, v any) (types.AttributeValue, error) {
	if v == nil {
		if isNullable(t) {
			return attrNull, nil
		}
		return nil, cherr.New(cherr.TypeMismatch, "NULL value for non-Nullable type %s", typeName(t))
	}
	b := baseType(t)
	switch b.Kind {
	case chtype.Bool:
		x, ok := v.(bool)
		if !ok {
			return nil, valueMismatch(t, v)
		}
		if x {
			return attrTrue, nil
		}
		return attrFalse, nil
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64:
		switch x := v.(type) {
		case uint64:
			return &types.AttributeValueMemberN{Value: strconv.FormatUint(x, 10)}, nil
		case int64:
			if x >= 0 {
				return &types.AttributeValueMemberN{Value: strconv.FormatInt(x, 10)}, nil
			}
		}
		return nil, valueMismatch(t, v)
	case chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		switch x := v.(type) {
		case int64:
			return &types.AttributeValueMemberN{Value: strconv.FormatInt(x, 10)}, nil
		case uint64:
			if x <= math.MaxInt64 {
				return &types.AttributeValueMemberN{Value: strconv.FormatUint(x, 10)}, nil
			}
		}
		return nil, valueMismatch(t, v)
	case chtype.Float32, chtype.Float64:
		f, ok := v.(float64)
		if !ok {
			return nil, valueMismatch(t, v)
		}
		return encodeFloat(f, floatBits(b)), nil
	case chtype.String, chtype.FixedString, chtype.Enum8, chtype.Enum16, chtype.UUID:
		var s string
		switch x := v.(type) {
		case string:
			s = x
		case []byte:
			s = string(x)
		default:
			return nil, valueMismatch(t, v)
		}
		if utf8.ValidString(s) {
			return &types.AttributeValueMemberS{Value: s}, nil
		}
		return &types.AttributeValueMemberB{Value: []byte(s)}, nil
	case chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		tm, ok := v.(time.Time)
		if !ok {
			return nil, valueMismatch(t, v)
		}
		return &types.AttributeValueMemberN{Value: timeToNanos(tm)}, nil
	case chtype.Array:
		arr, ok := v.([]any)
		if !ok || b.Elem == nil {
			return nil, valueMismatch(t, v)
		}
		out := make([]types.AttributeValue, len(arr))
		for i, e := range arr {
			av, err := encodeValue(*b.Elem, e)
			if err != nil {
				return nil, err
			}
			out[i] = av
		}
		return &types.AttributeValueMemberL{Value: out}, nil
	case chtype.Tuple:
		tup, ok := v.([]any)
		if !ok || len(tup) != len(b.Fields) {
			return nil, valueMismatch(t, v)
		}
		out := make([]types.AttributeValue, len(tup))
		for i, f := range b.Fields {
			av, err := encodeValue(f.Type, tup[i])
			if err != nil {
				return nil, err
			}
			out[i] = av
		}
		return &types.AttributeValueMemberL{Value: out}, nil
	case chtype.Map:
		pairs, ok := v.([]any)
		if !ok || b.Key == nil || b.Elem == nil {
			return nil, valueMismatch(t, v)
		}
		out := make([]types.AttributeValue, len(pairs))
		for i, p := range pairs {
			kv, ok := p.([]any)
			if !ok || len(kv) != 2 {
				return nil, valueMismatch(t, v)
			}
			k, err := encodeValue(*b.Key, kv[0])
			if err != nil {
				return nil, err
			}
			val, err := encodeValue(*b.Elem, kv[1])
			if err != nil {
				return nil, err
			}
			out[i] = &types.AttributeValueMemberL{Value: []types.AttributeValue{k, val}}
		}
		return &types.AttributeValueMemberL{Value: out}, nil
	case chtype.Nothing:
		return attrNull, nil
	}
	return nil, cherr.New(cherr.NotImplemented, "type %s cannot be stored as a DynamoDB attribute", typeName(t))
}

func floatBits(t chtype.Type) int {
	if t.Kind == chtype.Float32 {
		return 32
	}
	return 64
}

// encodeFloat stores finite floats in N range as N; NaN, ±Inf, -0 and
// magnitudes outside N's range as S (parsed back with strconv).
func encodeFloat(f float64, bits int) types.AttributeValue {
	if s, ok := floatNumber(f, bits); ok {
		return &types.AttributeValueMemberN{Value: s}
	}
	switch {
	case math.IsNaN(f) && math.Signbit(f):
		return &types.AttributeValueMemberS{Value: "-nan"} // sign bit kept, as ClickHouse stores it
	case math.IsNaN(f):
		return &types.AttributeValueMemberS{Value: "nan"}
	case math.IsInf(f, 1):
		return &types.AttributeValueMemberS{Value: "inf"}
	case math.IsInf(f, -1):
		return &types.AttributeValueMemberS{Value: "-inf"}
	}
	return &types.AttributeValueMemberS{Value: strconv.FormatFloat(f, 'g', -1, bits)}
}

// floatNumber renders f as an N value if DynamoDB can store it exactly.
func floatNumber(f float64, bits int) (string, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) || (f == 0 && math.Signbit(f)) {
		return "", false
	}
	if a := math.Abs(f); a != 0 && (a < minNumberMagnitude || a >= maxNumberMagnitude) {
		return "", false
	}
	return strconv.FormatFloat(f, 'g', -1, bits), true
}

var (
	bigBillion = big.NewInt(1e9)
	minNanos   = time.Unix(0, math.MinInt64)
	maxNanos   = time.Unix(0, math.MaxInt64)
)

// timeToNanos renders the instant as exact decimal unix nanoseconds (also
// outside the int64 nanosecond range, e.g. Date32 2299-12-31).
func timeToNanos(tm time.Time) string {
	if !tm.Before(minNanos) && !tm.After(maxNanos) {
		return strconv.FormatInt(tm.UnixNano(), 10)
	}
	n := new(big.Int).Mul(big.NewInt(tm.Unix()), bigBillion)
	return n.Add(n, big.NewInt(int64(tm.Nanosecond()))).String()
}

func nanosToTime(s string) (time.Time, error) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, n).UTC(), nil
	}
	n, ok := parseBigInt(s)
	if !ok {
		return time.Time{}, cherr.New(cherr.IncorrectData, "invalid time value %q in DynamoDB item", s)
	}
	sec, nsec := new(big.Int).DivMod(n, bigBillion, new(big.Int))
	if !sec.IsInt64() {
		return time.Time{}, cherr.New(cherr.IncorrectData, "time value %q out of range", s)
	}
	return time.Unix(sec.Int64(), nsec.Int64()).UTC(), nil
}

// parseBigInt parses an integral DynamoDB number, which may use exponent
// notation ("1E5").
func parseBigInt(s string) (*big.Int, bool) {
	if n, ok := new(big.Int).SetString(s, 10); ok {
		return n, true
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok || !r.IsInt() {
		return nil, false
	}
	return new(big.Int).Set(r.Num()), true
}

// decodeValue converts an attribute back to the canonical value of type t.
// Integers are wrapped to the column width (sums kept in DynamoDB may exceed
// it, and ClickHouse sums wrap).
func decodeValue(t chtype.Type, av types.AttributeValue) (any, error) {
	if av == nil {
		return zeroValue(t), nil
	}
	if _, ok := av.(*types.AttributeValueMemberNULL); ok {
		if isNullable(t) {
			return nil, nil
		}
		return zeroValue(t), nil
	}
	b := baseType(t)
	switch b.Kind {
	case chtype.Bool:
		if x, ok := av.(*types.AttributeValueMemberBOOL); ok {
			return x.Value, nil
		}
		if x, ok := av.(*types.AttributeValueMemberN); ok { // tolerate numeric booleans
			return x.Value != "0", nil
		}
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64,
		chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		if x, ok := av.(*types.AttributeValueMemberN); ok {
			return decodeInt(b.Kind, x.Value)
		}
	case chtype.Float32, chtype.Float64:
		var s string
		switch x := av.(type) {
		case *types.AttributeValueMemberN:
			s = x.Value
		case *types.AttributeValueMemberS:
			s = x.Value
		default:
			return nil, attrMismatch(t, av)
		}
		if s == "-nan" {
			return math.Copysign(math.NaN(), -1), nil
		}
		f, err := strconv.ParseFloat(s, floatBits(b))
		if err != nil && !isRangeErr(err) {
			return nil, cherr.New(cherr.IncorrectData, "invalid float value %q in DynamoDB item", s)
		}
		return f, nil
	case chtype.String, chtype.FixedString, chtype.Enum8, chtype.Enum16, chtype.UUID:
		switch x := av.(type) {
		case *types.AttributeValueMemberS:
			return x.Value, nil
		case *types.AttributeValueMemberB:
			return string(x.Value), nil
		}
	case chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		if x, ok := av.(*types.AttributeValueMemberN); ok {
			tm, err := nanosToTime(x.Value)
			if err != nil {
				return nil, err
			}
			// Values written for a finer type before ALTER TABLE MODIFY
			// COLUMN (DateTime -> Date, DateTime64(9) -> DateTime64(3))
			// read as the conversion ClickHouse applies; values written for
			// this type are already exact.
			return tm.Truncate(timeUnit(b)), nil
		}
	case chtype.Array:
		if x, ok := av.(*types.AttributeValueMemberL); ok && b.Elem != nil {
			out := make([]any, len(x.Value))
			for i, e := range x.Value {
				v, err := decodeValue(*b.Elem, e)
				if err != nil {
					return nil, err
				}
				out[i] = v
			}
			return out, nil
		}
	case chtype.Tuple:
		if x, ok := av.(*types.AttributeValueMemberL); ok {
			out := make([]any, len(b.Fields))
			for i, f := range b.Fields {
				var e types.AttributeValue
				if i < len(x.Value) {
					e = x.Value[i]
				}
				v, err := decodeValue(f.Type, e)
				if err != nil {
					return nil, err
				}
				out[i] = v
			}
			return out, nil
		}
	case chtype.Map:
		if x, ok := av.(*types.AttributeValueMemberL); ok && b.Key != nil && b.Elem != nil {
			out := make([]any, len(x.Value))
			for i, e := range x.Value {
				pair, ok := e.(*types.AttributeValueMemberL)
				if !ok || len(pair.Value) != 2 {
					return nil, attrMismatch(t, av)
				}
				k, err := decodeValue(*b.Key, pair.Value[0])
				if err != nil {
					return nil, err
				}
				v, err := decodeValue(*b.Elem, pair.Value[1])
				if err != nil {
					return nil, err
				}
				out[i] = []any{k, v}
			}
			return out, nil
		}
	case chtype.Nothing:
		return nil, nil
	}
	return nil, attrMismatch(t, av)
}

// timeUnit is the precision of a date/time type.
func timeUnit(b chtype.Type) time.Duration {
	switch b.Kind {
	case chtype.Date, chtype.Date32:
		return 24 * time.Hour
	case chtype.DateTime:
		return time.Second
	case chtype.DateTime64:
		d := time.Duration(1)
		for p := b.Precision; p < 9; p++ {
			d *= 10
		}
		return d
	}
	return 1
}

func isRangeErr(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}

var intBits = map[chtype.Kind]uint{
	chtype.UInt8: 8, chtype.UInt16: 16, chtype.UInt32: 32, chtype.UInt64: 64,
	chtype.Int8: 8, chtype.Int16: 16, chtype.Int32: 32, chtype.Int64: 64,
}

func signedKind(k chtype.Kind) bool {
	return k == chtype.Int8 || k == chtype.Int16 || k == chtype.Int32 || k == chtype.Int64
}

// decodeInt parses an integer N and wraps it modulo 2^bits like ClickHouse
// integer overflow.
func decodeInt(k chtype.Kind, s string) (any, error) {
	bits := intBits[k]
	if signedKind(k) {
		if n, err := strconv.ParseInt(s, 10, int(bits)); err == nil {
			return n, nil
		}
	} else if n, err := strconv.ParseUint(s, 10, int(bits)); err == nil {
		return n, nil
	}
	n, ok := parseBigInt(s)
	if !ok {
		return nil, cherr.New(cherr.IncorrectData, "invalid integer value %q in DynamoDB item", s)
	}
	mod := new(big.Int).Lsh(big.NewInt(1), bits)
	n.Mod(n, mod) // Euclidean: result in [0, 2^bits)
	u := n.Uint64()
	if !signedKind(k) {
		return u, nil
	}
	if bits < 64 && u >= 1<<(bits-1) {
		return int64(u) - int64(1)<<bits, nil
	}
	return int64(u), nil
}

// zeroValue is ClickHouse's default value of t (chtype.Zero's contract,
// implemented locally for the types storage can hold).
func zeroValue(t chtype.Type) any {
	if isNullable(t) {
		return nil
	}
	b := baseType(t)
	switch b.Kind {
	case chtype.Bool:
		return false
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64:
		return uint64(0)
	case chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		return int64(0)
	case chtype.Float32, chtype.Float64:
		return float64(0)
	case chtype.String:
		return ""
	case chtype.FixedString:
		return strings.Repeat("\x00", b.N)
	case chtype.UUID:
		return "00000000-0000-0000-0000-000000000000"
	case chtype.Enum8, chtype.Enum16:
		if len(b.EnumValues) > 0 {
			return b.EnumValues[0].Name
		}
		return ""
	case chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		return chtype.Epoch
	case chtype.Array, chtype.Map:
		return []any{}
	case chtype.Tuple:
		out := make([]any, len(b.Fields))
		for i, f := range b.Fields {
			out[i] = zeroValue(f.Type)
		}
		return out
	}
	return nil
}

func typeName(t chtype.Type) string {
	if s := t.String(); s != "" && s != "?" {
		return s
	}
	return "kind " + strconv.Itoa(int(t.Kind))
}

func valueMismatch(t chtype.Type, v any) error {
	return cherr.New(cherr.TypeMismatch, "value of Go type %T does not match column type %s", v, typeName(t))
}

func attrMismatch(t chtype.Type, av types.AttributeValue) error {
	return cherr.New(cherr.IncorrectData, "DynamoDB attribute of type %T cannot be decoded as %s", av, typeName(t))
}
