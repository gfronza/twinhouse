// Package keyenc implements the order-preserving tuple encoding used for
// DynamoDB partition and sort keys (DESIGN.md §4, FoundationDB-tuple style).
//
// For values a, b of the same type, bytes.Compare(Encode(a), Encode(b)) has
// the sign of chtype.Compare(a, b) (NULL first, numbers by value, strings and
// UUIDs bytewise, times by instant, arrays/tuples lexicographically), with two
// normalisations that make equal values encode identically: -0.0 encodes as
// +0.0 and every NaN encodes as one canonical NaN, which sorts after +Inf.
// One exception: a NULL inside an array, tuple or map sorts first here,
// while chtype.Compare (ClickHouse's comparison operators) puts it after
// every value. The encoding is kept stable because stored keys depend on
// it; nothing derives key ranges or row order from nested elements.
//
// The encoding of a tuple prefix is a byte prefix of the encoding of the full
// tuple, and the next byte after a complete element is always < 0xFF (a type
// code, or a MergeTree uniquifier whose first byte is kept < 0xFF). Hence the
// keys whose leading elements equal a prefix p are exactly the byte range
// [enc(p), PrefixUpper(enc(p))]. Plain begins_with(enc(p)) is a superset: it
// also matches a string element continued by 0x00 bytes ("" vs "\x00").
//
// Enum values encode by name, i.e. their byte order is NOT ClickHouse's enum
// order; callers must not derive range bounds on Enum columns (see RangeSafe).
package keyenc

import (
	"encoding/binary"
	"math"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// Type codes.
const (
	codeNull   = 0x00
	codeBytes  = 0x02
	codeNested = 0x05
	codeInt    = 0x15
	codeUint   = 0x16
	codeFloat  = 0x21
	codeFalse  = 0x26
	codeTrue   = 0x27
	codeTime   = 0x33

	escapeByte = 0xFF
)

// PrefixUpper returns the greatest byte string of length <= limit that starts
// with p followed by a byte < 0xFF: p ‖ 0xFE ‖ 0xFF…, truncated to limit (p
// itself when len(p) >= limit). Every key of length <= limit that extends p
// with a complete element (or uniquifier) sorts <= it.
func PrefixUpper(p []byte, limit int) []byte {
	if len(p) >= limit {
		return append([]byte(nil), p[:limit]...)
	}
	out := make([]byte, limit)
	copy(out, p)
	out[len(p)] = 0xFE
	for i := len(p) + 1; i < limit; i++ {
		out[i] = 0xFF
	}
	return out
}

// minNanosTime / maxNanosTime bound the instants representable as int64 unix
// nanoseconds (about 1677-09-21 .. 2262-04-11).
var (
	minNanosTime = time.Unix(0, math.MinInt64).UTC()
	maxNanosTime = time.Unix(0, math.MaxInt64).UTC()
)

// CheckType reports whether values of t can be part of a sorting key.
func CheckType(t chtype.Type) error {
	switch t.Kind {
	case chtype.Bool, chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64,
		chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64,
		chtype.Float32, chtype.Float64,
		chtype.String, chtype.FixedString, chtype.UUID, chtype.Enum8, chtype.Enum16,
		chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		return nil
	case chtype.Nullable, chtype.LowCardinality, chtype.Array, chtype.SimpleAggregateFunction:
		if t.Elem == nil {
			return cherr.New(cherr.IllegalColumn, "malformed type %s: missing element type", kindName(t))
		}
		return CheckType(*t.Elem)
	case chtype.Map:
		if t.Key == nil || t.Elem == nil {
			return cherr.New(cherr.IllegalColumn, "malformed Map type")
		}
		if err := CheckType(*t.Key); err != nil {
			return err
		}
		return CheckType(*t.Elem)
	case chtype.Tuple:
		for _, f := range t.Fields {
			if err := CheckType(f.Type); err != nil {
				return err
			}
		}
		return nil
	}
	return cherr.New(cherr.IllegalColumn, "type %s cannot be used in a sorting key", kindName(t))
}

// RangeSafe reports whether byte order of encoded values of t equals the
// ClickHouse comparison order, i.e. whether range predicates (<, >, BETWEEN)
// may be translated to byte ranges. False for Enum (encoded by name), for
// UUID (encoded as its text, while ClickHouse orders it as a UInt128 whose
// high half is the last 8 bytes), for composite types containing either, and
// for composite types with Nullable elements (a nested NULL encodes first but
// compares after every value).
func RangeSafe(t chtype.Type) bool {
	switch t.Kind {
	case chtype.Enum8, chtype.Enum16, chtype.UUID:
		return false
	case chtype.Nullable, chtype.LowCardinality, chtype.SimpleAggregateFunction:
		return t.Elem != nil && RangeSafe(*t.Elem)
	case chtype.Array:
		return t.Elem != nil && elemRangeSafe(*t.Elem)
	case chtype.Map:
		return t.Key != nil && t.Elem != nil && elemRangeSafe(*t.Key) && elemRangeSafe(*t.Elem)
	case chtype.Tuple:
		for _, f := range t.Fields {
			if !elemRangeSafe(f.Type) {
				return false
			}
		}
		return true
	}
	return CheckType(t) == nil
}

// elemRangeSafe is RangeSafe for an element of a composite type.
func elemRangeSafe(t chtype.Type) bool {
	for t.Kind == chtype.LowCardinality && t.Elem != nil {
		t = *t.Elem
	}
	return t.Kind != chtype.Nullable && RangeSafe(t)
}

// Append appends the encoding of v (of type t) to dst.
func Append(dst []byte, t chtype.Type, v any) ([]byte, error) {
	return appendValue(dst, t, v, false)
}

// AppendTuple appends the encodings of vs (of types ts) to dst.
func AppendTuple(dst []byte, ts []chtype.Type, vs []any) ([]byte, error) {
	if len(ts) != len(vs) {
		return nil, cherr.New(cherr.LogicalError, "key tuple has %d values for %d types", len(vs), len(ts))
	}
	var err error
	for i := range ts {
		if dst, err = appendValue(dst, ts[i], vs[i], false); err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func appendValue(dst []byte, t chtype.Type, v any, nested bool) ([]byte, error) {
	switch t.Kind {
	case chtype.LowCardinality, chtype.SimpleAggregateFunction:
		if t.Elem == nil {
			return nil, malformed(t)
		}
		return appendValue(dst, *t.Elem, v, nested)
	case chtype.Nullable:
		if t.Elem == nil {
			return nil, malformed(t)
		}
		if v == nil {
			return appendNull(dst, nested), nil
		}
		return appendValue(dst, *t.Elem, v, nested)
	}
	if v == nil {
		return nil, cherr.New(cherr.TypeMismatch, "NULL value in non-Nullable key column of type %s", kindName(t))
	}
	switch t.Kind {
	case chtype.Bool:
		b, ok := v.(bool)
		if !ok {
			return nil, mismatch(t, v)
		}
		if b {
			return append(dst, codeTrue), nil
		}
		return append(dst, codeFalse), nil
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64:
		u, ok := asUint(v)
		if !ok {
			return nil, mismatch(t, v)
		}
		dst = append(dst, codeUint)
		return binary.BigEndian.AppendUint64(dst, u), nil
	case chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		i, ok := asInt(v)
		if !ok {
			return nil, mismatch(t, v)
		}
		dst = append(dst, codeInt)
		return binary.BigEndian.AppendUint64(dst, uint64(i)^(1<<63)), nil
	case chtype.Float32, chtype.Float64:
		f, ok := v.(float64)
		if !ok {
			return nil, mismatch(t, v)
		}
		dst = append(dst, codeFloat)
		return binary.BigEndian.AppendUint64(dst, floatKey(f)), nil
	case chtype.String, chtype.FixedString, chtype.Enum8, chtype.Enum16:
		s, ok := asString(v)
		if !ok {
			return nil, mismatch(t, v)
		}
		return appendBytes(dst, s), nil
	case chtype.UUID:
		s, ok := asString(v)
		if !ok {
			return nil, mismatch(t, v)
		}
		return appendBytes(dst, strings.ToLower(s)), nil
	case chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		tm, ok := v.(time.Time)
		if !ok {
			return nil, mismatch(t, v)
		}
		if tm.Before(minNanosTime) || tm.After(maxNanosTime) {
			return nil, cherr.New(cherr.BadArguments,
				"time %s is outside the range supported in sorting keys (1677-09-21 .. 2262-04-11)", tm.UTC().Format(time.RFC3339Nano))
		}
		dst = append(dst, codeTime)
		return binary.BigEndian.AppendUint64(dst, uint64(tm.UnixNano())^(1<<63)), nil
	case chtype.Array:
		if t.Elem == nil {
			return nil, malformed(t)
		}
		arr, ok := v.([]any)
		if !ok {
			return nil, mismatch(t, v)
		}
		dst = append(dst, codeNested)
		var err error
		for _, e := range arr {
			if dst, err = appendValue(dst, *t.Elem, e, true); err != nil {
				return nil, err
			}
		}
		return append(dst, 0x00), nil
	case chtype.Tuple:
		tup, ok := v.([]any)
		if !ok || len(tup) != len(t.Fields) {
			return nil, mismatch(t, v)
		}
		dst = append(dst, codeNested)
		var err error
		for i, f := range t.Fields {
			if dst, err = appendValue(dst, f.Type, tup[i], true); err != nil {
				return nil, err
			}
		}
		return append(dst, 0x00), nil
	case chtype.Map:
		if t.Key == nil || t.Elem == nil {
			return nil, malformed(t)
		}
		pairs, ok := v.([]any)
		if !ok {
			return nil, mismatch(t, v)
		}
		dst = append(dst, codeNested)
		var err error
		for _, p := range pairs {
			kv, ok := p.([]any)
			if !ok || len(kv) != 2 {
				return nil, mismatch(t, v)
			}
			dst = append(dst, codeNested)
			if dst, err = appendValue(dst, *t.Key, kv[0], true); err != nil {
				return nil, err
			}
			if dst, err = appendValue(dst, *t.Elem, kv[1], true); err != nil {
				return nil, err
			}
			dst = append(dst, 0x00)
		}
		return append(dst, 0x00), nil
	}
	return nil, cherr.New(cherr.IllegalColumn, "type %s cannot be used in a sorting key", kindName(t))
}

func appendNull(dst []byte, nested bool) []byte {
	if nested {
		return append(dst, codeNull, escapeByte)
	}
	return append(dst, codeNull)
}

// appendBytes writes 0x02, s with every 0x00 escaped as 0x00 0xFF, then 0x00.
func appendBytes(dst []byte, s string) []byte {
	dst = append(dst, codeBytes)
	for i := 0; i < len(s); i++ {
		dst = append(dst, s[i])
		if s[i] == 0x00 {
			dst = append(dst, escapeByte)
		}
	}
	return append(dst, 0x00)
}

// floatKey maps a float64 to a uint64 whose unsigned order is the numeric
// order (-0 normalised to +0, NaN canonical and greatest).
func floatKey(f float64) uint64 {
	if f == 0 {
		f = 0
	}
	if math.IsNaN(f) {
		f = math.NaN()
	}
	b := math.Float64bits(f)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | 1<<63
}

func asUint(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case int64:
		if x >= 0 {
			return uint64(x), true
		}
	}
	return 0, false
}

func asInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case uint64:
		if x <= math.MaxInt64 {
			return int64(x), true
		}
	}
	return 0, false
}

func asString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	}
	return "", false
}

func mismatch(t chtype.Type, v any) error {
	return cherr.New(cherr.TypeMismatch, "key value of Go type %T does not match column type %s", v, kindName(t))
}

func malformed(t chtype.Type) error {
	return cherr.New(cherr.LogicalError, "malformed type %s: missing element type", kindName(t))
}

// kindName renders a type for error messages without depending on
// chtype.Type.String being available.
func kindName(t chtype.Type) string {
	if s := t.String(); s != "" && s != "?" {
		return s
	}
	return kindNames[t.Kind]
}

var kindNames = map[chtype.Kind]string{
	chtype.Invalid: "Invalid", chtype.Nothing: "Nothing", chtype.Bool: "Bool",
	chtype.UInt8: "UInt8", chtype.UInt16: "UInt16", chtype.UInt32: "UInt32", chtype.UInt64: "UInt64",
	chtype.Int8: "Int8", chtype.Int16: "Int16", chtype.Int32: "Int32", chtype.Int64: "Int64",
	chtype.Float32: "Float32", chtype.Float64: "Float64", chtype.String: "String",
	chtype.FixedString: "FixedString", chtype.UUID: "UUID", chtype.Date: "Date", chtype.Date32: "Date32",
	chtype.DateTime: "DateTime", chtype.DateTime64: "DateTime64", chtype.Enum8: "Enum8", chtype.Enum16: "Enum16",
	chtype.Nullable: "Nullable", chtype.LowCardinality: "LowCardinality", chtype.Array: "Array",
	chtype.Tuple: "Tuple", chtype.Map: "Map", chtype.AggregateFunction: "AggregateFunction",
	chtype.SimpleAggregateFunction: "SimpleAggregateFunction",
}
