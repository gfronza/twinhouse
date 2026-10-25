// Package chtype is the ClickHouse type system: type descriptors, the Go
// representation of values, text parsing/formatting and comparison.
//
// # Value representation (CONTRACT — every package relies on this)
//
// A ClickHouse value is held in a Go `any` with exactly one canonical
// representation per type:
//
//	Bool                         bool
//	UInt8/16/32/64               uint64
//	Int8/16/32/64                int64
//	Float32/Float64              float64
//	String, FixedString(N)       string (raw bytes; FixedString is NUL-padded to N)
//	UUID                         string, canonical lower-case 8-4-4-4-12 form
//	Date, Date32                 time.Time at 00:00:00 UTC
//	DateTime, DateTime64(p[,tz]) time.Time in UTC (tz only affects text rendering)
//	Enum8/Enum16                 string (the enum name)
//	Nullable(T)                  nil for NULL, else T's representation
//	LowCardinality(T)            T's representation
//	Array(T)                     []any
//	Tuple(T1, ...)               []any (one element per field)
//	Map(K, V)                    []any of []any{key, value} pairs (insertion order)
//	SimpleAggregateFunction(f,T) T's representation
//	AggregateFunction(f, ...)    *AggPartial (see aggpartial.go)
//	Nothing                      nil
//
// Code must never put int, int32, float32, []string, etc. into a value slot;
// use Normalize to coerce foreign Go values.
package chtype

import "time"

// Kind enumerates ClickHouse type constructors.
type Kind uint8

const (
	Invalid Kind = iota
	Nothing
	Bool
	UInt8
	UInt16
	UInt32
	UInt64
	Int8
	Int16
	Int32
	Int64
	Float32
	Float64
	String
	FixedString
	UUID
	Date
	Date32
	DateTime
	DateTime64
	Enum8
	Enum16
	Nullable
	LowCardinality
	Array
	Tuple
	Map
	AggregateFunction
	SimpleAggregateFunction
)

// Type is a parsed ClickHouse type.
type Type struct {
	Kind Kind

	// Elem is the inner type for Nullable, LowCardinality, Array,
	// SimpleAggregateFunction (the value type) and Map (the value type).
	Elem *Type
	// Key is the Map key type.
	Key *Type
	// Fields are Tuple elements; Name is "" for unnamed tuples.
	Fields []Field

	N         int    // FixedString length
	Precision int    // DateTime64 precision (0..9)
	TZ        string // DateTime/DateTime64 explicit timezone, "" if none

	// EnumValues holds Enum8/Enum16 name->value pairs in declaration order.
	EnumValues []EnumValue

	// AggFunc is the aggregate function name for AggregateFunction and
	// SimpleAggregateFunction, e.g. "uniqExact", "sum", "argMax".
	AggFunc string
	// AggParams are parametric-aggregate parameters, e.g. quantile(0.9) -> [0.9].
	AggParams []any
	// AggArgs are the AggregateFunction argument types (not used for
	// SimpleAggregateFunction, whose value type is Elem).
	AggArgs []Type
}

// Field is one Tuple element.
type Field struct {
	Name string
	Type Type
}

// EnumValue is one Enum member.
type EnumValue struct {
	Name  string
	Value int64
}

// Simple type singletons, handy for type inference.
var (
	TBool     = Type{Kind: Bool}
	TUInt8    = Type{Kind: UInt8}
	TUInt16   = Type{Kind: UInt16}
	TUInt32   = Type{Kind: UInt32}
	TUInt64   = Type{Kind: UInt64}
	TInt8     = Type{Kind: Int8}
	TInt16    = Type{Kind: Int16}
	TInt32    = Type{Kind: Int32}
	TInt64    = Type{Kind: Int64}
	TFloat32  = Type{Kind: Float32}
	TFloat64  = Type{Kind: Float64}
	TString   = Type{Kind: String}
	TUUID     = Type{Kind: UUID}
	TDate     = Type{Kind: Date}
	TDateTime = Type{Kind: DateTime}
	TNothing  = Type{Kind: Nothing}
)

// NewNullable returns Nullable(t).
func NewNullable(t Type) Type { return Type{Kind: Nullable, Elem: &t} }

// NewArray returns Array(t).
func NewArray(t Type) Type { return Type{Kind: Array, Elem: &t} }

// NewLowCardinality returns LowCardinality(t).
func NewLowCardinality(t Type) Type { return Type{Kind: LowCardinality, Elem: &t} }

// NewTuple returns Tuple(fields...).
func NewTuple(fields ...Field) Type { return Type{Kind: Tuple, Fields: fields} }

// NewDateTime64 returns DateTime64(p[, tz]).
func NewDateTime64(p int, tz string) Type { return Type{Kind: DateTime64, Precision: p, TZ: tz} }

// Epoch is the zero value of Date/DateTime types.
var Epoch = time.Unix(0, 0).UTC()
