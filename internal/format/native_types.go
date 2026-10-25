package format

import (
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
)

// nativeCodec is a compiled ClickHouse type tree used by the Native format
// encoder and decoder. Compiling once per column keeps the per-value hot
// loops free of type inspection.
type nativeCodec struct {
	t    chtype.Type
	kind chtype.Kind

	// elem: Nullable / Array / LowCardinality / SimpleAggregateFunction inner
	// codec, Map value codec.
	elem *nativeCodec
	// key: Map key codec.
	key *nativeCodec
	// fields: Tuple element codecs.
	fields []*nativeCodec

	// lcNullable: LowCardinality(Nullable(T)); dict then holds T (elem is the
	// non-nullable codec for T).
	lcNullable bool

	enumByName  map[string]int64
	enumByValue map[int64]string
}

// compileNative builds the codec tree for t. It fails with NOT_IMPLEMENTED
// for types the Native format cannot carry here (AggregateFunction states).
func compileNative(t chtype.Type) (*nativeCodec, error) {
	c := &nativeCodec{t: t, kind: t.Kind}
	var err error
	switch t.Kind {
	case chtype.Nothing, chtype.Bool,
		chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64,
		chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64,
		chtype.Float32, chtype.Float64, chtype.String, chtype.UUID,
		chtype.Date, chtype.Date32, chtype.DateTime:
	case chtype.FixedString:
		if t.N <= 0 {
			return nil, cherr.New(cherr.UnknownType, "FixedString size must be positive, got %d", t.N)
		}
	case chtype.DateTime64:
		if t.Precision < 0 || t.Precision > 9 {
			return nil, cherr.New(cherr.UnknownType, "DateTime64 precision %d is out of range [0, 9]", t.Precision)
		}
	case chtype.Enum8, chtype.Enum16:
		c.enumByName = make(map[string]int64, len(t.EnumValues))
		c.enumByValue = make(map[int64]string, len(t.EnumValues))
		for _, ev := range t.EnumValues {
			c.enumByName[ev.Name] = ev.Value
			c.enumByValue[ev.Value] = ev.Name
		}
	case chtype.Nullable, chtype.Array, chtype.SimpleAggregateFunction:
		if t.Elem == nil {
			return nil, cherr.New(cherr.LogicalError, "type %v has no element type", t.Kind)
		}
		if c.elem, err = compileNative(*t.Elem); err != nil {
			return nil, err
		}
	case chtype.LowCardinality:
		if t.Elem == nil {
			return nil, cherr.New(cherr.LogicalError, "LowCardinality has no element type")
		}
		inner := *t.Elem
		if inner.Kind == chtype.Nullable {
			if inner.Elem == nil {
				return nil, cherr.New(cherr.LogicalError, "Nullable has no element type")
			}
			c.lcNullable = true
			inner = *inner.Elem
		}
		switch inner.Kind {
		case chtype.LowCardinality, chtype.Nullable, chtype.Array, chtype.Tuple, chtype.Map,
			chtype.AggregateFunction, chtype.SimpleAggregateFunction, chtype.Nothing:
			return nil, cherr.New(cherr.IllegalColumn, "LowCardinality(%v) is not supported", inner.Kind)
		}
		if c.elem, err = compileNative(inner); err != nil {
			return nil, err
		}
	case chtype.Tuple:
		c.fields = make([]*nativeCodec, len(t.Fields))
		for i, f := range t.Fields {
			if c.fields[i], err = compileNative(f.Type); err != nil {
				return nil, err
			}
		}
	case chtype.Map:
		if t.Key == nil || t.Elem == nil {
			return nil, cherr.New(cherr.LogicalError, "Map has no key or value type")
		}
		if c.key, err = compileNative(*t.Key); err != nil {
			return nil, err
		}
		if c.elem, err = compileNative(*t.Elem); err != nil {
			return nil, err
		}
	case chtype.AggregateFunction:
		return nil, cherr.New(cherr.NotImplemented,
			"Serialization of AggregateFunction(%s) columns is not supported; use finalizeAggregation or -Merge", t.AggFunc)
	default:
		return nil, cherr.New(cherr.UnknownType, "unsupported type kind %d", t.Kind)
	}
	return c, nil
}

// zero returns the value written in place of NULL / used as LowCardinality default.
func (c *nativeCodec) zero() any {
	switch c.kind {
	case chtype.Nothing:
		return nil
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
		return string(make([]byte, c.t.N))
	case chtype.UUID:
		return "00000000-0000-0000-0000-000000000000"
	case chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		return chtype.Epoch
	case chtype.Enum8, chtype.Enum16:
		// An Enum's default is its member with the smallest value; this is
		// what ClickHouse writes into NULL slots of computed Nullable(Enum)
		// columns. (Slots read from storage may hold 0; decoders tolerate it.)
		if len(c.t.EnumValues) == 0 {
			return nil
		}
		best := c.t.EnumValues[0]
		for _, ev := range c.t.EnumValues[1:] {
			if ev.Value < best.Value {
				best = ev
			}
		}
		return best.Name
	case chtype.Nullable:
		return nil
	case chtype.LowCardinality:
		if c.lcNullable {
			return nil
		}
		return c.elem.zero()
	case chtype.SimpleAggregateFunction:
		return c.elem.zero()
	case chtype.Array, chtype.Map:
		return []any{}
	case chtype.Tuple:
		out := make([]any, len(c.fields))
		for i, f := range c.fields {
			out[i] = f.zero()
		}
		return out
	}
	return nil
}

// findAggregate returns the first AggregateFunction type nested in t.
func findAggregate(t chtype.Type) (chtype.Type, bool) {
	switch t.Kind {
	case chtype.AggregateFunction:
		return t, true
	case chtype.Tuple:
		for _, f := range t.Fields {
			if a, ok := findAggregate(f.Type); ok {
				return a, true
			}
		}
	case chtype.Map:
		if t.Key != nil {
			if a, ok := findAggregate(*t.Key); ok {
				return a, true
			}
		}
	}
	if t.Elem != nil {
		return findAggregate(*t.Elem)
	}
	return chtype.Type{}, false
}

// checkOutputType rejects types no output format here can render:
// AggregateFunction states are opaque.
func checkOutputType(t chtype.Type) error {
	if a, ok := findAggregate(t); ok {
		return cherr.New(cherr.NotImplemented,
			"Output of AggregateFunction(%s) columns is not supported; use finalizeAggregation or -Merge", a.AggFunc)
	}
	return nil
}

// checkInputType rejects target column types no input format here can fill.
func checkInputType(t chtype.Type) error {
	if a, ok := findAggregate(t); ok {
		return cherr.New(cherr.NotImplemented,
			"Input of AggregateFunction(%s) columns is not supported", a.AggFunc)
	}
	return nil
}

// columnErr prefixes err's message with the column name, keeping its code.
func columnErr(err error, name string) error {
	ce := cherr.As(err)
	return &cherr.Error{Code: ce.Code, Msg: "column " + name + ": " + ce.Msg, Cause: ce.Cause}
}
