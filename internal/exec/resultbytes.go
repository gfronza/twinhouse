package exec

import (
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

// columnTypes returns the types of cols.
func columnTypes(cols []format.Column) []chtype.Type {
	ts := make([]chtype.Type, len(cols))
	for i, c := range cols {
		ts[i] = c.Type
	}
	return ts
}

// resultRowBytes is what a row adds to ClickHouse's columns, the bytes
// max_result_bytes counts.
func resultRowBytes(types []chtype.Type, row []any) uint64 {
	n := uint64(0)
	for i, t := range types {
		n += columnValueBytes(t, row[i])
	}
	return n
}

// columnValueBytes is what v adds to a ClickHouse column of type t: its width,
// a String's bytes plus an 8-byte offset, a Nullable's null-map byte, an
// Array's or Map's offset. LowCardinality counts as its value type, and an
// aggregate state as twinhouse's estimate of it.
func columnValueBytes(t chtype.Type, v any) uint64 {
	switch t.Kind {
	case chtype.Nullable:
		return 1 + columnValueBytes(*t.Elem, v)
	case chtype.LowCardinality, chtype.SimpleAggregateFunction:
		return columnValueBytes(*t.Elem, v)
	case chtype.String:
		s, _ := v.(string)
		return uint64(len(s)) + 8
	case chtype.FixedString:
		return uint64(t.N)
	case chtype.Array:
		n := uint64(8)
		for _, x := range listValue(v) {
			n += columnValueBytes(*t.Elem, x)
		}
		return n
	case chtype.Map:
		n := uint64(8)
		for _, e := range listValue(v) {
			if kv := listValue(e); len(kv) == 2 {
				n += columnValueBytes(*t.Key, kv[0]) + columnValueBytes(*t.Elem, kv[1])
			}
		}
		return n
	case chtype.Tuple:
		xs, n := listValue(v), uint64(0)
		for i, f := range t.Fields {
			if i < len(xs) {
				n += columnValueBytes(f.Type, xs[i])
			}
		}
		return n
	case chtype.AggregateFunction:
		return uint64(memtrack.Value(v))
	}
	return fixedBytes(t.Kind)
}

func listValue(v any) []any {
	xs, _ := v.([]any)
	return xs
}

// fixedBytes is the width of a fixed-size ClickHouse type.
func fixedBytes(k chtype.Kind) uint64 {
	switch k {
	case chtype.Bool, chtype.UInt8, chtype.Int8, chtype.Enum8:
		return 1
	case chtype.UInt16, chtype.Int16, chtype.Enum16, chtype.Date:
		return 2
	case chtype.UInt32, chtype.Int32, chtype.Float32, chtype.Date32, chtype.DateTime:
		return 4
	case chtype.UInt64, chtype.Int64, chtype.Float64, chtype.DateTime64:
		return 8
	case chtype.UUID:
		return 16
	}
	return 0
}
