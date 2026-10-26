package format

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/google/uuid"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// chgoUnsupported lists cases clickhouse-go's generic Row/Scan cannot
// represent (unnamed tuples nested in tuples); the bytes are still verified
// against ClickHouse by TestNativeGolden.
var chgoUnsupported = map[string]bool{"nested_tuple": true}

// TestNativeClickHouseGoDecode decodes our Native output with clickhouse-go's
// own block decoder, as its HTTP transport does for SELECT results.
func TestNativeClickHouseGoDecode(t *testing.T) {
	const rev = chGoRevision
	for _, c := range nativeCases() {
		t.Run(fmt.Sprintf("%s/rev%d", c.name, rev), func(t *testing.T) {
			if chgoUnsupported[c.name] {
				t.Skip("clickhouse-go cannot materialise this type generically")
			}
			data := encodeNative(t, c.cols, c.rows, rev)
			block := proto.Block{ServerContext: &column.ServerContext{Revision: rev, Timezone: time.UTC}}
			r := chproto.NewReader(bytes.NewReader(data))
			if err := block.Decode(r, rev); err != nil {
				t.Fatalf("clickhouse-go decode: %v", err)
			}
			if block.Rows() != len(c.rows) || len(block.Columns) != len(c.cols) {
				t.Fatalf("decoded %d rows x %d cols, want %d x %d", block.Rows(), len(block.Columns), len(c.rows), len(c.cols))
			}
			for ci, col := range block.Columns {
				if col.Name() != c.cols[ci].Name || string(col.Type()) != c.cols[ci].Type.String() {
					t.Fatalf("column %d is %s %s, want %s %s", ci, col.Name(), col.Type(), c.cols[ci].Name, c.cols[ci].Type.String())
				}
				for ri := range c.rows {
					got := fromChGo(c.cols[ci].Type, col.Row(ri, false))
					want := sortMaps(c.cols[ci].Type, c.rows[ri][ci])
					if !valuesEqual(got, want) {
						t.Fatalf("column %s row %d: got %#v, want %#v", col.Name(), ri, got, want)
					}
				}
			}
		})
	}
}

// TestNativeDecodeClickHouseGoBatch decodes what clickhouse-go sends for an
// HTTP batch INSERT (block encoded with revision 0).
func TestNativeDecodeClickHouseGoBatch(t *testing.T) {
	cols := []Column{
		col("id", "UInt64"), col("name", "String"), col("lc", "LowCardinality(String)"),
		col("lcn", "LowCardinality(Nullable(String))"), col("n", "Nullable(Int32)"),
		col("ts", "DateTime64(9)"), col("d", "Date"), col("u", "UUID"), col("tags", "Array(String)"),
		col("m", "Map(String, UInt64)"), col("tup", "Tuple(a UInt32, b Array(String))"), col("e", "Enum8('x' = 1, 'y' = 2)"),
		col("b", "Bool"), col("f", "Float32"), col("fs", "FixedString(2)"),
	}
	block := proto.Block{ServerContext: &column.ServerContext{Timezone: time.UTC}}
	for _, c := range cols {
		if err := block.AddColumn(c.Name, column.Type(c.Type.String())); err != nil {
			t.Fatal(err)
		}
	}
	ts := time.Date(2024, 5, 6, 7, 8, 9, 123456789, time.UTC)
	u := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	s := "x"
	n := int32(-5)
	rowsIn := [][]any{
		{uint64(1), "a", "p", &s, &n, ts, ts, u, []string{"t1", "t2"}, map[string]uint64{"k": 1}, map[string]any{"a": uint32(7), "b": []string{"z"}}, "x", true, float32(1.5), "ab"},
		{uint64(2), "", "q", nil, nil, ts.Add(time.Second), ts, u, []string{}, map[string]uint64{}, map[string]any{"a": uint32(0), "b": []string{}}, "y", false, float32(-2), "c"},
		{uint64(3), "c", "p", &s, nil, ts, ts, u, []string{"t3"}, map[string]uint64{"k": 1}, map[string]any{"a": uint32(1), "b": []string{"q", "r"}}, "x", true, float32(0), ""},
	}
	for _, r := range rowsIn {
		if err := block.Append(r...); err != nil {
			t.Fatal(err)
		}
	}
	var buf chproto.Buffer
	if err := block.Encode(&buf, 0); err != nil {
		t.Fatal(err)
	}
	// Two blocks back to back, like a batch flushed twice.
	data := append(append([]byte(nil), buf.Buf...), buf.Buf...)

	dec, err := NewDecoder("Native", bytes.NewReader(data), cols, Options{})
	if err != nil {
		t.Fatal(err)
	}
	got := readAll(t, dec)
	d := time.Date(2024, 5, 6, 0, 0, 0, 0, time.UTC)
	us := u.String()
	want := [][]any{
		{uint64(1), "a", "p", "x", int64(-5), ts, d, us, []any{"t1", "t2"}, []any{[]any{"k", uint64(1)}}, []any{uint64(7), []any{"z"}}, "x", true, 1.5, "ab"},
		{uint64(2), "", "q", nil, nil, ts.Add(time.Second), d, us, []any{}, []any{}, []any{uint64(0), []any{}}, "y", false, -2.0, "c\x00"},
		{uint64(3), "c", "p", "x", nil, ts, d, us, []any{"t3"}, []any{[]any{"k", uint64(1)}}, []any{uint64(1), []any{"q", "r"}}, "x", true, 0.0, "\x00\x00"},
	}
	assertRows(t, got, append(want, want...))
}

// fromChGo converts a clickhouse-go row value into our canonical form.
func fromChGo(t chtype.Type, v any) any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	v = rv.Interface()
	for t.Kind == chtype.Nullable || t.Kind == chtype.LowCardinality || t.Kind == chtype.SimpleAggregateFunction {
		t = *t.Elem
	}
	switch x := v.(type) {
	case time.Time:
		return x.UTC()
	case uuid.UUID:
		return x.String()
	case float32:
		return float64(x)
	case string, bool, float64:
		return x
	}
	switch rv.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		return rv.Int()
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return rv.Uint()
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := range out {
			switch t.Kind {
			case chtype.Array:
				out[i] = fromChGo(*t.Elem, rv.Index(i).Interface())
			case chtype.Tuple:
				out[i] = fromChGo(t.Fields[i].Type, rv.Index(i).Interface())
			default:
				out[i] = rv.Index(i).Interface()
			}
		}
		return out
	case reflect.Map:
		if t.Kind == chtype.Tuple {
			out := make([]any, len(t.Fields))
			for i, f := range t.Fields {
				out[i] = fromChGo(f.Type, rv.MapIndex(reflect.ValueOf(f.Name)).Interface())
			}
			return out
		}
		var pairs []any
		iter := rv.MapRange()
		for iter.Next() {
			pairs = append(pairs, []any{fromChGo(*t.Key, iter.Key().Interface()), fromChGo(*t.Elem, iter.Value().Interface())})
		}
		if pairs == nil {
			pairs = []any{}
		}
		return sortMaps(t, pairs)
	}
	return v
}

// sortMaps orders Map pairs by key so they compare independent of Go map order.
func sortMaps(t chtype.Type, v any) any {
	for t.Kind == chtype.Nullable || t.Kind == chtype.LowCardinality || t.Kind == chtype.SimpleAggregateFunction {
		t = *t.Elem
	}
	arr, ok := v.([]any)
	if !ok {
		return v
	}
	out := make([]any, len(arr))
	switch t.Kind {
	case chtype.Array:
		for i, e := range arr {
			out[i] = sortMaps(*t.Elem, e)
		}
	case chtype.Tuple:
		for i, e := range arr {
			out[i] = sortMaps(t.Fields[i].Type, e)
		}
	case chtype.Map:
		for i, e := range arr {
			kv := e.([]any)
			out[i] = []any{kv[0], sortMaps(*t.Elem, kv[1])}
		}
		sort.SliceStable(out, func(i, j int) bool {
			return fmt.Sprint(out[i].([]any)[0]) < fmt.Sprint(out[j].([]any)[0])
		})
	default:
		copy(out, arr)
	}
	return out
}
