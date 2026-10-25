package format

import (
	"bytes"
	"io"
	"strconv"
	"testing"
	"time"

	"dynamodb-eventstream/internal/cherr"
)

func TestNativeEmptyResult(t *testing.T) {
	cols := []Column{col("a", "UInt8")}
	for _, rev := range goldenRevisions {
		var buf bytes.Buffer
		enc, err := NewEncoder("native", &buf, cols, Options{ClientProtocolVersion: rev})
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteRows(nil); err != nil {
			t.Fatal(err)
		}
		if err := enc.Finish(Stats{}); err != nil {
			t.Fatal(err)
		}
		// ClickHouse returns an empty body for an empty Native result.
		if buf.Len() != 0 {
			t.Fatalf("rev %d: expected empty output, got %x", rev, buf.Bytes())
		}
	}
}

func TestNativeMultipleBlocks(t *testing.T) {
	cols := []Column{col("a", "UInt64"), col("s", "LowCardinality(String)")}
	var buf bytes.Buffer
	enc, err := NewEncoder("Native", &buf, cols, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var want [][]any
	for b := 0; b < 3; b++ {
		var rows [][]any
		for i := 0; i < 5; i++ {
			rows = append(rows, []any{uint64(b*10 + i), "v" + strconv.Itoa(i%2)})
		}
		want = append(want, rows...)
		if err := enc.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Finish(Stats{}); err != nil {
		t.Fatal(err)
	}
	dec, err := NewDecoder("Native", &buf, cols, Options{})
	if err != nil {
		t.Fatal(err)
	}
	assertRows(t, readAll(t, dec), want)
}

func TestNativeEncodeErrors(t *testing.T) {
	cases := []struct {
		name string
		typ  string
		val  any
		code int
	}{
		{"non-canonical int", "UInt8", 5, cherr.LogicalError},
		{"uint out of range", "UInt8", uint64(256), cherr.CannotConvertType},
		{"int out of range", "Int16", int64(-40000), cherr.CannotConvertType},
		{"fixedstring too long", "FixedString(2)", "abc", cherr.CannotConvertType},
		{"bad uuid", "UUID", "nope", cherr.CannotParseText},
		{"unknown enum", "Enum8('a' = 1)", "z", cherr.CannotConvertType},
		{"date before epoch", "Date", day(1969, 12, 31), cherr.CannotConvertType},
		{"datetime overflow", "DateTime", day(2107, 1, 1), cherr.CannotConvertType},
		{"null in non-nullable lc", "LowCardinality(String)", nil, cherr.LogicalError},
		{"bad array", "Array(UInt8)", "x", cherr.LogicalError},
		{"bad tuple arity", "Tuple(UInt8, UInt8)", []any{uint64(1)}, cherr.LogicalError},
		{"bad map entry", "Map(String, UInt8)", []any{"k"}, cherr.LogicalError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			enc, err := NewEncoder("Native", io.Discard, []Column{col("c", c.typ)}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			err = enc.WriteRows([][]any{{c.val}})
			if !cherr.Is(err, c.code) {
				t.Fatalf("got %v, want code %d", err, c.code)
			}
		})
	}
	if _, err := NewEncoder("Native", io.Discard, []Column{col("st", "AggregateFunction(uniqExact, String)")}, Options{}); !cherr.Is(err, cherr.NotImplemented) {
		t.Fatalf("AggregateFunction output: got %v", err)
	}
	if _, err := NewEncoder("JSON", io.Discard, []Column{col("st", "Array(AggregateFunction(sum, UInt64))")}, Options{}); !cherr.Is(err, cherr.NotImplemented) {
		t.Fatalf("nested AggregateFunction output: got %v", err)
	}
	if _, err := NewEncoder("Null", io.Discard, []Column{col("st", "AggregateFunction(sum, UInt64)")}, Options{}); err != nil {
		t.Fatalf("Null format accepts anything: %v", err)
	}
	enc, _ := NewEncoder("Native", io.Discard, []Column{col("a", "UInt8")}, Options{})
	if err := enc.WriteRows([][]any{{uint64(1), uint64(2)}}); !cherr.Is(err, cherr.LogicalError) {
		t.Fatalf("row width: got %v", err)
	}
}

func TestNativeDecodeMapping(t *testing.T) {
	src := []Column{col("b", "UInt8"), col("extra", "String"), col("a", "String")}
	data := encodeNative(t, src, [][]any{{uint64(7), "x", "s1"}, {uint64(9), "y", "s2"}}, 0)

	target := []Column{col("a", "String"), col("b", "UInt64"), col("c", "Nullable(String)")}
	dec, err := NewDecoder("Native", bytes.NewReader(data), target, Options{})
	if err != nil {
		t.Fatal(err)
	}
	assertRows(t, readAll(t, dec), [][]any{{"s1", uint64(7), Missing}, {"s2", uint64(9), Missing}})

	dec, _ = NewDecoder("Native", bytes.NewReader(data), target, Options{Settings: map[string]string{"input_format_skip_unknown_fields": "0"}})
	if _, err := dec.Next(); !cherr.Is(err, cherr.IncorrectData) {
		t.Fatalf("unknown column: got %v", err)
	}
}

func TestNativeDecodeCorrupt(t *testing.T) {
	cols := []Column{col("a", "String"), col("arr", "Array(UInt8)")}
	data := encodeNative(t, cols, [][]any{{"hello", []any{uint64(1), uint64(2)}}}, 0)
	for n := 1; n < len(data); n++ {
		dec, err := NewDecoder("Native", bytes.NewReader(data[:n]), cols, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := dec.Next(); err == nil || err == io.EOF {
			t.Fatalf("truncated at %d: expected error, got %v", n, err)
		}
	}
	bad := []byte{1, 1, 1, 'a', 4, 'J', 'u', 'n', 'k'}
	dec, _ := NewDecoder("Native", bytes.NewReader(bad), cols, Options{})
	if _, err := dec.Next(); !cherr.Is(err, cherr.UnknownType) {
		t.Fatalf("unknown type: got %v", err)
	}
	huge := []byte{1, 0xff, 0xff, 0xff, 0xff, 0x0f}
	dec, _ = NewDecoder("Native", bytes.NewReader(huge), cols, Options{})
	if _, err := dec.Next(); !cherr.Is(err, cherr.IncorrectData) {
		t.Fatalf("huge block: got %v", err)
	}
	dec, _ = NewDecoder("Native", bytes.NewReader(nil), cols, Options{})
	if _, err := dec.Next(); err != io.EOF {
		t.Fatalf("empty input: got %v", err)
	}
}

func TestNativeDateTime64Negative(t *testing.T) {
	cols := []Column{col("d", "DateTime64(3)"), col("d9", "DateTime64(9)")}
	v := time.Date(1969, 12, 31, 23, 59, 58, 500000000, time.UTC)
	data := encodeNative(t, cols, [][]any{{v, v}}, 0)
	dec, _ := NewDecoder("Native", bytes.NewReader(data), cols, Options{})
	assertRows(t, readAll(t, dec), [][]any{{v, v}})
	// -1500 ms little-endian.
	if !bytes.Contains(data, []byte{0x24, 0xfa, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) {
		t.Fatalf("unexpected DateTime64 ticks encoding: %x", data)
	}
}

func BenchmarkNativeEncode100k(b *testing.B) {
	cols := []Column{
		col("id", "UInt64"), col("name", "String"), col("ts", "DateTime64(9, 'UTC')"),
		col("n", "Nullable(Int32)"), col("lc", "LowCardinality(String)"), col("tags", "Array(String)"),
	}
	ts := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	rows := make([][]any, 100_000)
	for i := range rows {
		var n any
		if i%3 != 0 {
			n = int64(i)
		}
		rows[i] = []any{uint64(i), "name-" + strconv.Itoa(i), ts, n, "region-" + strconv.Itoa(i%4), []any{"a", "b"}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		enc, err := NewEncoder("Native", io.Discard, cols, Options{ClientProtocolVersion: chGoRevision})
		if err != nil {
			b.Fatal(err)
		}
		for start := 0; start < len(rows); start += 65536 {
			if err := enc.WriteRows(rows[start:min(start+65536, len(rows))]); err != nil {
				b.Fatal(err)
			}
		}
		if err := enc.Finish(Stats{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNativeDecode100k(b *testing.B) {
	cols := []Column{col("id", "UInt64"), col("name", "String"), col("lc", "LowCardinality(String)")}
	rows := make([][]any, 100_000)
	for i := range rows {
		rows[i] = []any{uint64(i), "name-" + strconv.Itoa(i), "region-" + strconv.Itoa(i%4)}
	}
	data := encodeNative(b, cols, rows, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		dec, _ := NewDecoder("Native", bytes.NewReader(data), cols, Options{})
		for {
			if _, err := dec.Next(); err != nil {
				if err != io.EOF {
					b.Fatal(err)
				}
				break
			}
		}
	}
}
