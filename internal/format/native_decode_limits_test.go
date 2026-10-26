package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/limits"
)

// allocated returns the bytes f allocates on the heap.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// nativeBlock starts a one-column Native block of column c; data follows as
// little-endian UInt64 values.
func nativeBlock(nrows uint64, typ string, data ...uint64) []byte {
	return nativeColumn(nativeHeader(1, nrows), "c", typ, data...)
}

func nativeHeader(ncols, nrows uint64) []byte {
	return binary.AppendUvarint(binary.AppendUvarint(nil, ncols), nrows)
}

// nativeColumn appends the name and type of a column to b, then data as
// little-endian UInt64 values.
func nativeColumn(b []byte, name, typ string, data ...uint64) []byte {
	b = binary.AppendUvarint(b, uint64(len(name)))
	b = append(b, name...)
	b = binary.AppendUvarint(b, uint64(len(typ)))
	b = append(b, typ...)
	for _, v := range data {
		b = binary.LittleEndian.AppendUint64(b, v)
	}
	return b
}

// decodeNativeErr decodes body into column c of type typ until an error and
// returns it with the bytes allocated meanwhile.
func decodeNativeErr(t *testing.T, typ string, body []byte) (error, uint64) {
	t.Helper()
	var err error
	n := allocated(func() {
		dec, _ := NewDecoder("Native", bytes.NewReader(body), []Column{col("c", typ)}, Options{})
		for err == nil {
			_, err = dec.Next()
		}
	})
	return err, n
}

func TestNativeShortBodyAllocatesLittle(t *testing.T) {
	const declared = 1 << 23
	lcFlags := uint64(lcHasAdditionalKeysBit)
	cases := []struct {
		name, typ string
		body      []byte
	}{
		{"rows", "UInt64", nativeBlock(declared, "UInt64")},
		{"string length", "String", append(nativeBlock(1, "String"), 0x80, 0x80, 0x80, 0x04)},
		{"fixed string", "FixedString(16777215)", nativeBlock(4, "FixedString(16777215)")},
		{"nullable mask", "Nullable(UInt8)", nativeBlock(declared, "Nullable(UInt8)")},
		{"array offsets", "Array(UInt8)", nativeBlock(declared, "Array(UInt8)")},
		{"array elements", "Array(UInt8)", nativeBlock(1, "Array(UInt8)", declared)},
		{"map elements", "Map(String, UInt8)", nativeBlock(1, "Map(String, UInt8)", declared)},
		{"tuple", "Tuple(UInt8, String)", nativeBlock(declared, "Tuple(UInt8, String)")},
		{"tuple with an empty tuple", "Tuple(Tuple(), UInt8)", nativeBlock(declared, "Tuple(Tuple(), UInt8)")},
		{"map of empty tuples", "Map(String, Tuple())", nativeBlock(1, "Map(String, Tuple())", declared)},
		{"low cardinality rows", "LowCardinality(String)", nativeBlock(declared, "LowCardinality(String)", 1)},
		{"low cardinality dictionary", "LowCardinality(String)", nativeBlock(1, "LowCardinality(String)", 1, lcFlags, declared)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err, n := decodeNativeErr(t, tc.typ, tc.body)
			if n > 4<<20 {
				t.Errorf("allocated %d bytes for a %d-byte body", n, len(tc.body))
			}
			if !cherr.Is(err, cherr.CannotReadAllData) {
				t.Errorf("got %v, want CANNOT_READ_ALL_DATA", err)
			}
		})
	}
}

func TestNativeFixedStringSizeIsCapped(t *testing.T) {
	typ := chtype.Type{Kind: chtype.FixedString, N: limits.MaxFixedStringSize + 1}
	if _, err := compileNative(typ); !cherr.Is(err, cherr.ArgumentOutOfBound) {
		t.Fatalf("FixedString(%d): got %v, want ARGUMENT_OUT_OF_BOUND", typ.N, err)
	}
	err, n := decodeNativeErr(t, "String", nativeBlock(1, "FixedString(16777216)"))
	if err == nil || err == io.EOF {
		t.Fatal("FixedString(16777216) was accepted")
	}
	if n > 4<<20 {
		t.Fatalf("allocated %d bytes", n)
	}
}

func TestNativeStringSizeIsCapped(t *testing.T) {
	body := binary.AppendUvarint(nativeBlock(1, "String"), maxNativeString+1)
	err, n := decodeNativeErr(t, "String", body)
	if !cherr.Is(err, cherr.TooLargeStringSize) {
		t.Errorf("got %v, want TOO_LARGE_STRING_SIZE", err)
	}
	if n > 4<<20 {
		t.Errorf("allocated %d bytes", n)
	}
}

func TestNativeEmptyTupleColumnIsShared(t *testing.T) {
	const declared = 1 << 23
	typ := "Tuple(" + strings.Repeat("Tuple(), ", 999) + "Tuple())"
	var rows [][]any
	n := allocated(func() {
		dec, _ := NewDecoder("Native", bytes.NewReader(nativeBlock(declared, typ)), []Column{col("c", typ)}, Options{})
		for range 1000 {
			row, err := dec.Next()
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
	})
	if n > 8<<20 {
		t.Errorf("allocated %d bytes for 1000 of %d rows", n, declared)
	}
	if len(rows[999][0].([]any)) != 1000 {
		t.Errorf("got %v", rows[999])
	}
}

func TestNativeValuesWithoutDataAreCappedPerBlock(t *testing.T) {
	const limit = maxNativeValuesWithoutData
	many := strings.Repeat("Tuple(), ", 99) + "Tuple())"
	cases := []struct {
		name string
		typ  string // of column c
		body []byte
	}{
		{"array", "Array(Tuple())", nativeBlock(1, "Array(Tuple())", limit+1<<10)},
		{"map", "Map(Tuple(), Tuple())", nativeBlock(1, "Map(Tuple(), Tuple())", limit+1<<10)},
		{"shared by the columns", "Array(Tuple())",
			nativeColumn(nativeColumn(nativeHeader(2, 1), "c", "Array(Tuple())", limit/2+1<<10), "d", "Array(Tuple())", limit/2+1<<10)},
		{"tuple of many fields", "Tuple(UInt8, " + many,
			append(nativeBlock(limit/50, "Tuple(UInt8, "+many), make([]byte, limit/50)...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err, n := decodeNativeErr(t, tc.typ, tc.body)
			if !cherr.Is(err, cherr.TooLargeArraySize) {
				t.Errorf("got %v, want TOO_LARGE_ARRAY_SIZE", err)
			}
			if n > 64<<20 {
				t.Errorf("allocated %d bytes for a %d-byte body", n, len(tc.body))
			}
		})
	}
}

func TestNativeEmptyElementRowsShareOneRun(t *testing.T) {
	const rows, perRow = 1000, 2000
	offs := make([]uint64, rows)
	for i := range offs {
		offs[i] = uint64(i+1) * perRow
	}
	for _, typ := range []string{"Array(Tuple())", "Map(Tuple(), Tuple())"} {
		t.Run(typ, func(t *testing.T) {
			var got [][]any
			n := allocated(func() {
				dec, _ := NewDecoder("Native", bytes.NewReader(nativeBlock(rows, typ, offs...)), []Column{col("c", typ)}, Options{})
				got = readAll(t, dec)
			})
			if n > 4<<20 {
				t.Errorf("allocated %d bytes", n)
			}
			if len(got) != rows {
				t.Fatalf("got %d rows", len(got))
			}
			for _, row := range got {
				if v := row[0].([]any); len(v) != perRow || cap(v) != perRow {
					t.Fatalf("got a row of len %d cap %d, want %d", len(v), cap(v), perRow)
				}
			}
		})
	}
}

// TestNativeValuesWithoutDataCountOneSlotEach checks that a value without
// bytes on the wire counts once against the block, whatever its tuple
// nesting.
func TestNativeValuesWithoutDataCountOneSlotEach(t *testing.T) {
	const limit = maxNativeValuesWithoutData
	wide := "Tuple(" + strings.Repeat("Tuple(), ", 99) + "Tuple())"
	cases := []struct {
		name, typ string
		rows      uint64
		body      []byte
	}{
		{"array of nested empty tuples", "Array(Tuple(Tuple(), Tuple()))", 1, nativeBlock(1, "Array(Tuple(Tuple(), Tuple()))", limit/2)},
		{"tuple with a wide empty tuple", "Tuple(UInt8, " + wide + ")", limit / 50,
			append(nativeBlock(limit/50, "Tuple(UInt8, "+wide+")"), make([]byte, limit/50)...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dec, _ := NewDecoder("Native", bytes.NewReader(tc.body), []Column{col("c", tc.typ)}, Options{})
			if got := readAll(t, dec); len(got) != int(tc.rows) {
				t.Fatalf("got %d rows, want %d", len(got), tc.rows)
			}
		})
	}
}

func TestNativeValuesWithoutDataAllowanceGrowsWithData(t *testing.T) {
	const elems = maxNativeValuesWithoutData + 1000
	body := nativeColumn(nativeHeader(2, 1), "s", "String")
	body = append(binary.AppendUvarint(body, 2000), strings.Repeat("x", 2000)...)
	body = nativeColumn(body, "c", "Array(Tuple())", elems)
	cols := []Column{col("s", "String"), col("c", "Array(Tuple())")}
	dec, _ := NewDecoder("Native", bytes.NewReader(body), cols, Options{})
	rows := readAll(t, dec)
	if len(rows) != 1 || len(rows[0][1].([]any)) != elems {
		t.Fatalf("got %d rows", len(rows))
	}
	dec, _ = NewDecoder("Native", bytes.NewReader(nativeBlock(2, "Array(Tuple())", 0, 2)), []Column{col("c", "Array(Tuple())")}, Options{})
	assertRows(t, readAll(t, dec), [][]any{{[]any{}}, {[]any{[]any{}, []any{}}}})
}

func TestNativeDecodesBlocksLargerThanAChunk(t *testing.T) {
	const rows = 2*limits.NativeAllocChunkRows + 123
	cols := []Column{
		col("u", "UInt64"), col("s", "String"), col("n", "Nullable(Int32)"),
		col("a", "Array(UInt8)"), col("lc", "LowCardinality(String)"), col("t", "Tuple(Tuple(), UInt8)"),
	}
	want := make([][]any, rows)
	for i := range want {
		var n any
		if i%3 != 0 {
			n = int64(i)
		}
		want[i] = []any{uint64(i), fmt.Sprint(i), n, []any{uint64(i % 7)}, fmt.Sprint(i % 5), []any{[]any{}, uint64(i % 9)}}
	}
	dec, _ := NewDecoder("Native", bytes.NewReader(encodeNative(t, cols, want, 0)), cols, Options{})
	assertRows(t, readAll(t, dec), want)
}
