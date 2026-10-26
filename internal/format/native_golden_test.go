package format

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// Golden Native tests: testdata/native/<case>.rev<N>.hex holds the exact
// bytes real ClickHouse 25.8 returns for the case's query with
// default_format=Native (and client_protocol_version=N when N > 0).
// Regenerate with:
//
//	FORMAT_GOLDEN_CH=http://localhost:18127 go test ./internal/format -run TestNativeGolden
const goldenEnv = "FORMAT_GOLDEN_CH"

// clickhouse-go's TCP protocol revision (DBMS_TCP_PROTOCOL_VERSION).
const chGoRevision = 54460

// 54300 predates DateTime timezones and LowCardinality in the protocol.
var goldenRevisions = []uint64{0, 54300, chGoRevision}

type nativeCase struct {
	name  string
	query string
	cols  []Column
	rows  [][]any
}

func col(name, typ string) Column { return Column{Name: name, Type: chtype.MustParse(typ)} }

func utc(y int, mo time.Month, d, h, mi, s, ns int) time.Time {
	return time.Date(y, mo, d, h, mi, s, ns, time.UTC)
}

func day(y int, mo time.Month, d int) time.Time { return utc(y, mo, d, 0, 0, 0, 0) }

// chNaN is the NaN bit pattern ClickHouse produces for `nan`.
var chNaN = math.Float64frombits(0x7ff8000000000000)

func nativeCases() []nativeCase {
	manyRows := make([][]any, 1000)
	for i := range manyRows {
		manyRows[i] = []any{uint64(i), strconv.Itoa(i)}
	}
	lc300 := make([][]any, 300)
	for i := range lc300 {
		lc300[i] = []any{strconv.Itoa(i)}
	}
	long := strings.Repeat("x", 200)
	return []nativeCase{
		{
			name: "ints",
			query: `SELECT toUInt8(number*100) u8, toUInt16(number*30000) u16, toUInt32(number*2000000000) u32,
				toUInt64(number)*9000000000000000000 u64, toInt8(number*60-60) i8, toInt16(number*-16000) i16,
				toInt32(number*1000000000-1000000000) i32, toInt64(number)*-4000000000000000000 i64 FROM numbers(3)`,
			cols: []Column{col("u8", "UInt8"), col("u16", "UInt16"), col("u32", "UInt32"), col("u64", "UInt64"),
				col("i8", "Int8"), col("i16", "Int16"), col("i32", "Int32"), col("i64", "Int64")},
			rows: [][]any{
				{uint64(0), uint64(0), uint64(0), uint64(0), int64(-60), int64(0), int64(-1000000000), int64(0)},
				{uint64(100), uint64(30000), uint64(2000000000), uint64(9000000000000000000), int64(0), int64(-16000), int64(0), int64(-4000000000000000000)},
				{uint64(200), uint64(60000), uint64(4000000000), uint64(18000000000000000000), int64(60), int64(-32000), int64(1000000000), int64(-8000000000000000000)},
			},
		},
		{
			name:  "int_extremes",
			query: `SELECT toUInt64(18446744073709551615) a, toInt64(-9223372036854775808) b, toInt8(-128) c, toUInt8(255) d`,
			cols:  []Column{col("a", "UInt64"), col("b", "Int64"), col("c", "Int8"), col("d", "UInt8")},
			rows:  [][]any{{uint64(math.MaxUint64), int64(math.MinInt64), int64(-128), uint64(255)}},
		},
		{
			name:  "floats",
			query: `SELECT toFloat32(number/4) f32, number/3 f64 FROM numbers(3)`,
			cols:  []Column{col("f32", "Float32"), col("f64", "Float64")},
			rows: [][]any{
				{0.0, 0.0},
				{0.25, 1.0 / 3},
				{0.5, 2.0 / 3},
			},
		},
		{
			name:  "float_specials",
			query: `SELECT nan a, inf b, -inf c, -0. d, toFloat32(-inf) e, 1e308 f`,
			cols:  []Column{col("a", "Float64"), col("b", "Float64"), col("c", "Float64"), col("d", "Float64"), col("e", "Float32"), col("f", "Float64")},
			rows:  [][]any{{chNaN, math.Inf(1), math.Inf(-1), math.Copysign(0, -1), math.Inf(-1), 1e308}},
		},
		{
			name:  "bool_string",
			query: `SELECT toBool(number % 2) b, toString(number) s, if(number = 2, repeat('x', 200), '') l, 'é\0\n' u FROM numbers(3)`,
			cols:  []Column{col("b", "Bool"), col("s", "String"), col("l", "String"), col("u", "String")},
			rows: [][]any{
				{false, "0", "", "é\x00\n"},
				{true, "1", "", "é\x00\n"},
				{false, "2", long, "é\x00\n"},
			},
		},
		{
			name:  "fixedstring",
			query: `SELECT toFixedString(repeat('a', number), 3) fs FROM numbers(4)`,
			cols:  []Column{col("fs", "FixedString(3)")},
			rows:  [][]any{{"\x00\x00\x00"}, {"a\x00\x00"}, {"aa\x00"}, {"aaa"}},
		},
		{
			name:  "uuid",
			query: `SELECT if(number = 0, toUUID('00112233-4455-6677-8899-aabbccddeeff'), toUUID('f0e1d2c3-b4a5-9687-7869-5a4b3c2d1e0f')) u FROM numbers(2)`,
			cols:  []Column{col("u", "UUID")},
			rows:  [][]any{{"00112233-4455-6677-8899-aabbccddeeff"}, {"f0e1d2c3-b4a5-9687-7869-5a4b3c2d1e0f"}},
		},
		{
			name: "dates",
			query: `SELECT toDate('2024-01-02') + number d, toDate32('1900-01-01') + number d32,
				toDateTime('2024-01-02 03:04:05', 'UTC') + number dt, toDateTime('2024-01-02 03:04:05') dtz,
				toDateTime64('2024-01-02 03:04:05.123456789', 9, 'UTC') + number d9,
				toDateTime64('1960-05-06 07:08:09.5', 3, 'Asia/Tokyo') d3,
				toDateTime64(0, 0) d0 FROM numbers(2)`,
			cols: []Column{col("d", "Date"), col("d32", "Date32"), col("dt", "DateTime('UTC')"), col("dtz", "DateTime"),
				col("d9", "DateTime64(9, 'UTC')"), col("d3", "DateTime64(3, 'Asia/Tokyo')"), col("d0", "DateTime64(0)")},
			rows: [][]any{
				{day(2024, 1, 2), day(1900, 1, 1), utc(2024, 1, 2, 3, 4, 5, 0), utc(2024, 1, 2, 3, 4, 5, 0),
					utc(2024, 1, 2, 3, 4, 5, 123456789), utc(1960, 5, 5, 22, 8, 9, 500000000), chtype.Epoch},
				{day(2024, 1, 3), day(1900, 1, 2), utc(2024, 1, 2, 3, 4, 6, 0), utc(2024, 1, 2, 3, 4, 5, 0),
					utc(2024, 1, 2, 3, 4, 6, 123456789), utc(1960, 5, 5, 22, 8, 9, 500000000), chtype.Epoch},
			},
		},
		{
			name:  "enums",
			query: `SELECT CAST(if(number = 0, 'a', 'b') AS Enum8('a' = 1, 'b' = -2)) e8, CAST(if(number = 0, 'x', 'y') AS Enum16('x' = 1000, 'y' = -1000)) e16 FROM numbers(2)`,
			cols:  []Column{col("e8", "Enum8('b' = -2, 'a' = 1)"), {Name: "e16", Type: chtype.Type{Kind: chtype.Enum16, EnumValues: []chtype.EnumValue{{Name: "y", Value: -1000}, {Name: "x", Value: 1000}}}}},
			rows:  [][]any{{"a", "x"}, {"b", "y"}},
		},
		{
			// ClickHouse leaves whatever the computation produced in the nested
			// slot of a NULL row (e.g. if(c, NULL, x) keeps x); we write the
			// type's default, which matches literal-built NULLs like these.
			name: "nullable",
			query: `SELECT [0, NULL, 2][number + 1] n, [NULL, '1', '2'][number + 1] ns,
				CAST(arrayJoin([NULL, 'b']) AS Nullable(Enum8('a' = 1, 'b' = 2))) ne,
				[toDateTime64('2024-01-02 03:04:05.5', 1, 'UTC'), NULL][number % 2 + 1] nd FROM numbers(1)`,
			cols: []Column{col("n", "Nullable(UInt8)"), col("ns", "Nullable(String)"),
				col("ne", "Nullable(Enum8('a' = 1, 'b' = 2))"), col("nd", "Nullable(DateTime64(1, 'UTC'))")},
			rows: [][]any{
				{uint64(0), nil, nil, utc(2024, 1, 2, 3, 4, 5, 500000000)},
				{uint64(0), nil, "b", utc(2024, 1, 2, 3, 4, 5, 500000000)},
			},
		},
		{
			name:  "nullable_slots",
			query: `SELECT [0, NULL, 2][number + 1] n, [NULL, '1', '2'][number + 1] ns, [1.5, NULL, 2][number + 1] nf FROM numbers(3)`,
			cols:  []Column{col("n", "Nullable(UInt8)"), col("ns", "Nullable(String)"), col("nf", "Nullable(Float64)")},
			rows: [][]any{
				{uint64(0), nil, 1.5},
				{nil, "1", nil},
				{uint64(2), "2", 2.0},
			},
		},
		{
			name: "nullable_types",
			query: `SELECT [toDate('2024-01-01'), NULL][number + 1] d, [toUUID('00112233-4455-6677-8899-aabbccddeeff'), NULL][number + 1] u,
				[toFixedString('ab', 2), NULL][number + 1] f, [toDateTime('2024-01-01 00:00:00', 'UTC'), NULL][number + 1] dt,
				[true, NULL][number + 1] b, [toInt32(-1), NULL][number + 1] i, toLowCardinality([1.5, NULL][number + 1]) lf,
				toLowCardinality([toDate('2024-01-01'), NULL][number + 1]) ld,
				[toDateTime64('2024-01-01 00:00:00.25', 2, 'UTC'), NULL][number + 1] n64 FROM numbers(2)`,
			cols: []Column{col("d", "Nullable(Date)"), col("u", "Nullable(UUID)"), col("f", "Nullable(FixedString(2))"),
				col("dt", "Nullable(DateTime('UTC'))"), col("b", "Nullable(Bool)"), col("i", "Nullable(Int32)"),
				col("lf", "LowCardinality(Nullable(Float64))"), col("ld", "LowCardinality(Nullable(Date))"),
				col("n64", "Nullable(DateTime64(2, 'UTC'))")},
			rows: [][]any{
				{day(2024, 1, 1), "00112233-4455-6677-8899-aabbccddeeff", "ab", day(2024, 1, 1), true, int64(-1), 1.5, day(2024, 1, 1), utc(2024, 1, 1, 0, 0, 0, 250000000)},
				{nil, nil, nil, nil, nil, nil, nil, nil, nil},
			},
		},
		{
			name:  "null_literal",
			query: `SELECT NULL n`,
			cols:  []Column{col("n", "Nullable(Nothing)")},
			rows:  [][]any{{nil}},
		},
		{
			name: "lowcardinality",
			query: `SELECT toLowCardinality(toString(number % 2)) l, toLowCardinality(if(number = 1, NULL, toString(number))) ln,
				toLowCardinality(['b', '', 'a', 'b'][number + 1]) ld, toLowCardinality(number) lu FROM numbers(4)`,
			cols: []Column{col("l", "LowCardinality(String)"), col("ln", "LowCardinality(Nullable(String))"),
				col("ld", "LowCardinality(String)"), col("lu", "LowCardinality(UInt64)")},
			rows: [][]any{
				{"0", "0", "b", uint64(0)},
				{"1", nil, "", uint64(1)},
				{"0", "2", "a", uint64(2)},
				{"1", "3", "b", uint64(3)},
			},
		},
		{
			name:  "lowcardinality_u16",
			query: `SELECT toLowCardinality(toString(number)) l FROM numbers(300)`,
			cols:  []Column{col("l", "LowCardinality(String)")},
			rows:  lc300,
		},
		{
			name: "arrays",
			query: `SELECT [number, number + 1] a, range(number) r, [[1, 2], [3]] aa, [] e, [NULL] an,
				[toLowCardinality('a'), toLowCardinality('b')] alc, ['x', NULL] ans FROM numbers(2)`,
			cols: []Column{col("a", "Array(UInt64)"), col("r", "Array(UInt64)"), col("aa", "Array(Array(UInt8))"),
				col("e", "Array(Nothing)"), col("an", "Array(Nullable(Nothing))"),
				col("alc", "Array(LowCardinality(String))"), col("ans", "Array(Nullable(String))")},
			rows: [][]any{
				{[]any{uint64(0), uint64(1)}, []any{}, []any{[]any{uint64(1), uint64(2)}, []any{uint64(3)}}, []any{}, []any{nil}, []any{"a", "b"}, []any{"x", nil}},
				{[]any{uint64(1), uint64(2)}, []any{uint64(0)}, []any{[]any{uint64(1), uint64(2)}, []any{uint64(3)}}, []any{}, []any{nil}, []any{"a", "b"}, []any{"x", nil}},
			},
		},
		{
			name:  "tuples",
			query: `SELECT (number, toString(number)) t, CAST((1, 'b'), 'Tuple(x UInt8, y String)') nt, tuple(toLowCardinality('z'), 1) tlc FROM numbers(2)`,
			cols:  []Column{col("t", "Tuple(UInt64, String)"), col("nt", "Tuple(x UInt8, y String)"), col("tlc", "Tuple(LowCardinality(String), UInt8)")},
			rows: [][]any{
				{[]any{uint64(0), "0"}, []any{uint64(1), "b"}, []any{"z", uint64(1)}},
				{[]any{uint64(1), "1"}, []any{uint64(1), "b"}, []any{"z", uint64(1)}},
			},
		},
		{
			// clickhouse-go cannot materialise unnamed nested tuples generically;
			// the chgo tests skip this case (see chgoUnsupported).
			name:  "nested_tuple",
			query: `SELECT ((1, 2), [3]) tn, CAST(((1, 'a'), [(2, 'b')]) AS Tuple(p Tuple(x UInt8, y String), q Array(Tuple(UInt8, String)))) tnn`,
			cols:  []Column{col("tn", "Tuple(Tuple(UInt8, UInt8), Array(UInt8))"), col("tnn", "Tuple(p Tuple(x UInt8, y String), q Array(Tuple(UInt8, String)))")},
			rows: [][]any{{
				[]any{[]any{uint64(1), uint64(2)}, []any{uint64(3)}},
				[]any{[]any{uint64(1), "a"}, []any{[]any{uint64(2), "b"}}},
			}},
		},
		{
			name:  "maps",
			query: `SELECT map('k', number, 'j', 2) m, map() em, map(toLowCardinality('a'), 1) mlc, map(1, [2, 3]) ma FROM numbers(2)`,
			cols: []Column{col("m", "Map(String, UInt64)"), {Name: "em", Type: chtype.Type{Kind: chtype.Map, Key: &chtype.TNothing, Elem: &chtype.TNothing}},
				col("mlc", "Map(LowCardinality(String), UInt8)"), col("ma", "Map(UInt8, Array(UInt8))")},
			rows: [][]any{
				{[]any{[]any{"k", uint64(0)}, []any{"j", uint64(2)}}, []any{}, []any{[]any{"a", uint64(1)}}, []any{[]any{uint64(1), []any{uint64(2), uint64(3)}}}},
				{[]any{[]any{"k", uint64(1)}, []any{"j", uint64(2)}}, []any{}, []any{[]any{"a", uint64(1)}}, []any{[]any{uint64(1), []any{uint64(2), uint64(3)}}}},
			},
		},
		{
			name:  "simple_aggregate",
			query: `SELECT CAST(number AS SimpleAggregateFunction(sum, UInt64)) s, CAST(toString(number) AS SimpleAggregateFunction(any, String)) a FROM numbers(2)`,
			cols:  []Column{col("s", "SimpleAggregateFunction(sum, UInt64)"), col("a", "SimpleAggregateFunction(any, String)")},
			rows:  [][]any{{uint64(0), "0"}, {uint64(1), "1"}},
		},
		{
			name:  "many_rows",
			query: `SELECT number, toString(number) s FROM numbers(1000)`,
			cols:  []Column{col("number", "UInt64"), col("s", "String")},
			rows:  manyRows,
		},
		{
			name:  "column_names",
			query: "SELECT 1 AS `weird name ✓`, 2 AS `plus(1, 1)`",
			cols:  []Column{col("weird name ✓", "UInt8"), col("plus(1, 1)", "UInt8")},
			rows:  [][]any{{uint64(1), uint64(2)}},
		},
	}
}

func goldenPath(name string, rev uint64) string {
	return filepath.Join("testdata", "native", fmt.Sprintf("%s.rev%d.hex", name, rev))
}

func fetchNative(t *testing.T, base, query string, rev uint64) []byte {
	t.Helper()
	q := url.Values{"default_format": {"Native"}, "max_block_size": {"100000"}}
	if rev > 0 {
		q.Set("client_protocol_version", strconv.FormatUint(rev, 10))
	}
	resp, err := http.Post(base+"/?"+q.Encode(), "text/plain", strings.NewReader(query))
	if err != nil {
		t.Fatalf("query ClickHouse: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ClickHouse %d: %s", resp.StatusCode, body)
	}
	return body
}

func readGolden(t *testing.T, name string, rev uint64) []byte {
	t.Helper()
	path := goldenPath(name, rev)
	if base := os.Getenv(goldenEnv); base != "" {
		c := findCase(t, name)
		data := fetchNative(t, base, c.query, rev)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(hexDump(data)), 0o644); err != nil {
			t.Fatal(err)
		}
		return data
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing golden file %s (set %s to regenerate): %v", path, goldenEnv, err)
	}
	data, err := hex.DecodeString(strings.Join(strings.Fields(string(raw)), ""))
	if err != nil {
		t.Fatalf("bad golden file %s: %v", path, err)
	}
	return data
}

// hexDump renders data as 32 hex bytes per line.
func hexDump(data []byte) string {
	var sb strings.Builder
	for i := 0; i < len(data); i += 32 {
		sb.WriteString(hex.EncodeToString(data[i:min(i+32, len(data))]))
		sb.WriteByte('\n')
	}
	return sb.String()
}

func findCase(t *testing.T, name string) nativeCase {
	for _, c := range nativeCases() {
		if c.name == name {
			return c
		}
	}
	t.Fatalf("no case %s", name)
	return nativeCase{}
}

func encodeNative(t testing.TB, cols []Column, rows [][]any, rev uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := NewEncoder("Native", &buf, cols, Options{ClientProtocolVersion: rev})
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	if err := enc.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := enc.Finish(Stats{}); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return buf.Bytes()
}

func TestNativeGolden(t *testing.T) {
	for _, c := range nativeCases() {
		for _, rev := range goldenRevisions {
			t.Run(fmt.Sprintf("%s/rev%d", c.name, rev), func(t *testing.T) {
				want := readGolden(t, c.name, rev)
				got := encodeNative(t, c.cols, c.rows, rev)
				if !bytes.Equal(got, want) {
					t.Fatalf("Native bytes differ from ClickHouse\n got: %s\nwant: %s\nfirst difference at byte %d",
						hex.EncodeToString(got), hex.EncodeToString(want), firstDiff(got, want))
				}
			})
		}
	}
}

func firstDiff(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// TestNativeDecodeGolden decodes ClickHouse-produced Native (revision 0, the
// INSERT layout) and checks the canonical rows.
func TestNativeDecodeGolden(t *testing.T) {
	for _, c := range nativeCases() {
		t.Run(c.name, func(t *testing.T) {
			for _, col := range c.cols {
				if _, err := chtype.Parse(col.Type.String()); err != nil {
					t.Skipf("chtype cannot parse %s (needed to read the Native header): %v", col.Type.String(), err)
				}
			}
			data := readGolden(t, c.name, 0)
			dec, err := NewDecoder("Native", bytes.NewReader(data), c.cols, Options{})
			if err != nil {
				t.Fatal(err)
			}
			got := readAll(t, dec)
			assertRows(t, got, c.rows)
		})
	}
}

func readAll(t testing.TB, dec Decoder) [][]any {
	t.Helper()
	var rows [][]any
	for {
		r, err := dec.Next()
		if err == io.EOF {
			return rows
		}
		if err != nil {
			t.Fatalf("Next (after %d rows): %v", len(rows), err)
		}
		rows = append(rows, r)
	}
}

func assertRows(t testing.TB, got, want [][]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if !valuesEqual(got[i], want[i]) {
			t.Fatalf("row %d:\n got %#v\nwant %#v", i, got[i], want[i])
		}
	}
}

// valuesEqual compares canonical values exactly (NaN equals NaN with the
// same bits, -0 differs from 0, times by instant).
func valuesEqual(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		return ok && math.Float64bits(x) == math.Float64bits(y)
	case time.Time:
		y, ok := b.(time.Time)
		return ok && x.Equal(y) && x.Location() == time.UTC && y.Location() == time.UTC
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !valuesEqual(x[i], y[i]) {
				return false
			}
		}
		return true
	}
	return a == b
}
