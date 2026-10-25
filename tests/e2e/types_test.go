//go:build e2e

package e2e

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// typedRow is one row of the all-types table.
type typedRow struct {
	ID     uint64
	Tenant string
	LC     string
	NStr   *string
	NU64   *uint64
	Tags   []string
	TS     time.Time
	TS64   time.Time
	Day    time.Time
	Flag   bool
	U8     uint8
	U16    uint16
	U32    uint32
	U64    uint64
	I8     int8
	I16    int16
	I32    int32
	I64    int64
	F64    float64
	F32    float32
	UID    uuid.UUID
	Attrs  map[string]uint64
	Nums   []uint32
	LCN    *string
	FS     string
	Kind   string
}

const typesDDL = `CREATE TABLE {db}.typed (
	id     UInt64,
	tenant String,
	lc     LowCardinality(String),
	nstr   Nullable(String),
	nu64   Nullable(UInt64),
	tags   Array(String),
	ts     DateTime,
	ts64   DateTime64(3),
	day    Date,
	flag   Bool,
	u8     UInt8,
	u16    UInt16,
	u32    UInt32,
	u64    UInt64,
	i8     Int8,
	i16    Int16,
	i32    Int32,
	i64    Int64,
	f64    Float64,
	f32    Float32,
	uid    UUID,
	attrs  Map(String, UInt64),
	nums   Array(UInt32),
	lcn    LowCardinality(Nullable(String)),
	fs     FixedString(4),
	kind   Enum8('imp' = 1, 'click' = 2, 'conv' = 3)
) ENGINE = MergeTree ORDER BY (tenant, id)`

const typedCols = "id, tenant, lc, nstr, nu64, tags, ts, ts64, day, flag, u8, u16, u32, u64, i8, i16, i32, i64, f64, f32, uid, attrs, nums, lcn, fs, kind"

func genTyped(n int) []typedRow {
	base := time.Date(2023, 12, 31, 23, 59, 0, 0, time.UTC)
	words := []string{"", "alpha", "beta", "ünïcødé ✓", "tab\there", "quote'and\"dq", "back\\slash", "new\nline"}
	kinds := []string{"imp", "click", "conv"}
	out := make([]typedRow, n)
	for i := range out {
		r := typedRow{
			ID:     uint64(i),
			Tenant: fmt.Sprintf("tenant-%d", i%7),
			LC:     []string{"web", "app", "ctv"}[i%3],
			Tags:   []string{},
			TS:     base.Add(time.Duration(i) * 37 * time.Second),
			TS64:   base.Add(time.Duration(i) * 1234567 * time.Microsecond).Truncate(time.Millisecond),
			Day:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i%400),
			Flag:   i%3 == 0,
			U8:     uint8(i),
			U16:    uint16(i * 13),
			U32:    uint32(i) * 100003,
			U64:    uint64(i) * 1000000007,
			I8:     int8(i%256 - 128),
			I16:    int16(i*7 - 20000),
			I32:    int32(i)*-40000 + 7,
			I64:    int64(i)*-1234567891 + 42,
			F64:    float64(i)/8 - 100,
			F32:    float32(i%1000) / 4,
			UID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprint(i))),
			Attrs:  map[string]uint64{},
			Nums:   []uint32{},
			FS:     fmt.Sprintf("%04d", i%10000),
			Kind:   kinds[i%3],
		}
		if i%4 != 0 {
			s := words[i%len(words)] + fmt.Sprint(i)
			r.NStr = &s
		}
		if i%5 != 0 {
			v := uint64(i) * 3
			r.NU64 = &v
		}
		for j := 0; j < i%4; j++ {
			r.Tags = append(r.Tags, words[(i+j)%len(words)])
			r.Nums = append(r.Nums, uint32(i*j))
		}
		for j := 0; j < i%3; j++ {
			r.Attrs[fmt.Sprintf("k%d", j)] = uint64(i + j)
		}
		if i%6 != 0 {
			s := []string{"red", "green", "blue"}[i%3]
			r.LCN = &s
		}
		out[i] = r
	}
	// Edge values.
	out[1].U64 = math.MaxUint64
	out[1].I64 = math.MinInt64
	out[2].I64 = math.MaxInt64
	out[1].U32 = math.MaxUint32
	out[1].U16 = math.MaxUint16
	out[1].I8, out[2].I8 = math.MinInt8, math.MaxInt8
	out[1].I16, out[2].I16 = math.MinInt16, math.MaxInt16
	out[1].I32, out[2].I32 = math.MinInt32, math.MaxInt32
	out[3].F64 = 1e300
	out[4].F64 = -2.5e-300
	out[5].F64 = 0.1
	return out
}

func TestTypesBatchRoundTrip(t *testing.T) {
	t.Parallel()
	heavy(t)
	db := newDB(t, "types")
	mustExec(t, "", strings.ReplaceAll(typesDDL, "{db}", db))
	conn := openNative(t, db)
	rows := genTyped(5000)

	start := time.Now()
	b, err := conn.PrepareBatch(ctxT(t), "INSERT INTO typed ("+typedCols+")")
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	for _, r := range rows {
		if err := b.Append(r.ID, r.Tenant, r.LC, r.NStr, r.NU64, r.Tags, r.TS, r.TS64, r.Day, r.Flag, r.U8, r.U16, r.U32, r.U64,
			r.I8, r.I16, r.I32, r.I64, r.F64, r.F32, r.UID, r.Attrs, r.Nums, r.LCN, r.FS, r.Kind); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := b.Send(); err != nil {
		t.Fatalf("Send: %v", err)
	}
	t.Logf("inserted %d typed rows in %s", len(rows), time.Since(start))

	t.Run("native_scan", func(t *testing.T) {
		q, err := conn.Query(ctxT(t), "SELECT "+typedCols+" FROM typed ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		i, bad := 0, 0
		for q.Next() {
			var g typedRow
			if err := q.Scan(&g.ID, &g.Tenant, &g.LC, &g.NStr, &g.NU64, &g.Tags, &g.TS, &g.TS64, &g.Day, &g.Flag, &g.U8, &g.U16, &g.U32, &g.U64,
				&g.I8, &g.I16, &g.I32, &g.I64, &g.F64, &g.F32, &g.UID, &g.Attrs, &g.Nums, &g.LCN, &g.FS, &g.Kind); err != nil {
				t.Fatalf("scan row %d: %v", i, err)
			}
			if i < len(rows) {
				if d := diffTyped(g, rows[i]); d != "" {
					if bad++; bad <= 5 {
						t.Errorf("row %d mismatch: %s", i, d)
					}
				}
			}
			i++
		}
		if err := q.Err(); err != nil {
			t.Fatal(err)
		}
		if i != len(rows) {
			t.Errorf("scanned %d rows, want %d", i, len(rows))
		}
		if bad > 0 {
			t.Errorf("%d mismatching rows", bad)
		}
	})

	t.Run("sql_scan", func(t *testing.T) {
		sqlc := openSQL(t, db)
		for _, id := range []uint64{0, 1, 2, 3, 4, 5, 7, 4999} {
			var g typedRow
			err := sqlc.QueryRowContext(ctxT(t), "SELECT "+typedCols+" FROM typed WHERE tenant = ? AND id = ?", rows[id].Tenant, id).Scan(
				&g.ID, &g.Tenant, &g.LC, &g.NStr, &g.NU64, &g.Tags, &g.TS, &g.TS64, &g.Day, &g.Flag, &g.U8, &g.U16, &g.U32, &g.U64,
				&g.I8, &g.I16, &g.I32, &g.I64, &g.F64, &g.F32, &g.UID, &g.Attrs, &g.Nums, &g.LCN, &g.FS, &g.Kind)
			if err != nil {
				t.Errorf("row %d: %v", id, err)
				continue
			}
			if d := diffTyped(g, rows[id]); d != "" {
				t.Errorf("database/sql row %d: %s", id, d)
			}
		}
	})

	t.Run("server_aggregates", func(t *testing.T) {
		var sumU32, cntFlag, nullStr, nullU64, tagsTotal uint64
		var sumI32 int64
		var sumF64 float64
		var sumNU64 uint64
		for _, r := range rows {
			sumU32 += uint64(r.U32)
			sumI32 += int64(r.I32)
			if r.Flag {
				cntFlag++
			}
			if r.NStr == nil {
				nullStr++
			}
			if r.NU64 == nil {
				nullU64++
			} else {
				sumNU64 += *r.NU64
			}
			tagsTotal += uint64(len(r.Tags))
			if r.ID > 5 {
				sumF64 += r.F64
			}
		}
		var gU32, gFlag, gNullStr, gNullU64, gTags, gNU64 uint64
		var gI32 int64
		var gF64 float64
		err := conn.QueryRow(ctxT(t), `SELECT sum(u32), countIf(flag), countIf(isNull(nstr)), countIf(nu64 IS NULL), sum(length(tags)),
			sum(nu64), sum(i32), sumIf(f64, id > 5) FROM typed`).Scan(&gU32, &gFlag, &gNullStr, &gNullU64, &gTags, &gNU64, &gI32, &gF64)
		if err != nil {
			t.Fatal(err)
		}
		if gU32 != sumU32 || gFlag != cntFlag || gNullStr != nullStr || gNullU64 != nullU64 || gTags != tagsTotal || gNU64 != sumNU64 || gI32 != sumI32 {
			t.Errorf("aggregates got u32=%d flag=%d nullstr=%d nullu64=%d tags=%d nu64=%d i32=%d; want %d %d %d %d %d %d %d",
				gU32, gFlag, gNullStr, gNullU64, gTags, gNU64, gI32, sumU32, cntFlag, nullStr, nullU64, tagsTotal, sumNU64, sumI32)
		}
		if math.Abs(gF64-sumF64) > 1e-6 {
			t.Errorf("sum(f64) = %v, want %v", gF64, sumF64)
		}
		// Type names reported by the server.
		var names string
		if err := conn.QueryRow(ctxT(t), `SELECT concat(toTypeName(lc), '|', toTypeName(nstr), '|', toTypeName(ts64), '|', toTypeName(attrs), '|', toTypeName(kind), '|', toTypeName(uid)) FROM typed LIMIT 1`).Scan(&names); err != nil {
			t.Fatal(err)
		}
		if names != "LowCardinality(String)|Nullable(String)|DateTime64(3)|Map(String, UInt64)|Enum8('imp' = 1, 'click' = 2, 'conv' = 3)|UUID" {
			t.Errorf("toTypeName = %s", names)
		}
		// Map / array access.
		out := tsv(t, db, "SELECT id, attrs['k1'], has(tags, 'alpha'), nums[2], length(attrs) FROM typed WHERE tenant = 'tenant-2' AND id IN (2, 9, 16) ORDER BY id")
		want := ""
		for _, id := range []int{2, 9, 16} {
			r := rows[id]
			has := 0
			for _, tg := range r.Tags {
				if tg == "alpha" {
					has = 1
				}
			}
			var n2 uint32
			if len(r.Nums) >= 2 {
				n2 = r.Nums[1]
			}
			want += fmt.Sprintf("%d\t%d\t%d\t%d\t%d\n", id, r.Attrs["k1"], has, n2, len(r.Attrs))
		}
		if out != want {
			t.Errorf("map/array access =\n%s\nwant\n%s", out, want)
		}
	})
}

func diffTyped(g, w typedRow) string {
	var diffs []string
	gv, wv := reflect.ValueOf(g), reflect.ValueOf(w)
	tp := gv.Type()
	for i := 0; i < tp.NumField(); i++ {
		a, b := gv.Field(i).Interface(), wv.Field(i).Interface()
		eq := false
		switch x := a.(type) {
		case time.Time:
			eq = x.Equal(b.(time.Time))
		case []string:
			eq = strings.Join(x, "\x00") == strings.Join(b.([]string), "\x00") && len(x) == len(b.([]string))
		case []uint32:
			eq = fmt.Sprint(x) == fmt.Sprint(b)
		case map[string]uint64:
			eq = fmt.Sprint(x) == fmt.Sprint(b)
		case float64:
			eq = x == b.(float64)
		default:
			eq = reflect.DeepEqual(a, b)
		}
		if !eq {
			diffs = append(diffs, fmt.Sprintf("%s: got %s want %s", tp.Field(i).Name, show(a), show(b)))
		}
	}
	return strings.Join(diffs, "; ")
}

func show(v any) string {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "<nil>"
		}
		return fmt.Sprintf("&%#v", rv.Elem().Interface())
	}
	if t, ok := v.(time.Time); ok {
		return t.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%#v", v)
}
