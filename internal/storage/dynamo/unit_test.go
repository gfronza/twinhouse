package dynamo

import (
	"bytes"
	"context"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/storage"
	"github.com/gfronza/twinhouse/internal/storage/keyenc"
)

var ctxBG = context.Background()

func ty(s string) chtype.Type { return chtype.MustParse(s) }

func TestCodecRoundTrip(t *testing.T) {
	ts := time.Date(2024, 3, 4, 5, 6, 7, 123456789, time.UTC)
	far := time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC)
	early := time.Date(1900, 1, 1, 0, 0, 0, 1, time.UTC)
	cases := []struct {
		typ  string
		v    any
		kind string // expected attribute type
	}{
		{"UInt64", uint64(math.MaxUint64), "N"},
		{"Int64", int64(math.MinInt64), "N"},
		{"UInt8", uint64(255), "N"},
		{"Float64", 0.1, "N"},
		{"Float64", math.Inf(1), "S"},
		{"Float64", math.Inf(-1), "S"},
		{"Float64", math.NaN(), "S"},
		{"Float64", math.Copysign(0, -1), "S"},
		{"Float64", 1e300, "S"},
		{"Float64", 5e-324, "S"},
		{"Float64", -1.5e125, "N"},
		{"Float32", float64(float32(0.1)), "N"},
		{"String", "", "S"},
		{"String", "héllo", "S"},
		{"String", "a\x00\xff\xfe", "B"},
		{"FixedString(3)", "ab\x00", "S"},
		{"UUID", "123e4567-e89b-12d3-a456-426614174000", "S"},
		{"Bool", true, "BOOL"},
		{"DateTime64(9)", ts, "N"},
		{"Date32", far, "N"},
		{"DateTime64(9)", early, "N"},
		{"Date", chtype.Epoch, "N"},
		{"Nullable(String)", nil, "NULL"},
		{"Nullable(String)", "x", "S"},
		{"LowCardinality(Nullable(String))", nil, "NULL"},
		{"Array(Nullable(Int64))", []any{int64(1), nil, int64(-3)}, "L"},
		{"Array(String)", []any{}, "L"},
		{"Tuple(a String, b Array(UInt8))", []any{"x", []any{uint64(1)}}, "L"},
		{"Map(String, Float64)", []any{[]any{"k", math.Inf(1)}, []any{"j", 2.5}}, "L"},
		{"Enum8('a' = 1, 'b' = 2)", "b", "S"},
		{"SimpleAggregateFunction(sum, UInt64)", uint64(7), "N"},
	}
	for _, c := range cases {
		typ := ty(c.typ)
		av, err := encodeValue(typ, c.v)
		if err != nil {
			t.Fatalf("%s %v: encode: %v", c.typ, c.v, err)
		}
		if got := attrKind(av); got != c.kind {
			t.Errorf("%s %#v: attribute %s, want %s", c.typ, c.v, got, c.kind)
		}
		back, err := decodeValue(typ, av)
		if err != nil {
			t.Fatalf("%s: decode: %v", c.typ, err)
		}
		if !sameValue(back, c.v) {
			t.Errorf("%s: round trip %#v -> %#v", c.typ, c.v, back)
		}
	}
}

func attrKind(av types.AttributeValue) string {
	switch av.(type) {
	case *types.AttributeValueMemberN:
		return "N"
	case *types.AttributeValueMemberS:
		return "S"
	case *types.AttributeValueMemberB:
		return "B"
	case *types.AttributeValueMemberBOOL:
		return "BOOL"
	case *types.AttributeValueMemberNULL:
		return "NULL"
	case *types.AttributeValueMemberL:
		return "L"
	}
	return "?"
}

func TestCodecErrorsAndDefaults(t *testing.T) {
	if _, err := encodeValue(ty("String"), nil); !cherr.Is(err, cherr.TypeMismatch) {
		t.Errorf("NULL into String: %v", err)
	}
	if _, err := encodeValue(ty("UInt64"), int64(-1)); !cherr.Is(err, cherr.TypeMismatch) {
		t.Errorf("negative into UInt64: %v", err)
	}
	if _, err := encodeValue(ty("Tuple(String, String)"), []any{"a"}); !cherr.Is(err, cherr.TypeMismatch) {
		t.Errorf("tuple arity: %v", err)
	}
	if _, err := decodeValue(ty("UInt64"), attrS("x")); !cherr.Is(err, cherr.IncorrectData) {
		t.Errorf("S into UInt64: %v", err)
	}
	// Missing attributes (ALTER ADD COLUMN) decode as the type default.
	for _, c := range []struct {
		typ  string
		want any
	}{
		{"String", ""}, {"UInt32", uint64(0)}, {"Nullable(Int8)", nil}, {"Array(String)", []any{}},
		{"DateTime", chtype.Epoch}, {"FixedString(2)", "\x00\x00"}, {"Tuple(UInt8, String)", []any{uint64(0), ""}},
	} {
		got, err := decodeValue(ty(c.typ), nil)
		if err != nil || !sameValue(got, c.want) {
			t.Errorf("default of %s = %#v, %v", c.typ, got, err)
		}
		if z := chtype.Zero(ty(c.typ)); !sameValue(got, z) {
			t.Errorf("default of %s differs from chtype.Zero: %#v vs %#v", c.typ, got, z)
		}
	}
}

func TestDecodeIntWraps(t *testing.T) {
	cases := []struct {
		kind chtype.Kind
		in   string
		want any
	}{
		{chtype.UInt64, "36893488147419103230", uint64(math.MaxUint64 - 1)}, // 2*(2^64-1)
		{chtype.UInt8, "300", uint64(44)},
		{chtype.Int8, "200", int64(-56)},
		{chtype.Int8, "-129", int64(127)},
		{chtype.Int64, "9223372036854775808", int64(math.MinInt64)},
		{chtype.Int32, "-5", int64(-5)},
		{chtype.UInt32, "1E5", uint64(100000)},
		{chtype.UInt64, "-1", uint64(math.MaxUint64)},
	}
	for _, c := range cases {
		got, err := decodeInt(c.kind, c.in)
		if err != nil || got != c.want {
			t.Errorf("decodeInt(%v, %s) = %v, %v; want %v", c.kind, c.in, got, err, c.want)
		}
	}
	if _, err := decodeInt(chtype.UInt8, "1.5"); err == nil {
		t.Error("fractional integer accepted")
	}
}

func TestTimeNanosFullRange(t *testing.T) {
	for _, tm := range []time.Time{
		time.Date(2299, 12, 31, 23, 59, 59, 999999999, time.UTC),
		time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1677, 9, 21, 0, 12, 44, 0, time.UTC),
		chtype.Epoch,
		time.Unix(0, -1).UTC(),
	} {
		got, err := nanosToTime(timeToNanos(tm))
		if err != nil || !got.Equal(tm) {
			t.Errorf("%v -> %s -> %v (%v)", tm, timeToNanos(tm), got, err)
		}
	}
}

func testLayout(t *testing.T, tbl *catalog.Table) *layout {
	t.Helper()
	c := catalog.NewMemory()
	if err := c.CreateTable(ctxBG, tbl, false); err != nil {
		t.Fatal(err)
	}
	stored, _ := c.Table(tbl.Database, tbl.Name)
	s := &storeImpl{opts: Options{Prefix: "x_", Region: "r1"}}
	l, err := s.layoutOf(stored)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func cols(spec ...string) []catalog.Column {
	var out []catalog.Column
	for i := 0; i < len(spec); i += 2 {
		out = append(out, catalog.Column{Name: spec[i], Type: ty(spec[i+1]), TypeText: spec[i+1]})
	}
	return out
}

func TestKeyIntervals(t *testing.T) {
	l := testLayout(t, &catalog.Table{
		Database: "default", Name: "ev", Engine: catalog.EngineMergeTree,
		Columns: cols("tenant", "String", "ts", "DateTime64(9)", "kind", "Enum8('b' = 1, 'a' = 2)", "n", "UInt64"),
		OrderBy: []string{"tenant", "ts", "kind", "n"},
	})
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	enc := func(vs ...any) []byte {
		b, err := keyenc.AppendTuple(nil, l.skTypes[:len(vs)], vs)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	up := func(b []byte) []byte { return keyenc.PrefixUpper(b, maxSortKeyLen) }
	cases := []struct {
		name string
		kr   storage.KeyRange
		want interval
		ok   bool
	}{
		{"partition only", storage.KeyRange{Partition: []any{"a"}}, interval{}, true},
		{"prefix", storage.KeyRange{Partition: []any{"a"}, SortPrefix: []any{t0}}, interval{enc(t0), up(enc(t0))}, true},
		{"lower incl", storage.KeyRange{Partition: []any{"a"}, Lower: &storage.Bound{Value: t0, Inclusive: true}}, interval{enc(t0), nil}, true},
		{"lower excl stays inclusive", storage.KeyRange{Partition: []any{"a"}, Lower: &storage.Bound{Value: t0}}, interval{enc(t0), nil}, true},
		{"upper incl", storage.KeyRange{Partition: []any{"a"}, Upper: &storage.Bound{Value: t1, Inclusive: true}}, interval{nil, up(enc(t1))}, true},
		{"upper excl", storage.KeyRange{Partition: []any{"a"}, Upper: &storage.Bound{Value: t1}}, interval{nil, enc(t1)}, true},
		{"between", storage.KeyRange{Partition: []any{"a"}, Lower: &storage.Bound{Value: t0, Inclusive: true}, Upper: &storage.Bound{Value: t1, Inclusive: true}},
			interval{enc(t0), up(enc(t1))}, true},
		{"empty", storage.KeyRange{Partition: []any{"a"}, Lower: &storage.Bound{Value: t1, Inclusive: true}, Upper: &storage.Bound{Value: t0, Inclusive: true}},
			interval{}, false},
		{"enum bound dropped", storage.KeyRange{Partition: []any{"a"}, SortPrefix: []any{t0}, Lower: &storage.Bound{Value: "a", Inclusive: true}},
			interval{enc(t0), up(enc(t0))}, true},
		{"bad bound value dropped", storage.KeyRange{Partition: []any{"a"}, Lower: &storage.Bound{Value: "not a time", Inclusive: true}}, interval{}, true},
		{"bounds after full prefix ignored", storage.KeyRange{Partition: []any{"a"}, SortPrefix: []any{t0, "a", uint64(3)}, Lower: &storage.Bound{Value: uint64(9)}},
			interval{enc(t0, "a", uint64(3)), up(enc(t0, "a", uint64(3)))}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pk, iv, ok, err := l.keyInterval(c.kr)
			if err != nil {
				t.Fatal(err)
			}
			if ok != c.ok {
				t.Fatalf("ok = %v", ok)
			}
			if !ok {
				return
			}
			wantPK, _ := keyenc.AppendTuple(nil, l.pkTypes, c.kr.Partition)
			if !bytes.Equal(pk, wantPK) || !bytes.Equal(iv.lo, c.want.lo) || !bytes.Equal(iv.hi, c.want.hi) ||
				(iv.lo == nil) != (c.want.lo == nil) || (iv.hi == nil) != (c.want.hi == nil) {
				t.Fatalf("got %x [%x, %x], want [%x, %x]", pk, iv.lo, iv.hi, c.want.lo, c.want.hi)
			}
		})
	}
	if _, _, _, err := l.keyInterval(storage.KeyRange{}); !cherr.Is(err, cherr.LogicalError) {
		t.Errorf("missing partition: %v", err)
	}
	if _, _, _, err := l.keyInterval(storage.KeyRange{Partition: []any{uint64(1)}}); !cherr.Is(err, cherr.TypeMismatch) {
		t.Errorf("wrong partition type: %v", err)
	}
	// A 2000-byte prefix cannot match any key.
	if _, _, ok, _ := l.keyInterval(storage.KeyRange{Partition: []any{"a"}, SortPrefix: []any{t0, strings.Repeat("x", 2000)}}); ok {
		t.Error("oversized prefix should be empty")
	}
}

func TestMergeIntervals(t *testing.T) {
	b := func(s string) []byte { return []byte(s) }
	cases := []struct {
		in, want []interval
	}{
		{[]interval{{b("c"), b("d")}, {b("a"), b("b")}}, []interval{{b("a"), b("b")}, {b("c"), b("d")}}},
		{[]interval{{b("a"), b("c")}, {b("b"), b("d")}}, []interval{{b("a"), b("d")}}},
		{[]interval{{b("a"), b("d")}, {b("b"), b("c")}}, []interval{{b("a"), b("d")}}},
		{[]interval{{b("a"), b("b")}, {b("b"), b("c")}}, []interval{{b("a"), b("c")}}},
		{[]interval{{b("b"), nil}, {b("a"), b("c")}}, []interval{{b("a"), nil}}},
		{[]interval{{b("x"), b("y")}, {nil, b("a")}}, []interval{{nil, b("a")}, {b("x"), b("y")}}},
		{[]interval{{nil, nil}, {b("x"), b("y")}}, []interval{{nil, nil}}},
	}
	for i, c := range cases {
		if got := mergeIntervals(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("case %d: got %q want %q", i, got, c.want)
		}
	}
}

func TestPlanRangesGroupsAndReverses(t *testing.T) {
	l := testLayout(t, &catalog.Table{
		Database: "default", Name: "ev", Engine: catalog.EngineReplacingMergeTree,
		Columns: cols("tenant", "String", "id", "UInt64"), OrderBy: []string{"tenant", "id"},
	})
	rng := func(p string, id uint64) storage.KeyRange {
		return storage.KeyRange{Partition: []any{p}, SortPrefix: []any{id}}
	}
	parts, err := l.planRanges([]storage.KeyRange{rng("b", 2), rng("a", 1), rng("b", 1), rng("b", 2)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || len(parts[0].ivs) != 2 || len(parts[1].ivs) != 1 {
		t.Fatalf("parts: %+v", parts)
	}
	// Reverse: partition "b" intervals in descending order.
	if bytes.Compare(parts[0].ivs[0].lo, parts[0].ivs[1].lo) <= 0 {
		t.Fatal("intervals not reversed")
	}
}

func TestQueryInput(t *testing.T) {
	cases := []struct {
		iv   interval
		cond string
	}{
		{interval{}, "#kp = :kp"},
		{interval{lo: []byte{1}}, "#kp = :kp AND #ks >= :klo"},
		{interval{hi: []byte{1}}, "#kp = :kp AND #ks <= :khi"},
		{interval{[]byte{1}, []byte{2}}, "#kp = :kp AND #ks BETWEEN :klo AND :khi"},
		{interval{[]byte{1}, []byte{1}}, "#kp = :kp AND #ks = :klo"},
	}
	for _, c := range cases {
		q := queryInput("tbl", []byte{9}, c.iv)
		if got := aws.ToString(q.KeyConditionExpression); got != c.cond {
			t.Errorf("got %q want %q", got, c.cond)
		}
		for ph := range q.ExpressionAttributeValues {
			if !strings.Contains(aws.ToString(q.KeyConditionExpression), ph) {
				t.Errorf("unused value %s", ph)
			}
		}
	}
}

func TestReplicaRequests(t *testing.T) {
	desc := &types.TableDescription{Replicas: []types.ReplicaDescription{{RegionName: aws.String("eu-west-1")}}}
	got := replicasToAdd(desc, "us-east-1", []string{"eu-west-1", "ap-south-1", "us-east-1", "ap-south-1", "", "sa-east-1"})
	if !reflect.DeepEqual(got, []string{"ap-south-1", "sa-east-1"}) {
		t.Fatalf("replicasToAdd = %v", got)
	}
	if got := replicasToAdd(nil, "us-east-1", nil); len(got) != 0 {
		t.Fatalf("no replicas wanted: %v", got)
	}
	in := replicaCreateInput("p_db.t", "ap-south-1")
	if aws.ToString(in.TableName) != "p_db.t" || len(in.ReplicaUpdates) != 1 ||
		aws.ToString(in.ReplicaUpdates[0].Create.RegionName) != "ap-south-1" || in.ReplicaUpdates[0].Delete != nil {
		t.Fatalf("create input: %+v", in)
	}
	del := replicaDeleteInput("p_db.t", "eu-west-1")
	if aws.ToString(del.ReplicaUpdates[0].Delete.RegionName) != "eu-west-1" || del.ReplicaUpdates[0].Create != nil {
		t.Fatalf("delete input: %+v", del)
	}
}

func TestCreateTableInput(t *testing.T) {
	in := createTableInput(tableSpec{name: "x", keyType: types.ScalarAttributeTypeB, rangeKey: true, stream: true})
	if in.BillingMode != types.BillingModePayPerRequest || len(in.KeySchema) != 2 ||
		in.StreamSpecification == nil || in.StreamSpecification.StreamViewType != types.StreamViewTypeNewAndOldImages {
		t.Fatalf("row table input: %+v", in)
	}
	in = createTableInput(tableSpec{name: "c", keyType: types.ScalarAttributeTypeS})
	if len(in.KeySchema) != 1 || in.StreamSpecification != nil || in.AttributeDefinitions[0].AttributeType != types.ScalarAttributeTypeS {
		t.Fatalf("system table input: %+v", in)
	}
}

func TestVersionCondition(t *testing.T) {
	plain := &colSpec{name: "ver", typ: ty("UInt64")}
	e := newExpr()
	got := versionCondition(e, plain, attrN("5"))
	want := "attribute_not_exists(#0) OR attribute_not_exists(#1) OR #1 <= :0"
	if got != want || e.names["#1"] != "ver" {
		t.Fatalf("got %q", got)
	}
	nullable := &colSpec{name: "ver", typ: ty("Nullable(DateTime64(9))")}
	e = newExpr()
	if got := versionCondition(e, nullable, attrNull); got != "attribute_not_exists(#0) OR attribute_not_exists(#1) OR attribute_type(#1, :0)" {
		t.Fatalf("null version: %q", got)
	}
	e = newExpr()
	if got := versionCondition(e, nullable, attrN("1")); !strings.HasSuffix(got, "OR attribute_type(#1, :0) OR #1 <= :1") {
		t.Fatalf("nullable version: %q", got)
	}
}

func TestCompareValues(t *testing.T) {
	cases := []struct {
		a, b any
		want int
	}{
		{nil, uint64(0), -1}, {uint64(2), uint64(10), -1}, {int64(-1), uint64(0), -1}, {uint64(1<<63 + 1), int64(5), 1},
		{1.5, 1.5, 0}, {math.NaN(), 1.0, 0}, {"b", "ab", 1}, {true, false, 1},
		{[]any{"a", uint64(1)}, []any{"a", uint64(2)}, -1}, {[]any{"a"}, []any{"a", nil}, -1},
		{time.Unix(1, 0), time.Unix(0, 5), 1},
	}
	for i, c := range cases {
		if got := compareValues(c.a, c.b); got != c.want {
			t.Errorf("case %d: compare(%v, %v) = %d", i, c.a, c.b, got)
		}
		if chtypeCmp := chtype.Compare(c.a, c.b); !hasNaNValue(c.a) && sign(chtypeCmp) != c.want {
			t.Errorf("case %d: chtype.Compare disagrees: %d", i, chtypeCmp)
		}
	}
}

func hasNaNValue(v any) bool { f, ok := v.(float64); return ok && math.IsNaN(f) }

func sign(c int) int {
	switch {
	case c < 0:
		return -1
	case c > 0:
		return 1
	}
	return 0
}

func TestUniquifiersAndAttrs(t *testing.T) {
	src := storage.SourceRef{View: "db.v", RowID: "r1"}
	a, b := mvUniquifier(src, 0), mvUniquifier(src, 1)
	if len(a) != 16 || bytes.Equal(a, b) || !bytes.Equal(a, mvUniquifier(src, 0)) || a[0] >= 0x80 {
		t.Fatalf("mv uniquifier: %x %x", a, b)
	}
	for i := 0; i < 100; i++ {
		u := newUniquifier(time.Now())
		if len(u) != 16 || u[0] == 0xFF {
			t.Fatalf("uniquifier %x", u)
		}
	}
	// Length-prefixing makes field boundaries unambiguous.
	if bytes.Equal(memberPK([]byte("ab"), []byte("c"), "x"), memberPK([]byte("a"), []byte("bc"), "x")) {
		t.Fatal("memberPK ambiguous")
	}
	long := strings.Repeat("x", 2000)
	sk, err := memberSK(ty("String"), long)
	if err != nil || len(sk) != 33 || sk[0] != 0xFF {
		t.Fatalf("long member sk: %x %v", sk, err)
	}
	l := testLayout(t, &catalog.Table{
		Database: "default", Name: "agg", Engine: catalog.EngineAggregatingMergeTree,
		Columns: cols("k", "String", "c", "AggregateFunction(count)", "u", "AggregateFunction(uniqExact, String)",
			"m", "AggregateFunction(argMax, String, UInt64)", "a", "AggregateFunction(avg, Float64)",
			"g", "SimpleAggregateFunction(groupUniqArrayArray, Array(String))", "s", "SimpleAggregateFunction(sum, UInt64)"),
		OrderBy: []string{"k"},
	})
	want := map[string][]string{
		"c": {"c#n"}, "u": {"u#z"}, "m": {"m#v", "m#k", "m#h"}, "a": {"a#s", "a#n"}, "g": {"g#z"}, "s": {"s"}, "k": {"k"},
	}
	for _, c := range l.cols {
		if got := c.attrs(); !reflect.DeepEqual(got, want[c.name]) {
			t.Errorf("attrs(%s) = %v", c.name, got)
		}
	}
	if !l.hasMembers || !l.merging {
		t.Fatal("layout flags")
	}
}

// TestCodecNegativeNaN: the sign bit of a NaN survives a round trip, so
// JSON with output_format_json_quote_denormals=1 can print "-nan" like
// ClickHouse does for a stored Float32 -nan.
func TestCodecNegativeNaN(t *testing.T) {
	for _, typ := range []string{"Float32", "Float64"} {
		for _, neg := range []bool{false, true} {
			f := math.NaN()
			if neg {
				f = math.Copysign(f, -1)
			}
			av, err := encodeValue(ty(typ), f)
			if err != nil {
				t.Fatal(err)
			}
			back, err := decodeValue(ty(typ), av)
			if err != nil {
				t.Fatal(err)
			}
			g, ok := back.(float64)
			if !ok || !math.IsNaN(g) || math.Signbit(g) != neg {
				t.Errorf("%s neg=%v: got %#v (%#v)", typ, neg, back, av)
			}
		}
	}
}

// TestAWSReplicasSkipVersionedReplacing: in GLOBAL_MODE=aws only tables whose
// conflicts item-level LWW resolves correctly become AWS global tables;
// ReplacingMergeTree, with or without a version, stays per region
// (replicated by cmd/replicator, whose conflict hook applies the views).
func TestAWSReplicasSkipVersionedReplacing(t *testing.T) {
	s := &storeImpl{opts: Options{Prefix: "x_", Region: "r1", GlobalMode: "aws"}}
	mk := func(engine, ver, repl string) *catalog.Table {
		return &catalog.Table{Database: "db", Name: "t", Engine: engine, VersionColumn: ver, Replication: repl}
	}
	cases := []struct {
		t    *catalog.Table
		want bool
	}{
		{mk(catalog.EngineMergeTree, "", catalog.ReplicationGlobal), true},
		{mk(catalog.EngineReplacingMergeTree, "", catalog.ReplicationGlobal), false},
		{mk(catalog.EngineReplacingMergeTree, "ver", catalog.ReplicationGlobal), false},
		{mk(catalog.EngineAggregatingMergeTree, "", catalog.ReplicationRegional), false},
	}
	for _, c := range cases {
		if got := s.awsReplicas(c.t); got != c.want {
			t.Errorf("%s ver=%q %s: awsReplicas=%v want %v", c.t.Engine, c.t.VersionColumn, c.t.Replication, got, c.want)
		}
		if got := s.rowTableSpec(&layout{t: c.t, physical: "x_db.t"}).global; got != c.want {
			t.Errorf("%s ver=%q: rowTableSpec.global=%v", c.t.Engine, c.t.VersionColumn, got)
		}
	}
}
