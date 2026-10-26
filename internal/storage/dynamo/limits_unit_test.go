package dynamo

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/storage"
)

// writeCounter is a ddbAPI that records writes and serves nothing else
// (calling any other method panics on the nil embedded interface).
type writeCounter struct {
	ddbAPI
	writes atomic.Int64
}

func (w *writeCounter) BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	w.writes.Add(1)
	return &dynamodb.BatchWriteItemOutput{}, nil
}

func (w *writeCounter) PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	w.writes.Add(1)
	return &dynamodb.PutItemOutput{}, nil
}

// TestItemSizeCheckedBeforeWrites: one row over DynamoDB's 400 KB item
// limit rejects the whole INSERT before any write is sent (the batches are
// written in parallel, so a late failure would leave a random subset stored).
func TestItemSizeCheckedBeforeWrites(t *testing.T) {
	big := strings.Repeat("x", 450000)
	rows := func() []storage.Row {
		out := make([]storage.Row, 100)
		for i := range out {
			v := "small"
			if i == 30 {
				v = big
			}
			out[i] = storage.Row{uint64(i), v}
		}
		return out
	}
	for _, engine := range []string{catalog.EngineMergeTree, catalog.EngineReplacingMergeTree} {
		w := &writeCounter{}
		st, err := newStoreWithAPI(w, "us-east-1", Options{Prefix: "x_"})
		if err != nil {
			t.Fatal(err)
		}
		tbl := &catalog.Table{Database: "default", Name: "big", Engine: engine, Columns: cols("id", "UInt64", "s", "String"), OrderBy: []string{"id"}}
		l := testLayout(t, tbl)
		_, err = st.Insert(ctxBG, l.t, rows())
		if !cherr.Is(err, cherr.BadArguments) || !strings.Contains(err.Error(), "row 31") || !strings.Contains(err.Error(), "400 KB") {
			t.Fatalf("%s: %v", engine, err)
		}
		if n := w.writes.Load(); n != 0 {
			t.Fatalf("%s: %d writes sent before the oversized row was rejected", engine, n)
		}
		// View output into append and Replacing targets is checked too.
		var ms []mergeSource
		for i, r := range rows() {
			ms = append(ms, mergeSource{MergeRow: storage.MergeRow{Row: r, Source: storage.SourceRef{View: "v", RowID: fmt.Sprint(i)}}})
		}
		if engine == catalog.EngineMergeTree {
			err = st.impl.mergeAppend(ctxBG, l, ms)
		} else {
			err = st.impl.mergeReplacing(ctxBG, l, ms)
		}
		if !cherr.Is(err, cherr.BadArguments) || w.writes.Load() != 0 {
			t.Fatalf("%s merge: %v (%d writes)", engine, err, w.writes.Load())
		}
		// Rows under the limit are written.
		ok := rows()
		ok[30][1] = strings.Repeat("x", 300000)
		if _, err := st.Insert(ctxBG, l.t, ok); err != nil {
			t.Fatalf("%s: %v", engine, err)
		}
	}
}

func TestItemSize(t *testing.T) {
	item := map[string]types.AttributeValue{
		"s": attrS("abc"),                                                                       // 1 + 3
		"n": attrN("12345"),                                                                     // 1 + 4 (5 digits: 3 bytes + 1)
		"z": attrN("1000"),                                                                      // 1 + 2 (trailing zeros trimmed)
		"b": attrTrue,                                                                           // 1 + 1
		"l": &types.AttributeValueMemberL{Value: []types.AttributeValue{attrS("ab"), attrNull}}, // 1 + 3 + (2+1) + (1+1)
	}
	if got, want := itemSize(item), 4+5+3+2+9; got != want {
		t.Fatalf("itemSize = %d, want %d", got, want)
	}
	if got := numberSize("0.000123456789012345678901234567890123456789"); got != 21 {
		t.Fatalf("38-digit number size %d", got)
	}
}

// wideTable is a merging table with n non-key columns of type typ.
func wideTable(engine, typ string, n int) *catalog.Table {
	spec := []string{"k", "String"}
	for i := 0; i < n; i++ {
		spec = append(spec, fmt.Sprintf("metric_%03d", i), typ)
	}
	return &catalog.Table{Database: "default", Name: "wide", Engine: engine, Columns: cols(spec...), OrderBy: []string{"k"}}
}

// TestExpressionLimits: DynamoDB caps each expression at 4 KB. Merging
// tables whose row update cannot fit are rejected at CREATE/ALTER (the
// catalog's CheckTable) instead of failing every merge; reads of wide
// tables drop the projection.
func TestExpressionLimits(t *testing.T) {
	s := &storeImpl{opts: Options{Prefix: "x_", Region: "r1"}, now: time.Now}
	cases := []struct {
		engine, typ string
		n           int
		ok          bool
	}{
		{catalog.EngineSummingMergeTree, "UInt64", 300, true},
		{catalog.EngineSummingMergeTree, "UInt64", 400, true}, // fits only with short placeholders
		{catalog.EngineSummingMergeTree, "Float64", 300, true},
		{catalog.EngineSummingMergeTree, "UInt64", 700, false},
		{catalog.EngineAggregatingMergeTree, "SimpleAggregateFunction(any, String)", 130, true},
		{catalog.EngineAggregatingMergeTree, "SimpleAggregateFunction(any, String)", 200, false},
		{catalog.EngineAggregatingMergeTree, "AggregateFunction(avg, Float64)", 300, false},
		{catalog.EngineMergeTree, "UInt64", 1000, true}, // plain puts: no expression
	}
	for _, c := range cases {
		l := testLayout(t, wideTable(c.engine, c.typ, c.n))
		err := s.checkTable(l.t)
		if (err == nil) != c.ok {
			t.Errorf("%s %d x %s: %v", c.engine, c.n, c.typ, err)
			continue
		}
		if err != nil && !cherr.Is(err, cherr.NotImplemented) {
			t.Errorf("%s %d x %s: code %v", c.engine, c.n, c.typ, err)
		}
		if !c.ok || !l.merging || strings.HasPrefix(c.typ, "AggregateFunction") {
			continue
		}
		// The bound holds for a real row with every column present.
		row := make(storage.Row, len(l.cols))
		row[0] = "key"
		for i := 1; i < len(row); i++ {
			switch {
			case strings.Contains(c.typ, "String"):
				row[i] = "v"
			case strings.Contains(c.typ, "Float"):
				row[i] = 1.5
			default:
				row[i] = uint64(i)
			}
		}
		rp, err := s.planRow(ctxBG, l, &storage.MergeRow{Row: row})
		if err != nil {
			t.Fatal(err)
		}
		e, cond, _, err := s.render(l, groupUpdate([]*rowPlan{rp}), []byte("k"), []byte{1}, true, map[string]types.AttributeValue{attrCAS: attrS("t")})
		if err != nil {
			t.Fatal(err)
		}
		worst, _ := worstCaseUpdateLen(s, l)
		if n := len(aws.ToString(e.updateExpression())); n > worst || n > maxExpressionLen || len(aws.ToString(cond)) > maxExpressionLen {
			t.Errorf("%s %d x %s: update %d bytes, bound %d", c.engine, c.n, c.typ, n, worst)
		}
	}

	// UpdateTable only enforces the check when the old definition passes.
	cat := catalog.New(nil, catalog.Options{CheckTable: s.checkTable})
	if err := cat.CreateTable(ctxBG, wideTable(catalog.EngineSummingMergeTree, "UInt64", 700), false); !cherr.Is(err, cherr.NotImplemented) {
		t.Fatalf("CREATE of a too-wide table: %v", err)
	}
	if err := cat.CreateTable(ctxBG, wideTable(catalog.EngineSummingMergeTree, "UInt64", 300), false); err != nil {
		t.Fatal(err)
	}
	stored, _ := cat.Table("default", "wide")
	wider := *stored
	wider.Columns = wideTable(catalog.EngineSummingMergeTree, "UInt64", 700).Columns
	if err := cat.UpdateTable(ctxBG, &wider); !cherr.Is(err, cherr.NotImplemented) {
		t.Fatalf("ALTER ADD COLUMN beyond the limit: %v", err)
	}

	// A projection longer than 4 KB reads whole items.
	l := testLayout(t, wideTable(catalog.EngineMergeTree, "UInt64", 1000))
	all := make([]int, len(l.cols))
	for i := range all {
		all[i] = i
	}
	if p := s.newReadPlan(l, storage.ScanRequest{Columns: all}); p.projection != nil {
		t.Fatalf("projection of %d bytes kept", len(*p.projection))
	}
	if p := s.newReadPlan(l, storage.ScanRequest{Columns: []int{0, 5}}); p.projection == nil || len(*p.projection) > maxExpressionLen {
		t.Fatal("narrow projection dropped")
	}
}

// TestStreamsOnlyWithConsumers: regional rollups get no stream.
func TestStreamsOnlyWithConsumers(t *testing.T) {
	s := &storeImpl{opts: Options{Prefix: "x_", Region: "r1"}}
	for _, c := range []struct {
		engine, repl string
		stream       bool
	}{
		{catalog.EngineMergeTree, "", true},
		{catalog.EngineMergeTree, catalog.ReplicationRegional, true},
		{catalog.EngineReplacingMergeTree, "", true},
		{catalog.EngineSummingMergeTree, "", false},
		{catalog.EngineAggregatingMergeTree, catalog.ReplicationRegional, false},
		{catalog.EngineAggregatingMergeTree, catalog.ReplicationGlobal, true},
		{catalog.EngineSummingMergeTree, catalog.ReplicationGlobal, true},
	} {
		tbl := &catalog.Table{Database: "default", Name: "t", Engine: c.engine, Replication: c.repl,
			Columns: cols("k", "String", "v", "UInt64"), OrderBy: []string{"k"}}
		l := testLayout(t, tbl)
		if got := s.rowTableSpec(l).stream; got != c.stream {
			t.Errorf("%s %q: stream %v, want %v", c.engine, c.repl, got, c.stream)
		}
	}
}

// TestGroupFold: rows of one key fold into one update; float sums add in
// IEEE order with the column's width; any keeps the first, anyLast the
// last value, groupArray concatenates in row order.
func TestGroupFold(t *testing.T) {
	s := &storeImpl{opts: Options{Prefix: "x_", Region: "r1"}, now: time.Now}
	l := testLayout(t, &catalog.Table{Database: "default", Name: "f", Engine: catalog.EngineAggregatingMergeTree,
		Columns: cols("k", "String", "s", "SimpleAggregateFunction(sum, Float64)", "s32", "SimpleAggregateFunction(sum, Float32)",
			"n", "SimpleAggregateFunction(sum, UInt64)", "a", "SimpleAggregateFunction(any, String)",
			"z", "SimpleAggregateFunction(anyLast, String)", "g", "AggregateFunction(groupArray, UInt8)"),
		OrderBy: []string{"k"}})
	noise := 0.1
	noise = noise + 0.2 - 0.3
	in := []storage.Row{
		{"x", 1500000.25, 0.1, uint64(math.MaxUint64), "first", "a", &chtype.AggPartial{Func: "groupArray", List: []any{uint64(1), uint64(2)}}},
		{"x", noise, 0.2, uint64(2), "second", "b", &chtype.AggPartial{Func: "groupArray", List: []any{uint64(3)}}},
		{"x", 1e-20, 0.3, uint64(0), "third", "c", &chtype.AggPartial{Func: "groupArray", List: []any{}}},
	}
	var rows []*rowPlan
	for i := range in {
		rp, err := s.planRow(ctxBG, l, &storage.MergeRow{Row: in[i]})
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, rp)
	}
	u := groupUpdate(rows)
	cur := map[string]types.AttributeValue{"s": attrN("3.00000000000000005551115123125783")} // a legacy exact decimal sum
	e, cond, next, err := s.render(l, u, []byte("x"), []byte{1}, true, cur)
	if err != nil {
		t.Fatal(err)
	}
	want := 3.00000000000000005551115123125783
	for _, f := range []float64{1500000.25, noise, 1e-20} {
		want += f
	}
	if got := next["s"].(*types.AttributeValueMemberN).Value; got != fmt.Sprint(want) {
		t.Errorf("float sum %s, want %v", got, want)
	}
	f32 := float32(0)
	for _, f := range []float32{0.1, 0.2, 0.3} {
		f32 += f
	}
	if got := next["s32"].(*types.AttributeValueMemberN).Value; got != fmt.Sprint(f32) {
		t.Errorf("Float32 sum %s, want %v", got, f32)
	}
	if aws.ToString(cond) != "attribute_not_exists("+e.byAttr[attrCAS]+")" {
		t.Errorf("condition %q", aws.ToString(cond))
	}
	if next[attrCAS] == nil {
		t.Error("no CAS token written")
	}
	expr := aws.ToString(e.updateExpression())
	val := func(attr string) types.AttributeValue {
		ph := e.byAttr[attr]
		for _, c := range append(append([]string{}, e.set...), e.add...) {
			if strings.HasPrefix(c, ph+"=") || strings.HasPrefix(c, ph+" ") {
				c = strings.TrimRight(c, ")")
				return e.values[c[strings.LastIndex(c, ":"):]]
			}
		}
		t.Fatalf("no clause for %s in %s", attr, expr)
		return nil
	}
	if got := val("n").(*types.AttributeValueMemberN).Value; got != "18446744073709551617" {
		t.Errorf("integer sum %s (exact, wrapped on read)", got)
	}
	if got := val("a").(*types.AttributeValueMemberS).Value; got != "first" {
		t.Errorf("any kept %q", got)
	}
	if got := val("z").(*types.AttributeValueMemberS).Value; got != "c" {
		t.Errorf("anyLast kept %q", got)
	}
	g := val("g#l").(*types.AttributeValueMemberL).Value
	if len(g) != 3 || g[0].(*types.AttributeValueMemberN).Value != "1" || g[2].(*types.AttributeValueMemberN).Value != "3" {
		t.Errorf("groupArray %v", g)
	}
	// With a stored token the condition compares it; NaN/Inf sums are S.
	cur = map[string]types.AttributeValue{attrCAS: attrS("old"), "s": attrS("inf")}
	e, cond, next, err = s.render(l, u, []byte("x"), []byte{1}, true, cur)
	if err != nil || !strings.HasPrefix(aws.ToString(cond), e.byAttr[attrCAS]+"=") ||
		e.values[lastValue(aws.ToString(cond))].(*types.AttributeValueMemberS).Value != "old" {
		t.Fatalf("condition %q %v", aws.ToString(cond), err)
	}
	if got := next["s"].(*types.AttributeValueMemberS).Value; got != "inf" {
		t.Errorf("inf sum stored as %q", got)
	}
}

func lastValue(cond string) string { return cond[strings.LastIndex(cond, ":"):] }
