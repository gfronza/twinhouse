package dynamo

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	stypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/storage"
)

func (e *env) describe(name string) *ddbtypes.TableDescription {
	e.t.Helper()
	out, err := e.clients.DynamoDB.DescribeTable(e.ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	if err != nil {
		return nil
	}
	return out.Table
}

func TestIntegrationTableLifecycle(t *testing.T) {
	e := newEnv(t)
	for _, name := range []string{"__catalog", "__mv_markers", "__checkpoints"} {
		d := e.describe(e.prefix + name)
		if d == nil || d.KeySchema[0].KeyType != ddbtypes.KeyTypeHash || len(d.KeySchema) != 1 {
			t.Fatalf("system table %s: %+v", name, d)
		}
		if name == "__catalog" && (d.StreamSpecification == nil || !*d.StreamSpecification.StreamEnabled) {
			t.Fatal("catalog stream not enabled")
		}
	}
	ttl, err := e.clients.DynamoDB.DescribeTimeToLive(e.ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(e.prefix + "__mv_markers")})
	if err != nil || ttl.TimeToLiveDescription.TimeToLiveStatus != ddbtypes.TimeToLiveStatusEnabled || *ttl.TimeToLiveDescription.AttributeName != "_exp" {
		t.Fatalf("markers TTL: %+v %v", ttl, err)
	}
	if err := e.store.EnsureSystemTables(e.ctx); err != nil {
		t.Fatalf("EnsureSystemTables not idempotent: %v", err)
	}

	mt := e.table(&catalog.Table{Database: "default", Name: "plain", Engine: catalog.EngineMergeTree, Columns: cols("k", "String"), OrderBy: []string{"k"}})
	if err := e.store.EnsureTable(e.ctx, mt); err != nil {
		t.Fatal(err)
	}
	d := e.describe(mt.PhysicalName)
	if d.BillingModeSummary == nil || d.BillingModeSummary.BillingMode != ddbtypes.BillingModePayPerRequest ||
		d.StreamSpecification.StreamViewType != ddbtypes.StreamViewTypeNewAndOldImages {
		t.Fatalf("row table: %+v", d)
	}
	if e.describe(membersTable(mt.PhysicalName)) != nil {
		t.Fatal("members table created for a MergeTree table")
	}
	agg := uniqTable(e, "agg")
	if e.describe(membersTable(agg.PhysicalName)) == nil {
		t.Fatal("members table missing for uniqExact")
	}
	sum := e.table(&catalog.Table{Database: "default", Name: "s", Engine: catalog.EngineAggregatingMergeTree,
		Columns: cols("k", "String", "c", "AggregateFunction(count)"), OrderBy: []string{"k"}})
	if e.describe(membersTable(sum.PhysicalName)) != nil {
		t.Fatal("members table created without set columns")
	}
	// Regional rollups have no stream consumer: no stream. A global one
	// (replicated) keeps it.
	if d := e.describe(sum.PhysicalName); d.StreamSpecification != nil && aws.ToBool(d.StreamSpecification.StreamEnabled) {
		t.Fatal("stream enabled on a regional AggregatingMergeTree table")
	}
	if arn, err := e.store.StreamARN(e.ctx, sum.PhysicalName); err != nil || arn != "" {
		t.Fatalf("regional rollup stream ARN: %q %v", arn, err)
	}
	gsum := e.table(&catalog.Table{Database: "default", Name: "gs", Engine: catalog.EngineSummingMergeTree, Replication: catalog.ReplicationGlobal,
		Columns: cols("k", "String", "c", "UInt64"), OrderBy: []string{"k"}})
	if d := e.describe(gsum.PhysicalName); d.StreamSpecification == nil || !aws.ToBool(d.StreamSpecification.StreamEnabled) {
		t.Fatal("stream missing on a global SummingMergeTree table")
	}
	arn, err := e.store.StreamARN(e.ctx, mt.PhysicalName)
	if err != nil || arn == "" {
		t.Fatalf("stream ARN: %q %v", arn, err)
	}
	if _, err := e.store.StreamARN(e.ctx, "nope-"+e.prefix); !cherr.Is(err, cherr.UnknownTable) {
		t.Fatalf("missing table ARN: %v", err)
	}

	// A table created elsewhere without streams gets them enabled.
	bare := e.prefix + "default.bare"
	in := createTableInput(tableSpec{name: bare, keyType: ddbtypes.ScalarAttributeTypeB, rangeKey: true})
	if _, err := e.clients.DynamoDB.CreateTable(e.ctx, in); err != nil {
		t.Fatal(err)
	}
	bt := e.table(&catalog.Table{Database: "default", Name: "bare", Engine: catalog.EngineMergeTree, Columns: cols("k", "String"), OrderBy: []string{"k"}})
	if d := e.describe(bt.PhysicalName); d.StreamSpecification == nil || !*d.StreamSpecification.StreamEnabled {
		t.Fatal("streams not enabled on existing table")
	}
	// Incompatible existing schema is reported.
	wrong := e.prefix + "default.wrong"
	if _, err := e.clients.DynamoDB.CreateTable(e.ctx, createTableInput(tableSpec{name: wrong, keyType: ddbtypes.ScalarAttributeTypeS})); err != nil {
		t.Fatal(err)
	}
	wt := &catalog.Table{Database: "default", Name: "wrong", Engine: catalog.EngineMergeTree, Columns: cols("k", "String"), OrderBy: []string{"k"}}
	if err := e.cat.CreateTable(e.ctx, wt, false); err != nil {
		t.Fatal(err)
	}
	wt, _ = e.cat.Table("default", "wrong")
	if err := e.store.EnsureTable(e.ctx, wt); !cherr.Is(err, cherr.BadArguments) {
		t.Fatalf("incompatible schema: %v", err)
	}

	// Drop is idempotent, removes side tables, and the name can be reused.
	e.insert(agg, uniqRow("a", 1, []any{"x"}, 1))
	for i := 0; i < 2; i++ {
		if err := e.store.DropTable(e.ctx, agg); err != nil {
			t.Fatal(err)
		}
	}
	if e.describe(agg.PhysicalName) != nil || e.describe(membersTable(agg.PhysicalName)) != nil {
		t.Fatal("tables survive DropTable")
	}
	if err := e.store.EnsureTable(e.ctx, agg); err != nil {
		t.Fatal(err)
	}
	if n := len(e.all(agg)); n != 0 {
		t.Fatalf("recreated table has %d rows", n)
	}
}

func TestIntegrationKeyLimits(t *testing.T) {
	e := newEnv(t)
	tbl := e.table(&catalog.Table{Database: "default", Name: "keys", Engine: catalog.EngineReplacingMergeTree,
		Columns: cols("p", "String", "s", "String"), OrderBy: []string{"p", "s"}})
	if _, err := e.store.Insert(e.ctx, tbl, []storage.Row{{"a", strings.Repeat("x", 1100)}}); !cherr.Is(err, cherr.BadArguments) {
		t.Fatalf("oversized sort key: %v", err)
	}
	if _, err := e.store.Insert(e.ctx, tbl, []storage.Row{{strings.Repeat("x", 2100), "a"}}); !cherr.Is(err, cherr.BadArguments) {
		t.Fatalf("oversized partition key: %v", err)
	}
	// Keys near the limit work and range queries with long bounds stay valid.
	s := strings.Repeat("y", 1000)
	e.insert(tbl, storage.Row{"a", s}, storage.Row{"a", ""}, storage.Row{"a", "\x00"}, storage.Row{"a", "\x00\x00"})
	got := e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"a"},
		Lower: &storage.Bound{Value: s, Inclusive: true}, Upper: &storage.Bound{Value: s + strings.Repeat("z", 100), Inclusive: true}}}})
	if len(got) != 1 {
		t.Fatalf("long bound range: %d", len(got))
	}
	// Prefix equality on "" must not return "\x00..." strings.
	got = e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"a"}, SortPrefix: []any{""}}}})
	if len(got) != 1 || got[0][1] != "" {
		t.Fatalf("prefix \"\": %q", got)
	}
	got = e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"a"}, SortPrefix: []any{"\x00"}}}})
	if len(got) != 1 || got[0][1] != "\x00" {
		t.Fatalf("prefix \\x00: %q", got)
	}
	got = e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"a"}, Upper: &storage.Bound{Value: "\x00", Inclusive: false}}}})
	// Exclusive bounds may return the bound itself (the engine re-filters).
	if len(got) == 0 || len(got) > 2 || got[0][1] != "" {
		t.Fatalf("< \\x00: %q", got)
	}
}

// fromStream converts a DynamoDB Streams attribute value to the DynamoDB
// type (what the stream worker does before DecodeRecord).
func fromStream(av stypes.AttributeValue) ddbtypes.AttributeValue {
	switch x := av.(type) {
	case *stypes.AttributeValueMemberS:
		return &ddbtypes.AttributeValueMemberS{Value: x.Value}
	case *stypes.AttributeValueMemberN:
		return &ddbtypes.AttributeValueMemberN{Value: x.Value}
	case *stypes.AttributeValueMemberB:
		return &ddbtypes.AttributeValueMemberB{Value: x.Value}
	case *stypes.AttributeValueMemberBOOL:
		return &ddbtypes.AttributeValueMemberBOOL{Value: x.Value}
	case *stypes.AttributeValueMemberNULL:
		return &ddbtypes.AttributeValueMemberNULL{Value: x.Value}
	case *stypes.AttributeValueMemberL:
		out := make([]ddbtypes.AttributeValue, len(x.Value))
		for i, e := range x.Value {
			out[i] = fromStream(e)
		}
		return &ddbtypes.AttributeValueMemberL{Value: out}
	case *stypes.AttributeValueMemberM:
		out := map[string]ddbtypes.AttributeValue{}
		for k, e := range x.Value {
			out[k] = fromStream(e)
		}
		return &ddbtypes.AttributeValueMemberM{Value: out}
	}
	return nil
}

func TestIntegrationStreamRecords(t *testing.T) {
	e := newEnv(t)
	tbl := e.table(&catalog.Table{Database: "default", Name: "src", Engine: catalog.EngineMergeTree,
		Columns: cols("k", "String", "n", "Nullable(UInt64)", "f", "Float64", "arr", "Array(Tuple(UInt32, Array(String)))"), OrderBy: []string{"k"}})
	rows := []storage.Row{{"a", uint64(1), 1.5, []any{[]any{uint64(12), []any{"1", "3"}}}}, {"b\xff", nil, 0.0, []any{}}}
	res := e.insert(tbl, rows...)
	arn, err := e.store.StreamARN(e.ctx, tbl.PhysicalName)
	if err != nil {
		t.Fatal(err)
	}
	st := e.clients.Streams
	ds, err := st.DescribeStream(e.ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(arn)})
	if err != nil {
		t.Fatal(err)
	}
	var recs []*storage.StreamRecord
	deadline := time.Now().Add(10 * time.Second)
	for _, sh := range ds.StreamDescription.Shards {
		itOut, err := st.GetShardIterator(e.ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn: aws.String(arn), ShardId: sh.ShardId, ShardIteratorType: stypes.ShardIteratorTypeTrimHorizon})
		if err != nil {
			t.Fatal(err)
		}
		iter := itOut.ShardIterator
		for iter != nil && len(recs) < len(rows) && time.Now().Before(deadline) {
			out, err := st.GetRecords(e.ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: iter})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range out.Records {
				img := map[string]ddbtypes.AttributeValue{}
				for k, v := range r.Dynamodb.NewImage {
					img[k] = fromStream(v)
				}
				rec, err := e.store.DecodeRecord(tbl, string(r.EventName), img)
				if err != nil {
					t.Fatal(err)
				}
				recs = append(recs, rec)
			}
			iter = out.NextShardIterator
			if len(out.Records) == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	if len(recs) != 2 {
		t.Fatalf("got %d stream records", len(recs))
	}
	var gotRows []storage.Row
	ids := map[string]bool{res.RowIDs[0]: true, res.RowIDs[1]: true}
	for _, r := range recs {
		if r.EventName != "INSERT" || r.IsMVOutput || r.Origin != "us-east-1" || !ids[r.RowID] || r.Table != tbl {
			t.Fatalf("record: %+v", r)
		}
		gotRows = append(gotRows, r.Row)
	}
	eqRows(t, gotRows, rows)
	// Non-row images are skipped.
	if rec, err := e.store.DecodeRecord(tbl, "INSERT", map[string]ddbtypes.AttributeValue{"_pk": &ddbtypes.AttributeValueMemberS{Value: "x"}}); rec != nil || err != nil {
		t.Fatalf("non-row image: %v %v", rec, err)
	}
}

func TestIntegrationCatalog(t *testing.T) {
	e := newEnv(t)
	a, b := e.store.NewCatalog(), e.store.NewCatalog()
	ctx := e.ctx
	if err := a.CreateDatabase(ctx, &catalog.Database{Name: "app"}, false); err != nil {
		t.Fatal(err)
	}
	src := &catalog.Table{Database: "app", Name: "events", Engine: catalog.EngineMergeTree,
		Columns: cols("tenant", "LowCardinality(String)", "ts", "DateTime64(9, 'UTC')", "u", "Nullable(UUID)", "m", "Map(String, Array(UInt8))"),
		OrderBy: []string{"tenant", "ts"}, Settings: map[string]string{"dynamodb_partition_key": "tenant"}, DDL: "CREATE TABLE app.events ..."}
	if err := a.CreateTable(ctx, src, false); err != nil {
		t.Fatal(err)
	}
	roll := &catalog.Table{Database: "app", Name: "rollup", Engine: catalog.EngineAggregatingMergeTree,
		Columns: cols("tenant", "String", "users", "AggregateFunction(uniqExact, String)", "s", "SimpleAggregateFunction(sum, UInt64)"),
		OrderBy: []string{"tenant"}}
	if err := a.CreateTable(ctx, roll, false); err != nil {
		t.Fatal(err)
	}
	mv := &catalog.View{Database: "app", Name: "mv", Materialized: true, ToDatabase: "app", ToTable: "rollup",
		SourceDatabase: "app", SourceTable: "events", SelectSQL: "SELECT tenant, uniqExactState(toString(u)) AS users FROM app.events GROUP BY tenant"}
	if err := a.CreateView(ctx, mv, false); err != nil {
		t.Fatal(err)
	}
	if err := b.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"events", "rollup"} {
		want, _ := a.Table("app", name)
		got, err := b.Table("app", name)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
			t.Fatalf("catalog round trip:\n got %+v\nwant %+v", got, want)
		}
		if !got.Columns[1].Type.Equal(want.Columns[1].Type) {
			t.Fatal("type not re-parsed")
		}
	}
	got, _ := b.Table("app", "events")
	if !strings.HasPrefix(got.PhysicalName, e.prefix+"app.events.") || catalog.StripIncarnation(strings.TrimPrefix(got.PhysicalName, e.prefix+"app.")) != "events" {
		t.Fatalf("physical name %s: want %sapp.events.<incarnation>", got.PhysicalName, e.prefix)
	}
	roll, _ = b.Table("app", "rollup")
	if pt, ok := b.TableByPhysicalName(roll.PhysicalName); !ok || pt.Name != "rollup" {
		t.Fatal("TableByPhysicalName")
	}
	if vs := b.ViewsFrom("app", "events"); len(vs) != 1 || vs[0].SelectSQL != mv.SelectSQL {
		t.Fatalf("ViewsFrom: %v", vs)
	}

	// Raw items carry kind/def/_ts/_origin for the replicator.
	out, err := e.clients.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(e.prefix + "__catalog"),
		Key: map[string]ddbtypes.AttributeValue{"_pk": &ddbtypes.AttributeValueMemberS{Value: "table#app.events"}}, ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	for _, attr := range []string{"kind", "def", "_ts", "_origin", "ver"} {
		if out.Item[attr] == nil {
			t.Fatalf("catalog item lacks %s: %v", attr, out.Item)
		}
	}
	for _, k := range []string{"db#app", "view#app.mv"} {
		o, _ := e.clients.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(e.prefix + "__catalog"),
			Key: map[string]ddbtypes.AttributeValue{"_pk": &ddbtypes.AttributeValueMemberS{Value: k}}})
		if o.Item == nil {
			t.Fatalf("missing catalog item %s", k)
		}
	}

	// Conflicts detected through DynamoDB conditions despite stale snapshots.
	dflt := &catalog.Table{Database: "default", Name: "t", Engine: catalog.EngineMergeTree, Columns: cols("k", "String"), OrderBy: []string{"k"}}
	if err := a.CreateTable(ctx, dflt, false); err != nil {
		t.Fatal(err)
	}
	c := e.store.NewCatalog() // never refreshed
	if err := c.CreateTable(ctx, dflt, false); !cherr.Is(err, cherr.TableAlreadyExists) {
		t.Fatalf("stale create: %v", err)
	}
	if err := c.CreateTable(ctx, dflt, true); err != nil {
		t.Fatalf("stale create IF NOT EXISTS: %v", err)
	}
	if err := c.CreateView(ctx, &catalog.View{Database: "default", Name: "t", SelectSQL: "SELECT 1"}, false); !cherr.Is(err, cherr.TableAlreadyExists) {
		t.Fatalf("stale view over a table: %v", err)
	}
	if err := c.CreateDatabase(ctx, &catalog.Database{Name: "app"}, false); !cherr.Is(err, cherr.DatabaseAlreadyExists) {
		t.Fatalf("stale create database: %v", err)
	}
	if err := a.DropTable(ctx, "default", "t", false); err != nil {
		t.Fatal(err)
	}
	if err := c.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	// Optimistic ALTER.
	t1, _ := a.Table("app", "events")
	alt := *t1
	alt.Columns = append(append([]catalog.Column{}, t1.Columns...), catalog.Column{Name: "extra", Type: chtype.TUInt8, TypeText: "UInt8"})
	if err := a.UpdateTable(ctx, &alt); err != nil {
		t.Fatal(err)
	}
	stale, _ := c.Table("app", "events")
	if err := c.UpdateTable(ctx, stale); !cherr.Is(err, cherr.LogicalError) {
		t.Fatalf("stale ALTER: %v", err)
	}
	if err := c.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if t2, _ := c.Table("app", "events"); t2.Version != 2 || len(t2.Columns) != 5 {
		t.Fatalf("after ALTER: v%d %d columns", t2.Version, len(t2.Columns))
	}
	// DROP DATABASE removes everything in it.
	if err := b.DropDatabase(ctx, "app", false); err != nil {
		t.Fatal(err)
	}
	if err := a.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if len(a.Tables("app")) != 0 || len(a.Views("app")) != 0 {
		t.Fatal("objects survive DropDatabase")
	}
	if _, err := a.Database("app"); !cherr.Is(err, cherr.UnknownDatabase) {
		t.Fatalf("database survives: %v", err)
	}
	if n := e.itemCount(e.prefix + "__catalog"); n != 0 {
		t.Fatalf("%d catalog items left", n)
	}
}
