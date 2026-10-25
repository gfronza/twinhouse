package catalog

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
)

// memBackend is a Backend with DynamoDB-like conditional semantics.
type memBackend struct {
	mu      sync.Mutex
	entries map[string]Entry
	fail    error
}

func newMemBackend() *memBackend { return &memBackend{entries: map[string]Entry{}} }

func (b *memBackend) LoadAll(context.Context) ([]Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail != nil {
		return nil, b.fail
	}
	out := make([]Entry, 0, len(b.entries))
	for _, e := range b.entries {
		out = append(out, e)
	}
	return out, nil
}

func (b *memBackend) Put(_ context.Context, e Entry, conds []Cond) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, c := range conds {
		cur, ok := b.entries[c.Key]
		if ok != c.Exists || (c.Version > 0 && cur.Version != c.Version) {
			return &CondFailedError{Index: i}
		}
	}
	b.entries[e.Key] = e
	return nil
}

func (b *memBackend) Delete(_ context.Context, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range keys {
		delete(b.entries, k)
	}
	return nil
}

func tptr(t chtype.Type) *chtype.Type { return &t }

// testTypes maps type text to types so tests do not depend on chtype.Parse.
var testTypes = map[string]chtype.Type{
	"UInt8":                                chtype.TUInt8,
	"UInt64":                               chtype.TUInt64,
	"Int64":                                chtype.TInt64,
	"Float64":                              chtype.TFloat64,
	"String":                               chtype.TString,
	"DateTime64(9)":                        chtype.NewDateTime64(9, ""),
	"Nullable(String)":                     chtype.NewNullable(chtype.TString),
	"Array(String)":                        chtype.NewArray(chtype.TString),
	"AggregateFunction(uniqExact, String)": {Kind: chtype.AggregateFunction, AggFunc: "uniqExact", AggArgs: []chtype.Type{chtype.TString}},
	"AggregateFunction(sum, UInt64)":       {Kind: chtype.AggregateFunction, AggFunc: "sum", AggArgs: []chtype.Type{chtype.TUInt64}},
	"AggregateFunction(quantile, UInt64)":  {Kind: chtype.AggregateFunction, AggFunc: "quantile", AggArgs: []chtype.Type{chtype.TUInt64}},
	"SimpleAggregateFunction(sum, UInt64)": {Kind: chtype.SimpleAggregateFunction, AggFunc: "sum", Elem: tptr(chtype.TUInt64)},
	"SimpleAggregateFunction(groupUniqArrayArray, Array(String))": {Kind: chtype.SimpleAggregateFunction, AggFunc: "groupUniqArrayArray", Elem: tptr(chtype.NewArray(chtype.TString))},
	"SimpleAggregateFunction(median, UInt64)":                     {Kind: chtype.SimpleAggregateFunction, AggFunc: "median", Elem: tptr(chtype.TUInt64)},
}

func testParse(s string) (chtype.Type, error) {
	if t, ok := testTypes[s]; ok {
		return t, nil
	}
	return chtype.Parse(s)
}

func col(name, typ string) Column {
	t, err := testParse(typ)
	if err != nil {
		panic(fmt.Sprintf("test type %s: %v", typ, err))
	}
	return Column{Name: name, Type: t, TypeText: typ}
}

func newTestCatalog(b Backend) Catalog {
	return New(b, Options{ParseType: testParse, PhysicalName: func(db, t string) string { return "p_" + db + "." + t }})
}

func eventsTable(db, name string) *Table {
	return &Table{
		Database: db, Name: name, Engine: EngineMergeTree,
		Columns: []Column{col("tenant", "String"), col("ts", "DateTime64(9)"), col("id", "UInt64"), col("body", "Nullable(String)")},
		OrderBy: []string{"tenant", "ts", "id"},
	}
}

var ctx = context.Background()

func wantCode(t *testing.T, err error, code int) {
	t.Helper()
	if !cherr.Is(err, code) {
		t.Fatalf("got error %v, want code %d", err, code)
	}
}

func TestDatabases(t *testing.T) {
	c := newTestCatalog(newMemBackend())
	if _, err := c.Database("default"); err != nil {
		t.Fatalf("default database must exist: %v", err)
	}
	wantCode(t, c.CreateDatabase(ctx, &Database{Name: "default"}, false), cherr.DatabaseAlreadyExists)
	wantCode(t, c.CreateDatabase(ctx, &Database{Name: "system"}, false), cherr.DatabaseAlreadyExists)
	if err := c.CreateDatabase(ctx, &Database{Name: "default"}, true); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateDatabase(ctx, &Database{Name: "analytics"}, false); err != nil {
		t.Fatal(err)
	}
	wantCode(t, c.CreateDatabase(ctx, &Database{Name: "analytics"}, false), cherr.DatabaseAlreadyExists)
	wantCode(t, c.CreateDatabase(ctx, &Database{Name: "bad.name"}, false), cherr.BadArguments)
	var names []string
	for _, d := range c.Databases() {
		names = append(names, d.Name)
	}
	if !reflect.DeepEqual(names, []string{"analytics", "default"}) {
		t.Fatalf("Databases() = %v", names)
	}
	wantCode(t, mustErr(c.Database("nope")), cherr.UnknownDatabase)
	wantCode(t, c.DropDatabase(ctx, "nope", false), cherr.UnknownDatabase)
	if err := c.DropDatabase(ctx, "nope", true); err != nil {
		t.Fatal(err)
	}
}

func mustErr[T any](_ T, err error) error { return err }

func TestCreateTableDerivesFields(t *testing.T) {
	c := newTestCatalog(newMemBackend())
	if err := c.CreateTable(ctx, eventsTable("default", "events"), false); err != nil {
		t.Fatal(err)
	}
	got, err := c.Table("default", "events")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.PartitionKey, []string{"tenant"}) || !reflect.DeepEqual(got.SortKey, []string{"ts", "id"}) {
		t.Fatalf("keys: pk=%v sk=%v", got.PartitionKey, got.SortKey)
	}
	if got.Replication != ReplicationGlobal || !strings.HasPrefix(got.PhysicalName, "p_default.events.") || StripIncarnation(strings.TrimPrefix(got.PhysicalName, "p_default.")) != "events" || got.Version != 1 || got.CreatedAt.IsZero() {
		t.Fatalf("derived fields: %+v", got)
	}
	if pt, ok := c.TableByPhysicalName(got.PhysicalName); !ok || pt != got {
		t.Fatalf("TableByPhysicalName")
	}
	wantCode(t, c.CreateTable(ctx, eventsTable("default", "events"), false), cherr.TableAlreadyExists)
	if err := c.CreateTable(ctx, eventsTable("default", "events"), true); err != nil {
		t.Fatal(err)
	}
	wantCode(t, c.CreateTable(ctx, eventsTable("nodb", "events"), false), cherr.UnknownDatabase)
	wantCode(t, mustErr(c.Table("default", "missing")), cherr.UnknownTable)
	wantCode(t, mustErr(c.Table("nodb", "x")), cherr.UnknownDatabase)

	// Partition key setting and constant partition.
	pt := eventsTable("default", "events2")
	pt.Settings = map[string]string{"dynamodb_partition_key": "tenant, `ts`"}
	if err := c.CreateTable(ctx, pt, false); err != nil {
		t.Fatal(err)
	}
	got, _ = c.Table("default", "events2")
	if !reflect.DeepEqual(got.PartitionKey, []string{"tenant", "ts"}) || !reflect.DeepEqual(got.SortKey, []string{"id"}) {
		t.Fatalf("keys with setting: %v %v", got.PartitionKey, got.SortKey)
	}
	mem := &Table{Database: "default", Name: "mem", Engine: EngineMemory, Columns: []Column{col("x", "String")}}
	if err := c.CreateTable(ctx, mem, false); err != nil {
		t.Fatal(err)
	}
	got, _ = c.Table("default", "mem")
	if len(got.PartitionKey) != 0 || len(got.SortKey) != 0 || got.PartitionKey == nil {
		t.Fatalf("Memory keys: %#v %#v", got.PartitionKey, got.SortKey)
	}

	agg := &Table{
		Database: "default", Name: "rollup", Engine: EngineAggregatingMergeTree,
		Columns: []Column{col("tenant", "String"), col("users", "AggregateFunction(uniqExact, String)"),
			col("total", "SimpleAggregateFunction(sum, UInt64)")},
		OrderBy: []string{"tenant"},
	}
	if err := c.CreateTable(ctx, agg, false); err != nil {
		t.Fatal(err)
	}
	got, _ = c.Table("default", "rollup")
	if got.Replication != ReplicationRegional {
		t.Fatalf("aggregating replication = %s", got.Replication)
	}

	rep := &Table{Database: "default", Name: "snap", Engine: EngineReplacingMergeTree, EngineArgs: []string{"`ver`"},
		Columns: []Column{col("k", "String"), col("ver", "UInt64")}, OrderBy: []string{"k"}}
	if err := c.CreateTable(ctx, rep, false); err != nil {
		t.Fatal(err)
	}
	got, _ = c.Table("default", "snap")
	if got.VersionColumn != "ver" {
		t.Fatalf("version column = %q", got.VersionColumn)
	}

	// Caller mutations after create must not leak into the catalog.
	in := eventsTable("default", "iso")
	if err := c.CreateTable(ctx, in, false); err != nil {
		t.Fatal(err)
	}
	in.Columns[0].Name = "mutated"
	got, _ = c.Table("default", "iso")
	if got.Columns[0].Name != "tenant" {
		t.Fatal("catalog shares caller's column slice")
	}
}

func TestCreateTableValidation(t *testing.T) {
	base := func() *Table { return eventsTable("default", "t") }
	cases := []struct {
		name   string
		mutate func(*Table)
		code   int
	}{
		{"reserved column", func(t *Table) { t.Columns = append(t.Columns, col("_rid", "String")) }, cherr.IllegalColumn},
		{"reserved _origin", func(t *Table) { t.Columns = append(t.Columns, col("_origin", "String")) }, cherr.IllegalColumn},
		{"hash in column", func(t *Table) { t.Columns = append(t.Columns, col("a#n", "String")) }, cherr.IllegalColumn},
		{"duplicate column", func(t *Table) { t.Columns = append(t.Columns, col("id", "String")) }, cherr.IllegalColumn},
		{"bad table name", func(t *Table) { t.Name = "a.b" }, cherr.BadArguments},
		{"bad db name", func(t *Table) { t.Database = "a b" }, cherr.BadArguments},
		{"too long name", func(t *Table) { t.Name = string(make([]byte, 0)) + fmt.Sprintf("%0250d", 0) }, cherr.BadArguments},
		{"no columns", func(t *Table) { t.Columns = nil; t.OrderBy = nil }, cherr.EmptyDataPassed},
		{"unknown engine", func(t *Table) { t.Engine = "Kafka" }, cherr.UnknownStorage},
		{"order by missing col", func(t *Table) { t.OrderBy = []string{"nope"} }, cherr.UnknownIdentifier},
		{"order by duplicate", func(t *Table) { t.OrderBy = []string{"id", "id"} }, cherr.BadArguments},
		{"bad partition split", func(t *Table) { t.PartitionKey = []string{"ts"}; t.SortKey = []string{"tenant", "id"} }, cherr.BadArguments},
		{"partition setting not prefix", func(t *Table) { t.Settings = map[string]string{"dynamodb_partition_key": "ts"} }, cherr.BadArguments},
		{"agg in MergeTree", func(t *Table) {
			t.Columns = append(t.Columns, col("u", "AggregateFunction(uniqExact, String)"))
		}, cherr.NotImplemented},
		{"unsupported agg", func(t *Table) {
			t.Engine = EngineAggregatingMergeTree
			t.Columns = append(t.Columns, col("q", "AggregateFunction(quantile, UInt64)"))
		}, cherr.NotImplemented},
		{"unsupported simple agg", func(t *Table) { t.Columns = append(t.Columns, col("m", "SimpleAggregateFunction(median, UInt64)")) }, cherr.NotImplemented},
		{"agg in key", func(t *Table) {
			t.Engine = EngineAggregatingMergeTree
			t.Columns = append(t.Columns, col("u", "AggregateFunction(sum, UInt64)"))
			t.OrderBy = []string{"tenant", "u"}
		}, cherr.IllegalColumn},
		{"version missing", func(t *Table) { t.Engine = EngineReplacingMergeTree; t.EngineArgs = []string{"nope"} }, cherr.NoSuchColumnInTable},
		{"version bad type", func(t *Table) { t.Engine = EngineReplacingMergeTree; t.VersionColumn = "tenant" }, cherr.BadArguments},
		{"is_deleted bad type", func(t *Table) { t.Engine = EngineReplacingMergeTree; t.EngineArgs = []string{"id", "tenant"} }, cherr.BadArguments},
		{"sum column key", func(t *Table) { t.Engine = EngineSummingMergeTree; t.SumColumns = []string{"id"} }, cherr.BadArguments},
		{"sum column non-numeric", func(t *Table) { t.Engine = EngineSummingMergeTree; t.SumColumns = []string{"body"} }, cherr.BadArguments},
		{"sum column missing", func(t *Table) { t.Engine = EngineSummingMergeTree; t.EngineArgs = []string{"(nope)"} }, cherr.NoSuchColumnInTable},
		{"memory with order by", func(t *Table) { t.Engine = EngineMemory }, cherr.BadArguments},
		{"bad replication", func(t *Table) { t.Replication = "both" }, cherr.BadArguments},
		{"no type", func(t *Table) { t.Columns[3] = Column{Name: "body"} }, cherr.UnknownType},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCatalog(newMemBackend())
			tbl := base()
			tc.mutate(tbl)
			wantCode(t, c.CreateTable(ctx, tbl, false), tc.code)
			if len(c.Tables("default")) != 0 {
				t.Fatal("invalid table was stored")
			}
		})
	}
}

func TestPersistRoundTrip(t *testing.T) {
	b := newMemBackend()
	a := newTestCatalog(b)
	if err := a.CreateDatabase(ctx, &Database{Name: "app"}, false); err != nil {
		t.Fatal(err)
	}
	src := eventsTable("app", "events")
	src.Settings = map[string]string{"index_granularity": "8192"}
	src.Columns[3].DefaultKind, src.Columns[3].DefaultExpr, src.Columns[3].Comment = "DEFAULT", "'x'", "body text"
	src.DDL, src.Comment = "CREATE TABLE app.events ...", "events table"
	if err := a.CreateTable(ctx, src, false); err != nil {
		t.Fatal(err)
	}
	roll := &Table{Database: "app", Name: "rollup", Engine: EngineSummingMergeTree, EngineArgs: []string{"(n)"},
		Columns: []Column{col("tenant", "String"), col("n", "UInt64"), col("f", "Float64")}, OrderBy: []string{"tenant"}}
	if err := a.CreateTable(ctx, roll, false); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mv_b", "mv_a"} {
		v := &View{Database: "app", Name: name, Materialized: true, ToDatabase: "app", ToTable: "rollup",
			SourceDatabase: "app", SourceTable: "events", SelectSQL: "SELECT tenant, count() AS n FROM app.events GROUP BY tenant"}
		if err := a.CreateView(ctx, v, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.CreateView(ctx, &View{Database: "app", Name: "plain", SelectSQL: "SELECT 1"}, false); err != nil {
		t.Fatal(err)
	}

	bcat := newTestCatalog(b)
	if err := bcat.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"events", "rollup"} {
		want, _ := a.Table("app", name)
		got, err := bcat.Table("app", name)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
		}
	}
	if roll, err := bcat.Table("app", "rollup"); err != nil {
		t.Fatal(err)
	} else if _, ok := bcat.TableByPhysicalName(roll.PhysicalName); !ok || StripIncarnation(strings.TrimPrefix(roll.PhysicalName, "p_app.")) != "rollup" {
		t.Fatal("physical lookup after refresh")
	}
	var names []string
	for _, v := range bcat.ViewsFrom("app", "events") {
		names = append(names, v.Name)
	}
	if !reflect.DeepEqual(names, []string{"mv_a", "mv_b"}) {
		t.Fatalf("ViewsFrom = %v", names)
	}
	if v, err := bcat.View("app", "plain"); err != nil || v.Materialized {
		t.Fatalf("plain view: %v %v", v, err)
	}
	if len(bcat.ViewsFrom("app", "rollup")) != 0 {
		t.Fatal("ViewsFrom must match the source")
	}

	// Names are shared between tables and views.
	wantCode(t, bcat.CreateView(ctx, &View{Database: "app", Name: "events", SelectSQL: "SELECT 1"}, false), cherr.TableAlreadyExists)
	wantCode(t, a.CreateTable(ctx, eventsTable("app", "plain"), false), cherr.TableAlreadyExists)
	// Target of a materialized view must exist.
	wantCode(t, a.CreateView(ctx, &View{Database: "app", Name: "mv_x", Materialized: true, ToDatabase: "app", ToTable: "nope",
		SelectSQL: "SELECT 1"}, false), cherr.UnknownTable)

	// A stale snapshot still detects conflicts through backend conditions.
	stale := newTestCatalog(b)
	wantCode(t, stale.CreateTable(ctx, eventsTable("app", "x"), false), cherr.UnknownDatabase)
	if err := stale.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bcat.DropTable(ctx, "app", "events", false); err != nil {
		t.Fatal(err)
	}
	// The stale snapshot still lists the table until it refreshes.
	wantCode(t, stale.CreateTable(ctx, eventsTable("app", "events"), false), cherr.TableAlreadyExists)
	if err := stale.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := stale.CreateTable(ctx, eventsTable("app", "events"), false); err != nil {
		t.Fatalf("recreate after drop elsewhere: %v", err)
	}
	other := newTestCatalog(b) // empty snapshot, but "default" is implicit
	wantCode(t, other.CreateDatabase(ctx, &Database{Name: "app"}, false), cherr.DatabaseAlreadyExists)
}

func TestUpdateTable(t *testing.T) {
	b := newMemBackend()
	a, other := newTestCatalog(b), newTestCatalog(b)
	if err := a.CreateTable(ctx, eventsTable("default", "events"), false); err != nil {
		t.Fatal(err)
	}
	if err := other.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	cur, _ := a.Table("default", "events")
	upd := *cur
	upd.Columns = append(append([]Column{}, cur.Columns...), col("extra", "UInt8"))
	upd.PhysicalName, upd.Version = "ignored", 99
	if err := a.UpdateTable(ctx, &upd); err != nil {
		t.Fatal(err)
	}
	got, _ := a.Table("default", "events")
	if got.Version != 2 || len(got.Columns) != 5 || got.PhysicalName != cur.PhysicalName || !got.CreatedAt.Equal(cur.CreatedAt) {
		t.Fatalf("after update: %+v", got)
	}
	if cur.Version != 1 || len(cur.Columns) != 4 {
		t.Fatal("previously returned table was mutated")
	}
	// other still has version 1: its update must be rejected.
	stale, _ := other.Table("default", "events")
	wantCode(t, other.UpdateTable(ctx, stale), cherr.LogicalError)
	wantCode(t, a.UpdateTable(ctx, eventsTable("default", "missing")), cherr.UnknownTable)
	bad := *got
	bad.Columns = append(append([]Column{}, got.Columns...), col("_ts", "UInt8"))
	wantCode(t, a.UpdateTable(ctx, &bad), cherr.IllegalColumn)
}

func TestDropDatabaseRemovesObjects(t *testing.T) {
	b := newMemBackend()
	a := newTestCatalog(b)
	must(t, a.CreateDatabase(ctx, &Database{Name: "app"}, false))
	must(t, a.CreateTable(ctx, eventsTable("app", "events"), false))
	// Created by another process: DropDatabase must still remove it.
	other := newTestCatalog(b)
	must(t, other.Refresh(ctx))
	must(t, other.CreateView(ctx, &View{Database: "app", Name: "v", SelectSQL: "SELECT 1"}, false))
	must(t, a.CreateTable(ctx, eventsTable("default", "keep"), false))

	must(t, a.DropDatabase(ctx, "app", false))
	if _, ok := a.TableByPhysicalName("p_app.events"); ok {
		t.Fatal("physical index not cleaned")
	}
	keys := backendKeys(b)
	if !reflect.DeepEqual(keys, []string{"table#default.keep"}) {
		t.Fatalf("backend after DropDatabase: %v", keys)
	}
	wantCode(t, mustErr(a.Table("app", "events")), cherr.UnknownDatabase)
	must(t, a.DropTable(ctx, "default", "keep", false))
	wantCode(t, a.DropTable(ctx, "default", "keep", false), cherr.UnknownTable)
	must(t, a.DropTable(ctx, "default", "keep", true))
	wantCode(t, a.DropView(ctx, "default", "v", false), cherr.UnknownTable)
	if len(backendKeys(b)) != 0 {
		t.Fatalf("backend not empty: %v", backendKeys(b))
	}
}

func backendKeys(b *memBackend) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var keys []string
	for k := range b.entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// An undecodable entry is quarantined; the rest of the catalog still loads
// (one bad entry must not halt refresh, the stream workers or startup).
func TestRefreshQuarantinesUndecodableEntries(t *testing.T) {
	b := newMemBackend()
	a := newTestCatalog(b)
	must(t, a.CreateTable(ctx, eventsTable("default", "events"), false))
	b.entries["table#default.bad"] = Entry{Key: "table#default.bad", Kind: KindTable, Def: []byte(`{"database":"default","name":"bad","physical_name":"p.default.bad","columns":[{"name":"x","type":"NoSuchType"}]}`)}
	b.entries["view#default.vbad"] = Entry{Key: "view#default.vbad", Kind: KindView, Def: []byte(`{"database":"default","name":"vbad","source_database":"default","source_table":"events","created_at":"bogus"}`)}
	b.entries["junk"] = Entry{Key: "junk", Kind: KindMalformed}
	other := newTestCatalog(b)
	must(t, other.Refresh(ctx))
	if _, err := other.Table("default", "events"); err != nil {
		t.Fatalf("good entry not loaded: %v", err)
	}
	wantCode(t, func() error { _, err := other.Table("default", "bad"); return err }(), cherr.IncorrectData)
	q := QuarantinedEntries(other)
	if len(q) != 3 || q[0].Key != "junk" || q[1].Key != "table#default.bad" || q[1].PhysicalName != "p.default.bad" || q[1].Err == nil ||
		q[2].SourceTable != "events" {
		t.Fatalf("quarantine = %+v", q)
	}
	// DROP DATABASE removes quarantined objects of the database too.
	must(t, other.CreateDatabase(ctx, &Database{Name: "x"}, false))
	b.entries["table#x.bad"] = Entry{Key: "table#x.bad", Kind: KindTable, Def: []byte(`{"database":"x","name":"bad","columns":[{"name":"x","type":"NoSuchType"}]}`)}
	must(t, other.DropDatabase(ctx, "x", false))
	if _, ok := b.entries["table#x.bad"]; ok {
		t.Fatal("DROP DATABASE left a quarantined table entry behind")
	}
	delete(b.entries, "table#default.bad")
	delete(b.entries, "view#default.vbad")
	delete(b.entries, "junk")
	must(t, other.Refresh(ctx))
	if q := QuarantinedEntries(other); len(q) != 0 {
		t.Fatalf("quarantine not cleared: %+v", q)
	}
	b.fail = errors.New("boom")
	wantCode(t, a.Refresh(ctx), cherr.Unavailable)
	if _, err := a.Table("default", "events"); err != nil {
		t.Fatal("snapshot lost after failed refresh")
	}
}

func TestMemoryCatalog(t *testing.T) {
	c := NewMemory()
	tbl := &Table{Database: "default", Name: "t", Engine: EngineMergeTree,
		Columns: []Column{{Name: "a", Type: chtype.TUInt64, TypeText: "UInt64"}}, OrderBy: []string{"a"}}
	must(t, c.CreateTable(ctx, tbl, false))
	must(t, c.Refresh(ctx))
	got, err := c.Table("default", "t")
	if err != nil || got.PhysicalName != "default.t" {
		t.Fatalf("memory table: %+v %v", got, err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	b := newMemBackend()
	c := newTestCatalog(b)
	var wg sync.WaitGroup
	errs := make(chan error, 16+16*5)
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			errs <- c.CreateTable(ctx, eventsTable("default", fmt.Sprintf("t%d", i%8)), true)
		}(i)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = c.Tables("default")
				_ = c.ViewsFrom("default", "t1")
				if j%10 == 0 {
					errs <- c.Refresh(ctx)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := len(c.Tables("default")); n != 8 {
		t.Fatalf("got %d tables", n)
	}
}

func TestAggregateSpecs(t *testing.T) {
	cases := []struct {
		typ   chtype.Type
		fn    string
		nargs int
		code  int
	}{
		{chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "COUNT"}, AggCount, 0, 0},
		{chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "sumIf", AggArgs: []chtype.Type{chtype.TUInt64, chtype.TUInt8}}, AggSum, 1, 0},
		{chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "argMax", AggArgs: []chtype.Type{chtype.TString, chtype.TUInt64}}, AggArgMax, 2, 0},
		{chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "argMax", AggArgs: []chtype.Type{chtype.TString}}, "", 0, cherr.NumberOfArgsDoesntMatch},
		{chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "uniq", AggArgs: []chtype.Type{chtype.TString, chtype.TUInt8}}, AggUniq, 2, 0},
		{chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "uniqExact"}, "", 0, cherr.NumberOfArgsDoesntMatch},
		{chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "groupArray", AggArgs: []chtype.Type{chtype.TString}, AggParams: []any{uint64(5)}}, "", 0, cherr.NotImplemented},
		{chtype.Type{Kind: chtype.AggregateFunction, AggFunc: "uniqexact", AggArgs: []chtype.Type{chtype.TString}}, "", 0, cherr.NotImplemented},
	}
	for i, c := range cases {
		spec, err := ResolveAggregate(c.typ)
		if c.code != 0 {
			if !cherr.Is(err, c.code) {
				t.Errorf("case %d: err %v want code %d", i, err, c.code)
			}
			continue
		}
		if err != nil || spec.Func != c.fn || len(spec.Args) != c.nargs {
			t.Errorf("case %d: %+v %v", i, spec, err)
		}
	}
	if k := (AggregateSpec{Func: AggSum, Args: []chtype.Type{chtype.NewNullable(chtype.TInt64)}}).SumKind(); k != chtype.Int64 {
		t.Errorf("SumKind(Nullable(Int64)) = %v", k)
	}
	mt := (AggregateSpec{Func: AggUniq, Args: []chtype.Type{chtype.TString, chtype.TUInt8}}).MemberType()
	if mt.Kind != chtype.Tuple || len(mt.Fields) != 2 {
		t.Errorf("MemberType = %+v", mt)
	}
	saf := []struct {
		typ  chtype.Type
		fn   string
		code int
	}{
		{testTypes["SimpleAggregateFunction(sum, UInt64)"], AggSum, 0},
		{testTypes["SimpleAggregateFunction(groupUniqArrayArray, Array(String))"], AggGroupUniqArray, 0},
		{chtype.Type{Kind: chtype.SimpleAggregateFunction, AggFunc: "groupUniqArray", Elem: tptr(chtype.TString)}, "", cherr.IllegalTypeOfArgument},
		{chtype.Type{Kind: chtype.SimpleAggregateFunction, AggFunc: "sum", Elem: tptr(chtype.TString)}, "", cherr.IllegalTypeOfArgument},
		{chtype.Type{Kind: chtype.SimpleAggregateFunction, AggFunc: "anyLast", Elem: tptr(chtype.TString)}, AggAnyLast, 0},
	}
	for i, c := range saf {
		fn, err := ResolveSimpleAggregate(c.typ)
		if c.code != 0 {
			if !cherr.Is(err, c.code) {
				t.Errorf("saf %d: err %v want %d", i, err, c.code)
			}
			continue
		}
		if err != nil || fn != c.fn {
			t.Errorf("saf %d: %q %v", i, fn, err)
		}
	}
}

func TestCodecPreservesTimes(t *testing.T) {
	tbl := eventsTable("default", "t")
	tbl.CreatedAt = time.Date(2024, 5, 6, 7, 8, 9, 123456789, time.FixedZone("x", 3600))
	tbl.PartitionKey, tbl.SortKey = []string{}, []string{"tenant", "ts", "id"}
	b, err := EncodeTable(tbl)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeTable(b, testParse)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(tbl.CreatedAt) || got.CreatedAt.Location() != time.UTC {
		t.Fatalf("CreatedAt %v", got.CreatedAt)
	}
	if got.PartitionKey == nil || len(got.PartitionKey) != 0 {
		t.Fatalf("empty partition key must decode as empty slice")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// staleLoadBackend returns, from LoadAll, the entries as they were before
// running hook (which makes a local DDL): the load misses that DDL, as a
// scan that passed a key before the DDL committed it does.
type staleLoadBackend struct {
	*memBackend
	hook func()
}

func (b *staleLoadBackend) LoadAll(ctx context.Context) ([]Entry, error) {
	out, err := b.memBackend.LoadAll(ctx)
	if h := b.hook; h != nil {
		h()
	}
	return out, err
}

// A Refresh racing with local DDL keeps the DDL, however often it races
// (a CREATE DATABASE must be usable by the next statement on the server).
func TestRefreshKeepsConcurrentLocalDDL(t *testing.T) {
	b := &staleLoadBackend{memBackend: newMemBackend()}
	c := newTestCatalog(b)
	for i := range 6 {
		name := fmt.Sprintf("d%d", i)
		b.hook = func() {
			must(t, c.CreateDatabase(ctx, &Database{Name: name}, false))
			must(t, c.CreateTable(ctx, eventsTable(name, "t"), false))
		}
		must(t, c.Refresh(ctx))
		b.hook = nil
		if _, err := c.Database(name); err != nil {
			t.Fatalf("round %d: database lost by a racing refresh: %v", i, err)
		}
		if _, err := c.Table(name, "t"); err != nil {
			t.Fatalf("round %d: table lost by a racing refresh: %v", i, err)
		}
	}
	// A dropped table stays dropped.
	b.hook = func() { must(t, c.DropTable(ctx, "d0", "t", false)) }
	must(t, c.Refresh(ctx))
	b.hook = nil
	if _, err := c.Table("d0", "t"); err == nil {
		t.Fatal("dropped table resurrected by a racing refresh")
	}
	// Once loaded normally, the backend is the truth again.
	must(t, c.Refresh(ctx))
	if _, err := c.Table("d1", "t"); err != nil {
		t.Fatal(err)
	}
}

// Each incarnation of a persisted table gets its own physical name; an
// ALTER keeps it; the memory catalog keeps plain names.
func TestPhysicalNamePerIncarnation(t *testing.T) {
	c := newTestCatalog(newMemBackend())
	must(t, c.CreateTable(ctx, eventsTable("default", "events"), false))
	first, _ := c.Table("default", "events")
	if !strings.HasPrefix(first.PhysicalName, "p_default.events.") || len(first.PhysicalName) != len("p_default.events.")+8 {
		t.Fatalf("physical name %q", first.PhysicalName)
	}
	alt := *first
	alt.Comment = "x"
	must(t, c.UpdateTable(ctx, &alt))
	if got, _ := c.Table("default", "events"); got.PhysicalName != first.PhysicalName {
		t.Fatalf("ALTER changed the physical name: %q -> %q", first.PhysicalName, got.PhysicalName)
	}
	must(t, c.DropTable(ctx, "default", "events", false))
	must(t, c.CreateTable(ctx, eventsTable("default", "events"), false))
	second, _ := c.Table("default", "events")
	if second.PhysicalName == first.PhysicalName || StripIncarnation(strings.TrimPrefix(second.PhysicalName, "p_default.")) != "events" {
		t.Fatalf("recreated table reuses %q (%q)", first.PhysicalName, second.PhysicalName)
	}
	if _, ok := c.TableByPhysicalName(first.PhysicalName); ok {
		t.Fatal("old incarnation still resolvable")
	}
	m := NewMemory()
	must(t, m.CreateTable(ctx, eventsTable("default", "events"), false))
	if got, _ := m.Table("default", "events"); got.PhysicalName != "default.events" {
		t.Fatalf("memory catalog physical name %q", got.PhysicalName)
	}
	for in, want := range map[string]string{
		"t": "t", "t.members": "t.members", "t.abcdefg2": "t", "campaign": "campaign",
		"t.abcdefg2.members": "t.members", "t.ABCDEFG2": "t.ABCDEFG2", "campaign.members": "campaign.members",
	} {
		if got := StripIncarnation(in); got != want {
			t.Errorf("StripIncarnation(%q) = %q, want %q", in, got, want)
		}
	}
}
