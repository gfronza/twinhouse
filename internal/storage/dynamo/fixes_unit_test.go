package dynamo

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/storage"
)

func TestSourceIDs(t *testing.T) {
	cases := []struct {
		id    string
		rid   string
		ts    int64
		gated bool
	}{
		{sourceID("abc", 42, true), "abc", 42, true},
		{sourceID("abc", 42, false), "abc", 42, false},
		{sourceID("abc", 0, true), "abc", 0, false},
		{"populate:run:7", "populate:run:7", 0, false},
		{"r1", "r1", 0, false},
		{"a@b", "a@b", 0, false},
		{"a@", "a@", 0, false},
		{"@5", "@5", 0, false},
		{"x@=-3", "x@=-3", 0, false},
	}
	for _, c := range cases {
		rid, ts, gated := parseSourceID(c.id)
		if rid != c.rid || ts != c.ts || gated != c.gated {
			t.Errorf("parseSourceID(%q) = %q %d %v, want %q %d %v", c.id, rid, ts, gated, c.rid, c.ts, c.gated)
		}
	}
	rows := []storage.MergeRow{
		{Source: storage.SourceRef{View: "v", RowID: sourceID("old", 10, true)}},
		{Source: storage.SourceRef{View: "v", RowID: sourceID("new", 30, true)}},
		{Source: storage.SourceRef{View: "v", RowID: sourceID("populate", 10, false)}},
		{Source: storage.SourceRef{View: "v", RowID: "plain"}},
	}
	var got []string
	for _, m := range parseMergeRows(rows, 20) {
		got = append(got, m.Source.RowID)
	}
	if strings.Join(got, ",") != "new,populate,plain" {
		t.Fatalf("gated rows: %v", got)
	}
}

func TestStampIsMonotonic(t *testing.T) {
	s := &storeImpl{now: func() time.Time { return time.Unix(0, 100) }}
	a := s.stamp(3) // 100..102
	b := s.stamp(1)
	s.bumpStamp(500)
	c := s.stamp(1)
	if a != 100 || b != 103 || c != 501 {
		t.Fatalf("stamps %d %d %d", a, b, c)
	}
}

// TestDecodeModifiedColumns covers values stored before ALTER TABLE MODIFY
// COLUMN changed the type (MODIFY is catalog-only).
func TestDecodeModifiedColumns(t *testing.T) {
	tbl := &catalog.Table{Database: "default", Name: "m", Engine: catalog.EngineMergeTree, OrderBy: []string{"k"},
		Columns: cols("k", "UInt32", "d", "Date", "g", "String", "c", "Int32", "u", "UInt8", "s", "String", "f", "DateTime64(3)")}
	tbl.Columns[2].PrevTypes = []string{"Int64"}
	tbl.Columns[3].PrevTypes = []string{"Float64"}
	tbl.Columns[4].PrevTypes = []string{"String"}
	tbl.Columns[5].PrevTypes = []string{"DateTime"}
	l := testLayout(t, tbl)
	t13 := time.Date(2024, 1, 1, 13, 0, 0, 123456789, time.UTC)
	item := map[string]types.AttributeValue{
		"k": attrN("1"),
		"d": attrN(itoa64(t13.UnixNano())),                                          // DateTime64(9) -> Date
		"g": attrN("5"),                                                             // Int64 -> String
		"c": attrN("-2.7"),                                                          // Float64 -> Int32
		"u": attrS("7"),                                                             // String -> UInt8
		"s": attrN(itoa64(time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC).UnixNano())), // DateTime -> String
		"f": attrN(itoa64(t13.UnixNano())),                                          // DateTime64(9) -> DateTime64(3)
	}
	row, err := (&storeImpl{}).decodeRow(context.Background(), l, item, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{uint64(1), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "5", int64(-2), uint64(7),
		"2024-01-01 13:00:00", time.Date(2024, 1, 1, 13, 0, 0, 123000000, time.UTC)}
	for i, w := range want {
		if !chtype.Equal(row[i], w) {
			t.Errorf("column %s: got %#v want %#v", l.cols[i].name, row[i], w)
		}
	}
	// Without a previous type the mismatch is still reported.
	l.cols[2].prev = nil
	if _, err := (&storeImpl{}).decodeRow(context.Background(), l, item, nil, nil); !cherr.Is(err, cherr.IncorrectData) {
		t.Fatalf("want INCORRECT_DATA, got %v", err)
	}
}

func TestHomeRegion(t *testing.T) {
	cases := []struct {
		opts Options
		want string
	}{
		{Options{Region: "us-east-1"}, "us-east-1"},
		{Options{Region: "us-east-1", ReplicaRegions: []string{"us-east-1", "eu-west-1"}}, "us-east-1"},
		{Options{Region: "eu-west-1", ReplicaRegions: []string{"us-east-1", "eu-west-1"}}, "us-east-1"},
		{Options{Region: "us-east-1", ReplicaRegions: []string{"eu-west-1"}}, "eu-west-1"},
		{Options{Region: "eu-west-1", ReplicaRegions: []string{"us-east-1"}}, "eu-west-1"},
		{Options{Region: "eu-west-1", ReplicaRegions: []string{"us-east-1"}, HomeRegion: "us-east-1"}, "us-east-1"},
	}
	for _, c := range cases {
		if got := (&storeImpl{opts: c.opts}).homeRegion(); got != c.want {
			t.Errorf("%+v: home %s, want %s", c.opts, got, c.want)
		}
	}
}

// fakeWorld models the AWS control plane of several regions: CreateTable
// leaves a table CREATING for createDelay, a replica can only be added in a
// region that has no table of that name, and replicas share a group.
type fakeWorld struct {
	mu          sync.Mutex
	createDelay time.Duration
	tables      map[string]map[string]*fakeTable // region -> name -> table
	creates     []string
}

type fakeTable struct {
	in     *dynamodb.CreateTableInput
	ready  time.Time
	group  *[]string
	stream bool
}

func newFakeWorld(delay time.Duration) *fakeWorld {
	return &fakeWorld{createDelay: delay, tables: map[string]map[string]*fakeTable{}}
}

type fakeRegion struct {
	ddbAPI // nil: unexpected calls panic
	w      *fakeWorld
	region string
}

func (w *fakeWorld) region(name string) *fakeRegion {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tables[name] == nil {
		w.tables[name] = map[string]*fakeTable{}
	}
	return &fakeRegion{w: w, region: name}
}

func notFound(name string) error {
	return &types.ResourceNotFoundException{Message: aws.String("Requested resource not found: Table: " + name + " not found")}
}

func (f *fakeRegion) CreateTable(_ context.Context, in *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	f.w.mu.Lock()
	defer f.w.mu.Unlock()
	name := aws.ToString(in.TableName)
	if f.w.tables[f.region][name] != nil {
		return nil, &types.ResourceInUseException{Message: aws.String("Table already exists: " + name)}
	}
	f.w.creates = append(f.w.creates, f.region+":"+name)
	g := []string{f.region}
	f.w.tables[f.region][name] = &fakeTable{in: in, ready: time.Now().Add(f.w.createDelay), group: &g, stream: in.StreamSpecification != nil}
	return &dynamodb.CreateTableOutput{}, nil
}

func (f *fakeRegion) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	f.w.mu.Lock()
	defer f.w.mu.Unlock()
	name := aws.ToString(in.TableName)
	tb := f.w.tables[f.region][name]
	if tb == nil {
		return nil, notFound(name)
	}
	status := types.TableStatusActive
	if time.Now().Before(tb.ready) {
		status = types.TableStatusCreating
	}
	d := &types.TableDescription{TableName: in.TableName, TableStatus: status, KeySchema: tb.in.KeySchema,
		AttributeDefinitions: tb.in.AttributeDefinitions, LatestStreamArn: aws.String("arn:" + f.region + ":" + name)}
	if tb.stream {
		d.StreamSpecification = &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewAndOldImages}
	}
	if len(*tb.group) > 1 {
		for _, r := range *tb.group {
			d.Replicas = append(d.Replicas, types.ReplicaDescription{RegionName: aws.String(r)})
		}
	}
	return &dynamodb.DescribeTableOutput{Table: d}, nil
}

type validationErr struct{ msg string }

func (e *validationErr) Error() string                 { return "ValidationException: " + e.msg }
func (e *validationErr) ErrorCode() string             { return "ValidationException" }
func (e *validationErr) ErrorMessage() string          { return e.msg }
func (e *validationErr) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func (f *fakeRegion) UpdateTable(_ context.Context, in *dynamodb.UpdateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	f.w.mu.Lock()
	defer f.w.mu.Unlock()
	name := aws.ToString(in.TableName)
	tb := f.w.tables[f.region][name]
	if tb == nil {
		return nil, notFound(name)
	}
	for _, u := range in.ReplicaUpdates {
		if u.Create == nil {
			continue
		}
		r := aws.ToString(u.Create.RegionName)
		if f.w.tables[r] == nil {
			f.w.tables[r] = map[string]*fakeTable{}
		}
		if f.w.tables[r][name] != nil {
			return nil, &validationErr{"Failed to create a the new replica of table with name: " + name +
				" because one or more replicas already existed as tables."}
		}
		*tb.group = append(*tb.group, r)
		f.w.tables[r][name] = &fakeTable{in: tb.in, ready: time.Now(), group: tb.group, stream: tb.stream}
	}
	return &dynamodb.UpdateTableOutput{}, nil
}

func (f *fakeRegion) DescribeTimeToLive(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return &dynamodb.DescribeTimeToLiveOutput{TimeToLiveDescription: &types.TimeToLiveDescription{TimeToLiveStatus: types.TimeToLiveStatusEnabled}}, nil
}

// Item calls (reset items in __mv_markers) see no markers table.
func (f *fakeRegion) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, notFound(aws.ToString(in.TableName))
}

func (f *fakeRegion) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return nil, notFound(aws.ToString(in.TableName))
}

func awsStore(t *testing.T, w *fakeWorld, region string) *Store {
	t.Helper()
	s, err := newStoreWithAPI(w.region(region), region, Options{Prefix: "p_", GlobalMode: "aws",
		ReplicaRegions: []string{"us-east-1", "eu-west-1"}})
	if err != nil {
		t.Fatal(err)
	}
	s.impl.tableWait = 20 * time.Second
	return s
}

// TestAWSOnlyHomeRegionCreatesGlobalTables: the other region's reconciler
// (EnsureTable as soon as the catalog entry replicates) and a concurrent
// bootstrap (EnsureSystemTables) must not create their own copy of a global
// table, or AWS refuses to add the replica and the regions never replicate.
func TestAWSOnlyHomeRegionCreatesGlobalTables(t *testing.T) {
	w := newFakeWorld(400 * time.Millisecond)
	us, eu := awsStore(t, w, "us-east-1"), awsStore(t, w, "eu-west-1")
	ctx := context.Background()

	// Concurrent bootstrap: us is the home of __catalog (first REPLICA_REGIONS entry).
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = us.EnsureSystemTables(ctx) }()
	go func() { defer wg.Done(); time.Sleep(50 * time.Millisecond); errs[1] = eu.EnsureSystemTables(ctx) }()
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("bootstrap: us %v / eu %v", errs[0], errs[1])
	}

	// CREATE TABLE in eu: the entry's home is eu; us's reconciler runs
	// while eu's CreateTable is still in progress.
	tbl := &catalog.Table{Database: "default", Name: "events", Engine: catalog.EngineMergeTree,
		Columns: cols("k", "String"), OrderBy: []string{"k"}, Replication: catalog.ReplicationGlobal,
		PhysicalName: "p_default.events", HomeRegion: "eu-west-1", CreatedAt: time.Now()}
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = eu.EnsureTable(ctx, tbl) }()
	go func() { defer wg.Done(); time.Sleep(100 * time.Millisecond); errs[1] = us.EnsureTable(ctx, tbl) }()
	wg.Wait()
	if errs[0] != nil {
		t.Fatalf("home region CREATE: %v", errs[0])
	}
	if errs[1] != nil && !cherr.Is(errs[1], cherr.Unavailable) {
		t.Fatalf("reconciler before the replica exists must report it pending, got %v", errs[1])
	}
	if err := us.EnsureTable(ctx, tbl); err != nil { // next reconcile round
		t.Fatalf("reconciler after the replica exists: %v", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, name := range []string{"p___catalog", "p_default.events"} {
		ga, gb := w.tables["us-east-1"][name].group, w.tables["eu-west-1"][name].group
		if ga != gb || len(*ga) != 2 {
			t.Errorf("%s: not one replication group: us %v eu %v", name, *ga, *gb)
		}
	}
	if got := strings.Join(w.creates, " "); strings.Contains(got, "us-east-1:p_default.events") || strings.Contains(got, "eu-west-1:p___catalog") {
		t.Errorf("a non-home region created a global table: %s", got)
	}
	// Regional tables (and system tables other than __catalog) exist everywhere.
	if w.tables["eu-west-1"]["p___mv_markers"] == nil || w.tables["us-east-1"]["p___mv_markers"] == nil {
		t.Error("regional system tables must be created in every region")
	}
}

func TestReplicaExistsAsTableError(t *testing.T) {
	w := newFakeWorld(0)
	us, eu := awsStore(t, w, "us-east-1"), awsStore(t, w, "eu-west-1")
	ctx := context.Background()
	// A stray standalone table in eu (e.g. created by an older build).
	spec := tableSpec{name: "p_x", keyType: types.ScalarAttributeTypeS}
	if err := eu.impl.ensurePhysical(ctx, spec); err != nil {
		t.Fatal(err)
	}
	spec.global, spec.home = true, "us-east-1"
	err := us.impl.ensurePhysical(ctx, spec)
	if !cherr.Is(err, cherr.BadArguments) || !strings.Contains(err.Error(), "Delete that table in eu-west-1") {
		t.Fatalf("want an actionable error, got %v", err)
	}
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		t.Fatal("cause must be kept")
	}
}

// TestRegionalMutationsNeedPropagation: in multi-region modes a DELETE on a
// regional table, or a TRUNCATE whose reset is not recorded in the catalog,
// would only change this region's copy, so it is refused.
func TestRegionalMutationsNeedPropagation(t *testing.T) {
	w := newFakeWorld(0)
	s, err := newStoreWithAPI(w.region("us-east-1"), "us-east-1", Options{Prefix: "p_", GlobalMode: "emulated"})
	if err != nil {
		t.Fatal(err)
	}
	roll := &catalog.Table{Database: "default", Name: "roll", Engine: catalog.EngineSummingMergeTree,
		Columns: cols("k", "String", "n", "UInt64"), OrderBy: []string{"k"}, PartitionKey: []string{"k"},
		Replication: catalog.ReplicationRegional, CreatedAt: time.Now()}
	if err := s.DeleteRows(context.Background(), roll, []storage.Row{{"a", uint64(1)}}); !cherr.Is(err, cherr.NotImplemented) {
		t.Fatalf("DELETE on a regional table: %v", err)
	}
	if err := s.TruncateTable(context.Background(), roll); !cherr.Is(err, cherr.NotImplemented) {
		t.Fatalf("TRUNCATE without a catalog reset: %v", err)
	}
	src := *roll
	src.Engine, src.Replication = catalog.EngineMergeTree, catalog.ReplicationGlobal
	if err := s.DeleteRows(context.Background(), &src, nil); err != nil {
		t.Fatalf("DELETE on a global table: %v", err)
	}
}
