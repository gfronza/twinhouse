package mvworker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/storage"
)

// fakeCatalog is a minimal in-memory catalog.Catalog.
type fakeCatalog struct {
	mu        sync.Mutex
	tables    []*catalog.Table
	views     []*catalog.View
	refreshes int
	onRefresh func(*fakeCatalog)
	quarant   []catalog.Quarantined
}

func (c *fakeCatalog) Quarantined() []catalog.Quarantined {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]catalog.Quarantined(nil), c.quarant...)
}

func (c *fakeCatalog) Refresh(context.Context) error {
	c.mu.Lock()
	c.refreshes++
	f := c.onRefresh
	c.mu.Unlock()
	if f != nil {
		f(c)
	}
	return nil
}

func (c *fakeCatalog) Databases() []*catalog.Database {
	return []*catalog.Database{{Name: "default"}, {Name: "db"}}
}

func (c *fakeCatalog) Database(name string) (*catalog.Database, error) {
	return &catalog.Database{Name: name}, nil
}

func (c *fakeCatalog) CreateDatabase(context.Context, *catalog.Database, bool) error {
	return cherr.New(cherr.NotImplemented, "fake")
}

func (c *fakeCatalog) DropDatabase(context.Context, string, bool) error {
	return cherr.New(cherr.NotImplemented, "fake")
}

func (c *fakeCatalog) Tables(db string) []*catalog.Table {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*catalog.Table
	for _, t := range c.tables {
		if t.Database == db {
			out = append(out, t)
		}
	}
	return out
}

func (c *fakeCatalog) Table(db, name string) (*catalog.Table, error) {
	for _, t := range c.Tables(db) {
		if t.Name == name {
			return t, nil
		}
	}
	return nil, cherr.New(cherr.UnknownTable, "Table %s.%s does not exist", db, name)
}

func (c *fakeCatalog) TableByPhysicalName(physical string) (*catalog.Table, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.tables {
		if t.PhysicalName == physical {
			return t, true
		}
	}
	return nil, false
}

func (c *fakeCatalog) CreateTable(context.Context, *catalog.Table, bool) error {
	return cherr.New(cherr.NotImplemented, "fake")
}
func (c *fakeCatalog) UpdateTable(context.Context, *catalog.Table) error {
	return cherr.New(cherr.NotImplemented, "fake")
}
func (c *fakeCatalog) DropTable(context.Context, string, string, bool) error {
	return cherr.New(cherr.NotImplemented, "fake")
}
func (c *fakeCatalog) Views(string) []*catalog.View { return nil }
func (c *fakeCatalog) View(db, name string) (*catalog.View, error) {
	return nil, cherr.New(cherr.UnknownTable, "no view")
}
func (c *fakeCatalog) CreateView(context.Context, *catalog.View, bool) error {
	return cherr.New(cherr.NotImplemented, "fake")
}
func (c *fakeCatalog) DropView(context.Context, string, string, bool) error {
	return cherr.New(cherr.NotImplemented, "fake")
}

func (c *fakeCatalog) ViewsFrom(db, table string) []*catalog.View {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*catalog.View
	for _, v := range c.views {
		if v.SourceDatabase == db && v.SourceTable == table {
			out = append(out, v)
		}
	}
	return out
}

// fakeDecoder decodes items of the form {_rid S, _mv BOOL?, v S, bad?}.
type fakeDecoder struct{}

func (fakeDecoder) DecodeRecord(t *catalog.Table, ev string, img map[string]ddbtypes.AttributeValue) (*storage.StreamRecord, error) {
	if _, bad := img["bad"]; bad {
		return nil, errors.New("cannot decode")
	}
	rid, ok := img["_rid"].(*ddbtypes.AttributeValueMemberS)
	if !ok {
		return nil, nil // not a row item
	}
	rec := &storage.StreamRecord{Table: t, EventName: ev, RowID: rid.Value}
	if mv, ok := img["_mv"].(*ddbtypes.AttributeValueMemberBOOL); ok && mv.Value {
		rec.IsMVOutput = true
	}
	if v, ok := img["v"].(*ddbtypes.AttributeValueMemberS); ok {
		rec.Row = storage.Row{v.Value}
	}
	return rec, nil
}

type applyCall struct {
	table string
	rows  []storage.Row
	ids   []string
}

type fakeApplier struct {
	mu    sync.Mutex
	calls []applyCall
	err   error
}

func (a *fakeApplier) ApplyViews(_ context.Context, t *catalog.Table, rows []storage.Row, ids []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.calls = append(a.calls, applyCall{t.FullName(), rows, ids})
	return nil
}

func (a *fakeApplier) rowIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, c := range a.calls {
		out = append(out, c.ids...)
	}
	return out
}

func newTestProcessor() (*processor, *fakeCatalog, *fakeApplier) {
	cat := &fakeCatalog{
		tables: []*catalog.Table{
			{Database: "db", Name: "events", PhysicalName: "p_db.events"},
			{Database: "db", Name: "rollup", PhysicalName: "p_db.rollup"},
		},
		views: []*catalog.View{{Database: "db", Name: "mv", Materialized: true, SourceDatabase: "db", SourceTable: "events", ToDatabase: "db", ToTable: "rollup"}},
	}
	app := &fakeApplier{}
	p := &processor{cat: cat, dec: fakeDecoder{}, app: app, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		policy: PoisonDeadLetter, poisonAfter: defaultPoisonAfter}
	return p, cat, app
}

func img(rid, v string, extra ...string) map[string]ddbtypes.AttributeValue {
	m := map[string]ddbtypes.AttributeValue{
		"_rid": &ddbtypes.AttributeValueMemberS{Value: rid},
		"v":    &ddbtypes.AttributeValueMemberS{Value: v},
	}
	for _, e := range extra {
		switch e {
		case "mv":
			m["_mv"] = &ddbtypes.AttributeValueMemberBOOL{Value: true}
		case "bad":
			m["bad"] = &ddbtypes.AttributeValueMemberNULL{Value: true}
		}
	}
	return m
}

func TestProcessFiltersAndApplies(t *testing.T) {
	p, _, app := newTestProcessor()
	changes := []change{
		{EventName: eventInsert, NewImage: img("r1", "a"), Created: time.Now().Add(-time.Second)},
		{EventName: eventModify, NewImage: img("r2", "b")},
		{EventName: eventRemove},
		{EventName: eventInsert, NewImage: img("r3", "c", "mv")},                         // MV output: no chaining
		{EventName: eventInsert, NewImage: map[string]ddbtypes.AttributeValue{"x": nil}}, // side item
	}
	if err := p.process(context.Background(), "p_db.events", changes); err != nil {
		t.Fatal(err)
	}
	if len(app.calls) != 1 {
		t.Fatalf("calls = %d", len(app.calls))
	}
	c := app.calls[0]
	if c.table != "db.events" || !reflect.DeepEqual(c.ids, []string{"r1", "r2"}) ||
		!reflect.DeepEqual(c.rows, []storage.Row{{"a"}, {"b"}}) {
		t.Fatalf("call = %+v", c)
	}
	if p.applied.Load() != 2 || p.skipped.Load() != 3 || p.errs.Load() != 0 {
		t.Fatalf("counters applied=%d skipped=%d errs=%d", p.applied.Load(), p.skipped.Load(), p.errs.Load())
	}
	if lag := p.lagMillis.Load(); lag < 900 {
		t.Fatalf("lag = %dms", lag)
	}
}

func TestProcessOnlyRemovesMakesNoCall(t *testing.T) {
	p, _, app := newTestProcessor()
	if err := p.process(context.Background(), "p_db.events", []change{{EventName: eventRemove}}); err != nil {
		t.Fatal(err)
	}
	if len(app.calls) != 0 {
		t.Fatal("ApplyViews called without rows")
	}
}

func TestProcessChunksLargeBatches(t *testing.T) {
	p, _, app := newTestProcessor()
	var changes []change
	for i := range applyChunk*2 + 5 {
		changes = append(changes, change{EventName: eventInsert, NewImage: img(string(rune('a'+i%26))+string(rune(i)), "v")})
	}
	if err := p.process(context.Background(), "p_db.events", changes); err != nil {
		t.Fatal(err)
	}
	if len(app.calls) != 3 || len(app.calls[2].ids) != 5 {
		t.Fatalf("chunks: %d", len(app.calls))
	}
}

func TestProcessErrorsAreReturned(t *testing.T) {
	t.Run("decode", func(t *testing.T) {
		p, _, app := newTestProcessor()
		err := p.process(context.Background(), "p_db.events", []change{
			{EventName: eventInsert, NewImage: img("r1", "a")},
			{EventName: eventInsert, NewImage: img("r2", "b", "bad"), Seq: "42"},
		})
		if err == nil || len(app.calls) != 0 || p.errs.Load() != 1 {
			t.Fatalf("err=%v calls=%d", err, len(app.calls))
		}
	})
	t.Run("apply", func(t *testing.T) {
		p, _, app := newTestProcessor()
		app.err = errors.New("throttled")
		err := p.process(context.Background(), "p_db.events", []change{{EventName: eventInsert, NewImage: img("r1", "a")}})
		if err == nil || p.applied.Load() != 0 {
			t.Fatalf("err=%v applied=%d", err, p.applied.Load())
		}
	})
}

func TestProcessUnknownTable(t *testing.T) {
	p, cat, app := newTestProcessor()
	// Not in the snapshot: refreshed once, then skipped.
	if err := p.process(context.Background(), "p_db.gone", []change{{EventName: eventInsert, NewImage: img("r", "v")}}); err != nil {
		t.Fatal(err)
	}
	if cat.refreshes != 1 || len(app.calls) != 0 || p.skipped.Load() != 1 {
		t.Fatalf("refreshes=%d calls=%d skipped=%d", cat.refreshes, len(app.calls), p.skipped.Load())
	}
	// Appears after a refresh (created moments ago elsewhere).
	cat.onRefresh = func(c *fakeCatalog) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.tables) == 2 {
			c.tables = append(c.tables, &catalog.Table{Database: "db", Name: "new", PhysicalName: "p_db.new"})
			c.views = append(c.views, &catalog.View{Database: "db", Name: "mv_new", Materialized: true, SourceDatabase: "db", SourceTable: "new"})
		}
	}
	if err := p.process(context.Background(), "p_db.new", []change{{EventName: eventInsert, NewImage: img("r", "v")}}); err != nil {
		t.Fatal(err)
	}
	if len(app.calls) != 1 || app.calls[0].table != "db.new" {
		t.Fatalf("calls = %+v", app.calls)
	}
}

func TestSourceTables(t *testing.T) {
	_, cat, _ := newTestProcessor()
	cat.tables = append(cat.tables, &catalog.Table{Database: "default", Name: "src", PhysicalName: "p_default.src"},
		&catalog.Table{Database: "default", Name: "roll", Engine: catalog.EngineSummingMergeTree, PhysicalName: "p_default.roll"})
	cat.views = append(cat.views,
		&catalog.View{Name: "mv2", Materialized: true, SourceDatabase: "default", SourceTable: "src"},
		&catalog.View{Name: "mv3", Materialized: true, SourceDatabase: "db", SourceTable: "events"},
		// Legacy view on a merging source: ignored, no stream reader.
		&catalog.View{Name: "mv4", Materialized: true, SourceDatabase: "default", SourceTable: "roll"})
	got := sourceTables(cat)
	if want := []string{"p_db.events", "p_default.src"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestChangesFromStream(t *testing.T) {
	now := time.Now()
	recs := []sttypes.Record{
		{EventName: sttypes.OperationTypeInsert, Dynamodb: &sttypes.StreamRecord{
			SequenceNumber: aws.String("1"), ApproximateCreationDateTime: &now,
			NewImage: map[string]sttypes.AttributeValue{"v": &sttypes.AttributeValueMemberS{Value: "x"}},
		}},
		{EventName: sttypes.OperationTypeRemove, Dynamodb: &sttypes.StreamRecord{
			SequenceNumber: aws.String("2"),
			OldImage:       map[string]sttypes.AttributeValue{"v": &sttypes.AttributeValueMemberS{Value: "x"}},
		}},
	}
	got, err := changesFromStream(recs)
	if err != nil {
		t.Fatal(err)
	}
	want := []change{
		{EventName: "INSERT", Seq: "1", Created: now, NewImage: map[string]ddbtypes.AttributeValue{"v": &ddbtypes.AttributeValueMemberS{Value: "x"}}},
		{EventName: "REMOVE", Seq: "2"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	bad := []sttypes.Record{{EventName: sttypes.OperationTypeInsert, Dynamodb: &sttypes.StreamRecord{
		NewImage: map[string]sttypes.AttributeValue{"v": &sttypes.UnknownUnionMember{Tag: "?"}},
	}}}
	if _, err := changesFromStream(bad); err == nil {
		t.Fatal("expected conversion error")
	}
}

func TestFromLambda(t *testing.T) {
	in := map[string]events.DynamoDBAttributeValue{
		"s":    events.NewStringAttribute(""),
		"n":    events.NewNumberAttribute("12.5"),
		"b":    events.NewBinaryAttribute([]byte{0, 1}),
		"bool": events.NewBooleanAttribute(false),
		"null": events.NewNullAttribute(),
		"ss":   events.NewStringSetAttribute([]string{"a"}),
		"ns":   events.NewNumberSetAttribute([]string{"1", "2"}),
		"bs":   events.NewBinarySetAttribute([][]byte{{9}}),
		"l":    events.NewListAttribute([]events.DynamoDBAttributeValue{events.NewNumberAttribute("1")}),
		"m":    events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{"k": events.NewStringAttribute("v")}),
		"em":   events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{}),
	}
	want := map[string]ddbtypes.AttributeValue{
		"s":    &ddbtypes.AttributeValueMemberS{Value: ""},
		"n":    &ddbtypes.AttributeValueMemberN{Value: "12.5"},
		"b":    &ddbtypes.AttributeValueMemberB{Value: []byte{0, 1}},
		"bool": &ddbtypes.AttributeValueMemberBOOL{Value: false},
		"null": &ddbtypes.AttributeValueMemberNULL{Value: true},
		"ss":   &ddbtypes.AttributeValueMemberSS{Value: []string{"a"}},
		"ns":   &ddbtypes.AttributeValueMemberNS{Value: []string{"1", "2"}},
		"bs":   &ddbtypes.AttributeValueMemberBS{Value: [][]byte{{9}}},
		"l":    &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{&ddbtypes.AttributeValueMemberN{Value: "1"}}},
		"m":    &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{"k": &ddbtypes.AttributeValueMemberS{Value: "v"}}},
		"em":   &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{}},
	}
	got, err := FromLambdaItem(in)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}
}

func TestTableFromStreamARN(t *testing.T) {
	cases := []struct {
		arn, table string
		ok         bool
	}{
		{"arn:aws:dynamodb:us-east-1:123456789012:table/twinhouse_db.events/stream/2024-01-01T00:00:00.000", "twinhouse_db.events", true},
		{"arn:aws:dynamodb:ddblocal:000000000000:table/t/stream/x", "t", true},
		{"arn:aws:dynamodb:us-east-1:1:table/t", "", false},
		{"arn:aws:dynamodb:us-east-1:1:table//stream/x", "", false},
		{"garbage", "", false},
	}
	for _, c := range cases {
		got, ok := TableFromStreamARN(c.arn)
		if got != c.table || ok != c.ok {
			t.Errorf("%s: got %q %v", c.arn, got, ok)
		}
	}
}

func lambdaRecord(table, seq, ev string, image map[string]events.DynamoDBAttributeValue) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventName:      ev,
		EventSourceArn: "arn:aws:dynamodb:us-east-1:1:table/" + table + "/stream/l",
		Change:         events.DynamoDBStreamRecord{SequenceNumber: seq, NewImage: image},
	}
}

func lambdaImg(rid string, extra ...string) map[string]events.DynamoDBAttributeValue {
	m := map[string]events.DynamoDBAttributeValue{
		"_rid": events.NewStringAttribute(rid),
		"v":    events.NewStringAttribute("v-" + rid),
	}
	for _, e := range extra {
		if e == "bad" {
			m["bad"] = events.NewNullAttribute()
		}
	}
	return m
}

func TestLambdaHandler(t *testing.T) {
	ctx := context.Background()
	t.Run("success", func(t *testing.T) {
		p, _, app := newTestProcessor()
		h := newLambdaHandler(p)
		resp, err := h(ctx, events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
			lambdaRecord("p_db.events", "1", "INSERT", lambdaImg("a")),
			lambdaRecord("p_db.events", "2", "REMOVE", nil),
			lambdaRecord("p_db.events", "3", "MODIFY", lambdaImg("b")),
		}})
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("resp=%+v err=%v", resp, err)
		}
		if got := app.rowIDs(); !reflect.DeepEqual(got, []string{"a", "b"}) {
			t.Fatalf("applied %v", got)
		}
	})
	t.Run("decode failure reports first record of the failed group", func(t *testing.T) {
		p, _, _ := newTestProcessor()
		h := newLambdaHandler(p)
		resp, err := h(ctx, events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
			lambdaRecord("p_db.events", "10", "INSERT", lambdaImg("a")),
			lambdaRecord("p_db.events", "11", "INSERT", lambdaImg("b", "bad")),
		}})
		if err != nil {
			t.Fatal(err)
		}
		if want := []events.DynamoDBBatchItemFailure{{ItemIdentifier: "10"}}; !reflect.DeepEqual(resp.BatchItemFailures, want) {
			t.Fatalf("failures = %+v", resp.BatchItemFailures)
		}
	})
	t.Run("apply failure after an applied group", func(t *testing.T) {
		p, cat, app := newTestProcessor()
		cat.tables = append(cat.tables, &catalog.Table{Database: "db", Name: "other", PhysicalName: "p_db.other"})
		cat.views = append(cat.views, &catalog.View{Database: "db", Name: "mv_other", Materialized: true, SourceDatabase: "db", SourceTable: "other"})
		h := newLambdaHandler(p)
		failing := &failSecond{inner: app}
		p.app = failing
		resp, err := h(ctx, events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
			lambdaRecord("p_db.events", "20", "INSERT", lambdaImg("a")),
			lambdaRecord("p_db.other", "21", "INSERT", lambdaImg("b")),
			lambdaRecord("p_db.other", "22", "INSERT", lambdaImg("c")),
		}})
		if err != nil {
			t.Fatal(err)
		}
		if want := []events.DynamoDBBatchItemFailure{{ItemIdentifier: "21"}}; !reflect.DeepEqual(resp.BatchItemFailures, want) {
			t.Fatalf("failures = %+v", resp.BatchItemFailures)
		}
		if got := app.rowIDs(); !reflect.DeepEqual(got, []string{"a"}) {
			t.Fatalf("applied %v", got)
		}
	})
	t.Run("bad ARN", func(t *testing.T) {
		p, _, _ := newTestProcessor()
		rec := lambdaRecord("x", "30", "INSERT", lambdaImg("a"))
		rec.EventSourceArn = "nope"
		resp, err := newLambdaHandler(p)(ctx, events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{rec}})
		if err != nil || len(resp.BatchItemFailures) != 1 || resp.BatchItemFailures[0].ItemIdentifier != "30" {
			t.Fatalf("resp=%+v err=%v", resp, err)
		}
	})
	t.Run("empty event", func(t *testing.T) {
		p, _, _ := newTestProcessor()
		resp, err := newLambdaHandler(p)(ctx, events.DynamoDBEvent{})
		if err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("resp=%+v err=%v", resp, err)
		}
	})
}

// failSecond fails every ApplyViews call after the first.
type failSecond struct {
	inner *fakeApplier
	n     int
}

func (f *failSecond) ApplyViews(ctx context.Context, t *catalog.Table, rows []storage.Row, ids []string) error {
	f.n++
	if f.n > 1 {
		return errors.New("boom")
	}
	return f.inner.ApplyViews(ctx, t, rows, ids)
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for empty config")
	}
	if _, err := NewLambdaHandler(LambdaConfig{}); err == nil {
		t.Fatal("expected error for empty lambda config")
	}
	if DefaultOwner() == "" {
		t.Fatal("empty owner")
	}
}

func TestWriteTime(t *testing.T) {
	def := time.Unix(100, 0)
	cases := []struct {
		name string
		img  map[string]ddbtypes.AttributeValue
		want time.Time
	}{
		{"ts", map[string]ddbtypes.AttributeValue{"_ts": &ddbtypes.AttributeValueMemberN{Value: "1700000000123456789"}}, time.Unix(0, 1700000000123456789)},
		{"missing", map[string]ddbtypes.AttributeValue{}, def},
		{"not a number", map[string]ddbtypes.AttributeValue{"_ts": &ddbtypes.AttributeValueMemberN{Value: "x"}}, def},
		{"wrong type", map[string]ddbtypes.AttributeValue{"_ts": &ddbtypes.AttributeValueMemberS{Value: "1"}}, def},
	}
	for _, c := range cases {
		if got := writeTime(c.img, def); !got.Equal(c.want) {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// listApplier also implements viewListApplier.
type listApplier struct {
	fakeApplier
	lists [][]string // view names per ApplyViewList call
	ids   [][]string
}

func (a *listApplier) ApplyViewList(_ context.Context, t *catalog.Table, views []*catalog.View, rows []storage.Row, ids []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var names []string
	for _, v := range views {
		names = append(names, v.Name)
	}
	a.lists = append(a.lists, names)
	a.ids = append(a.ids, ids)
	return nil
}

func at(rid string, wt time.Time) change {
	return change{EventName: eventInsert, NewImage: img(rid, "v"), WriteTime: wt, Created: wt}
}

// A materialized view only sees rows inserted after it was created, like
// ClickHouse: stream records of older rows (replayed from TRIM_HORIZON when
// the table gets its first view) are not applied to it.
func TestProcessSkipsRowsWrittenBeforeViewCreation(t *testing.T) {
	p, cat, app := newTestProcessor()
	created := time.Now().Add(-time.Minute)
	cat.views[0].CreatedAt = created
	p.markLoaded(time.Now()) // snapshot is fresh
	err := p.process(context.Background(), "p_db.events", []change{
		at("old1", created.Add(-time.Hour)),
		at("old2", created.Add(-time.Nanosecond)),
		at("new1", created),
		at("new2", created.Add(time.Second)),
		{EventName: eventInsert, NewImage: img("unknown-time", "v")}, // no _ts: applied
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := app.rowIDs(); !reflect.DeepEqual(got, []string{"new1", "new2", "unknown-time"}) {
		t.Fatalf("applied %v", got)
	}
	if p.applied.Load() != 3 || p.skipped.Load() != 2 {
		t.Fatalf("applied=%d skipped=%d", p.applied.Load(), p.skipped.Load())
	}
}

// With several views of different ages, each row goes only to the views
// that existed when it was written, in stream order.
func TestProcessAppliesEachRowToViewsOlderThanIt(t *testing.T) {
	p, cat, _ := newTestProcessor()
	app := &listApplier{}
	p.app = app
	t1 := time.Now().Add(-time.Hour)
	t2 := t1.Add(30 * time.Minute)
	cat.views[0].CreatedAt = t1
	cat.views = append(cat.views, &catalog.View{Database: "db", Name: "mv2", Materialized: true,
		SourceDatabase: "db", SourceTable: "events", ToDatabase: "db", ToTable: "rollup", CreatedAt: t2})
	p.markLoaded(time.Now())
	err := p.process(context.Background(), "p_db.events", []change{
		at("before-both", t1.Add(-time.Second)),
		at("mv-only-1", t1.Add(time.Second)),
		at("mv-only-2", t1.Add(2*time.Second)),
		at("both-1", t2.Add(time.Second)),
		at("both-2", t2.Add(2*time.Second)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(app.lists, [][]string{{"mv"}}) || !reflect.DeepEqual(app.ids, [][]string{{"mv-only-1", "mv-only-2"}}) {
		t.Fatalf("ApplyViewList calls: %v %v", app.lists, app.ids)
	}
	if got := app.rowIDs(); !reflect.DeepEqual(got, []string{"both-1", "both-2"}) {
		t.Fatalf("ApplyViews rows %v", got)
	}
	if p.applied.Load() != 4 || p.skipped.Load() != 1 {
		t.Fatalf("applied=%d skipped=%d", p.applied.Load(), p.skipped.Load())
	}
}

// Records written after the catalog snapshot was loaded make the processor
// reload it first, so a view dropped (or created) just before the write is
// honoured even though the periodic refresh has not run yet.
func TestProcessRefreshesCatalogForRecordsNewerThanSnapshot(t *testing.T) {
	p, cat, app := newTestProcessor()
	cat.views[0].CreatedAt = time.Now().Add(-time.Hour)
	loaded := time.Now().Add(-10 * time.Second)
	p.markLoaded(loaded)

	// Older than the snapshot: no refresh needed.
	if err := p.process(context.Background(), "p_db.events", []change{at("r1", loaded.Add(-time.Second))}); err != nil {
		t.Fatal(err)
	}
	if cat.refreshes != 0 || len(app.rowIDs()) != 1 {
		t.Fatalf("refreshes=%d applied=%v", cat.refreshes, app.rowIDs())
	}

	// DROP VIEW happened, then a row was written: the stale snapshot still
	// lists the view, the refresh removes it.
	cat.onRefresh = func(c *fakeCatalog) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.views = nil
	}
	if err := p.process(context.Background(), "p_db.events", []change{at("after-drop", time.Now())}); err != nil {
		t.Fatal(err)
	}
	if cat.refreshes != 1 {
		t.Fatalf("refreshes = %d", cat.refreshes)
	}
	if got := app.rowIDs(); !reflect.DeepEqual(got, []string{"r1"}) {
		t.Fatalf("dropped view applied: %v", got)
	}

	// A batch older than the refresh does not refresh again.
	if err := p.process(context.Background(), "p_db.events", []change{at("x", time.Now().Add(-time.Second))}); err != nil {
		t.Fatal(err)
	}
	if cat.refreshes != 1 {
		t.Fatalf("refreshes = %d", cat.refreshes)
	}
}

func TestProcessRefreshFailureRetriesBatch(t *testing.T) {
	p, _, app := newTestProcessor()
	p.markLoaded(time.Now().Add(-time.Minute))
	p.cat = &failingRefresh{p.cat}
	err := p.process(context.Background(), "p_db.events", []change{at("r", time.Now())})
	if err == nil || len(app.calls) != 0 {
		t.Fatalf("err=%v calls=%d", err, len(app.calls))
	}
}

type failingRefresh struct{ catalog.Catalog }

func (failingRefresh) Refresh(context.Context) error { return errors.New("catalog unavailable") }

// Records of a merging view source (whose views the engine never applies)
// are counted as skipped, without calling the engine.
func TestProcessSkipsMergingSource(t *testing.T) {
	p, cat, app := newTestProcessor()
	cat.mu.Lock()
	cat.tables[0] = &catalog.Table{Database: "db", Name: "events", PhysicalName: "p_db.events", Engine: catalog.EngineSummingMergeTree}
	cat.mu.Unlock()
	for range 2 {
		if err := p.process(context.Background(), "p_db.events", []change{{EventName: eventInsert, NewImage: img("r1", "a")}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(app.calls) != 0 || p.skipped.Load() != 2 || p.applied.Load() != 0 {
		t.Fatalf("calls=%d skipped=%d applied=%d", len(app.calls), p.skipped.Load(), p.applied.Load())
	}
}

// Records of a table, or of a source whose view, has an undecodable
// (quarantined) catalog entry are retried as transient, never skipped.
func TestProcessWaitsForQuarantinedEntries(t *testing.T) {
	p, cat, app := newTestProcessor()
	cat.mu.Lock()
	cat.quarant = []catalog.Quarantined{{Key: "table#db.newer", Kind: catalog.KindTable, PhysicalName: "p_db.newer", Err: errors.New("unknown type")}}
	cat.mu.Unlock()
	err := p.process(context.Background(), "p_db.newer", []change{{EventName: eventInsert, NewImage: img("r", "v")}})
	if !cherr.Is(err, cherr.Unavailable) || !isTransient(err, true) || p.skipped.Load() != 0 {
		t.Fatalf("err=%v skipped=%d", err, p.skipped.Load())
	}
	// A view of events that cannot be decoded: events' records wait too.
	cat.mu.Lock()
	cat.quarant = []catalog.Quarantined{{Key: "view#db.mv2", Kind: catalog.KindView, SourceDatabase: "db", SourceTable: "events", Err: errors.New("bad")}}
	cat.mu.Unlock()
	err = p.process(context.Background(), "p_db.events", []change{{EventName: eventInsert, NewImage: img("r", "v")}})
	if !cherr.Is(err, cherr.Unavailable) || len(app.calls) != 0 {
		t.Fatalf("err=%v calls=%d", err, len(app.calls))
	}
	// A quarantined view of another table does not block events.
	cat.mu.Lock()
	cat.quarant[0].SourceTable = "other"
	cat.mu.Unlock()
	if err := p.process(context.Background(), "p_db.events", []change{{EventName: eventInsert, NewImage: img("r", "v")}}); err != nil || len(app.calls) != 1 {
		t.Fatalf("err=%v calls=%d", err, len(app.calls))
	}
}
