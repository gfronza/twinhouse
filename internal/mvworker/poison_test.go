package mvworker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/storage"
)

func TestIsTransient(t *testing.T) {
	apiErr := func(code string) error { return &smithy.GenericAPIError{Code: code, Message: "x"} }
	// tce builds the TransactionCanceledException TransactWriteItems
	// returns (mergeOne: marker Put + row Update; addMembers: member Puts).
	tce := func(codes ...string) error {
		e := &ddbtypes.TransactionCanceledException{Message: aws.String("Transaction cancelled")}
		for _, c := range codes {
			r := ddbtypes.CancellationReason{Code: aws.String(c)}
			if c == "ValidationError" {
				r.Message = aws.String("Item size to update has exceeded the maximum allowed size")
			}
			e.CancellationReasons = append(e.CancellationReasons, r)
		}
		return e
	}
	sdkWrap := func(err error) error {
		return &smithy.OperationError{ServiceID: "DynamoDB", OperationName: "TransactWriteItems", Err: err}
	}
	cases := []struct {
		name      string
		err       error
		pure      bool
		transient bool
	}{
		{"cannot parse", cherr.New(cherr.CannotParseText, "Cannot parse string 'oops' as UInt64"), false, false},
		{"type mismatch", cherr.New(cherr.TypeMismatch, "x"), false, false},
		{"dropped target", cherr.New(cherr.UnknownTable, "materialized view db.mv: target table db.t is not available"), false, false},
		{"wrapped cannot parse", fmt.Errorf("apply views: %w", cherr.New(cherr.CannotParseText, "x")), false, false},
		{"item too large", cherr.Wrap(cherr.BadArguments, apiErr("ValidationException"), "merge"), false, false},
		{"tx item too large", cherr.Wrap(cherr.StdException, sdkWrap(tce("None", "ValidationError")), "merge"), false, false},
		{"tx item too large (bad arguments)", cherr.Wrap(cherr.BadArguments, sdkWrap(tce("None", "ValidationError")), "merge"), false, false},
		{"tx item too large, bare", tce("None", "ValidationError"), false, false},
		{"tx validation with conflict", cherr.Wrap(cherr.StdException, sdkWrap(tce("ValidationError", "TransactionConflict")), "merge"), false, true},
		{"tx validation with throttling", sdkWrap(tce("ValidationError", "ThrottlingError")), false, true},
		{"tx conflict", cherr.Wrap(cherr.Unavailable, sdkWrap(tce("None", "TransactionConflict")), "merge"), false, true},
		{"tx condition race", sdkWrap(tce("ConditionalCheckFailed", "None")), false, true},
		{"logical error", cherr.New(cherr.LogicalError, "panic"), false, false},
		{"unavailable", cherr.New(cherr.Unavailable, "too much contention"), false, true},
		{"timeout", cherr.New(cherr.Timeout, "x"), false, true},
		{"throttled", cherr.Wrap(cherr.Unavailable, apiErr("ProvisionedThroughputExceededException"), "merge"), false, true},
		{"table being created", cherr.Wrap(cherr.UnknownTable, apiErr("ResourceNotFoundException"), "merge"), false, true},
		{"flattened table being created", cherr.New(cherr.UnknownTable, "merge into t: ResourceNotFoundException: Requested resource not found"), false, true},
		{"condition race", apiErr("ConditionalCheckFailedException"), false, true},
		{"network", cherr.Wrap(cherr.StdException, &net.OpError{Op: "dial", Err: errors.New("connection refused")}, "merge"), false, true},
		{"operation error", &smithy.OperationError{ServiceID: "DynamoDB", OperationName: "Query", Err: errors.New("EOF")}, false, true},
		{"canceled", fmt.Errorf("x: %w", context.Canceled), false, true},
		{"unknown error", errors.New("boom"), false, true},
		{"std exception", cherr.New(cherr.StdException, "boom"), false, true},
		{"pure unknown error", errors.New("cannot decode"), true, false},
		{"pure network", &net.OpError{Op: "dial", Err: errors.New("refused")}, true, true},
	}
	for _, c := range cases {
		if got := isTransient(c.err, c.pure); got != c.transient {
			t.Errorf("%s: isTransient = %v, want %v", c.name, got, c.transient)
		}
	}
}

// parseApplier mimics the engine: a call fails as a whole (before merging
// anything) if any of its rows is "oops"; applied rows are recorded.
type parseApplier struct {
	mu      sync.Mutex
	applied []string
	calls   int
	failErr error // returned for "oops" rows (default CANNOT_PARSE_TEXT)
}

func (a *parseApplier) ApplyViews(_ context.Context, _ *catalog.Table, rows []storage.Row, ids []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	for _, r := range rows {
		if r[0] == "oops" {
			if a.failErr != nil {
				return a.failErr
			}
			return cherr.New(cherr.CannotParseText, "Cannot parse string 'oops' as UInt64")
		}
	}
	a.applied = append(a.applied, ids...)
	return nil
}

func (a *parseApplier) ids() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.applied)
}

type memDLQ struct {
	mu    sync.Mutex
	items []deadLetter
	err   error
}

func (m *memDLQ) put(_ context.Context, d deadLetter) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.items = append(m.items, d)
	return nil
}

func (m *memDLQ) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, d := range m.items {
		out = append(out, d.Seq+"/"+d.RowID+"/"+d.View)
	}
	return out
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func poisonProcessor(t *testing.T, app viewApplier, policy string) (*processor, *memDLQ, *fakeClock) {
	t.Helper()
	p, _, _ := newTestProcessor()
	p.app = app
	p.policy = policy
	p.poisonAfter = 10 * time.Second
	clk := &fakeClock{t: time.Now()}
	p.now = clk.now
	dlq := &memDLQ{}
	p.dlq = dlq
	p.origin = "stream"
	return p, dlq, clk
}

func rec(seq, rid, v string) change {
	return change{EventName: eventInsert, Seq: seq, NewImage: img(rid, v)}
}

// The reviewer's scenario: [a, b(oops), c, a2] in one batch, then [d]. The
// good rows are applied at once, the batch is retried while the error may
// still be transient, then the poison record is dead-lettered, the batch
// succeeds (so the checkpoint advances) and the next batch is applied.
func TestPoisonRecordIsIsolatedAndDeadLettered(t *testing.T) {
	app := &parseApplier{}
	p, dlq, clk := poisonProcessor(t, app, PoisonDeadLetter)
	ctx := context.Background()
	batch := []change{rec("1", "a", "5"), rec("2", "b", "oops"), rec("3", "c", "7"), rec("4", "a2", "1")}
	var scratch any

	err := p.handle(ctx, "p_db.events", batch, &scratch)
	if err == nil {
		t.Fatal("first delivery succeeded: the poison record was skipped without waiting PoisonAfter")
	}
	if !cherr.Is(err, cherr.CannotParseText) {
		t.Fatalf("error does not carry the engine error: %v", err)
	}
	if got := app.ids(); !reflect.DeepEqual(got, []string{"a", "c", "a2"}) {
		t.Fatalf("good rows applied = %v, want [a c a2]", got)
	}
	if len(dlq.keys()) != 0 {
		t.Fatal("dead-lettered before PoisonAfter")
	}

	// A retry within PoisonAfter re-applies nothing that already landed.
	clk.advance(5 * time.Second)
	if err := p.handle(ctx, "p_db.events", batch, &scratch); err == nil {
		t.Fatal("retry within PoisonAfter succeeded")
	}
	if got := app.ids(); len(got) != 3 {
		t.Fatalf("rows re-applied on retry: %v", got)
	}

	clk.advance(6 * time.Second)
	if err := p.handle(ctx, "p_db.events", batch, &scratch); err != nil {
		t.Fatalf("retry after PoisonAfter: %v", err)
	}
	if got := dlq.keys(); !reflect.DeepEqual(got, []string{"2/b/"}) {
		t.Fatalf("dead letters = %v", got)
	}
	d := dlq.items[0]
	if d.Table != "db.events" || d.Origin != "stream" || !cherr.Is(d.Err, cherr.CannotParseText) || d.Image == nil {
		t.Fatalf("dead letter %+v", d)
	}
	if p.deadLettered.Load() != 1 {
		t.Fatalf("deadLettered = %d", p.deadLettered.Load())
	}

	// The next batch on the shard flows normally.
	var next any
	if err := p.handle(ctx, "p_db.events", []change{rec("5", "d", "3")}, &next); err != nil {
		t.Fatal(err)
	}
	if got := app.ids(); !reflect.DeepEqual(got, []string{"a", "c", "a2", "d"}) {
		t.Fatalf("applied = %v", got)
	}
}

// Transient errors never dead-letter, however long they last.
func TestTransientErrorsAreRetriedForever(t *testing.T) {
	app := &parseApplier{failErr: cherr.New(cherr.Unavailable, "throttled")}
	p, dlq, clk := poisonProcessor(t, app, PoisonDeadLetter)
	batch := []change{rec("1", "a", "5"), rec("2", "b", "oops")}
	var scratch any
	for range 5 {
		if err := p.handle(context.Background(), "p_db.events", batch, &scratch); err == nil {
			t.Fatal("transient failure reported success")
		}
		clk.advance(time.Hour)
	}
	if scratch != nil || len(dlq.keys()) != 0 {
		t.Fatalf("transient error isolated or dead-lettered: scratch=%v dlq=%v", scratch, dlq.keys())
	}
}

// A dead-letter write failure fails the batch (retried); nothing is lost.
func TestDeadLetterWriteFailureRetries(t *testing.T) {
	app := &parseApplier{}
	p, dlq, clk := poisonProcessor(t, app, PoisonDeadLetter)
	dlq.err = errors.New("dlq unavailable")
	batch := []change{rec("1", "b", "oops")}
	var scratch any
	_ = p.handle(context.Background(), "p_db.events", batch, &scratch)
	clk.advance(time.Minute)
	if err := p.handle(context.Background(), "p_db.events", batch, &scratch); err == nil {
		t.Fatal("batch succeeded although its dead letter was not stored")
	}
	dlq.err = nil
	if err := p.handle(context.Background(), "p_db.events", batch, &scratch); err != nil {
		t.Fatal(err)
	}
	if len(dlq.keys()) != 1 {
		t.Fatalf("dead letters %v", dlq.keys())
	}
}

// PoisonBlock keeps the batch failing (the good rows are still applied).
func TestPoisonBlockPolicy(t *testing.T) {
	app := &parseApplier{}
	p, dlq, clk := poisonProcessor(t, app, PoisonBlock)
	batch := []change{rec("1", "a", "5"), rec("2", "b", "oops")}
	var scratch any
	for range 3 {
		if err := p.handle(context.Background(), "p_db.events", batch, &scratch); err == nil {
			t.Fatal("block policy let the batch through")
		}
		clk.advance(time.Hour)
	}
	if len(dlq.keys()) != 0 || !reflect.DeepEqual(app.ids(), []string{"a"}) {
		t.Fatalf("dlq=%v applied=%v", dlq.keys(), app.ids())
	}
}

// viewFailApplier fails ApplyViewList for one view on "oops" rows.
type viewFailApplier struct {
	parseApplier
	failView string
	perView  map[string][]string
}

func (a *viewFailApplier) ApplyViewList(_ context.Context, _ *catalog.Table, views []*catalog.View, rows []storage.Row, ids []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, v := range views {
		for _, r := range rows {
			if v.Name == a.failView && r[0] == "oops" {
				return cherr.New(cherr.CannotParseText, "view %s: cannot parse", v.Name)
			}
		}
	}
	if a.perView == nil {
		a.perView = map[string][]string{}
	}
	for _, v := range views {
		a.perView[v.Name] = append(a.perView[v.Name], ids...)
	}
	return nil
}

// With an engine that applies single views, only the failing view of the
// poison row is dead-lettered; the row's other views get it.
func TestPoisonIsolationPerView(t *testing.T) {
	app := &viewFailApplier{failView: "mv"}
	p, dlq, clk := poisonProcessor(t, app, PoisonDeadLetter)
	cat := p.cat.(*fakeCatalog)
	cat.views = append(cat.views, &catalog.View{Database: "db", Name: "mv2", Materialized: true,
		SourceDatabase: "db", SourceTable: "events", ToDatabase: "db", ToTable: "rollup"})
	// ApplyViews (the chunked first attempt) fails as in the engine.
	batch := []change{rec("1", "a", "5"), rec("2", "b", "oops")}
	var scratch any
	_ = p.handle(context.Background(), "p_db.events", batch, &scratch)
	clk.advance(time.Minute)
	if err := p.handle(context.Background(), "p_db.events", batch, &scratch); err != nil {
		t.Fatal(err)
	}
	if want := map[string][]string{"mv": {"a"}, "mv2": {"a", "b"}}; !reflect.DeepEqual(app.perView, want) {
		t.Fatalf("per view = %v, want %v", app.perView, want)
	}
	if got := dlq.keys(); !reflect.DeepEqual(got, []string{"2/b/db.mv"}) {
		t.Fatalf("dead letters %v", got)
	}
}

// An undecodable image is poison too.
func TestUndecodableRecordIsDeadLettered(t *testing.T) {
	app := &parseApplier{}
	p, dlq, clk := poisonProcessor(t, app, PoisonDeadLetter)
	batch := []change{rec("1", "a", "5"), {EventName: eventInsert, Seq: "2", NewImage: img("x", "v", "bad")}}
	var scratch any
	if err := p.handle(context.Background(), "p_db.events", batch, &scratch); err == nil {
		t.Fatal("expected failure within PoisonAfter")
	}
	clk.advance(time.Minute)
	if err := p.handle(context.Background(), "p_db.events", batch, &scratch); err != nil {
		t.Fatal(err)
	}
	if got := dlq.keys(); !reflect.DeepEqual(got, []string{"2//"}) || !reflect.DeepEqual(app.ids(), []string{"a"}) {
		t.Fatalf("dlq=%v applied=%v", got, app.ids())
	}
}

// The Lambda handler isolates the poison record too: it reports the group
// until PoisonAfter, then dead-letters it and reports success.
func TestLambdaPoisonIsDeadLettered(t *testing.T) {
	app := &parseApplier{}
	p, dlq, clk := poisonProcessor(t, app, PoisonDeadLetter)
	p.origin = "lambda"
	h := newLambdaHandler(p)
	bad := lambdaImg("b")
	bad["v"] = events.NewStringAttribute("oops")
	ev := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		lambdaRecord("p_db.events", "1", "INSERT", lambdaImg("a")),
		lambdaRecord("p_db.events", "2", "INSERT", bad),
		lambdaRecord("p_db.events", "3", "INSERT", lambdaImg("c")),
	}}
	resp, err := h(context.Background(), ev)
	if err != nil || len(resp.BatchItemFailures) != 1 {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
	if got := app.ids(); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Fatalf("applied %v", got)
	}
	clk.advance(time.Minute)
	resp, err = h(context.Background(), ev)
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("after PoisonAfter: resp=%+v err=%v", resp, err)
	}
	if got := dlq.keys(); !reflect.DeepEqual(got, []string{"2/b/"}) || dlq.items[0].Origin != "lambda" {
		t.Fatalf("dead letters %v", got)
	}
	if got := app.ids(); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Fatalf("re-applied: %v", got)
	}
}

func TestResolvePoison(t *testing.T) {
	t.Setenv("MV_POISON_POLICY", "")
	t.Setenv("MV_POISON_AFTER", "")
	t.Setenv("CATALOG_MIN_RELOAD", "")
	pol, after, mr, err := resolvePoison("", 0, 0)
	if err != nil || pol != PoisonDeadLetter || after != defaultPoisonAfter || mr != defaultCatalogMinReload {
		t.Fatalf("defaults: %q %v %v %v", pol, after, mr, err)
	}
	t.Setenv("MV_POISON_POLICY", "block")
	t.Setenv("MV_POISON_AFTER", "2s")
	t.Setenv("CATALOG_MIN_RELOAD", "250ms")
	pol, after, mr, err = resolvePoison("", 0, 0)
	if err != nil || pol != PoisonBlock || after != 2*time.Second || mr != 250*time.Millisecond {
		t.Fatalf("env: %q %v %v %v", pol, after, mr, err)
	}
	if pol, _, _, _ := resolvePoison(PoisonDeadLetter, time.Second, time.Second); pol != PoisonDeadLetter {
		t.Fatal("explicit config must win over the environment")
	}
	t.Setenv("MV_POISON_POLICY", "skip")
	if _, _, _, err := resolvePoison("", 0, 0); err == nil {
		t.Fatal("invalid policy accepted")
	}
}

func TestDeadLetterTableName(t *testing.T) {
	if got := DeadLetterTableName("chddb___checkpoints"); got != "chddb___dead_letters" {
		t.Fatal(got)
	}
	if got := DeadLetterTableName("p__checkpoints"); got != "p__dead_letters" {
		t.Fatal(got)
	}
}

// Under steady writes every batch is newer than the last reload; reloads
// (full consistent catalog Scans) must still be spaced minReload apart, and
// concurrent batches must share them, while each batch is still applied
// with a catalog loaded after its newest write.
func TestCatalogReloadsAreCoalesced(t *testing.T) {
	p, cat, app := newTestProcessor()
	p.minReload = 40 * time.Millisecond
	cat.views[0].CreatedAt = time.Now().Add(-time.Hour)
	var mu sync.Mutex
	var reloadStarts []time.Time
	cat.onRefresh = func(*fakeCatalog) {
		mu.Lock()
		reloadStarts = append(reloadStarts, time.Now())
		mu.Unlock()
	}
	start := time.Now()
	var wg sync.WaitGroup
	for g := range 4 { // four shards
		wg.Go(func() {
			for i := range 10 {
				written := time.Now()
				if err := p.process(context.Background(), "p_db.events", []change{at(fmt.Sprintf("g%d-%d", g, i), written)}); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				last := reloadStarts[len(reloadStarts)-1]
				mu.Unlock()
				if last.Before(written) {
					t.Errorf("batch written at %v applied with a catalog loaded at %v", written, last)
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	n := cat.refreshes
	if limit := int(elapsed/p.minReload) + 2; n > limit {
		t.Fatalf("%d catalog reloads for 40 batches in %v (limit %d)", n, elapsed, limit)
	}
	if len(app.rowIDs()) != 40 {
		t.Fatalf("applied %d rows", len(app.rowIDs()))
	}
	if p.reloads.Load() != uint64(n) {
		t.Fatalf("reload counter %d != %d", p.reloads.Load(), n)
	}
}

// ImageApplier feeds single images (superseded replicated versions) to the
// views with the stream worker's rules: skipped without views, eligibility
// by write time, idempotent retries, poison handling.
func TestImageApplier(t *testing.T) {
	p, cat, app := newTestProcessor()
	a := &ImageApplier{p: p}
	ctx := context.Background()
	created := time.Now().Add(-time.Minute)
	cat.views[0].CreatedAt = created
	p.markLoaded(time.Now())

	// A table without views: nothing is applied.
	if err := a.Apply(ctx, "p_db.rollup", "us:1", img("x", "v")); err != nil {
		t.Fatal(err)
	}
	if len(app.rowIDs()) != 0 {
		t.Fatalf("applied=%v", app.rowIDs())
	}
	withTS := func(rid string, ts time.Time) map[string]ddbtypes.AttributeValue {
		m := img(rid, "v")
		m["_ts"] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(ts.UnixNano(), 10)}
		return m
	}
	if err := a.Apply(ctx, "p_db.events", "us:2", withTS("old", created.Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(ctx, "p_db.events", "us:3", withTS("new", created.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if got := app.rowIDs(); !reflect.DeepEqual(got, []string{"new"}) {
		t.Fatalf("applied %v (a row written before the view must not reach it)", got)
	}
	mvOut := withTS("mv-out", created.Add(time.Second))
	mvOut["_mv"] = &ddbtypes.AttributeValueMemberBOOL{Value: true}
	if err := a.Apply(ctx, "p_db.events", "us:4", mvOut); err != nil {
		t.Fatal(err)
	}
	if got := app.rowIDs(); !reflect.DeepEqual(got, []string{"new"}) {
		t.Fatalf("MV output applied: %v", got)
	}

	// A view created after the snapshot was loaded, before the row was
	// written: the catalog is reloaded and the view applied.
	cat.onRefresh = func(c *fakeCatalog) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.views) == 1 {
			c.views = append(c.views, &catalog.View{Database: "db", Name: "mv_r", Materialized: true,
				SourceDatabase: "db", SourceTable: "rollup", ToDatabase: "db", ToTable: "events", CreatedAt: time.Now()})
		}
	}
	if err := a.Apply(ctx, "p_db.rollup", "us:6", withTS("fresh", time.Now().Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if got := app.rowIDs(); !reflect.DeepEqual(got, []string{"new", "fresh"}) {
		t.Fatalf("view created just before the write not applied: %v", got)
	}

	// Poison: retried until PoisonAfter, then dead-lettered.
	pa := &parseApplier{}
	pp, dlq, clk := poisonProcessor(t, pa, PoisonDeadLetter)
	pp.origin = "replicator"
	a = &ImageApplier{p: pp}
	bad := img("b", "oops")
	if err := a.Apply(ctx, "p_db.events", "us:5", bad); err == nil {
		t.Fatal("poison accepted before PoisonAfter")
	}
	clk.advance(time.Minute)
	if err := a.Apply(ctx, "p_db.events", "us:5", bad); err != nil {
		t.Fatal(err)
	}
	if got := dlq.keys(); !reflect.DeepEqual(got, []string{"us:5/b/"}) || dlq.items[0].Origin != "replicator" {
		t.Fatalf("dead letters %v", got)
	}
}
