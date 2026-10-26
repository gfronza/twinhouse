package mvworker

// Integration test of Worker.Run against DynamoDB Local with the real dynamo
// store and exec engine. It runs when DDB_TEST_ENDPOINT is set (e.g.
// http://localhost:18101) and is skipped otherwise.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/awsx"
	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/config"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/storage/dynamo"
)

// node is one process: a store, its own catalog snapshot and an engine.
type node struct {
	store *dynamo.Store
	cat   catalog.Catalog
	eng   *exec.Engine
}

func newNode(t *testing.T, c *awsx.Clients, prefix string) *node {
	t.Helper()
	store, err := dynamo.New(c, dynamo.Options{Prefix: prefix, Region: "us-east-1", GlobalMode: config.GlobalNone})
	if err != nil {
		t.Fatal(err)
	}
	cat := store.NewCatalog()
	eng, err := exec.New(exec.Config{Store: store, Catalog: cat, Region: "us-east-1", InlineViews: false,
		ConsistentReads: true, PhysicalName: store.PhysicalName})
	if err != nil {
		t.Fatal(err)
	}
	return &node{store: store, cat: cat, eng: eng}
}

func (n *node) exec(t *testing.T, q string) {
	t.Helper()
	if _, err := n.eng.Execute(context.Background(), &exec.Request{Query: q, Database: "default"}); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func (n *node) query(t *testing.T, q string) string {
	t.Helper()
	res, err := n.eng.Execute(context.Background(), &exec.Request{Query: q, Database: "default"})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	var lines []string
	for _, r := range res.Rows {
		f := make([]string, len(r))
		for i, v := range r {
			f[i] = fmt.Sprint(v)
		}
		lines = append(lines, strings.Join(f, "\t"))
	}
	return strings.Join(lines, "\n")
}

// startWorker runs a worker on its own node (a separate mvworker process).
// refresh is the periodic catalog refresh.
func startWorker(t *testing.T, c *awsx.Clients, prefix, owner string, refresh time.Duration) (*Worker, func()) {
	t.Helper()
	n := newNode(t, c, prefix)
	w, err := New(Config{Clients: c, Store: n.store, Catalog: n.cat, Engine: n.eng, Owner: owner,
		PollInterval: 20 * time.Millisecond, LeaseDuration: 3 * time.Second, CatalogRefresh: refresh})
	if err != nil {
		t.Fatal(err)
	}
	w.impl.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return w, stop
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// stable asserts that q keeps returning want for d.
func stable(t *testing.T, n *node, q, want string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if got := n.query(t, q); got != want {
			t.Fatalf("%s changed:\n got %q\nwant %q", q, got, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestIntegrationWorkerAppliesViewsExactlyOnce(t *testing.T) {
	ep := os.Getenv("DDB_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("DDB_TEST_ENDPOINT not set")
	}
	ctx := context.Background()
	c, err := awsx.New(ctx, "us-east-1", ep)
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("mvit%d_", time.Now().UnixNano())
	srv := newNode(t, c, prefix) // the "chserver": DDL and inserts, no inline views
	if err := srv.store.EnsureSystemTables(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = srv.eng.Execute(context.Background(), &exec.Request{Query: "DROP DATABASE IF EXISTS app", Database: "default"})
		for _, tbl := range []string{srv.store.CatalogTableName(), srv.store.MarkersTableName(), srv.store.CheckpointsTableName()} {
			_, _ = c.DynamoDB.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tbl)})
		}
	})

	srv.exec(t, "CREATE DATABASE app")
	srv.exec(t, `CREATE TABLE app.events (tenant_id String, campaign_id String, user_id String,
		event_time DateTime, clicks UInt64) ENGINE = MergeTree ORDER BY (tenant_id, event_time, user_id)`)
	srv.exec(t, `CREATE TABLE app.daily (tenant_id String, campaign_id String,
		events SimpleAggregateFunction(sum, UInt64), clicks SimpleAggregateFunction(sum, UInt64),
		last_seen SimpleAggregateFunction(max, DateTime), users AggregateFunction(uniqExact, String))
		ENGINE = AggregatingMergeTree ORDER BY (tenant_id, campaign_id)`)
	// Rows inserted before the view exists must never reach it.
	srv.exec(t, `INSERT INTO app.events VALUES ('t1','c1','old','2026-01-01 00:00:00',100), ('t2','c9','old','2026-01-01 00:00:00',100)`)
	srv.exec(t, `CREATE MATERIALIZED VIEW app.mv TO app.daily AS
		SELECT tenant_id, campaign_id, count() AS events, sum(clicks) AS clicks, max(event_time) AS last_seen,
		uniqExactState(user_id) AS users FROM app.events GROUP BY tenant_id, campaign_id`)
	srv.exec(t, `INSERT INTO app.events VALUES
		('t1','c1','u1','2026-01-01 10:00:00',1), ('t1','c1','u2','2026-01-01 11:00:00',2),
		('t1','c1','u1','2026-01-01 12:00:00',3), ('t1','c2','u3','2026-01-01 10:30:00',4),
		('t2','c1','u4','2026-01-02 08:00:00',5)`)

	const rollup = `SELECT tenant_id, campaign_id, sum(events), sum(clicks), toUnixTimestamp(max(last_seen)), uniqExactMerge(users)
		FROM app.daily GROUP BY tenant_id, campaign_id ORDER BY tenant_id, campaign_id`
	want := strings.Join([]string{
		"t1\tc1\t3\t6\t1767268800\t2",
		"t1\tc2\t1\t4\t1767263400\t1",
		"t2\tc1\t1\t5\t1767340800\t1",
	}, "\n")
	if got := srv.query(t, rollup); got != "" {
		t.Fatalf("rollup before the worker ran: %q (inline views are off)", got)
	}

	// The worker refreshes its catalog snapshot only once an hour on its own:
	// everything below relies on the per-batch freshness check.
	w1, stop1 := startWorker(t, c, prefix, "w1", time.Hour)
	eventually(t, "worker rollup", 60*time.Second, func() bool { return srv.query(t, rollup) == want })
	stable(t, srv, rollup, want, time.Second)
	if st := w1.Stats(); st.RecordsApplied != 5 || st.Errors != 0 {
		t.Fatalf("stats %+v (want 5 applied: the 2 pre-view rows are skipped)", st)
	}
	stop1()

	// Redelivery: forget every checkpoint and replay the whole stream with a
	// new worker. Exactly-once markers make it a no-op.
	ckpt := srv.store.CheckpointsTableName()
	out, err := c.DynamoDB.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(ckpt), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) == 0 {
		t.Fatal("no checkpoints were written")
	}
	for _, it := range out.Items {
		if _, err := c.DynamoDB.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(ckpt),
			Key: map[string]ddbtypes.AttributeValue{"_pk": it["_pk"]}}); err != nil {
			t.Fatal(err)
		}
	}
	w2, _ := startWorker(t, c, prefix, "w2", time.Hour)
	eventually(t, "replay", 60*time.Second, func() bool { return w2.Stats().RecordsApplied >= 5 })
	stable(t, srv, rollup, want, time.Second)

	// A view created after the worker started is picked up for rows written
	// after it, without waiting for the periodic refresh.
	srv.exec(t, `CREATE TABLE app.by_tenant (tenant_id String, events SimpleAggregateFunction(sum, UInt64))
		ENGINE = AggregatingMergeTree ORDER BY tenant_id`)
	srv.exec(t, `CREATE MATERIALIZED VIEW app.mv_tenant TO app.by_tenant AS
		SELECT tenant_id, count() AS events FROM app.events GROUP BY tenant_id`)
	srv.exec(t, `INSERT INTO app.events VALUES ('t1','c1','u5','2026-01-03 00:00:00',10)`)
	want = strings.Join([]string{
		"t1\tc1\t4\t16\t1767398400\t3",
		"t1\tc2\t1\t4\t1767263400\t1",
		"t2\tc1\t1\t5\t1767340800\t1",
	}, "\n")
	const tenants = "SELECT tenant_id, sum(events) FROM app.by_tenant GROUP BY tenant_id ORDER BY tenant_id"
	eventually(t, "new view applied", 60*time.Second, func() bool {
		return srv.query(t, rollup) == want && srv.query(t, tenants) == "t1\t1"
	})

	// DROP VIEW, then insert at once: the worker's snapshot still lists the
	// view, but the record is newer than the snapshot, so it reloads first.
	srv.exec(t, "DROP VIEW app.mv_tenant")
	srv.exec(t, `INSERT INTO app.events VALUES ('t1','c1','u6','2026-01-04 00:00:00',20)`)
	want = strings.Join([]string{
		"t1\tc1\t5\t36\t1767484800\t4",
		"t1\tc2\t1\t4\t1767263400\t1",
		"t2\tc1\t1\t5\t1767340800\t1",
	}, "\n")
	eventually(t, "remaining view applied", 60*time.Second, func() bool { return srv.query(t, rollup) == want })
	stable(t, srv, tenants, "t1\t1", 2*time.Second)
	if st := w2.Stats(); st.Errors != 0 {
		t.Fatalf("stats %+v", st)
	}
}

// A poison record with the real engine: a view that cannot evaluate
// one source row (toUInt64('oops')) must not hold back the other rows of
// its batch or later batches; the row is dead-lettered and the checkpoint
// advances. A second view of the same source, which can evaluate the row,
// still gets it.
func TestIntegrationPoisonRecordIsDeadLettered(t *testing.T) {
	ep := os.Getenv("DDB_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("DDB_TEST_ENDPOINT not set")
	}
	ctx := context.Background()
	c, err := awsx.New(ctx, "us-east-1", ep)
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("mvpoison%d_", time.Now().UnixNano())
	srv := newNode(t, c, prefix)
	if err := srv.store.EnsureSystemTables(ctx); err != nil {
		t.Fatal(err)
	}
	dlqTable := DeadLetterTableName(srv.store.CheckpointsTableName())
	t.Cleanup(func() {
		_, _ = srv.eng.Execute(context.Background(), &exec.Request{Query: "DROP DATABASE IF EXISTS app", Database: "default"})
		for _, tbl := range []string{srv.store.CatalogTableName(), srv.store.MarkersTableName(), srv.store.CheckpointsTableName(), dlqTable} {
			_, _ = c.DynamoDB.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tbl)})
		}
	})
	srv.exec(t, "CREATE DATABASE app")
	srv.exec(t, "CREATE TABLE app.events (k String, amount String) ENGINE = MergeTree ORDER BY k")
	srv.exec(t, "CREATE TABLE app.totals (k String, s UInt64) ENGINE = SummingMergeTree ORDER BY k")
	srv.exec(t, "CREATE TABLE app.counts (k String, n SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k")
	srv.exec(t, "CREATE MATERIALIZED VIEW app.mv TO app.totals AS SELECT k, toUInt64(amount) AS s FROM app.events")
	srv.exec(t, "CREATE MATERIALIZED VIEW app.mv_count TO app.counts AS SELECT k, count() AS n FROM app.events GROUP BY k")
	srv.exec(t, "INSERT INTO app.events VALUES ('a','5'), ('b','oops'), ('c','7'), ('a','1')")

	n := newNode(t, c, prefix)
	w, err := New(Config{Clients: c, Store: n.store, Catalog: n.cat, Engine: n.eng, Owner: "w-poison",
		PollInterval: 20 * time.Millisecond, LeaseDuration: 3 * time.Second, CatalogRefresh: time.Hour,
		PoisonAfter: 2 * time.Second, CatalogMinReload: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.impl.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	w.impl.proc.log = w.impl.log
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = w.Run(wctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	const totals = "SELECT k, sum(s) FROM app.totals GROUP BY k ORDER BY k"
	const counts = "SELECT k, sum(n) FROM app.counts GROUP BY k ORDER BY k"
	// Good rows of the batch are applied before the poison is set aside.
	eventually(t, "good rows applied", 30*time.Second, func() bool { return srv.query(t, totals) == "a\t6\nc\t7" })
	eventually(t, "dead letter", 30*time.Second, func() bool { return w.Stats().DeadLettered == 1 })
	srv.exec(t, "INSERT INTO app.events VALUES ('d','3')")
	eventually(t, "later batch applied", 30*time.Second, func() bool { return srv.query(t, totals) == "a\t6\nc\t7\nd\t3" })
	eventually(t, "other view applied to every row", 30*time.Second, func() bool {
		return srv.query(t, counts) == "a\t2\nb\t1\nc\t1\nd\t1"
	})

	out, err := c.DynamoDB.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(dlqTable), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("dead letters: %d items", len(out.Items))
	}
	it := out.Items[0]
	str := func(k string) string {
		v, _ := it[k].(*ddbtypes.AttributeValueMemberS)
		if v == nil {
			return ""
		}
		return v.Value
	}
	if str("table") != "app.events" || str("view") != "app.mv" || str("error_name") != "CANNOT_PARSE_TEXT" || str("row_id") == "" {
		t.Fatalf("dead letter item %v", it)
	}
	img, _ := it["image"].(*ddbtypes.AttributeValueMemberM)
	if img == nil {
		t.Fatal("dead letter without image")
	}
	if a, _ := img.Value["amount"].(*ddbtypes.AttributeValueMemberS); a == nil || a.Value != "oops" {
		t.Fatalf("dead letter image: %v", img.Value)
	}
	if st := w.Stats(); st.RetryingBatches != 0 || st.PendingAgeMillis != 0 {
		t.Fatalf("stats after recovery %+v", st)
	}
}

// A rollup row that would grow past the 400 KB item limit fails inside
// TransactWriteItems (marker Put + row Update) as a TransactionCanceledException
// with a ValidationError reason, not as a ValidationException. It is
// deterministic: the records that would overflow the row are dead-lettered
// and the rest of the shard (other keys, later records) is applied instead
// of the batch being retried forever.
func TestIntegrationOversizedRollupRowIsDeadLettered(t *testing.T) {
	ep := os.Getenv("DDB_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("DDB_TEST_ENDPOINT not set")
	}
	ctx := context.Background()
	c, err := awsx.New(ctx, "us-east-1", ep)
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("mvbig%d_", time.Now().UnixNano())
	srv := newNode(t, c, prefix)
	if err := srv.store.EnsureSystemTables(ctx); err != nil {
		t.Fatal(err)
	}
	dlqTable := DeadLetterTableName(srv.store.CheckpointsTableName())
	t.Cleanup(func() {
		_, _ = srv.eng.Execute(context.Background(), &exec.Request{Query: "DROP DATABASE IF EXISTS app", Database: "default"})
		for _, tbl := range []string{srv.store.CatalogTableName(), srv.store.MarkersTableName(), srv.store.CheckpointsTableName(), dlqTable} {
			_, _ = c.DynamoDB.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tbl)})
		}
	})
	srv.exec(t, "CREATE DATABASE app")
	srv.exec(t, "CREATE TABLE app.src (k String, s String) ENGINE = MergeTree ORDER BY k")
	srv.exec(t, "CREATE TABLE app.agg (k String, l AggregateFunction(groupArray, String), c AggregateFunction(count)) ENGINE = AggregatingMergeTree ORDER BY k")
	srv.exec(t, "CREATE MATERIALIZED VIEW app.mv TO app.agg AS SELECT k, groupArrayState(s) AS l, countState() AS c FROM app.src GROUP BY k")
	// Four 90 KB values fit in one item, a fifth does not.
	for i := 0; i < 6; i++ {
		srv.exec(t, "INSERT INTO app.src SELECT 'a', repeat('x', 90000)")
	}

	n := newNode(t, c, prefix)
	w, err := New(Config{Clients: c, Store: n.store, Catalog: n.cat, Engine: n.eng, Owner: "w-big",
		PollInterval: 20 * time.Millisecond, LeaseDuration: 3 * time.Second, CatalogRefresh: time.Hour,
		PoisonAfter: 2 * time.Second, CatalogMinReload: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	w.impl.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	w.impl.proc.log = w.impl.log
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = w.Run(wctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	const q = "SELECT k, length(groupArrayMerge(l)), countMerge(c) FROM app.agg GROUP BY k ORDER BY k"
	eventually(t, "overflowing records dead-lettered", 60*time.Second, func() bool { return w.Stats().DeadLettered == 2 })
	srv.exec(t, "INSERT INTO app.src VALUES ('c', 'y')")
	eventually(t, "later record applied", 30*time.Second, func() bool { return srv.query(t, q) == "a\t4\t4\nc\t1\t1" })

	out, err := c.DynamoDB.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(dlqTable), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 2 {
		t.Fatalf("dead letters: %d items", len(out.Items))
	}
	for _, it := range out.Items {
		v, _ := it["error"].(*ddbtypes.AttributeValueMemberS)
		if v == nil || !strings.Contains(v.Value, "ValidationError") || it["view"] == nil {
			t.Fatalf("dead letter item %v", it["error"])
		}
	}
	if st := w.Stats(); st.RetryingBatches != 0 || st.PendingAgeMillis != 0 {
		t.Fatalf("stats after recovery %+v", st)
	}
}
