package dynamo

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/storage"
)

// srcRecords inserts rows into a source table and returns the stream
// worker's view of them: the source row ids DecodeRecord yields for the
// stored items (they must equal the ids Insert returned).
func srcRecords(t *testing.T, e *env, src *catalog.Table, keys ...string) []string {
	t.Helper()
	rows := make([]storage.Row, len(keys))
	for i, k := range keys {
		rows[i] = storage.Row{k}
	}
	res := e.insert(src, rows...)
	want := map[string]bool{}
	for _, id := range res.RowIDs {
		if _, ts, gated := parseSourceID(id); ts == 0 || !gated {
			t.Fatalf("Insert row id %q carries no write time", id)
		}
		want[id] = true
	}
	out, err := e.clients.DynamoDB.Scan(e.ctx, &dynamodb.ScanInput{TableName: aws.String(src.PhysicalName), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, it := range out.Items {
		rec, err := e.store.DecodeRecord(src, "INSERT", it)
		if err != nil {
			t.Fatal(err)
		}
		if want[rec.RowID] {
			found++
		}
	}
	if found != len(keys) {
		t.Fatalf("stream record ids do not match Insert's: %d of %d", found, len(keys))
	}
	return res.RowIDs
}

// countRows applies a count view (k -> 1) of source rows to a Summing target.
func countRows(keys []string, ids []string) []storage.MergeRow {
	out := make([]storage.MergeRow, len(ids))
	for i, id := range ids {
		out[i] = storage.MergeRow{Row: storage.Row{keys[i], uint64(1)}, Source: storage.SourceRef{View: "default.mv", RowID: id}}
	}
	return out
}

func sumsOf(t *testing.T, e *env, tbl *catalog.Table) string {
	t.Helper()
	return strings.Join(rowStrings(e.all(tbl)), " ")
}

func srcAndRollup(e *env, name string, repl string) (*catalog.Table, *catalog.Table) {
	src := e.table(&catalog.Table{Database: "default", Name: name + "_src", Engine: catalog.EngineMergeTree,
		Columns: cols("k", "String"), OrderBy: []string{"k"}, Replication: catalog.ReplicationGlobal})
	roll := e.table(&catalog.Table{Database: "default", Name: name, Engine: catalog.EngineSummingMergeTree,
		Columns: cols("k", "String", "n", "UInt64"), OrderBy: []string{"k"}, Replication: repl})
	return src, roll
}

// TestIntegrationTruncateBacklogNotReapplied: TRUNCATE (and DROP + CREATE)
// of a rollup while the stream worker lags must neither double count the
// backlog after a rebuild nor bring truncated rows back.
func TestIntegrationTruncateBacklogNotReapplied(t *testing.T) {
	e := newEnv(t)
	src, roll := srcAndRollup(e, "roll", catalog.ReplicationRegional)
	_, bare := srcAndRollup(e, "bare", catalog.ReplicationRegional)
	keys := []string{"a", "a", "b"}
	ids := srcRecords(t, e, src, keys...)
	e.merge(roll, countRows(keys, ids)...) // inline
	e.merge(bare, countRows(keys, ids)...)
	if got := sumsOf(t, e, roll); got != "[a 2] [b 1]" {
		t.Fatalf("inline: %s", got)
	}

	// TRUNCATE + rebuild (INSERT INTO roll SELECT ...), then the worker
	// catches up with the backlog.
	if err := e.store.TruncateTable(e.ctx, roll); err != nil {
		t.Fatal(err)
	}
	e.insert(roll, storage.Row{"a", uint64(2)}, storage.Row{"b", uint64(1)})
	e.merge(roll, countRows(keys, ids)...)
	if got := sumsOf(t, e, roll); got != "[a 2] [b 1]" {
		t.Fatalf("backlog after TRUNCATE + rebuild double counted: %s", got)
	}
	// TRUNCATE without rebuild: the backlog does not resurrect rows.
	if err := e.store.TruncateTable(e.ctx, bare); err != nil {
		t.Fatal(err)
	}
	e.merge(bare, countRows(keys, ids)...)
	if got := sumsOf(t, e, bare); got != "" {
		t.Fatalf("truncated rows came back: %s", got)
	}
	// Rows inserted after the TRUNCATE are applied once (inline + worker).
	later := srcRecords(t, e, src, "x")
	e.merge(roll, countRows([]string{"x"}, later)...)
	e.merge(roll, countRows([]string{"x"}, later)...)
	if got := sumsOf(t, e, roll); got != "[a 2] [b 1] [x 1]" {
		t.Fatalf("after a later insert: %s", got)
	}

	// POPULATE after TRUNCATE (scan row ids) applies every source row once,
	// and coincides with the worker's later replay.
	if err := e.store.TruncateTable(e.ctx, bare); err != nil {
		t.Fatal(err)
	}
	it, err := e.store.Scan(e.ctx, src, storage.ScanRequest{Consistent: true})
	if err != nil {
		t.Fatal(err)
	}
	var pop []storage.MergeRow
	for {
		r, err := it.Next(e.ctx)
		if err != nil {
			break
		}
		id := it.(interface{ RowID() string }).RowID()
		if _, ts, gated := parseSourceID(id); ts == 0 || gated {
			t.Fatalf("scan row id %q", id)
		}
		pop = append(pop, storage.MergeRow{Row: storage.Row{r[0], uint64(1)}, Source: storage.SourceRef{View: "default.mv", RowID: id}})
	}
	it.Close()
	e.merge(bare, pop...)
	e.merge(bare, countRows(keys, ids)...)
	e.merge(bare, countRows([]string{"x"}, later)...)
	if got := sumsOf(t, e, bare); got != "[a 2] [b 1] [x 1]" {
		t.Fatalf("POPULATE after TRUNCATE: %s", got)
	}

	// DROP + CREATE + rebuild, then the backlog — also through a
	// caller whose catalog snapshot still has the dropped table.
	old := roll
	if err := e.store.DropTable(e.ctx, roll); err != nil {
		t.Fatal(err)
	}
	if err := e.cat.DropTable(e.ctx, "default", roll.Name, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	roll = e.table(&catalog.Table{Database: "default", Name: "roll", Engine: catalog.EngineSummingMergeTree,
		Columns: cols("k", "String", "n", "UInt64"), OrderBy: []string{"k"}, Replication: catalog.ReplicationRegional})
	e.insert(roll, storage.Row{"a", uint64(2)}, storage.Row{"b", uint64(1)}, storage.Row{"x", uint64(1)})
	e.merge(roll, countRows(keys, ids)...)
	e.merge(old, countRows([]string{"x"}, later)...)
	if got := sumsOf(t, e, roll); got != "[a 2] [b 1] [x 1]" {
		t.Fatalf("backlog after DROP + CREATE + rebuild: %s", got)
	}
}

// truncateHookAPI runs hook once, on the first Scan of table (the TRUNCATE
// deleting rows).
type truncateHookAPI struct {
	ddbAPI
	table string
	once  sync.Once
	hook  func()
}

func (h *truncateHookAPI) Scan(ctx context.Context, in *dynamodb.ScanInput, o ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if aws.ToString(in.TableName) == h.table && h.hook != nil {
		h.once.Do(h.hook)
	}
	return h.ddbAPI.Scan(ctx, in, o...)
}

// TestIntegrationTruncateConcurrentInsert: view output applied while
// TRUNCATE deletes rows waits for it and lands on one side of the epoch;
// the stream worker replaying the same rows afterwards changes nothing.
func TestIntegrationTruncateConcurrentInsert(t *testing.T) {
	var hook *truncateHookAPI
	e := newEnvWith(t, func(*Options) {}, func(d ddbAPI) ddbAPI { hook = &truncateHookAPI{ddbAPI: d}; return hook })
	src, roll := srcAndRollup(e, "conc", catalog.ReplicationRegional)
	before := srcRecords(t, e, src, "pre") // written before the TRUNCATE, applied late
	var (
		during []string
		wg     sync.WaitGroup
		merged atomic.Int32
		errs   = make(chan error, 2)
	)
	hook.table = roll.PhysicalName
	hook.hook = func() {
		during = srcRecords(t, e, src, "during") // written while TRUNCATE runs
		for _, m := range [][]storage.MergeRow{countRows([]string{"pre"}, before), countRows([]string{"during"}, during)} {
			wg.Add(1)
			go func(m []storage.MergeRow) {
				defer wg.Done()
				errs <- e.store.Merge(e.ctx, roll, m) // inline application
				merged.Add(1)
			}(m)
		}
		time.Sleep(300 * time.Millisecond)
		if merged.Load() != 0 {
			errs <- context.Canceled // Merge did not wait for the reset
		}
	}
	e.merge(roll, storage.MergeRow{Row: storage.Row{"old", uint64(1)}, Source: storage.SourceRef{View: "default.mv", RowID: "o"}})
	if err := e.store.TruncateTable(e.ctx, roll); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent merge: %v", err)
		}
	}
	if got := sumsOf(t, e, roll); got != "[during 1]" {
		t.Fatalf("after TRUNCATE (inline only): %s", got)
	}
	e.merge(roll, countRows([]string{"pre"}, before)...) // worker
	e.merge(roll, countRows([]string{"during"}, during)...)
	if got := sumsOf(t, e, roll); got != "[during 1]" {
		t.Fatalf("after the worker: %s", got)
	}
}

// TestIntegrationCatalogResetOtherRegion: a TRUNCATE issued in another
// region (catalog ResetAt) resets this region's copy of a regional table
// when the reconciler ensures the new entry, or before the next Merge; a
// global table's copy only records the epoch (the deletes replicate).
func TestIntegrationCatalogResetOtherRegion(t *testing.T) {
	e := newEnv(t)
	src, roll := srcAndRollup(e, "rr", catalog.ReplicationRegional)
	_, roll2 := srcAndRollup(e, "rr2", catalog.ReplicationRegional)
	g := e.table(&catalog.Table{Database: "default", Name: "gl", Engine: catalog.EngineMergeTree,
		Columns: cols("k", "String", "n", "UInt64"), OrderBy: []string{"k"}, Replication: catalog.ReplicationGlobal})
	keys := []string{"a", "a"}
	ids := srcRecords(t, e, src, keys...)
	e.merge(roll, countRows(keys, ids)...)
	e.merge(roll2, countRows(keys, ids)...)

	reset := func(tbl *catalog.Table) *catalog.Table {
		if err := e.cat.UpdateTable(e.ctx, catalog.MarkTruncated(tbl, time.Now())); err != nil {
			t.Fatal(err)
		}
		nt, err := e.cat.Table(tbl.Database, tbl.Name)
		if err != nil {
			t.Fatal(err)
		}
		return nt
	}
	roll = reset(roll)
	if err := e.store.EnsureTable(e.ctx, roll); err != nil { // reconciler
		t.Fatal(err)
	}
	if got := sumsOf(t, e, roll); got != "" {
		t.Fatalf("regional copy not reset by EnsureTable: %s", got)
	}
	e.merge(roll, countRows(keys, ids)...) // backlog
	if got := sumsOf(t, e, roll); got != "" {
		t.Fatalf("backlog after a remote TRUNCATE: %s", got)
	}
	roll2 = reset(roll2)
	later := srcRecords(t, e, src, "b")
	e.merge(roll2, countRows([]string{"b"}, later)...) // Merge applies the pending reset first
	if got := sumsOf(t, e, roll2); got != "[b 1]" {
		t.Fatalf("pending reset before Merge: %s", got)
	}

	e.merge(g, countRows(keys, ids)...)
	g = reset(g)
	if err := e.store.EnsureTable(e.ctx, g); err != nil {
		t.Fatal(err)
	}
	if n := len(e.all(g)); n != 2 {
		t.Fatalf("global copy must not be deleted by a remote reset: %d rows", n)
	}
}

// TestIntegrationReplacingTargetSourceOrder: view output into a
// ReplacingMergeTree target is ordered by the source rows, so the stream
// worker re-applying (or applying shards out of order) keeps the newest
// source row, like ClickHouse.
func TestIntegrationReplacingTargetSourceOrder(t *testing.T) {
	e := newEnv(t)
	src := e.table(&catalog.Table{Database: "default", Name: "status_events", Engine: catalog.EngineMergeTree,
		Columns: cols("k", "String"), OrderBy: []string{"k"}})
	type tcase struct {
		name, engineArg, want string
		v1, v2                uint64
	}
	cases := []tcase{
		{"nover", "", "[u banned 0]", 0, 0},
		{"eqver", "v", "[u banned 1]", 1, 1},
		{"higher_older", "v", "[u active 2]", 2, 1},
	}
	targets := map[string][2]*catalog.Table{}
	for _, tc := range cases { // targets exist before the source rows are written
		tbl := &catalog.Table{Database: "default", Name: "user_status_" + tc.name, Engine: catalog.EngineReplacingMergeTree,
			Columns: cols("user", "String", "status", "String", "v", "UInt64"), OrderBy: []string{"user"}}
		if tc.engineArg != "" {
			tbl.EngineArgs = []string{tc.engineArg}
		}
		w := *tbl
		w.Name += "_w"
		targets[tc.name] = [2]*catalog.Table{e.table(tbl), e.table(&w)}
	}
	ids := srcRecords(t, e, src, "e1", "e2")
	for _, tc := range cases {
		tgt, e2 := targets[tc.name][0], targets[tc.name][1]
		r1 := storage.MergeRow{Row: storage.Row{"u", "active", tc.v1}, Source: storage.SourceRef{View: "default.mv", RowID: ids[0]}}
		r2 := storage.MergeRow{Row: storage.Row{"u", "banned", tc.v2}, Source: storage.SourceRef{View: "default.mv", RowID: ids[1]}}
		e.merge(tgt, r1, r2) // inline, insertion order
		e.merge(tgt, r2)     // worker: e2's shard first
		e.merge(tgt, r1)     // then e1's
		if got := sumsOf(t, e, tgt); got != tc.want {
			t.Errorf("%s: got %s want %s", tc.name, got, tc.want)
		}
		e.merge(e2, r2) // a region without inline application
		e.merge(e2, r1)
		if got := sumsOf(t, e, e2); got != tc.want {
			t.Errorf("%s (worker only): got %s want %s", tc.name, got, tc.want)
		}
	}
}

// TestIntegrationReplacingWriteOrderMatchesReplicator: a local write that
// is later than a stored row with a larger _ts (written by a clock running
// ahead in another region) still replaces it, and is re-stamped after it
// so cmd/replicator copies it to the other region too.
func TestIntegrationReplacingWriteOrderMatchesReplicator(t *testing.T) {
	e := newEnv(t)
	for _, ver := range []bool{false, true} {
		tbl := &catalog.Table{Database: "default", Name: "s_nover", Engine: catalog.EngineReplacingMergeTree,
			Columns: cols("k", "String", "v", "String", "ver", "UInt64"), OrderBy: []string{"k"}}
		if ver {
			tbl.Name, tbl.EngineArgs = "s_ver", []string{"ver"}
		}
		s := e.table(tbl)
		e.insert(s, storage.Row{"k", "eu-write", uint64(1)})
		ahead := time.Now().Add(30 * time.Second).UnixNano()
		raw, err := e.clients.DynamoDB.Scan(e.ctx, &dynamodb.ScanInput{TableName: aws.String(s.PhysicalName)})
		if err != nil || len(raw.Items) != 1 {
			t.Fatal(err)
		}
		key := map[string]types.AttributeValue{attrPK: raw.Items[0][attrPK], attrSK: raw.Items[0][attrSK]}
		if _, err := e.clients.DynamoDB.UpdateItem(e.ctx, &dynamodb.UpdateItemInput{TableName: aws.String(s.PhysicalName), Key: key,
			UpdateExpression:          aws.String("SET #t = :t, #o = :o"),
			ExpressionAttributeNames:  map[string]string{"#t": attrTS, "#o": attrOrigin},
			ExpressionAttributeValues: map[string]types.AttributeValue{":t": attrN(itoa64(ahead)), ":o": attrS("eu-west-1")}}); err != nil {
			t.Fatal(err)
		}
		res := e.insert(s, storage.Row{"k", "us-write", uint64(1)})
		if res.Skipped[0] {
			t.Fatalf("ver=%v: later write skipped", ver)
		}
		if got := sumsOf(t, e, s); got != "[k us-write 1]" {
			t.Fatalf("ver=%v: %s", ver, got)
		}
		item, err := e.clients.DynamoDB.GetItem(e.ctx, &dynamodb.GetItemInput{TableName: aws.String(s.PhysicalName), Key: key, ConsistentRead: aws.Bool(true)})
		if err != nil {
			t.Fatal(err)
		}
		if ts := numAttr(item.Item, attrTS); ts <= ahead {
			t.Fatalf("ver=%v: stored _ts %d not after %d", ver, ts, ahead)
		}
		if _, ts, _ := parseSourceID(res.RowIDs[0]); ts != numAttr(item.Item, attrTS) {
			t.Fatalf("ver=%v: Insert row id %s does not carry the stored _ts", ver, res.RowIDs[0])
		}
		if ver {
			if res := e.insert(s, storage.Row{"k", "lower", uint64(0)}); !res.Skipped[0] {
				t.Fatal("a lower version must be skipped")
			}
		}
		// Rows of one INSERT sharing a key: the last one wins.
		e.insert(s, storage.Row{"k", "first", uint64(5)}, storage.Row{"k", "last", uint64(5)})
		if got := sumsOf(t, e, s); got != "[k last 5]" {
			t.Fatalf("ver=%v: same-key rows of one INSERT: %s", ver, got)
		}
	}
}
