package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/storage"
)

// Expected values below come from ClickHouse 25.8.

func TestTupleComparisonThreeValued(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT (1,NULL) < (2,1), (1,NULL) = (1,NULL), (1,nan) < (1,2), (1,nan) = (1,nan), ([NULL],1) < ([1],1), [nan] = [nan], [1,NULL] = [1,NULL], toTypeName((1,NULL) < (2,1)), toTypeName((1,2) < (2,1)), (1,2) <= (1,2), (1,nan) <= (1,nan), (1,nan) != (1,nan), ((1,NULL),2) = ((1,NULL),2), ((1,nan),2) < ((1,2),3), (nan,1) < (2,1)`,
		"\\N\t\\N\t0\t0\t0\t1\t1\tNullable(Nothing)\tUInt8\t1\t0\t1\t\\N\t0\t0")
	te.expectRows(`SELECT toTypeName((1,toNullable(2)) < (2,1)), (1,toNullable(2)) < (2,1), toTypeName((1,2) = (1,NULL)), toTypeName(((1,toNullable(2)),3) < ((1,2),3)), ((1,toNullable(nan)),3) < ((1,2),3), toTypeName(([NULL],1) < ([1],1))`,
		"Nullable(UInt8)\t1\tNullable(Nothing)\tNullable(UInt8)\t0\tUInt8")
	te.expectRows(`WITH CAST(NULL, 'Nullable(UInt8)') AS n SELECT (1,n) < (1,2), (2,n) < (1,2), (1,n) = (2,1), (1,n) = (1,1), (1,n) != (2,1), (1,n) != (1,1), (1,n) <= (1,n), (n,1) > (1,1), ((1,n),1) < ((2,1),1), ([n],1) < ([1],1), (1,n) >= (0, 5)`,
		"\\N\t0\t0\t\\N\t1\t\\N\t\\N\t\\N\t1\t0\t1")
}

func TestNestedNullNaNExtremaAndOrder(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT greatest([1,NULL],[1,2]), least([NULL],[1]), greatest([1,nan],[1,2]), least([1,nan],[1,2])`, "[1,2]\t[1]\t[1,2]\t[1,2]")
	te.expectRows(`SELECT max(x), min(x) FROM (SELECT arrayJoin([[1,NULL],[1,2],[NULL],[1]]) x)`, "[1,2]\t[1]")
	te.expectRows(`SELECT max(x), min(x) FROM (SELECT arrayJoin([[1,nan],[1,2]]) x)`, "[1,2]\t[1,2]")
	te.expectRows(`SELECT argMax(s, x), argMin(s, x) FROM (SELECT arrayJoin([([1,NULL],'a'),([1,2],'b'),([NULL],'c'),([1],'d')]) t, t.1 x, t.2 s)`, "b\td")
	const src = `SELECT arrayJoin([[1,NULL],[1,nan],[NULL],[1],[nan],[inf],[1,2],[]]) x`
	for _, c := range []struct{ order, want string }{
		{"x DESC", "[[inf],[1,2],[1,nan],[1,NULL],[1],[nan],[NULL],[]]"},
		{"x NULLS FIRST", "[[],[NULL],[nan],[1],[1,NULL],[1,nan],[1,2],[inf]]"},
		{"x", "[[],[1],[1,2],[1,nan],[1,NULL],[inf],[nan],[NULL]]"},
		{"x DESC NULLS FIRST", "[[NULL],[nan],[inf],[1,NULL],[1,nan],[1,2],[1],[]]"},
	} {
		te.expectRows(`SELECT groupArray(x) FROM (SELECT x FROM (`+src+`) ORDER BY `+c.order+`)`, c.want)
	}
	// Top-level NULL and NaN keep their places.
	te.expectRows(`SELECT x FROM (SELECT arrayJoin([1, NULL, nan, -1]) x) ORDER BY x DESC`, "1\n-1\nnan\n\\N")
	te.expectRows(`SELECT x FROM (SELECT arrayJoin([1, NULL, nan, -1]) x) ORDER BY x NULLS FIRST`, "\\N\nnan\n-1\n1")
}

// TRUNCATE records the reset in the catalog (a newer ResetAt) before
// deleting rows, so other regions apply it to their copies.
func TestTruncateRecordsReset(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE r (k String, n UInt64) ENGINE = SummingMergeTree ORDER BY k`)
	te.exec(`INSERT INTO r VALUES ('a', 1)`)
	before, err := te.cat.Table("default", "r")
	if err != nil {
		t.Fatal(err)
	}
	te.exec(`TRUNCATE TABLE r`)
	after, err := te.cat.Table("default", "r")
	if err != nil {
		t.Fatal(err)
	}
	if !after.ResetAt.After(before.ResetAt) || after.Version != before.Version+1 {
		t.Fatalf("ResetAt %v -> %v, version %d -> %d", before.ResetAt, after.ResetAt, before.Version, after.Version)
	}
	te.expectRows(`SELECT count() FROM r`, "0")
	te.exec(`TRUNCATE TABLE r`)
	again, _ := te.cat.Table("default", "r")
	if !again.ResetAt.After(after.ResetAt) {
		t.Fatalf("second TRUNCATE did not advance ResetAt")
	}
}

var errMergeSentinel = errors.New("sentinel merge failure")

type failingMergeStore struct{ *spyStore }

func (s failingMergeStore) Merge(ctx context.Context, t *catalog.Table, rows []storage.MergeRow) error {
	return fmt.Errorf("wrapped: %w", errMergeSentinel)
}

// A failing view's error keeps its cause chain (the stream worker
// classifies AWS errors with errors.As), also when several views fail.
func TestApplyViewsKeepsCause(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE s (k String) ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE TABLE a (k String, n UInt64) ENGINE = SummingMergeTree ORDER BY k`)
	te.exec(`CREATE MATERIALIZED VIEW m1 TO a AS SELECT k, count() AS n FROM s GROUP BY k`)
	te.exec(`CREATE MATERIALIZED VIEW m2 TO a AS SELECT k, count() AS n FROM s GROUP BY k`)
	te.e.cfg.Store = failingMergeStore{te.spy}
	_, err := te.run(`INSERT INTO s VALUES ('x')`)
	if err == nil || !errors.Is(err, errMergeSentinel) {
		t.Fatalf("err = %v; want the Merge cause in its chain", err)
	}
	if !strings.Contains(err.Error(), "sentinel merge failure; ") {
		t.Fatalf("both views' messages expected: %v", err)
	}
}

// A write to a physical table that is gone reports a re-created table as
// retryable, and a dropped table as unknown.
func TestStaleTableError(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE st (k String) ENGINE = MergeTree ORDER BY k`)
	stale, err := te.cat.Table("default", "st")
	if err != nil {
		t.Fatal(err)
	}
	gone := cherr.New(cherr.UnknownTable, "DynamoDB table is gone")
	time.Sleep(time.Millisecond)
	te.exec(`DROP TABLE st`)
	te.exec(`CREATE TABLE st (k String, v UInt8) ENGINE = MergeTree ORDER BY k`)
	q := te.e.newQuery(context.Background(), &Request{})
	if err := q.staleTableError(stale, gone); !cherr.Is(err, cherr.UnknownTable) || !strings.Contains(err.Error(), "dropped and created again") {
		t.Fatalf("recreated: %v", err)
	}
	te.exec(`DROP TABLE st`)
	q = te.e.newQuery(context.Background(), &Request{})
	if err := q.staleTableError(stale, gone); !cherr.Is(err, cherr.UnknownTable) || strings.Contains(err.Error(), "created again") {
		t.Fatalf("dropped: %v", err)
	}
	other := cherr.New(cherr.Unavailable, "throttled")
	if err := q.staleTableError(stale, other); err != other {
		t.Fatalf("other errors pass through: %v", err)
	}
}

// Maps compare like ClickHouse's compareAt over their (key, value) pairs.
func TestMapComparison(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT map(1,NULL) > map(1,2), map(1,2) = map(1,2), map(1,2) < map(2,1), map(1,2) < map(1,2,3,4), toTypeName(map(1,2) < map(2,1)), map('a',nan) = map('a',nan), map(1,2) != map(1,3)`,
		"1\t1\t1\t1\tUInt8\t1\t1")
}
