package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/storage"
)

func atomicSchema(te *testEnv) {
	te.exec(`CREATE TABLE pe (k String, v UInt32) ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE TABLE pr (k String, s UInt64, n UInt64) ENGINE = SummingMergeTree ORDER BY k`)
	te.exec(`CREATE MATERIALIZED VIEW pmv TO pr AS SELECT k, sum(v) AS s, count() AS n FROM pe GROUP BY k`)
}

func tsvBody(rows int, bad bool) string {
	var b strings.Builder
	for i := 1; i <= rows; i++ {
		if bad && i == rows {
			b.WriteString("k0\tnotanumber\n")
			continue
		}
		fmt.Fprintf(&b, "k0\t%d\n", i)
	}
	return b.String()
}

// A parse error anywhere in the block writes nothing (ClickHouse parses the
// whole block before writing it), so fixing the data and resending it does
// not count rows twice.
func TestInsertAtomicParseError(t *testing.T) {
	te := newTestEnv(t)
	atomicSchema(te)
	_, err := te.runReq(&Request{Query: `INSERT INTO pe FORMAT TSV`, Data: strings.NewReader(tsvBody(2500, true))})
	if !cherr.Is(err, cherr.CannotParseText) && !cherr.Is(err, cherr.CannotParseInputAssertionFailed) {
		t.Fatalf("want a parse error, got %v", err)
	}
	te.expectRows(`SELECT count() FROM pe`, "0")
	te.expectRows(`SELECT count() FROM pr`, "0")
	res, err := te.runReq(&Request{Query: `INSERT INTO pe FORMAT TSV`, Data: strings.NewReader(tsvBody(2500, false))})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.WrittenRows, uint64(2500))
	te.expectRows(`SELECT count() FROM pe`, "2500")
	te.expectRows(`SELECT k, sum(s), sum(n) FROM pr GROUP BY k`, "k0\t3126250\t2500")
}

// min_insert_block_size_rows bounds the block: earlier blocks stay written
// (ClickHouse is likewise atomic only per block).
func TestInsertAtomicBlockSize(t *testing.T) {
	te := newTestEnv(t)
	atomicSchema(te)
	_, err := te.runReq(&Request{Query: `INSERT INTO pe SETTINGS min_insert_block_size_rows = 1000 FORMAT TSV`, Data: strings.NewReader(tsvBody(2500, true))})
	if err == nil {
		t.Fatal("want a parse error")
	}
	te.expectRows(`SELECT count() FROM pe`, "2000")
	te.expectRows(`SELECT sum(n) FROM pr`, "2000")
}

// Conversion and DEFAULT errors in VALUES and INSERT SELECT write nothing.
func TestInsertAtomicValuesAndSelect(t *testing.T) {
	te := newTestEnv(t)
	atomicSchema(te)
	var vals []string
	for i := 0; i < 1500; i++ {
		vals = append(vals, fmt.Sprintf("('k%d', %d)", i%3, i))
	}
	vals = append(vals, "('bad', 'x')")
	if _, err := te.run(`INSERT INTO pe VALUES ` + strings.Join(vals, ", ")); err == nil {
		t.Fatal("want a conversion error")
	}
	te.expectRows(`SELECT count() FROM pe`, "0")

	_, err := te.run(`INSERT INTO pe SELECT 'k', if(number = 2499, 'x', toString(number)) FROM numbers(2500)`)
	if err == nil {
		t.Fatal("want a conversion error")
	}
	te.expectRows(`SELECT count() FROM pe`, "0")
	te.expectRows(`SELECT count() FROM pr`, "0")
	te.exec(`INSERT INTO pe SELECT 'k', toString(number) FROM numbers(2500)`)
	te.expectRows(`SELECT count() FROM pe`, "2500")
	te.expectRows(`SELECT sum(n) FROM pr`, "2500")
}

// failMergeStore fails view-target merges while fail is set.
type failMergeStore struct {
	*spyStore
	fail atomic.Bool
}

func (s *failMergeStore) Merge(ctx context.Context, t *catalog.Table, rows []storage.MergeRow) error {
	if s.fail.Load() {
		return cherr.Wrap(cherr.Unavailable, errors.New("injected"), "rollup unavailable")
	}
	return s.spyStore.Merge(ctx, t, rows)
}

// With StreamViews, an inline view failure after the source rows are stored
// does not fail the INSERT (the stream worker applies the views exactly
// once); without it the error is returned.
func TestInsertInlineViewFailure(t *testing.T) {
	for _, streamViews := range []bool{false, true} {
		t.Run(fmt.Sprint("StreamViews=", streamViews), func(t *testing.T) {
			te := newTestEnv(t)
			fs := &failMergeStore{spyStore: te.spy}
			e, err := New(Config{Store: fs, Catalog: te.cat, InlineViews: true, StreamViews: streamViews,
				ConsistentReads: true, Timezone: "UTC", ServerVersion: "25.8.1.1", ServerRevision: 54477, DisplayName: "test"})
			if err != nil {
				t.Fatal(err)
			}
			e.impl.fresh.interval = 0
			te.e = e
			atomicSchema(te)
			fs.fail.Store(true)
			_, err = te.run(`INSERT INTO pe VALUES ('a', 1), ('a', 2)`)
			fs.fail.Store(false)
			if streamViews {
				if err != nil {
					t.Fatalf("INSERT failed: %v", err)
				}
			} else if !cherr.Is(err, cherr.Unavailable) {
				t.Fatalf("want UNAVAILABLE, got %v", err)
			}
			te.expectRows(`SELECT count() FROM pe`, "2")
		})
	}
}

// FixedString compares with a FixedString of another length as if both were
// zero padded (ClickHouse 25.8).
func TestFixedStringMixedLengthCompare(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT toFixedString('ab',4) = toFixedString('ab',2), toFixedString('ab',4) > toFixedString('ab',2), toFixedString('ab',4) < toFixedString('ab',2), toFixedString('ab',4) IN (toFixedString('ab',2)), toFixedString('abc',4) > toFixedString('ab',2), toFixedString('ab\0c',4) > toFixedString('ab',2), toFixedString('ab',2) = toFixedString('ab\0\0\0', 5), toFixedString('ab',2) < toFixedString('ab\0\0c', 5)`,
		"1\t0\t0\t1\t1\t1\t1\t1")
	te.exec(`CREATE TABLE fs (id UInt8, f FixedString(4), g FixedString(4)) ENGINE = MergeTree ORDER BY (f, id)`)
	te.exec(`INSERT INTO fs VALUES (1, 'ab', 'ab'), (2, 'abc', 'abc')`)
	for _, col := range []string{"f", "g"} {
		te.expectRows(`SELECT id FROM fs WHERE `+col+` = toFixedString('ab', 2) ORDER BY id`, "1")
		te.expectRows(`SELECT id FROM fs WHERE `+col+` IN (toFixedString('ab', 2), toFixedString('abc', 3)) ORDER BY id`, "1\n2")
		te.expectRows(`SELECT id FROM fs WHERE `+col+` > toFixedString('ab', 2) ORDER BY id`, "2")
		te.expectRows(`SELECT id FROM fs WHERE `+col+` <= toFixedString('ab', 2) ORDER BY id`, "1")
	}
	// (ClickHouse 25.8 returns nothing here for the key column f.)
	te.expectRows(`SELECT id FROM fs WHERE g = toFixedString('abc', 8) ORDER BY id`, "2")
}
