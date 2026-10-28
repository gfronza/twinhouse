package exec

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/storage"
)

// A plan's constants count against the limit while the query holds them,
// so a query cannot hold many large ones uncounted.
func TestPlannedConstantsCountAgainstTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("plans constants of about 40 MB under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const s = " SETTINGS max_memory_usage = 32000000"
	terms := func(n int) string {
		var b []string
		for i := range n {
			b = append(b, fmt.Sprintf("has(range(%d), number)", 300000+i))
		}
		return "SELECT count() FROM numbers(10) WHERE " + strings.Join(b, " AND ")
	}
	te.expectRows(terms(2)+s, "10")
	wantMemoryLimit(t, te, terms(4)+s, nil)
	// A folded expression is charged, not the constants it was folded
	// from.
	te.expectRows("SELECT count() FROM numbers(10) WHERE number < length(range(400000)) AND number < length(range(400001))"+s, "10")
}

// A scalar subquery's value is charged once, with the subquery's cached
// rows, also where an alias or the tuple of several columns holds it. The
// array takes 12.2 MiB and the sort more than 6 MiB: charged twice, the
// queries would pass the limit. (The aggregation holds the array charged
// until the collect has charged it too: 24.4 MiB.) This is not parity:
// ClickHouse 25.8 gives 241, as arrayElement expands the constant array.
func TestScalarSubqueryValueIsChargedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("plans values of about 12 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const s = " SETTINGS max_memory_usage = 32000000"
	sorted := func(cond string) string {
		return "SELECT count() FROM (SELECT number FROM numbers(140000) WHERE " + cond + " ORDER BY number DESC)" + s
	}
	const arr = "(SELECT groupArray(number) FROM numbers(400000))"
	te.expectRows(sorted(arr+"[number + 1] = number"), "140000")
	te.expectRows(sorted("arrayElement("+arr+" AS a, number + 1) = number AND a[number + 1] < 400000"), "140000")
	te.expectRows(sorted("tupleElement((SELECT groupArray(number), 1 FROM numbers(400000)), 1)[number + 1] = number"), "140000")
}

// Constants that are compiled again and dropped (each VALUES row, a view
// planned for each block) are not kept charged.
func TestDroppedConstantsAreReleased(t *testing.T) {
	if testing.Short() {
		t.Skip("plans constants of 50 MB under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const s = " SETTINGS max_memory_usage = 32000000"
	te.exec("CREATE TABLE t (s String) ENGINE = MergeTree ORDER BY tuple()")
	te.exec("INSERT INTO t" + s + " VALUES " + strings.Repeat("(repeat('x', 100000)), ", 499) + "(repeat('x', 100000))")
	te.expectRows("SELECT count(), sum(length(s)) FROM t", "500\t50000000")
	te.exec("CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id")
	te.exec("CREATE TABLE dst (id UInt64, h UInt8) ENGINE = MergeTree ORDER BY id")
	te.exec("CREATE MATERIALIZED VIEW mv TO dst AS SELECT id, has(range(300000), id) AS h FROM src")
	te.exec("INSERT INTO src SELECT number FROM numbers(20000)" + s)
	te.expectRows("SELECT count(), sum(h) FROM dst", "20000\t20000")
}

// A constant IN set is kept with the plan that built it. A view is planned
// again for each block, and drops its plan, set included, once the block
// is applied: the sets do not add up over a long INSERT.
func TestViewConstantInSetsDoNotAddUp(t *testing.T) {
	if testing.Short() {
		t.Skip("applies a view to 20 blocks")
	}
	guardFunctionTest(t)
	for _, limit := range []int64{0, 32000000} {
		var insert *query
		sets, accounts := 0, 0
		te := newTestEnvConfig(t, func(c *Config) {
			c.MaxMemoryUsage = limit
			c.Store = onMerge{c.Store, func() {
				sets = max(sets, len(insert.inSets)+len(insert.plan.consts.sets))
				accounts = max(accounts, len(*insert.lifetime))
			}}
		})
		te.exec("CREATE TABLE src (k UInt64, n UInt64) ENGINE = MergeTree ORDER BY k")
		te.exec("CREATE TABLE dst (k UInt64, n UInt64) ENGINE = MergeTree ORDER BY k")
		te.exec("CREATE MATERIALIZED VIEW mv TO dst AS SELECT k, n FROM src WHERE n IN range(20000)")
		te.e.impl.testHookStarted = func(q *query) { insert = q }
		te.exec("INSERT INTO src SELECT number, number % 40000 FROM numbers(20000)")
		te.e.impl.testHookStarted = nil
		te.expectRows("SELECT count() FROM dst", "20000")
		t.Logf("limit %d: at most %d sets and %d lifetime accounts", limit, sets, accounts)
		if sets != 1 || accounts > 4 {
			t.Errorf("limit %d: the INSERT held up to %d sets and %d lifetime accounts, want 1 set", limit, sets, accounts)
		}
	}
}

// onMerge calls fn before each Merge.
type onMerge struct {
	storage.Store
	fn func()
}

func (s onMerge) Merge(ctx context.Context, t *catalog.Table, rows []storage.MergeRow) error {
	s.fn()
	return s.Store.Merge(ctx, t, rows)
}
