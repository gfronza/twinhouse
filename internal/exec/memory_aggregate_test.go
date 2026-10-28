package exec

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

// freshRows yields n rows of the compileAggregation scope (a Int64,
// b Nullable(Int64), s String), each built anew as storage decodes them.
// At the end it calls atEnd, while the aggregation still holds its map.
type freshRows struct {
	n, i  int
	row   func(i int) []any
	atEnd func()
}

func (f *freshRows) next() ([]any, error) {
	if f.i == f.n {
		if f.atEnd != nil {
			f.atEnd()
			f.atEnd = nil
		}
		return nil, io.EOF
	}
	f.i++
	return f.row(f.i - 1), nil
}

func (f *freshRows) close() {}

func TestAggregationStateMemoryLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("aggregates up to 32 MB per query")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const limit = " SETTINGS max_memory_usage = 32000000"
	for _, sql := range []string{
		"SELECT count() FROM (SELECT number AS k, count() FROM numbers(10000000) GROUP BY k)",
		"SELECT count() FROM (SELECT toString(number) AS k FROM numbers(10000000) GROUP BY k)",
		"SELECT uniqExact(number) FROM numbers(10000000)",
		"SELECT uniqExact(number, toString(number)) FROM numbers(10000000)",
		"SELECT count(DISTINCT number) FROM numbers(10000000)",
		"SELECT length(groupUniqArray(number)) FROM numbers(10000000)",
		"SELECT length(groupArray(number)) FROM numbers(10000000)",
		"SELECT length(any(repeat(repeat('x', 1000), 40000))) FROM numbers(1)",
		"SELECT length(argMax(repeat(repeat('x', 1000), 40000), number)) FROM numbers(2)",
		"SELECT count() FROM (SELECT number AS k, any(repeat(toString(number), 10000)) FROM numbers(100000) GROUP BY k)",
		"SELECT count() FROM (SELECT number AS k, argMax(repeat(toString(number), 10000), number) FROM numbers(100000) GROUP BY k)",
	} {
		wantMemoryLimit(t, te, sql+limit, nil)
	}
	// tests/compat TestMemoryLimitMessage runs this on both servers.
	_, err := te.run("SELECT length(groupArray(number)) FROM numbers(10000000) SETTINGS max_memory_usage = 20000000")
	if ce := cherr.As(err); ce == nil || ce.Code != cherr.MemoryLimitExceeded || !strings.HasSuffix(ce.Msg, "maximum: 19.07 MiB") {
		t.Errorf("groupArray at 20000000: %v", err)
	}
}

// A replaced kept value stops counting: anyLast over 500 values of 1 MB
// holds one at a time.
func TestAggregationReleasesReplacedValues(t *testing.T) {
	if testing.Short() {
		t.Skip("builds 500 values of 1 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const limit = " SETTINGS max_memory_usage = 32000000"
	te.expectRows("SELECT length(anyLast(if(number % 2 = 0, repeat(toString(number), 250000), 'y'))) FROM numbers(1000)"+limit, "1")
	te.expectRows("SELECT length(argMax(if(number % 2 = 0, repeat(repeat('x', 1000), 1000 + number), 'y'), number)) FROM numbers(1000)"+limit, "1")
	te.expectRows("SELECT length(min(repeat(toString(9 - number % 10), 1000000 - number))) FROM numbers(500)"+limit, "999501")
}

// The groups' charges go as their rows are emitted, and the rest when the
// output closes.
func TestAggregationOutputReleasesEmittedGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("aggregates 40000 rows")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	ac := compileAggregation(t, q, []string{"a"}, []string{"count()", "groupArray(s)"})
	in := &freshRows{n: 40000, row: func(i int) []any { return []any{int64(i / 2), nil, fmt.Sprint("value-", i)} }}
	out, err := runAggregation(q, in, ac)
	if err != nil {
		t.Fatal(err)
	}
	built := q.mem.Used()
	if built < 20000*int64(stateBytes) {
		t.Fatalf("20000 groups charged %d", built)
	}
	for range 10000 {
		if _, err := out.next(); err != nil {
			t.Fatal(err)
		}
	}
	if half := q.mem.Used(); half > built/2+memtrack.Pointers(20000)+memtrack.Chunk {
		t.Errorf("half the groups out: %d of %d charged", half, built)
	}
	drainRows(t, out)
	// The order array stays until close.
	if rest := q.mem.Used(); rest > memtrack.Pointers(20000)+memtrack.Chunk {
		t.Errorf("all groups out: %d charged", rest)
	}
	out.close()
	if u := q.mem.Used(); u != 0 {
		t.Errorf("closed: %d charged", u)
	}
	q.finish()
}

// -State and -Merge over an AggregatingMergeTree give the results of the
// direct aggregates with the states charged, and fail at a small limit.
func TestAggregationOfStoredStatesUnderMemoryLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("merges stored states under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	te.exec(`CREATE TABLE st (k UInt64, u AggregateFunction(uniqExact, UInt64), q AggregateFunction(groupUniqArray, UInt64),
		g AggregateFunction(groupArray, UInt64), a AggregateFunction(argMax, String, UInt64), m AggregateFunction(min, String),
		s AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k`)
	for i := range 3 {
		te.exec(fmt.Sprintf(`INSERT INTO st SELECT number %% 10, uniqExactState(number), groupUniqArrayState(number %% 1000),
			groupArrayState(number), argMaxState(toString(number), number), minState(toString(number)), sumState(number)
			FROM numbers(%d, 20000) GROUP BY number %% 10`, i*20000))
	}
	want := te.rows(`SELECT number % 10 AS k, uniqExact(number), length(groupUniqArray(number % 1000)), length(groupArray(number)),
		argMax(toString(number), number), min(toString(number)), sum(number) FROM numbers(60000) GROUP BY k ORDER BY k`)
	const merge = `SELECT k, uniqExactMerge(u), length(groupUniqArrayMerge(q)), length(groupArrayMerge(g)), argMaxMerge(a),
		minMerge(m), sumMerge(s) FROM st GROUP BY k ORDER BY k`
	te.expectRows(merge+" SETTINGS max_memory_usage = 32000000", want)
	te.expectRows(`SELECT k, uniqExactMerge(u), length(groupArrayMerge(g)) FROM
		(SELECT k, uniqExactMergeState(u) AS u, groupArrayMergeState(g) AS g FROM st GROUP BY k) GROUP BY k ORDER BY k
		SETTINGS max_memory_usage = 32000000`,
		te.rows(`SELECT number % 10 AS k, uniqExact(number), length(groupArray(number)) FROM numbers(60000) GROUP BY k ORDER BY k`))
	wantMemoryLimit(t, te, "SELECT uniqExactMerge(u) FROM st SETTINGS max_memory_usage = 2000000", nil)
	wantMemoryLimit(t, te, "SELECT length(groupArrayMerge(g)) FROM st SETTINGS max_memory_usage = 2000000", nil)
}

// A GROUP BY of 2000000 groups, scaled down five times, fits a fifth of
// the default limit of a 2 GiB task: 0.6 × (0.9 × 2 GiB − 96 MiB).
func TestHighCardinalityGroupByFitsTheDefaultLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("aggregates 400000 groups")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	budget := 0.9 * 2 * float64(1<<30)
	limit := int64(0.6 * (budget - 96<<20) / 5)
	res, err := te.run(fmt.Sprintf("SELECT number %% 400000 AS k, count() FROM numbers(800000) GROUP BY k SETTINGS max_memory_usage = %d", limit))
	if err != nil {
		t.Fatalf("limit %d: %v", limit, err)
	}
	if len(res.Rows) != 400000 {
		t.Fatalf("%d groups", len(res.Rows))
	}
	for i, r := range res.Rows {
		if r[0] != uint64(i) || r[1] != uint64(2) {
			t.Fatalf("row %d: %v", i, r)
		}
	}
}

// A stored set state merged into a group is charged while it waits
// unloaded; once its members are loaded into the group, only they are.
func TestMergedSetStateIsChargedOnce(t *testing.T) {
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	spec, _ := parseAggName("uniqExactMerge")
	call, err := newAggCall(q.ctx, spec, "uniqExactMerge", []*expr{{t: chtype.MustParse("AggregateFunction(uniqExact, UInt64)")}})
	if err != nil {
		t.Fatal(err)
	}
	members := func(from int) []any {
		m := make([]any, 1000)
		for i := range m {
			m[i] = uint64(from + i)
		}
		return m
	}
	stored := func(from int) *chtype.AggPartial {
		p := aggstate.New("uniqExact", []chtype.Type{chtype.TUInt64})
		m := members(from)
		p.SetLoaded, p.SetSize, p.LoadSetBytes = false, uint64(len(m)), int32(memtrack.Value(m))
		p.LoadSet = func(context.Context) ([]any, error) { return m, nil }
		return p
	}
	merged := call.newAcc()
	merged.mem = q.mem.NewAccount()
	for _, from := range []int{0, 1000} {
		if err := call.mergeState(merged, stored(from)); err != nil {
			t.Fatal(err)
		}
	}
	built := call.newAcc()
	built.mem = q.mem.NewAccount()
	if err := built.addMembers(append(members(0), members(1000)...)); err != nil {
		t.Fatal(err)
	}
	if merged.bytes != built.bytes {
		t.Errorf("merged states charged %d, the same members added %d", merged.bytes, built.bytes)
	}
}
