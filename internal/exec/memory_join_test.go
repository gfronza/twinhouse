package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/format"
)

// TestJoinBuildIsReleased checks that a join build is charged while it
// lives and released when the join closes, or when it fails.
func TestJoinBuildIsReleased(t *testing.T) {
	if testing.Short() {
		t.Skip("builds joins under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, c := range []struct {
		sql  string
		code int
	}{
		{sql: "SELECT count() FROM numbers(10) AS a JOIN numbers(200000) AS b ON a.number = b.number SETTINGS max_memory_usage = 64000000"},
		{sql: "SELECT count() FROM numbers(10) AS a JOIN numbers(200000) AS b ON a.number = b.number SETTINGS max_memory_usage = 4000000",
			code: cherr.MemoryLimitExceeded},
		// The right side fails after 150000 rows are built.
		{sql: "SELECT count() FROM numbers(10) AS a JOIN (SELECT number AS k, intDiv(1, toInt64(number) - 150000) AS x FROM numbers(200000)) AS b " +
			"ON a.number = b.k SETTINGS max_memory_usage = 64000000", code: cherr.IllegalDivision},
	} {
		wantNoTrackedMemory(t, te, context.Background(), c.sql, c.code)
	}
}

// TestJoinBuildIsReleasedWhenTheLeftSideFails checks the build's release
// when the left side cannot be opened after it.
func TestJoinBuildIsReleasedWhenTheLeftSideFails(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a join of 10000 rows")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	var rows [][]any
	for i := range 10000 {
		rows = append(rows, []any{uint64(i)})
	}
	spec := &joinSpec{kind: "CROSS", leftW: 1, rightW: 1, rightCols: []format.Column{{Type: chtype.TUInt64}}}
	fail := errors.New("left side failed")
	if _, err := startHashJoin(q, spec, &sliceIter{rows: rows}, func() (rowIter, error) { return nil, fail }); err != fail {
		t.Fatalf("got %v", err)
	}
	if u := q.mem.Used(); u != 0 {
		t.Errorf("holding %d tracked bytes", u)
	}
}
