package exec

import (
	"context"
	"io"
	"math"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// stableSortOracle is sortRows before top-N: every row is held and sorted
// with sort.SliceStable. It is the reference the top-N heap must match.
func stableSortOracle(q *query, it rowIter, width int, items []sqlparse.OrderItem) ([][]any, error) {
	var rows [][]any
	for {
		row, err := it.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		if len(rows)&1023 == 0 {
			if err := q.ctx.Err(); err != nil {
				return nil, cherr.Wrap(cherr.Timeout, err, "query cancelled")
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for k, o := range items {
			nullsFirst := o.NullsFirst != nil && *o.NullsFirst
			if c := compareOrder(rows[i][width+k], rows[j][width+k], o.Desc, nullsFirst); c != 0 {
				return c < 0
			}
		}
		return false
	})
	return rows, nil
}

// randomOrderValue draws from a small domain of one kind so that ties,
// NULL, NaN and infinities are common.
func randomOrderValue(r *rand.Rand, kind int) any {
	if r.Intn(6) == 0 {
		return nil
	}
	switch kind {
	case 0:
		return int64(r.Intn(5) - 2)
	case 1:
		return []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, -0.5, 1.5}[r.Intn(6)]
	case 2:
		return []string{"", "a", "ab", "b", "B"}[r.Intn(5)]
	default:
		n := r.Intn(3)
		a := make([]any, n)
		for i := range a {
			a[i] = randomOrderValue(r, 1)
		}
		return a
	}
}

func randomOrderItems(r *rand.Rand, n int) []sqlparse.OrderItem {
	items := make([]sqlparse.OrderItem, n)
	for i := range items {
		items[i].Desc = r.Intn(2) == 0
		if c := r.Intn(3); c > 0 {
			first := c == 1
			items[i].NullsFirst = &first
		}
	}
	return items
}

// rowIDs lists the first column of each row (the row's arrival number).
func rowIDs(rows [][]any) []any {
	ids := make([]any, len(rows))
	for i, row := range rows {
		ids[i] = row[0]
	}
	return ids
}

// Inputs come shuffled, presorted, reversed and presorted with a shuffled
// tail, so every way the heap forms and fills is checked against the oracle.
func TestTopNMatchesStableSort(t *testing.T) {
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 3000; iter++ {
		nkeys := 1 + r.Intn(3)
		kinds := make([]int, nkeys)
		for i := range kinds {
			kinds[i] = r.Intn(4)
		}
		n := r.Intn(60)
		rows := make([][]any, n)
		for i := range rows {
			for _, k := range kinds {
				rows[i] = append(rows[i], randomOrderValue(r, k))
			}
		}
		items := randomOrderItems(r, nkeys)
		sorted, err := stableSortOracle(q, &sliceIter{rows: slices.Clone(rows)}, 0, items)
		if err != nil {
			t.Fatal(err)
		}
		reversed := slices.Clone(sorted)
		slices.Reverse(reversed)
		tail := slices.Clone(sorted)
		cut := r.Intn(n + 1)
		r.Shuffle(n-cut, func(i, j int) { tail[cut+i], tail[cut+j] = tail[cut+j], tail[cut+i] })
		for _, input := range [][][]any{rows, sorted, reversed, tail} {
			numbered := make([][]any, n)
			for i, row := range input {
				numbered[i] = append([]any{i}, row...)
			}
			want, err := stableSortOracle(q, &sliceIter{rows: slices.Clone(numbered)}, 1, items)
			if err != nil {
				t.Fatal(err)
			}
			for _, keep := range []uint64{0, 1, uint64(r.Intn(n + 1)), uint64(n), uint64(n + 1 + r.Intn(5)), keepAll} {
				got, total, err := sortRows(q, &sliceIter{rows: slices.Clone(numbered)}, 1, items, keep)
				if err != nil {
					t.Fatal(err)
				}
				wantKept := want[:min(keep, uint64(n))]
				if total != uint64(n) || !slices.Equal(rowIDs(got), rowIDs(wantKept)) {
					t.Fatalf("keep %d of %v by %+v:\n got %v (total %d)\nwant %v (total %d)",
						keep, numbered, items, rowIDs(got), total, rowIDs(wantKept), n)
				}
			}
		}
	}
}
