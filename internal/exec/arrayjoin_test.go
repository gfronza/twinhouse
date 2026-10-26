package exec

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"reflect"
	"runtime"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// eagerExpand is the pre-streaming ajStage.expand, kept as the oracle: it
// unrolls one stage over every row of the previous stage's product.
func eagerExpand(st *ajStage, rows [][]any) ([][]any, error) {
	var out [][]any
	for _, r := range rows {
		arrays := make([][]any, len(st.items))
		n := -1
		for i, x := range st.items {
			v, err := x.eval(r)
			if err != nil {
				return nil, err
			}
			arr, _ := v.([]any)
			if n >= 0 && len(arr) != n {
				return nil, cherr.New(cherr.SizesOfArraysDontMatch, "Sizes of ARRAY-JOIN-ed arrays do not match")
			}
			n = len(arr)
			arrays[i] = arr
		}
		if n == 0 && st.left {
			nr := append([]any(nil), r...)
			for i, slot := range st.slots {
				nr[slot] = zeroValue(st.elems[i])
			}
			out = append(out, nr)
			continue
		}
		for k := 0; k < n; k++ {
			nr := append([]any(nil), r...)
			for i, slot := range st.slots {
				nr[slot] = arrays[i][k]
			}
			out = append(out, nr)
		}
	}
	return out, nil
}

// eagerArrayJoin is the pre-streaming arrayJoinIter drained: the rows it
// emitted before failing, and the error.
func eagerArrayJoin(stages []*ajStage, width int, input [][]any) ([][]any, error) {
	var out [][]any
	for _, row := range input {
		r := make([]any, width)
		copy(r, row)
		rows := [][]any{r}
		var err error
		for _, st := range stages {
			if rows, err = eagerExpand(st, rows); err != nil {
				return out, err
			}
		}
		out = append(out, rows...)
	}
	return out, nil
}

var ajInt = chtype.Type{Kind: chtype.Int64}

// ajCase is a random ARRAY JOIN: source rows of arrays and stages over them.
// dependent marks a stage that reads an earlier stage's element.
type ajCase struct {
	input     [][]any
	stages    []*ajStage
	width     int
	dependent bool
}

const ajSourceCols = 4

func randomArray(r *rand.Rand, n int) []any {
	if n < 0 {
		return nil
	}
	arr := make([]any, n)
	for i := range arr {
		arr[i] = int64(r.IntN(4))
	}
	return arr
}

// randomInput makes rows whose arrays usually share one length, so zipped
// items mostly match and sometimes fail with 190.
func randomInput(r *rand.Rand) [][]any {
	rows := make([][]any, r.IntN(5))
	for i := range rows {
		base := r.IntN(4)
		row := make([]any, ajSourceCols)
		for c := range row {
			n := base
			if r.IntN(6) == 0 {
				n = r.IntN(5) - 1 // -1 is a non-array value
			}
			row[c] = randomArray(r, n)
		}
		rows[i] = row
	}
	return rows
}

func columnItem(c int) *expr {
	return &expr{fn: func(row []any) (any, error) { return row[c], nil }}
}

// rangeItem is range(row[slot]): an array that depends on an earlier stage's
// element. It fails on 3 when failing is set.
func rangeItem(slot int, failing bool) *expr {
	return &expr{fn: func(row []any) (any, error) {
		n, _ := row[slot].(int64)
		if failing && n == 3 {
			return nil, cherr.New(cherr.IllegalTypeOfArgument, "element 3")
		}
		arr := make([]any, n)
		for i := range arr {
			arr[i] = int64(i)
		}
		return arr, nil
	}}
}

// randomStages mixes ARRAY JOIN clause stages (several zipped items, maybe
// LEFT) with arrayJoin() stages (one item, never LEFT). Items read source
// columns or earlier stages' elements.
func randomStages(r *rand.Rand) ajCase {
	c := ajCase{width: ajSourceCols, stages: make([]*ajStage, 1+r.IntN(3))}
	var earlier []int
	for i := range c.stages {
		st := &ajStage{}
		items := 1
		if r.IntN(2) == 0 {
			st.left = r.IntN(2) == 0
			items += r.IntN(3)
		}
		for range items {
			x := columnItem(r.IntN(ajSourceCols))
			if len(earlier) > 0 && r.IntN(2) == 0 {
				x = rangeItem(earlier[r.IntN(len(earlier))], r.IntN(8) == 0)
				c.dependent = true
			}
			st.items = append(st.items, x)
			st.slots = append(st.slots, c.width)
			st.elems = append(st.elems, ajInt)
			c.width++
		}
		earlier = append(earlier, st.slots...)
		c.stages[i] = st
	}
	return c
}

func randomCase(r *rand.Rand) ajCase {
	c := randomStages(r)
	c.input = randomInput(r)
	return c
}

func errCode(err error) int {
	if ce := cherr.As(err); ce != nil {
		return ce.Code
	}
	return -1
}

// checkAgainstEager drains the streaming iterator and compares it with the
// eager oracle. Each row is overwritten once checked, so a row shared with
// the iterator's working state would corrupt the rows after it. A failing
// case must fail at the same row with the same code: a stage's arrays are
// fixed for an input row, so its error comes before the row's first output.
// Only a dependent stage, which SQL cannot build today, may differ: the
// eager code evaluated it for every earlier combination first, so the stream
// can emit more rows and meet another failing combination.
func checkAgainstEager(t *testing.T, n int, c ajCase) {
	t.Helper()
	want, wantErr := eagerArrayJoin(c.stages, c.width, c.input)
	it := newArrayJoinIter(context.Background(), &sliceIter{rows: c.input}, c.stages, c.width)
	defer it.close()
	var got int
	for {
		row, err := it.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if wantErr == nil || (!c.dependent && errCode(err) != errCode(wantErr)) {
				t.Fatalf("case %d: error %v, want %v", n, err, wantErr)
			}
			if got < len(want) || (!c.dependent && got != len(want)) {
				t.Fatalf("case %d: failed after %d rows, eager emitted %d first", n, got, len(want))
			}
			return
		}
		if got >= len(want) {
			if wantErr == nil {
				t.Fatalf("case %d: extra row %v after %d rows", n, row, len(want))
			}
		} else if !reflect.DeepEqual(row, want[got]) {
			t.Fatalf("case %d: row %d = %v, want %v", n, got, row, want[got])
		}
		got++
		for i := range row {
			row[i] = "overwritten"
		}
	}
	if wantErr != nil {
		t.Fatalf("case %d: finished after %d rows, want error %v", n, got, wantErr)
	}
	if got != len(want) {
		t.Fatalf("case %d: %d rows, want %d", n, got, len(want))
	}
}

func TestArrayJoinMatchesEagerExpansion(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for n := range 5000 {
		checkAgainstEager(t, n, randomCase(r))
	}
}

func TestArrayJoinLeftEmptyEmitsDefaultRow(t *testing.T) {
	st := &ajStage{items: []*expr{columnItem(0), columnItem(1)}, slots: []int{2, 3}, elems: []chtype.Type{ajInt, ajInt}, left: true}
	c := ajCase{input: [][]any{{[]any{}, []any{}}, {[]any{int64(5)}, []any{int64(6)}}}, stages: []*ajStage{st}, width: 4}
	checkAgainstEager(t, 0, c)
	it := newArrayJoinIter(context.Background(), &sliceIter{rows: c.input}, c.stages, c.width)
	row, err := it.next()
	if err != nil || row[2] != int64(0) || row[3] != int64(0) {
		t.Fatalf("first row %v, %v; want defaults in the joined columns", row, err)
	}
}

func TestArrayJoinUnequalSizesFail(t *testing.T) {
	st := &ajStage{items: []*expr{columnItem(0), columnItem(1)}, slots: []int{2, 3}, elems: []chtype.Type{ajInt, ajInt}}
	input := [][]any{{[]any{int64(1), int64(2)}, []any{int64(3)}}}
	it := newArrayJoinIter(context.Background(), &sliceIter{rows: input}, []*ajStage{st}, 4)
	if _, err := it.next(); !cherr.Is(err, cherr.SizesOfArraysDontMatch) {
		t.Fatalf("got %v, want SIZES_OF_ARRAYS_DONT_MATCH", err)
	}
}

// repeatIter repeats one row n times.
type repeatIter struct {
	row []any
	n   int
}

func (e *repeatIter) next() ([]any, error) {
	if e.n == 0 {
		return nil, io.EOF
	}
	e.n--
	return e.row, nil
}
func (e *repeatIter) close() {}

func TestArrayJoinPollsContextAcrossEmptyArrays(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	const rows = 1 << 16
	st := &ajStage{items: []*expr{columnItem(0)}, slots: []int{1}, elems: []chtype.Type{ajInt}}
	it := newArrayJoinIter(ctx, &repeatIter{row: []any{[]any{}}, n: rows}, []*ajStage{st}, 2)
	_, err := it.next()
	if !cherr.Is(err, cherr.Timeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("no cancellation within %d empty rows: got %v", rows, err)
	}
}

func TestArrayJoinStreams(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got := te.rows(`SELECT arrayJoin(range(3000)) AS a, arrayJoin(range(3001)) AS b LIMIT 1`)
	runtime.ReadMemStats(&after)
	if got != "0\t0\n" {
		t.Fatalf("got %q, want 0\\t0", got)
	}
	if d := after.TotalAlloc - before.TotalAlloc; d > 10<<20 {
		t.Fatalf("allocated %d bytes for one row; the product was materialized", d)
	}
}

// TestArrayJoinQueryResults pins results, row order included, that HEAD
// produced before the iterator streamed.
func TestArrayJoinQueryResults(t *testing.T) {
	te := newTestEnv(t)
	for _, c := range []struct{ sql, want string }{
		{`SELECT k, a FROM (SELECT arrayJoin([0,1,2,3]) AS k, range(k) AS arr) ARRAY JOIN arr AS a`,
			"1\t0\n2\t0\n2\t1\n3\t0\n3\t1\n3\t2\n"},
		{`SELECT k, a FROM (SELECT arrayJoin([0,1,2]) AS k, range(k) AS arr) LEFT ARRAY JOIN arr AS a`,
			"0\t0\n1\t0\n2\t0\n2\t1\n"},
		{`SELECT a, b, arrayJoin(['x','y']) AS s, arrayJoin([10,20,30]) AS u FROM (SELECT [[1],[],[2,3]] AS arr, ['p','q','r'] AS brr) LEFT ARRAY JOIN arr AS a, brr AS b`,
			"[1]\tp\tx\t10\n[1]\tp\tx\t20\n[1]\tp\tx\t30\n[1]\tp\ty\t10\n[1]\tp\ty\t20\n[1]\tp\ty\t30\n" +
				"[]\tq\tx\t10\n[]\tq\tx\t20\n[]\tq\tx\t30\n[]\tq\ty\t10\n[]\tq\ty\t20\n[]\tq\ty\t30\n" +
				"[2,3]\tr\tx\t10\n[2,3]\tr\tx\t20\n[2,3]\tr\tx\t30\n[2,3]\tr\ty\t10\n[2,3]\tr\ty\t20\n[2,3]\tr\ty\t30\n"},
		{`SELECT arrayJoin([1,2]) AS x, arrayJoin([1,2]) AS y, arrayJoin([3,4]) AS z`,
			"1\t1\t3\n1\t1\t4\n2\t2\t3\n2\t2\t4\n"},
		{`SELECT n, a FROM (SELECT number AS n, range(number % 4) AS arr FROM numbers(8)) ARRAY JOIN arr AS a WHERE a % 2 = 0 LIMIT 5`,
			"1\t0\n2\t0\n3\t0\n3\t2\n5\t0\n"},
		{`SELECT n, a, b FROM (SELECT number AS n, range(number % 3) AS arr, arrayReverse(range(number % 3)) AS s FROM numbers(6)) LEFT ARRAY JOIN arr AS a, s AS b`,
			"0\t0\t0\n1\t0\t0\n2\t0\t1\n2\t1\t0\n3\t0\t0\n4\t0\t0\n5\t0\t1\n5\t1\t0\n"},
		{`SELECT count(), sum(a), sum(b) FROM (SELECT range(number % 50) AS arr FROM numbers(200)) ARRAY JOIN arr AS a, arrayReverse(arr) AS b`,
			"4900\t78400\t78400\n"},
		{`SELECT arr, a FROM (SELECT [] :: Array(String) AS arr) LEFT ARRAY JOIN arr AS a`,
			"[]\t\n"},
	} {
		te.expectRows(c.sql, c.want)
	}
	te.expectErr(`SELECT a, b FROM (SELECT [1,2] AS x, [3] AS y) ARRAY JOIN x AS a, y AS b`, cherr.SizesOfArraysDontMatch)
	te.expectErr(`SELECT n, a FROM (SELECT number AS n, range(number) AS arr FROM numbers(3)) ARRAY JOIN arr AS a, range(2) AS b`, cherr.SizesOfArraysDontMatch)
}

func BenchmarkArrayJoin(b *testing.B) {
	te := newTestEnv(b)
	te.exec(`CREATE TABLE aj (k UInt64, arr Array(UInt32)) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO aj SELECT number, range(number % 16) FROM numbers(4000)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		te.exec(`SELECT k, a, b FROM aj ARRAY JOIN arr AS a, arrayReverse(arr) AS b`)
	}
	b.ReportMetric(float64(b.N*4000)/b.Elapsed().Seconds(), "rows/s")
}
