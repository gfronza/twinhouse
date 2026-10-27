package exec

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

var sizerCols = []format.Column{
	{Type: chtype.TUInt64}, {Type: chtype.TString}, {Type: chtype.TDateTime},
	{Type: chtype.NewArray(chtype.TString)}, {Type: chtype.NewNullable(chtype.TFloat64)},
}

// heldRows keeps test rows on the heap, as a holder keeps engine rows: the
// sizer tells values apart by address, and values that never escape could
// be boxed at the same stack address in every iteration.
var heldRows [][]any

func TestRowSizerMatchesRowForDistinctValues(t *testing.T) {
	s := newRowSizer(sizerCols)
	for i := 0; i < 100; i++ {
		r := []any{uint64(1000 + i), strings.Repeat("s", i+1), time.Unix(int64(i), 0).UTC(),
			[]any{strconv.Itoa(i), "x"}, float64(i)}
		heldRows = append(heldRows, r)
		if got, want := s.size(r), memtrack.Row(r); got != want {
			t.Fatalf("row %d: size %d, memtrack.Row %d", i, got, want)
		}
	}
}

func TestRowSizerChargesNullAsItsType(t *testing.T) {
	s := newRowSizer(sizerCols)
	r := []any{uint64(1000), "a", time.Unix(0, 0).UTC(), []any{}, nil}
	if got, want := s.size(r), memtrack.Row(r)+memtrack.Value(0.5); got != want {
		t.Fatalf("size %d, want %d", got, want)
	}
}

func TestRowSizerChargesASharedValueOnce(t *testing.T) {
	s := newRowSizer(sizerCols)
	big := any(strings.Repeat("x", 1<<20))
	arr := any([]any{"a", "b"})
	first := s.size([]any{uint64(1000), big, time.Unix(0, 0).UTC(), arr, 0.5})
	if first < 1<<20 {
		t.Fatalf("first row %d bytes", first)
	}
	if again := s.size([]any{uint64(1001), big, time.Unix(1, 0).UTC(), arr, 1.5}); again != memtrack.Slice(5)+16+24+16 {
		t.Fatalf("row sharing its string and array: %d bytes", again)
	}
}

// A function that returns its String argument boxes it anew: the bytes are
// a recent row's, the box is new.
func TestRowSizerChargesANewBoxOverSharedBytesItsHeader(t *testing.T) {
	s := newRowSizer([]format.Column{{Type: chtype.TString}})
	big := strings.Repeat("x", 1<<20)
	heldRows = append(heldRows, []any{big}, []any{big})
	s.size(heldRows[len(heldRows)-2])
	if got := s.size(heldRows[len(heldRows)-1]); got != memtrack.Slice(1)+memtrack.StringHeader {
		t.Fatalf("a new box over the same bytes: %d bytes", got)
	}
	if got := s.size(heldRows[len(heldRows)-1]); got != memtrack.Slice(1) {
		t.Fatalf("the same box again: %d bytes", got)
	}
	if got := s.size([]any{big[1:]}); got < 1<<20 {
		t.Fatalf("other bytes: %d", got)
	}
}

// if and multiIf return one of a few constants: each is charged once.
func TestRowSizerChargesAFewAlternatingValuesOnce(t *testing.T) {
	s := newRowSizer([]format.Column{{Type: chtype.TString}, {Type: chtype.NewArray(chtype.TUInt64)}})
	strs := []any{strings.Repeat("a", 1000), strings.Repeat("b", 1000), strings.Repeat("c", 1000), strings.Repeat("d", 1000)}
	arrs := []any{[]any{uint64(1000)}, []any{uint64(2000)}}
	first := int64(0)
	for i := 0; i < 4; i++ {
		first += s.size([]any{strs[i], arrs[i%2]})
	}
	if want := 4*memtrack.Slice(2) + 4*memtrack.Value(strs[0]) + 2*memtrack.Value(arrs[0]); first != want {
		t.Fatalf("first four rows: %d bytes, want %d", first, want)
	}
	for i := 0; i < 100; i++ {
		if got := s.size([]any{strs[i%4], arrs[i%2]}); got != memtrack.Slice(2) {
			t.Fatalf("row %d repeating a recent value: %d bytes", i, got)
		}
	}
	if got := s.size([]any{strings.Repeat("e", 1000), arrs[0]}); got != memtrack.Slice(2)+memtrack.Value(strs[0]) {
		t.Fatalf("a fifth string: %d bytes", got)
	}
}

func TestRowSizerChargesValuesAgainAfterForget(t *testing.T) {
	s := newRowSizer(sizerCols)
	big, arr := any(strings.Repeat("x", 1<<20)), any([]any{"a"})
	r := []any{uint64(1000), big, time.Unix(0, 0).UTC(), arr, 0.5}
	first := s.size(r)
	s.forget()
	if again := s.size(r); again != first {
		t.Fatalf("after forget: %d bytes, first time %d", again, first)
	}
}

func TestRowSizerUsesTheRowCapacity(t *testing.T) {
	s := newRowSizer([]format.Column{{Type: chtype.TUInt64}})
	r := make([]any, 1, 40)
	r[0] = uint64(5000)
	if got, want := s.size(r), memtrack.Row(r); got != want {
		t.Fatalf("size %d, want %d", got, want)
	}
}

// ARRAY JOIN copies the source row into each output row, so the 1 MB
// string is held once, not 1000 times.
func TestMemoryLimitChargesValuesSharedByRowsOnce(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT count() FROM (SELECT s, x FROM (SELECT repeat('a', 1000000) AS s) ARRAY JOIN range(1000) AS x)`, "1000")
	res, err := te.run(`SELECT length(s) AS n, x FROM (SELECT repeat('a', 1000000) AS s) ARRAY JOIN range(3) AS x`)
	if err != nil || len(res.Rows) != 3 {
		t.Fatal(res, err)
	}
	if _, err := te.run(`SELECT s, x FROM (SELECT repeat('a', 1000000) AS s) ARRAY JOIN range(1000) AS x SETTINGS max_memory_usage = 20000000`); err != nil {
		t.Fatal(err)
	}
}

// While append copies the rows' headers, the old array and the new one are
// both live: the new one is charged before the copy, the old one released
// after.
func TestRowChargeChargesTheNewHeaderArrayBeforeTheCopy(t *testing.T) {
	// full is the first row count of at least 100000 that fills the
	// header array append grows.
	var probe [][]any
	for len(probe) < 100_000 || len(probe) < cap(probe) {
		probe = append(probe, nil)
	}
	full := len(probe)
	src := make([][]any, full+1)
	for i := range src {
		src[i] = []any{uint64(5000)}
	}
	// collect charges the first n rows under limit, as a query collects.
	collect := func(n int, limit int64) (used int64, rowsCap int, err error) {
		tr := memtrack.New(limit)
		rel := &relation{cols: []format.Column{{Type: chtype.TUInt64}},
			open: func() (rowIter, error) { return &sliceIter{rows: src[:n]}, nil }}
		ch := newRowCharge(tr.NewAccount(), rel.cols)
		defer ch.acct.Close()
		rows, err := rel.collectFirst(&query{ctx: context.Background()}, ch, 0, 0)
		return tr.Used(), cap(rows), err
	}
	held, old, err := collect(full, 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	used, grown, err := collect(full+1, 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	// Once the old array is released, the rows hold their new headers.
	settled := held + sliceHeader*int64(grown-old)
	if used < settled-memtrack.Chunk || used > settled+memtrack.Chunk {
		t.Errorf("after growing %d headers to %d: used %d, want about %d", old, grown, used, settled)
	}
	// Charging only the settled bytes would never pass this limit; both
	// arrays at once do.
	_, _, err = collect(full+1, settled+2*memtrack.Chunk)
	if ce := cherr.As(err); ce == nil || ce.Code != cherr.MemoryLimitExceeded {
		t.Errorf("limit just above the settled rows: %v, want MEMORY_LIMIT_EXCEEDED", err)
	}
}
