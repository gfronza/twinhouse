//go:build !race

package exec

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// A piece of a long string (substring, left, trim, a splitByChar part, a
// replaceRegexpOne that drops a prefix or a suffix) or a lowerUTF8 or
// upperUTF8 that shrinks or grows it does not keep the long string or
// its buffer alive: a holder of the pieces stays within its charge.
func TestStringPiecesDoNotHoldTheirSource(t *testing.T) {
	if testing.Short() {
		t.Skip("builds strings of 1 MB under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const big = "concat(repeat('x', 1000000), toString(number))"
	for _, sql := range []string{
		"SELECT count() FROM (SELECT substring(" + big + ", 1, 20000) AS s FROM numbers(120) ORDER BY s)",
		"SELECT count() FROM (SELECT left(" + big + ", 20000) AS s FROM numbers(120) ORDER BY s LIMIT 1000)",
		"SELECT length(groupArray(right(" + big + ", 20000))) FROM numbers(120)",
		"SELECT count() FROM numbers(3) AS l JOIN (SELECT number AS k, trimLeft(concat(repeat(' ', 1000000), repeat('x', 20000), toString(number))) AS s " +
			"FROM numbers(120)) AS r ON l.number = r.k",
		"SELECT count() FROM (SELECT splitByChar(',', concat(repeat('x', 20000), ',', " + big + "))[1] AS s FROM numbers(120) ORDER BY s)",
		"SELECT substring(" + big + ", 1, 20000) FROM numbers(120)",
	} {
		if err := wantPeakWithinLimit(t, te, sql, 32000000); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
	// These pieces are charged too little for the sample hook to fire.
	for _, piece := range []string{
		"replaceRegexpOne(" + big + ", '^x+', '')",
		"replaceRegexpOne(concat(toString(number), repeat('x', 1000000)), 'x+$', '')",
	} {
		for _, sql := range heldShapes(piece, 200) {
			if err := wantLiveWithinLimit(t, te, sql, 32000000); err != nil {
				t.Errorf("%s: %v", sql, err)
			}
		}
	}
	// Case maps that shrink a string (KELVIN SIGN to 'k', 3 B to 1, and
	// 'ı' to 'I', 2 B to 1) are charged a third or a half of their buffer
	// if it is kept, and one that grows it ('ȿ' to 'Ȿ', 2 B to 3) about
	// two thirds of its grown buffer. They may fill the limit, but stay
	// within it.
	for _, c := range []struct {
		piece string
		rows  int
	}{
		{"lowerUTF8(concat(repeat('\\xE2\\x84\\xAA', 333333), toString(number)))", 80},
		{"upperUTF8(concat(repeat('ı', 500000), toString(number)))", 60},
		{"upperUTF8(concat(repeat('ȿ', 40), toString(number)))", 300000},
	} {
		for _, sql := range heldShapes(c.piece, c.rows) {
			_ = wantLiveWithinLimit(t, te, sql, 32000000)
		}
	}
	// Long pieces, pieces cut from pieces (nested, or through subquery
	// aliases) and 2 B pieces of 52 B sources may fill the limit (the ORDER
	// BY evaluates its alias twice), but stay within it.
	const chain = "right(right(right(" + big + ", 800002), 640002), 512002)"
	for _, sql := range []string{
		"SELECT length(groupArray(right(" + big + ", 500001))) FROM numbers(56)",
		"SELECT substring(" + big + ", 1, 500001) FROM numbers(56)",
		"SELECT count() FROM (SELECT substring(" + big + ", 1, 500001) AS s FROM numbers(56) ORDER BY s)",
		"SELECT count() FROM (SELECT trimRight(concat(" + big + ", repeat(' ', 999999))) AS s FROM numbers(56) ORDER BY s)",
		"SELECT length(groupArray(" + chain + ")) FROM numbers(200)",
		"SELECT count() FROM (SELECT " + chain + " AS s, number FROM numbers(200) ORDER BY number DESC LIMIT 150)",
		"SELECT length(groupArray(s3)) FROM (SELECT right(s2, 512002) AS s3 FROM (SELECT right(s1, 640002) AS s2 FROM " +
			"(SELECT right(" + big + ", 800002) AS s1 FROM numbers(200))))",
		"SELECT length(groupArray(substring(concat(repeat('x', 50), toString(number)), 1, 2))) FROM numbers(3000000)",
	} {
		_ = wantPeakWithinLimit(t, te, sql, 32000000)
	}
}

// heldShapes returns queries that hold piece for rows rows: in a
// groupArray, as a collected result and in an ORDER BY.
func heldShapes(piece string, rows int) []string {
	from := fmt.Sprintf(" FROM numbers(%d)", rows)
	return []string{
		"SELECT length(groupArray(" + piece + "))" + from,
		"SELECT " + piece + from,
		"SELECT count() FROM (SELECT " + piece + " AS s" + from + " ORDER BY s)",
	}
}

// A FixedString cast or converted to String does not keep its zero
// padding alive: the result held stays within the limit.
func TestFixedStringToStringDoesNotHoldItsPadding(t *testing.T) {
	if testing.Short() {
		t.Skip("builds 1000 FixedStrings of 100 KB under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, sql := range []string{
		"SELECT CAST(toFixedString(toString(number), 100000) AS String) FROM numbers(1000)",
		"SELECT toString(toFixedString(toString(number), 100000)) FROM numbers(1000)",
		"SELECT groupArray(CAST(toFixedString(toString(number), 100000) AS String)) FROM numbers(1000)",
		"SELECT arrayStringConcat([toFixedString(toString(number), 100000)]) FROM numbers(1000)",
	} {
		if err := wantLiveWithinLimit(t, te, sql, 32000000); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
	}
}

// wantLiveWithinLimit runs sql under limit, sampling the live heap every
// 20 ms while it runs and once more while its result is held, and fails
// t if it grew past the limit + 8 MiB. Unlike wantPeakWithinLimit it
// needs no charge to sample. A 241 is allowed and returned.
func wantLiveWithinLimit(t *testing.T, te *testEnv, sql string, limit int64) error {
	t.Helper()
	base := liveHeapBytes()
	done, sampled := make(chan struct{}), make(chan int64)
	go func() {
		var peak int64
		for {
			peak = max(peak, liveHeapBytes()-base)
			select {
			case <-done:
				sampled <- peak
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	res, err := te.runReq(&Request{Query: fmt.Sprintf("%s SETTINGS max_memory_usage = %d", sql, limit)})
	close(done)
	peak := max(<-sampled, liveHeapBytes()-base)
	runtime.KeepAlive(res)
	t.Logf("live heap grew by %.1f MiB (%v): %s", float64(peak)/(1<<20), err != nil, sql)
	if err != nil && !cherr.Is(err, cherr.MemoryLimitExceeded) {
		t.Errorf("%s: %v", sql, err)
	}
	if peak > limit+8<<20 {
		t.Errorf("%s: live heap grew by %d B, over the limit of %d B + 8 MiB", sql, peak, limit)
	}
	return err
}
