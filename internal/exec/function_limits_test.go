package exec

import (
	"fmt"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/limits"
)

// guardFunctionTest makes a regression fail fast instead of exhausting the
// machine: the stack is capped and the process exits once the heap passes
// 1 GiB.
func guardFunctionTest(t *testing.T) {
	t.Helper()
	name := t.Name()
	oldStack := debug.SetMaxStack(64 << 20)
	done := make(chan struct{})
	go func() {
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				metrics.Read(s)
				if s[0].Value.Uint64() > 1<<30 {
					fmt.Fprintf(os.Stderr, "%s: heap passed 1 GiB\n", name)
					os.Exit(3)
				}
			}
		}
	}()
	t.Cleanup(func() {
		close(done)
		debug.SetMaxStack(oldStack)
	})
}

func (c *regexpCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

func (c *regexpCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m, c.insts = nil, 0
}

func TestRangeCountsWithoutOverflow(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, c := range []struct{ sql, want string }{
		{"SELECT range(2, 9223372036854775807, 9223372036854775806)", "[2]"},
		{"SELECT range(-9223372036854775808, toInt64(9223372036854775807), toInt64(9223372036854775807))",
			"[-9223372036854775808,-1,9223372036854775806]"},
		{"SELECT range(toUInt64(9223372036854775806), toUInt64(9223372036854775809))",
			"[9223372036854775806,9223372036854775807,9223372036854775808]"},
		{"SELECT range(toUInt64(18446744073709551613), toUInt64(18446744073709551615))",
			"[18446744073709551613,18446744073709551614]"},
		{"SELECT range(toUInt64(1), toUInt64(18446744073709551615), toUInt64(9223372036854775808))",
			"[1,9223372036854775809]"},
		{"SELECT range(3), range(5, 1), range(-3, 3, 2), range(1, 7, 3)", "[0,1,2]\t[]\t[-3,-1,1]\t[1,4]"},
		{"SELECT range(toInt8(-2), toInt8(2)), toTypeName(range(toInt8(-2), toInt8(2)))", "[-2,-1,0,1]\tArray(Int8)"},
		{"SELECT range(toUInt8(250), toUInt8(255), toUInt8(2))", "[250,252,254]"},
		{"SELECT range(toInt8(-5), toUInt8(3), toUInt8(4)), toTypeName(range(toInt8(-5), toUInt8(3)))", "[-5,-1]\tArray(Int16)"},
		{"SELECT range(number) FROM numbers(4)", "[]\n[0]\n[0,1]\n[0,1,2]"},
		{"SELECT length(range(" + strconv.Itoa(limits.MaxRangeElements) + "))", strconv.Itoa(limits.MaxRangeElements)},
	} {
		te.expectRows(c.sql, c.want)
	}
}

func TestRangeRejectsTooManyElements(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, sql := range []string{
		"SELECT range(" + strconv.Itoa(limits.MaxRangeElements+1) + ")",
		"SELECT range(toUInt64(18446744073709551615))",
		"SELECT range(-9223372036854775808, toInt64(9223372036854775807))",
		"SELECT range(0, 9223372036854775807, 2)",
		"SELECT range(number * 1000000000) FROM numbers(3)",
	} {
		te.expectErr(sql, cherr.ArgumentOutOfBound)
	}
	te.expectErr("SELECT range(1, 10, 0)", cherr.BadArguments)
	te.expectErr("SELECT range(1, 10, -1)", cherr.BadArguments)
}

func TestRepeatRejectsTooLargeResult(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, sql := range []string{
		"SELECT repeat('abcd', 4611686018427387904)",
		"SELECT repeat('x', " + strconv.Itoa(limits.MaxRepeatTimes+1) + ")",
		"SELECT repeat('x', toUInt64(18446744073709551615))",
		"SELECT length(repeat(repeat('x', 70), 1000000))",
		"SELECT length(repeat(r, 2)) FROM (SELECT repeat(repeat('x', 40), 1000000) AS r)",
	} {
		te.expectErr(sql, cherr.TooLargeStringSize)
	}
	te.expectRows("SELECT repeat('ab', 3), repeat('x', -1), repeat('x', 0), length(repeat('ab', 1000000))", "ababab\t\t\t2000000")
}

func TestToFixedStringRejectsTooLargeSize(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	te.expectErr("SELECT toFixedString('', "+strconv.Itoa(limits.MaxFixedStringSize+1)+")", cherr.ArgumentOutOfBound)
	te.expectErr("SELECT toFixedString(toString(number), "+strconv.Itoa(limits.MaxFixedStringSize+1)+") FROM numbers(0)",
		cherr.ArgumentOutOfBound)
	te.expectErr("SELECT toFixedString('', toUInt64(18446744073709551615))", cherr.ArgumentOutOfBound)
	te.expectErr("SELECT toFixedString('', 0)", cherr.IllegalColumn)
	te.expectRows("SELECT toFixedString('ab', 3) = 'ab\\0', length(toFixedString('a', "+strconv.Itoa(limits.MaxFixedStringSize)+"))",
		"1\t"+strconv.Itoa(limits.MaxFixedStringSize))
}

func TestRegexpCacheStaysBounded(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	n := limits.MaxRegexpCacheEntries * 3
	for _, sql := range []string{
		"SELECT count() FROM numbers(%d) WHERE toString(number) LIKE concat('%%', toString(number))",
		"SELECT count() FROM numbers(%d) WHERE NOT toString(number) NOT LIKE concat('%%', toString(number))",
		"SELECT count() FROM numbers(%d) WHERE toString(number) ILIKE concat('%%', toString(number))",
		"SELECT count() FROM numbers(%d) WHERE match(toString(number), concat('^', toString(number), '$'))",
		"SELECT count() FROM numbers(%d) WHERE replaceRegexpAll(toString(number), toString(number), 'x') = 'x'",
		"SELECT count() FROM numbers(%d) WHERE replaceRegexpOne(toString(number), toString(number), 'x') = 'x'",
	} {
		regexps.clear()
		sql = fmt.Sprintf(sql, n)
		te.expectRows(sql, strconv.Itoa(n))
		if got := regexps.len(); got == 0 || got > limits.MaxRegexpCacheEntries {
			t.Errorf("%s: regexp cache holds %d entries, want 1..%d", sql, got, limits.MaxRegexpCacheEntries)
		}
	}
}

func TestRegexpCacheSkipsLongPatterns(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	regexps.clear()
	// Each of these compiles to a few instructions, however long it is.
	te.expectRows("SELECT count() FROM numbers(32) WHERE match('x', concat('[', repeat('a', 100000), 'b', toString(number), ']'))", "0")
	te.expectRows("SELECT count() FROM numbers(32) WHERE match('x', concat(repeat('(?i)', 10000), toString(number)))", "0")
	te.expectRows("SELECT count() FROM numbers(4) WHERE 'x' LIKE concat(repeat('a', 50000), toString(number))", "0")
	te.expectRows("SELECT count() FROM numbers(4) WHERE match(toString(number), concat('^', toString(number)))", "4")
	if got := regexps.len(); got != 4 {
		t.Errorf("regexp cache holds %d entries, want only the 4 short patterns", got)
	}
}

func TestRegexpCacheKeepsLargePrograms(t *testing.T) {
	te := newTestEnv(t)
	regexps.clear()
	te.expectRows("SELECT count() FROM numbers(200) WHERE match(toString(number), concat('^\\\\w{1,600}', toString(number % 2)))", "46")
	if got := regexps.len(); got != 2 {
		t.Errorf("regexp cache holds %d entries, want the 2 distinct patterns", got)
	}
}

func TestRegexpCacheStaysUnderInstructionBudget(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	regexps.clear()
	// 40 patterns of about 50k instructions each pass the budget twice.
	te.expectRows("SELECT count() FROM numbers(40) WHERE match('x', concat(repeat('[ab]{1000}', 50), toString(number)))", "0")
	regexps.mu.RLock()
	n, insts := len(regexps.m), regexps.insts
	regexps.mu.RUnlock()
	if n == 0 || n >= 40 || insts > maxCachedRegexpInsts {
		t.Errorf("regexp cache holds %d entries of %d instructions, want 1..39 within %d", n, insts, maxCachedRegexpInsts)
	}
}

func TestRegexpRejectsTooLargePattern(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	regexps.clear()
	te.expectErr("SELECT match('x', concat(repeat('a{1000}', 3000), '0'))", cherr.CannotCompileRegexp)
	te.expectErr("SELECT match(toString(number), concat(repeat('a{1000}', 3000), toString(number))) FROM numbers(2)", cherr.CannotCompileRegexp)
	te.expectErr("SELECT 'x' LIKE concat('%', repeat('b', 1000000), '%x_')", cherr.CannotCompileRegexp)
	te.expectErr("SELECT replaceRegexpAll('x', repeat('a{1000}', 3000), 'y')", cherr.BadArguments)
	te.expectErr("SELECT replaceRegexpOne(toString(number), concat(repeat('a{1000}', 3000), toString(number)), 'y') FROM numbers(2)", cherr.BadArguments)
	te.expectRows("SELECT match('x', repeat('a{1000}', 100))", "0")
	if got := regexps.len(); got != 0 {
		t.Errorf("rejected patterns left %d entries in the regexp cache", got)
	}
}

func TestRegexpAcceptsOrdinaryPatterns(t *testing.T) {
	te := newTestEnv(t)
	words := make([]string, 300)
	for i := range words {
		words[i] = "keyword" + strconv.Itoa(i)
	}
	te.expectRows("SELECT match('keyword299', '^(?i)("+strings.Join(words, "|")+")$'), match('2026-10-26T10:00:00.5Z', "+
		"'^\\\\d{4}-\\\\d{2}-\\\\d{2}[ T]\\\\d{2}:\\\\d{2}:\\\\d{2}(\\\\.\\\\d+)?(Z|[+-]\\\\d{2}:\\\\d{2})?$'), "+
		"match('10.0.0.1 - - [26/Oct/2026:10:00:00 +0000] \"GET /x HTTP/1.1\" 200 512', "+
		"'^(\\\\d{1,3}(?:\\\\.\\\\d{1,3}){3}) \\\\S+ \\\\S+ \\\\[([^\\\\]]+)\\\\] \"([A-Z]+) ([^ \"]+) HTTP/[0-9.]+\" (\\\\d{3}) (\\\\d+|-)$'), "+
		"match(repeat('a', 600), '^\\\\w{1,600}$'), 'x' LIKE concat('%', repeat('b', 50000), '%x_')", "1\t1\t1\t1\t0")
}

func TestRegexpCacheConcurrentUse(t *testing.T) {
	regexps.clear()
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 2 * limits.MaxRegexpCacheEntries {
				p := "^" + strconv.Itoa((i*7+g)%1500) + "$"
				re, err := regexps.get(p, cherr.CannotCompileRegexp)
				if err != nil || !re.MatchString(p[1:len(p)-1]) {
					t.Errorf("get(%q) = %v, %v", p, re, err)
					return
				}
			}
		})
	}
	wg.Wait()
	if got := regexps.len(); got > limits.MaxRegexpCacheEntries {
		t.Errorf("regexp cache holds %d entries, want at most %d", got, limits.MaxRegexpCacheEntries)
	}
}

func TestConstantPatternSkipsRegexpCache(t *testing.T) {
	te := newTestEnv(t)
	regexps.clear()
	te.expectRows("SELECT countIf(toString(number) LIKE '%1%'), countIf(toString(number) NOT ILIKE '%2%'), "+
		"countIf(match(toString(number), '^2')), countIf(replaceRegexpAll(toString(number), '3', 'x') != toString(number)), "+
		"countIf(replaceRegexpOne(toString(number), '(4)', '[\\\\1]') LIKE '%[4]%') FROM numbers(100)", "19\t81\t11\t19\t19")
	te.expectErr("SELECT match(toString(number), '(') FROM numbers(1)", cherr.CannotCompileRegexp)
	te.expectErr("SELECT replaceRegexpAll(toString(number), '(', 'x') FROM numbers(1)", cherr.BadArguments)
	te.expectRows("SELECT count() FROM numbers(0) WHERE match(toString(number), '(')", "0")
	if got := regexps.len(); got != 0 {
		t.Errorf("constant patterns left %d entries in the regexp cache", got)
	}
}

func TestReplaceRejectsTooLargeResult(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, sql := range []string{
		"SELECT length(replaceAll(repeat('a', 1000), 'a', repeat('b', 100000)))",
		"SELECT length(replace(repeat('a', 1000), 'a', repeat('b', 100000)))",
		"SELECT length(replaceRegexpAll(repeat('a', 1000), '', repeat('b', 100000)))",
		"SELECT length(replaceRegexpAll(repeat('a', 1000), left(toString(number), 0), repeat('b', 100000))) FROM numbers(1)",
		`SELECT length(replaceRegexpAll(repeat('a', 10000), 'a', repeat('\\0', 10000)))`,
		`SELECT length(replaceRegexpOne(repeat('a', 10000), '.*', repeat('\\0', 10000)))`,
	} {
		te.expectErr(sql, cherr.TooLargeStringSize)
	}
	te.expectRows(`SELECT replaceAll('abc', 'b', 'xx'), replaceOne('aXbX', 'X', 'yy'), replaceAll('abc', '', 'x'), `+
		`replaceRegexpAll('hello world', '(o)', '[\\1]'), replaceRegexpOne('hello', 'l', '\\0\\0'), replaceRegexpAll('abc', '', '-')`,
		"axxc\tayybX\tabc\thell[o] w[o]rld\thelllo\t-a-b-c-")
	te.expectRows(`SELECT length(replaceAll(repeat('ab', 1000000), 'b', repeat('c', 30))), `+
		`length(replaceRegexpAll(repeat('ab', 1000000), '(b)', '\\1\\1')), `+
		`length(replaceRegexpAll(repeat('ab', 1000000), 'b', repeat('c', 40)))`, "31000000\t3000000\t41000000")
	te.expectRows(`SELECT length(replaceAll(s, 'x', 'xy')), length(replaceRegexpAll(s, 'x', 'xy')), `+
		`length(replaceRegexpOne(s, '(x)', '\\1\\1')) FROM (SELECT repeat(repeat('x', 40), 1000000) AS s)`,
		"80000000\t80000000\t40000001")
}

func TestArrayStringConcatRejectsTooLargeResult(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	te.expectErr("SELECT length(arrayStringConcat(range(1000), repeat('x', 100000)))", cherr.TooLargeStringSize)
	te.expectRows("SELECT arrayStringConcat([1, 2, 3], '-'), arrayStringConcat(['a', 'b']), arrayStringConcat([], ','), "+
		"length(arrayStringConcat(range(100000), repeat('x', 100)))", "1-2-3\tab\t\t10488790")
	te.expectRows("SELECT length(arrayStringConcat(groupArray(repeat('x', 1000)))), "+
		"length(arrayStringConcat(groupArray(repeat('x', 1000)), ',')) FROM numbers(70000)", "70000000\t70069999")
}
