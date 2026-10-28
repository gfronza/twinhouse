//go:build compat

package compat

import (
	"context"
	"regexp"
	"testing"
)

// exceptionText is the message of an error body, without the version.
var exceptionText = regexp.MustCompile(`DB::Exception: (.*) \(version `)

func exceptionMessage(r httpResult) string {
	if m := exceptionText.FindSubmatch(r.body); m != nil {
		return string(m[1])
	}
	return ""
}

// TestMaxMemoryUsageValues pins how max_memory_usage values are read: the
// size suffixes, literal conversion and the errors of bad values, from a
// SETTINGS clause and from the URL.
func TestMaxMemoryUsageValues(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	for _, c := range []struct {
		value string
		code  int
		msg   string
	}{
		{"'10G'", 0, ""},
		{"'10Gi'", 0, ""},
		{"'10k'", 0, ""},
		{"1.5", 0, ""},
		{"1e9", 0, ""},
		{"'abc'", 27, "Cannot parse input: expected 'eof' before: 'abc'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)"},
		{"'10KB'", 27, "Cannot parse input: expected 'eof' before: 'B'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)"},
		{"'1.5'", 27, "Cannot parse input: expected 'eof' before: '.5'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)"},
		{"''", 32, "Attempt to read after eof. (ATTEMPT_TO_READ_AFTER_EOF)"},
		{"'-1'", 72, "Unsigned type must not contain '-' symbol. (CANNOT_PARSE_NUMBER)"},
		{"-1", 70, "Field value -1 is out of range of unsigned long type. (CANNOT_CONVERT_TYPE)"},
		{"-1.5", 70, "Cannot convert out of range floating point value to integer type. (CANNOT_CONVERT_TYPE)"},
		{"nan", 72, "Float setting value must be finite, got nan. (CANNOT_PARSE_NUMBER)"},
		{"DEFAULT", 0, ""},
		{"default", 0, ""},
		{"'DEFAULT'", 27, "Cannot parse input: expected 'eof' before: 'DEFAULT'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)"},
	} {
		sql := "SELECT 1 SETTINGS max_memory_usage = " + c.value
		checkSettingProbe(t, ctx, ref, our, sql, reqOpts{}, c.code, c.msg)
	}
	// The size settings twinhouse ignores still read their values.
	for _, name := range []string{"max_memory_usage_for_user", "max_memory_usage_for_all_queries",
		"max_bytes_before_external_sort", "max_bytes_before_external_group_by"} {
		checkSettingProbe(t, ctx, ref, our, "SELECT 1 SETTINGS "+name+" = '10Gi'", reqOpts{}, 0, "")
		checkSettingProbe(t, ctx, ref, our, "SELECT 1 SETTINGS "+name+" = 'x'", reqOpts{}, 27,
			"Cannot parse input: expected 'eof' before: 'x'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)")
		checkSettingProbe(t, ctx, ref, our, "SELECT 1 SETTINGS "+name+" = -1", reqOpts{}, 70,
			"Field value -1 is out of range of unsigned long type. (CANNOT_CONVERT_TYPE)")
	}
	for _, c := range []struct {
		value string
		code  int
		msg   string
	}{
		{"10G", 0, ""},
		{"0", 0, ""},
		{"abc", 27, "Cannot parse input: expected 'eof' before: 'abc'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)"},
		{"-1", 72, "Unsigned type must not contain '-' symbol. (CANNOT_PARSE_NUMBER)"},
		{"1.5", 27, "Cannot parse input: expected 'eof' before: '.5'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)"},
	} {
		o := reqOpts{settings: []kv{{"max_memory_usage", c.value}}}
		checkSettingProbe(t, ctx, ref, our, "SELECT 1", o, c.code, c.msg)
	}
}

// checkSettingProbe runs sql on both servers: the reference must answer
// code with msg (or succeed for code 0), and twinhouse the same.
func checkSettingProbe(t *testing.T, ctx context.Context, ref, our *target, sql string, o reqOpts, code int, msg string) {
	t.Helper()
	w, g := ref.do(ctx, sql, o), our.do(ctx, sql, o)
	if w.code != code || exceptionMessage(w) != msg {
		t.Fatalf("%s %v: reference %s; probe expects code %d %q (probe bug)", sql, o.settings, w.describe(), code, msg)
	}
	if g.code != w.code || g.status != w.status || exceptionMessage(g) != msg {
		t.Errorf("%s %v:\nreference: %s\ntwinhouse: %s", sql, o.settings, w.describe(), g.describe())
	}
}

// memoryLimitBody is ClickHouse 25.8's per-query 241 for a 20000000-byte
// limit; the reference names the processor that allocated, twinhouse
// none.
var memoryLimitBody = regexp.MustCompile(`^Code: 241\. DB::Exception: Query memory limit exceeded: would use \d+\.\d\d (B|KiB|MiB|GiB) \(attempt to allocate chunk of \d+\.\d\d (B|KiB|MiB|GiB) bytes\), maximum: 19\.07 MiB(: While executing \w+)?\. \(MEMORY_LIMIT_EXCEEDED\)`)

// TestMemoryLimitMessage pins the 241 text, from an aggregate state that
// passes the limit.
func TestMemoryLimitMessage(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	const sql = "SELECT length(groupArray(number)) FROM numbers(10000000) SETTINGS max_memory_usage = 20000000"
	for _, tg := range []*target{ref, our} {
		r := tg.do(ctx, sql, reqOpts{})
		if r.code != 241 || r.status != 500 || !memoryLimitBody.Match(r.body) {
			t.Errorf("%s: %s\n  -> %s", tg.name, sql, r.describe())
		}
	}
}

// TestMemoryLimitComesFromTheLastUnionBranch pins which SETTINGS clause of
// a SELECT sets the limit: the last plain branch's, applied after the
// statement's (written after FORMAT or after a parenthesized last
// branch); a parenthesized branch's never does. twinhouse has no INSERT
// INTO FUNCTION null(...), the read-only reference's only INSERT, so the
// INSERT ... SELECT rules are pinned by exec tests.
func TestMemoryLimitComesFromTheLastUnionBranch(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	const in = "SELECT count() FROM numbers(200000) WHERE number IN (SELECT number FROM numbers(200000))"
	// Without it the reference sends the light branch's rows before
	// failing, with status 200.
	o := reqOpts{settings: []kv{{"wait_end_of_query", "1"}}}
	for _, c := range []struct {
		sql  string
		code int
	}{
		{in + " SETTINGS max_memory_usage = 1000000 UNION ALL SELECT 1", 0},
		{"SELECT 1 UNION ALL " + in + " SETTINGS max_memory_usage = 1000000", 241},
		{in + " SETTINGS max_memory_usage = 1000000 UNION ALL SELECT 1 SETTINGS max_memory_usage = 2000000", 241},
		{"SELECT 1 SETTINGS max_memory_usage = 3000000 UNION ALL " + in + " SETTINGS max_memory_usage = 1000000 UNION ALL SELECT 2", 0},
		// An earlier branch's value is still validated (the reference
		// adds which setting it was setting to the message).
		{"SELECT 1 SETTINGS max_memory_usage = 'abc' UNION ALL SELECT 2", 27},
		{"SELECT 1 UNION ALL (SELECT 2 SETTINGS max_memory_usage = 'abc' UNION ALL SELECT 3)", 27},
		{"SELECT 1 UNION ALL (" + in + " SETTINGS max_memory_usage = 1000000)", 0},
		{in + " UNION ALL (SELECT 1 SETTINGS max_memory_usage = 1000000)", 0},
		{"SELECT 1 UNION ALL (SELECT 2 UNION ALL " + in + " SETTINGS max_memory_usage = 1000000)", 0},
		{"SELECT 1 UNION ALL (" + in + ") SETTINGS max_memory_usage = 1000000", 241},
		{"(" + in + " SETTINGS max_memory_usage = 1000000) UNION ALL SELECT 1", 0},
		{"(" + in + " SETTINGS max_memory_usage = 1000000)", 241},
		{"(SELECT 1 UNION ALL " + in + " SETTINGS max_memory_usage = 1000000)", 241},
		{in + " SETTINGS max_memory_usage = 1000000 FORMAT TSV SETTINGS max_memory_usage = 0", 241},
		{in + " SETTINGS max_memory_usage = 0 FORMAT TSV SETTINGS max_memory_usage = 1000000", 0},
		{in + " UNION ALL SELECT 1 SETTINGS max_memory_usage = 1000000 FORMAT TSV SETTINGS max_memory_usage = 0", 241},
		{in + " UNION ALL SELECT 1 SETTINGS max_memory_usage = 0 FORMAT TSV SETTINGS max_memory_usage = 1000000", 0},
		// DEFAULT wins within one clause, wherever it is written.
		{in + " SETTINGS max_memory_usage = DEFAULT, max_memory_usage = 1000000", 0},
		{in + " SETTINGS max_memory_usage = 1000000, max_memory_usage = DEFAULT", 0},
		{in + " UNION ALL SELECT 1 SETTINGS max_memory_usage = DEFAULT, max_memory_usage = 1000000, max_threads = 2", 0},
	} {
		w, g := ref.do(ctx, c.sql, o), our.do(ctx, c.sql, o)
		if w.code != c.code {
			t.Fatalf("%s: reference %s; probe expects code %d (probe bug)", c.sql, w.describe(), c.code)
		}
		if g.code != w.code || g.status != w.status {
			t.Errorf("%s:\nreference: %s\ntwinhouse: %s", c.sql, w.describe(), g.describe())
		}
	}
}

// TestParenthesizedBranchDefaultKeepsTheLimit pins that DEFAULT in a
// parenthesized last branch does not reset the URL's limit.
func TestParenthesizedBranchDefaultKeepsTheLimit(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	const sql = "SELECT 1 UNION ALL (SELECT count() FROM numbers(200000) WHERE number IN (SELECT number FROM numbers(200000)) SETTINGS max_memory_usage = DEFAULT)"
	o := reqOpts{settings: []kv{{"wait_end_of_query", "1"}, {"max_memory_usage", "1000000"}}}
	w, g := ref.do(ctx, sql, o), our.do(ctx, sql, o)
	if w.code != 241 {
		t.Fatalf("reference %s; probe expects 241 (probe bug)", w.describe())
	}
	if g.code != w.code || g.status != w.status {
		t.Errorf("reference: %s\ntwinhouse: %s", w.describe(), g.describe())
	}
}
