package exec

import (
	"context"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/sqlparse"
	"github.com/gfronza/twinhouse/internal/storage"
)

// newMemoryEnv is a test env whose server memory limits are maxMem for
// statements and viewMem for ApplyViews.
func newMemoryEnv(t *testing.T, maxMem, viewMem int64) *testEnv {
	return newTestEnvConfig(t, func(c *Config) {
		c.MaxMemoryUsage, c.ViewMemoryUsage = maxMem, viewMem
	})
}

// limitOf runs req and returns the memory limit the query ran with.
func limitOf(te *testEnv, req *Request) (int64, error) {
	var limit int64
	te.e.impl.testHookFinished = func(q *query) {
		checkNoTrackedMemory(te.t, q)
		limit = q.mem.Limit()
	}
	_, err := te.runReq(req)
	return limit, err
}

// wantCode checks that err is a *cherr.Error with code and message.
func wantCode(t *testing.T, what string, err error, code int, msg string) {
	t.Helper()
	ce := cherr.As(err)
	if err == nil || ce == nil || ce.Code != code || (msg != "" && ce.Msg != msg) {
		t.Errorf("%s: got %v, want code %d %q", what, err, code, msg)
	}
}

func TestMaxMemoryUsageParsesLikeClickHouse(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	for _, c := range []struct {
		value string
		want  int64
		code  int
		msg   string
	}{
		{value: "'10G'", want: 10_000_000_000},
		{value: "'10Gi'", want: 10 << 30},
		{value: "'10K'", want: 10_000},
		{value: "'10k'", want: 10_000},
		{value: "'10ki'", want: 10 << 10},
		{value: "'1M'", want: 1_000_000},
		{value: "'1Mi'", want: 1 << 20},
		{value: "'1T'", want: 1_000_000_000_000},
		{value: "'1Ti'", want: 1 << 40},
		{value: "'+5'", want: 5},
		{value: "'007'", want: 7},
		{value: "'k'", want: 0},
		{value: "'18446744073709551617'", want: 1}, // wraps, as ClickHouse
		{value: "'18446744073709551615'", want: math.MaxInt64},
		{value: "1.5", want: 1},
		{value: "2.9", want: 2},
		{value: "1e9", want: 1_000_000_000},
		{value: "18446744073709551616", want: math.MaxInt64},
		{value: "true", want: 1},
		{value: "false", want: 0},
		{value: "-0", want: 0},
		{value: "0", want: 0},
		{value: "'abc'", code: 27, msg: "Cannot parse input: expected 'eof' before: 'abc'"},
		{value: "'10KB'", code: 27, msg: "Cannot parse input: expected 'eof' before: 'B'"},
		{value: "'10KI'", code: 27, msg: "Cannot parse input: expected 'eof' before: 'I'"},
		{value: "'10 G'", code: 27, msg: "Cannot parse input: expected 'eof' before: ' G'"},
		{value: "'1.5'", code: 27, msg: "Cannot parse input: expected 'eof' before: '.5'"},
		{value: "'1e9'", code: 27, msg: "Cannot parse input: expected 'eof' before: 'e9'"},
		{value: "'1P'", code: 27, msg: "Cannot parse input: expected 'eof' before: 'P'"},
		{value: "''", code: 32, msg: "Attempt to read after eof"},
		{value: "'-1'", code: 72, msg: "Unsigned type must not contain '-' symbol"},
		{value: "'+'", code: 72, msg: "Cannot parse number without any digits"},
		{value: "'++1'", code: 72, msg: "Cannot parse number with multiple sign (+/-) characters"},
		{value: "-1", code: 70, msg: "Field value -1 is out of range of unsigned long type"},
		{value: "-1.5", code: 70, msg: "Cannot convert out of range floating point value to integer type"},
		{value: "1e30", code: 70, msg: "Cannot convert out of range floating point value to integer type"},
		{value: "NULL", code: 70, msg: "Invalid value NULL of the setting, which needs unsigned long"},
		{value: "nan", code: 72, msg: "Float setting value must be finite, got nan"},
		{value: "inf", code: 72, msg: "Float setting value must be finite, got inf"},
		{value: "-inf", code: 72, msg: "Float setting value must be finite, got -inf"},
	} {
		for _, sql := range []string{
			"SELECT 1 SETTINGS max_memory_usage = " + c.value,
			"SET max_memory_usage = " + c.value,
		} {
			got, err := limitOf(te, &Request{Query: sql})
			if c.code != 0 {
				wantCode(t, sql, err, c.code, c.msg)
			} else if err != nil || got != c.want {
				t.Errorf("%s: limit %d, %v; want %d", sql, got, err, c.want)
			}
		}
	}
}

func TestMaxMemoryUsageURLValueParsesAsText(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	for _, c := range []struct {
		value string
		want  int64
		code  int
	}{
		{value: "10G", want: 10_000_000_000},
		{value: "10Gi", want: 10 << 30},
		{value: "0", want: 0},
		{value: "abc", code: 27},
		{value: "1.5", code: 27},
		{value: "1e9", code: 27},
		{value: "-1", code: 72},
		{value: "", code: 32},
	} {
		req := &Request{Query: "SELECT 1", Settings: map[string]string{"max_memory_usage": c.value}}
		got, err := limitOf(te, req)
		if c.code != 0 {
			wantCode(t, c.value, err, c.code, "")
		} else if err != nil || got != c.want {
			t.Errorf("%q: limit %d, %v; want %d", c.value, got, err, c.want)
		}
	}
}

func TestMaxMemoryUsagePrecedence(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	te.exec("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
	url := map[string]string{"max_memory_usage": "1000000"}
	for _, c := range []struct {
		name     string
		sql      string
		settings map[string]string
		want     int64
	}{
		{"server default", "SELECT 1", nil, 0},
		{"URL over default", "SELECT 1", url, 1_000_000},
		{"SETTINGS over URL", "SELECT 1 SETTINGS max_memory_usage = 2000000", url, 2_000_000},
		{"SETTINGS 0 over URL", "SELECT 1 SETTINGS max_memory_usage = 0", url, 0},
		{"SET over URL", "SET max_memory_usage = 3000000", url, 3_000_000},
		{"last UNION branch", "SELECT 1 SETTINGS max_memory_usage = 4 UNION ALL SELECT 2 SETTINGS max_memory_usage = 5000000", nil, 5_000_000},
		{"only the last UNION branch", "SELECT 1 SETTINGS max_memory_usage = 4 UNION ALL SELECT 2", url, 1_000_000},
		{"INSERT over SELECT", "INSERT INTO t SETTINGS max_memory_usage = 2000000 SELECT 1 SETTINGS max_memory_usage = 3000000", nil, 2_000_000},
		{"INSERT DEFAULT over SELECT", "INSERT INTO t SETTINGS max_memory_usage = DEFAULT SELECT 1 SETTINGS max_memory_usage = 3000000", nil, 0},
		{"first INSERT branch", "INSERT INTO t SELECT 1 SETTINGS max_memory_usage = 2000000 UNION ALL SELECT 2 SETTINGS max_memory_usage = 3000000", nil, 2_000_000},
		{"first INSERT branch setting it", "INSERT INTO t SELECT 1 SETTINGS max_threads = 1 UNION ALL SELECT 2 SETTINGS max_memory_usage = 3000000", nil, 3_000_000},
		{"INSERT branch DEFAULT", "INSERT INTO t SELECT 1 SETTINGS max_memory_usage = DEFAULT UNION ALL SELECT 2 SETTINGS max_memory_usage = 3000000", url, 0},
		// A parenthesized branch's clause is not the query's.
		{"parenthesized last branch", "SELECT 1 UNION ALL (SELECT 2 SETTINGS max_memory_usage = 1000000)", nil, 0},
		{"parenthesized last branch DEFAULT", "SELECT 1 UNION ALL (SELECT 2 SETTINGS max_memory_usage = DEFAULT)", url, 1_000_000},
		{"nested UNION's last branch", "SELECT 1 UNION ALL (SELECT 2 UNION ALL SELECT 3 SETTINGS max_memory_usage = 1000000)", nil, 0},
		{"after a parenthesized last branch", "SELECT 1 UNION ALL (SELECT 2) SETTINGS max_memory_usage = 2000000", nil, 2_000_000},
		{"parenthesized SELECT alone", "(SELECT 1 SETTINGS max_memory_usage = 2000000)", nil, 2_000_000},
		{"parenthesized UNION alone", "(SELECT 1 UNION ALL SELECT 2 SETTINGS max_memory_usage = 2000000)", nil, 2_000_000},
		{"INSERT parenthesized branch", "INSERT INTO t SELECT 1 UNION ALL (SELECT 2 SETTINGS max_memory_usage = 1000000)", nil, 0},
		{"INSERT plain branch after a parenthesized one", "INSERT INTO t SELECT 1 UNION ALL (SELECT 2 SETTINGS max_memory_usage = 1000000) UNION ALL SELECT 3 SETTINGS max_memory_usage = 2000000", nil, 2_000_000},
		{"INSERT branch of a nested UNION", "INSERT INTO t SELECT 1 UNION ALL (SELECT 2 SETTINGS max_memory_usage = 1000000 UNION ALL SELECT 3) UNION ALL SELECT 4", nil, 0},
		// The SELECT's own clause is applied after the one after FORMAT.
		{"own clause over FORMAT's", "SELECT 1 SETTINGS max_memory_usage = 2000000 FORMAT TSV SETTINGS max_memory_usage = 3000000", nil, 2_000_000},
		{"FORMAT's clause alone", "SELECT 1 FORMAT TSV SETTINGS max_memory_usage = 3000000", nil, 3_000_000},
		{"last branch's clause over FORMAT's", "SELECT 1 UNION ALL SELECT 2 SETTINGS max_memory_usage = 2000000 FORMAT TSV SETTINGS max_memory_usage = 3000000", nil, 2_000_000},
	} {
		got, err := limitOf(te, &Request{Query: c.sql, Settings: c.settings})
		if err != nil || got != c.want {
			t.Errorf("%s: limit %d, %v; want %d", c.name, got, err, c.want)
		}
	}
}

// memoryLimitMsg is the shape of the 241 message.
var memoryLimitMsg = regexp.MustCompile(`^Query memory limit exceeded: would use \d+\.\d\d [KMG]?i?B \(attempt to allocate chunk of \d+\.\d\d [KMG]?i?B bytes\), maximum: `)

func wantMemoryLimit(t *testing.T, te *testEnv, sql string, settings map[string]string) {
	t.Helper()
	_, err := te.runReq(&Request{Query: sql, Settings: settings})
	ce := cherr.As(err)
	if ce == nil || ce.Code != cherr.MemoryLimitExceeded || !memoryLimitMsg.MatchString(ce.Msg) {
		t.Errorf("%s: got %v, want MEMORY_LIMIT_EXCEEDED", sql, err)
	}
}

func TestMemoryLimitStopsCollectedResult(t *testing.T) {
	if testing.Short() {
		t.Skip("collects about 64 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	sql := "SELECT number FROM numbers(10000000) SETTINGS max_memory_usage = 64000000"
	_, err := te.run(sql)
	ce := cherr.As(err)
	if ce == nil || ce.Code != cherr.MemoryLimitExceeded {
		t.Fatalf("%s: %v", sql, err)
	}
	if !memoryLimitMsg.MatchString(ce.Msg) || !strings.HasSuffix(ce.Msg, "maximum: 61.04 MiB") {
		t.Fatalf("message %q", ce.Msg)
	}
}

func TestMemoryLimitZeroIsUnlimited(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	url := map[string]string{"max_memory_usage": "1000"}
	wantMemoryLimit(t, te, "SELECT number FROM numbers(100000)", url)
	if _, err := te.runReq(&Request{Query: "SELECT number FROM numbers(100000) SETTINGS max_memory_usage = 0", Settings: url}); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryLimitCountsSubqueryResults(t *testing.T) {
	te := newTestEnv(t)
	wantMemoryLimit(t, te, "SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000)) SETTINGS max_memory_usage = 1000000", nil)
	te.expectRows("SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000)) SETTINGS max_memory_usage = 20000000", "10")
}

// A view in another database is planned on a copy of the query (withDB),
// which must keep the query's limit.
func TestMemoryLimitReachesViewsInOtherDatabases(t *testing.T) {
	te := newTestEnv(t)
	te.exec("CREATE DATABASE other")
	te.exec("CREATE VIEW other.v AS SELECT count() AS c FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000))")
	wantMemoryLimit(t, te, "SELECT c FROM other.v SETTINGS max_memory_usage = 1000000", nil)
	te.expectRows("SELECT c FROM other.v", "10")
}

func TestServerMemoryLimitCanOnlyBeLowered(t *testing.T) {
	const def = 4_000_000
	te := newMemoryEnv(t, def, 0)
	const violation = "Setting max_memory_usage shouldn't be greater than 4000000"

	wantMemoryLimit(t, te, "SELECT number FROM numbers(100000)", nil)
	te.expectRows("SELECT count() FROM (SELECT number FROM numbers(30000))", "30000")
	wantMemoryLimit(t, te, "SELECT number FROM numbers(30000) SETTINGS max_memory_usage = 1000000", nil)

	for _, ok := range []string{"4000000", "1", "'4M'"} {
		if got, err := limitOf(te, &Request{Query: "SELECT 1 SETTINGS max_memory_usage = " + ok}); err != nil || got == 0 {
			t.Errorf("lowering to %s: limit %d, %v", ok, got, err)
		}
	}
	for _, bad := range []string{"4000001", "0", "'10G'", "'4Mi'"} {
		for _, req := range []*Request{
			{Query: "SELECT 1 SETTINGS max_memory_usage = " + bad},
			{Query: "SET max_memory_usage = " + bad},
			{Query: "SELECT 1", Settings: map[string]string{"max_memory_usage": strings.Trim(bad, "'")}},
		} {
			_, err := te.runReq(req)
			wantCode(t, req.Query+" "+bad, err, cherr.SettingConstraintViolation, violation)
		}
	}
}

func TestServerWithoutMemoryLimitAcceptsAnyValue(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	if got, err := limitOf(te, &Request{Query: "SELECT 1 SETTINGS max_memory_usage = 1000000000000000000"}); err != nil || got != 1e18 {
		t.Fatalf("limit %d, %v", got, err)
	}
}

func TestViewMemoryUsageLimitsApplyViews(t *testing.T) {
	for _, c := range []struct {
		name            string
		maxMem, viewMem int64
	}{
		{"view limit", 0, 1_000_000},
		{"statement limit when no view limit", 1_000_000, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			te := newTestEnvConfig(t, func(cfg *Config) {
				cfg.MaxMemoryUsage, cfg.ViewMemoryUsage, cfg.InlineViews = c.maxMem, c.viewMem, false
			})
			te.exec("CREATE TABLE src (n UInt64) ENGINE = MergeTree ORDER BY n")
			te.exec("CREATE TABLE dst (xs Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()")
			te.exec("CREATE MATERIALIZED VIEW mv TO dst AS SELECT range(n) AS xs FROM src")
			src, err := te.cat.Table("default", "src")
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			err = te.e.ApplyViews(ctx, src, []storage.Row{{uint64(100_000)}}, []string{"r1"})
			if ce := cherr.As(err); ce == nil || ce.Code != cherr.MemoryLimitExceeded {
				t.Fatalf("ApplyViews of a 100000-element output row: %v", err)
			}
			if err := te.e.ApplyViews(ctx, src, []storage.Row{{uint64(1000)}, {uint64(1000)}}, []string{"r2", "r3"}); err != nil {
				t.Fatalf("ApplyViews of small rows: %v", err)
			}
			te.expectRows("SELECT count(), sum(length(xs)) FROM dst", "2\t2000")
		})
	}
}

func TestQueryFinishReleasesQueryLifetimeMemory(t *testing.T) {
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	if err := q.lifetimeAccount().Grow(1 << 20); err != nil {
		t.Fatal(err)
	}
	if q.mem.Used() == 0 {
		t.Fatal("a lifetime account reserved nothing")
	}
	q.finish()
	if q.mem.Used() != 0 {
		t.Fatalf("used after finish = %d", q.mem.Used())
	}
}

func TestUnlimitedQueryHasNoAccounts(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	q := te.e.newQuery(context.Background(), &Request{})
	if q.lifetimeAccount() != nil || len(*q.lifetime) != 0 {
		t.Fatal("an unlimited query created an account")
	}
}

func TestSettingTextKeepsTheMemoryLimitValue(t *testing.T) {
	for _, c := range []struct {
		sql, want string
		reset     bool
	}{
		{sql: "SET max_memory_usage = 1e9", want: "1000000000"},
		{sql: "SET max_memory_usage = 1.5", want: "1"},
		{sql: "SET max_memory_usage = '10G'", want: "10G"},
		{sql: "SET max_bytes_before_external_sort = 2.5", want: "2"},
		{sql: "SET max_memory_usage = DEFAULT", reset: true},
		{sql: "SET max_memory_usage_for_user = default", reset: true},
	} {
		st, err := sqlparse.Parse(c.sql)
		if err != nil {
			t.Fatal(err)
		}
		set := st.(*sqlparse.SetStmt).Settings[0]
		if got, reset, ok := SettingText(set.Name, set.Value); !ok || got != c.want || reset != c.reset {
			t.Errorf("%s: %q reset %v ok %v, want %q reset %v", c.sql, got, reset, ok, c.want, c.reset)
		}
	}
	if _, _, ok := SettingText("max_memory_usage", &sqlparse.Literal{Value: int64(-1)}); ok {
		t.Error("SettingText accepted an invalid value")
	}
	if _, _, ok := SettingText("max_threads", &sqlparse.Literal{Value: 1.5}); ok {
		t.Error("SettingText rewrote another setting")
	}
	if _, reset, _ := SettingText("max_threads", &sqlparse.Ident{Parts: []string{"DEFAULT"}}); reset {
		t.Error("SettingText reset another setting")
	}
}

// DEFAULT drops max_memory_usage, so the server default applies even under
// a lower URL limit; the quoted string 'DEFAULT' is bad text.
func TestMaxMemoryUsageDefaultResetsToTheServerLimit(t *testing.T) {
	const def = 4_000_000
	te := newMemoryEnv(t, def, 0)
	url := map[string]string{"max_memory_usage": "1000000"}
	for _, sql := range []string{
		"SELECT 1 SETTINGS max_memory_usage = DEFAULT",
		"SELECT 1 SETTINGS max_memory_usage = default",
		"SELECT 1 SETTINGS max_memory_usage = 2000000, max_memory_usage = DEFAULT",
		// As in ClickHouse, DEFAULT wins within one clause.
		"SELECT 1 SETTINGS max_memory_usage = DEFAULT, max_memory_usage = 2000000",
		"SELECT 1 SETTINGS max_memory_usage = DEFAULT, max_memory_usage = 2000000, max_threads = 2",
		"SELECT 1 UNION ALL SELECT 2 SETTINGS max_memory_usage = DEFAULT, max_memory_usage = 2000000",
		"SET max_memory_usage = DEFAULT",
		"SET max_memory_usage = DEFAULT, max_memory_usage = 2000000",
	} {
		if got, err := limitOf(te, &Request{Query: sql, Settings: url}); err != nil || got != def {
			t.Errorf("%s: limit %d, %v; want %d", sql, got, err, def)
		}
	}
	_, err := te.runReq(&Request{Query: "SELECT 1 SETTINGS max_memory_usage = 'DEFAULT'"})
	wantCode(t, "'DEFAULT'", err, cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: 'DEFAULT'")
}

// The settings that only name memory sizes are ignored, but their values
// must still read as sizes.
func TestIgnoredSizeSettingsAreValidated(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	for _, name := range []string{"max_memory_usage_for_user", "max_memory_usage_for_all_queries",
		"max_bytes_before_external_sort", "max_bytes_before_external_group_by"} {
		for _, ok := range []string{"'10G'", "1.5", "0", "DEFAULT"} {
			if _, err := te.runReq(&Request{Query: "SELECT 1 SETTINGS " + name + " = " + ok}); err != nil {
				t.Errorf("%s = %s: %v", name, ok, err)
			}
		}
		for _, sql := range []string{"SELECT 1 SETTINGS " + name + " = 'abc'", "SET " + name + " = 'abc'"} {
			_, err := te.runReq(&Request{Query: sql})
			wantCode(t, sql, err, cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: 'abc'")
		}
		_, err := te.runReq(&Request{Query: "SELECT 1 SETTINGS " + name + " = -1"})
		wantCode(t, name+" = -1", err, cherr.CannotConvertType, "Field value -1 is out of range of unsigned long type")
		_, err = te.runReq(&Request{Query: "SELECT 1", Settings: map[string]string{name: "abc"}})
		wantCode(t, name+" in the URL", err, cherr.CannotParseInputAssertionFailed, "")
	}
}

// In INSERT ... SELECT, the SELECT's SETTINGS apply to the whole INSERT, as
// in ClickHouse.
func TestInsertSelectAppliesTheSelectSettings(t *testing.T) {
	te := newMemoryEnv(t, 32_000_000, 0)
	te.exec("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
	const sel = "INSERT INTO t SELECT number FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000))"
	wantMemoryLimit(t, te, sel+" SETTINGS max_memory_usage = 1000000", nil)
	_, err := te.runReq(&Request{Query: sel + " SETTINGS max_memory_usage = 'abc'"})
	wantCode(t, "'abc'", err, cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: 'abc'")
	_, err = te.runReq(&Request{Query: sel + " SETTINGS max_memory_usage = 0"})
	wantCode(t, "0", err, cherr.SettingConstraintViolation, "Setting max_memory_usage shouldn't be greater than 32000000")
	te.exec(sel)
	te.expectRows("SELECT count() FROM t", "10")
}

// As in ClickHouse, CREATE TABLE ... AS SELECT applies the SELECT's
// SETTINGS.
func TestCreateAsSelectAppliesTheSelectSettings(t *testing.T) {
	te := newMemoryEnv(t, 32_000_000, 0)
	const sel = " ENGINE = MergeTree ORDER BY n AS SELECT number AS n FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000))"
	wantMemoryLimit(t, te, "CREATE TABLE a"+sel+" SETTINGS max_memory_usage = 1000000", nil)
	_, err := te.runReq(&Request{Query: "CREATE TABLE b" + sel + " SETTINGS max_memory_usage = 'abc'"})
	wantCode(t, "'abc'", err, cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: 'abc'")
	_, err = te.runReq(&Request{Query: "CREATE TABLE c" + sel + " SETTINGS max_memory_usage = 0"})
	wantCode(t, "0", err, cherr.SettingConstraintViolation, "Setting max_memory_usage shouldn't be greater than 32000000")
	te.exec("CREATE TABLE d" + sel)
	te.expectRows("SELECT count() FROM d", "10")
}

// An earlier UNION branch's limit does not limit the query, as in
// ClickHouse.
func TestMemoryLimitOfAnEarlierUnionBranchIsIgnored(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	te.expectRows("SELECT count() FROM (SELECT number FROM numbers(100000) SETTINGS max_memory_usage = 1000000 UNION ALL SELECT 1)", "100001")
	te.exec("SELECT number FROM numbers(100000) SETTINGS max_memory_usage = 1000000 UNION ALL SELECT 1")
	wantMemoryLimit(t, te, "SELECT number FROM numbers(100000) UNION ALL SELECT 1 SETTINGS max_memory_usage = 1000000", nil)
}

func TestMemoryLimitOfInsertBeatsItsSelect(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	te.exec("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
	const sel = " SELECT number FROM numbers(10) WHERE number IN (SELECT number FROM numbers(100000))"
	wantMemoryLimit(t, te, "INSERT INTO t SETTINGS max_memory_usage = 1000000"+sel+" SETTINGS max_memory_usage = 32000000", nil)
	wantMemoryLimit(t, te, "INSERT INTO t"+sel+" SETTINGS max_memory_usage = 1000000 UNION ALL SELECT 1 SETTINGS max_memory_usage = 32000000", nil)
	te.exec("INSERT INTO t SETTINGS max_memory_usage = 32000000" + sel + " SETTINGS max_memory_usage = 1000000")
	te.expectRows("SELECT count() FROM t", "10")
}

// SETTINGS that do not set the limit are still validated.
func TestMemoryLimitOfEveryBranchIsValidated(t *testing.T) {
	te := newMemoryEnv(t, 4_000_000, 0)
	te.exec("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
	const violation = "Setting max_memory_usage shouldn't be greater than 4000000"
	for _, c := range []struct {
		sql  string
		code int
		msg  string
	}{
		{"SELECT 1 SETTINGS max_memory_usage = 'abc' UNION ALL SELECT 2", cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: 'abc'"},
		{"SELECT 1 SETTINGS max_memory_usage = 0 UNION ALL SELECT 2", cherr.SettingConstraintViolation, violation},
		{"INSERT INTO t SETTINGS max_memory_usage = 1000 SELECT 1 SETTINGS max_memory_usage = 5000000", cherr.SettingConstraintViolation, violation},
		{"INSERT INTO t SELECT 1 SETTINGS max_memory_usage = 1000 UNION ALL SELECT 2 SETTINGS max_memory_usage = -1", cherr.CannotConvertType, "Field value -1 is out of range of unsigned long type"},
		{"SELECT 1 UNION ALL (SELECT 2 SETTINGS max_memory_usage = 'abc' UNION ALL SELECT 3)", cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: 'abc'"},
		{"INSERT INTO t SELECT 1 UNION ALL (SELECT 2 SETTINGS max_memory_usage = 0)", cherr.SettingConstraintViolation, violation},
	} {
		_, err := te.run(c.sql)
		wantCode(t, c.sql, err, c.code, c.msg)
	}
}
