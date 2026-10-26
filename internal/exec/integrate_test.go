package exec

import (
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// Expectations in this file were verified against ClickHouse 25.8.

// TestUseBuiltinDatabases: USE on the virtual databases succeeds without a
// catalog entry (case-sensitive, like ClickHouse).
func TestUseBuiltinDatabases(t *testing.T) {
	te := newTestEnv(t)
	for _, db := range []string{"default", "system", "information_schema", "INFORMATION_SCHEMA"} {
		res := te.exec("USE " + db)
		if res.Database != db {
			t.Errorf("USE %s: Database = %q", db, res.Database)
		}
	}
	te.expectErr("USE System", cherr.UnknownDatabase)
	te.expectErr("USE nosuchdb", cherr.UnknownDatabase)
}

// TestToNumberEmptyAndSignOnly: toIntN/toFloatN error codes for strings
// without digits.
func TestToNumberEmptyAndSignOnly(t *testing.T) {
	te := newTestEnv(t)
	for _, c := range []struct {
		q    string
		code int
		msg  string
	}{
		{"SELECT toUInt64('')", 32, "Attempt to read after eof: Cannot parse UInt64 from String, because value is too short"},
		{"SELECT toInt32('')", 32, "Attempt to read after eof: Cannot parse Int32 from String, because value is too short"},
		{"SELECT toInt32('-')", 72, "Cannot parse number without any digits: Cannot parse Int32 from String"},
		{"SELECT toUInt64('+')", 72, "Cannot parse number without any digits: Cannot parse UInt64 from String"},
		{"SELECT toFloat64('')", 72, "Cannot read floating point value: Cannot parse Float64 from String"},
		{"SELECT toFloat64('-')", 72, "Cannot read floating point value: no digits read: Cannot parse Float64 from String"},
		{"SELECT toFloat64('1e')", 72, "Cannot read floating point value: nothing after exponent: Cannot parse Float64 from String"},
		{"SELECT toFloat64('abc')", 6, "Cannot parse string 'abc' as Float64"},
		{"SELECT toUInt8('abc')", 6, "Cannot parse string 'abc' as UInt8"},
		{"SELECT CAST('' AS UInt64)", 32, ""},
		{"SELECT CAST('-' AS Float64)", 72, ""},
	} {
		_, err := te.run(c.q)
		if !cherr.Is(err, c.code) {
			t.Errorf("%s: got %v, want code %d", c.q, err, c.code)
			continue
		}
		if c.msg != "" && !strings.HasPrefix(cherr.As(err).Msg, c.msg) {
			t.Errorf("%s: message %q, want prefix %q", c.q, cherr.As(err).Msg, c.msg)
		}
	}
	if got := te.tsv("SELECT toUInt64OrZero(''), toInt32OrNull('-'), toFloat64OrZero('')"); !strings.HasSuffix(got, "0\t\\N\t0\n") {
		t.Errorf("OrZero/OrNull: %q", got)
	}
}

// TestBestEffortSaturates: parseDateTimeBestEffort clamps to the DateTime
// range; parseDateTime64BestEffort clamps at 1900-01-01.
func TestBestEffortSaturates(t *testing.T) {
	te := newTestEnv(t)
	got := te.tsv(`SELECT parseDateTimeBestEffort('2200-01-01 00:00:00') a, parseDateTimeBestEffort('1960-01-01 00:00:00') b,
		parseDateTimeBestEffortOrZero('2106-02-07 06:28:16') c, parseDateTime64BestEffort('1800-01-01 00:00:00') d,
		parseDateTimeBestEffort('2024-05-01 10:00:00') e`)
	want := "2106-02-07 06:28:15\t1970-01-01 00:00:00\t2106-02-07 06:28:15\t1900-01-01 00:00:00.000\t2024-05-01 10:00:00\n"
	if !strings.HasSuffix(got, want) {
		t.Errorf("got %q, want suffix %q", got, want)
	}
}

// TestBoundParamColumnNameRawType: a bound parameter's column name uses the
// type as written.
func TestBoundParamColumnNameRawType(t *testing.T) {
	te := newTestEnv(t)
	for q, want := range map[string]string{
		"SELECT {p:Array( UInt8 )}":     "_CAST([1], 'Array( UInt8 )')",
		"SELECT { p : Array( UInt8 ) }": "_CAST([1], 'Array( UInt8 ) ')",
		"SELECT {p:Array(UInt8)}":       "_CAST([1], 'Array(UInt8)')",
	} {
		res := te.exec(q, "p", "[1]")
		if len(res.Columns) != 1 || res.Columns[0].Name != want {
			t.Errorf("%s: columns %+v, want %q", q, res.Columns, want)
		}
	}
}

// TestIdentifierParamsAndUnionMode: {name:Identifier} parameters are
// substituted at parse time and request settings provide union_default_mode.
func TestIdentifierParamsAndUnionMode(t *testing.T) {
	te := newTestEnv(t)
	te.exec("CREATE DATABASE idp")
	te.exec("CREATE TABLE idp.t (k String) ENGINE = MergeTree ORDER BY k")
	te.exec("INSERT INTO idp.t VALUES ('a'), ('b')")
	if got := te.tsv("SELECT count() FROM {db:Identifier}.{tbl:Identifier}", "db", "idp", "tbl", "t"); !strings.HasSuffix(got, "\n2\n") {
		t.Errorf("identifier params: %q", got)
	}
	te.expectErr("SELECT count() FROM idp.{tbl:Identifier}", cherr.UnknownQueryParameter)

	te.expectErr("SELECT 1 UNION SELECT 1", 558)
	res, err := te.runReq(&Request{Query: "SELECT 1 AS x UNION SELECT 1", Settings: map[string]string{"union_default_mode": "DISTINCT"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Errorf("UNION DISTINCT via settings: %d rows", len(res.Rows))
	}
	res, err = te.runReq(&Request{Query: "SELECT 1 AS x UNION SELECT 1", Settings: map[string]string{"union_default_mode": "ALL"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 {
		t.Errorf("UNION ALL via settings: %d rows", len(res.Rows))
	}
}
