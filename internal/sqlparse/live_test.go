package sqlparse

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// Differential tests against a real ClickHouse server. They run only when
// SQLPARSE_CLICKHOUSE_URL is set, e.g.
//
//	docker run -d -p 18125:8123 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:25.8-alpine
//	SQLPARSE_CLICKHOUSE_URL=http://localhost:18125 go test ./internal/sqlparse -run Live
func liveURL(t *testing.T) string {
	u := os.Getenv("SQLPARSE_CLICKHOUSE_URL")
	if u == "" {
		t.Skip("SQLPARSE_CLICKHOUSE_URL not set")
	}
	return u
}

// chColumnName asks ClickHouse for the name of `SELECT <expr>`; ok is false
// when ClickHouse rejects the query (type errors etc.).
func chColumnName(t *testing.T, base, from, expr string) (string, bool) {
	t.Helper()
	q := "SELECT " + expr + " FROM " + from + " LIMIT 0 FORMAT JSONCompact"
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(base+"/?"+url.Values{}.Encode(), "text/plain", strings.NewReader(q))
	if err != nil {
		t.Fatalf("clickhouse: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return string(body), false
	}
	var out struct {
		Meta []struct{ Name string } `json:"meta"`
	}
	if err := json.Unmarshal(body, &out); err != nil || len(out.Meta) != 1 {
		return string(body), false
	}
	return out.Meta[0].Name, true
}

const liveTable = "(SELECT 1::UInt32 AS x, 2::UInt32 AS a, 3::UInt32 AS b, 'v' AS s, true AS c, " +
	"[1, 2]::Array(UInt32) AS arr, (1, 'a')::Tuple(UInt32, String) AS tup, " +
	"CAST(NULL, 'Nullable(String)') AS n, now64(9) AS ts, map('a', 1) AS m) AS t"

func TestLiveColumnNameCorpus(t *testing.T) {
	base := liveURL(t)
	for _, c := range loadColnameCorpus(t) {
		if strings.Contains(strings.ToUpper(c.expr), "SELECT") {
			continue // _subquery_N numbering depends on the FROM subquery
		}
		got, ok := chColumnName(t, base, liveTable, c.expr)
		if !ok {
			continue // the corpus was recorded against a table; some casts differ
		}
		if got != c.name {
			t.Errorf("ClickHouse now names %q %q (corpus says %q)", c.expr, got, c.name)
		}
	}
}

// TestLiveRandomPrecedence generates random numeric expressions and checks
// that our name (which encodes the parse tree) matches ClickHouse's.
func TestLiveRandomPrecedence(t *testing.T) {
	base := liveURL(t)
	seed := int64(42)
	if s := os.Getenv("SQLPARSE_LIVE_SEED"); s != "" {
		fmt.Sscan(s, &seed)
	}
	r := rand.New(rand.NewSource(seed))
	n := 400
	if s := os.Getenv("SQLPARSE_LIVE_N"); s != "" {
		fmt.Sscan(s, &n)
	}
	checked := 0
	for i := 0; i < n; i++ {
		expr := genExpr(r, 4)
		want, ok := chColumnName(t, base, liveTable, expr)
		if !ok {
			continue
		}
		checked++
		item, err := Parse("SELECT " + expr + " FROM t")
		if err != nil {
			t.Errorf("we reject %q which ClickHouse accepts: %v", expr, err)
			continue
		}
		e := item.(*SelectStmt).Columns[0].Expr
		if got := ColumnName(e); got != want {
			t.Errorf("%s\n ours: %s\n   CH: %s", expr, got, want)
		}
		if e2, err := ParseExpr(String(e)); err != nil || !astEqual(e, e2) {
			t.Errorf("round trip of %s via %s failed: %v", expr, String(e), err)
		}
	}
	if checked < n/2 {
		t.Fatalf("only %d of %d random expressions were accepted by ClickHouse", checked, n)
	}
	t.Logf("checked %d random expressions", checked)
}

func genExpr(r *rand.Rand, depth int) string {
	if depth == 0 || r.Intn(4) == 0 {
		leaves := []string{"x", "a", "b", "1", "2.5", "-3", "- 4", "-(5)", "0x10", "1e2", "1.50", "t.x", "NULL",
			"arr[1]", "tup.1", "(s LIKE 'a%')", "(s NOT ILIKE 'b')", "length(s)", "count()", "(x, a).2", "[x, 1][2]"}
		return leaves[r.Intn(len(leaves))]
	}
	sub := func() string { return genExpr(r, depth-1) }
	switch r.Intn(17) {
	case 13:
		return "arrayMap(z -> z + " + sub() + ", arr)[1]"
	case 14:
		return "CASE WHEN x > a THEN " + sub() + " ELSE " + sub() + " END"
	case 15:
		return "toUInt32(" + sub() + " AS al" + fmt.Sprint(r.Intn(1000)) + ")"
	case 16:
		return "(" + sub() + ", " + sub() + ") IN ((1, 2), (3, 4))"
	case 0:
		return "NOT " + sub()
	case 1:
		return "- " + sub()
	case 2:
		return "(" + sub() + ")"
	case 3:
		return sub() + " BETWEEN " + sub() + " AND " + sub()
	case 4:
		return sub() + []string{" IS NULL", " IS NOT NULL"}[r.Intn(2)]
	case 5:
		return sub() + []string{" IN (1, 2)", " NOT IN (3)", " IN (" + sub() + ")"}[r.Intn(3)]
	case 6:
		// Conditions are never constant: ClickHouse folds if(<const>, a, b).
		return "(x > a ? " + sub() + " : " + sub() + ")"
	case 7:
		return sub() + "::Float64"
	case 8:
		return "if(x < b, " + sub() + ", " + sub() + ")"
	default:
		ops := []string{"OR", "AND", "=", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "||", "==", "<>", "MOD", "DIV"}
		return sub() + " " + ops[r.Intn(len(ops))] + " " + sub()
	}
}

// liveStatements are checked for syntax agreement with ClickHouse's own
// parser (via formatQuery, which parses without executing).
var liveStatements = []string{
	"SELECT 1", "SELECT 1, 2, FROM t", "SELECT a b FROM t t1", "SELECT * FROM t FINAL", "SELECT * FROM t AS x FINAL",
	"SELECT * FROM t FINAL AS x", "SELECT 1 UNION ALL SELECT 2",
	"(SELECT 1) UNION ALL (SELECT 2 UNION ALL SELECT 3)", "SELECT 1 LIMIT 1 LIMIT 2", "SELECT 1 LIMIT 2 BY a LIMIT 1",
	"SELECT 1 LIMIT 1, 2", "SELECT 1 OFFSET 2", "SELECT TOP 5 a FROM t", "SELECT a FROM t ARRAY JOIN arr AS x",
	"SELECT a FROM t LEFT ARRAY JOIN arr", "SELECT * FROM a LEFT ANY JOIN b USING id", "SELECT * FROM a, b",
	"SELECT * FROM a JOIN b", "SELECT * FROM a GLOBAL ALL INNER JOIN b ON a.x = b.x",
	"WITH 1 AS x, y AS (SELECT 2) SELECT x", "WITH x AS (1 + 1) SELECT x", "SELECT x FROM t GROUP BY ALL",
	"SELECT x FROM t GROUP BY x WITH TOTALS HAVING x > 1 ORDER BY x DESC NULLS FIRST",
	"SELECT x FROM t SETTINGS a = 1 FORMAT JSON", "SELECT x FROM t FORMAT JSON SETTINGS a = 1",
	"SELECT 1 + 2 AS x * 3", "SELECT count(*) FROM t", "SELECT f(1,)", "SELECT (1, 2,)", "SELECT [1, 2,]",
	"SELECT 1;", "SELECT 1 2", "SELEC 1", "SELECT x::Array(String)", "SELECT a IS NULL::String",
	"SELECT c ? 1 : c ? 2 : 3", "SELECT INTERVAL '1 day'", "SELECT trim(LEADING FROM s)",
	"INSERT INTO t VALUES (1, 'a')", "INSERT INTO t (a, b) VALUES (1, 2), (3, 4)", "INSERT INTO TABLE db.t SELECT 1",
	"INSERT INTO t SETTINGS async_insert = 1 VALUES (1)", "INSERT INTO t FORMAT JSONEachRow {\"a\":1}",
	"CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY a", "CREATE TABLE IF NOT EXISTS db.t (a String, b UInt8 DEFAULT 1 COMMENT 'x' CODEC(ZSTD(1))) ENGINE = MergeTree() PARTITION BY a ORDER BY (a, b) PRIMARY KEY a SETTINGS index_granularity = 8192 COMMENT 'c'",
	"CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY tuple()", "CREATE TABLE t AS db.other",
	"CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY a, b", "CREATE TABLE t (a String, INDEX i a TYPE set(1) GRANULARITY 1) ENGINE = Memory",
	"CREATE TABLE t (a String NULL, b Nullable(String) NOT NULL) ENGINE = Memory", "CREATE TABLE t (x DEFAULT 1) ENGINE = Memory",
	"CREATE TABLE t ENGINE = Memory AS SELECT 1", "CREATE OR REPLACE TABLE t (a UInt8) ENGINE = Memory",
	"CREATE MATERIALIZED VIEW IF NOT EXISTS db.v TO db.t AS SELECT a FROM s", "CREATE MATERIALIZED VIEW v TO t (a String) AS SELECT a FROM s",
	"CREATE MATERIALIZED VIEW v ENGINE = Memory POPULATE AS SELECT 1", "CREATE VIEW v AS SELECT 1", "CREATE DATABASE IF NOT EXISTS d ENGINE = Atomic",
	"DROP TABLE IF EXISTS db.t SYNC", "DROP DATABASE d", "DROP VIEW v", "TRUNCATE TABLE IF EXISTS t", "TRUNCATE t",
	"SHOW TABLES FROM db LIKE 'x%'", "SHOW DATABASES", "SHOW CREATE TABLE t", "SHOW COLUMNS FROM t", "DESCRIBE TABLE t", "DESC t",
	"EXISTS TABLE t", "EXISTS DATABASE d", "USE d", "SET a = 1, b = 'x'", "OPTIMIZE TABLE t FINAL", "SYSTEM FLUSH LOGS",
	"ALTER TABLE t ADD COLUMN IF NOT EXISTS c String AFTER b, DROP COLUMN d", "ALTER TABLE t MODIFY COLUMN c String DEFAULT 'x'",
	"ALTER TABLE t DELETE WHERE a = 1", "ALTER TABLE t UPDATE a = 1 WHERE b = 2", "ALTER TABLE t MODIFY SETTING a = 1",
	"ALTER TABLE t COMMENT COLUMN c 'x'", "DELETE FROM t WHERE a = 1", "EXPLAIN SELECT 1", "EXPLAIN AST SELECT 1",
	"EXPLAIN SYNTAX SELECT 1", "EXPLAIN PLAN header = 1 SELECT 1",
}

// Deliberate differences: we accept `SELECT .5` (ClickHouse only lexes .5
// as a number after an operator) and column modifiers in any order; we
// reject bare UNION (ClickHouse parses it but fails at execution with
// EXPECTED_ALL_OR_DISTINCT unless union_default_mode is set).
func TestLiveSyntaxAgreement(t *testing.T) {
	base := liveURL(t)
	for _, sql := range liveStatements {
		chErr := chFormatQuery(t, base, sql)
		_, err := Parse(sql)
		ours := err == nil || cherr.Is(err, cherr.NotImplemented)
		theirs := chErr == ""
		if ours != theirs {
			t.Errorf("%q: we accept=%v (%v), ClickHouse accept=%v (%s)", sql, ours, err, theirs, truncate(chErr, 200))
		}
	}
}

// chFormatQuery returns "" if ClickHouse's parser accepts sql, else the error.
func chFormatQuery(t *testing.T, base, sql string) string {
	t.Helper()
	q := "SELECT formatQuery(" + quoteString(sql) + ") FORMAT TSV"
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(base+"/", "text/plain", strings.NewReader(q))
	if err != nil {
		t.Fatalf("clickhouse: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		return ""
	}
	return string(body)
}
