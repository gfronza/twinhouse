package sqlparse

import (
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

func TestInsertValues(t *testing.T) {
	st := mustParse(t, "INSERT INTO db.e (a, b, c) VALUES ({a:String}, {b:DateTime64(9)}, {c:Nullable(String)})")
	assertAST(t, "insert", st, &InsertStmt{
		Database: "db", Table: "e", Columns: []string{"a", "b", "c"}, Format: "Values",
		Values: [][]Expr{{param("a", "String"), param("b", "DateTime64(9)"), param("c", "Nullable(String)")}},
	})

	st = mustParse(t, "insert into TABLE t values (1, 'x', -2.5, NULL, [1,2], now()), (2,'y',3,'z',[],now()) (3, '', 0, NULL, [3], today());")
	ins := st.(*InsertStmt)
	if ins.Table != "t" || ins.Columns != nil || ins.Format != "Values" || len(ins.Values) != 3 {
		t.Fatalf("insert: %+v", ins)
	}
	assertAST(t, "row 0", ins.Values[0], []Expr{u(1), str("x"), &Literal{Value: -2.5, Raw: "-2.5"},
		&Literal{Value: nil, Raw: "NULL"}, &ArrayExpr{Elems: []Expr{u(1), u(2)}}, fn("now")})

	ins = mustParse(t, "INSERT INTO t VALUES ({'a': 1, 'b': -2}, {}, {1: [{'x': {p:String}}]})").(*InsertStmt)
	assertAST(t, "map literals", ins.Values[0], []Expr{
		&FuncCall{Name: "map", Args: []Expr{str("a"), u(1), str("b"), &Literal{Value: int64(-2), Raw: "-2"}}},
		&FuncCall{Name: "map", Args: []Expr{}},
		&FuncCall{Name: "map", Args: []Expr{u(1), &ArrayExpr{Elems: []Expr{
			&FuncCall{Name: "map", Args: []Expr{str("x"), param("p", "String")}}}}}},
	})
	expectErr(t, "SELECT {'a': 1}", cherr.SyntaxError, "position 9")

	for _, sql := range []string{"INSERT INTO t VALUES", "INSERT INTO t VALUES ", "INSERT INTO t (a) VALUES;"} {
		ins := mustParse(t, sql).(*InsertStmt)
		if ins.Format != "Values" || ins.Values != nil || ins.Data != "" {
			t.Errorf("%q: %+v", sql, ins)
		}
	}

	st = mustParse(t, "INSERT INTO t (a, `b c`) SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (1, 2)")
	ins = st.(*InsertStmt)
	assertAST(t, "settings", ins.Settings, []Setting{{"async_insert", u(1)}, {"wait_for_async_insert", u(0)}})
	assertAST(t, "cols", ins.Columns, []string{"a", "b c"})

	expectErr(t, "INSERT INTO t VALUES (1", cherr.SyntaxError, "closing round bracket")
	expectErr(t, "INSERT INTO t VALUES 1", cherr.SyntaxError, "opening round bracket")
	expectErr(t, "INSERT INTO t", cherr.SyntaxError, "VALUES, FORMAT, SELECT")
	expectErr(t, "INSERT t VALUES (1)", cherr.SyntaxError, "INTO")
	expectErr(t, "INSERT INTO FUNCTION s3('x') VALUES (1)", cherr.NotImplemented, "FUNCTION")
}

func TestInsertFormatData(t *testing.T) {
	tests := []struct {
		sql, format, data string
	}{
		{"INSERT INTO db.e FORMAT JSONEachRow\n{\"a\":1}\n{\"a\":2}", "JSONEachRow", "{\"a\":1}\n{\"a\":2}"},
		{"INSERT INTO e FORMAT JSONEachRow", "JSONEachRow", ""},
		{"INSERT INTO e FORMAT JSONEachRow   ", "JSONEachRow", ""},
		{"INSERT INTO e FORMAT TSV\r\n  a\tb\n", "TSV", "  a\tb\n"},
		{"INSERT INTO e FORMAT TSV  \n\nx", "TSV", "\nx"},
		{"INSERT INTO e FORMAT CSV 1,'unterminated", "CSV", "1,'unterminated"},
		{"INSERT INTO e FORMAT Values (1, 'a') -- not a comment", "Values", "(1, 'a') -- not a comment"},
		{"INSERT INTO e FORMAT JSONEachRow;", "JSONEachRow", ""},
		{"INSERT INTO e (a) FORMAT RowBinary\n\x00\x01\xff", "RowBinary", "\x00\x01\xff"},
		{"INSERT INTO e SETTINGS x = 1 FORMAT Native\n", "Native", ""},
		{"/* c */ INSERT INTO e FORMAT `CSVWithNames`\na,b", "CSVWithNames", "a,b"},
	}
	for _, tt := range tests {
		ins := mustParse(t, tt.sql).(*InsertStmt)
		if ins.Format != tt.format || ins.Data != tt.data || ins.Values != nil {
			t.Errorf("%q: format=%q data=%q values=%v", tt.sql, ins.Format, ins.Data, ins.Values)
		}
	}
	expectErr(t, "INSERT INTO e FORMAT JSONEachRow; {\"a\":1}", cherr.SyntaxError, "excessive ';'")
	expectErr(t, "INSERT INTO e FORMAT", cherr.SyntaxError, "format name")
}

func TestInsertSelect(t *testing.T) {
	ins := mustParse(t, "INSERT INTO db.t (a, b) SELECT a, b FROM db.s WHERE c > 1").(*InsertStmt)
	if ins.Select == nil || ins.Format != "" || ins.Select.Where == nil {
		t.Fatalf("insert select: %+v", ins)
	}
	ins = mustParse(t, "INSERT INTO t WITH 1 AS x SELECT x").(*InsertStmt)
	if ins.Select == nil || len(ins.Select.With) != 1 {
		t.Fatalf("insert with select: %+v", ins)
	}
	ins = mustParse(t, "INSERT INTO t (SELECT 1 UNION ALL SELECT 2)").(*InsertStmt)
	if ins.Select == nil || ins.Columns != nil || len(ins.Select.Union) != 1 {
		t.Fatalf("insert (select): %+v", ins)
	}
}

const createEvents = `CREATE TABLE IF NOT EXISTS analytics.events ON CLUSTER main
(
    -- identity
    event_id   UUID DEFAULT generateUUIDv4() COMMENT 'unique id',
    tenant     LowCardinality( Nullable( String ) ),
    ts         DateTime64(9, 'UTC') DEFAULT now64(9) CODEC(Delta, ZSTD(1)),
    kind       Enum8('add' = 1, 'remove' = -1),
    quals      Array(Tuple(qualification_id UInt32, precodes Array(String))), -- nested
    total      SimpleAggregateFunction(sum, UInt64),
    uniq_state AggregateFunction(uniqExact, String),
    day        Date MATERIALIZED toDate(ts),
    label      String ALIAS concat(tenant, '-', kind),
    amount     Decimal(18, 4) TTL ts + INTERVAL 1 YEAR,
    note       String NULL,
    flag       Bool NOT NULL,
    tmp        UInt8 EPHEMERAL,
    INDEX idx_kind kind TYPE set(10) GRANULARITY 4,
    PROJECTION p (SELECT tenant, count() GROUP BY tenant)
)
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMM(ts)
ORDER BY (tenant, ts, event_id)
PRIMARY KEY (tenant, ts)
TTL ts + INTERVAL 3 YEAR DELETE, ts + INTERVAL 1 YEAR TO VOLUME 'cold'
SETTINGS index_granularity = 8192, dynamodb_partition_key = 'tenant'
COMMENT 'events table';`

func TestCreateTable(t *testing.T) {
	ct := mustParse(t, createEvents).(*CreateTableStmt)
	if !ct.IfNotExists || ct.Database != "analytics" || ct.Name != "events" || ct.OrReplace {
		t.Fatalf("header: %+v", ct)
	}
	wantTypes := []struct{ name, typ, kind, def string }{
		{"event_id", "UUID", "DEFAULT", "generateUUIDv4()"},
		{"tenant", "LowCardinality(Nullable(String))", "", ""},
		{"ts", "DateTime64(9, 'UTC')", "DEFAULT", "now64(9)"},
		{"kind", "Enum8('add' = 1, 'remove' = -1)", "", ""},
		{"quals", "Array(Tuple(qualification_id UInt32, precodes Array(String)))", "", ""},
		{"total", "SimpleAggregateFunction(sum, UInt64)", "", ""},
		{"uniq_state", "AggregateFunction(uniqExact, String)", "", ""},
		{"day", "Date", "MATERIALIZED", "toDate(ts)"},
		{"label", "String", "ALIAS", "concat(tenant, '-', kind)"},
		{"amount", "Decimal(18, 4)", "", ""},
		{"note", "Nullable(String)", "", ""},
		{"flag", "Bool", "", ""},
		{"tmp", "UInt8", "EPHEMERAL", ""},
	}
	if len(ct.Columns) != len(wantTypes) {
		t.Fatalf("got %d columns: %+v", len(ct.Columns), ct.Columns)
	}
	for i, w := range wantTypes {
		c := ct.Columns[i]
		if c.Name != w.name || c.Type != w.typ || c.DefaultKind != w.kind || c.DefaultText != w.def {
			t.Errorf("column %d = {%q %q %q %q}, want %+v", i, c.Name, c.Type, c.DefaultKind, c.DefaultText, w)
		}
		if (c.Default == nil) != (w.def == "") {
			t.Errorf("column %s default expr = %v", c.Name, c.Default)
		}
	}
	if ct.Columns[0].Comment != "unique id" || ct.Columns[2].Codec != "Delta, ZSTD(1)" || ct.Columns[9].TTL != "ts + INTERVAL 1 YEAR" {
		t.Errorf("modifiers: %+v %+v %+v", ct.Columns[0], ct.Columns[2], ct.Columns[9])
	}
	assertAST(t, "engine", ct.Engine, &EngineClause{Name: "ReplacingMergeTree", Args: []Expr{id("ts")}})
	assertAST(t, "partition", ct.PartitionBy, fn("toYYYYMM", id("ts")))
	assertAST(t, "order", ct.OrderBy, []Expr{id("tenant"), id("ts"), id("event_id")})
	assertAST(t, "pk", ct.PrimaryKey, []Expr{id("tenant"), id("ts")})
	if ct.TTL != "ts + INTERVAL 3 YEAR DELETE, ts + INTERVAL 1 YEAR TO VOLUME 'cold'" {
		t.Errorf("ttl %q", ct.TTL)
	}
	assertAST(t, "settings", ct.Settings, []Setting{{"index_granularity", u(8192)}, {"dynamodb_partition_key", str("tenant")}})
	if ct.Comment != "events table" {
		t.Errorf("comment %q", ct.Comment)
	}
	if want := strings.TrimSuffix(createEvents, ";"); ct.Text != want {
		t.Errorf("Text not the exact original:\n%q\n%q", ct.Text, want)
	}
}

func TestCreateTableForms(t *testing.T) {
	ct := mustParse(t, "CREATE TABLE t (a String, b UInt64) ENGINE = MergeTree() ORDER BY tuple()").(*CreateTableStmt)
	if ct.OrderBy == nil || len(ct.OrderBy) != 0 {
		t.Errorf("ORDER BY tuple() = %#v, want empty non-nil", ct.OrderBy)
	}
	assertAST(t, "engine", ct.Engine, &EngineClause{Name: "MergeTree"})

	ct = mustParse(t, "CREATE TABLE t (a String) ENGINE MergeTree ORDER BY a").(*CreateTableStmt)
	assertAST(t, "order col", ct.OrderBy, []Expr{id("a")})
	assertAST(t, "engine", ct.Engine, &EngineClause{Name: "MergeTree"})

	ct = mustParse(t, "CREATE TABLE t (a String) ENGINE = SummingMergeTree((x, y)) ORDER BY (a)").(*CreateTableStmt)
	assertAST(t, "summing", ct.Engine, &EngineClause{Name: "SummingMergeTree", Args: []Expr{&TupleExpr{Elems: []Expr{id("x"), id("y")}}}})
	assertAST(t, "order paren", ct.OrderBy, []Expr{id("a")})

	ct = mustParse(t, "CREATE TABLE t (a String, b UInt8 PRIMARY KEY) ENGINE = Memory").(*CreateTableStmt)
	assertAST(t, "column pk", ct.PrimaryKey, []Expr{id("b")})
	if ct.OrderBy != nil {
		t.Errorf("order: %v", ct.OrderBy)
	}

	ct = mustParse(t, "CREATE TABLE t (a String, PRIMARY KEY (a)) ENGINE = MergeTree").(*CreateTableStmt)
	assertAST(t, "pk entry", ct.PrimaryKey, []Expr{id("a")})

	ct = mustParse(t, "CREATE TABLE db2.t2 AS db.other").(*CreateTableStmt)
	if ct.AsDatabase != "db" || ct.AsTable != "other" || ct.Columns != nil || ct.Engine != nil {
		t.Errorf("as table: %+v", ct)
	}
	ct = mustParse(t, "CREATE TABLE t2 AS other ENGINE = Log").(*CreateTableStmt)
	if ct.AsTable != "other" || ct.Engine == nil || ct.Engine.Name != "Log" {
		t.Errorf("as table + engine: %+v", ct)
	}
	ct = mustParse(t, "CREATE OR REPLACE TABLE t ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a").(*CreateTableStmt)
	if !ct.OrReplace || ct.AsSelect == nil || len(ct.OrderBy) != 1 {
		t.Errorf("as select: %+v", ct)
	}
	ct = mustParse(t, "CREATE TABLE t (x DEFAULT 1, `my col` Nullable(Int64), \"q\" Map(String, Array(UInt8))) ENGINE = Memory").(*CreateTableStmt)
	if c := ct.Columns[0]; c.Type != "" || c.DefaultKind != "DEFAULT" || c.DefaultText != "1" {
		t.Errorf("untyped default: %+v", c)
	}
	if c := ct.Columns[1]; c.Name != "my col" || c.Type != "Nullable(Int64)" {
		t.Errorf("quoted: %+v", c)
	}
	if c := ct.Columns[2]; c.Name != "q" || c.Type != "Map(String, Array(UInt8))" {
		t.Errorf("map: %+v", c)
	}
	ct = mustParse(t, "CREATE TABLE t (d Double Precision, s String) ENGINE = Memory").(*CreateTableStmt)
	if ct.Columns[0].Type != "Double Precision" {
		t.Errorf("multi-word type %q", ct.Columns[0].Type)
	}

	expectErr(t, "CREATE TABLE t (a String, CONSTRAINT c CHECK a != '') ENGINE = Memory", cherr.NotImplemented, "constraints")
	expectErr(t, "CREATE TABLE t (a String", cherr.SyntaxError, "")
	expectErr(t, "CREATE TABLE t (a String, b String) ENGINE = MergeTree ORDER BY a, b", cherr.SyntaxError, "position 66")
	expectErr(t, "CREATE TABLE t AS numbers(10)", cherr.NotImplemented, "table function")
	expectErr(t, "CREATE TEMPORARY TABLE t (a String)", cherr.NotImplemented, "TEMPORARY")
	expectErr(t, "CREATE DICTIONARY d (a String) PRIMARY KEY a", cherr.NotImplemented, "DICTIONARY")
	expectErr(t, "CREATE TABLE t (id UInt64 AUTO_INCREMENT) ENGINE = Memory", cherr.NotImplemented, "AUTO_INCREMENT")
}

func TestCreateDatabase(t *testing.T) {
	assertAST(t, "db", mustParse(t, "CREATE DATABASE IF NOT EXISTS analytics"), &CreateDatabaseStmt{Name: "analytics", IfNotExists: true})
	assertAST(t, "db engine", mustParse(t, "CREATE DATABASE d ON CLUSTER c ENGINE = Atomic COMMENT 'x'"), &CreateDatabaseStmt{Name: "d", Engine: "Atomic"})
	assertAST(t, "db engine args", mustParse(t, "CREATE DATABASE d ENGINE = Replicated('/p', '{shard}')"), &CreateDatabaseStmt{Name: "d", Engine: "Replicated('/p', '{shard}')"})
}

func TestCreateMaterializedView(t *testing.T) {
	sql := `CREATE MATERIALIZED VIEW IF NOT EXISTS db.mv_counts TO db.counts
AS SELECT tenant, toStartOfHour(ts) AS hour, countState() AS c, uniqExactState(user) AS u
FROM db.events
WHERE kind = 'add'
GROUP BY tenant, hour`
	cv := mustParse(t, sql+";").(*CreateViewStmt)
	if !cv.Materialized || !cv.IfNotExists || cv.Database != "db" || cv.Name != "mv_counts" ||
		cv.ToDatabase != "db" || cv.ToTable != "counts" || cv.Populate || cv.Engine != nil {
		t.Fatalf("mv: %+v", cv)
	}
	if cv.Text != sql {
		t.Errorf("Text %q", cv.Text)
	}
	if want := sql[strings.Index(sql, "SELECT"):]; cv.SelectText != want {
		t.Errorf("SelectText %q, want %q", cv.SelectText, want)
	}
	if len(cv.Select.Columns) != 4 || cv.Select.From.Table != "events" || len(cv.Select.GroupBy) != 2 {
		t.Errorf("select: %s", SelectString(cv.Select))
	}
	// The stored SelectText reparses to the same AST.
	again := mustParse(t, cv.SelectText)
	assertAST(t, "reparse", again, cv.Select)

	cv = mustParse(t, "CREATE MATERIALIZED VIEW v TO t (a String, n UInt64) POPULATE AS SELECT a, count() AS n FROM s GROUP BY a").(*CreateViewStmt)
	if !cv.Populate || len(cv.Columns) != 2 || cv.Columns[1].Type != "UInt64" {
		t.Errorf("mv cols/populate: %+v", cv)
	}

	cv = mustParse(t, "CREATE MATERIALIZED VIEW v ENGINE = AggregatingMergeTree ORDER BY a POPULATE AS SELECT a, sumState(x) AS s FROM t GROUP BY a").(*CreateViewStmt)
	if cv.Engine == nil || cv.Engine.Name != "AggregatingMergeTree" || len(cv.OrderBy) != 1 || !cv.Populate || cv.ToTable != "" {
		t.Errorf("inner engine mv: %+v", cv)
	}

	cv = mustParse(t, "CREATE OR REPLACE VIEW v AS WITH 1 AS x SELECT x").(*CreateViewStmt)
	if cv.Materialized || !cv.OrReplace || cv.SelectText != "WITH 1 AS x SELECT x" || len(cv.Select.With) != 1 {
		t.Errorf("view: %+v", cv)
	}
	cv = mustParse(t, "CREATE VIEW v AS (SELECT 1)").(*CreateViewStmt)
	if cv.SelectText != "(SELECT 1)" {
		t.Errorf("paren view text %q", cv.SelectText)
	}

	expectErr(t, "CREATE MATERIALIZED VIEW v TO t ENGINE = Memory AS SELECT 1", cherr.SyntaxError, "TO")
	expectErr(t, "CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR AS SELECT 1", cherr.NotImplemented, "Refreshable")
	expectErr(t, "CREATE LIVE VIEW v AS SELECT 1", cherr.NotImplemented, "LIVE")
	expectErr(t, "CREATE MATERIALIZED VIEW v TO t SELECT 1", cherr.SyntaxError, "AS")
}

func TestDropTruncate(t *testing.T) {
	tests := []struct {
		sql  string
		want Statement
	}{
		{"DROP TABLE t", &DropStmt{Kind: "TABLE", Name: "t"}},
		{"DROP TABLE IF EXISTS db.t SYNC", &DropStmt{Kind: "TABLE", IfExists: true, Database: "db", Name: "t", Sync: true}},
		{"drop view db.v on cluster c", &DropStmt{Kind: "VIEW", Database: "db", Name: "v"}},
		{"DROP DATABASE IF EXISTS analytics", &DropStmt{Kind: "DATABASE", IfExists: true, Name: "analytics"}},
		{"DROP DICTIONARY d", &DropStmt{Kind: "DICTIONARY", Name: "d"}},
		{"DROP TEMPORARY TABLE t NO DELAY", &DropStmt{Kind: "TABLE", Name: "t", Sync: true}},
		{"TRUNCATE t", &TruncateStmt{Name: "t"}},
		{"TRUNCATE TABLE IF EXISTS db.t", &TruncateStmt{IfExists: true, Database: "db", Name: "t"}},
	}
	for _, tt := range tests {
		assertAST(t, tt.sql, mustParse(t, tt.sql), tt.want)
	}
	expectErr(t, "DROP USER u", cherr.NotImplemented, "USER")
	expectErr(t, "DROP TABLE a, b", cherr.NotImplemented, "several")
	expectErr(t, "TRUNCATE DATABASE d", cherr.NotImplemented, "DATABASE")
}

func TestShowDescribeExistsUseSet(t *testing.T) {
	tests := []struct {
		sql  string
		want Statement
	}{
		{"SHOW DATABASES", &ShowStmt{Kind: "DATABASES"}},
		{"SHOW DATABASES LIKE 'a%' FORMAT JSON", &ShowStmt{Kind: "DATABASES", Like: "a%", Format: "JSON"}},
		{"SHOW TABLES", &ShowStmt{Kind: "TABLES"}},
		{"SHOW TABLES FROM db NOT LIKE '%tmp%'", &ShowStmt{Kind: "TABLES", Database: "db", Like: "%tmp%", NotLike: true}},
		{"SHOW TABLES IN db", &ShowStmt{Kind: "TABLES", Database: "db"}},
		{"SHOW CREATE TABLE db.t", &ShowStmt{Kind: "CREATE TABLE", Database: "db", Name: "t"}},
		{"SHOW CREATE t FORMAT TSVRaw", &ShowStmt{Kind: "CREATE TABLE", Name: "t", Format: "TSVRaw"}},
		{"SHOW CREATE VIEW v", &ShowStmt{Kind: "CREATE VIEW", Name: "v"}},
		{"SHOW CREATE DATABASE d", &ShowStmt{Kind: "CREATE DATABASE", Name: "d"}},
		{"SHOW COLUMNS FROM t FROM db", &ShowStmt{Kind: "COLUMNS", Database: "db", Name: "t"}},
		{"SHOW COLUMNS IN db.t LIKE 'a%'", &ShowStmt{Kind: "COLUMNS", Database: "db", Name: "t", Like: "a%"}},
		{"SHOW PROCESSLIST", &ShowStmt{Kind: "PROCESSLIST"}},
		{"DESCRIBE t", &DescribeStmt{Name: "t"}},
		{"DESC TABLE db.t FORMAT JSON", &DescribeStmt{Database: "db", Name: "t", Format: "JSON"}},
		{"EXISTS t", &ExistsStmt{Kind: "TABLE", Name: "t"}},
		{"EXISTS TABLE db.t FORMAT TSV", &ExistsStmt{Kind: "TABLE", Database: "db", Name: "t", Format: "TSV"}},
		{"EXISTS DATABASE d", &ExistsStmt{Kind: "DATABASE", Name: "d"}},
		{"EXISTS VIEW db.v", &ExistsStmt{Kind: "VIEW", Database: "db", Name: "v"}},
		{"USE analytics", &UseStmt{Database: "analytics"}},
		{"USE `my db`;", &UseStmt{Database: "my db"}},
		{"SET max_threads = 4, mode = 'x', flag = true", &SetStmt{Settings: []Setting{
			{"max_threads", u(4)}, {"mode", str("x")}, {"flag", &Literal{Value: true, Raw: "true"}}}}},
		{"SET param_x = -1", &SetStmt{Settings: []Setting{{"param_x", &Literal{Value: int64(-1), Raw: "-1"}}}}},
	}
	for _, tt := range tests {
		assertAST(t, tt.sql, mustParse(t, tt.sql), tt.want)
	}
	expectErr(t, "SHOW TABLES LIMIT 3", cherr.NotImplemented, "LIMIT")
	expectErr(t, "SHOW TABLES ILIKE 'x'", cherr.NotImplemented, "ILIKE")
	expectErr(t, "SHOW GRANTS", cherr.NotImplemented, "GRANTS")
	expectErr(t, "DESCRIBE (SELECT 1)", cherr.NotImplemented, "subquery")
	expectErr(t, "SET ROLE admin", cherr.NotImplemented, "ROLE")
	expectErr(t, "SET x", cherr.SyntaxError, "equals sign")
}

func TestOptimizeSystem(t *testing.T) {
	assertAST(t, "optimize", mustParse(t, "OPTIMIZE TABLE db.t FINAL"), &OptimizeStmt{Database: "db", Name: "t", Final: true})
	assertAST(t, "optimize partition", mustParse(t, "OPTIMIZE TABLE t PARTITION (202401) DEDUPLICATE BY a, b"), &OptimizeStmt{Name: "t"})
	assertAST(t, "system", mustParse(t, "SYSTEM  FLUSH LOGS ;"), &SystemStmt{Command: "FLUSH LOGS"})
	assertAST(t, "system multi", mustParse(t, "SYSTEM STOP MERGES db.t"), &SystemStmt{Command: "STOP MERGES db.t"})
	expectErr(t, "OPTIMIZE t", cherr.SyntaxError, "TABLE")
}

func TestAlter(t *testing.T) {
	st := mustParse(t, `ALTER TABLE db.t ON CLUSTER c
		ADD COLUMN IF NOT EXISTS c1 Nullable(String) DEFAULT 'x' AFTER b,
		ADD COLUMN c2 UInt8 FIRST,
		DROP COLUMN IF EXISTS old,
		MODIFY COLUMN c3 LowCardinality(String) COMMENT 'lc',
		MODIFY COLUMN c4 REMOVE DEFAULT,
		COMMENT COLUMN c5 'hello',
		DELETE WHERE ts < now() - INTERVAL 1 DAY,
		UPDATE a = 1, b = b + 1 WHERE id = 5,
		MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600,
		RENAME COLUMN x TO y`).(*AlterStmt)
	if st.Database != "db" || st.Name != "t" || len(st.Commands) != 10 {
		t.Fatalf("alter: %+v", st)
	}
	c := st.Commands
	if c[0].Kind != "ADD COLUMN" || !c[0].IfNotExists || c[0].Column.Type != "Nullable(String)" || c[0].Column.DefaultText != "'x'" || c[0].After != "b" {
		t.Errorf("add: %+v %+v", c[0], c[0].Column)
	}
	if c[1].Kind != "ADD COLUMN" || !c[1].First || c[1].ColumnName != "c2" {
		t.Errorf("add first: %+v", c[1])
	}
	if c[2].Kind != "DROP COLUMN" || !c[2].IfExists || c[2].ColumnName != "old" {
		t.Errorf("drop: %+v", c[2])
	}
	if c[3].Kind != "MODIFY COLUMN" || c[3].Column.Type != "LowCardinality(String)" || c[3].Column.Comment != "lc" {
		t.Errorf("modify: %+v", c[3])
	}
	if c[4].Kind != "OTHER" || c[4].Raw != "MODIFY COLUMN c4 REMOVE DEFAULT" {
		t.Errorf("modify remove: %+v", c[4])
	}
	if c[5].Kind != "COMMENT COLUMN" || c[5].ColumnName != "c5" || c[5].Column.Comment != "hello" {
		t.Errorf("comment: %+v", c[5])
	}
	if c[6].Kind != "DELETE" || c[6].Where == nil {
		t.Errorf("delete: %+v", c[6])
	}
	assertAST(t, "update", c[7].Assignments, []Setting{{"a", u(1)}, {"b", bin("+", id("b"), u(1))}})
	assertAST(t, "update where", c[7].Where, bin("=", id("id"), u(5)))
	assertAST(t, "modify setting", c[8].Assignments, []Setting{{"ttl_only_drop_parts", u(1)}, {"merge_with_ttl_timeout", u(3600)}})
	if c[9].Kind != "OTHER" || c[9].Raw != "RENAME COLUMN x TO y" {
		t.Errorf("other: %+v", c[9])
	}
	expectErr(t, "ALTER USER u", cherr.NotImplemented, "USER")
	expectErr(t, "ALTER TABLE t", cherr.SyntaxError, "ALTER command")
	expectErr(t, "ALTER TABLE t UPDATE a = 1", cherr.SyntaxError, "WHERE")
}

func TestDeleteExplain(t *testing.T) {
	assertAST(t, "delete", mustParse(t, "DELETE FROM db.t WHERE id = {id:String}"),
		&DeleteStmt{Database: "db", Name: "t", Where: bin("=", id("id"), param("id", "String"))})
	expectErr(t, "DELETE FROM t", cherr.SyntaxError, "WHERE")

	ex := mustParse(t, "EXPLAIN SELECT 1").(*ExplainStmt)
	if ex.Kind != "" || ex.Stmt == nil {
		t.Errorf("explain: %+v", ex)
	}
	for sql, kind := range map[string]string{
		"EXPLAIN AST SELECT 1":                 "AST",
		"EXPLAIN SYNTAX SELECT 1":              "SYNTAX",
		"EXPLAIN PLAN header = 1 SELECT 1":     "PLAN",
		"EXPLAIN PIPELINE SELECT 1":            "PIPELINE",
		"EXPLAIN QUERY TREE SELECT 1":          "QUERY TREE",
		"EXPLAIN ESTIMATE SELECT 1 FROM t":     "ESTIMATE",
		"EXPLAIN AST INSERT INTO t VALUES (1)": "AST",
	} {
		ex := mustParse(t, sql).(*ExplainStmt)
		if ex.Kind != kind || ex.Stmt == nil {
			t.Errorf("%s: %+v", sql, ex)
		}
	}
}

func TestUnknownAndUnsupportedStatements(t *testing.T) {
	expectErr(t, "SELEC 1", cherr.SyntaxError, "failed at position 1 (SELEC)")
	expectErr(t, "", cherr.SyntaxError, "end of query")
	expectErr(t, "   ;", cherr.SyntaxError, "")
	expectErr(t, "42", cherr.SyntaxError, "")
	for _, sql := range []string{"RENAME TABLE a TO b", "GRANT SELECT ON *.* TO u", "KILL QUERY WHERE 1", "ATTACH TABLE t"} {
		expectErr(t, sql, cherr.NotImplemented, "")
	}
}

func TestSyntaxErrorFormat(t *testing.T) {
	_, err := Parse("SELECT 1,\n  2 FROM t WHERE ) x")
	ce := err.(*cherr.Error)
	want := "Syntax error: failed at position 28 ()) (line 2, col 18): ) x. Expected expression"
	if ce.Code != cherr.SyntaxError || ce.Msg != want {
		t.Errorf("got %q\nwant %q", ce.Msg, want)
	}
	_, err = Parse("SELECT 'abc")
	ce = err.(*cherr.Error)
	if want := "Single quoted string is not closed: Syntax error: failed at position 8 ('abc): 'abc."; ce.Msg != want {
		t.Errorf("got %q\nwant %q", ce.Msg, want)
	}
	if ce.HTTPStatus() != 400 {
		t.Errorf("status %d", ce.HTTPStatus())
	}
	_, err = Parse("SELECT 1 + 'abc' 'x' FROM t")
	if want := "failed at position 18 ('x'): 'x' FROM t. Expected end of query"; !strings.Contains(err.Error(), want) {
		t.Errorf("got %q", err)
	}
	// Excerpts of long queries are truncated.
	_, err = Parse("SELECT ) " + strings.Repeat("x", 500))
	if len(err.Error()) > 400 {
		t.Errorf("unbounded error message (%d bytes)", len(err.Error()))
	}
}

func TestDeepNestingIsBounded(t *testing.T) {
	deep := strings.Repeat("(", 100000) + "1" + strings.Repeat(")", 100000)
	expectErr(t, "SELECT "+deep, cherr.SyntaxError, "depth")
	mustParse(t, "SELECT "+strings.Repeat("(", 100)+"1"+strings.Repeat(")", 100))
	mustParse(t, "SELECT "+strings.Repeat("-", 0)+strings.Repeat("NOT ", 100)+"1")
}
