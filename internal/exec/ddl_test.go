package exec

import (
	"reflect"
	"strings"
	"testing"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
)

func TestCreateTableCatalogMapping(t *testing.T) {
	cases := []struct {
		name, ddl string
		check     func(t *testing.T, tb *catalog.Table)
	}{
		{"mergetree_defaults", `CREATE TABLE t (a String, b UInt64, c DateTime64(9) DEFAULT now64(9) COMMENT 'when', d String MATERIALIZED upper(a) CODEC(ZSTD(1)), e String ALIAS lower(a)) ENGINE = MergeTree PARTITION BY toYYYYMM(c) ORDER BY (a, b, c) SETTINGS index_granularity = 8192`,
			func(t *testing.T, tb *catalog.Table) {
				eq(t, tb.Engine, catalog.EngineMergeTree)
				eq(t, tb.OrderBy, []string{"a", "b", "c"})
				eq(t, tb.PartitionKey, []string{"a"})
				eq(t, tb.SortKey, []string{"b", "c"})
				eq(t, tb.Replication, catalog.ReplicationGlobal)
				eq(t, tb.Settings["index_granularity"], "8192")
				eq(t, tb.Columns[2].DefaultKind, "DEFAULT")
				eq(t, tb.Columns[2].DefaultExpr, "now64(9)")
				eq(t, tb.Columns[2].Comment, "when")
				eq(t, tb.Columns[2].TypeText, "DateTime64(9)")
				eq(t, tb.Columns[3].DefaultKind, "MATERIALIZED")
				eq(t, tb.Columns[4].DefaultKind, "ALIAS")
			}},
		{"partition_key_setting", `CREATE TABLE t (a String, b UInt64, c UInt8) ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS dynamodb_partition_key = 'a, b', dynamodb_replication = 'regional'`,
			func(t *testing.T, tb *catalog.Table) {
				eq(t, tb.PartitionKey, []string{"a", "b"})
				eq(t, tb.SortKey, []string{"c"})
				eq(t, tb.Replication, catalog.ReplicationRegional)
			}},
		{"replacing_version", `CREATE TABLE t (id String, ver UInt64, v String) ENGINE = ReplacingMergeTree(ver) ORDER BY id`,
			func(t *testing.T, tb *catalog.Table) {
				eq(t, tb.Engine, catalog.EngineReplacingMergeTree)
				eq(t, tb.VersionColumn, "ver")
				eq(t, tb.EngineArgs, []string{"ver"})
				eq(t, tb.PartitionKey, []string{"id"})
				eq(t, len(tb.SortKey), 0)
			}},
		{"replacing_no_version", `CREATE TABLE t (id String, v String) ENGINE = ReplacingMergeTree ORDER BY id`,
			func(t *testing.T, tb *catalog.Table) { eq(t, tb.VersionColumn, "") }},
		{"replicated_prefix", `CREATE TABLE t (id String, ver DateTime) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/{shard}/t', '{replica}', ver) ORDER BY id`,
			func(t *testing.T, tb *catalog.Table) {
				eq(t, tb.Engine, catalog.EngineReplacingMergeTree)
				eq(t, tb.VersionColumn, "ver")
			}},
		{"aggregating_regional", `CREATE TABLE t (k String, s SimpleAggregateFunction(sum, UInt64), u AggregateFunction(uniqExact, String)) ENGINE = AggregatingMergeTree ORDER BY k`,
			func(t *testing.T, tb *catalog.Table) {
				eq(t, tb.Replication, catalog.ReplicationRegional)
				eq(t, tb.Columns[2].TypeText, "AggregateFunction(uniqExact, String)")
			}},
		{"summing_columns", `CREATE TABLE t (k String, a UInt64, b Int32, c Float64) ENGINE = SummingMergeTree((a, b)) ORDER BY k`,
			func(t *testing.T, tb *catalog.Table) {
				eq(t, tb.SumColumns, []string{"a", "b"})
				eq(t, tb.Replication, catalog.ReplicationRegional)
			}},
		{"primary_key_only", `CREATE TABLE t (a String, b UInt8) ENGINE = MergeTree PRIMARY KEY (a, b)`,
			func(t *testing.T, tb *catalog.Table) { eq(t, tb.OrderBy, []string{"a", "b"}) }},
		{"tuple_order", `CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY tuple()`,
			func(t *testing.T, tb *catalog.Table) {
				eq(t, len(tb.OrderBy), 0)
				eq(t, len(tb.PartitionKey), 0)
			}},
		{"memory", `CREATE TABLE t (a String) ENGINE = Memory`,
			func(t *testing.T, tb *catalog.Table) { eq(t, tb.Engine, catalog.EngineMemory) }},
		{"type_from_default", `CREATE TABLE t (a UInt8, b DEFAULT 'x') ENGINE = MergeTree ORDER BY a`,
			func(t *testing.T, tb *catalog.Table) { eq(t, tb.Columns[1].TypeText, "String") }},
		{"aliases_types", `CREATE TABLE t (a INT, b BIGINT, c VARCHAR(10), d DOUBLE, e BOOLEAN, f TEXT) ENGINE = MergeTree ORDER BY a`,
			func(t *testing.T, tb *catalog.Table) {
				got := []string{}
				for _, c := range tb.Columns {
					got = append(got, c.TypeText)
				}
				eq(t, got, []string{"Int32", "Int64", "String", "Float64", "Bool", "String"})
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			te := newTestEnv(t)
			te.exec(c.ddl)
			tb, err := te.cat.Table("default", "t")
			if err != nil {
				t.Fatal(err)
			}
			if !te.store.HasTable(tb) {
				t.Fatal("physical table was not created")
			}
			eq(t, tb.DDL, c.ddl)
			c.check(t, tb)
		})
	}
}

func TestCreateTableErrors(t *testing.T) {
	cases := []struct {
		ddl  string
		code int
	}{
		{`CREATE TABLE t (a String) ENGINE = MergeTree`, cherr.NumberOfArgsDoesntMatch},
		{`CREATE TABLE t (a Nullable(String)) ENGINE = MergeTree ORDER BY a`, cherr.IllegalColumn},
		{`CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY b`, cherr.UnknownIdentifier},
		{`CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY lower(a)`, cherr.NotImplemented},
		{`CREATE TABLE t (a String, v UInt64) ENGINE = AggregatingMergeTree ORDER BY a`, cherr.NotImplemented},
		{`CREATE TABLE t (a String) ENGINE = Kafka`, cherr.NotImplemented},
		{`CREATE TABLE t (a String, b String) ENGINE = MergeTree ORDER BY (a, b) PRIMARY KEY b`, cherr.BadArguments},
		{`CREATE TABLE t (a String, a UInt8) ENGINE = MergeTree ORDER BY a`, duplicateColumn},
		{`CREATE TABLE t (_pk String) ENGINE = MergeTree ORDER BY _pk`, cherr.BadArguments},
		{`CREATE TABLE nodb.t (a String) ENGINE = MergeTree ORDER BY a`, cherr.UnknownDatabase},
		{`CREATE TABLE t (a String, v String) ENGINE = ReplacingMergeTree(v) ORDER BY a`, cherr.BadArguments},
		{`CREATE TABLE t (a String, v String) ENGINE = ReplacingMergeTree(nope) ORDER BY a`, cherr.UnknownIdentifier},
		{`CREATE TABLE t (a String, b UInt8) ENGINE = MergeTree ORDER BY (a, b) SETTINGS dynamodb_partition_key = 'b'`, cherr.BadArguments},
		{`CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY a SETTINGS dynamodb_replication = 'everywhere'`, cherr.BadArguments},
		{`CREATE TABLE t (a String, s AggregateFunction(quantile(0.5), UInt64)) ENGINE = AggregatingMergeTree ORDER BY a`, 0},
		{`CREATE TABLE t (a String, b UInt8 DEFAULT nope + 1) ENGINE = MergeTree ORDER BY a`, cherr.UnknownIdentifier},
		{`CREATE TABLE t (a String) ENGINE = Memory ORDER BY a`, cherr.BadArguments},
		{`CREATE TABLE t (a String, s AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY s`, cherr.IllegalColumn},
	}
	for _, c := range cases {
		te := newTestEnv(t)
		_, err := te.run(c.ddl)
		if err == nil {
			t.Errorf("%s: expected an error", c.ddl)
			continue
		}
		if c.code != 0 && !cherr.Is(err, c.code) {
			t.Errorf("%s: expected code %d, got %v", c.ddl, c.code, err)
		}
		if _, err := te.cat.Table("default", "t"); err == nil {
			t.Errorf("%s: table must not exist after a failed CREATE", c.ddl)
		}
	}
}

func TestCreateTableExisting(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY a`)
	te.expectErr(`CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY a`, cherr.TableAlreadyExists)
	te.exec(`CREATE TABLE IF NOT EXISTS t (b UInt8) ENGINE = MergeTree ORDER BY b`)
	tb, _ := te.cat.Table("default", "t")
	eq(t, tb.Columns[0].Name, "a")
	te.exec(`INSERT INTO t VALUES ('x')`)
	te.expectErr(`CREATE OR REPLACE TABLE t (b UInt8) ENGINE = MergeTree ORDER BY nope`, cherr.UnknownIdentifier)
	te.expectRows(`SELECT count() FROM t`, "1") // the failed replace kept the table
	te.exec(`CREATE OR REPLACE TABLE t (b UInt8) ENGINE = MergeTree ORDER BY b`)
	tb, _ = te.cat.Table("default", "t")
	eq(t, tb.Columns[0].Name, "b")
	te.expectRows(`SELECT count() FROM t`, "0")
}

func TestCreateTableAsSelectAndLike(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE src (a String, n UInt64) ENGINE = MergeTree ORDER BY a`)
	te.exec(`INSERT INTO src VALUES ('x', 1), ('y', 2)`)
	te.exec(`CREATE TABLE copy ENGINE = MergeTree ORDER BY a AS SELECT a, n * 10 AS n10 FROM src`)
	te.expectRows(`SELECT a, n10, toTypeName(n10) FROM copy ORDER BY a`, "x\t10\tUInt64\ny\t20\tUInt64")
	te.exec(`CREATE TABLE like_src AS src`)
	tb, err := te.cat.Table("default", "like_src")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, tb.OrderBy, []string{"a"})
	te.expectRows(`SELECT count() FROM like_src`, "0")
}

func TestDatabases(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE DATABASE app`)
	te.expectErr(`CREATE DATABASE app`, cherr.DatabaseAlreadyExists)
	te.exec(`CREATE DATABASE IF NOT EXISTS app`)
	te.exec(`CREATE TABLE app.t (a String) ENGINE = MergeTree ORDER BY a`)
	te.exec(`INSERT INTO app.t VALUES ('x')`)
	te.expectRows(`SELECT a FROM app.t`, "x")
	res := te.exec(`USE app`)
	eq(t, res.Database, "app")
	res, err := te.runReq(&Request{Query: `SELECT a, currentDatabase() FROM t`, Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.Rows[0], []any{"x", "app"})
	te.expectRows(`SHOW DATABASES`, "app\ndefault\nsystem")
	te.expectRows(`SHOW TABLES FROM app`, "t")
	te.expectRows(`EXISTS DATABASE app`, "1")
	te.expectRows(`EXISTS TABLE app.t`, "1")
	te.expectRows(`EXISTS TABLE app.nope`, "0")
	tb, _ := te.cat.Table("app", "t")
	te.exec(`DROP DATABASE app`)
	if te.store.HasTable(tb) {
		t.Fatal("DROP DATABASE must drop physical tables")
	}
	te.expectErr(`SELECT * FROM app.t`, cherr.UnknownDatabase)
	te.exec(`DROP DATABASE IF EXISTS app`)
	te.expectErr(`DROP DATABASE app`, cherr.UnknownDatabase)
	te.expectErr(`USE nope`, cherr.UnknownDatabase)
}

func TestDropTruncate(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (a String) ENGINE = MergeTree ORDER BY a`)
	te.exec(`INSERT INTO t VALUES ('x'), ('y')`)
	te.exec(`TRUNCATE TABLE t`)
	te.expectRows(`SELECT count() FROM t`, "0")
	tb, _ := te.cat.Table("default", "t")
	te.exec(`DROP TABLE t`)
	if te.store.HasTable(tb) {
		t.Fatal("physical table still exists")
	}
	te.expectErr(`DROP TABLE t`, cherr.UnknownTable)
	te.exec(`DROP TABLE IF EXISTS t`)
	te.expectErr(`TRUNCATE TABLE t`, cherr.UnknownTable)
	te.exec(`TRUNCATE TABLE IF EXISTS t`)
}

func TestAlterTable(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, a UInt8) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t VALUES ('x', 1)`)
	te.exec(`ALTER TABLE t ADD COLUMN b String DEFAULT 'dflt' AFTER k`)
	te.exec(`ALTER TABLE t ADD COLUMN IF NOT EXISTS b UInt8`)
	te.expectErr(`ALTER TABLE t ADD COLUMN b UInt8`, duplicateColumn)
	te.exec(`INSERT INTO t (k, a) VALUES ('y', 2)`)
	// Existing rows read the new column's DEFAULT expression (ClickHouse).
	te.expectRows(`SELECT k, b, a FROM t ORDER BY k`, "x\tdflt\t1\ny\tdflt\t2")
	te.expectRows(`SELECT name FROM system.columns WHERE table = 't' ORDER BY position`, "k\nb\na")
	te.exec(`ALTER TABLE t MODIFY COLUMN a UInt64`)
	te.expectRows(`SELECT toTypeName(a) FROM t LIMIT 1`, "UInt64")
	te.exec(`ALTER TABLE t DROP COLUMN b`)
	te.expectRows(`SELECT * FROM t ORDER BY k`, "x\t1\ny\t2")
	te.expectErr(`ALTER TABLE t DROP COLUMN k`, cherr.BadArguments)
	te.expectErr(`ALTER TABLE t MODIFY COLUMN k UInt8`, cherr.BadArguments)
	te.exec(`ALTER TABLE t DROP COLUMN IF EXISTS nope`)
	te.expectErr(`ALTER TABLE t DROP COLUMN nope`, cherr.NoSuchColumnInTable)
	te.expectErr(`ALTER TABLE t UPDATE a = 1 WHERE 1`, cherr.NotImplemented)
	te.exec(`ALTER TABLE t DELETE WHERE k = 'x'`)
	te.expectRows(`SELECT k FROM t`, "y")
	te.exec(`DELETE FROM t WHERE a = 2`)
	te.expectRows(`SELECT count() FROM t`, "0")
}

func TestDescribeShow(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String COMMENT 'key', n UInt8 DEFAULT 1, m String MATERIALIZED k) ENGINE = MergeTree ORDER BY k`)
	res := te.exec(`DESCRIBE t`)
	names := []string{}
	for _, c := range res.Columns {
		names = append(names, c.Name)
	}
	eq(t, names, []string{"name", "type", "default_type", "default_expression", "comment", "codec_expression", "ttl_expression"})
	eq(t, res.Rows[0], []any{"k", "String", "", "", "key", "", ""})
	eq(t, res.Rows[1], []any{"n", "UInt8", "DEFAULT", "1", "", "", ""})
	eq(t, res.Rows[2], []any{"m", "String", "MATERIALIZED", "k", "", "", ""})
	stmt := te.exec(`SHOW CREATE TABLE t`).Rows[0][0].(string)
	if !strings.Contains(stmt, "CREATE TABLE default.t") || !strings.Contains(stmt, "`n` UInt8 DEFAULT 1") || !strings.Contains(stmt, "ORDER BY k") {
		t.Errorf("unexpected SHOW CREATE TABLE output:\n%s", stmt)
	}
	te.exec(`CREATE TABLE t2 (k String) ENGINE = MergeTree ORDER BY k`)
	te.expectRows(`SHOW TABLES LIKE 't%'`, "t\nt2")
	te.expectRows(`SHOW TABLES NOT LIKE '%2'`, "t")
	te.expectRows(`SELECT name, engine, sorting_key FROM system.tables WHERE database = 'default' ORDER BY name`, "t\tMergeTree\tk\nt2\tMergeTree\tk")
	te.expectRows(`SELECT name FROM system.databases ORDER BY name`, "default\nsystem")
	te.expectErr(`DESCRIBE nope`, cherr.UnknownTable)
	te.exec(`OPTIMIZE TABLE t FINAL`)
	te.exec(`SYSTEM FLUSH LOGS`)
	te.expectErr(`OPTIMIZE TABLE nope`, cherr.UnknownTable)
}

func TestPlainView(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, n UInt64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 5)`)
	te.exec(`CREATE VIEW v AS SELECT k, sum(n) AS s FROM t GROUP BY k`)
	te.expectRows(`SELECT * FROM v ORDER BY k`, "a\t3\nb\t5")
	te.expectRows(`SELECT s FROM v WHERE k = 'b'`, "5")
	res := te.exec(`DESCRIBE v`)
	eq(t, res.Rows[1][:2], []any{"s", "UInt64"})
	te.expectRows(`EXISTS VIEW v`, "1")
	te.expectErr(`CREATE VIEW v AS SELECT 1`, cherr.TableAlreadyExists)
	te.exec(`CREATE OR REPLACE VIEW v AS SELECT 1 AS one`)
	te.expectRows(`SELECT * FROM v`, "1")
	te.exec(`DROP VIEW v`)
	te.expectErr(`SELECT * FROM v`, cherr.UnknownTable)
	te.expectErr(`CREATE VIEW v2 AS SELECT nope FROM t`, cherr.UnknownIdentifier)
}

func TestReadonly(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String) ENGINE = MergeTree ORDER BY k`)
	ro := map[string]string{"readonly": "1"}
	for _, q := range []string{`INSERT INTO t VALUES ('a')`, `CREATE TABLE t2 (k String) ENGINE = Memory`, `DROP TABLE t`, `TRUNCATE TABLE t`, `ALTER TABLE t DELETE WHERE 1`, `SET max_threads = 1`} {
		_, err := te.runReq(&Request{Query: q, Settings: ro})
		if !cherr.Is(err, cherr.ReadonlyQuery) {
			t.Errorf("%s: expected READONLY, got %v", q, err)
		}
	}
	for _, q := range []string{`SELECT count() FROM t`, `SHOW TABLES`, `DESCRIBE t`, `EXISTS t`} {
		if _, err := te.runReq(&Request{Query: q, Settings: ro}); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
	if _, err := te.runReq(&Request{Query: `SET max_threads = 1`, Settings: map[string]string{"readonly": "2"}}); err != nil {
		t.Errorf("readonly=2 allows SET: %v", err)
	}
}

func eq(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestAlterModifySetting(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192`)
	te.exec(`ALTER TABLE t MODIFY SETTING index_granularity = 1024, ttl_only_drop_parts = 1`)
	tb, _ := te.cat.Table("default", "t")
	eq(t, tb.Settings["index_granularity"], "1024")
	eq(t, tb.Settings["ttl_only_drop_parts"], "1")
	te.expectErr(`ALTER TABLE t MODIFY SETTING dynamodb_partition_key = 'k'`, cherr.NotImplemented)
	te.expectErr(`ALTER TABLE t MATERIALIZE INDEX idx`, cherr.NotImplemented)
	te.exec(`ALTER TABLE t COMMENT COLUMN k 'the key'`)
	te.expectRows(`SELECT comment FROM system.columns WHERE table = 't'`, "the key")
}
