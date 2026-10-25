-- Error codes (and HTTP statuses) for invalid statements. Message texts may
-- differ; codes must not.

CREATE TABLE t (tenant_id String, n UInt64, ts DateTime, s Nullable(String)) ENGINE = MergeTree ORDER BY (tenant_id, ts);

INSERT INTO t VALUES ('a', 1, '2024-01-01 00:00:00', NULL);

-- error: 60
SELECT * FROM no_such_table;

-- error: 60
SELECT * FROM $DB.no_such_table;

-- error: 81
SELECT * FROM no_such_db_xyz.t;

-- error: 47
SELECT no_such_column FROM t;

-- error: 47
SELECT n FROM t WHERE missing = 1;

-- error: 46
SELECT no_such_function(1);

-- error: 46
SELECT sumx(n) FROM t;

-- error: 62
SELECT FROM WHERE;

-- error: 62
SELECT 1 +;

-- error: 62
SELEC 1;

-- error: 62
SELECT (1, 2;

-- error: 62
SELECT 'unterminated;

-- error: 43
SELECT 'a' + 1;

-- error: 43
SELECT n + 'x' FROM t;

-- error: 43
SELECT sum(tenant_id) FROM t;

-- error: 43
SELECT length(1);

-- error: 42
SELECT length();

-- error: 42
SELECT toDate();

-- error: 215
SELECT tenant_id, count() FROM t;

-- error: 215
SELECT n, sum(n) FROM t GROUP BY tenant_id;

-- error: 184
SELECT sum(count()) FROM t;

-- error: 184
SELECT n FROM t WHERE sum(n) > 1;

-- error: 386
SELECT if(1, 'a', 1);

-- error: 386
SELECT [1, 'a'];

-- error: 6
SELECT toUInt64('abc');

-- error: 6
SELECT CAST('x' AS Int32);

-- error: 43
SELECT CAST([1] AS UInt8);

-- error: 50
SELECT CAST(1 AS NoSuchType);

-- error: 50
CREATE TABLE bad_type (x Strin) ENGINE = MergeTree ORDER BY x;

-- error: 57
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;

-- error: 47
CREATE TABLE bad_order (x UInt8) ENGINE = MergeTree ORDER BY y;

-- error: 60
DROP TABLE no_such_table;

-- error: 60
INSERT INTO no_such_table VALUES (1);

-- error: 16
INSERT INTO t (tenant_id, nope) VALUES ('a', 1);

-- error: 27
INSERT INTO t FORMAT JSONEachRow
{"tenant_id": "a", "n": "not a number"};

-- error: 6
INSERT INTO t VALUES ('a', 'x', '2024-01-01 00:00:00', NULL);

-- error: 62
INSERT INTO t VALUES ('a', 1;

-- error: 73
SELECT 1 FORMAT NoSuchFormat;

-- error: 153
SELECT intDiv(n, 0) FROM t;

-- error: 456
SELECT {p:String};

-- error: 457
-- param: p=abc
SELECT {p:UInt8};

-- error: 36
SELECT dateDiff('fortnight', toDate('2024-01-01'), toDate('2024-02-01'));

-- error: 36
SELECT toDateTime('2024-01-01 00:00:00', 'Not/AZone');

-- error: 179
SELECT 1 AS x, 2 AS x;

-- error: 47
SELECT * FROM t ARRAY JOIN nope;

-- error: 258
SELECT 1 UNION ALL SELECT 'a', 2;

-- error: 386
SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 'a');

-- error: 47
SELECT x FROM (SELECT 1 AS y);

-- error: 125
SELECT (SELECT number FROM numbers(2));

-- unsupported: throwIf is outside the DESIGN §3 function list
SELECT throwIf(1, 'custom');

-- error: 41
INSERT INTO t FORMAT JSONEachRow
{"tenant_id": "a", "n": 1, "ts": "garbage"};

-- Unknown JSONEachRow keys are skipped (input_format_skip_unknown_fields=1).
INSERT INTO t FORMAT JSONEachRow
{"tenant_id": "skip", "zzz": 1, "n": 5};

-- query
SELECT tenant_id, n, ts, s FROM t ORDER BY tenant_id;

-- error: 117
-- setting: input_format_skip_unknown_fields=0
INSERT INTO t FORMAT JSONEachRow
{"tenant_id": "a", "zzz": 1};

-- error: 27
INSERT INTO t FORMAT CSV
a,notanumber,2024-01-01 00:00:00,\N;

-- error: 164
-- setting: readonly=1
INSERT INTO t VALUES ('a', 1, '2024-01-01 00:00:00', NULL);

-- error: 164
-- setting: readonly=1
CREATE TABLE ro (x UInt8) ENGINE = MergeTree ORDER BY x;

-- error: 62
SELECT * FROM t SETTINGS;

-- error: 47
SELECT t.zzz FROM t;
