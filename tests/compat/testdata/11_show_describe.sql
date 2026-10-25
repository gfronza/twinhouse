-- SHOW / DESCRIBE / EXISTS / system tables, DDL variants and defaults.

CREATE TABLE users (
    tenant_id String,
    user_id String,
    email String DEFAULT '',
    signup DateTime DEFAULT toDateTime('2024-01-01 00:00:00'),
    signup_day Date MATERIALIZED toDate(signup),
    domain String ALIAS substring(email, position(email, '@') + 1),
    score Float64 DEFAULT 1.5 COMMENT 'quality score',
    plan LowCardinality(String),
    flags Array(UInt8),
    meta Map(String, String),
    ref Nullable(UInt64),
    created DateTime64(3, 'UTC') DEFAULT '2024-01-01 00:00:00.000'
) ENGINE = MergeTree
PRIMARY KEY (tenant_id)
ORDER BY (tenant_id, user_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS users (x UInt8) ENGINE = MergeTree ORDER BY x;

CREATE TABLE logs (id UInt64, msg String) ENGINE = Log;

CREATE TABLE alters (id UInt64, msg String) ENGINE = MergeTree ORDER BY id;

CREATE TABLE codecs (id UInt64 CODEC(Delta, ZSTD(1)), payload String COMMENT 'raw' CODEC(ZSTD(3))) ENGINE = MergeTree ORDER BY id;

CREATE TABLE showme (tenant_id String, day Date, n UInt64 DEFAULT 0) ENGINE = ReplacingMergeTree(n) PARTITION BY toYYYYMM(day) PRIMARY KEY tenant_id ORDER BY (tenant_id, day);

CREATE TABLE mem (id UInt64) ENGINE = Memory;

CREATE TABLE totals (k String, n UInt64, s Int64) ENGINE = SummingMergeTree ORDER BY k;

CREATE TABLE emptykey (a UInt8, b String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO users (tenant_id, user_id, email, plan) VALUES ('acme', 'u1', 'ann@example.com', 'pro'), ('acme', 'u2', '', 'free');

INSERT INTO users (tenant_id, user_id, signup, plan, ref) VALUES ('globex', 'v1', '2024-06-01 12:00:00', 'pro', 7);

-- query
DESCRIBE TABLE users;

-- query
-- formats: TSVWithNamesAndTypes,TabSeparated,JSONCompact,JSONEachRow,Native
DESC users;

-- query
DESCRIBE TABLE logs;

-- query
DESCRIBE $DB.totals;

-- query
DESCRIBE emptykey;

-- query: describe_codec_column
DESCRIBE codecs;

-- query
SHOW TABLES;

-- query
SHOW TABLES FROM $DB;

-- query
SHOW TABLES LIKE 'u%';

-- query
SHOW DATABASES LIKE '$DB';

-- query
EXISTS TABLE users;

-- query
EXISTS users;

-- query
EXISTS TABLE nope;

-- query
EXISTS DATABASE $DB;

-- query
EXISTS DATABASE nope_db_xyz;

-- query
SELECT tenant_id, user_id, email, signup, signup_day, domain, score, plan, flags, meta, ref, created FROM users ORDER BY tenant_id, user_id;

-- query
SELECT * FROM users ORDER BY tenant_id, user_id;

-- query
SELECT domain, count() FROM users GROUP BY domain ORDER BY domain;

-- query
SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;

-- query
SELECT name, type, position FROM system.columns WHERE database = currentDatabase() AND table = 'users' ORDER BY position;

-- query
SELECT name FROM system.databases WHERE name = currentDatabase();

-- query
SHOW CREATE TABLE emptykey;

-- query
SHOW CREATE TABLE users;

-- query: show_create_partition_primary_key
SHOW CREATE TABLE showme;

-- query: show_create_codec
SHOW CREATE TABLE codecs;

INSERT INTO totals VALUES ('a', 1, -1), ('b', 2, 2);
INSERT INTO totals VALUES ('a', 10, -10);

-- query
SELECT k, sum(n), sum(s) FROM totals GROUP BY k ORDER BY k;

-- query
SELECT k, n, s FROM totals FINAL ORDER BY k;

INSERT INTO logs VALUES (2, 'b'), (1, 'a');
INSERT INTO mem VALUES (5), (3);

-- query
SELECT * FROM logs ORDER BY id;

-- query
SELECT sum(id), count() FROM mem;

TRUNCATE TABLE mem;

-- query
SELECT count() FROM mem;

INSERT INTO alters VALUES (2, 'b'), (1, 'a');

ALTER TABLE alters ADD COLUMN level UInt8 DEFAULT 3;

INSERT INTO alters VALUES (3, 'c', 9);

-- query
SELECT id, msg, level FROM alters ORDER BY id;

-- query: describe_after_alter
DESCRIBE alters;

ALTER TABLE alters DROP COLUMN msg;

-- query: select_after_drop_column
SELECT * FROM alters ORDER BY id;

ALTER TABLE alters MODIFY COLUMN level UInt32;

-- query: select_after_modify_column
SELECT id, level, toTypeName(level) FROM alters ORDER BY id;

INSERT INTO users (tenant_id, user_id, plan) VALUES ('acme', 'u3', 'free');

-- setting: mutations_sync=2
ALTER TABLE users DELETE WHERE user_id = 'u3';

-- query: after_alter_delete
SELECT user_id FROM users ORDER BY user_id;

DROP TABLE emptykey;

-- query
EXISTS emptykey;

DROP TABLE IF EXISTS emptykey;

-- error: 60
DROP TABLE emptykey;

-- error: 57
CREATE TABLE users (x UInt8) ENGINE = MergeTree ORDER BY x;

-- error: 82
CREATE DATABASE $DB;

-- query
SELECT 'still here';
