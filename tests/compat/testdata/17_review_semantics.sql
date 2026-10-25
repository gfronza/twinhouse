-- Comparison, conversion and materialized-view semantics fixed after review
-- (enums by value, Date vs DateTime time zones, FixedString padding, float
-- modulo, toXOrNull ranges, ANTI join keys, settings, MV restrictions).

CREATE TABLE en_t (id UInt32, en Enum8('x' = 1, 'a' = 2, 'm' = 3)) ENGINE = MergeTree ORDER BY id;

INSERT INTO en_t VALUES (1, 'x'), (2, 'a'), (3, 'm'), (4, 'x'), (5, 'a');

-- query: enum range filters
SELECT count() FROM en_t WHERE en > 'x';

-- query: enum countIf and BETWEEN
SELECT countIf(en < 'm'), countIf(en BETWEEN 'x' AND 'a') FROM en_t;

-- query: enum order
SELECT id, en FROM en_t ORDER BY en, id;

-- query: enum order desc
SELECT en FROM en_t GROUP BY en ORDER BY en DESC;

-- query: enum extrema
SELECT min(en), max(en), argMax(id, en), argMin(id, en) FROM en_t WHERE id != 4;

-- query: enum constants
SELECT CAST('a', 'Enum8(\'x\' = 1, \'a\' = 2)') > 'x', CAST('a', 'Enum8(\'x\' = 1, \'a\' = 2)') > 1, CAST('a', 'Enum8(\'x\' = 1, \'a\' = 2)') = 'zz', greatest(CAST('a', 'Enum8(\'x\' = 1, \'a\' = 2)'), CAST('x', 'Enum8(\'x\' = 1, \'a\' = 2)'));

-- query: enum arraySort
SELECT arraySort([CAST('a', 'Enum8(\'x\' = 1, \'a\' = 2)'), CAST('x', 'Enum8(\'x\' = 1, \'a\' = 2)')]);

CREATE TABLE tz_t (id UInt32, ts DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY id;

INSERT INTO tz_t VALUES (1, '2024-01-01 05:00:00'), (2, '2024-01-01 21:00:00');

-- query: date vs datetime with explicit zone
SELECT count() FROM tz_t WHERE ts >= toDate('2024-01-01');

-- query: date between datetime with explicit zone
SELECT count() FROM tz_t WHERE ts BETWEEN toDate('2024-01-01') AND toDate('2024-01-02');

-- query: date equals datetime with explicit zone
SELECT toDate('2024-01-01') = toDateTime('2024-01-01 00:00:00', 'Asia/Tokyo'), toDate('2024-01-01') IN (toDateTime('2024-01-01 00:00:00', 'Asia/Tokyo'));

-- query: date vs datetime in the session zone
-- setting: session_timezone=America/New_York
SELECT toDateTime('2024-01-01 00:00:00') = toDate('2024-01-01'), toDateTime('2024-01-01 00:00:00') IN (toDate('2024-01-01')), toDateTime('2024-01-01 00:00:00') = '2024-01-01 00:00:00';

-- query: session_timezone drives parsing and timezone()
-- setting: session_timezone=Asia/Kolkata
SELECT timezone(), toUnixTimestamp(toDateTime('2024-01-01 00:00:00'));

CREATE TABLE fs_t (fs FixedString(3), id UInt32) ENGINE = MergeTree ORDER BY fs;

INSERT INTO fs_t VALUES ('a', 1), ('a', 2), ('b', 3);

-- query: fixedstring vs string with trailing zero bytes
SELECT countIf(fs = 'a\0'), countIf(fs = 'a\0\0'), countIf(fs IN ('a\0')), countIf(fs < 'a\0') FROM fs_t;

-- query: fixedstring key lookup with trailing zero bytes
SELECT count() FROM fs_t WHERE fs = 'a\0';

-- query: float modulo
SELECT 1 % 0.1, 10 % 0.3, 1e20 % 3.0, -1 % 0.1, -7.5 % 2, 5.5 % -2, -4. % 2;

-- query: toXOrNull ranges
SELECT toUInt8OrNull('-1'), toUInt8OrNull('300'), toInt8OrNull('200'), toUInt8OrZero('-1'), toInt32OrNull('3000000000'), toUInt64OrNull('-1'), toUInt8OrNull('255'), toInt8OrNull('-128'), toInt8OrZero('-129');

-- error: 72
SELECT toUInt8('-1');

CREATE TABLE jl (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE jr (rid UInt32, k UInt64) ENGINE = MergeTree ORDER BY rid;

INSERT INTO jl VALUES (1, 1), (2, 2), (3, 3);

INSERT INTO jr VALUES (10, 1), (11, 1), (12, 2), (13, 4);

-- query: anti join right key
SELECT l.id, r.k FROM jl AS l LEFT ANTI JOIN jr AS r USING (k) ORDER BY l.id;

-- query: anti join filter on right key
SELECT count() FROM jl AS l LEFT ANTI JOIN jr AS r USING (k) WHERE r.k = 3;

-- not-implemented: result-affecting setting
SELECT number FROM numbers(5) SETTINGS limit = 2;

-- not-implemented: result-affecting setting
SELECT count() FROM (SELECT arrayJoin([NULL, 1, 2]) AS x) WHERE x IN (NULL) SETTINGS transform_null_in = 1;

-- error: 115
SELECT 1 SETTINGS no_such_setting_xyz = 1;

CREATE TABLE daily (k String, n UInt64) ENGINE = SummingMergeTree ORDER BY k;

CREATE TABLE total (k String, n UInt64) ENGINE = SummingMergeTree ORDER BY k;

-- not-implemented: a view cannot read FROM a merging table
CREATE MATERIALIZED VIEW daily_mv TO total AS SELECT k, sum(n) AS n FROM daily GROUP BY k;

CREATE TABLE ev_src (id UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE per_sec (t DateTime, c UInt64) ENGINE = SummingMergeTree ORDER BY t;

-- not-implemented: volatile target key
CREATE MATERIALIZED VIEW per_sec_mv TO per_sec AS SELECT now() AS t, count() AS c FROM ev_src;

CREATE TABLE rsrc (k String, ver UInt64, tag String) ENGINE = ReplacingMergeTree(ver) ORDER BY k;

CREATE TABLE rroll (tag String, c UInt64) ENGINE = SummingMergeTree ORDER BY tag;

CREATE MATERIALIZED VIEW rroll_mv TO rroll AS SELECT tag, count() AS c FROM rsrc GROUP BY tag;

INSERT INTO rsrc VALUES ('k1', 5, 'a');

INSERT INTO rsrc VALUES ('k1', 3, 'b');

-- query: replacing source keeps the newest version
SELECT k, ver, tag FROM rsrc FINAL;

-- The rollup itself (ClickHouse: a 1, b 1) is checked by exec's unit tests:
-- in a multi-region deployment a version that is superseded when written
-- feeds no view (DESIGN §1, §5), so the result depends on the deployment.
