-- UUID ordering: ClickHouse compares a UUID as a UInt128 whose high half is
-- the last 8 bytes (hex digits 19..35), so the text order is not the value
-- order. Covers comparison operators, ORDER BY, extrema, greatest/least,
-- nested UUIDs, range predicates and keyset pagination on a UUID sorting-key
-- column, and a SimpleAggregateFunction rollup.

-- query: scalar comparison operators
SELECT toUUID('00000000-0000-0001-0000-000000000000') < toUUID('00000000-0000-0000-0000-000000000001'), toUUID('10000000-0000-0000-0000-000000000000') > toUUID('00000000-0000-0000-0000-000000000001'), toUUID('00000000-0000-0001-0000-000000000000') < '00000000-0000-0000-0000-000000000001', toUUID('00000000-0000-0000-ff00-000000000000') >= toUUID('00000000-0000-0000-0000-000000000100');

-- query: order by uuid
SELECT u FROM (SELECT toUUID(arrayJoin(['00000000-0000-0001-0000-000000000000','00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000000','00000000-0000-0000-ff00-000000000000','00000000-0001-0000-0000-000000000000','00000000-0000-0000-0100-000000000000'])) AS u) ORDER BY u;

-- query: order by uuid desc
SELECT u FROM (SELECT toUUID(arrayJoin(['00000000-0000-0001-0000-000000000000','00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000000','00000000-0000-0000-ff00-000000000000'])) AS u) ORDER BY u DESC;

-- query: min max argMin argMax over uuid
SELECT min(u), max(u), argMin(n, u), argMax(n, u) FROM (SELECT arrayJoin([(1,'00000000-0000-0001-0000-000000000000'),(2,'00000000-0000-0000-0000-000000000001'),(3,'10000000-0000-0000-0000-000000000000'),(4,'00000000-0000-0000-ff00-000000000000')]) AS t, t.1 AS n, toUUID(t.2) AS u);

-- query: greatest and least
SELECT greatest(toUUID('00000000-0000-0001-0000-000000000000'), toUUID('00000000-0000-0000-0000-000000000001')), least(toUUID('00000000-0000-0001-0000-000000000000'), toUUID('00000000-0000-0000-0000-000000000001'));

-- query: arrays and tuples of uuid compare element-wise as uuids
SELECT [toUUID('00000000-0000-0001-0000-000000000000')] < [toUUID('00000000-0000-0000-0000-000000000001')], (toUUID('00000000-0000-0001-0000-000000000000'), 1) < (toUUID('00000000-0000-0000-0000-000000000001'), 1);

CREATE TABLE uo (g UInt8, u UUID, n UInt32) ENGINE = MergeTree ORDER BY (g, u);

INSERT INTO uo VALUES (1,'00000000-0000-0001-0000-000000000000',1),(1,'00000000-0000-0000-0000-000000000001',2),(1,'10000000-0000-0000-0000-000000000000',3),(1,'00000000-0000-0000-ff00-000000000000',4),(2,'00000000-0000-0000-0000-000000000001',5);

-- query: range predicate on a uuid sorting-key column
SELECT count() FROM uo WHERE g = 1 AND u < '00000000-0000-0000-0000-000000000001';

-- query: between on a uuid sorting-key column
SELECT n FROM uo WHERE g = 1 AND u BETWEEN '10000000-0000-0000-0000-000000000000' AND '00000000-0000-0000-0000-000000000001' ORDER BY n;

-- query: table order by sorting key
SELECT g, u, n FROM uo ORDER BY g, u;

-- query: keyset pagination over uuid
SELECT u FROM uo WHERE g = 1 AND u > '00000000-0000-0001-0000-000000000000' ORDER BY u LIMIT 2;

-- query: min and max per group from a table
SELECT g, min(u), max(u) FROM uo GROUP BY g ORDER BY g;

CREATE TABLE uo_src (g UInt8, id UInt32, u UUID) ENGINE = MergeTree ORDER BY (g, id);

CREATE TABLE uo_roll (g UInt8, lo SimpleAggregateFunction(min, UUID), hi SimpleAggregateFunction(max, UUID), hi_id AggregateFunction(argMax, UInt32, UUID)) ENGINE = AggregatingMergeTree ORDER BY g;

CREATE MATERIALIZED VIEW uo_mv TO uo_roll AS SELECT g, min(u) AS lo, max(u) AS hi, argMaxState(id, u) AS hi_id FROM uo_src GROUP BY g;

INSERT INTO uo_src VALUES (1, 1, '00000000-0000-0000-ff00-000000000000');

INSERT INTO uo_src VALUES (1, 2, '00000000-0000-0001-0000-000000000000');

INSERT INTO uo_src VALUES (1, 3, '00000000-0000-0000-0000-000000000001'), (1, 4, '10000000-0000-0000-0000-000000000000');

-- query: uuid extrema in a rollup compare as uuids
SELECT g, min(lo), max(hi), argMaxMerge(hi_id) FROM uo_roll GROUP BY g ORDER BY g;
