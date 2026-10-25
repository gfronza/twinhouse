-- Nullable propagation, conditional functions, aggregates over NULLs,
-- type conversions and supertypes.

CREATE TABLE np (
    id UInt8,
    a Nullable(Int32),
    b Nullable(String),
    c Nullable(Float64),
    d Nullable(DateTime),
    lc LowCardinality(Nullable(String)),
    arr Array(Nullable(UInt8))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO np VALUES
    (1, 1, 'x', 1.5, '2024-01-01 00:00:00', 'p', [1, NULL]),
    (2, NULL, NULL, NULL, NULL, NULL, []),
    (3, -5, '', 0, '2024-06-01 00:00:00', '', [NULL]),
    (4, 10, 'y', NULL, NULL, 'q', [2]),
    (5, NULL, 'z', -2.25, '2023-01-01 00:00:00', NULL, [3, 3]);

-- query
SELECT id, a + 1, a * c, b || '!', concat(b, 'x'), a > 0, a = NULL, NULL = NULL FROM np ORDER BY id;

-- query
SELECT toTypeName(a + 1), toTypeName(a * c), toTypeName(a > 0), toTypeName(concat(b, 'x')), toTypeName(coalesce(a, 0)), toTypeName(ifNull(b, '')) FROM np WHERE id = 1;

-- query
SELECT id, coalesce(a, 0), coalesce(b, lc, 'none'), ifNull(c, -1), nullIf(b, ''), assumeNotNull(a), isNull(d), isNotNull(lc) FROM np ORDER BY id;

-- query
SELECT count(), count(a), count(b), sum(a), avg(a), min(a), max(a), sum(c), avg(c), min(b), max(b), min(d), max(d) FROM np;

-- query
SELECT toTypeName(sum(a)), toTypeName(avg(a)), toTypeName(min(b)), toTypeName(count(a)), toTypeName(uniqExact(b)) FROM np;

-- query
SELECT uniqExact(a), uniqExact(b), uniq(lc), count(DISTINCT a), groupArray(a), countIf(a IS NULL) FROM (SELECT * FROM np ORDER BY id);

-- query
SELECT sum(a) FROM np WHERE a IS NULL;

-- query
SELECT avg(a), max(b) FROM np WHERE id = 2;

-- query
SELECT b, count() FROM np GROUP BY b ORDER BY b;

-- query
SELECT lc, count() FROM np GROUP BY lc ORDER BY lc NULLS FIRST;

-- query
SELECT id FROM np WHERE a > 0 ORDER BY id;

-- query
SELECT id FROM np WHERE NOT (a > 0) ORDER BY id;

-- query
SELECT id FROM np WHERE b IN ('x', 'y') ORDER BY id;

-- query
SELECT id FROM np WHERE b NOT IN ('x', 'y') ORDER BY id;

-- query
SELECT id FROM np WHERE a IS NULL OR b = '' ORDER BY id;

-- query
SELECT id, a FROM np ORDER BY a, id;

-- query
SELECT id, a FROM np ORDER BY a DESC, id;

-- query
SELECT id, a FROM np ORDER BY a NULLS FIRST, id;

-- query
SELECT id, c FROM np ORDER BY c DESC NULLS LAST, id;

-- query
SELECT id, if(a > 0, 'pos', 'nonpos'), if(isNull(a), 'null', 'set'), multiIf(a IS NULL, 0, a < 0, -1, 1) FROM np ORDER BY id;

-- query
SELECT toTypeName(if(id > 2, a, NULL)), toTypeName(if(id > 2, 1, NULL)), toTypeName(multiIf(id = 1, 'a', id = 2, NULL, 'c')) FROM np WHERE id = 1;

-- query
SELECT id, arr, arr[1], has(arr, NULL), length(arr) FROM np ORDER BY id;

-- query
SELECT id, toString(a), toString(d), toInt64(c), toUInt8(a) FROM np ORDER BY id;

-- query
SELECT CAST(a AS Nullable(String)), CAST(b AS Nullable(String)), toNullable(id) FROM np ORDER BY id;

-- query
SELECT argMax(b, id), argMin(b, id), argMax(id, a) FROM (SELECT * FROM np WHERE b IS NOT NULL);

-- query
SELECT toTypeName(argMax(b, id)), toTypeName(argMax(id, a)), toTypeName(max(lc)) FROM np;

-- query
SELECT sumIf(a, b != ''), countIf(b = 'x'), avgIf(c, c > 0), minIf(a, a IS NOT NULL), maxIf(b, id < 5) FROM np;

-- query
SELECT 1 + NULL, NULL AND 0, NULL OR 1, NULL AND 1, NOT NULL, NULL > 1, if(NULL, 1, 2);

-- query
SELECT toTypeName(NULL AND 0), toTypeName(if(NULL, 1, 2)), toTypeName([NULL]), toTypeName(coalesce(NULL, NULL));

-- query
SELECT greatest(1, NULL), least(toNullable(2), 3), toTypeName(greatest(1, NULL));

-- query
SELECT toTypeName(if(1, toInt8(1), toUInt8(1))), toTypeName(if(1, toInt32(1), toUInt32(1))), toTypeName(if(1, toUInt32(1), toFloat32(1))), toTypeName(if(1, toDate('2024-01-01'), toDateTime('2024-01-01 00:00:00')));

-- query
SELECT toTypeName([toInt8(1), toUInt16(2)]), toTypeName([1, -1, 1000]), toTypeName([toUInt32(1), toInt8(-1)]), toTypeName([1.5, toFloat32(2)]);

-- query
SELECT x, toTypeName(x) FROM (SELECT toInt8(-1) AS x UNION ALL SELECT toUInt8(200)) ORDER BY x;

-- query
SELECT toTypeName(coalesce(toNullable(toUInt8(1)), toInt16(2))), toTypeName(ifNull(toNullable(toUInt8(1)), 300));

-- query
SELECT b, length(b), upper(b), empty(b), startsWith(b, 'x') FROM np ORDER BY id;

-- query
SELECT d, toDate(d), toYYYYMM(d), toStartOfHour(d), formatDateTime(d, '%Y') FROM np ORDER BY id;
