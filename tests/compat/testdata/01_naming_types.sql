-- Column naming of expressions and result-type inference (no tables).

-- query
SELECT 1, 1 + 1, 2 * 3 - 1, -5, 1.5, 'str';

-- query
SELECT 255, 256, 65535, 65536, 4294967295, 4294967296, 18446744073709551615;

-- query
SELECT -1, -128, -129, -32769, -2147483649, -9223372036854775808;

-- query
SELECT 1 AS a, a + 1 AS b, b * 2;

-- query
SELECT toTypeName(1), toTypeName(-1), toTypeName(1.0), toTypeName('x'), toTypeName(NULL), toTypeName(1 + 1);

-- query
SELECT toTypeName(toUInt8(1) + toUInt8(1)), toTypeName(toUInt8(1) * toInt8(1)), toTypeName(toUInt32(1) - toUInt32(2)), toTypeName(toInt64(1) + toUInt64(1));

-- query
SELECT toTypeName(1 / 2), toTypeName(intDiv(7, 2)), toTypeName(7 % 3), toTypeName(toFloat32(1) + 1);

-- query
SELECT toTypeName(if(1, 1, -1)), toTypeName(if(1, toUInt8(1), toUInt16(2))), toTypeName(if(0, 1, 2.5)), toTypeName(if(1, 'a', NULL));

-- query
SELECT toTypeName(multiIf(1, 1, 0, 300, -1)), toTypeName(coalesce(NULL, 1)), toTypeName(ifNull(NULL, 'x')), toTypeName(nullIf(1, 1));

-- query
SELECT toTypeName([1, 2]), toTypeName([1, -1]), toTypeName(['a']), toTypeName([]), toTypeName(tuple(1, 'a')), toTypeName((1, 2.5));

-- query
SELECT toTypeName(map('a', 1)), toTypeName([NULL, 1]), toTypeName(toNullable(1)), toTypeName(toLowCardinality('x'));

-- query
SELECT toTypeName(toDate('2024-01-01')), toTypeName(toDateTime('2024-01-01 00:00:00')), toTypeName(toDateTime64('2024-01-01 00:00:00', 3)), toTypeName(toDateTime('2024-01-01 00:00:00', 'Europe/Berlin'));

-- query
SELECT toTypeName(now()), toTypeName(today()), toTypeName(now64()), toTypeName(now64(6));

-- query
SELECT 1 = 1, 1 != 2, 1 < 2, 'a' < 'b', NOT 1, 1 AND 0, 1 OR 0;

-- query
SELECT toTypeName(1 = 1), toTypeName(NULL = 1), toTypeName(1 AND NULL), toTypeName(isNull(1));

-- query
SELECT sum(1), count(), avg(1), min(1), max(1) FROM numbers(3);

-- query
SELECT toTypeName(sum(toUInt8(1))), toTypeName(sum(toInt8(1))), toTypeName(sum(1.5)), toTypeName(avg(toUInt8(1))), toTypeName(count()) FROM numbers(3);

-- query
SELECT toTypeName(min(toUInt8(1))), toTypeName(max(toInt16(1))), toTypeName(uniqExact(number)), toTypeName(any(number)), toTypeName(argMax('a', number)) FROM numbers(3);

-- query
SELECT count(*), count(1), COUNT(), Sum(number), SUM(number), countIf(number > 1) FROM numbers(5);

-- query
SELECT number AS n, n * 2 AS twice, toString(n) FROM numbers(3) ORDER BY n;

-- query
SELECT number + 1 FROM numbers(3) ORDER BY number + 1 DESC;

-- query
SELECT toDate('2024-03-05'), toString(toDate('2024-03-05')), CAST(1 AS String), CAST('7' AS UInt8), CAST(1.9 AS Int32);

-- query
SELECT 1 IN (1, 2), 3 NOT IN (1, 2), 'x' IN ('x'), (1, 'a') IN ((1, 'a'), (2, 'b'));

-- query
SELECT 1 BETWEEN 0 AND 2, 5 NOT BETWEEN 0 AND 2, 'b' LIKE 'a%', 'abc' LIKE '%b%', 'ABC' ILIKE 'a%', 'abc' NOT LIKE 'z%';

-- query
SELECT -(1), -(-1), +1, - 1.5, -toUInt8(1), toTypeName(-toUInt8(1));

-- query
SELECT 1 AS `weird name`, 2 AS "double quoted", 3 AS ascii_name;

-- query
SELECT tuple(1, 2).1, (1, 'b').2, [10, 20, 30][2], map('k', 'v')['k'];

-- query
SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END, CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END;

-- query
SELECT CASE WHEN 0 THEN 1 END, toTypeName(CASE WHEN 0 THEN 1 END);

-- query
SELECT true, false, toTypeName(true), true AND false, NOT true;

-- query
SELECT NULL, NULL IS NULL, 1 IS NOT NULL, NULL + 1, toTypeName(NULL + 1);

-- query
SELECT currentDatabase();

-- query
SELECT * FROM system.one;

-- query
SELECT dummy, dummy + 1 FROM system.one;

-- query
SELECT number FROM numbers(5);

-- query
SELECT number FROM numbers(3, 4);

-- query
SELECT toUInt64(3) AS x, x IN (1, 2, 3) AS hit;

-- query
SELECT 1 AS x HAVING x = 1;

-- query
SELECT 10 AS x WHERE x > 5;
