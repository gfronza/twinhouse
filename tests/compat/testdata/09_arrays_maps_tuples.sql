-- Array / Tuple / Map construction and access, arrayJoin, ARRAY JOIN.

CREATE TABLE seg (
    id UInt32,
    tags Array(String),
    scores Array(Int32),
    attrs Map(String, String),
    pair Tuple(String, UInt64),
    nested Array(Array(UInt8)),
    maybe Array(Nullable(String))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO seg VALUES
    (1, ['a', 'b', 'c'], [3, -1, 2], {'country': 'US', 'tier': 'gold'}, ('x', 1), [[1, 2], []], ['p', NULL]),
    (2, [], [], {}, ('', 0), [], []),
    (3, ['b'], [100], {'country': 'DE'}, ('y\'q', 18446744073709551615), [[255]], [NULL]),
    (4, ['tab\t', 'nl\n', 'q"'], [-2147483648, 2147483647], {'k': 'v\\w'}, ('z', 5), [[0], [1, 1]], ['']);

-- query
-- formats: TSVWithNamesAndTypes,JSONCompact,JSONEachRow,CSV,Values,Native
SELECT * FROM seg ORDER BY id;

-- query
SELECT [1, 2, 3], ['a', 'b'], [[1], [2, 3]], [1, NULL], [], array(1, 2), [toNullable(1)];

-- query
-- formats: TSVWithNamesAndTypes,JSONCompact,JSONEachRow
SELECT (1, 'a'), tuple(1, 'a', 2.5), ((1, 2), 'x');

-- query
-- formats: TSVWithNamesAndTypes,JSONCompact
SELECT tuple(), toTypeName(tuple());

-- query
SELECT map('a', 1, 'b', 2), map(), map(1, [1]), toTypeName(map('a', 1));

-- query
SELECT [1, 2, 3][1], [1, 2, 3][-1], [1, 2, 3][5], ['a'][2], arrayElement([10, 20], 2);

-- query
SELECT id, tags[1], tags[2], scores[-1], length(tags), empty(tags), notEmpty(scores) FROM seg ORDER BY id;

-- query
SELECT id, attrs['country'], attrs['missing'], mapContains(attrs, 'tier'), length(attrs) FROM seg ORDER BY id;

-- query
SELECT id, mapKeys(attrs), mapValues(attrs) FROM seg ORDER BY id;

-- query
SELECT id, pair.1, pair.2, tupleElement(pair, 1), tupleElement(pair, 2) FROM seg ORDER BY id;

-- query
SELECT id, has(tags, 'b'), has(scores, 100), indexOf(tags, 'c') FROM seg ORDER BY id;

-- unsupported: countEqual is outside the DESIGN §3 function list
SELECT countEqual(['a', 'b'], 'b');

-- query
SELECT id, arrayConcat(tags, ['z']), arraySlice(scores, 1, 1), arrayReverse(tags), arrayDistinct(['a', 'a', 'b']) FROM seg ORDER BY id;

-- query
SELECT id, arraySort(scores), arrayReverseSort(scores), arrayUniq(tags), arrayCount([1, 0, 1]) FROM seg ORDER BY id;

-- query
SELECT range(5), range(1, 4), range(0), arrayStringConcat(range(3), ',');

-- query
SELECT id, nested, nested[1], length(nested), maybe, maybe[2] FROM seg ORDER BY id;

-- query
SELECT arrayJoin([1, 2, 3]) AS x, x * 10;

-- query
SELECT id, arrayJoin(tags) AS tag FROM seg ORDER BY id, tag;

-- query
SELECT tag, count() FROM seg ARRAY JOIN tags AS tag GROUP BY tag ORDER BY tag;

-- query
SELECT id, s FROM seg ARRAY JOIN scores AS s WHERE s > 0 ORDER BY id, s;

-- query
SELECT id, t FROM seg LEFT ARRAY JOIN tags AS t ORDER BY id, t;

-- query
SELECT arrayJoin(['a', 'b']) AS l, arrayJoin([1, 2]) AS n ORDER BY l, n;

-- query
SELECT groupArray(x) FROM (SELECT arrayJoin([3, 1, 2]) AS x ORDER BY x);

-- query
SELECT [1, 2] = [1, 2], [1] < [2], (1, 'a') = (1, 'a'), (1, 2) < (1, 3), ['a', 'b'] != ['a'];

-- query
SELECT id FROM seg WHERE has(tags, 'b') ORDER BY id;

-- query
SELECT id FROM seg WHERE tags = [] ORDER BY id;

-- query
SELECT id FROM seg WHERE attrs['country'] = 'US';

-- query
SELECT id FROM seg WHERE pair = ('x', 1);

-- query
SELECT id FROM seg WHERE pair.2 > 1 ORDER BY id;

-- query: colname_float_array_literal
SELECT toTypeName([1, 2.5]), toTypeName(['a', NULL]), toTypeName([[1], []]), toTypeName((1, NULL)), toTypeName([(1, 'a')]);

-- query
SELECT splitByChar('/', '/a/b/'), arrayStringConcat(splitByChar('/', 'x/y'), '+'), length(splitByChar(',', 'a,b,c'));

-- query
-- formats: TabSeparated,CSV,JSONEachRow,JSONCompactEachRow,Values
SELECT ['it\'s', 'tab\t', 'back\\', 'nl\n'] AS a, ('q"', 1) AS t, map('k\'', 'v\t') AS m;

-- query
SELECT [toDate('2024-01-01'), toDate('2024-01-02')], [toDateTime('2024-01-01 00:00:00')], (toDateTime64('2024-01-01 00:00:00.5', 3), 1);

-- query
SELECT tuple(1, 'a') AS t, t.1, t.2, toTypeName(t);

-- query
SELECT CAST((1, 'a') AS Tuple(a UInt8, b String)) AS t, tupleElement(t, 'a'), tupleElement(t, 2);

-- query: named_tuple_dot_access
SELECT CAST((1, 'a') AS Tuple(a UInt8, b String)) AS t, t.a, t.b;

-- not-implemented: lambdas / higher-order functions
SELECT arrayMap(x -> x * 2, [1, 2, 3]);

-- not-implemented: lambdas / higher-order functions
SELECT arrayFilter(x -> x > 1, [1, 2, 3]);

-- Nested NULL compares greater than every value (and NaN) in array/map
-- comparison and ORDER BY: [1] < [1,2] < [1,nan] < [1,NULL] < [NULL].

-- query: nested_null_array_compare
SELECT [1, NULL] < [1, 2], [NULL] > [1], [1, NULL] > [1, nan], [nan] < [NULL], [1] < [1, NULL], [NULL] = [NULL], [1, NULL] = [1, NULL];

-- query: nested_null_array_order_asc
SELECT x FROM (SELECT arrayJoin([[1, NULL], [1, 2], [NULL], [1], [], [1, nan], [inf], [nan]]) AS x) ORDER BY x;

-- query: nested_null_array_where
SELECT x FROM (SELECT arrayJoin([[1, NULL], [1, 2], [NULL], [1]]) AS x) WHERE x > [1, 2] ORDER BY x;

-- query: nested_null_array_column
SELECT id, maybe FROM seg WHERE maybe > ['p'] ORDER BY maybe, id;
