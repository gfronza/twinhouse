-- NULL and NaN inside arrays and tuples (comparison operators, extrema,
-- ORDER BY), enum extrema kept in DynamoDB rollups, and TRUNCATE of a
-- rollup.

-- query: tuple comparison with literal NULL elements
SELECT (1,NULL) < (2,1), (1,NULL) = (1,NULL), toTypeName((1,NULL) < (2,1)), toTypeName((1,2) = (1,NULL));

-- query: tuple comparison with NaN elements
SELECT (1,nan) < (1,2), (1,nan) = (1,nan), (1,nan) != (1,nan), (1,nan) <= (1,nan), (nan,1) < (2,1), ((1,nan),2) < ((1,2),3);

-- query: tuple comparison with nullable elements
WITH CAST(NULL, 'Nullable(UInt8)') AS n SELECT (1,n) < (1,2), (2,n) < (1,2), (1,n) = (2,1), (1,n) = (1,1), (1,n) != (2,1), (1,n) != (1,1), (n,1) > (1,1), ((1,n),1) < ((2,1),1), (1,n) >= (0,5), toTypeName((1,n) < (2,1));

-- query: arrays keep compareAt semantics
SELECT [nan] = [nan], [1,NULL] = [1,NULL], ([NULL],1) < ([1],1), toTypeName(([NULL],1) < ([1],1));

-- query: map comparison
SELECT map(1,NULL) > map(1,2), map(1,2) = map(1,2), map(1,2) < map(2,1), map(1,2) < map(1,2,3,4), toTypeName(map(1,2) < map(2,1)), map('a',nan) = map('a',nan), map(1,2) != map(1,3);

-- query: greatest and least over arrays with NULL and NaN
SELECT greatest([1,NULL],[1,2]), least([NULL],[1]), greatest([1,nan],[1,2]), least([1,nan],[1,2]);

-- query: min and max over arrays with NULL
SELECT max(x), min(x) FROM (SELECT arrayJoin([[1,NULL],[1,2],[NULL],[1]]) AS x);

-- query: min and max over arrays with NaN
SELECT max(x), min(x) FROM (SELECT arrayJoin([[1,nan],[1,2]]) AS x);

-- query: argMin and argMax by arrays with NULL
SELECT argMax(s, x), argMin(s, x) FROM (SELECT arrayJoin([([1,NULL],'a'),([1,2],'b'),([NULL],'c'),([1],'d')]) AS t, t.1 AS x, t.2 AS s);

-- query: order by array with NULL and NaN
SELECT x FROM (SELECT arrayJoin([[1,NULL],[1,nan],[NULL],[1],[nan],[inf],[1,2],[]]) AS x) ORDER BY x;

-- query: order by array desc
SELECT x FROM (SELECT arrayJoin([[1,NULL],[1,nan],[NULL],[1],[nan],[inf],[1,2],[]]) AS x) ORDER BY x DESC;

-- query: order by array nulls first
SELECT x FROM (SELECT arrayJoin([[1,NULL],[1,nan],[NULL],[1],[nan],[inf],[1,2],[]]) AS x) ORDER BY x NULLS FIRST;

-- query: order by array desc nulls first
SELECT x FROM (SELECT arrayJoin([[1,NULL],[1,nan],[NULL],[1],[nan],[inf],[1,2],[]]) AS x) ORDER BY x DESC NULLS FIRST;

CREATE TABLE lvl_src (k String, id UInt32, lvl Enum8('high' = 3, 'low' = 1, 'mid' = 2)) ENGINE = MergeTree ORDER BY (k, id);

CREATE TABLE lvl_roll (k String, top SimpleAggregateFunction(max, Enum8('high' = 3, 'low' = 1, 'mid' = 2)), bottom SimpleAggregateFunction(min, Enum8('high' = 3, 'low' = 1, 'mid' = 2)), top_id AggregateFunction(argMax, UInt32, Enum8('high' = 3, 'low' = 1, 'mid' = 2)), n SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k;

CREATE MATERIALIZED VIEW lvl_mv TO lvl_roll AS SELECT k, max(lvl) AS top, min(lvl) AS bottom, argMaxState(id, lvl) AS top_id, count() AS n FROM lvl_src GROUP BY k;

INSERT INTO lvl_src VALUES ('a', 1, 'mid');

INSERT INTO lvl_src VALUES ('a', 2, 'high');

INSERT INTO lvl_src VALUES ('a', 3, 'low');

INSERT INTO lvl_src VALUES ('b', 4, 'low'), ('b', 5, 'mid');

-- query: enum extrema in a rollup compare by value
SELECT k, max(top), min(bottom), argMaxMerge(top_id), sum(n) FROM lvl_roll GROUP BY k ORDER BY k;

TRUNCATE TABLE lvl_roll;

-- query: truncated rollup is empty
SELECT count() FROM lvl_roll;

INSERT INTO lvl_src VALUES ('c', 6, 'high');

-- query: rollup after truncate counts only new rows
SELECT k, max(top), min(bottom), argMaxMerge(top_id), sum(n) FROM lvl_roll GROUP BY k ORDER BY k;
