-- Float rollup sums whose exact decimal sum needs more than 38 significant
-- digits (DynamoDB's number precision): they used to fail the view
-- application ('precision up to 38 digits') and drop the row from every
-- rollup column of its key, permanently for later rows of the key too.

CREATE TABLE fsrc (k String, v Float64) ENGINE = MergeTree ORDER BY k;

CREATE TABLE fagg (
    k String,
    s AggregateFunction(sum, Float64),
    a AggregateFunction(avg, Float64),
    c AggregateFunction(count),
    ss SimpleAggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree ORDER BY k;

CREATE MATERIALIZED VIEW fagg_mv TO fagg AS
SELECT k, sumState(v) AS s, avgState(v) AS a, countState() AS c, sum(v) AS ss FROM fsrc GROUP BY k;

INSERT INTO fsrc VALUES ('x', 1500000.25);

INSERT INTO fsrc SELECT 'x', 0.1 + 0.2 - 0.3;

INSERT INTO fsrc VALUES ('x', 1e-20);

-- The noise first, then large values (the key used to stay broken).
INSERT INTO fsrc SELECT 'z', 0.1 + 0.2 - 0.3;

INSERT INTO fsrc VALUES ('z', 1500000.25);

INSERT INTO fsrc VALUES ('z', 1000000);

-- query: source_rows
SELECT k, count(), sum(v) FROM fsrc GROUP BY k ORDER BY k;

-- query: rollup_matches_source
SELECT k, countMerge(c), sumMerge(s), avgMerge(a), sum(ss) FROM fagg GROUP BY k ORDER BY k;

CREATE TABLE fsum (k UInt8, v Float64, w Float32, n UInt64) ENGINE = SummingMergeTree ORDER BY k;

INSERT INTO fsum VALUES (1, 1e30, 0.1, 1);

INSERT INTO fsum VALUES (1, 0.30000000000000004, 0.2, 2);

INSERT INTO fsum VALUES (2, 0.1, 0.5, 0);

INSERT INTO fsum VALUES (2, 0.2, 0.5, 0);

-- query: summing_float_sums
SELECT k, v, w, n FROM fsum FINAL ORDER BY k;
