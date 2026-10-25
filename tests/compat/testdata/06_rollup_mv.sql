-- Workload shape 2: MATERIALIZED VIEW ... TO an AggregatingMergeTree rollup
-- with SimpleAggregateFunction counters/last_seen and uniqExact states.
-- Rollups are always read with FINAL or -Merge + GROUP BY on BOTH servers so
-- ClickHouse's unmerged parts cannot produce false diffs.

CREATE TABLE impressions (
    tenant_id String,
    event_time DateTime,
    event_id String,
    campaign_id UInt64,
    user_id String,
    is_click UInt8,
    cost_micros UInt64,
    bid Float64
) ENGINE = MergeTree ORDER BY (tenant_id, event_time, event_id);

CREATE TABLE campaign_hourly (
    tenant_id String,
    campaign_id UInt64,
    hour DateTime,
    events SimpleAggregateFunction(sum, UInt64),
    clicks SimpleAggregateFunction(sum, UInt64),
    spend SimpleAggregateFunction(sum, UInt64),
    last_seen SimpleAggregateFunction(max, DateTime),
    first_seen SimpleAggregateFunction(min, DateTime),
    users AggregateFunction(uniqExact, String),
    avg_bid AggregateFunction(avg, Float64),
    top_bid AggregateFunction(max, Float64)
) ENGINE = AggregatingMergeTree ORDER BY (tenant_id, campaign_id, hour);

CREATE MATERIALIZED VIEW campaign_hourly_mv TO campaign_hourly AS
SELECT
    tenant_id,
    campaign_id,
    toStartOfHour(event_time) AS hour,
    count() AS events,
    sum(is_click) AS clicks,
    sum(cost_micros) AS spend,
    max(event_time) AS last_seen,
    min(event_time) AS first_seen,
    uniqExactState(user_id) AS users,
    avgState(bid) AS avg_bid,
    maxState(bid) AS top_bid
FROM impressions
GROUP BY tenant_id, campaign_id, hour;

CREATE TABLE tenant_daily (
    tenant_id String,
    day Date,
    events SimpleAggregateFunction(sum, UInt64),
    users AggregateFunction(uniqExact, String)
) ENGINE = AggregatingMergeTree ORDER BY (tenant_id, day);

CREATE MATERIALIZED VIEW tenant_daily_mv TO tenant_daily AS
SELECT tenant_id, toDate(event_time) AS day, count() AS events, uniqExactState(user_id) AS users
FROM impressions
GROUP BY tenant_id, day;

-- Float sums are added in IEEE arithmetic, like ClickHouse (0.1 + 0.2).
CREATE TABLE bid_totals (
    tenant_id String,
    bid_sum SimpleAggregateFunction(sum, Float64),
    bid_avg AggregateFunction(avg, Float64)
) ENGINE = AggregatingMergeTree ORDER BY tenant_id;

CREATE MATERIALIZED VIEW bid_totals_mv TO bid_totals AS
SELECT tenant_id, sum(bid) AS bid_sum, avgState(bid) AS bid_avg FROM impressions GROUP BY tenant_id;

-- batch 1
INSERT INTO impressions VALUES
    ('acme', '2024-05-01 10:00:00', 'e01', 1, 'u1', 0, 1500, 0.25),
    ('acme', '2024-05-01 10:05:00', 'e02', 1, 'u1', 1, 0, 0.75),
    ('acme', '2024-05-01 10:59:59', 'e03', 1, 'u2', 0, 1700, 0.5),
    ('acme', '2024-05-01 11:00:00', 'e04', 1, 'u3', 0, 1200, 0.125),
    ('globex', '2024-05-01 10:30:00', 'g01', 7, 'v1', 0, 18446744073709551615, 2.5);

-- batch 2 (same keys again: counters add, uniq users merge, max/min fold)
INSERT INTO impressions VALUES
    ('acme', '2024-05-01 10:10:00', 'e05', 1, 'u2', 1, 0, 1.0),
    ('acme', '2024-05-01 10:20:00', 'e06', 1, 'u9', 0, 100, 0.25),
    ('acme', '2024-05-01 10:45:00', 'e07', 2, 'u1', 0, 300, 0.5),
    ('globex', '2024-05-01 10:31:00', 'g02', 7, 'v1', 1, 1, 0.5),
    ('initech', '2024-05-02 00:00:00', 'i01', 3, '', 0, 0, 0);

-- batch 3: JSONEachRow, one row per new user, tz-free DateTime text
INSERT INTO impressions FORMAT JSONEachRow
{"tenant_id":"acme","event_time":"2024-05-01 10:30:00","event_id":"e08","campaign_id":1,"user_id":"ü-unicode","is_click":0,"cost_micros":5,"bid":0.0625}
{"tenant_id":"acme","event_time":"2024-05-02 23:59:59","event_id":"e09","campaign_id":1,"user_id":"u1","is_click":1,"cost_micros":7,"bid":0.375};

-- query: rollup_readback
SELECT tenant_id, campaign_id, hour, sum(events) AS events, sum(clicks) AS clicks, sum(spend) AS spend, max(last_seen) AS last_seen, min(first_seen) AS first_seen, uniqExactMerge(users) AS users, avgMerge(avg_bid) AS avg_bid, maxMerge(top_bid) AS top_bid
FROM campaign_hourly
GROUP BY tenant_id, campaign_id, hour
ORDER BY tenant_id, campaign_id, hour;

-- query: rollup_readback_per_tenant
SELECT tenant_id, sum(events), sum(clicks), sum(spend), max(last_seen), uniqExactMerge(users)
FROM campaign_hourly
GROUP BY tenant_id
ORDER BY tenant_id;

-- query: rollup_final_simple_columns
SELECT tenant_id, campaign_id, hour, events, clicks, spend, last_seen, first_seen
FROM campaign_hourly FINAL
ORDER BY tenant_id, campaign_id, hour;

-- query: rollup_final_where_key
SELECT hour, events, clicks FROM campaign_hourly FINAL WHERE tenant_id = 'acme' AND campaign_id = 1 ORDER BY hour;

-- query: rollup_final_finalize
SELECT tenant_id, campaign_id, hour, finalizeAggregation(users), finalizeAggregation(avg_bid), finalizeAggregation(top_bid)
FROM campaign_hourly FINAL
ORDER BY tenant_id, campaign_id, hour;

-- query: rollup_merge_with_param
-- param: tenant=acme
-- param: from=2024-05-01 10:00:00
-- param: to=2024-05-01 23:00:00
SELECT campaign_id, sum(events) AS events, uniqExactMerge(users) AS users
FROM campaign_hourly
WHERE tenant_id = {tenant:String} AND hour BETWEEN {from:DateTime} AND {to:DateTime}
GROUP BY campaign_id
ORDER BY campaign_id;

-- query: rollup_daily
SELECT tenant_id, day, sum(events), uniqExactMerge(users) FROM tenant_daily GROUP BY tenant_id, day ORDER BY tenant_id, day;

-- query: rollup_daily_final
SELECT tenant_id, day, events FROM tenant_daily FINAL ORDER BY tenant_id, day;

-- query: rollup_types
SELECT toTypeName(events), toTypeName(last_seen), toTypeName(users), toTypeName(avg_bid) FROM campaign_hourly FINAL WHERE tenant_id = 'globex';

-- query: merge_result_types
SELECT toTypeName(sum(events)), toTypeName(uniqExactMerge(users)), toTypeName(avgMerge(avg_bid)), toTypeName(maxMerge(top_bid)), toTypeName(max(last_seen)) FROM campaign_hourly;

-- query: rollup_matches_source
SELECT r.tenant_id, r.events, s.events, r.users, s.users
FROM (SELECT tenant_id, sum(events) AS events, uniqExactMerge(users) AS users FROM campaign_hourly GROUP BY tenant_id) AS r
INNER JOIN (SELECT tenant_id, count() AS events, uniqExact(user_id) AS users FROM impressions GROUP BY tenant_id) AS s ON r.tenant_id = s.tenant_id
ORDER BY r.tenant_id;

-- query: rollup_having
SELECT tenant_id, campaign_id, sum(events) AS e FROM campaign_hourly GROUP BY tenant_id, campaign_id HAVING uniqExactMerge(users) > 1 ORDER BY tenant_id, campaign_id;

-- query: rollup_count_rows_final
SELECT count() FROM campaign_hourly FINAL;

-- query: state_merge_inline
SELECT tenant_id, uniqExactMerge(s), sumMerge(c), avgMerge(a)
FROM (SELECT tenant_id, uniqExactState(user_id) AS s, sumState(cost_micros) AS c, avgState(bid) AS a FROM impressions GROUP BY tenant_id, campaign_id)
GROUP BY tenant_id
ORDER BY tenant_id;

-- query: count_state_merge
SELECT countMerge(c), minMerge(m), maxMerge(x)
FROM (SELECT countState() AS c, minState(event_time) AS m, maxState(event_time) AS x FROM impressions GROUP BY tenant_id);

-- query: argmax_state_merge
SELECT tenant_id, argMaxMerge(st) FROM (SELECT tenant_id, argMaxState(event_id, event_time) AS st FROM impressions GROUP BY tenant_id, campaign_id) GROUP BY tenant_id ORDER BY tenant_id;

-- query: describe_rollup
DESCRIBE TABLE campaign_hourly;

-- query: describe_source
DESCRIBE impressions;

-- batch 4 after the reads: rollup must keep accumulating
INSERT INTO impressions VALUES ('acme', '2024-05-01 10:00:01', 'e10', 1, 'u7', 1, 1, 9.5);

-- query: rollup_after_more_inserts
SELECT tenant_id, campaign_id, hour, sum(events), sum(clicks), uniqExactMerge(users), maxMerge(top_bid), max(last_seen)
FROM campaign_hourly WHERE tenant_id = 'acme'
GROUP BY tenant_id, campaign_id, hour
ORDER BY campaign_id, hour;

-- query: rollup_final_after_more_inserts
SELECT hour, events, clicks, spend, last_seen FROM campaign_hourly FINAL WHERE tenant_id = 'acme' ORDER BY campaign_id, hour;

INSERT INTO impressions VALUES ('floaty', '2024-05-01 00:00:00', 'f1', 1, 'a', 0, 0, 0.1), ('floaty', '2024-05-01 00:00:01', 'f2', 1, 'b', 0, 0, 0.2);

-- query: float_sum_dyadic_matches
SELECT tenant_id, sum(bid_sum), avgMerge(bid_avg) FROM bid_totals WHERE tenant_id != 'floaty' GROUP BY tenant_id ORDER BY tenant_id;

-- query: float_sum_ieee
SELECT tenant_id, sum(bid_sum), avgMerge(bid_avg) FROM bid_totals WHERE tenant_id = 'floaty' GROUP BY tenant_id;
