-- Workload shape 1: append-only MergeTree event table (campaign analytics),
-- with filtering, grouping, aggregates, ORDER BY/NULLS, LIMIT BY, DISTINCT.

CREATE TABLE ad_events (
    tenant_id String,
    event_time DateTime,
    event_id String,
    campaign_id UInt64,
    user_id String,
    kind LowCardinality(String),
    is_click UInt8,
    cost_micros UInt64,
    bid Float64,
    delta Int64,
    country Nullable(String),
    referrer Nullable(String),
    ts_ms DateTime64(3),
    tags Array(String)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, event_time, event_id);

INSERT INTO ad_events VALUES
    ('acme', '2024-05-01 10:00:00', 'e01', 1, 'u1', 'impression', 0, 1500, 0.25, 1, 'US', NULL, '2024-05-01 10:00:00.001', ['a', 'b']),
    ('acme', '2024-05-01 10:00:05', 'e02', 1, 'u1', 'click', 1, 0, 0.25, -1, 'US', 'https://ex.com/?q=1&r=2', '2024-05-01 10:00:05.250', []),
    ('acme', '2024-05-01 10:15:00', 'e03', 1, 'u2', 'impression', 0, 1700, 0.5, 3, 'DE', NULL, '2024-05-01 10:15:00.999', ['b']),
    ('acme', '2024-05-01 11:20:00', 'e04', 2, 'u3', 'impression', 0, 1200, 0.125, 0, NULL, NULL, '2024-05-01 11:20:00.000', []),
    ('acme', '2024-05-01 11:21:00', 'e05', 2, 'u3', 'click', 1, 0, 0.125, 5, NULL, '', '2024-05-01 11:21:00.500', ['c']),
    ('acme', '2024-05-02 00:00:00', 'e06', 2, 'u4', 'conversion', 0, 99000, 1.75, -7, 'FR', 'ref "quoted"', '2024-05-02 00:00:00.000', ['a', 'c']),
    ('globex', '2024-05-01 09:59:59', 'g01', 10, 'u1', 'impression', 0, 0, 0, 0, 'US', NULL, '2024-05-01 09:59:59.999', []),
    ('globex', '2024-05-01 10:00:00', 'g02', 10, 'v9', 'click', 1, 18446744073709551615, 2.5, 9223372036854775807, 'BR', 'tab\there', '2024-05-01 10:00:00.000', ['x']),
    ('globex', '2024-05-03 23:59:59', 'g03', 11, 'v9', 'impression', 0, 10, 0.1, -9223372036854775808, 'BR', 'ünï', '2024-05-03 23:59:59.123', ['y', 'z']),
    ('initech', '2024-04-30 12:00:00', 'i01', 20, '', 'impression', 0, 1, 0.2, 2, '', NULL, '2024-04-30 12:00:00.010', []);

INSERT INTO ad_events (tenant_id, event_time, event_id, campaign_id, user_id, kind, ts_ms) VALUES
    ('initech', '2024-04-30 12:00:01', 'i02', 20, 'w1', 'impression', '2024-04-30 12:00:01.000');

-- query
-- formats: TSVWithNamesAndTypes,JSONCompact,JSONEachRow,CSVWithNames,Native
SELECT * FROM ad_events ORDER BY tenant_id, event_time, event_id;

-- query
SELECT count() FROM ad_events;

-- query
SELECT tenant_id, count() AS n FROM ad_events GROUP BY tenant_id ORDER BY tenant_id;

-- query
SELECT event_id FROM ad_events WHERE tenant_id = 'acme' ORDER BY event_time, event_id;

-- query
SELECT event_id, event_time FROM ad_events WHERE tenant_id = 'acme' AND event_time >= '2024-05-01 10:00:05' AND event_time < '2024-05-02 00:00:00' ORDER BY event_time;

-- query
SELECT event_id FROM ad_events WHERE tenant_id IN ('acme', 'initech') AND event_time BETWEEN '2024-04-30 00:00:00' AND '2024-05-01 10:15:00' ORDER BY tenant_id, event_id;

-- query
SELECT event_id FROM ad_events WHERE tenant_id = 'acme' AND event_time = '2024-05-01 10:00:00' AND event_id = 'e01';

-- query
SELECT event_id FROM ad_events WHERE campaign_id = 2 ORDER BY event_id;

-- query
SELECT event_id FROM ad_events WHERE tenant_id = 'nobody';

-- query
SELECT count(), sum(is_click), sum(cost_micros), avg(bid), min(bid), max(bid), sum(delta) FROM ad_events WHERE tenant_id = 'acme';

-- query: sum_uint64_wraps_in_group
SELECT tenant_id, sum(cost_micros), sum(delta), sum(bid) FROM ad_events GROUP BY tenant_id ORDER BY tenant_id;

-- query
SELECT tenant_id, campaign_id, count() AS impressions, countIf(is_click = 1) AS clicks, sumIf(cost_micros, kind = 'impression') AS spend, avgIf(bid, is_click = 0) AS avg_bid FROM ad_events GROUP BY tenant_id, campaign_id ORDER BY tenant_id, campaign_id;

-- query
SELECT tenant_id, uniqExact(user_id), uniq(user_id), count(DISTINCT user_id), uniqExactIf(user_id, is_click = 1) FROM ad_events GROUP BY tenant_id ORDER BY tenant_id;

-- query
SELECT tenant_id, argMax(event_id, ts_ms), argMin(event_id, ts_ms), max(ts_ms), min(ts_ms) FROM ad_events GROUP BY tenant_id ORDER BY tenant_id;

-- query
SELECT tenant_id, minIf(event_time, is_click = 1), maxIf(cost_micros, kind = 'impression'), anyIf(event_id, event_id = 'e02') FROM ad_events WHERE tenant_id = 'acme' GROUP BY tenant_id;

-- query
SELECT kind, count() FROM ad_events GROUP BY kind ORDER BY count() DESC, kind;

-- query
SELECT toStartOfHour(event_time) AS hour, tenant_id, count() FROM ad_events GROUP BY hour, tenant_id ORDER BY hour, tenant_id;

-- query
SELECT toDate(event_time) AS day, count() AS c FROM ad_events GROUP BY day HAVING c > 1 ORDER BY day;

-- query
SELECT tenant_id, count() FROM ad_events GROUP BY 1 ORDER BY 1;

-- query
SELECT tenant_id, sum(cost_micros) AS s FROM ad_events GROUP BY tenant_id HAVING sum(is_click) >= 1 ORDER BY s DESC;

-- query
SELECT country, count() FROM ad_events GROUP BY country ORDER BY country;

-- query
SELECT country, count() FROM ad_events GROUP BY country ORDER BY country NULLS FIRST;

-- query
SELECT country, count() FROM ad_events GROUP BY country ORDER BY country DESC;

-- query
SELECT country, count() FROM ad_events GROUP BY country ORDER BY country DESC NULLS FIRST;

-- query
SELECT event_id, referrer FROM ad_events ORDER BY referrer NULLS LAST, event_id;

-- query
SELECT count(country), count(referrer), count(*), countIf(country IS NULL) FROM ad_events;

-- query
SELECT event_id, coalesce(country, '??'), ifNull(referrer, '-'), isNull(country), isNotNull(referrer) FROM ad_events ORDER BY event_id;

-- query
SELECT tenant_id, event_id, cost_micros FROM ad_events ORDER BY cost_micros DESC, event_id LIMIT 3;

-- query
SELECT event_id FROM ad_events ORDER BY event_id LIMIT 3 OFFSET 2;

-- query
SELECT event_id FROM ad_events ORDER BY event_id LIMIT 2, 3;

-- query
SELECT tenant_id, event_id FROM ad_events ORDER BY tenant_id, event_time DESC, event_id LIMIT 1 BY tenant_id;

-- query
SELECT tenant_id, campaign_id, event_id FROM ad_events ORDER BY tenant_id, campaign_id, event_id LIMIT 2 BY tenant_id, campaign_id;

-- query
SELECT DISTINCT tenant_id FROM ad_events ORDER BY tenant_id;

-- query
SELECT DISTINCT tenant_id, kind FROM ad_events ORDER BY tenant_id, kind;

-- query
SELECT DISTINCT country FROM ad_events ORDER BY country;

-- query
-- unordered
SELECT DISTINCT user_id FROM ad_events;

-- query
-- unordered
SELECT tenant_id, kind, count() FROM ad_events GROUP BY tenant_id, kind;

-- query
SELECT event_id, length(tags), has(tags, 'a'), tags[1] FROM ad_events ORDER BY event_id;

-- query
SELECT event_id, ts_ms, toUnixTimestamp(ts_ms), toStartOfMinute(ts_ms), formatDateTime(ts_ms, '%F %T') FROM ad_events ORDER BY ts_ms;

-- query
SELECT event_id FROM ad_events WHERE ts_ms > '2024-05-01 10:00:00.000' AND ts_ms <= '2024-05-01 10:15:00.999' ORDER BY ts_ms;

-- query
SELECT event_id, bid * 4, bid / 3, round(bid * 100) FROM ad_events WHERE tenant_id = 'acme' ORDER BY event_id;

-- query
SELECT event_id, multiIf(cost_micros = 0, 'free', cost_micros < 1500, 'cheap', 'pricey') AS tier FROM ad_events ORDER BY event_id;

-- query
SELECT event_id, if(is_click, 'click', 'view'), if(country = 'US', 1, 0) FROM ad_events ORDER BY event_id;

-- query
SELECT event_id, user_id FROM ad_events WHERE user_id = '' OR referrer = '' ORDER BY event_id;

-- query
SELECT event_id FROM ad_events WHERE referrer LIKE '%quoted%' OR referrer LIKE '%\t%' ORDER BY event_id;

-- query
SELECT event_id FROM ad_events WHERE NOT (tenant_id = 'acme') AND campaign_id > 10 ORDER BY event_id;

-- query
SELECT event_id FROM ad_events PREWHERE tenant_id = 'acme' WHERE kind = 'click' ORDER BY event_id;

-- query
SELECT event_id FROM ad_events WHERE tenant_id = 'acme' AND (is_click = 1 OR cost_micros > 1600) ORDER BY event_id;

-- query
SELECT tenant_id, max(event_time) AS last_seen, min(event_time) AS first_seen, dateDiff('second', min(event_time), max(event_time)) AS span FROM ad_events GROUP BY tenant_id ORDER BY tenant_id;

-- query
SELECT sum(cost_micros) FROM ad_events WHERE tenant_id != 'globex';

-- query
SELECT tenant_id, groupArray(event_id) FROM (SELECT tenant_id, event_id FROM ad_events ORDER BY tenant_id, event_id) GROUP BY tenant_id ORDER BY tenant_id;

-- query
SELECT tenant_id, arraySort(groupUniqArray(kind)) FROM ad_events GROUP BY tenant_id ORDER BY tenant_id;

-- query
SELECT any(tenant_id) FROM ad_events WHERE event_id = 'g02';

-- query
SELECT count() FROM ad_events WHERE country IN ('US', 'BR');

-- query
SELECT count() FROM ad_events WHERE country NOT IN ('US', 'BR');

-- query
SELECT event_id FROM ad_events WHERE event_id IN (SELECT event_id FROM ad_events WHERE is_click = 1) ORDER BY event_id;

-- query
SELECT event_id, toTypeName(kind), toTypeName(country), toTypeName(tags) FROM ad_events WHERE event_id = 'e01';

-- query
SELECT tenant_id, sum(is_click) / count() AS ctr FROM ad_events GROUP BY tenant_id ORDER BY tenant_id;

-- query
SELECT * FROM ad_events WHERE event_id = 'i02';

-- query
SELECT tenant_id, event_id FROM ad_events ORDER BY tenant_id DESC, event_id DESC LIMIT 4;
