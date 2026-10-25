-- Subqueries, CTEs, UNION ALL, JOINs, scalar subqueries, IN (subquery), views.

CREATE TABLE campaigns (campaign_id UInt64, tenant_id String, name String, budget Nullable(UInt64)) ENGINE = MergeTree ORDER BY campaign_id;

CREATE TABLE stats (tenant_id String, campaign_id UInt64, day Date, clicks UInt64, spend Float64) ENGINE = MergeTree ORDER BY (tenant_id, campaign_id, day);

INSERT INTO campaigns VALUES (1, 'acme', 'Spring Sale', 1000), (2, 'acme', 'Summer', NULL), (3, 'globex', 'Launch', 50), (4, 'initech', 'Dormant', 0);

INSERT INTO stats VALUES
    ('acme', 1, '2024-05-01', 10, 1.5),
    ('acme', 1, '2024-05-02', 5, 0.25),
    ('acme', 2, '2024-05-01', 0, 0),
    ('globex', 3, '2024-05-01', 7, 3.75),
    ('globex', 9, '2024-05-02', 1, 0.5);

-- query
SELECT c, n FROM (SELECT campaign_id AS c, count() AS n FROM stats GROUP BY campaign_id) ORDER BY c;

-- query
SELECT * FROM (SELECT tenant_id, sum(clicks) AS clicks FROM stats GROUP BY tenant_id) WHERE clicks > 5 ORDER BY tenant_id;

-- query
SELECT max(total) FROM (SELECT tenant_id, sum(clicks) AS total FROM stats GROUP BY tenant_id);

-- query
WITH top AS (SELECT campaign_id, sum(clicks) AS c FROM stats GROUP BY campaign_id) SELECT campaign_id, c FROM top WHERE c > 0 ORDER BY c DESC;

-- query
WITH 10 AS threshold SELECT campaign_id, clicks FROM stats WHERE clicks >= threshold;

-- query
WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT y FROM b;

-- query
WITH (SELECT max(clicks) FROM stats) AS mx SELECT campaign_id, day FROM stats WHERE clicks = mx;

-- query
SELECT campaign_id, (SELECT count() FROM campaigns) AS total FROM stats ORDER BY campaign_id, day LIMIT 2;

-- query
SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) ORDER BY x DESC;

-- query: union_default_mode_distinct_url_setting
-- setting: union_default_mode=DISTINCT
SELECT x FROM (SELECT 1 AS x UNION SELECT 1 UNION SELECT 2) ORDER BY x;

-- query: union_default_mode_all_url_setting
-- setting: union_default_mode=ALL
SELECT x FROM (SELECT 1 AS x UNION SELECT 1 UNION SELECT 2) ORDER BY x;

-- query: identifier_params_db_and_table
-- param: d=$DB
-- param: t=campaigns
SELECT count() FROM {d:Identifier}.{t:Identifier};

-- error: 558
SELECT 1 UNION SELECT 2;

-- query
-- unordered
SELECT tenant_id FROM campaigns UNION ALL SELECT tenant_id FROM stats;

-- query
SELECT * FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) ORDER BY k;

-- query
SELECT toTypeName(x) FROM (SELECT toUInt8(1) AS x UNION ALL SELECT toUInt16(2)) ORDER BY x;

-- query
SELECT campaign_id FROM campaigns WHERE campaign_id IN (SELECT campaign_id FROM stats WHERE clicks > 0) ORDER BY campaign_id;

-- query
SELECT campaign_id FROM campaigns WHERE campaign_id NOT IN (SELECT campaign_id FROM stats) ORDER BY campaign_id;

-- query
SELECT c.name, s.day, s.clicks FROM stats AS s INNER JOIN campaigns AS c ON s.campaign_id = c.campaign_id ORDER BY c.name, s.day;

-- query
SELECT c.campaign_id, c.name, s.clicks FROM campaigns AS c LEFT JOIN stats AS s ON s.campaign_id = c.campaign_id ORDER BY c.campaign_id, s.clicks;

-- query
SELECT s.campaign_id, c.name, c.budget FROM stats AS s LEFT JOIN campaigns AS c ON s.campaign_id = c.campaign_id ORDER BY s.campaign_id, s.day;

-- query
SELECT c.name, sum(s.clicks) AS clicks, sum(s.spend) AS spend FROM campaigns AS c INNER JOIN stats AS s ON c.campaign_id = s.campaign_id GROUP BY c.name ORDER BY c.name;

-- query
SELECT campaign_id, name, clicks FROM campaigns INNER JOIN (SELECT campaign_id, sum(clicks) AS clicks FROM stats GROUP BY campaign_id) AS agg USING (campaign_id) ORDER BY campaign_id;

-- query
SELECT c.tenant_id, c.name, s.day FROM campaigns AS c INNER JOIN stats AS s ON c.campaign_id = s.campaign_id AND c.tenant_id = s.tenant_id ORDER BY c.name, s.day;

-- query
SELECT count() FROM campaigns AS c CROSS JOIN stats AS s;

-- query
SELECT a.number, b.number FROM numbers(3) AS a INNER JOIN numbers(5) AS b ON a.number = b.number ORDER BY a.number;

-- query
SELECT tenant_id, campaign_id, clicks, clicks / (SELECT sum(clicks) FROM stats) AS share FROM stats ORDER BY tenant_id, campaign_id, day;

-- query
SELECT * FROM (SELECT * FROM (SELECT campaign_id, name FROM campaigns) WHERE campaign_id > 1) ORDER BY campaign_id DESC LIMIT 2;

-- query
SELECT tenant_id, count() FROM (SELECT DISTINCT tenant_id, campaign_id FROM stats) GROUP BY tenant_id ORDER BY tenant_id;

-- query
SELECT name, budget, coalesce(budget, 0) * 2 FROM campaigns ORDER BY budget NULLS FIRST, name;

-- unsupported: EXISTS (subquery) is not in the DESIGN §3 list
SELECT EXISTS (SELECT 1 FROM stats WHERE clicks > 100), EXISTS (SELECT 1 FROM stats WHERE clicks > 1);

CREATE VIEW campaign_clicks AS SELECT campaign_id, sum(clicks) AS clicks FROM stats GROUP BY campaign_id;

-- query
SELECT * FROM campaign_clicks ORDER BY campaign_id;

-- query
SELECT v.campaign_id, v.clicks, c.name FROM campaign_clicks AS v INNER JOIN campaigns AS c ON v.campaign_id = c.campaign_id ORDER BY v.campaign_id;

-- query
SELECT campaign_id FROM campaign_clicks WHERE clicks = 0;

-- not-implemented: EXCEPT / INTERSECT
SELECT campaign_id FROM campaigns EXCEPT SELECT campaign_id FROM stats;

-- not-implemented: EXCEPT / INTERSECT
SELECT campaign_id FROM campaigns INTERSECT SELECT campaign_id FROM stats;

-- not-implemented: window functions
SELECT campaign_id, row_number() OVER (ORDER BY campaign_id) FROM campaigns;

-- not-implemented: window functions
SELECT tenant_id, sum(clicks) OVER (PARTITION BY tenant_id) FROM stats;

-- not-implemented: WITH TOTALS
SELECT tenant_id, sum(clicks) FROM stats GROUP BY tenant_id WITH TOTALS;

-- not-implemented: ROLLUP / CUBE
SELECT tenant_id, campaign_id, sum(clicks) FROM stats GROUP BY ROLLUP(tenant_id, campaign_id);

-- not-implemented: ROLLUP / CUBE
SELECT tenant_id, sum(clicks) FROM stats GROUP BY tenant_id WITH CUBE;

-- not-implemented: parametric aggregates (quantile)
SELECT quantile(0.5)(clicks) FROM stats;

-- not-implemented: EXPLAIN
EXPLAIN SELECT * FROM stats;

-- unsupported: DISTINCT ON is not in the DESIGN §3 list
SELECT DISTINCT ON (tenant_id) tenant_id, campaign_id FROM stats ORDER BY tenant_id, campaign_id;
