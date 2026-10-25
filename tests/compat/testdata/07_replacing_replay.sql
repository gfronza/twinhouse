-- Workload shapes 3 and 5: ReplacingMergeTree(version) "current state" read
-- with FINAL + WHERE key = {p:String}, and a latest-wins replay with argMax /
-- countIf / sumIf over an append-only membership log.

CREATE TABLE campaign_state (
    campaign_id String,
    tenant_id String,
    status LowCardinality(String),
    budget_micros UInt64,
    owner Nullable(String),
    labels Array(String),
    version UInt64,
    updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree(version) ORDER BY campaign_id;

INSERT INTO campaign_state VALUES ('c1', 'acme', 'draft', 1000, NULL, [], 1, '2024-05-01 00:00:00.000');
INSERT INTO campaign_state VALUES ('c1', 'acme', 'active', 5000, 'ann', ['q2'], 3, '2024-05-01 00:00:03.000');
INSERT INTO campaign_state VALUES ('c1', 'acme', 'paused', 0, 'bob', ['stale'], 2, '2024-05-01 00:00:02.000');
INSERT INTO campaign_state VALUES ('c2', 'acme', 'draft', 10, 'cy', ['x', 'y'], 1, '2024-05-02 00:00:00.000'), ('c3', 'globex', 'active', 18446744073709551615, NULL, [], 7, '2024-05-03 00:00:00.000');
INSERT INTO campaign_state VALUES ('c3', 'globex', 'archived', 1, 'dee', [], 9, '2024-05-04 00:00:00.000');
INSERT INTO campaign_state VALUES ('c4', 'initech', 'active', 42, '', ['it''s'], 5, '2024-05-05 00:00:00.000');
INSERT INTO campaign_state VALUES ('c4', 'initech', 'ended', 43, 'eve', [], 6, '2024-05-05 00:00:01.000');

-- query: replacing_final_all
-- formats: TSVWithNamesAndTypes,JSONCompact,JSONEachRow,Native
SELECT * FROM campaign_state FINAL ORDER BY campaign_id;

-- query: replacing_final_by_key_param
-- param: p=c1
SELECT campaign_id, status, budget_micros, owner, labels, version FROM campaign_state FINAL WHERE campaign_id = {p:String};

-- query: replacing_final_missing_key
-- param: p=nope
SELECT campaign_id, status FROM campaign_state FINAL WHERE campaign_id = {p:String};

-- query: replacing_final_filter_non_key
SELECT campaign_id, status FROM campaign_state FINAL WHERE tenant_id = 'acme' ORDER BY campaign_id;

-- query: replacing_final_filter_on_replaced_value
-- 'paused'/'draft' rows of c1 are superseded: FINAL applies before WHERE.
SELECT campaign_id FROM campaign_state FINAL WHERE status = 'draft' ORDER BY campaign_id;

-- query: replacing_final_count
SELECT count() FROM campaign_state FINAL;

-- query: replacing_final_group
SELECT tenant_id, count(), sum(budget_micros) FROM campaign_state FINAL GROUP BY tenant_id ORDER BY tenant_id;

-- query: replacing_final_in
SELECT campaign_id, version FROM campaign_state FINAL WHERE campaign_id IN ('c1', 'c3', 'zz') ORDER BY campaign_id;

-- query: replacing_without_final_argmax_view
-- Latest-wins without relying on FINAL (works on both regardless of merges).
SELECT campaign_id, argMax(status, version), max(version) FROM campaign_state GROUP BY campaign_id ORDER BY campaign_id;

-- query: replacing_final_settings_clause
SELECT campaign_id, status FROM campaign_state FINAL WHERE campaign_id = 'c4' SETTINGS max_threads = 1;

-- Equal versions: the last inserted row wins.
CREATE TABLE kv_state (k String, v String, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY k;

INSERT INTO kv_state VALUES ('a', 'first', 1);
INSERT INTO kv_state VALUES ('a', 'second', 1);
INSERT INTO kv_state VALUES ('b', 'only', 0);

-- query: replacing_equal_version_last_wins
SELECT k, v, ver FROM kv_state FINAL ORDER BY k;

-- No version column: the last inserted row wins.
CREATE TABLE kv_nover (k String, v UInt8) ENGINE = ReplacingMergeTree ORDER BY k;

INSERT INTO kv_nover VALUES ('a', 1);
INSERT INTO kv_nover VALUES ('a', 2), ('b', 3);
INSERT INTO kv_nover VALUES ('b', 4);

-- query: replacing_no_version
SELECT k, v FROM kv_nover FINAL ORDER BY k;

-- Composite sorting key.
CREATE TABLE seat_state (tenant_id String, seat UInt32, holder String, version UInt64) ENGINE = ReplacingMergeTree(version) ORDER BY (tenant_id, seat);

INSERT INTO seat_state VALUES ('acme', 1, 'u1', 1), ('acme', 2, 'u2', 1), ('globex', 1, 'v1', 1);
INSERT INTO seat_state VALUES ('acme', 1, 'u9', 2), ('globex', 1, 'old', 0);

-- query: replacing_composite_key
SELECT tenant_id, seat, holder, version FROM seat_state FINAL ORDER BY tenant_id, seat;

-- query: replacing_composite_key_point
-- param: t=acme
-- param: s=1
SELECT holder FROM seat_state FINAL WHERE tenant_id = {t:String} AND seat = {s:UInt32};

-- Latest-wins replay over an append-only operation log.
CREATE TABLE audience_ops (
    audience_id String,
    member_id String,
    op LowCardinality(String),
    delta UInt32,
    state String,
    version UInt64,
    at DateTime
) ENGINE = MergeTree ORDER BY (audience_id, version);

INSERT INTO audience_ops VALUES
    ('aud1', 'm1', 'add', 1, 'active', 1, '2024-05-01 00:00:01'),
    ('aud1', 'm2', 'add', 1, 'active', 2, '2024-05-01 00:00:02'),
    ('aud1', 'm1', 'remove', 1, 'removed', 3, '2024-05-01 00:00:03'),
    ('aud1', 'm3', 'add', 2, 'active', 4, '2024-05-01 00:00:04'),
    ('aud2', 'm1', 'add', 5, 'active', 1, '2024-05-02 00:00:00'),
    ('aud2', 'm1', 'update', 0, 'vip', 2, '2024-05-02 00:00:01'),
    ('aud3', 'm9', 'remove', 1, 'removed', 1, '2024-05-03 00:00:00');

INSERT INTO audience_ops VALUES ('aud2', 'm4', 'add', 1, 'active', 3, '2024-05-02 00:00:05');

-- query: latest_wins_replay
SELECT
    audience_id,
    argMax(state, version) AS latest_state,
    max(version) AS last_version,
    countIf(op = 'add') AS adds,
    countIf(op = 'remove') AS removes,
    sumIf(delta, op = 'add') - sumIf(delta, op = 'remove') AS netcount
FROM audience_ops
GROUP BY audience_id
ORDER BY audience_id;

-- query: latest_wins_types
SELECT toTypeName(sumIf(delta, op = 'add') - sumIf(delta, op = 'remove')), toTypeName(countIf(op = 'add')), toTypeName(argMax(state, version)) FROM audience_ops;

-- query: latest_wins_per_member
SELECT audience_id, member_id, argMax(op, version) AS last_op, argMax(state, version) AS st
FROM audience_ops
GROUP BY audience_id, member_id
HAVING last_op != 'remove'
ORDER BY audience_id, member_id;

-- query: latest_wins_param_key
-- param: aud=aud1
SELECT argMax(state, version), sumIf(delta, op = 'add') - sumIf(delta, op = 'remove') AS netcount FROM audience_ops WHERE audience_id = {aud:String};

-- query: latest_wins_param_in_array
-- param: auds=['aud1','aud3']
SELECT audience_id, argMax(member_id, version) FROM audience_ops WHERE audience_id IN {auds:Array(String)} GROUP BY audience_id ORDER BY audience_id;

-- query: argmin_argmax_tuple
SELECT audience_id, argMin(member_id, version), argMax((member_id, op), version) FROM audience_ops GROUP BY audience_id ORDER BY audience_id;
