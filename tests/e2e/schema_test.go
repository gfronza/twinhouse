//go:build e2e

package e2e

import (
	"strings"
	"testing"
)

// The neutral "campaign analytics" schema used across the suite.
//
//	ad_events          MergeTree: raw impressions/clicks/conversions
//	campaign_hourly    AggregatingMergeTree rollup fed by mv_campaign_hourly
//	campaign_daily     SummingMergeTree rollup fed by mv_campaign_daily
//	campaigns          ReplacingMergeTree(version): current campaign state
//	audience_ops       MergeTree: segment membership operations (latest-wins replay)
//	tenants            MergeTree: tenant dimension (JOIN target)
var schemaDDL = []string{
	`CREATE TABLE {db}.ad_events (
		tenant_id   String,
		event_time  DateTime,
		event_id    String,
		campaign_id String,
		user_id     String,
		event_type  LowCardinality(String),
		cost_micros UInt64,
		is_bot      UInt8,
		country     Nullable(String),
		client_ts   DateTime64(3)
	) ENGINE = MergeTree
	PARTITION BY toYYYYMM(event_time)
	ORDER BY (tenant_id, event_time, event_id)`,

	`CREATE TABLE {db}.campaign_hourly (
		tenant_id   String,
		campaign_id String,
		hour        DateTime,
		events      SimpleAggregateFunction(sum, UInt64),
		impressions SimpleAggregateFunction(sum, UInt64),
		clicks      SimpleAggregateFunction(sum, UInt64),
		cost        SimpleAggregateFunction(sum, UInt64),
		last_seen   SimpleAggregateFunction(max, DateTime),
		users       AggregateFunction(uniqExact, String)
	) ENGINE = AggregatingMergeTree
	ORDER BY (tenant_id, campaign_id, hour)`,

	`CREATE MATERIALIZED VIEW {db}.mv_campaign_hourly TO {db}.campaign_hourly AS
	SELECT
		tenant_id,
		campaign_id,
		toStartOfHour(event_time)          AS hour,
		count()                            AS events,
		countIf(event_type = 'impression') AS impressions,
		sum(event_type = 'click')          AS clicks,
		sum(cost_micros)                   AS cost,
		max(event_time)                    AS last_seen,
		uniqExactState(user_id)            AS users
	FROM {db}.ad_events
	GROUP BY tenant_id, campaign_id, hour`,

	`CREATE TABLE {db}.campaign_daily (
		tenant_id   String,
		day         Date,
		campaign_id String,
		clicks      UInt64,
		cost        UInt64
	) ENGINE = SummingMergeTree((clicks, cost))
	ORDER BY (tenant_id, day, campaign_id)`,

	`CREATE MATERIALIZED VIEW {db}.mv_campaign_daily TO {db}.campaign_daily AS
	SELECT tenant_id, toDate(event_time) AS day, campaign_id,
	       countIf(event_type = 'click') AS clicks, sum(cost_micros) AS cost
	FROM {db}.ad_events
	WHERE is_bot = 0
	GROUP BY tenant_id, day, campaign_id`,

	`CREATE TABLE {db}.campaigns (
		campaign_id String,
		tenant_id   String,
		name        String,
		status      LowCardinality(String),
		budget      UInt64,
		tags        Array(String),
		updated_at  DateTime,
		version     UInt64
	) ENGINE = ReplacingMergeTree(version)
	ORDER BY campaign_id`,

	`CREATE TABLE {db}.audience_ops (
		segment_id String,
		user_id    String,
		op         LowCardinality(String),
		delta      Int64,
		state      String,
		version    UInt64,
		ts         DateTime64(3)
	) ENGINE = MergeTree
	ORDER BY (segment_id, version, user_id)`,

	`CREATE TABLE {db}.tenants (
		tenant_id String,
		plan      LowCardinality(String),
		region    String
	) ENGINE = MergeTree
	ORDER BY tenant_id`,
}

// createSchema creates the campaign analytics schema in db.
func createSchema(t testing.TB, db string) {
	t.Helper()
	for _, ddl := range schemaDDL {
		mustExec(t, "", strings.ReplaceAll(ddl, "{db}", db))
	}
}
