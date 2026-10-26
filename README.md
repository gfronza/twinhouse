# twinhouse

A ClickHouse-compatible HTTP server that stores its data in Amazon DynamoDB,
so a ClickHouse-style workload can run **multi-region active-active**.

ClickHouse Cloud has no multi-region active-active mode. DynamoDB global
tables do. This server speaks ClickHouse's HTTP protocol, so an application
that talks to ClickHouse through clickhouse-go v2 (HTTP protocol), curl or
any other HTTP client can switch to it by changing host and port. The
workload it targets is append-only events (`MergeTree`), latest-state
snapshots (`ReplacingMergeTree`) and materialized-view rollups
(`AggregatingMergeTree` / `SummingMergeTree`). It runs in every region,
writes are accepted everywhere, and all regions converge to the same data.

> **Status: experimental.** The design is stable and well tested against
> emulated global tables, but it has no production track record yet. The
> `GLOBAL_MODE=aws` deployment path (real DynamoDB global tables), the
> serverless deployment ([deploy/aws-lambda](deploy/aws-lambda/README.md),
> Lambda behind a private API Gateway) and the container deployment
> ([deploy/aws-ecs](deploy/aws-ecs/README.md), ECS Fargate behind an
> internal load balancer) are implemented and unit-tested; the API Lambda
> passes the e2e suite under `sam local` and the ECS task definitions pass
> it as local containers, but the automated suites have not run against
> real AWS. Expect rough edges, and benchmark costs
> before relying on it.

---

## Contents

- [What is verified](#what-is-verified)
- [Architecture](#architecture)
- [Consistency model](#consistency-model)
- [Quickstart](#quickstart)
- [SQL support](#sql-support)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Limitations and roadmap](#limitations-and-roadmap)
- [License](#license)

---

## What is verified

Three black-box test suites run against a local Docker stack (two DynamoDB
Local instances standing in for two AWS regions), on top of the unit and
DynamoDB integration tests:

| Suite | What it checks |
|---|---|
| `tests/e2e` | An application's view of the server: clickhouse-go v2 over HTTP (native API and `database/sql`) and raw HTTP. Covers DDL, parameterised inserts, MV rollups, `FINAL`, key-range probes, latest-wins replays, bulk `JSONEachRow`, formats and error codes. |
| `tests/compat` | **Differential testing against real ClickHouse 25.8.** Over 20 scenario files (`tests/compat/testdata/*.sql`, several hundred statements) run against both servers. Results must match in several output formats (TSV with names and types, JSONCompact, and Native decoded by clickhouse-go). Expected failures must match too: the same ClickHouse error code and HTTP status. Each statement carries a directive (`-- query`, `-- error`, `-- not-implemented`, `-- unsupported`, `-- known-deviation`). A deviation has to be declared and documented; nothing differs silently. |
| `tests/multiregion` | Active-active behaviour on two regions: DDL and DROP propagation, table re-creation, write in one region and read in the other, concurrent writers in both regions on the same keys, exactly-once rollups while workers and the replicator are killed and restarted, region outage and catch-up, replication lag, clickhouse-go parity across regions. |

All three suites, `go vet` and `go test ./...` currently pass.

---

## Architecture

```
          clickhouse-go (HTTP) / curl / any HTTP client
                 │                                   │
     ┌───────────▼────────────┐          ┌───────────▼────────────┐
     │ chserver   (region A)  │          │ chserver   (region B)  │
     │ HTTP → SQL → storage   │          │ HTTP → SQL → storage   │
     │ (inline MV application)│          │ (inline MV application)│
     └───────────┬────────────┘          └───────────┬────────────┘
                 │ Put/Update/Query/Scan             │
     ┌───────────▼────────────┐          ┌───────────▼────────────┐
     │ DynamoDB (region A)    │          │ DynamoDB (region B)    │
     │                        │ global   │                        │
     │  source tables ◄───────┼──tables──┼──────► source tables   │
     │  (MergeTree, Replacing)│    or    │  (MergeTree, Replacing)│
     │  __catalog     ◄───────┼replicator┼──────► __catalog       │
     │                        │          │                        │
     │  rollups (regional)    │          │  rollups (regional)    │
     │  __mv_markers          │          │  __mv_markers          │
     │  __checkpoints         │          │  __checkpoints         │
     └───────────┬────────────┘          └───────────┬────────────┘
                 │ DynamoDB Streams                  │ DynamoDB Streams
                 │ (local + replicated rows)         │
     ┌───────────▼────────────┐          ┌───────────▼────────────┐
     │ mvworker   (region A)  │          │ mvworker   (region B)  │
     │ applies MVs to A's     │          │ applies MVs to B's     │
     │ rollups, exactly once  │          │ rollups, exactly once  │
     └────────────────────────┘          └────────────────────────┘
```

- **Every ClickHouse table is a DynamoDB table** named
  `{prefix}{db}.{table}.{id}` (`{id}` is a per-incarnation suffix, so a
  dropped and re-created table gets a fresh physical table). The partition
  key `_pk` is an order-preserving encoding of the partition-key columns
  (by default the first `ORDER BY` column). The sort key `_sk` encodes the
  remaining `ORDER BY` columns. For `MergeTree` it also carries a 16-byte
  uniquifier, so rows never collide. `WHERE` predicates on the sorting key
  become DynamoDB `Query` key conditions. Anything else becomes a parallel
  `Scan`.
- **Source tables are global, rollups are regional.** Global tables resolve
  concurrent writes to one item with last-writer-wins, which would lose
  counter increments made in two regions at once. So `MergeTree` and
  `ReplacingMergeTree` tables (immutable rows and whole-row snapshots) are
  replicated. `AggregatingMergeTree` and `SummingMergeTree` tables (the
  targets of materialized views) exist separately in every region. Each
  region rebuilds its own rollups from its own replica's stream, which
  carries both local and replicated rows.
- **Materialized views run twice and apply once.** The writing server
  applies views inline during `INSERT`, which gives read-your-writes in
  that region. The region's `mvworker` later sees the same rows on the
  stream. Every application is guarded by a marker item, written in the
  same `TransactWriteItems` as the rollup row's update (an exact `ADD` for
  integers, a compare-and-swap in IEEE arithmetic for float sums). A row is
  therefore counted exactly once per region, whichever path gets there
  first and however often a stream batch is redelivered. Rows of one
  rollup key in one batch are pre-aggregated: one update per 99 markers.
  `min`/`max`/`argMin`/`argMax` use one idempotent conditional update per
  column. `uniqExact`/`uniq`/`groupUniqArray` keep their set members in a
  side table, added in transactions of up to 50 new members.
- **Global tables come from AWS, or from the replicator.** With
  `GLOBAL_MODE=aws`, source tables are real DynamoDB global tables. On a
  laptop (`GLOBAL_MODE=emulated`), `cmd/replicator` tails each region's
  streams and copies rows to the other regions with the same
  last-writer-wins rule (`_ts`, then `_origin`). In AWS mode the replicator
  still has one job: it copies `ReplacingMergeTree` tables, which need the
  highest *version* to win (see below).
- **The schema is replicated too.** DDL is stored in a global `__catalog`
  table. Every server refreshes it and runs a reconciler that creates or
  drops this region's physical tables to match it.
- **The same code runs serverless.** On AWS the three roles above are
  Lambda functions: the HTTP API behind a private API Gateway, the stream
  worker and the replicator on DynamoDB-stream event-source mappings, and
  the reconciler on the `__catalog` stream plus a one-minute schedule
  (see [Deployment](#deployment)). The local stack and the ECS
  deployment run the long-running binaries in containers.

[`DESIGN.md`](DESIGN.md) is the detailed contract: physical layout,
encodings, write conditions, reset epochs, stream handling and error
classification.

---

## Consistency model

**In the region you write to**, behaviour is that of a single ClickHouse
node. After an `INSERT` returns, a `SELECT` in the same region sees the
rows, including their effect on materialized-view rollups. Reads are
strongly consistent by default (`CONSISTENT_READS=true`).

**Across regions**, every region converges to the same data. Source rows
appear in the other regions after replication (typically about a second
on AWS). Rollups there catch up once that region's `mvworker` has
processed the replicated rows.

- **Rollups are exactly-once.** Every source row is counted exactly once in
  every region's rollups, through stream redelivery, worker crashes,
  restarts, inline-plus-stream double application and replicator restarts.
  The rollups never conflict across regions because each region writes
  only its own copy.
- **`ReplacingMergeTree` is latest-wins.** Reads always behave like
  `FINAL`. With a version column, the highest version wins in every region
  regardless of write order. Write time (`_ts`, then the origin region)
  only breaks ties. Without a version column, the latest write wins. A
  write that is already older than the stored version is not stored, as in
  ClickHouse after a merge.
- **Conflicting writes** to the same `ReplacingMergeTree` key in two
  regions at once resolve to the same winner everywhere. Both versions
  still feed the materialized views of every region: the replicator
  applies the target region's views to a replicated version that region
  rejected, so rollups over a Replacing source converge too. One case
  is special in multi-region mode: a write that is *already* superseded
  when it arrives is never stored and never replicates, so it feeds no
  view anywhere. A single-region deployment feeds it to views, as
  ClickHouse does.
- **`MergeTree` rows never conflict.** Every row has a unique key.
- **DDL takes a moment to reach other regions.** That is catalog
  replication plus one reconcile round, a few seconds locally. A statement
  in region B right after `CREATE TABLE` in region A can fail with
  `UNKNOWN_TABLE`, so retry it. Concurrent DDL on the *same* object in two
  regions resolves last-writer-wins, and one of the two changes is lost.
  Run schema changes for an object from one region at a time.
- **Mutations of rollups.** `TRUNCATE` of a rollup is recorded in the
  replicated catalog and every region resets its own copy. Rows written
  within a moment of the `TRUNCATE` in another region may be missing from
  that region's copy. `ALTER TABLE … DELETE` / `DELETE FROM` on a rollup is
  rejected in multi-region mode, because nothing would carry it to the
  other regions. On source tables, deletes replicate.
- **Clock skew.** Some boundaries are decided by write time: whether a row
  predates a view's creation, and whether it predates a `TRUNCATE`. These
  boundaries are as approximate as the skew between the writers' clocks.
- **`INSERT` atomicity.** An `INSERT` is atomic per block (about 1M rows
  or 256 MiB, `min_insert_block_size_rows` / `_bytes`) for parse,
  conversion and `DEFAULT` errors: a bad row fails its block before any of
  it is written. A DynamoDB failure in the middle of a block can leave part
  of the block written. With `STREAM_VIEWS=true` (the default) a failed
  inline view application does not fail the `INSERT`: the source rows are
  stored and the rollups catch up through the stream worker.

---

## Quickstart

**Prerequisites:** Docker with Compose v2, Go **1.27** or newer (see
`go.mod`, only needed for the tests), `make` and `curl`.

```sh
git clone <this repository> && cd twinhouse
make up        # two "regions": us on :8123, eu on :8124
```

`make up` builds one image and starts the local multi-region stack
(`deploy/local/docker-compose.yml`). The stack has two DynamoDB Local instances
(us-east-1 and eu-west-1), a `chserver` and an `mvworker` per region, and
the replicator emulating global tables between them. `make up-single`
starts a single region without replication on `:8125`. `make down` removes
everything.

### Create a source table, a rollup and a materialized view (in us)

```sh
US=http://localhost:8123 EU=http://localhost:8124

curl -sS $US --data-binary "CREATE DATABASE demo"

curl -sS $US --data-binary "
CREATE TABLE demo.page_views (
    ts      DateTime64(3),
    site    LowCardinality(String),
    path    String,
    user_id UInt64
) ENGINE = MergeTree ORDER BY (site, ts)"

curl -sS $US --data-binary "
CREATE TABLE demo.daily_site_stats (
    day   Date,
    site  String,
    views SimpleAggregateFunction(sum, UInt64),
    users AggregateFunction(uniqExact, UInt64)
) ENGINE = AggregatingMergeTree ORDER BY (site, day)"

curl -sS $US --data-binary "
CREATE MATERIALIZED VIEW demo.daily_site_stats_mv TO demo.daily_site_stats AS
SELECT toDate(ts) AS day, site, count() AS views, uniqExactState(user_id) AS users
FROM demo.page_views GROUP BY day, site"
```

### Write in us, read the rollup immediately

```sh
curl -sS $US --data-binary "
INSERT INTO demo.page_views VALUES
  ('2026-10-01 10:00:00.000', 'example.org', '/',     1),
  ('2026-10-01 10:05:00.000', 'example.org', '/docs', 2),
  ('2026-10-01 11:00:00.000', 'example.org', '/',     1)"

curl -sS $US --data-binary "
SELECT day, site, sum(views) AS views, uniqExactMerge(users) AS users
FROM demo.daily_site_stats GROUP BY day, site ORDER BY day, site
FORMAT PrettyCompact"
```

```
   ┌────────day─┬─site────────┬─views─┬─users─┐
1. │ 2026-10-01 │ example.org │     3 │     2 │
   └────────────┴─────────────┴───────┴───────┘
```

### Read it in eu, write in eu, read it back in us

The schema, the source rows and (through eu's own `mvworker`) eu's rollup
arrive within a few seconds:

```sh
sleep 3
curl -sS $EU --data-binary "SELECT sum(views), uniqExactMerge(users) FROM demo.daily_site_stats"
# 3	2

curl -sS "$EU/?query=INSERT%20INTO%20demo.page_views%20FORMAT%20JSONEachRow" --data-binary \
  '{"ts":"2026-10-01 12:00:00.000","site":"example.org","path":"/blog","user_id":3}'

sleep 3
curl -sS $US --data-binary "SELECT sum(views), uniqExactMerge(users) FROM demo.daily_site_stats"
# 4	3
```

### Latest-wins snapshots

```sh
curl -sS $US --data-binary "
CREATE TABLE demo.users (user_id UInt64, plan String, updated_at DateTime64(3))
ENGINE = ReplacingMergeTree(updated_at) ORDER BY user_id"
sleep 3   # let the DDL reach eu

curl -sS $US --data-binary "INSERT INTO demo.users VALUES (1, 'free',  '2026-10-01 10:00:00')"
curl -sS $EU --data-binary "INSERT INTO demo.users VALUES (1, 'pro',   '2026-10-01 11:00:00')"
curl -sS $US --data-binary "INSERT INTO demo.users VALUES (1, 'stale', '2026-10-01 09:00:00')"

sleep 3
curl -sS $US --data-binary "SELECT * FROM demo.users"   # 1	pro	2026-10-01 11:00:00.000
curl -sS $EU --data-binary "SELECT * FROM demo.users"   # 1	pro	2026-10-01 11:00:00.000
```

Query parameters, URL settings, `default_format`, sessions, HTTP Basic or
`X-ClickHouse-User`/`X-ClickHouse-Key` auth and gzip/zstd/lz4/br
compression work as in ClickHouse:

```sh
curl -sS "$US/?param_site=example.org" \
  --data-binary "SELECT count() FROM demo.page_views WHERE site = {site:String}"
```

### From Go (clickhouse-go v2)

Use the **HTTP** protocol. The native TCP protocol (port 9000) is not
implemented.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{"localhost:8123"}, // the region's chserver
		Auth:     clickhouse.Auth{Database: "demo", Username: "default"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO page_views")
	if err != nil {
		log.Fatal(err)
	}
	if err := batch.Append(time.Now(), "example.org", "/pricing", uint64(42)); err != nil {
		log.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		log.Fatal(err)
	}

	rows, err := conn.Query(ctx, `
		SELECT day, sum(views), uniqExactMerge(users)
		FROM daily_site_stats WHERE site = {site:String}
		GROUP BY day ORDER BY day`,
		clickhouse.Named("site", "example.org"))
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var day time.Time
		var views, users uint64
		if err := rows.Scan(&day, &views, &users); err != nil {
			log.Fatal(err)
		}
		fmt.Println(day.Format("2006-01-02"), views, users)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
```

`database/sql` (`clickhouse.OpenDB`) works the same way.

---

## SQL support

This is a summary. The full contract, with error codes, is in
[DESIGN.md §2–§3](DESIGN.md#3-sql-surface-internalsqlparse-internalexec).
Anything outside it fails with `NOT_IMPLEMENTED` (48) or
`UNKNOWN_FUNCTION` (46). It never returns a wrong result.

**Engines.** `MergeTree`, `ReplacingMergeTree([ver[, is_deleted]])`,
`AggregatingMergeTree`, `SummingMergeTree([cols])`. `Memory` and `Log` are
accepted and stored like `MergeTree`, which means they are persistent.
`ORDER BY` (including `tuple()`) defines the DynamoDB key.
`SETTINGS dynamodb_partition_key = 'a, b'` changes how many leading
columns form the partition key. `SETTINGS dynamodb_replication =
'global' | 'regional'` overrides the default replication.

**DDL.** `CREATE DATABASE`, `CREATE [OR REPLACE] TABLE [IF NOT EXISTS]`
(also `AS other_table` and `AS SELECT`), `CREATE MATERIALIZED VIEW … TO
target AS SELECT` (with `POPULATE`, except into a rollup in multi-region
mode), `CREATE VIEW`, `DROP TABLE|VIEW|DATABASE`, `TRUNCATE`,
`ALTER TABLE ADD|DROP|MODIFY COLUMN`, `ALTER TABLE … DELETE`,
`DELETE FROM`, `SHOW DATABASES|TABLES|CREATE TABLE`, `DESCRIBE`, `EXISTS`,
`USE`, `SET`. `OPTIMIZE` and `SYSTEM …` are accepted as no-ops. Column
`DEFAULT`/`MATERIALIZED`/`ALIAS` work. `PARTITION BY`, `PRIMARY KEY`,
`CODEC`, `COMMENT` and table `TTL` are accepted with no storage effect.

**Queries.** `WITH`, `DISTINCT`, `FROM` table / subquery / `numbers(N)` /
`system.{one,numbers,databases,tables,columns}`, `FINAL`, `WHERE`,
`PREWHERE`, `GROUP BY`, `HAVING`, `ORDER BY … NULLS FIRST|LAST`,
`LIMIT … OFFSET`, `LIMIT n BY`, `UNION ALL` (and `UNION` per
`union_default_mode`), scalar and `IN` subqueries, `ARRAY JOIN`, hash
`JOIN`s (INNER/LEFT/RIGHT/FULL/CROSS, `ANY`, LEFT `SEMI`/`ANTI`),
`{name:Type}` query parameters (also `Identifier`), `SETTINGS`, `FORMAT`.

**Types.** `(U)Int8–64`, `Float32/64`, `Bool`, `String`, `FixedString`,
`UUID`, `Enum8/16`, `Date`, `Date32`, `DateTime[(tz)]`,
`DateTime64(p[, tz])`, `Nullable`, `LowCardinality`, `Array`, `Tuple`
(named or not), `Map`, `AggregateFunction`, `SimpleAggregateFunction`.

**Functions.** Comparison, logical, arithmetic and conditional functions
(`if`, `multiIf`, `coalesce`, …). Type conversions, including the
`-OrZero`/`-OrNull` variants and `CAST`. Date/time functions (`toDate*`,
`toStartOf*`, `toYYYYMM*`, `dateDiff`, `formatDateTime`,
`parseDateTimeBestEffort*`, `INTERVAL` arithmetic, …). Strings (`concat`,
`substring*`, `lower`/`upper`, `like`/`ilike`, `match`, `replace*`,
`splitBy*`, `trim*`, `position`, …). Arrays (`has`, `indexOf`,
`arraySort`, `arraySlice`, `arrayDistinct`, `arraySum`, `arrayJoin`, …),
maps (`map`, `mapKeys`, `mapValues`, `mapContains`), tuples and
introspection (`version`, `timezone`, `currentDatabase`, `toTypeName`, …).

**Aggregates.** `count`, `sum`, `avg`, `min`, `max`, `any`, `anyLast`,
`argMin`, `argMax`, `uniq`, `uniqExact`, `groupArray`, `groupUniqArray`,
`count(DISTINCT …)`. Combinators `-If`, `-State`, `-Merge`, `-MergeState`
and `finalizeAggregation` are supported. Result types follow ClickHouse.

**Formats.** Output: Native, TabSeparated (and its `Raw`/`WithNames`/
`WithNamesAndTypes` variants), CSV (and variants), JSON, JSONCompact,
JSONStrings, JSONCompactStrings, JSONEachRow, JSONCompactEachRow,
JSONStringsEachRow, JSONCompactStringsEachRow, Values, Pretty,
PrettyCompact, Null. Input: Native, the TabSeparated and CSV variants,
JSONEachRow, JSONCompactEachRow, Values. Common `output_format_*`,
`input_format_*`, `format_csv_*`, `date_time_input_format` and
`date_time_output_format` settings are honoured.

**HTTP protocol.** `/`, `/ping` and `/replicas_status`. GET is read-only.
The query goes in the URL or the body, with INSERT data in the body.
Sessions (`session_id`; not in the serverless deployment, which rejects
them with `NOT_IMPLEMENTED`), `max_execution_time`, ClickHouse response
headers (`X-ClickHouse-Summary`, …), and ClickHouse's error body and HTTP
status for every error code are supported. Request and response
compression works, including ClickHouse's native `compress=1`/`decompress=1`
framing.

### Not supported

- The native TCP protocol and the MySQL/PostgreSQL/gRPC interfaces (HTTP
  only). The server itself has no TLS: terminate it at a load balancer
  (the serverless deployment gets it from API Gateway, the ECS deployment
  from its load balancer's optional HTTPS listener).
- `Decimal`, `Int128/256`, `UInt128/256`, `IPv4/6`, `JSON`, `Variant`,
  `Dynamic`, `Nested`, geo types.
- Window functions, lambdas and higher-order functions (`arrayMap`,
  `arrayFilter`, …), parametric aggregates (`quantile*`, `topK`, …) and
  other aggregates (`stddev*`, `var*`, `uniqCombined`, `sumMap`, …).
- `EXCEPT`/`INTERSECT`, `GROUP BY ROLLUP/CUBE`, `WITH TOTALS`, `EXPLAIN`,
  `SAMPLE`, `ORDER BY … WITH FILL`, `LIMIT … WITH TIES`, `DISTINCT ON`,
  `EXISTS (subquery)`, `ASOF JOIN`.
- `ALTER TABLE … UPDATE`, `RENAME`, dictionaries, table functions other
  than `numbers`, external tables, `INSERT INTO FUNCTION`,
  `INTO OUTFILE`.
- Materialized views without `TO`, refreshable views, cascading views (MV
  output does not trigger other MVs), `JOIN` or `UNION` inside a view's
  `SELECT`, and views that read from an Aggregating/Summing table.
- Output and input formats outside the list above (RowBinary, Parquet,
  Arrow, ORC, Avro, Protobuf, …).
- Most `system.*` tables.
- Settings that would change results but are not implemented
  (`join_use_nulls=1`, `transform_null_in`, …). These are rejected, not
  ignored.

### Deliberate deviations from ClickHouse

- **Everything is always merged.** Reads of a `ReplacingMergeTree` table
  behave like `FINAL`. Summing and Aggregating tables never show unmerged
  rows. `OPTIMIZE` has nothing to do.
- **Views are evaluated per inserted row**, not per inserted block. For
  the usual `GROUP BY` rollups the result is identical. A view whose
  output column feeding the target's *sorting key* depends on `now()`,
  `rand()`, `generateUUIDv4()` and similar is rejected, because its output
  must be a function of the source row.
- **Floats in sets.** `GROUP BY`, `DISTINCT`, `IN` and `uniq*` treat `-0`
  as `0` and all NaNs as equal. ClickHouse compares bit patterns.
- **Range checks.** A JSON number outside an integer column's range, and
  out-of-range `DateTime` text, are rejected instead of wrapped. `Date32`
  and `DateTime64` stop at 2262-04-11 (int64 nanoseconds).
- **Pretty formats are best-effort.** They need not match ClickHouse byte
  for byte.
- **`POPULATE` is restricted.** In multi-region mode it is rejected for a
  regional (rollup) target, because it would back-fill only one region. Back-fill
  with `INSERT INTO target SELECT …` in every region instead.
- **`session_timezone` must be a URL setting.** In a query's `SETTINGS`
  clause it is rejected.

---

## Configuration

Every binary reads environment variables (`internal/config`; durations
use Go syntax such as `500ms` or `2s`). The deployment templates set them
from stack parameters; [Self-managed containers](#self-managed-containers)
lists the ones each process needs.

### Common (every binary, the Lambda functions included)

| Variable | Default | Meaning |
|---|---|---|
| `REGION` | `AWS_REGION`, then `AWS_DEFAULT_REGION`, then `local` | This process's region. It must be a real AWS region unless `DYNAMODB_ENDPOINT` is set. Lambda and ECS inject `AWS_REGION`. |
| `DYNAMODB_ENDPOINT` | *(AWS)* | Custom endpoint, e.g. DynamoDB Local. Dummy credentials are used when it is set. |
| `TABLE_PREFIX` | `twinhouse_` | Prefix of every physical DynamoDB table. |
| `GLOBAL_MODE` | `none` | `none` for a single region, `aws` for real global tables, `emulated` for local copies kept in sync by `cmd/replicator`. |
| `REPLICA_REGIONS` | | Comma-separated list of the other regions (required for `aws`; this region itself is dropped from the list). |
| `GLOBAL_HOME_REGION` | first of sorted `REGION` + `REPLICA_REGIONS` | (`aws`) The region that creates `__catalog` and adds its replicas. |
| `CATALOG_REFRESH` | `2s` | Catalog reload / reconcile / table discovery interval of the long-running binaries; in the Lambdas, the maximum snapshot age before a request or invocation reloads the catalog. |
| `CONSISTENT_READS` | `true` | Strongly consistent reads. |
| `STREAM_POLL_INTERVAL` | `250ms` | Idle poll interval of stream readers. |
| `DDB_HTTP_TIMEOUT` | `30s` | Per-attempt response-header timeout of AWS SDK calls (`0` disables). |
| `TZ_NAME` | `UTC` | Server time zone (`timezone()`, DateTime rendering). |
| `SERVER_VERSION` | `25.8.1.1` | Version reported to clients. |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` (JSON logs on stderr). |

### chserver

| Variable | Default | Meaning |
|---|---|---|
| `LISTEN_ADDR` | `:8123` | HTTP listen address. |
| `CH_USERS` | `default:` | `user:password,user2:password2` (an empty password is allowed). |
| `INLINE_VIEWS` | `true` | Apply materialized views during `INSERT` (read-your-writes). |
| `STREAM_VIEWS` | `true` | A stream worker applies the views (mvworker, Lambda or `RUN_STREAM_WORKER`, one per region). An inline view application that fails after the source rows are stored is then logged and the `INSERT` succeeds; the worker applies it. Set `false` for a deployment without any worker: such an `INSERT` then fails (the source rows stay written). |
| `RUN_STREAM_WORKER` | `false` | Also run the materialized-view stream worker in-process. |
| `MAX_EXECUTION_TIME` | `0` | Default `max_execution_time` (seconds or a duration; `0` = unlimited). Queries may override it. |
| `DISPLAY_NAME` | hostname`-<REGION>` | `displayName()` and `X-ClickHouse-Server-Display-Name`. |

`cmd/chserver-lambda` (the API as a Lambda function behind API Gateway,
[deploy/aws-lambda](deploy/aws-lambda/README.md)) reads the same variables except
`LISTEN_ADDR` and `RUN_STREAM_WORKER`; it rejects HTTP sessions
(`session_id`) with `NOT_IMPLEMENTED` (48), reloads the catalog at the
start of a request when the snapshot is older than `CATALOG_REFRESH`, and
is bound by the Lambda payload limit (6 MB per request and response).

### reconciler (chserver, reconciler-lambda)

The reconciler creates and drops this region's physical tables from the
catalog; it runs inside `chserver` and, in the serverless deployment, as
`cmd/reconciler-lambda` (invoked by the `{prefix}__catalog` stream and by
an EventBridge schedule). With `MVWORKER_FUNCTION_ARN` set it also manages
the Lambda event-source mappings of the table streams
([DESIGN.md §5](DESIGN.md)).

| Variable | Default | Meaning |
|---|---|---|
| `MVWORKER_FUNCTION_ARN` | | Lambda function (ARN or name) wired to the stream of every source table (MergeTree, ReplacingMergeTree). Empty disables event-source-mapping management (local stack, `sam local`). Rejected together with `DYNAMODB_ENDPOINT`. |
| `REPLICATOR_FUNCTION_ARN` | | (`GLOBAL_MODE=aws`) Lambda wired to the streams of ReplacingMergeTree tables. Needs `MVWORKER_FUNCTION_ARN`. |
| `RECONCILER_FUNCTION_ARN` | `AWS_LAMBDA_FUNCTION_NAME` | The function the `{prefix}__catalog` stream is wired to (the reconciler itself). |
| `ESM_BATCH_SIZE` | `100` | Batch size of the event-source mappings it creates (`TRIM_HORIZON`, `ReportBatchItemFailures`, unlimited retries). |
| `RECONCILER_DEADLINE_MARGIN` | `5s` | (reconciler-lambda) Time kept free before the invocation deadline. |

### mvworker (and chserver's in-process worker where noted)

| Variable | Default | Meaning |
|---|---|---|
| `MVWORKER_OWNER` | hostname:pid | Shard-lease owner id. |
| `STREAM_LEASE_DURATION` | `30s` | Shard lease TTL. Several workers per region share shards safely. |
| `STATS_INTERVAL` | `30s` | Progress log interval (`0` disables). Logs at ERROR when a shard has been stuck for over 1 h. |
| `MV_POISON_POLICY` | `deadletter` | A record whose view application keeps failing with a deterministic error is written to `{prefix}__dead_letters` and skipped (`deadletter`), or retried forever (`block`). Also used by the in-process worker and the replicator. |
| `MV_POISON_AFTER` | `30s` | How long such a failure must persist before it is dead-lettered. |
| `CATALOG_MIN_RELOAD` | `1s` | Minimum spacing of catalog reloads triggered by new rows. |
| `AWS_LAMBDA_RUNTIME_API` / `-lambda` | | Run as an AWS Lambda DynamoDB-stream handler. |

### replicator

| Variable | Default | Meaning |
|---|---|---|
| `REGIONS` | *(required)* | `name=endpoint,…` with at least two entries. An empty endpoint means the real AWS region (`us-east-1=,eu-west-1=`). |
| `GLOBAL_MODE` | `none` (behaves as `emulated`) | `emulated`: copy the catalog and every global table. `aws`: copy only the `ReplacingMergeTree` tables, because AWS replicates the rest. |
| `APPLY_CONFLICT_VIEWS` | `true` | Apply the target region's views to replicated versions it rejects as superseded, so rollups converge. |
| `REPLICATION_DELAY` | `0` | Artificial lag, useful for demos and tests. |
| `STREAM_LEASE_DURATION` | `30s` | Shard lease TTL. |
| `STREAM_DISCOVERY_INTERVAL` | `5s` (minimum) | How often each pipe re-describes its stream. |
| `REPLICATOR_OWNER` | hostname:pid | Shard-lease owner id. |
| `STATS_INTERVAL` | `30s` | Progress log interval. |
| `AWS_LAMBDA_RUNTIME_API` / `-lambda` | | Run as an AWS Lambda DynamoDB-stream handler (one function per region, `GLOBAL_MODE=aws`): it replicates from `REGION` to `REPLICA_REGIONS` and ignores `REGIONS`, `REPLICATION_DELAY`, the leases and the stats log. |

---

## Deployment

Every deployment runs the same binaries (`cmd/*`, or the image built from
`deploy/Dockerfile`) against the same DynamoDB layout, configured with the
environment variables of [Configuration](#configuration). The options
differ in where the processes run and what starts them.

> **Not run on real AWS yet.** The automated suites have not run against
> real AWS. The API Lambda passes the e2e suite under `sam local`, and the
> ECS task definitions pass it as local containers (`make ecs-local`), both
> against DynamoDB Local. Read [DESIGN.md §1, §2, §5, §6](DESIGN.md) before
> you deploy.

### Options

| Option | Directory | What runs | Use it for |
|---|---|---|---|
| Local stack | [`deploy/local`](deploy/local/docker-compose.yml) | Docker Compose: two DynamoDB Local instances as two regions, `chserver` and `mvworker` per region, the replicator emulating global tables (`GLOBAL_MODE=emulated`). | Development, the test suites, evaluation and demos. Not for production data. |
| AWS serverless | [`deploy/aws-lambda`](deploy/aws-lambda/README.md) | One AWS SAM stack per region: the API as a Lambda behind a private REST API Gateway, the stream worker and the replicator as Lambdas on DynamoDB-stream event-source mappings, and a reconciler Lambda that manages tables and mappings. | Low or spiky traffic, nothing to keep running, requests and responses under 6 MB, queries under about 25 s. |
| AWS containers | [`deploy/aws-ecs`](deploy/aws-ecs/README.md) | One CloudFormation stack per region on ECS Fargate in an existing VPC: `chserver` behind an internal Application Load Balancer on port 8123, an `mvworker` service (or the worker in-process), the replicator in the home region. | Steady traffic, large `INSERT`s and results, queries up to about 100 s, HTTP sessions, clients that switch by host and port only. |
| Self-managed containers | [`deploy/Dockerfile`](deploy/Dockerfile) | The same image and environment variables on Kubernetes, EC2 or any other scheduler, with your own load balancer, secrets and IAM. | A platform you already operate, or load balancer limits you need to control. |
| Hybrids | none | Combinations the binaries support but no template deploys, such as the container API with Lambda stream consumers. | See [Hybrid deployments](#hybrid-deployments). |

A short decision guide:

1. Not on AWS yet, or developing: use the **local stack**. It needs only
   Docker and holds its data in memory.
2. On AWS, if every request and response fits in 6 MB, every query
   finishes in about 25 s and no client needs `session_id`: use
   **serverless**. It has the lowest idle cost and no servers to size.
3. On AWS otherwise, or if clients must reach the server by host and port
   like a ClickHouse server: use **ECS containers**.
4. With an existing Kubernetes cluster or VM fleet: use **self-managed
   containers**, with `deploy/aws-ecs` as the worked example.

All options use the same tables, so the choice is per region and can be
changed later (see [Moving between options](#moving-between-options)).

### Comparison

| | Serverless (`deploy/aws-lambda`) | Containers (`deploy/aws-ecs`, or self-managed) |
|---|---|---|
| How clients connect | HTTPS on 443 from inside the VPC, through the `execute-api` interface endpoint: `https://<api-id>-<vpce-id>.execute-api.<region>.amazonaws.com/v1` (`ApiVpceEndpoint` output), or the plain invoke URL when the endpoint has private DNS. The stage is a path prefix, so clickhouse-go needs `HttpUrlPath: "v1"`; a private custom domain mapped with an empty base path removes it. Go clients must send `Accept: */*`. | `http://<internal-alb-dns>:8123` (ECS `ClickHouseUrl` output): a ClickHouse client switches by host and port only. HTTPS-only ECS stacks (`CertificateArn` set, `KeepHttpListener=false`) have no 8123 listener and no `ClickHouseUrl` output: clients use `https://<alias the certificate covers>:8443`. Self-managed: the address of your load balancer. |
| TLS termination | Always, at API Gateway. | ECS: optional HTTPS listener on 8443 with an ACM certificate (`CertificateArn`); plain HTTP on 8123 inside the VPC unless `KeepHttpListener=false`. Self-managed: your load balancer or proxy. chserver itself has no TLS. |
| HTTP sessions (`session_id`) | Rejected with `NOT_IMPLEMENTED` (48). | Supported. A session lives in the memory of one process, so with several instances every request of a session must reach the same one. |
| Request size | 6 MB per request (Lambda invocation payload). Request bodies reach the function base64-encoded, which inflates them by about a third toward that limit. | No fixed limit on `INSERT` data: a body over 1 MiB is split at the start of its inline data and streamed into the statement. Query text is limited to 256 MiB. |
| Response size | 6 MB per response. | No fixed limit. The result rows are held in memory before they are encoded, so process memory bounds a result. |
| Response streaming | No: the whole response is buffered and returned at the end. | Yes: once 1 MiB of encoded output is buffered, the rest is sent as it is encoded. `wait_end_of_query=1` buffers the whole response, so a late error still gets its HTTP status. |
| Query time limit | The API Gateway integration timeout, 29 s by default (`ApiIntegrationTimeoutMillis`; more needs a quota increase). `MaxExecutionTime` defaults to 25 s, so a slow query gets `TIMEOUT_EXCEEDED` (159) instead of a 504 without a ClickHouse error body. | ECS: `MaxExecutionTime` 100 s by default, below the load balancer idle timeout (`LoadBalancerIdleTimeout`, at most 115 s). Self-managed: chserver has no limit besides `MAX_EXECUTION_TIME`; the load balancer's idle timeout bounds a query that has not started sending, and it must stay below chserver's fixed 120 s keep-alive idle timeout. |
| DDL with `GLOBAL_MODE=aws` | Asynchronous. `CREATE TABLE` returns once the table is `ACTIVE` in the issuing region, and the reconciler adds the replicas (minutes). `DROP TABLE` returns at once, and the reconcilers delete tables and mappings within about two rounds. | Synchronous. `CREATE TABLE` of a global table waits until its replicas exist (minutes), which can outlast the load balancer idle timeout. The client then gets a timeout, but the catalog entry is already written and the reconciler completes the table. |
| Cold starts | A new execution environment checks the system tables and loads the catalog before its first request. `ApiProvisionedConcurrency` keeps environments warm at an hourly cost. | None once a process runs. chserver starts listening only after the system tables are ready. On the first start of a `GLOBAL_MODE=aws` deployment that takes minutes: the home region adds the `__catalog` replicas and waits for them, and another region waits for its replica. |
| Scaling model | API: one invocation per request, up to the account's concurrency or `ApiReservedConcurrency`. Stream worker and replicator: one concurrent invocation per stream shard. | chserver: add instances (ECS: `ChserverDesiredCount`, or CPU target tracking up to `ChserverMaxCount`). mvworker: instances share the shards through leases. Replicator: one per deployment. |
| Stream consumers managed by | The reconciler Lambda: it creates, updates and removes one event-source mapping per table stream as tables appear and disappear. | mvworker and the replicator themselves: they find tables in the catalog and read the streams with shard leases and checkpoints in `{prefix}__checkpoints`. |
| Reconciler | `cmd/reconciler-lambda`, on the `__catalog` stream and a one-minute schedule. | Inside every chserver process, every `CATALOG_REFRESH`. |
| Replicator (`GLOBAL_MODE=aws`) | One Lambda per region, on that region's ReplacingMergeTree streams. | One long-running process for the whole deployment, tailing every region's streams (ECS: in the home region). |
| Multi-region | One stack per region, home region first. | One stack (or set of processes) per region, home region first. |
| Network | The functions run outside your VPC; only the API is reached through the VPC endpoint. | Private subnets with a NAT gateway (or other internet egress) in every region. The stream consumers read DynamoDB Streams, which the DynamoDB gateway endpoint does not cover, and the replicator reaches the other regions' DynamoDB. VPC endpoints for DynamoDB, ECR, CloudWatch Logs and Secrets Manager are optional and reduce NAT traffic. |
| Idle cost | Low: the interface endpoint's hourly charge per Availability Zone, one scheduled reconciler run per minute, DynamoDB storage. | Tasks bill while they run (ECS defaults per region: two chserver tasks of 1 vCPU / 2 GB, one mvworker of 0.5 vCPU / 1 GB, and one replicator of 0.5 vCPU / 1 GB in the home region), and the load balancer and the NAT gateway or interface endpoints bill per hour. |
| Operational burden | No servers to size or patch. Event-source mappings are not stack resources, and API Gateway has client-side quirks (the `Accept` header, `+` in query strings, the `/v1` prefix). | Image builds and pushes per region, task sizes and counts, VPC networking. Self-managed adds the load balancer, the secret store, IAM and process supervision. |

### Common to every AWS deployment

**Regions and `GLOBAL_MODE`.**

- **Several regions:** use `GLOBAL_MODE=aws`. Source tables and `__catalog`
  become DynamoDB global tables, `ReplacingMergeTree` tables get one table
  per region kept in sync by the replicator, and rollups are regional
  (see [Consistency model](#consistency-model)).
- **One region:** use `GLOBAL_MODE=none`. Nothing is replicated and no
  replicator runs.
- **`emulated`** is for DynamoDB Local only. `GLOBAL_MODE=aws` refuses a
  `DYNAMODB_ENDPOINT` and needs at least one other region in
  `REPLICA_REGIONS`.
- **Same settings in every stack:** use the same `TablePrefix`, the same
  `ReplicaRegions` (every region, the stack's own included) and the same
  `HomeRegion` everywhere. `REPLICA_REGIONS` drops the process's own
  region, so the same list works in every region. `GLOBAL_HOME_REGION`
  defaults to the first of the sorted region list; set it explicitly.

**Deploy the home region first.** With `GLOBAL_MODE=aws` the home region
(`GLOBAL_HOME_REGION`) creates `__catalog` and adds its replicas. The
other regions wait at startup until their replica exists, and they join
global tables the same way rather than creating their own. AWS refuses to
add a replica where a table of that name already exists, so a replica
region deployed first would have to be undone by deleting its tables; the
server reports that case with the region holding the stray table. Tables
created later with `CREATE TABLE` are created and replicated by the region
that ran the statement; the other regions' reconcilers join them.

**Table prefix.** `TABLE_PREFIX` (default `twinhouse_`) prefixes every physical
table and is also the IAM scope (`table/<prefix>*`), so one account can
host several deployments with different prefixes. Prefixes must not be
prefixes of one another (`twinhouse_` and `twinhouse_prod_`). Otherwise the
shorter one's policies would cover the other's tables, and its reconciler,
which drops physical tables of its deployment that no catalog entry
names, could take the other's tables for its own.

**Users and secrets.** `CH_USERS` is `user:password,user2:password2`. The
default, `default:`, is the user `default` without a password, so set it
in every real deployment. Authentication is ClickHouse's user and password
only. The network boundary is the VPC endpoint's security group
(serverless) or the load balancer's (containers).

- **Serverless:** the `ChUsers` parameter. It is `NoEcho`, which masks it
  only in CloudFormation output; the value is in the API function's
  environment. Keep it in a git-ignored `samconfig.local.toml`, and set
  `KmsKeyArn` to restrict who can decrypt it (deploy/aws-lambda/README.md
  gives the order of the key policy and the deploys).
- **ECS:** a Secrets Manager secret (`ChUsersSecretArn`) that ECS injects
  at task start. The password never appears in a parameter, the task
  definition or the environment shown by the console.
- **Self-managed:** pass it from your secret store as an environment
  variable.

**Credentials.** The binaries use the AWS SDK's default credential chain:
the Lambda execution role, the ECS task role, EKS pod roles (IRSA or Pod
Identity), an EC2 instance profile or environment variables. Leave
`DYNAMODB_ENDPOINT` unset: it is for DynamoDB Local and switches to dummy
credentials. `REGION` falls back to `AWS_REGION`, which Lambda and ECS set.

**Tables are created at runtime.** No template declares a DynamoDB table.
The processes create them, so one stack serves any schema. All tables are
`PAY_PER_REQUEST`. Streams (`NEW_AND_OLD_IMAGES`) are on the catalog and
on every row table except the regional Aggregating and Summing rollups,
which have no stream consumer.

| Table | Kind |
|---|---|
| `{prefix}__catalog` | Global table. The home region (`GLOBAL_HOME_REGION`) creates it and adds the replicas; the other regions wait for their replica. |
| `{prefix}{db}.{table}.{id}` for `MergeTree` / `Memory` / `Log` | Global table, created in the region that ran the `CREATE`, plus replicas. |
| `{prefix}{db}.{table}.{id}` for `ReplacingMergeTree` | One table per region (each region's reconciler creates it), kept in sync by the replicator. |
| `{prefix}{db}.{table}.{id}` for `Aggregating` / `SummingMergeTree` (+ `.members` for `uniq*` / `groupUniqArray` states) | Regional: one independent copy per region. |
| `{prefix}__mv_markers` (TTL on `_exp`), `{prefix}__checkpoints`, `{prefix}__dead_letters` | Regional system tables. |

**Deleting a stack never deletes data.** CloudFormation does not own the
tables, so remove them deliberately: replicas first, then the table, as
in [deploy/aws-ecs/README.md](deploy/aws-ecs/README.md#teardown).
DynamoDB allows removing a replica, or deleting a table that was the
source of a replica, only 24 hours after the replica was added.

#### Moving between options

Every option reads and writes the same tables under the same
`TABLE_PREFIX`. To move a region:

1. Deploy the other option with the same `TablePrefix`,
   `ReplicaRegions` and `HomeRegion`.
2. Move the clients to it.
3. Delete the old stack. The tables stay.

While both run, views are still applied once, because the exactly-once
markers deduplicate them as they do for inline views and the worker. The
cost is duplicate stream processing. After removing a serverless stack,
delete any event-source mappings that its function deletion left behind.

#### IAM permissions

The tables below list what each role needs. Each option's subsection
lists what its template grants, and `go test ./deploy/aws-lambda/ ./deploy/aws-ecs/`
checks the scoping. The resources are
`arn:aws:dynamodb:*:*:table/{prefix}*` and
`arn:aws:dynamodb:*:*:table/{prefix}*/stream/*`, in every region of the
deployment.

| Who | Actions |
|---|---|
| chserver | `dynamodb:DescribeTable`, `CreateTable`, `UpdateTable`, `DeleteTable`, `DescribeTimeToLive`, `UpdateTimeToLive`, and the item actions `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `BatchGetItem`, `BatchWriteItem`, `ConditionCheckItem`, `Query`, `Scan` (`TransactWriteItems` is authorized through Put/Update/ConditionCheck) |
| mvworker | the same as chserver without `DeleteTable` |
| replicator | the item actions, `DescribeTable` and `CreateTable` (its checkpoint and `__dead_letters` tables) |
| stream consumers (mvworker, replicator, chserver with `RUN_STREAM_WORKER`) | `dynamodb:DescribeStream`, `GetShardIterator`, `GetRecords` (Lambda event-source mappings also need `ListStreams`) |
| chserver or reconciler-lambda with `GLOBAL_MODE=aws` (adds and removes replicas) | `dynamodb:CreateTableReplica`, `DeleteTableReplica`, and `iam:CreateServiceLinkedRole` for `replication.dynamodb.amazonaws.com`. Adding a replica also needs `CreateTable`, `UpdateTable` and the item actions on the table in the replica region. In the home region mvworker needs the same for `{prefix}__catalog` only (`EnsureSystemTables` at startup). |
| reconciler (chserver or reconciler-lambda) | `dynamodb:ListTables` (no resource scoping): finds physical tables of the deployment whose catalog entry vanished while no reconciler was running |
| reconciler with `MVWORKER_FUNCTION_ARN` (chserver or reconciler-lambda) | `dynamodb:DescribeTable` (stream ARNs), `lambda:ListEventSourceMappings` (no resource scoping), `lambda:CreateEventSourceMapping`, `UpdateEventSourceMapping`, `DeleteEventSourceMapping` scoped with `lambda:FunctionArn` to the stream worker, replicator and reconciler functions |

### Local stack (`deploy/local`)

`make up` builds the image and starts
[`deploy/local/docker-compose.yml`](deploy/local/docker-compose.yml) as the
compose project `twinhouse`:

- two DynamoDB Local instances, in memory, standing in for `us-east-1`
  (host port 18000) and `eu-west-1` (18001);
- a `chserver` per region: `us` on `:8123`, `eu` on `:8124`;
- an `mvworker` per region;
- the replicator, copying every global table between the two instances
  with last-writer-wins (`GLOBAL_MODE=emulated`, DESIGN.md §6).

```sh
make up                   # two regions: :8123 (us), :8124 (eu)
make up PROFILES=compat   # also the reference ClickHouse 25.8 on :18123 (tests/compat)
make up-single            # one region, no replication, worker in-process: :8125
make logs                 # follow the logs; make ps lists the services
make down                 # remove the containers and their data
```

- **Ports and projects:** the host ports can be overridden (`US_HTTP_PORT`,
  `EU_HTTP_PORT`, `DDB_US_PORT`, `DDB_EU_PORT`, `CH_REF_PORT`,
  `DDB_SINGLE_PORT`, `SINGLE_HTTP_PORT`), and `COMPOSE_PROJECT` runs a
  second stack beside the first (see [Testing](#testing)).
- **Users:** no `CH_USERS` is set, so the user is `default` without a
  password.
- **Data:** DynamoDB Local keeps everything in memory, and `make down`
  discards it. DynamoDB Local also slows down as state accumulates (see
  [Testing](#testing)).
- **Upgrades:** `make up` rebuilds the image and recreates the containers
  whose image changed.
- **Not for production:** the stack has no TLS, no real global tables and
  no durability.

### AWS serverless (`deploy/aws-lambda`)

[deploy/aws-lambda](deploy/aws-lambda/README.md) is an AWS SAM template
that deploys everything as Lambda functions, **one stack per region**
(`us-east-1` as the home region, then `eu-west-1`):

```
                 application VPC (region A)                        (same stack in region B)
                        │ https via the execute-api VPC endpoint
            ┌───────────▼─────────────┐
            │ private REST API Gateway│  ANY / and /{proxy+}, binary media */*
            └───────────┬─────────────┘
                        │ Lambda proxy integration (6 MB payloads, 29 s)
            ┌───────────▼─────────────┐
            │ API Lambda              │  cmd/chserver-lambda: the ClickHouse HTTP API
            │ (no sessions)           │  catalog reloaded per request when stale
            └───────────┬─────────────┘
                        │ Put/Update/Query/Scan; CREATE/DROP TABLE at runtime
   ┌────────────────────▼──────────────────────────────────────────────────┐
   │ DynamoDB (region A)   global: source tables, __catalog ◄──replication──┼──► region B
   │                       regional: rollups, __mv_markers, __checkpoints   │
   └───────┬──────────────────────────┬──────────────────────────┬─────────┘
           │ source-table streams     │ __catalog stream         │ ReplacingMergeTree streams
           │ (ESM, TRIM_HORIZON)      │ (ESM)                    │ (ESM, GLOBAL_MODE=aws)
   ┌───────▼─────────┐     ┌──────────▼──────────┐     ┌─────────▼──────────┐
   │ stream worker   │     │ reconciler Lambda   │     │ replicator Lambda  │
   │ Lambda          │     │ cmd/reconciler-     │     │ cmd/replicator     │
   │ cmd/mvworker    │     │ lambda: tables,     │     │ version-aware copy │
   │ applies MVs to  │     │ replicas, and the   │     │ to the other       │
   │ A's rollups     │     │ ESMs of every table │     │ regions            │
   └─────────────────┘     └──────────▲──────────┘     └────────────────────┘
                                      │ EventBridge rate(1 minute), the safety net
```

**What it creates:** the four functions (three with `GlobalMode=none`:
no replicator), one scoped role per function,
the private REST API with a resource policy that allows only
`VpcEndpointId`, the reconciler's schedule and the log groups. By default
it also creates the region's API Gateway CloudWatch Logs role
(`ManageApiGatewayAccount`).

- **API Lambda:** wraps the same `server.Server` as `cmd/chserver`
  (`aws-lambda-go-api-proxy`). It has no background loops. At the start of
  a request it checks that the system tables exist and reloads the
  catalog snapshot when it is older than `CATALOG_REFRESH`. HTTP sessions
  are rejected with `NOT_IMPLEMENTED` (48).
- **Stream worker and replicator:** the existing binaries in Lambda mode
  (detected through `AWS_LAMBDA_RUNTIME_API`). DynamoDB-stream
  **event-source mappings (ESMs)** feed them, with
  `ReportBatchItemFailures` and unlimited retries; the exactly-once
  markers make redeliveries harmless.
- **Reconciler Lambda:** runs one reconcile round per invocation. It
  ensures the system tables and this region's physical tables (and their
  replicas, in the region that owns them). It also keeps the ESMs: one per
  source-table stream to the worker, one per ReplacingMergeTree stream to
  the replicator, and the `__catalog` stream to itself. A DDL in any
  region therefore triggers a round within seconds, and the schedule is
  the safety net. Mappings of dropped tables are removed.

**Prerequisites:**

- An interface VPC endpoint for `execute-api` in every region.
- The SAM CLI.
- Go, for `sam build`.

**Quick start:**

```sh
make aws-validate                # sam validate --lint (no credentials needed)
make aws-build                   # the four arm64 bootstraps
# fill in VpcEndpointId in deploy/aws-lambda/samconfig.toml (both environments) first
make aws-deploy ENV=us           # home region first; wait for its first reconcile
make aws-deploy ENV=eu
make up && make aws-local-api    # optional: the API Lambda under sam local (:3000), no AWS
```

`make aws-deploy` refuses `--parameter-overrides`, because the SAM CLI
would replace the environment's whole parameter list with it. Secrets go
into a git-ignored copy of the file instead
(`make aws-deploy ENV=us SAM_CONFIG=samconfig.local.toml`).

**Limits**, detailed in
[deploy/aws-lambda/README.md](deploy/aws-lambda/README.md#limits-of-the-lambda-deployment):

- **6 MB payloads.** The Lambda invocation payload bounds request and
  response bodies, and base64 inflates binary bodies by about a third
  toward that limit. Split large `INSERT`s and page large `SELECT`s.
- **No HTTP sessions.** `session_id`, `session_check` and
  `session_timeout` fail with `NOT_IMPLEMENTED` (48). Use URL or
  `SETTINGS` settings instead of `SET`, and `database=` instead of `USE`.
- **Timeouts.** The API Gateway integration timeout (29 s by default)
  bounds a query. Keep `MAX_EXECUTION_TIME` below it, so a slow query gets
  `TIMEOUT_EXCEEDED` instead of a 504.
- **Cold starts.** A new execution environment first checks the system
  tables and loads the catalog before its first request. Provisioned
  concurrency removes the cold start at an hourly cost.
- **DDL propagation.**
  - `CREATE TABLE` returns when the table is `ACTIVE` in the issuing
    region. The reconciler then adds the replicas, which takes minutes;
    until then the table is unknown in the other regions.
  - `DROP TABLE` returns at once. The reconcilers delete the physical
    tables and mappings within about two rounds. DynamoDB allows deleting
    a table only 24 hours after a replica was added to it, so the
    reconciler retries until then.
- **Binary responses from Go clients.** API Gateway decodes a base64
  response only for a request whose `Accept` matches a binary type. Send
  `Accept: */*` from `net/http` and clickhouse-go (curl does by default).
- **Private only.** There is no public endpoint.

**Permissions:** the template gives each function only the calls it makes
in Lambda mode:

- **API:** the item actions, plus `DescribeTable`, `CreateTable`,
  `UpdateTable` and `DeleteTable`. It creates and drops tables but never
  touches replicas or TTL; replicas are the reconciler's job.
- **Stream worker:** the item actions in its own region, `DescribeTable`,
  `CreateTable` (for its `__dead_letters` table) and the stream actions.
  It needs no TTL actions: `__dead_letters` has no TTL, and in Lambda mode
  the worker does not create the system tables.
- **Replicator:** the same, with the item and table actions in every
  region (its targets).
- **Reconciler:** everything above, including the replica actions, the
  service-linked role, `ListTables` and the `lambda:*EventSourceMapping`
  actions.

SAM also attaches `AWSLambdaBasicExecutionRole` (CloudWatch Logs) to every
role.

**Upgrade:** `make aws-build`, then `make aws-deploy ENV=us` and
`ENV=eu`. Deploy the home region first when a release changes the table
layout or the catalog format. The API publishes a new version behind its
`live` alias. To roll back, deploy the previous build.

**Teardown:** `sam delete --config-env eu`, then `--config-env us`, run in
`deploy/aws-lambda`. This removes the functions, the API and the roles.
With `ManageApiGatewayAccount=true` (the default) one of those roles is the
region's API Gateway CloudWatch Logs role, so other REST APIs of the region
that log through it lose their logging; set `cloudwatchRoleArn` again (or
hand the setting to another owner first). It leaves the tables and the
reconciler-created event-source mappings; delete those deliberately. To
remove only one region, see
[deploy/aws-lambda/README.md](deploy/aws-lambda/README.md#rolling-out-a-new-version).

[deploy/aws-lambda/README.md](deploy/aws-lambda/README.md) has every
parameter, how to point clickhouse-go and curl at the stage URL, the
`ChUsers` and KMS handling, operations, scaling, cost notes and the
local `sam local` runs.

### AWS containers on ECS Fargate (`deploy/aws-ecs`)

[deploy/aws-ecs](deploy/aws-ecs/README.md) is a plain CloudFormation
template that runs the long-running binaries on ECS Fargate in an
existing VPC, **one stack per region**. It has no SAM transform; it is
deployed with `sam deploy` for parity with the serverless stack.

```
            application VPC (region A)                     (same stack in region B,
                   │ http :8123 / https :8443                without the replicator)
     ┌─────────────▼──────────────┐
     │ internal Application LB    │  health check /ping
     └─────────────┬──────────────┘
     ┌─────────────▼──────────────┐
     │ chserver service           │  cmd/chserver: HTTP API + reconciler
     └─────────────┬──────────────┘
   ┌───────────────▼─────────────────────────────────────────────┐
   │ DynamoDB (region A)   global tables ◄──replication──────────┼──► region B
   └───────┬──────────────────────────────────────┬──────────────┘
           │ source-table streams                 │ ReplacingMergeTree streams of every region
   ┌───────▼──────────┐                 ┌─────────▼───────────────────┐
   │ mvworker service │                 │ replicator service          │
   │ (or in-process)  │                 │ (home region only)          │
   └──────────────────┘                 └─────────────────────────────┘
```

**What it creates:** the ECS cluster (or it uses `ExistingClusterName`),
the internal Application Load Balancer, its listeners and target group,
the security groups, the log groups, a task execution role, one task role
per service, and these services:

- **chserver** runs behind the load balancer, whose listener uses
  ClickHouse's HTTP port **8123**, so clients switch by host and port
  only. An optional HTTPS listener on **8443** uses an ACM certificate.
  The target group checks `/ping`. `CH_USERS` comes from a Secrets
  Manager secret through the task definition's `Secrets`.
- **mvworker** is a separate service (`WorkerMode=service`; shard leases
  split the work between its tasks), or it runs inside every chserver task
  (`WorkerMode=in-process`, `RUN_STREAM_WORKER=true`).
- **replicator** runs once per deployment, by default in the home region
  (`RunReplicator=auto`), with `REGIONS=us-east-1=,eu-west-1=` built from
  `ReplicaRegions`.
- **Access:** the load balancer accepts only `AllowedCidr` and
  `ClientSecurityGroupId`, and the tasks accept only the load balancer.
- **Parameters** set optional CPU target-tracking autoscaling for
  chserver, log retention, task sizes and counts.

**Prerequisites** in every region:

- Private subnets in two or more Availability Zones, with a NAT gateway
  (or other internet egress). mvworker, the in-process worker and the
  replicator read DynamoDB Streams, which the DynamoDB gateway endpoint
  does not cover, and the replicator calls the other regions. VPC
  endpoints for DynamoDB, ECR, CloudWatch Logs and Secrets Manager are
  optional; STS is not needed.
- An ECR repository.
- The `CH_USERS` secret.

**Quick start:**

```sh
make ecs-validate                                   # sam validate --lint (no credentials needed)
make image-push IMAGE_REPO=<account>.dkr.ecr.us-east-1.amazonaws.com/twinhouse \
  IMAGE_TAG=v1 PLATFORM=linux/arm64                 # after docker login to ECR; per region
# fill in the CHANGE_ME values in deploy/aws-ecs/samconfig.toml (both environments) first
make ecs-deploy ENV=us                              # home region first
make ecs-deploy ENV=eu
make ecs-local                                      # optional: the task definitions as local
                                                    # containers + tests/e2e, no AWS (make up first)
```

The `CHANGE_ME` placeholders are `ImageUri`, `VpcId`, `SubnetIds`,
`AllowedCidr` and `ChUsersSecretArn`, and CloudFormation rejects them, so
a forgotten one cannot deploy. As with `aws-deploy`, `make ecs-deploy`
refuses `--parameter-overrides`. Use a git-ignored copy instead
(`ECS_CONFIG=samconfig.local.toml`).

**Limits:**

- **Sessions with several tasks.** A session lives in one chserver task.
  The load balancer keeps a client on one task only with target-group
  stickiness and a client that returns its cookie, which most ClickHouse
  clients do not. Prefer URL settings and `database=`, or run one task.
- **Query time.** `MaxExecutionTime` (100 s) must stay below
  `LoadBalancerIdleTimeout` (110 s, at most 115 s), which must stay below
  chserver's fixed 120 s keep-alive. Otherwise the load balancer can reuse
  a connection chserver just closed and return sporadic 502s.
- **DDL.** A global `CREATE TABLE` waits for its replicas (minutes) and
  can outlast the idle timeout (see [Comparison](#comparison)).
- **No Lambda permissions.** The template grants none, so event-source
  mappings are not used (`MVWORKER_FUNCTION_ARN` stays unset).

**Permissions:** each service has its own task role:

- **chserver:**
  - the item and table actions (`DeleteTable` included) and the TTL
    actions in its region, and `ListTables`;
  - with `GLOBAL_MODE=aws`, the replica actions, limited by
    `aws:RequestedRegion` to the deployment's regions;
  - the stream reads only with the in-process worker.
- **mvworker:** the item, table (no `DeleteTable`), TTL and stream actions
  in its region, plus the `__catalog` replica actions in the home region.
- **replicator:** the item actions, `DescribeTable`, `CreateTable` and the
  stream actions in every region of the deployment.

The task execution role pulls the image, writes logs and reads the
`CH_USERS` secret.

**Upgrade:**

1. Push a new tag (`make image-push ... IMAGE_TAG=v2`).
2. Set `ImageUri` to it in each environment of `samconfig.toml`, or of the
   local copy.
3. Run `make ecs-deploy`, home region first when a release changes the
   table layout or the catalog format.

Each service rolls out with 100% minimum healthy, and the deployment
circuit breaker rolls back a version whose tasks keep failing. Stopping
tasks first leave the load balancer for `DeregistrationDelay` (110 s), so
in-flight queries finish.

**Teardown:** `sam delete --config-env eu`, then `--config-env us`, run in
`deploy/aws-ecs`. This removes the services, the load balancer, the roles
and the log groups, not the tables.

[deploy/aws-ecs/README.md](deploy/aws-ecs/README.md) has the full
prerequisites, the image build for `linux/arm64` or `linux/amd64`, every
parameter, how to verify a stack, operations, scaling, upgrades, teardown
with table deletion, and what `make ecs-local` checks.

### Self-managed containers

The image built from `deploy/Dockerfile` contains `chserver`, `mvworker`
and `replicator` as static binaries on Alpine.

- **Command:** the container command picks the binary; the default is
  `chserver`.
- **Runtime:** it runs as the non-root user `app` (uid 10001) and exposes
  port 8123. The binaries need no writable filesystem, so a read-only
  root filesystem works, and they log JSON lines to stderr.
- **Building:** `make image-push IMAGE_REPO=<registry>/<repo> IMAGE_TAG=<tag>
  PLATFORM=linux/arm64` (or `linux/amd64`) builds and pushes it to any
  registry you are logged in to.

`deploy/aws-ecs/template.yaml` is a worked example of the following.

**Processes.** Run the following per region, plus one replicator for the
whole deployment:

| Process | How many | Required settings | Often set |
|---|---|---|---|
| `chserver` | One or more per region, behind a load balancer | `REGION` (or `AWS_REGION`), `TABLE_PREFIX`, `GLOBAL_MODE`; with `aws` also `REPLICA_REGIONS` and `GLOBAL_HOME_REGION`; `CH_USERS` from your secret store | `MAX_EXECUTION_TIME`, `INLINE_VIEWS`, `RUN_STREAM_WORKER`, `CATALOG_REFRESH`, `LOG_LEVEL`, `LISTEN_ADDR` (default `:8123`) |
| `mvworker` | One or more per region, unless chserver runs the worker in-process (`RUN_STREAM_WORKER=true`) | The same `REGION`, `TABLE_PREFIX`, `GLOBAL_MODE`, `REPLICA_REGIONS` and `GLOBAL_HOME_REGION` as chserver | `MV_POISON_POLICY`, `STREAM_LEASE_DURATION`, `STATS_INTERVAL` |
| `replicator` | Once per deployment, typically in the home region; only with `GLOBAL_MODE=aws` | `GLOBAL_MODE=aws`, `REGIONS=us-east-1=,eu-west-1=` (every region; empty endpoints mean real AWS), `TABLE_PREFIX` | `APPLY_CONFLICT_VIEWS` (default `true`), `MV_POISON_POLICY`, `STREAM_LEASE_DURATION` |

Keep `STREAM_VIEWS=true` (the default) on chserver whenever a worker runs.
Leave `DYNAMODB_ENDPOINT` unset. Give each process the permissions of
[IAM permissions](#iam-permissions).

**Why the replicator is needed.** In AWS mode it copies only the
`ReplacingMergeTree` tables. AWS's item-level last-writer-wins could let a
lower version written later win, and it would silently drop the losing
version of a concurrent write, which then never reaches that region's
views. The replicator uses a version-aware conditional put and applies the
views to the versions it rejects. Without it, `ReplacingMergeTree` tables
stay regional. It needs network access to DynamoDB in every listed region.

**Health checks.**

- **chserver:** `GET /ping` answers `Ok.` without authentication and
  without calling DynamoDB. Use it for the load balancer and for
  readiness. chserver starts listening only once the system tables are
  ready. On the first start of a `GLOBAL_MODE=aws` deployment that takes
  minutes: the home region adds the `__catalog` replicas and waits until
  they are `ACTIVE` (up to 5 minutes per attempt), and another region
  waits for its replica. Allow for that in the startup grace period (ECS
  uses 600 s). Later starts are fast.
- **mvworker and the replicator:** no HTTP endpoint. Supervise the
  process, and watch their progress log lines (every `STATS_INTERVAL`).

**Graceful shutdown.**

- **chserver:** on SIGTERM it stops its background loops (reconciler,
  in-process worker) and drains in-flight requests for up to 15 s, then
  exits. Take an instance out of the load balancer before sending SIGTERM,
  for as long as your longest query (ECS: `DeregistrationDelay`;
  Kubernetes: a `preStop` delay). Allow at least 15 s more before SIGKILL.
- **mvworker and the replicator:** on SIGTERM they stop their shard
  consumers. A lease that was not released expires after
  `STREAM_LEASE_DURATION` (30 s), and another instance resumes the shard
  from its checkpoint. The exactly-once markers make re-application
  harmless.
- **Timeouts:** keep the load balancer's idle timeout below chserver's
  120 s keep-alive, and `MAX_EXECUTION_TIME` below the idle timeout.

**Scaling.**

- **chserver** is stateless apart from HTTP sessions. Several instances
  per region are fine, and each runs its own reconciler.
- **mvworker** instances share the streams' shards through leases in
  `{prefix}__checkpoints`. More instances help with many source tables or
  shards, and a second instance gives fail-over within the lease
  duration.
- **The replicator** runs once per deployment. Several instances are safe,
  because shard leases keep them from conflicting, but each one tails
  every region.

### Hybrid deployments

The binaries support these combinations, but neither template deploys
them; you assemble them from the pieces above.

- **Container API with Lambda stream consumers.**
  1. Run `chserver` (ECS, Kubernetes, EC2) without an mvworker service,
     without `RUN_STREAM_WORKER` and without a replicator process.
  2. Deploy `cmd/mvworker` as a Lambda function. With `GLOBAL_MODE=aws`,
     also deploy `cmd/replicator` as a Lambda in every region. The same
     binaries switch to Lambda mode under `AWS_LAMBDA_RUNTIME_API`;
     package them as `deploy/aws-lambda/template.yaml` does
     (`provided.al2023`, arm64).
  3. Set `MVWORKER_FUNCTION_ARN` (and `REPLICATOR_FUNCTION_ARN`) on
     chserver. Its reconciler, which runs every `CATALOG_REFRESH`, then
     creates or updates a table's event-source mappings when it first sees
     that table definition, and removes them before the table is dropped.
     It does not check them again for an unchanged definition, so a
     mapping deleted or disabled outside chserver stays that way until
     chserver restarts or the table definition changes.

  Several chserver instances may manage them concurrently, because a
  concurrent creation converges on the existing mapping. The `__catalog`
  stream is not wired to anything in this setup, since chserver reloads
  the catalog on its own interval. The chserver role needs the Lambda
  permissions of the reconciler row in
  [IAM permissions](#iam-permissions), which the ECS template does not
  grant.
- **Container API with event-source mappings you manage yourself.**
  Instead of setting `MVWORKER_FUNCTION_ARN`, create the mappings the
  reconciler would create:
  - one from the stream of every table a view can read from (MergeTree,
    ReplacingMergeTree, Memory and Log tables) to the stream worker
    function;
  - with `GLOBAL_MODE=aws`, one from the stream of every global
    ReplacingMergeTree table to the replicator function, or run
    `cmd/replicator` as a long-running process instead. Without one of the
    two, those tables are not replicated to the other regions.

  Create every mapping with `StartingPosition` `TRIM_HORIZON`,
  `MaximumRetryAttempts` -1 (unlimited), `BisectBatchOnFunctionError`
  false, `FunctionResponseTypes` `ReportBatchItemFailures`, and the batch
  size you would set as `ESM_BATCH_SIZE` (default 100). Exactly-once
  delivery and no loss depend on these values: `LATEST` skips the rows
  written before the mapping existed, and a retry limit drops records that
  keep failing. A dropped and re-created table has a new physical name, so
  it needs new mappings.
- **Lambda API with long-running stream consumers.**
  1. Deploy `cmd/chserver-lambda` and `cmd/reconciler-lambda` with
     `MVWORKER_FUNCTION_ARN` empty. The reconciler then manages tables
     only and picks up DDL on its schedule, since the `__catalog` stream
     is not wired.
  2. Run `mvworker` per region and the replicator once, as containers.

  The SAM template always wires the Lambda consumers, so this needs your
  own template.

Within the ECS template, `WorkerMode` (separate service or in-process)
and `RunReplicator` (`auto`, `true`, `false`) choose where the worker and
the replicator run.

### Cost model

Writes dominate. Costs below are in DynamoDB on-demand
units (a write is 1 WRU per started KB of the item, 2 WRU in a
transaction); [DESIGN.md §4 "Cost model"](DESIGN.md#cost-model) has every
call. The per-option READMEs add the API Gateway, Lambda, Fargate, load
balancer and NAT charges.

| Per source row | Write units |
|---|---|
| `MergeTree` / `ReplacingMergeTree` row | 1 WRU per KB, plus one replicated write per KB in each other region |
| each materialized view into a rollup, in **every** region | 2 WRU exactly-once marker, plus 2 WRU per KB of the rollup row once per ≤ 99 rows of that key in a batch |
| … when the view has `min`/`max`/`argMin`/`argMax`/`uniq*` columns | + 1 WRU done flag, one conditional update per extremum column per key and batch, 2 WRU per new set member |
| … when the rollup has float sums | + one consistent read per key and batch |
| stream worker re-applying rows already applied inline | one cancelled transaction per ≤ 99 rows of a key |

A hot key is cheap (its row update is shared by up to 99 rows); a rollup
whose keys are almost unique costs about 2 + 2 × (rollup row KB) WRU per
source row and view. Large `groupArray` rollup rows make every update of
their key more expensive. Markers (about 150 bytes) are kept 7 days
(TTL, free to expire). Queries without a sorting-key predicate are full parallel `Scan`s
billed by the bytes read: this suits key-oriented rollup reads, not ad-hoc
analytics over large raw tables.

### Limits

These are DynamoDB's limits and apply to every option. An `INSERT` row
that breaks one is rejected with `BAD_ARGUMENTS` before anything of the
statement is written:

- Items are limited to 400 KB, attribute names included. A rollup row
  grows with every merge, so only the bytes a merge adds are checked up
  front; a row that would grow past 400 KB fails with `BAD_ARGUMENTS` and
  DynamoDB's reason, and the stream worker dead-letters such view
  applications. Rollup state (for example `groupArray`) is bounded by the
  item size.
- Encoded keys are limited to 2048 bytes (partition) and 1024 bytes (sort,
  including a 16-byte uniquifier on `MergeTree`). Strings take about
  len+2 bytes, so do not put long unbounded strings (URLs, user agents) in
  `ORDER BY`. View output whose key is too long fails the view, not the
  source rows (it is dead-lettered by the stream worker).
- A merging table may have about 500 summed/count, 250 `avg` or 150
  `any`/`anyLast`/`groupArray` columns (one 4 KB DynamoDB update
  expression); `CREATE` and `ALTER` reject wider ones with
  `NOT_IMPLEMENTED`. `min`/`max`/`uniq*` columns do not count.

### Operations

- **Lag and failures:** watch the stream worker's lag (the mvworker's
  `pending_age_ms`, or `IteratorAge` of the Lambda worker and replicator)
  and the `SHARD WEDGED`, `DEAD LETTER` and `DATA LOSS` log lines.
  Inspect `{prefix}__dead_letters` for records that were set aside.
- **Stream retention:** stream records are trimmed after 24 h, so a shard
  stuck that long, or a worker down that long, loses view updates.
- **Replication:** watch DynamoDB `ThrottledRequests` and the global
  tables' `ReplicationLatency`.

The per-option READMEs list the logs and metrics to alert on.

---

## Testing

```sh
make test               # go test ./... — unit tests, no Docker needed
make test-integration   # streams, replicator and mvworker against two throwaway DynamoDB Local containers
DYNAMODB_TEST_ENDPOINT=http://localhost:18101 go test ./internal/storage/dynamo/...   # storage integration tests (any DynamoDB Local)

make up PROFILES=compat # local stack + reference ClickHouse 25.8 on :18123
make e2e                # tests/e2e against :8123 (single-region; also works with make up-single)
make compat             # tests/compat: differential tests against the reference ClickHouse

make aws-validate && make aws-build   # SAM template lint + the four Lambda bootstraps
go test ./deploy/aws-lambda/   # template vs. the binaries' contract (env vars, IAM scope, private API, schedule)
make aws-local-api      # the API Lambda under sam local (:3000) against the twinhouse DynamoDB Local
make aws-local-e2e      # tests/e2e against it; the suite detects Lambda mode and skips sessions
go test -tags samlocal -count=1 ./cmd/chserver-lambda/   # API Gateway specifics (base64 bodies, compression) against sam local

make ecs-validate       # ECS template lint (sam validate --lint)
go test ./deploy/aws-ecs/   # template vs. the binaries' contract (env vars, ports, /ping, secrets, IAM scope, conditions)
make ecs-local          # the ECS task definitions as local containers (ecsemu-*) against the twinhouse
                        # DynamoDB Local: tests/e2e, mvworker applying a view, replicator start, SIGTERM
```

The API Lambda's unit tests (`cmd/chserver-lambda`, `go test ./...`) drive
the real server with synthetic API Gateway proxy events and compare every
response byte for byte with the same server over plain HTTP, so the proxy
layer needs no AWS account to be tested.

The **multiregion** suite stops, kills and recreates containers, so it
needs its own compose project on separate ports, never the development
stack:

```sh
COMPOSE_PROJECT=dhmr US_HTTP_PORT=28123 EU_HTTP_PORT=28124 \
  DDB_US_PORT=28000 DDB_EU_PORT=28001 CH_REF_PORT=28125 make up
make multiregion        # ~4 minutes on a fresh stack
```

See `tests/multiregion/doc.go` for its `MR_*` variables and the optional
soak test (`MR_SOAK=1`).

**DynamoDB Local slows down as state accumulates.** It keeps all stream
and item history in memory, and gets markedly slower after a few suite
runs: an insert with an inline view went from about 1 s to about 25 s,
and the multiregion suite from about 3 to about 17 minutes. If tests start
timing out, recreate the stack first (`make down && make up`, or
`docker compose -p dhmr -f deploy/local/docker-compose.yml down -v` for the
multiregion stack). For experiments, prefer a fresh private container:

```sh
docker run -d --rm --name ddb-scratch -p 18100:8000 \
  amazon/dynamodb-local:latest -jar DynamoDBLocal.jar -sharedDb -inMemory
```

---

## Project layout

```
cmd/
  chserver/      ClickHouse-compatible HTTP server (+ reconciler, optional in-process worker)
  chserver-lambda/   the same server as a Lambda behind API Gateway (sessions rejected)
  mvworker/      materialized-view stream worker (long-running or AWS Lambda)
  replicator/    global-table emulation; in AWS mode, ReplacingMergeTree copier (long-running or AWS Lambda)
  reconciler-lambda/ the reconciler as a Lambda (catalog stream + schedule, event-source mappings)
internal/
  server/        HTTP protocol: params, settings, sessions, compression, errors, reconciler
  esm/           Lambda event-source mappings of table streams (reconciler hook)
  sqlparse/      ClickHouse SQL lexer, parser, AST, column naming
  exec/          query engine: planning, key pushdown, functions, aggregates, joins, DDL, views
  exec/aggstate/ aggregate states and their merge rules
  chtype/        ClickHouse types, values, parsing, formatting, comparison
  format/        input/output formats (Native, TSV, CSV, JSON*, Values, Pretty)
  catalog/       schema model, validation, replicated catalog with quarantine
  storage/       storage interface
  storage/dynamo/   DynamoDB backend: layout, writes, exactly-once merges, scans, resets
  storage/keyenc/   order-preserving tuple encoding for _pk/_sk
  storage/memstore/ in-memory backend for unit tests
  streams/       DynamoDB Streams reader: shards, leases, checkpoints, retries
  mvworker/      view application from stream records, poison handling, Lambda
  replicator/    cross-region copier (LWW / version-aware) and conflict hook
  cherr/         ClickHouse error codes, names and HTTP statuses
  config/, awsx/ environment configuration and AWS SDK clients
deploy/          Dockerfile (one image, all long-running binaries)
  local/         docker-compose.yml: the local multi-region stack
  aws-lambda/    serverless AWS deployment (SAM): API Gateway + Lambda
  aws-ecs/       container AWS deployment (CloudFormation): ECS Fargate + internal ALB
tests/           e2e, compat and multiregion suites (build tags)
DESIGN.md        architecture and contracts
```

---

## Limitations and roadmap

Known gaps, roughly in priority order:

- **Real-AWS validation.** Run the multiregion scenarios against real
  global tables in two regions, and publish latency and cost numbers.
- **Cost.** Every source row is a DynamoDB write, and every view adds
  marker writes in every region (see **Cost model** under
  [Deployment](#deployment)).
  Rows of one rollup key are serialized and keys run in parallel. Real
  numbers from AWS are still to be published.
- **Oversized keys.** There is no hashed-key fallback for partition or sort
  key values over DynamoDB's limits, and no `CREATE`-time warning for
  unbounded `String` key columns; such rows are rejected.
- **UUID key ranges.** DynamoDB orders the stored UUID text differently
  from ClickHouse, so a range predicate (`<`, `BETWEEN`, keyset
  pagination) on a `UUID` sorting-key column is evaluated by the engine
  after a query on the equality prefix, not pushed down as a key range.
  Results are correct; an order-preserving UUID key encoding would need a
  new key-format version.
- **More of ClickHouse.** Decimal, window functions, higher-order
  functions, quantiles and other aggregates, `ALTER … UPDATE`, more
  formats (RowBinary, Parquet), and cascading materialized views.
- **Native TCP protocol**, so clients that do not offer HTTP can connect.
- **Multi-region DDL.** Conditional DDL is checked only against the local
  region's catalog. Concurrent changes to one object from two regions can
  silently overwrite each other.
- **Rollup mutations.** `ALTER … DELETE` on regional rollups is not
  available in multi-region mode, and `TRUNCATE` boundaries are approximate
  by clock skew.
- **Older catalog entries.** A table created by a build that predates
  per-incarnation physical names keeps its rows in regions that did not run
  a `DROP` when it is re-created under the same name. Re-create such
  tables once.

Issues and pull requests are welcome. Please keep examples and schemas
generic, and add a compat scenario for any new SQL behaviour.

---

## License

License: TBD.
