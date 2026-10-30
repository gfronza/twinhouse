# Comparison with ClickHouse

`cmd/chbench` runs one workload against twinhouse and against ClickHouse
on the same machine, and checks that both return the same results before
it times anything. This page shows what it measured and how to read it.

> **twinhouse ran on DynamoDB Local, not DynamoDB.** DynamoDB Local is a
> Java emulator. It has no network round trip, so key lookups are faster
> than on DynamoDB, but it runs one table on a single process, so scans and
> write-heavy work are much slower than on DynamoDB's partitioned storage.
> Neither direction says what twinhouse does in production. What the
> numbers do show is which query shapes stay close to ClickHouse and which
> fall far behind, and why. For cost, see
> [Costs, limits and operations](operations.md).

## Results

Measured on 2026-10-29 with the code at commit e4dc87c, on an Apple M5 Pro (15 CPUs,
24 GiB) with Docker Desktop given 14 GiB. Other containers were running
(load average 3–5). Each side was capped at 4 CPUs (see
[How it measures](#how-it-measures)). Versions: ClickHouse 25.8.33.6,
DynamoDB Local 3.3.2.

### Size M: 300k events

| Write | Rows | twinhouse rows/s | ClickHouse rows/s | ClickHouse is |
|---|--:|--:|--:|--:|
| W1 bulk load, 10k rows per INSERT, no views | 300k | 12.4k | 517k | 42× faster |
| W2 bulk load, 10k rows per INSERT, one SummingMergeTree view | 200k | 1.5k | 389k | 262× faster |

| Write | Server | p50 | p95 | p99 | inserts/s |
|---|---|--:|--:|--:|--:|
| W3 single-row INSERT, 8 concurrent clients | twinhouse | 1.6 ms | 3.7 ms | 6.3 ms | 4.0k |
| | ClickHouse | 6.3 ms | 11 ms | 21 ms | 1.2k |
| | ClickHouse, `async_insert=1` | 177 ms | 183 ms | 184 ms | 45 |

| Read | Shape | Clients | twinhouse p50 / p95 | ClickHouse p50 / p95 | twinhouse / ClickHouse (p50) |
|---|---|--:|--:|--:|--:|
| R1 | point lookup (ReplacingMergeTree FINAL) | 1 | 1.1 ms / 2.5 ms | 1.7 ms / 4.9 ms | 0.62× |
| R1 | | 8 | 1.6 ms / 3.5 ms | 2.4 ms / 4.4 ms | 0.64× |
| R2 | key range, aggregated (1 hour of a tenant) | 1 | 1.0 ms / 1.6 ms | 1.9 ms / 2.2 ms | 0.50× |
| R2 | | 8 | 2.2 ms / 3.5 ms | 4.5 ms / 18 ms | 0.48× |
| R3 | key range, top-N (latest 100 of a tenant) | 1 | 346 ms / 4.05 s | 7.0 ms / 14 ms | 49× |
| R3 | | 8 | 1.18 s / 9.12 s | 17 ms / 70 ms | 70× |
| R4 | rollup read (SummingMergeTree, one tenant) | 1 | 3.1 ms / 5.6 ms | 3.2 ms / 4.8 ms | 0.97× |
| R4 | | 8 | 4.4 ms / 6.6 ms | 3.3 ms / 4.0 ms | 1.3× |
| R5 | full scan + filter + GROUP BY | 1 | 27.9 s / 28.6 s | 21 ms / 48 ms | 1338× |
| R6 | high-cardinality GROUP BY (50k users) | 1 | 26.9 s / 27.5 s | 17 ms / 24 ms | 1554× |

R3 with 8 clients, R5 and R6 ran 5 rounds instead of 20 (a warm-up run
took over 2 s), so their p95 rests on 40, 5 and 5 samples.

### Size S: 100k events

| Write | Rows | twinhouse rows/s | ClickHouse rows/s | ClickHouse is |
|---|--:|--:|--:|--:|
| W1 bulk load, no views | 100k | 10.6k | 473k | 45× faster |
| W2 bulk load, one SummingMergeTree view | 100k | 1.5k | 350k | 236× faster |

W3 single-row inserts, p50: twinhouse 1.5 ms, ClickHouse 6.4 ms (177 ms
with `async_insert=1`).

| Read | Clients | twinhouse p50 / p95 | ClickHouse p50 / p95 | twinhouse / ClickHouse (p50) |
|---|--:|--:|--:|--:|
| R1 | 1 | 0.9 ms / 1.5 ms | 2.4 ms / 3.1 ms | 0.38× |
| R1 | 8 | 1.4 ms / 2.7 ms | 3.7 ms / 32 ms | 0.37× |
| R2 | 1 | 0.9 ms / 1.2 ms | 1.8 ms / 2.1 ms | 0.49× |
| R2 | 8 | 1.7 ms / 4.0 ms | 4.3 ms / 21 ms | 0.41× |
| R3 | 1 | 71 ms / 666 ms | 4.7 ms / 8.2 ms | 15× |
| R3 | 8 | 249 ms / 1.13 s | 11 ms / 27 ms | 23× |
| R4 | 1 | 3.1 ms / 4.3 ms | 2.1 ms / 3.1 ms | 1.5× |
| R4 | 8 | 5.2 ms / 9.1 ms | 3.1 ms / 4.4 ms | 1.7× |
| R5 | 1 | 2.81 s / 3.13 s | 12 ms / 20 ms | 243× |
| R6 | 1 | 2.75 s / 3.24 s | 9.9 ms / 12 ms | 278× |

R5 and R6 ran 5 rounds instead of 20, so their p95 is the slowest of 5.

### The read queries

Parameters change from round to round (the same for every run with the
same `-seed`); these are examples.

| Read | Query |
|---|---|
| R1 | `SELECT * FROM users_latest FINAL WHERE user_id = 27785` |
| R2 | `SELECT count(), sum(amount) FROM events WHERE tenant_id = 33 AND ts >= toDateTime64('2026-01-17 16:00:00.000', 3) AND ts < toDateTime64('2026-01-17 17:00:00.000', 3)` |
| R3 | `SELECT * FROM events WHERE tenant_id = 62 ORDER BY ts DESC LIMIT 100` |
| R4 | `SELECT day, kind, sum(n), sum(amount) FROM events_daily WHERE tenant_id = 62 GROUP BY day, kind ORDER BY day, kind` |
| R5 | `SELECT kind, count(), avg(duration_ms) FROM events WHERE url LIKE '%/checkout/%' GROUP BY kind ORDER BY kind` |
| R6 | `SELECT user_id, count() AS c FROM events GROUP BY user_id ORDER BY c DESC, user_id LIMIT 10` |

## What the numbers say

**Reads by sorting key keep up with ClickHouse.**
- **Point lookups (R1) and key ranges (R2):** twinhouse was about twice as
  fast. They become one DynamoDB `GetItem` or `Query` on a few items, and
  DynamoDB Local answers in well under a millisecond. ClickHouse reads at
  least one 8192-row granule per lookup.
- **Rollup reads (R4):** within 1–1.7× of ClickHouse. One `Query` over
  a tenant's rollup rows.
- **On DynamoDB,** each request adds a network round trip of a few
  milliseconds, so expect these to be close to ClickHouse rather than
  ahead.

**Top-N by the sorting key (R3) reads the whole key range.**
- `ORDER BY ts DESC LIMIT 100` on one tenant reads every event of that
  tenant and sorts them in memory. twinhouse does not yet turn it into a
  reverse `Query` that stops after 100 items.
- The cost follows the tenant's size: the busiest tenant has about a
  quarter of all events, hence the p95 of 4 s at 300k.
- This is an engine gap, not a DynamoDB limit.

**Full scans (R5, R6) are bound by DynamoDB Local's `Scan`.**
- Any predicate that is not on the sorting key becomes a parallel `Scan`
  of the whole table. Waiting on `Scan` pages is about 80% of R5's time;
  a bare `SELECT count() FROM events` takes almost as long.
- DynamoDB Local's `Scan` slows with table size, so the gap grows from
  about 250× at 100k events to about 1,400× at 300k.
- On DynamoDB, a parallel scan spreads over the table's partitions and a
  page's latency does not grow with the table, but it still reads and
  bills every item. ClickHouse scans 300k compressed rows in about 20 ms.
- Queries of this shape belong on a rollup (R4).

**Bulk writes are 40–260× slower.**
- **Without views (W1):** about 12k rows/s. Every row is one DynamoDB
  item, written in 25-item `BatchWriteItem`s, and DynamoDB Local was the
  busy process.
- **Through a view (W2):** about 1.5k rows/s. Each rollup group in a
  batch adds a `TransactWriteItems` with its exactly-once markers and the
  folded update ([cost model](../DESIGN.md#cost-model)). twinhouse's
  stream worker then re-checks every source row in the background; both
  servers had settled (twinhouse's re-check, ClickHouse's merges) within
  8–17 s of the load.
- ClickHouse appends a few column files per batch.

**Single-row inserts (W3) are faster on twinhouse.**
- twinhouse: 1.6 ms p50, one `BatchWriteItem` per insert.
- ClickHouse: 6.3 ms p50 as-is, because it creates a part per `INSERT`.
  With `async_insert=1` it waits about 180 ms for the flush to batch
  inserts together.
- On DynamoDB, twinhouse's latency is that of one write request.

## The workload

It follows what twinhouse is for: append-only events, a latest-state
table and a rollup fed by a materialized view. The same DDL runs on both
servers:

```sql
CREATE TABLE events (
  tenant_id UInt32, ts DateTime64(3), event_id UUID, user_id UInt64,
  kind String, url String, duration_ms UInt32, amount Float64
) ENGINE = MergeTree ORDER BY (tenant_id, ts);    -- DynamoDB: _pk = tenant_id, _sk = ts

CREATE TABLE users_latest (
  user_id UInt64, ver UInt64, plan String, country String, updated_at DateTime
) ENGINE = ReplacingMergeTree(ver) ORDER BY user_id;

CREATE TABLE events_daily (
  tenant_id UInt32, day Date, kind String, n UInt64, amount Float64
) ENGINE = SummingMergeTree ORDER BY (tenant_id, day, kind);

CREATE MATERIALIZED VIEW events_daily_mv TO events_daily AS
  SELECT tenant_id, toDate(ts) AS day, kind, count() AS n, sum(amount) AS amount
  FROM events_mv_src GROUP BY tenant_id, day, kind;  -- events_mv_src: same columns as events
```

The data comes from a seed, so both servers get the same rows:
- 100 tenants with skewed (Zipf) volume; the busiest has about a quarter
  of the events;
- 30 days of strictly increasing timestamps, 50k users, 6 event kinds
  (views are the most common, 40%) and URLs of 35–118 bytes;
- `users_latest`: every user in 3 versions, loaded version by version.

| Step | What it does |
|---|---|
| W1 | Loads `events` with `INSERT … FORMAT JSONEachRow`, 10k rows per request, one client. |
| W2 | Loads up to 200k of the same rows into `events_mv_src`, so every row also goes through the view. |
| W3 | Single-row `INSERT`s from 8 concurrent clients. ClickHouse runs it twice: as-is, and with `async_insert=1` (and `wait_for_async_insert=1`), the setting an application would use there. |
| R1–R6 | The read queries listed under the results. Their parameters (user, tenant, hour) come from a seed too, with tenants drawn as skewed as the data. R1–R4 run with 1 and 8 clients, R5–R6 with one. |

## How it measures

- Both servers get byte-identical HTTP requests; each response is read in
  full. Every request carries `max_execution_time` (120 s by default).
- Writes alternate between the servers batch by batch, and every read
  round runs on both, in alternating order. Load from other work on the
  machine hits both sides alike.
- Each read gets 3 untimed warm-up rounds, then 20 timed rounds (5 when a
  warm-up run takes over 2 s). With 8 clients a round is 8 queries.
- Before any read is timed, both servers must return the same rows for
  the table counts and for three parameter sets of every query (floats
  may differ in the last bits). A mismatch stops the run, so a wrong
  answer can never be reported as a fast one.
- After the loads it waits until every container is idle, so twinhouse's
  stream worker and ClickHouse's merges do not slow the reads.
- Each side is capped at 4 CPUs: ClickHouse alone, and twinhouse split
  between the server (1.5 CPUs) and DynamoDB Local (2.5 CPUs). Memory is
  not split evenly: ClickHouse (4 GiB) keeps its data on disk, while
  DynamoDB Local (6 GiB; the server 1 GiB) holds every item and stream
  record in memory. twinhouse runs single-region with the stream worker
  in-process (the compose `single` profile).

### A DynamoDB Local slowdown the benchmark works around

DynamoDB Local slows every write to a stream-enabled table in proportion
to the stream records written, by any table, before that table's own
first record. Deleting the older tables does not help. In a standalone
test (no twinhouse involved), a 25-item `BatchWriteItem` into a new
stream-enabled table took 9 ms on a fresh instance, 350 ms after 100k
stream records elsewhere and 1.3 s after 400k; a table that had written
one record before that history stayed at 3 ms.

twinhouse enables a stream on every source table, so the second large
load into a fresh DynamoDB Local runs tens to hundreds of times slower
than the first (W2 dropped from 1.4k to about 35 rows/s). DynamoDB has no such cost. chbench therefore:
- starts each run on a fresh DynamoDB Local (`make chbench` recreates the
  container);
- writes and deletes one sentinel row in each of twinhouse's
  stream-enabled tables before any load (ClickHouse does not need it).

The same effect makes a long-lived local development stack slower the
more it has been written to. Recreate it (`make down && make up`) when it
gets slow.

### Why size M is 300k events, not 1M

DynamoDB Local's `Scan` slows with the size of the table: each 1 MB page
(about 3,600 events) took about 0.3 s at 100k events and 10.5 s at 1M,
read directly from DynamoDB Local without twinhouse. The number of pages
grows with the table too, so a full scan grows with roughly the square of
the table size: `SELECT count() FROM events` took 2.3 s at 100k events and
7 min 35 s at 1M. At 1M, every full-scan query would only measure that.
On DynamoDB a page's latency does not grow with the table's size.

## Running it

```sh
make chbench-up                         # its own stack: twinhouse :38125, ClickHouse :38123, DynamoDB Local :38002
make chbench                            # size S (100k events), about 5 minutes
make chbench CHBENCH_ARGS='-size M'     # 300k events, about 17 minutes
make chbench CHBENCH_ARGS='-only R2,R5' # a few steps; reads load the data they need
make chbench-down
```

The stack is a separate compose project (`twinhouse-bench`) on its own
ports, so it never touches the development stack. `CHBENCH_UNCAPPED=1`
removes the CPU and memory caps (`deploy/local/bench.yml`). Docker needs
about 6 GiB of free memory for size M.

chbench prints the tables on this page and writes every sample as JSON to
`bin/chbench-<size>-<time>.json`. `go run ./cmd/chbench -h` lists its
flags (rounds, clients, seed, view rows, timeout).

## What it does not measure

- DynamoDB itself: its latency, throughput and cost.
- Multi-region: replication, the replicator, cross-region lag.
- The Lambda deployment.
- Large results: every read returns at most a few hundred rows.
