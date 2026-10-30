# Costs, limits and operations

These apply to every [deployment option](deployment.md).

## Cost model

Writes dominate. Costs below are in DynamoDB on-demand
units (a write is 1 WRU per started KB of the item, 2 WRU in a
transaction); [DESIGN.md §4 "Cost model"](../DESIGN.md#cost-model) has every
call. The per-option READMEs ([serverless](../deploy/aws-lambda/README.md),
[ECS](../deploy/aws-ecs/README.md)) add the API Gateway, Lambda, Fargate, load
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

## Limits

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

## Ingest throughput

How fast a table takes bulk writes on DynamoDB depends on two things
twinhouse does not choose for you. Measured with `cmd/chbench`, a load
of 100k events went from 1.4k to 16.6k rows/s once both were handled
([Comparison with ClickHouse](clickhouse-comparison.md#bulk-loads-on-dynamodb)).

- **The partition key.** A table's DynamoDB partition key is the leading
  `ORDER BY` column, or the columns named in `SETTINGS
  dynamodb_partition_key` ([SQL](sql.md)). DynamoDB writes about 1,000
  units a second per partition, so a key value that carries a large share
  of the rows, such as one big tenant, holds back the whole load however
  much capacity the table has.
  - Lead with a column that has many evenly used values, if the reads
    allow it: a query is a key `Query` only when it fixes the
    partition-key columns, and a full `Scan` otherwise.
  - Or spread a hot value over a bucket column: `ORDER BY (tenant_id,
    bucket, ts)` with `SETTINGS dynamodb_partition_key = 'tenant_id,
    bucket'`, where the writer sets `bucket` to a small hash of each row
    (for example a hash of the event id modulo 8; twinhouse has no hash
    functions to compute it). A tenant's reads then add `AND bucket IN
    (0, …, 7)`, which the planner turns into one `Query` per bucket. Keep
    a row's bucket stable on a `ReplacingMergeTree`, since the bucket is
    part of its key.
- **A new table.** twinhouse creates every table on demand, and each
  `CREATE TABLE` makes a new DynamoDB table. A new on-demand table starts
  with limited capacity and grows with its traffic, so a heavy load right
  after `CREATE TABLE` can be throttled. Before such a load, raise the
  table's warm throughput (`aws dynamodb update-table --warm-throughput
  WriteUnitsPerSecond=…`) or switch it to provisioned capacity, on the
  physical table `{prefix}{db}.{table}.{id}` (find it by listing the
  tables with that prefix). twinhouse leaves both
  settings alone. The system table `{prefix}__mv_markers`, written by every
  view, warms up with use.

Throttled writes are retried with backoff, so a throttled load is slow
rather than failed. DynamoDB's `WriteThrottleEvents` metric per table
shows it.

## Operations

- **Lag and failures:** watch the stream worker's lag (the mvworker's
  `pending_age_ms`, or `IteratorAge` of the Lambda worker and replicator)
  and the `SHARD WEDGED`, `DEAD LETTER` and `DATA LOSS` log lines.
  Inspect `{prefix}__dead_letters` for records that were set aside.
- **Stream retention:** stream records are trimmed after 24 h, so a shard
  stuck that long, or a worker down that long, loses view updates.
- **Replication:** watch DynamoDB `ThrottledRequests` and the global
  tables' `ReplicationLatency`.

The per-option READMEs ([serverless](../deploy/aws-lambda/README.md),
[ECS](../deploy/aws-ecs/README.md)) list the logs and metrics to
alert on.
