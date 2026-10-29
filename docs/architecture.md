# Architecture

How twinhouse maps ClickHouse tables, materialized views and the schema
onto DynamoDB across regions.

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
  highest *version* to win (see [Consistency model](consistency.md)).
- **The schema is replicated too.** DDL is stored in a global `__catalog`
  table. Every server refreshes it and runs a reconciler that creates or
  drops this region's physical tables to match it.
- **The same code runs serverless.** On AWS the three roles above are
  Lambda functions: the HTTP API behind a private API Gateway, the stream
  worker and the replicator on DynamoDB-stream event-source mappings, and
  the reconciler on the `__catalog` stream plus a one-minute schedule
  (see [Deployment](deployment.md)). The local stack and the ECS
  deployment run the long-running binaries in containers.

[`DESIGN.md`](../DESIGN.md) is the detailed contract: physical layout,
encodings, write conditions, reset epochs, stream handling and error
classification. What clients observe is in
[Consistency model](consistency.md).

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
tools/           pins module dependencies (build tag tools)
.github/workflows/  CI (GitHub Actions)
.githooks/       pre-commit and pre-push hooks (make hooks)
.golangci.yml    golangci-lint configuration
DESIGN.md        architecture and contracts
```
