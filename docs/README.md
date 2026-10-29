# twinhouse documentation

Start with [Getting started](getting-started.md) for a local two-region
stack, then read the pages that match what you are doing.

## Using twinhouse

| Page | What it covers |
|---|---|
| [Getting started](getting-started.md) | The local two-region stack, a source table with a materialized-view rollup, cross-region reads and writes, latest-wins snapshots, and clickhouse-go v2. |
| [Architecture](architecture.md) | How ClickHouse tables map to DynamoDB, why rollups are regional, exactly-once views, the replicator, the replicated catalog, and the project layout. |
| [Consistency model](consistency.md) | What a client sees in the region it writes to and across regions: rollups, `ReplacingMergeTree`, conflicting writes, DDL, mutations, clock skew and `INSERT` atomicity. |
| [SQL support](sql.md) | Engines, DDL, queries, types, functions, aggregates, formats and the HTTP protocol; what is not supported and the deliberate deviations from ClickHouse. |
| [Configuration](configuration.md) | The environment variables of every binary. |

## Running it

| Page | What it covers |
|---|---|
| [Deployment](deployment.md) | The options and how to choose, a side-by-side comparison, what every AWS deployment shares (regions, table prefix, users, credentials, tables, IAM), moving between options, and the local stack. |
| [AWS serverless](deployment-serverless.md) | `deploy/aws-lambda`: the API on Lambda behind a private API Gateway, with Lambda stream consumers. |
| [AWS containers on ECS Fargate](deployment-ecs.md) | `deploy/aws-ecs`: chserver behind an internal load balancer, the mvworker service and the replicator. |
| [Self-managed containers](deployment-self-managed.md) | The image on any scheduler: processes, health checks, shutdown, scaling, and hybrid deployments. |
| [Costs, limits and operations](operations.md) | DynamoDB write costs per source row, item and key limits, and what to monitor. |

## Working on it

| Page | What it covers |
|---|---|
| [Testing](testing.md) | What the suites verify, the local gate (`make verify`), and how to run the unit, integration, e2e, compat, multiregion and deployment tests. |
| [Limitations and roadmap](roadmap.md) | Known gaps, roughly in priority order, and how to contribute. |
| [DESIGN.md](../DESIGN.md) | The detailed contract: physical layout, encodings, write conditions, reset epochs, stream handling and error classification. |

The deployment templates have their own READMEs with every parameter:
[deploy/aws-lambda](../deploy/aws-lambda/README.md) and
[deploy/aws-ecs](../deploy/aws-ecs/README.md).
