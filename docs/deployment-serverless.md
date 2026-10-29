# AWS serverless (`deploy/aws-lambda`)

Part of [Deployment](deployment.md). The regions, table prefix, users,
credentials and IAM notes in
[Common to every AWS deployment](deployment.md#common-to-every-aws-deployment)
apply here too.

[deploy/aws-lambda](../deploy/aws-lambda/README.md) is an AWS SAM template
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

## What it creates

The four functions (three with `GlobalMode=none`: no replicator), one
scoped role per function, the private REST API with a resource policy that
allows only `VpcEndpointId`, the reconciler's schedule and the log groups.
By default it also creates the region's API Gateway CloudWatch Logs role
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

## Prerequisites

- An interface VPC endpoint for `execute-api` in every region.
- The SAM CLI.
- Go, for `sam build`.

## Quick start

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

## Limits

Detailed in
[deploy/aws-lambda/README.md](../deploy/aws-lambda/README.md#limits-of-the-lambda-deployment):

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

## Permissions

The template gives each function only the calls it makes in Lambda mode:

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

## Upgrade

`make aws-build`, then `make aws-deploy ENV=us` and `ENV=eu`. Deploy the
home region first when a release changes the table layout or the catalog
format. The API publishes a new version behind its `live` alias. To roll
back, deploy the previous build.

## Teardown

`sam delete --config-env eu`, then `--config-env us`, run in
`deploy/aws-lambda`. This removes the functions, the API and the roles.
With `ManageApiGatewayAccount=true` (the default) one of those roles is
the region's API Gateway CloudWatch Logs role, so other REST APIs of the
region that log through it lose their logging; set `cloudwatchRoleArn`
again (or hand the setting to another owner first). It leaves the tables
and the reconciler-created event-source mappings; delete those
deliberately. To remove only one region, see
[deploy/aws-lambda/README.md](../deploy/aws-lambda/README.md#rolling-out-a-new-version).

[deploy/aws-lambda/README.md](../deploy/aws-lambda/README.md) has every
parameter, how to point clickhouse-go and curl at the stage URL, the
`ChUsers` and KMS handling, operations, scaling, cost notes and the
local `sam local` runs.
