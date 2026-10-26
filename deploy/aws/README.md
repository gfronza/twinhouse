# Serverless deployment on AWS (SAM)

This directory deploys dynamodb-eventstream fully serverless, one
CloudFormation stack per region:

```
                    VPC (your application)
                       │ https, through the execute-api interface endpoint
          ┌────────────▼─────────────┐
          │ private REST API Gateway │  ANY / and /{proxy+}, BinaryMediaTypes */*
          └────────────┬─────────────┘
                       │ Lambda proxy integration
          ┌────────────▼─────────────┐
          │ ApiFunction              │  cmd/chserver-lambda: the ClickHouse HTTP API
          └────────────┬─────────────┘
                       │ DynamoDB (tables created at runtime, {prefix}*)
   ┌───────────────────┼──────────────────────────────────────────────┐
   │  global tables: events, __catalog      regional: rollups, markers │
   └───────┬───────────────────┬───────────────────────────┬───────────┘
           │ streams           │ __catalog stream           │ ReplacingMergeTree streams
  ┌────────▼─────────┐ ┌───────▼────────────┐   ┌──────────▼──────────┐
  │ MvWorkerFunction │ │ ReconcilerFunction │   │ ReplicatorFunction  │
  │ cmd/mvworker     │ │ cmd/reconciler-    │   │ cmd/replicator      │
  │ materialized     │ │ lambda: tables +   │   │ copies to the other │
  │ views            │ │ event-source maps  │   │ regions (aws mode)  │
  └──────────────────┘ └────────▲───────────┘   └─────────────────────┘
                                │ EventBridge rate(1 minute)
```

* `template.yaml` declares the four functions, their scoped roles (one
  inline policy per function, actions limited to `table/<prefix>*`; SAM
  also attaches `AWSLambdaBasicExecutionRole`, CloudWatch Logs on `*`, to
  every generated role), the private REST API, the reconciler schedule and
  the log groups. It declares **no DynamoDB tables**: the reconciler
  creates the system tables and converges every table (replicas included),
  the API creates the table of a `CREATE TABLE` at runtime exactly as the
  long-running `cmd/chserver` does (DESIGN.md §2, §4), so the same stack
  works for any schema.
* The event-source mappings that feed the stream worker and the replicator
  are created by the reconciler as tables appear (`internal/esm`), and
  removed when tables are dropped (also when the drop happened while no
  reconciler was running: every round lists the functions' mappings and
  the region's physical tables and removes those no catalog entry names).
  The reconciler wires the `{prefix}__catalog` stream to itself, so a DDL
  in any region reconciles within seconds; the one-minute schedule is the
  safety net.
* `samconfig.toml` has the `us` (us-east-1, home region) and `eu`
  (eu-west-1) environments. Add a `[xx.deploy.parameters]` section for
  another region and list it in `ReplicaRegions` of every stack.
* The local Docker stack (`deploy/local`) is unchanged and remains the
  development environment; `make aws-local-api` runs the API Lambda locally
  against it.

## Prerequisites

* AWS SAM CLI (1.160+), Go 1.27 (the functions are cross-compiled to
  `linux/arm64` by `sam build`), Docker only for `make aws-local-api`.
* Credentials for the target account with rights to create IAM roles,
  Lambda functions, API Gateway, EventBridge rules and log groups
  (`CAPABILITY_IAM`).
* In **every region**, a VPC with an **interface VPC endpoint for
  `com.amazonaws.<region>.execute-api`** whose security group allows HTTPS
  from your clients. Note its id (`vpce-...`): it is the `VpcEndpointId`
  parameter and the only source the API's resource policy allows. Enable
  *private DNS* on the endpoint if you want to call the plain invoke URL;
  otherwise use the `ApiVpceEndpoint` output (see "Pointing a client").
* Decide the table prefix (`TablePrefix`, default `chddb_`): the IAM
  policies are scoped to `table/<prefix>*`, so one account can host several
  deployments with different prefixes. The prefixes must not be prefixes
  of one another (`chddb_` and `chddb_prod_`): the policies of the shorter
  one would cover the other's tables, and its reconciler, which drops
  physical tables of its deployment that no catalog entry names, could
  mistake the other's tables for its own.
* API Gateway access logging needs the account's **per-region CloudWatch
  Logs role** (`AWS::ApiGateway::Account`, shared by every REST API of the
  region); without it the stage fails to deploy. The stack creates the
  role and the setting (`ManageApiGatewayAccount=true`, the default). Set
  it to `false` when another stack or an operator already owns the
  setting (`aws apigateway get-account` shows `cloudwatchRoleArn`).

## Deploying two regions

The home region's stack must be up **before** any other region: with
`GLOBAL_MODE=aws` only the home region creates `__catalog` and the other
global tables and adds their replicas; the other regions wait for the
replica to appear (DESIGN.md §6). AWS refuses to add a replica where a table
of the same name already exists, so deploying a replica region first would
have to be undone by deleting its tables.

Every parameter of a stack lives in its `[us.deploy.parameters]` /
`[eu.deploy.parameters]` section of `samconfig.toml`. Before the first
deploy replace the `VpcEndpointId=vpce-CHANGE_ME` placeholder of each
environment with the region's execute-api endpoint id (CloudFormation
rejects the placeholder, so a forgotten one cannot deploy).

```sh
make aws-validate                      # sam validate --lint, no credentials needed
make aws-build                         # four arm64 bootstraps under deploy/aws/.aws-sam/

# 1. home region
make aws-deploy ENV=us
# wait until the first reconciler run created chddb___catalog (CloudWatch logs
# of dynamodb-eventstream-reconciler, or `aws dynamodb describe-table`)

# 2. replica region
make aws-deploy ENV=eu
```

`sam deploy` shows the change set and asks for confirmation
(`confirm_changeset = true`). **Do not pass `--parameter-overrides` on the
command line**: the SAM CLI does not merge it with the file, it *replaces*
the environment's whole list for that run, so every parameter left out
silently falls back to the template default on a first deploy (`ChUsers`
would become the password-less `default:`, `TablePrefix` `chddb_`) or
keeps the stack's previous value on an update; `make aws-deploy` refuses
it. Values that must not be committed, `ChUsers` above all, go into a
git-ignored copy of the file with the full parameter lists:

```sh
cp deploy/aws/samconfig.toml deploy/aws/samconfig.local.toml   # git-ignored
# add "ChUsers=analytics:<password>" to both parameter lists (or let CI write the file from a secret)
make aws-deploy ENV=us SAM_CONFIG=samconfig.local.toml
```

`ChUsers` is `NoEcho`, which only masks it in CloudFormation's output and
change sets: the resolved value is in the API function's environment,
readable by every principal allowed `lambda:GetFunctionConfiguration`.
To restrict that, create a KMS key whose policy grants `kms:Decrypt` only
to the function's role and the operators, and set `KmsKeyArn` to it (the
function's environment is then encrypted with that key).

What the first reconciler invocation does in the home region:
`EnsureSystemTables` creates `__catalog` (a global table with replicas in
`ReplicaRegions`), `__mv_markers`, `__checkpoints`; then it creates the
event-source mapping from the `__catalog` stream to itself. In a replica
region `EnsureSystemTables` first waits for the `__catalog` replica to
appear (an invocation that runs out of time fails and the next schedule
tick retries), then creates the regional `__mv_markers` and
`__checkpoints` and wires the replica's own `__catalog` stream to that
region's reconciler. The API only checks that the system tables exist
(`UNAVAILABLE` until the reconciler created them). Tables you create through the API follow the
same path (DESIGN.md §2 "reconciler"): the request creates the table in
its region and returns; the reconciler adds the replicas.

### Parameters you are likely to change

| Parameter | Default | Notes |
|---|---|---|
| `TablePrefix` | `chddb_` | Physical table prefix; also the IAM scope. |
| `GlobalMode` | `aws` | `none` for a single region (no replicator, no replicas). |
| `ReplicaRegions` | `us-east-1,eu-west-1` | All regions, this one included; identical in every stack. |
| `HomeRegion` | `us-east-1` | Creates the global tables; deploy first. |
| `VpcEndpointId` | — | execute-api interface endpoint of this region's VPC; replace the `vpce-CHANGE_ME` placeholder in `samconfig.toml`. |
| `ApiStageName` | `v1` | Stage, and therefore the URL path prefix. |
| `ChUsers` | `default:` | `user:password,...`. NoEcho masks CloudFormation output only; the value is readable in the function's environment, see above. Never commit it. |
| `KmsKeyArn` | — | Optional customer managed key encrypting the API function's environment (`ChUsers`). |
| `ManageApiGatewayAccount` | `true` | Create the region's API Gateway CloudWatch Logs role and account setting (needed by access logging; a per-region singleton). |
| `CatalogRefresh` | `5s` | Max catalog snapshot age before a request reloads it. |
| `ApiTimeoutSeconds` / `ApiIntegrationTimeoutMillis` | 29 / 29000 | See "Limits". |
| `MaxExecutionTime` | `25` | Default `max_execution_time`; keep below the integration timeout. |
| `*MemorySize`, `*TimeoutSeconds` | 1024 MB, 120 s (reconciler 512 MB, 300 s) | Per function. |
| `ApiReservedConcurrency`, `ApiProvisionedConcurrency` | 0 | Optional throttling / warm capacity for the API. |
| `ReconcilerReservedConcurrency` | 1 | Serializes reconciles in a region (a scheduled tick during a long round is throttled and dropped, see Operations). Reserved concurrency needs the account to keep 100 unreserved executions; in an account with a small Lambda concurrency quota set it to 0. |
| `LogRetentionDays` | 30 | All log groups. |

## Pointing a client at the API

The API is reachable only from the VPC through the endpoint. Two URLs work:

* `ApiEndpoint` output, `https://<api-id>.execute-api.<region>.amazonaws.com/v1`,
  when the VPC endpoint has private DNS enabled;
* `ApiVpceEndpoint` output, `https://<api-id>-<vpce-id>.execute-api.<region>.amazonaws.com/v1`,
  which always resolves (to the endpoint's private IPs) and needs no private
  DNS.

The stage name is a path prefix, so a client needs the host **and** the path
`/v1`; everything else is the ClickHouse HTTP protocol (DESIGN.md §2).

clickhouse-go v2 over HTTP:

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Protocol: clickhouse.HTTP,
    Addr:     []string{"abc123def4-vpce-0aaaaaaaaaaaaaaaa.execute-api.us-east-1.amazonaws.com:443"},
    TLS:      &tls.Config{},
    HttpUrlPath: "v1",
    Auth:     clickhouse.Auth{Username: "default", Password: ""},
    Compression: &clickhouse.Compression{Method: clickhouse.CompressionZSTD},
    // Lambda has no process that outlives a request: sessions are rejected
    // (see Limits), so do not set HttpSession / session_id.
})
```

curl:

```sh
API=https://abc123def4-vpce-0aaaaaaaaaaaaaaaa.execute-api.us-east-1.amazonaws.com/v1
curl "$API/ping"
curl "$API/" --data-binary 'CREATE TABLE events (ts DateTime, user String, v UInt64) ENGINE = MergeTree ORDER BY (user, ts)'
curl "$API/?query=INSERT%20INTO%20events%20FORMAT%20JSONEachRow" --data-binary '{"ts":"2026-10-25 12:00:00","user":"a","v":1}'
curl "$API/?query=SELECT%20count()%20FROM%20events"
```

To drop the `/v1` prefix (host/port-only switch from ClickHouse), map a
[private custom domain name](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-private-custom-domains.html)
to the stage with an empty base path and point clients at that domain.

Binary formats (`Native`, `compress=1`, compressed request bodies) pass
through API Gateway unchanged: the API has `BinaryMediaTypes: */*`, so
every request body reaches the function base64-encoded and the adapter
base64-encodes responses when needed.

Two API Gateway REST behaviours to keep in mind on the client side:

* **`Accept` header.** API Gateway decodes a base64 response for the client
  only when the request's `Accept` header matches a binary media type.
  curl and Python `requests` send `Accept: */*`; Go's `net/http` and
  clickhouse-go send no `Accept` at all. `sam local` then returns the
  base64 text as the body; a real REST API with `BinaryMediaTypes: */*` is
  believed to treat a missing `Accept` as `*/*`, but this has not been
  verified on a deployment. Send `Accept: */*`
  from Go clients: `HttpHeaders: map[string]string{"Accept": "*/*"}` in
  `clickhouse.Options` (the e2e suite does this in Lambda mode). If a real
  deployment still returns base64 text, set
  `contentHandling: CONVERT_TO_BINARY` on the integration through an
  OpenAPI `DefinitionBody`.
* **`+` in query strings.** REST APIs pass `+` through as a literal plus
  sign, not a space. Clients that put the query in the URL encoded with
  `url.Values.Encode` (Go) or `application/x-www-form-urlencoded` rules may
  see `SELECT+1` arrive as is; encode spaces as `%20` in URL queries, or send
  the query in the POST body (clickhouse-go does, so it is unaffected).

## Limits of the Lambda deployment

* **Payloads: 6 MB** per request and per response (Lambda synchronous
  invocation payload; API Gateway REST allows 10 MB but the function's
  limit is lower; base64 encoding of binary bodies costs a further ~33%).
  Split large INSERTs into several requests (clickhouse-go batches of a few
  thousand rows are fine) and page large SELECTs with `LIMIT`/`OFFSET`
  or a narrower `WHERE`. Streaming results do not shrink the limit: the
  whole response is buffered.
* **No HTTP sessions**: a request carrying `session_id`, `session_check`
  or `session_timeout` fails with `NOT_IMPLEMENTED` (48) and the message
  `sessions are not supported by this deployment`. Use per-query `SETTINGS`
  or URL settings instead of `SET`, and `database=` or
  `X-ClickHouse-Database` instead of `USE`.
* **Timeouts**: API Gateway's integration timeout is 29 s by default
  (`ApiIntegrationTimeoutMillis`; higher values need a REST API quota
  increase in the account). A slower query is answered by API Gateway with
  a 504 and no ClickHouse error body; `MaxExecutionTime` (default 25 s)
  makes the server return `TIMEOUT_EXCEEDED` (159) first. The Lambda
  timeout (`ApiTimeoutSeconds`) bounds the function itself.
* **Cold starts**: a cold API invocation pays the Lambda environment start
  (a static Go binary of about 27 MB on arm64; expect some hundreds of
  milliseconds, not measured on AWS yet), plus the first request of a new
  execution environment checks the system tables exist (one
  `DescribeTable` each; it never creates or waits for a table, that is the
  reconciler's job) and loads the catalog (a consistent `Scan` of
  `__catalog`). `ApiProvisionedConcurrency` keeps environments warm
  (hourly cost).
* **DDL through the API is asynchronous where AWS is slow.** The API
  Gateway timeout (29 s) cannot cover adding a global-table replica or
  deleting a replicated table (minutes), so the API function runs DDL
  without waiting for those (`dynamo.Options.AsyncDDL`, DESIGN.md §2):
  `CREATE TABLE` returns once the table is `ACTIVE` in the issuing region
  (a few seconds; `INSERT`s there work immediately) and the home region's
  reconciler adds the replicas in `ReplicaRegions`, so queries in another
  region may answer `UNKNOWN_TABLE` or a transient error for a few
  minutes; `DROP TABLE` removes the catalog entry and returns (a table
  without replicas is deleted right away, not awaited), and the
  reconcilers drop the physical tables, replicas and event-source mappings
  within about two rounds (two minutes). A dropped table keeps billing
  storage until then. The reconciler, not the DDL request, also creates
  the physical tables in the non-issuing regions.
* **24 hour deletion window (GLOBAL_MODE=aws).** DynamoDB refuses to
  delete the source table of a global table within 24 hours of adding a
  replica. `DROP TABLE` still succeeds at once (the catalog entry is gone,
  the replicas are removed); the reconciler retries the deletion every
  minute (logged at debug level) and completes it when AWS allows it, so
  `CREATE`/`DROP` cycles against a real deployment leave the home-region
  table, billed for its storage, for up to a day. The per-incarnation
  physical names make a new `CREATE TABLE` of the same name independent of
  it.
* **Catalog freshness**: the API reloads its catalog snapshot at most every
  `CatalogRefresh` per execution environment, so a `CREATE TABLE` in one
  region is visible to queries in another region after replication of
  `__catalog` plus up to `CatalogRefresh`; the same holds for the stream
  worker and the replicator per invocation.
* **Private only**: there is no public endpoint and no authorizer beyond
  ClickHouse's own user/password (`ChUsers`); the VPC endpoint's security
  group is the network boundary.

## Operations

* **Logs**: `/aws/lambda/<stack>-api|-mvworker|-replicator|-reconciler`
  (JSON lines, `LogLevel`) and `/aws/apigateway/<stack>-api` (access log).
  Watch for `DEAD LETTER` (a poison record written to `{prefix}__dead_letters`)
  and `DATA LOSS` (a stream read past its 24 h retention), DESIGN.md §5.
* **Metrics to alert on**: Lambda `Errors` of every function and
  `Throttles` of the API, the stream worker and the replicator (the
  reconciler's `Throttles` are expected, see Scaling); `IteratorAge` of the
  stream worker and the replicator (records older than a few minutes mean
  views or replication are lagging; the stream is trimmed after 24 h); API
  Gateway `5XXError` and `Latency`; DynamoDB `ThrottledRequests` and
  `ReplicationLatency` of the global tables.
* **Event-source mappings** are not stack resources. List them with
  `aws lambda list-event-source-mappings --function-name <stack>-mvworker`
  (or `-replicator`); a mapping in state `Disabled`/`Problem` is
  re-converged by the next reconcile *of a new execution environment or a
  changed table definition* (a mapping deleted by hand while an
  environment stays warm comes back on its next cold start). Dropping a
  table removes its mappings and physical table within about two rounds,
  also when the drop happened while the region had no warm reconciler:
  every round the reconciler lists the two functions' mappings and the
  region's `<prefix>` tables and removes those that no catalog entry names
  (after the usual re-read of the catalog). The only orphans it cannot see
  are tables of older builds without the per-incarnation name suffix that
  it never observed in the catalog; `aws dynamodb list-tables` plus a look
  at `__catalog` finds them.
* **Scaling**: Lambda scales the stream worker and the replicator per
  shard (one concurrent invocation per shard, in order); the API scales with
  requests up to the account's concurrency or `ApiReservedConcurrency`.
  Retries: `MaximumRetryAttempts -1` on the mappings, i.e. a failing batch is
  retried until the records expire, with `ReportBatchItemFailures` so only
  the records from the first failure on are retried (exactly-once markers
  make re-application a no-op). The reconciler runs with reserved
  concurrency 1 (`ReconcilerReservedConcurrency`), so a catalog-stream
  invocation and a scheduled one never overlap in a region; a scheduled
  tick that lands during a long round (creating a global table waits
  minutes for its replicas) is throttled and, by the function's
  `EventInvokeConfig` (no retries, 60 s maximum age), dropped rather than
  queued: the next tick comes a minute later and every round is
  idempotent. Those throttles are expected and show up in the reconciler's
  `Throttles` metric during such DDL.

## Cost notes (on-demand, rough)

* **API Gateway (private REST)**: per million requests plus data transfer,
  and the interface endpoint's hourly charge per AZ plus per GB processed.
  Every ClickHouse HTTP request is one API request; a `SELECT` with a 10 KB
  result is dominated by the request charge.
* **Lambda**: requests and GB-seconds (arm64). The API function bills for
  the query time, the stream worker for the view application time per
  batch (`EsmBatchSize` 100 records); DynamoDB stream reads by Lambda
  event-source mappings are not charged by DynamoDB.
* **Reconciler**: 1 scheduled run per minute per region (about 44k
  invocations per month, well inside the free tier) plus one per DDL. Each
  run does a consistent `Scan` of `__catalog` and a `DescribeTable` per
  table, which is negligible unless the catalog has thousands of tables.
* **DynamoDB**: on-demand reads/writes as in the local design (DESIGN.md §4
  "Cost model"); a global table charges replicated write units (rWCU) in
  every replica region, and streams of every source table are enabled
  (`NEW_AND_OLD_IMAGES`). ReplacingMergeTree tables are copied by the
  replicator instead: a write in region A costs one conditional `PutItem`
  per other region.
* **CloudWatch Logs**: ingestion is the dominant log cost at `LogLevel`
  `debug`; keep `info` in production and tune `LogRetentionDays`.

## Rolling out a new version

```sh
make aws-build
make aws-deploy ENV=us        # review the change set, confirm
make aws-deploy ENV=eu
```

`sam build` runs with its cache disabled (`cached = false` in
`samconfig.toml`): the Go builder hashes only the function's `CodeUri`
(`cmd/<fn>`), so a change under `internal/` or in `go.mod` would otherwise
reuse the previous bootstrap and ship stale code. Every build compiles the
four binaries (a few seconds each).

A deploy replaces the function code of all four functions in that region;
the API function publishes a new version behind the `live` alias
(`AutoPublishAlias`), so provisioned concurrency, when enabled, follows the
new version. Deploy the home region first when a release changes the table
layout or the catalog format, then the others; the binaries of two
consecutive releases are expected to coexist during a rollout (one region
at a time). DynamoDB tables and their data are never touched by a deploy.

To **roll back**, deploy the previous build: check out the previous
commit, `make aws-build`, `make aws-deploy ENV=...`. CloudFormation rolls a
failed deployment back by itself (`disable_rollback = false`). Event-source
mappings are left in place by both.

To **remove a region**: `sam delete --config-env eu` deletes the functions,
the API and the roles. CloudFormation owns no table resources, so stack
deletion never touches DynamoDB: it leaves the tables (and the global-table
replicas) and the reconciler-created event-source mappings; remove the
mappings with `aws lambda delete-event-source-mapping --uuid ...` if the
function deletion did not, and delete or detach the tables deliberately.
The only principals of this template that can delete tables are the API
and reconciler function roles (`dynamodb:DeleteTable` on
`table/<prefix>*` in every region, needed by `DROP TABLE` and replica
removal); a compromised API function could therefore drop this
deployment's tables, which is what `DROP TABLE` does anyway.

## Local run of the API Lambda

With the local stack up (`make up`), `make aws-local-api` builds the
functions, runs one reconciler round from the host (`go run
./cmd/reconciler-lambda -once`, which creates the system tables: the API
Lambda only checks they exist) and starts `sam local start-api` on port
3000 against the `us` DynamoDB Local container of the `dhdev` compose
project (network `dhdev_default`, `GLOBAL_MODE=emulated`, event-source
mapping management disabled). It uses the built template
(`.aws-sam/build`), the parameter overrides from `samconfig.toml`, the
environment file `local-env.json` (`SAM_LOCAL_ENV` to use another, e.g.
with a different `TABLE_PREFIX`) and dummy credentials, so no AWS account
is involved. (`sam local` only
overrides environment variables the template declares, which is why
`DYNAMODB_ENDPOINT` exists in the template as the `DynamoDBEndpoint`
parameter, absent on AWS, and is set in both places locally.)

```sh
make up
make aws-local-api
curl 'http://127.0.0.1:3000/?query=SELECT%20version()'
curl 'http://127.0.0.1:3000/?session_id=x&query=SELECT%201'   # NOT_IMPLEMENTED (48)
```

`sam local` emulates the Lambda proxy integration, not API Gateway's
limits or the resource policy, and it runs only the API function: there is
no stream worker (inline views still apply the rollups) and no reconciler.
`make aws-local-e2e` (in another terminal) runs `tests/e2e` against the
emulated API; the suite recognises Lambda mode by the `NOT_IMPLEMENTED`
answer to `session_id` (or `E2E_LAMBDA=1`), sends `Accept: */*`, and skips
the session test. The other functions can be exercised with
`sam local invoke` against the same DynamoDB Local, for example

```sh
cd deploy/aws
sam local generate-event cloudwatch scheduled-event > /tmp/schedule.json
sam local invoke ReconcilerFunction -t .aws-sam/build/template.yaml \
  --docker-network dhdev_default --env-vars local-env.json -e /tmp/schedule.json
```

and `MvWorkerFunction` with a DynamoDB stream event whose records carry the
real item shape (`sam local generate-event dynamodb update` as a starting
point, `eventSourceARN` naming the physical table); replaying records the
inline views already applied is a no-op thanks to the exactly-once markers.

## Verification without an AWS account

`go test ./deploy/aws/` checks the template against the contract the
binaries implement (functions and their `cmd` directories, every environment
variable set here is read by some binary, IAM scoped to `TablePrefix`,
private endpoint, schedule, outputs) and runs `sam validate --lint` and
`docker compose config` when those tools are installed. `make aws-build`
cross-compiles the four bootstraps and the test checks they are 64-bit
AArch64 ELF binaries.

The functions themselves are tested without AWS too: `cmd/chserver-lambda`
drives the real server with synthetic API Gateway proxy events and
compares every response byte for byte with plain HTTP (part of `go test
./...`); `go test -tags samlocal ./cmd/chserver-lambda/` runs the same
kind of checks through `sam local start-api` (API Gateway's base64
handling, compression, error headers); `make aws-local-e2e` runs the whole
e2e suite against the emulated API; `cmd/reconciler-lambda` and
`internal/esm` use a fake Lambda API. What `sam local` cannot emulate, and
therefore remains unverified until a real deployment: API Gateway's
treatment of a missing `Accept` header, `+` in query strings, the 6 MB
payload limit, the resource policy, real cold-start and DynamoDB
latencies.
