# Configuration

Every binary reads environment variables (`internal/config`; durations
use Go syntax such as `500ms` or `2s`). The deployment templates set them
from stack parameters; [Self-managed containers](deployment-self-managed.md)
lists the ones each process needs.

## Common (every binary, the Lambda functions included)

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

## chserver

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
[deploy/aws-lambda](../deploy/aws-lambda/README.md)) reads the same variables except
`LISTEN_ADDR` and `RUN_STREAM_WORKER`; it rejects HTTP sessions
(`session_id`) with `NOT_IMPLEMENTED` (48), reloads the catalog at the
start of a request when the snapshot is older than `CATALOG_REFRESH`, and
is bound by the Lambda payload limit (6 MB per request and response).

## reconciler (chserver, reconciler-lambda)

The reconciler creates and drops this region's physical tables from the
catalog; it runs inside `chserver` and, in the serverless deployment, as
`cmd/reconciler-lambda` (invoked by the `{prefix}__catalog` stream and by
an EventBridge schedule). With `MVWORKER_FUNCTION_ARN` set it also manages
the Lambda event-source mappings of the table streams
([DESIGN.md §5](../DESIGN.md)).

| Variable | Default | Meaning |
|---|---|---|
| `MVWORKER_FUNCTION_ARN` | | Lambda function (ARN or name) wired to the stream of every source table (MergeTree, ReplacingMergeTree). Empty disables event-source-mapping management (local stack, `sam local`). Rejected together with `DYNAMODB_ENDPOINT`. |
| `REPLICATOR_FUNCTION_ARN` | | (`GLOBAL_MODE=aws`) Lambda wired to the streams of ReplacingMergeTree tables. Needs `MVWORKER_FUNCTION_ARN`. |
| `RECONCILER_FUNCTION_ARN` | `AWS_LAMBDA_FUNCTION_NAME` | The function the `{prefix}__catalog` stream is wired to (the reconciler itself). |
| `ESM_BATCH_SIZE` | `100` | Batch size of the event-source mappings it creates (`TRIM_HORIZON`, `ReportBatchItemFailures`, unlimited retries). |
| `RECONCILER_DEADLINE_MARGIN` | `5s` | (reconciler-lambda) Time kept free before the invocation deadline. |

## mvworker (and chserver's in-process worker where noted)

| Variable | Default | Meaning |
|---|---|---|
| `MVWORKER_OWNER` | hostname:pid | Shard-lease owner id. |
| `STREAM_LEASE_DURATION` | `30s` | Shard lease TTL. Several workers per region share shards safely. |
| `STATS_INTERVAL` | `30s` | Progress log interval (`0` disables). Logs at ERROR when a shard has been stuck for over 1 h. |
| `MV_POISON_POLICY` | `deadletter` | A record whose view application keeps failing with a deterministic error is written to `{prefix}__dead_letters` and skipped (`deadletter`), or retried forever (`block`). Also used by the in-process worker and the replicator. |
| `MV_POISON_AFTER` | `30s` | How long such a failure must persist before it is dead-lettered. |
| `CATALOG_MIN_RELOAD` | `1s` | Minimum spacing of catalog reloads triggered by new rows. |
| `AWS_LAMBDA_RUNTIME_API` / `-lambda` | | Run as an AWS Lambda DynamoDB-stream handler. |

## replicator

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
