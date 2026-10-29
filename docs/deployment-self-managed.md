# Self-managed containers

Part of [Deployment](deployment.md). The regions, table prefix, users,
credentials and IAM notes in
[Common to every AWS deployment](deployment.md#common-to-every-aws-deployment)
apply here too.

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

## Processes

Run the following per region, plus one replicator for the whole
deployment:

| Process | How many | Required settings | Often set |
|---|---|---|---|
| `chserver` | One or more per region, behind a load balancer | `REGION` (or `AWS_REGION`), `TABLE_PREFIX`, `GLOBAL_MODE`; with `aws` also `REPLICA_REGIONS` and `GLOBAL_HOME_REGION`; `CH_USERS` from your secret store | `MAX_EXECUTION_TIME`, `INLINE_VIEWS`, `RUN_STREAM_WORKER`, `CATALOG_REFRESH`, `LOG_LEVEL`, `LISTEN_ADDR` (default `:8123`) |
| `mvworker` | One or more per region, unless chserver runs the worker in-process (`RUN_STREAM_WORKER=true`) | The same `REGION`, `TABLE_PREFIX`, `GLOBAL_MODE`, `REPLICA_REGIONS` and `GLOBAL_HOME_REGION` as chserver | `MV_POISON_POLICY`, `STREAM_LEASE_DURATION`, `STATS_INTERVAL` |
| `replicator` | Once per deployment, typically in the home region; only with `GLOBAL_MODE=aws` | `GLOBAL_MODE=aws`, `REGIONS=us-east-1=,eu-west-1=` (every region; empty endpoints mean real AWS), `TABLE_PREFIX` | `APPLY_CONFLICT_VIEWS` (default `true`), `MV_POISON_POLICY`, `STREAM_LEASE_DURATION` |

Keep `STREAM_VIEWS=true` (the default) on chserver whenever a worker runs.
Leave `DYNAMODB_ENDPOINT` unset. Give each process the permissions of
[IAM permissions](deployment.md#iam-permissions).

## Why the replicator is needed

In AWS mode it copies only the `ReplacingMergeTree` tables. AWS's
item-level last-writer-wins could let a lower version written later win,
and it would silently drop the losing version of a concurrent write, which
then never reaches that region's views. The replicator uses a
version-aware conditional put and applies the views to the versions it
rejects. Without it, `ReplacingMergeTree` tables stay regional. It needs
network access to DynamoDB in every listed region.

## Health checks

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

## Graceful shutdown

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

## Scaling

- **chserver** is stateless apart from HTTP sessions. Several instances
  per region are fine, and each runs its own reconciler.
- **mvworker** instances share the streams' shards through leases in
  `{prefix}__checkpoints`. More instances help with many source tables or
  shards, and a second instance gives fail-over within the lease
  duration.
- **The replicator** runs once per deployment. Several instances are safe,
  because shard leases keep them from conflicting, but each one tails
  every region.

## Hybrid deployments

The binaries support these combinations, but neither template deploys
them; you assemble them from the pieces above and in
[AWS serverless](deployment-serverless.md).

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
  [IAM permissions](deployment.md#iam-permissions), which the ECS template does not
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
