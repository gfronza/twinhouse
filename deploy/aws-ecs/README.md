# Container deployment on AWS (ECS Fargate)

This directory deploys the long-running binaries of twinhouse on
Amazon ECS Fargate, one plain CloudFormation stack per region, into an
existing VPC:

```
              application VPC (region A)                         (same stack in region B,
                     │ http :8123 (or https :8443)                 without the replicator)
       ┌─────────────▼───────────────┐
       │ internal Application LB     │  health check GET /ping
       └─────────────┬───────────────┘
                     │ :8123
       ┌─────────────▼───────────────┐
       │ chserver service (N tasks)  │  ClickHouse HTTP API, reconciler,
       │ cmd/chserver                │  optional in-process stream worker
       └─────────────┬───────────────┘
                     │ DynamoDB (tables created at runtime, {prefix}*)
   ┌─────────────────▼────────────────────────────────────────────────┐
   │ global: source tables, __catalog ◄────── replication ───────────┼──► region B
   │ regional: rollups, ReplacingMergeTree copies, __mv_markers, ...  │
   └───────┬────────────────────────────────────────┬─────────────────┘
           │ source-table streams                   │ ReplacingMergeTree streams (A and B)
  ┌────────▼───────────────┐            ┌───────────▼───────────────────┐
  │ mvworker service       │            │ replicator service (home      │
  │ cmd/mvworker, shard    │            │ region only): version-aware   │
  │ leases split the work  │            │ copies between all regions    │
  └────────────────────────┘            └───────────────────────────────┘
```

* `template.yaml` declares the ECS cluster (or uses `ExistingClusterName`),
  the internal Application Load Balancer with a listener on **port 8123**
  (ClickHouse's HTTP port, so a client switches by host and port only) and
  an optional HTTPS listener on **8443**, the target group (health check
  `/ping`), security groups, log groups, the task execution role, one task
  role per service and the three services. It has no SAM transform and no
  DynamoDB tables: chserver and the worker create the tables at runtime,
  exactly as in the local stack (DESIGN.md §2, §4, §6).
* `samconfig.toml` has the `us` (us-east-1, home region) and `eu`
  (eu-west-1) environments for `sam deploy`, which deploys plain
  CloudFormation templates too. Add a `[xx.deploy.parameters]` section for
  another region and list it in `ReplicaRegions` of every stack.
* `template_test.go` checks the template against the binaries
  (`go test ./deploy/aws-ecs/`), and `ecslocal_test.go` runs the rendered
  task definitions as local containers (see "Local check without AWS").

## Prerequisites

* AWS SAM CLI (for `sam deploy` and `sam validate --lint`), Docker with
  buildx, the AWS CLI for the image push and the checks below.
* Credentials for the target account with rights to create IAM roles, ECS
  clusters and services, load balancers, security groups and log groups
  (`CAPABILITY_IAM`).
* In **every region**, a VPC with **private subnets in at least two
  Availability Zones** for the load balancer and the tasks (they get no
  public IP), and a **NAT gateway** (or other internet egress) for them.
  The tasks call these AWS APIs over HTTPS:
  * DynamoDB Streams, read by the stream consumers in every region:
    mvworker (or chserver with `WorkerMode=in-process`) and the
    replicator. The DynamoDB gateway endpoint does not cover Streams
    (`streams.dynamodb.<region>.amazonaws.com`), and the binaries do not
    set a Streams endpoint, so Streams is reached through NAT. Without it
    the consumers cannot read the streams, and no materialized view is
    applied in that region.
  * DynamoDB in this region, and in the **other** regions for the
    replicator and for the replica operations (VPC endpoints are
    regional, so those calls also go through NAT).
  * ECR, CloudWatch Logs and Secrets Manager.

  NAT covers all of these. Optionally, VPC endpoints keep part of the
  traffic off NAT (and its data charges): the DynamoDB gateway endpoint
  (`com.amazonaws.<region>.dynamodb`), the ECR interface endpoints
  (`ecr.api`, `ecr.dkr`) with the S3 gateway endpoint for the image
  layers, and the `logs` and `secretsmanager` interface endpoints. STS is
  not needed: Fargate tasks get their task role credentials from the
  container credentials endpoint.
* An **ECR repository** per region (or one with replication) holding the
  image built from `deploy/Dockerfile`, for the architecture of the
  `CpuArchitecture` parameter (see "Image").
* A **Secrets Manager secret** per region whose `SecretString` is the
  `CH_USERS` value, `user:password,user2:password2` (`default:` is the user
  `default` without a password). ECS injects it into chserver at task start
  (task definition `Secrets`); it never appears in the task definition, the
  stack parameters or the environment shown by the ECS console. Set
  `ChUsersSecretArn` to the secret's ARN only (full, or without the random
  suffix). For a JSON secret, put the key in `ChUsersSecretJsonKey`; the
  template builds the ECS `<arn>:<key>::` reference itself, and the
  execution role's grant stays on the secret's ARN (IAM does not match a
  key selector, so the parameter refuses one). Set `SecretKmsKeyArn` when
  the secret uses a customer managed key.

  ```sh
  aws secretsmanager create-secret --region us-east-1 --name twinhouse/ch-users \
    --secret-string 'analytics:<password>,default:'
  ```
* Decide the table prefix (`TablePrefix`, default `twinhouse_`): the task roles
  are scoped to `table/<prefix>*`, so one account can host several
  deployments with different prefixes. Prefixes must not be prefixes of one
  another (`twinhouse_` and `twinhouse_prod_`), as for the serverless deployment.

## Image

The image built from `deploy/Dockerfile` contains `chserver`, `mvworker`
and `replicator` (static Go binaries on Alpine, user `app`); each service
picks its binary with the container command. Build it for the platform of
`CpuArchitecture` (`ARM64` = `linux/arm64`, the default, Graviton;
`X86_64` = `linux/amd64`) and push it to ECR:

```sh
REPO=123456789012.dkr.ecr.us-east-1.amazonaws.com/twinhouse   # your repository
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin "${REPO%%/*}"
make image-push IMAGE_REPO=$REPO IMAGE_TAG=v1 PLATFORM=linux/arm64
# equivalent to:
docker buildx build --platform linux/arm64 -f deploy/Dockerfile -t $REPO:v1 --push .
```

`IMAGE_TAG` defaults to the short commit hash. Building for the other
architecture on the same machine works through buildx's emulation (QEMU or
Rosetta); `--platform linux/arm64,linux/amd64` pushes a multi-architecture
image that serves both parameter values. Push the same tag to every
region's repository (or use ECR replication), then set `ImageUri` to it.

## Deploying two regions

The home region's stack must be up **before** any other region: with
`GLOBAL_MODE=aws` only the home region creates `__catalog` and adds its
replicas, and the other regions wait at startup until their replica
exists (DESIGN.md §6). AWS refuses to add a replica where a table of that
name already exists, so a replica region deployed first would have to be
undone by deleting its tables.

The first chserver start in each region is slow. In the home region,
chserver creates `__catalog`, adds its replicas in the other regions and
waits until they are `ACTIVE` (minutes) before it listens; in another
region it waits for its replica. Health checks fail until then, so
`HealthCheckGracePeriod` defaults to 600 s, above the 300 s that chserver
waits for a table. If a first start still takes longer, ECS replaces the
task; the replica creation continues in AWS, and the replacement starts
quickly. Later starts do not wait.

The stack checks a few parameters before creating anything: with
`GlobalMode=aws`, `ReplicaRegions` must include the stack's region,
`HomeRegion` and at least one other region; `AllowedCidr` or
`ClientSecurityGroupId` must be set.

Fill in the `CHANGE_ME` values of both environments in `samconfig.toml`
(or in a git-ignored copy, `samconfig.local.toml`, deployed with
`ECS_CONFIG=samconfig.local.toml`): `ImageUri`, `VpcId`, `SubnetIds`,
`AllowedCidr` (or set it to `""` and add a
`"ClientSecurityGroupId=sg-..."` entry to the list, which has none by
default) and `ChUsersSecretArn`. CloudFormation rejects the placeholders, so a forgotten
one cannot deploy.

```sh
make ecs-validate                 # sam validate --lint, no credentials needed

# 1. home region: chserver, mvworker and the replicator
make ecs-deploy ENV=us
# wait until the services are stable and twinhouse___catalog has its replicas:
aws dynamodb describe-table --region us-east-1 --table-name twinhouse___catalog \
  --query 'Table.Replicas[].{region:RegionName,status:ReplicaStatus}'

# 2. replica region: chserver and mvworker
make ecs-deploy ENV=eu
```

`sam deploy` shows the change set and asks for confirmation
(`confirm_changeset = true`). As for the serverless deployment, **do not
pass `--parameter-overrides`** on the command line: the SAM CLI replaces
the environment's whole parameter list with it, so every parameter left
out takes the template default on the first deploy and keeps the stack's
previous value on an update, and the list in `samconfig.toml` is ignored
either way; `make ecs-deploy` refuses it. Without the SAM CLI,
`aws cloudformation deploy --template-file template.yaml
--capabilities CAPABILITY_IAM --parameter-overrides Key=Value ...` deploys
the same template. The template is under CloudFormation's 51,200-byte
inline limit, so `--s3-bucket` is optional; `samconfig.toml` uploads it to
S3 (`resolve_s3`) anyway, which leaves room for it to grow.

What starts in each region:

| Service | Home region | Other regions |
|---|---|---|
| chserver (`ChserverDesiredCount`, default 2) | creates `__catalog` (global, replicas in `ReplicaRegions`), `__mv_markers`, `__checkpoints`; reconciles every catalog entry | waits for the `__catalog` replica, creates the regional system tables, reconciles |
| mvworker (`WorkerMode=service`, `MvWorkerDesiredCount`, default 1) | applies this region's materialized views | same |
| replicator (`GlobalMode=aws`, `RunReplicator=auto`) | copies ReplacingMergeTree tables between all regions | not created |

Tables created with `CREATE TABLE` follow DESIGN.md §2: the region that
ran the statement creates the physical table and adds its replicas
(minutes for a global table), the other regions' reconcilers join it.

### Parameters

| Parameter | Default | Notes |
|---|---|---|
| `ImageUri` | — | ECR image (`...:tag` or `@sha256:...`) built from `deploy/Dockerfile`. A new value rolls out a new version. |
| `CpuArchitecture` | `ARM64` | `ARM64` or `X86_64`; the image must contain it. |
| `VpcId`, `SubnetIds` | — | Existing VPC and private subnets (two or more AZs), used by the load balancer and the tasks. |
| `AllowedCidr`, `ClientSecurityGroupId` | — | Clients allowed to reach the load balancer; set at least one. |
| `ExistingClusterName` | — | Run in an existing cluster instead of creating one. |
| `ContainerInsights` | `disabled` | `enabled` or `disabled`: Container Insights of the cluster the stack creates (ignored with `ExistingClusterName`). |
| `TablePrefix` | `twinhouse_` | `TABLE_PREFIX`; also the IAM scope. Same in every region. |
| `GlobalMode` | `aws` | `none` for a single region (no replicas, no replicator). |
| `ReplicaRegions` | `us-east-1,eu-west-1` | All regions (two or more), this one included; identical in every stack. `REPLICA_REGIONS` drops the current region; the replicator's `REGIONS` becomes `us-east-1=,eu-west-1=`. |
| `HomeRegion` | `us-east-1` | `GLOBAL_HOME_REGION`; deploy it first. |
| `ChUsersSecretArn` | — | ARN of the Secrets Manager secret holding `CH_USERS` (no `:<key>::` selector). |
| `ChUsersSecretJsonKey` | — | Key of a JSON secret whose value is `CH_USERS`. Empty: the whole secret string. |
| `SecretKmsKeyArn` | — | Customer managed key of that secret, if any. |
| `WorkerMode` | `service` | `service`: a separate mvworker service. `in-process`: `RUN_STREAM_WORKER=true` on every chserver task and no mvworker service (fewer tasks; the worker then scales with chserver). |
| `RunReplicator` | `auto` | `auto` runs it in `HomeRegion` only; `true`/`false` to choose explicitly. Run it in exactly one region (several instances are safe, shard leases keep them apart, but each one tails every region). |
| `InlineViews` | `true` | `INLINE_VIEWS`: read-your-writes for rollups in the writing region. `STREAM_VIEWS` is always `true` (a worker always runs). |
| `CatalogRefresh` | `2s` | `CATALOG_REFRESH`. |
| `MaxExecutionTime` | `100` | `MAX_EXECUTION_TIME` of chserver; keep it below `LoadBalancerIdleTimeout`. |
| `LoadBalancerIdleTimeout` | `110` | At most 115 s: it must stay below chserver's 120 s keep-alive idle timeout, or the load balancer may reuse a connection chserver just closed (sporadic 502s). |
| `DeregistrationDelay` | `110` | How long a stopping chserver task keeps serving in-flight requests before ECS sends SIGTERM; keep it at or above `MaxExecutionTime`. |
| `HealthCheckGracePeriod` | `600` | Seconds before health checks count for a new chserver task; covers the first start, which waits for the `__catalog` replicas. |
| `CertificateArn`, `KeepHttpListener` | —, `true` | ACM certificate for HTTPS on 8443; `KeepHttpListener=false` then removes the plain listener. |
| `ChserverCpu`/`ChserverMemory`, `MvWorkerCpu`/`MvWorkerMemory`, `ReplicatorCpu`/`ReplicatorMemory` | 1024/2048, 512/1024, 512/1024 | Fargate task sizes (valid CPU/memory pairs only). |
| `ChserverDesiredCount`, `MvWorkerDesiredCount`, `ReplicatorDesiredCount` | 2, 1, 1 | Task counts. |
| `ChserverMaxCount`, `ChserverTargetCpu` | 0, 60 | CPU target-tracking autoscaling of chserver between `ChserverDesiredCount` and `ChserverMaxCount` (0 disables it). |
| `MvPoisonPolicy` | `deadletter` | `MV_POISON_POLICY` of the worker and the replicator's conflict views. |
| `LogLevel`, `LogRetentionDays` | `info`, 30 | Logs go to `/ecs/<stack>/chserver`, `/mvworker`, `/replicator`. |

### Permissions

Each service has its own task role, scoped to `table/<prefix>*` (and its
`/index/*` and `/stream/*`); `go test ./deploy/aws-ecs/` checks it:

| Role | Grants | Why |
|---|---|---|
| chserver | item actions, `DescribeTable`, `CreateTable`, `UpdateTable`, `DeleteTable`, TTL actions in this region; `ListTables` (`*`, cannot be scoped); with `GlobalMode=aws` the replica actions (`CreateTableReplica`, `DeleteTableReplica` and the actions AWS requires in the replica regions, limited by `aws:RequestedRegion` to `ReplicaRegions`) and `iam:CreateServiceLinkedRole` for `replication.dynamodb.amazonaws.com`; stream reads with `WorkerMode=in-process` | queries and DDL, the reconciler (system tables, every catalog entry, orphan discovery), replicas of the global tables created in this region |
| mvworker | item actions, `DescribeTable`, `CreateTable`, `UpdateTable`, TTL actions and stream reads in this region; in the home region with `GlobalMode=aws` the replica actions on `__catalog` only | view application, `__checkpoints` leases, `__dead_letters`; `EnsureSystemTables` at startup may add the `__catalog` replicas |
| replicator | item actions, `DescribeTable`, `CreateTable` and stream reads in every region of `ReplicaRegions` | tails every region's ReplacingMergeTree streams, conditional copies, conflict views, leases and dead letters per region |
| task execution role | `AmazonECSTaskExecutionRolePolicy` (ECR pull, logs) and `secretsmanager:GetSecretValue` on the CH_USERS secret | used by ECS itself at task start; the binaries never see it |

No role has Lambda permissions: event-source mappings are a serverless
feature (`MVWORKER_FUNCTION_ARN` stays unset).

## Pointing a client at the service

The `ClickHouseUrl` output is `http://<internal-alb-dns>:8123`; it
resolves inside the VPC (and peered networks or VPNs with the VPC's DNS).
Use it like a ClickHouse server's HTTP interface:

```sh
URL=$(aws cloudformation describe-stacks --region us-east-1 --stack-name twinhouse-ecs \
  --query "Stacks[0].Outputs[?OutputKey=='ClickHouseUrl'].OutputValue" --output text)
curl -sS "$URL/ping"                                        # Ok.
curl -sS -u analytics:<password> "$URL" --data-binary 'SELECT version(), displayName()'
curl -sS -u analytics:<password> "$URL" --data-binary 'SHOW DATABASES'
```

With `CertificateArn` set and `KeepHttpListener=false` there is no plain
listener and no `ClickHouseUrl` output (the query above returns `None`):
use the `LoadBalancerDnsName` output, aliased by a name the certificate
covers, on port 8443 (see below).

clickhouse-go v2 with `Protocol: clickhouse.HTTP` and `Addr:
[]string{"<alb-dns>:8123"}` works unchanged. HTTP sessions (`session_id`)
live in the memory of one chserver task: with more than one task, every
request of a session must reach the same task, which the load balancer
only guarantees with target-group stickiness and a client that returns
its cookie (most ClickHouse clients do not). Prefer URL settings and
`database=` over sessions, or run one task. For HTTPS, set
`CertificateArn`, create a DNS alias the certificate covers for the load
balancer, and connect to `https://<that-name>:8443`
(`clickhouse.Options.TLS`).

## Operations

* **Logs**: JSON lines in `/ecs/<stack>/chserver|mvworker|replicator`.
  Watch the mvworker's `mvworker progress` lines (`lag_ms`,
  `pending_age_ms`), `SHARD WEDGED` / `DEAD LETTER` / `DATA LOSS`
  (DESIGN.md §5), and the replicator's `replicator progress` (`errors`).
  Stream records are trimmed after 24 h, so a worker that is down that long
  loses view updates.
* **Metrics to alert on**: ECS `CPUUtilization`/`MemoryUtilization` per
  service and `RunningTaskCount` below the desired count, the target group's
  `HTTPCode_Target_5XX_Count`, `UnHealthyHostCount` and
  `TargetResponseTime`, DynamoDB `ThrottledRequests` and the global tables'
  `ReplicationLatency`.
* **Graceful stop**: on a deployment or scale-in, ECS takes a chserver task
  out of the load balancer, waits `DeregistrationDelay`, then sends
  SIGTERM; chserver stops its background loops and drains in-flight
  requests for up to 15 s (`StopTimeout` 30 s before SIGKILL). The worker
  and the replicator stop their shard consumers on SIGTERM; a lease that
  was not released expires after `STREAM_LEASE_DURATION` (30 s) and another
  task takes the shard over, resuming from the checkpoint (exactly-once
  markers make re-application harmless).
* **Scaling**: chserver is stateless (besides HTTP sessions); scale it with
  `ChserverDesiredCount` or the CPU target-tracking policy. mvworker tasks
  share the streams' shards through leases, so more tasks help when there
  are many source tables or shards; a second task gives fail-over within
  the lease duration. With autoscaling on, a stack update resets the
  service's desired count to `ChserverDesiredCount` until the policy
  adjusts it again.
* **Execute into a task**: not enabled; the image has no shell tools beyond
  BusyBox, and every setting is visible in the task definition.

## Upgrades

Push a new tag, set it in `ImageUri` of each environment, and deploy, home
region first when a release changes the table layout or the catalog
format:

```sh
make image-push IMAGE_REPO=$REPO IMAGE_TAG=v2          # us-east-1
REPO_EU=123456789012.dkr.ecr.eu-west-1.amazonaws.com/twinhouse
aws ecr get-login-password --region eu-west-1 \
  | docker login --username AWS --password-stdin "${REPO_EU%%/*}"
make image-push IMAGE_REPO=$REPO_EU IMAGE_TAG=v2       # eu-west-1 (skip with ECR replication)
# edit ImageUri=...:v2 in samconfig.toml (or the local copy) for us and eu
make ecs-deploy ENV=us
make ecs-deploy ENV=eu
```

Each service rolls with 100 % minimum healthy and 200 % maximum, and the
deployment circuit breaker rolls a service back to the previous task
definition when new tasks keep failing. Two consecutive releases are
expected to coexist during a rollout. To roll back, deploy the previous
tag the same way. DynamoDB tables and their data are never touched by a
deploy.

## Teardown

```sh
sam delete --config-env eu     # replica regions first (from this directory)
sam delete --config-env us
```

Stack deletion removes the services, the load balancer, the roles and the
log groups. **It does not delete any DynamoDB table**: they are created at
runtime and CloudFormation does not own them. To remove the data, list the
deployment's tables and delete them, replicas before the table in the home
region of each global table:

```sh
P=twinhouse_
for r in eu-west-1 us-east-1; do
  aws dynamodb list-tables --region $r --output text --query 'TableNames[]' | tr '\t' '\n' | grep "^$P"
done
# a global table: remove its replicas from the region that holds them all, then delete it
aws dynamodb update-table --region us-east-1 --table-name ${P}__catalog \
  --replica-updates '[{"Delete":{"RegionName":"eu-west-1"}}]'
aws dynamodb wait table-exists --region us-east-1 --table-name ${P}__catalog
aws dynamodb delete-table --region us-east-1 --table-name ${P}__catalog
# a regional table (rollups, ReplacingMergeTree copies, __mv_markers, __checkpoints, __dead_letters)
aws dynamodb delete-table --region eu-west-1 --table-name ${P}__mv_markers
```

DynamoDB allows removing a replica only 24 hours after it was added.

## Differences from the serverless deployment

| | Containers (this directory) | Serverless (`deploy/aws-lambda`) |
|---|---|---|
| API | `cmd/chserver` on Fargate behind an internal ALB, port 8123/8443 | `cmd/chserver-lambda` behind a private REST API Gateway (execute-api VPC endpoint) |
| Request size and time | no payload limit; `MaxExecutionTime` and the ALB idle timeout (up to 115 s) | 6 MB per request and response; 29 s integration timeout by default |
| HTTP sessions | supported, per task (see "Pointing a client at the service") | rejected with `NOT_IMPLEMENTED` (48) |
| Stream worker | mvworker service polling streams with shard leases, or in-process | Lambda on event-source mappings the reconciler manages |
| Reconciler | inside chserver (every `CatalogRefresh`) | `cmd/reconciler-lambda` on the `__catalog` stream and a 1-minute schedule |
| Replicator | one long-running service tailing every region (home region) | one Lambda per region on its ReplacingMergeTree streams |
| Network | the VPC's private subnets with NAT (DynamoDB Streams has no gateway endpoint); VPC endpoints optional | no VPC for the functions; the API is reached through the execute-api endpoint |
| Cost shape | tasks run (and bill) continuously | per request and per batch; idle costs little |
| Cold start | none after the first startup | a new execution environment loads the catalog first |

Both use the same image or binaries, the same tables and the same
`TABLE_PREFIX` contract, so a region can move from one to the other by
deploying the other stack with the same parameters, moving the clients
and deleting the old stack (the tables stay; see
[Moving between options](../../docs/deployment.md#moving-between-options)).

## Local check without AWS

`go test ./deploy/aws-ecs/` parses the template and checks it against the
binaries; it also runs `sam validate --lint` when the SAM CLI is
installed. `make ecs-local` (with the local stack up, `make up`) goes
further: it renders the task definitions with concrete parameter values
(`GlobalMode=none`, `TablePrefix=ecs_`), builds the image, runs each
container definition as a local container (`ecsemu-*`, on the
`twinhouse_default` network, with the rendered environment, command,
read-only root filesystem, CPU and memory, stop timeout and health check)
against the `us` DynamoDB Local, runs `tests/e2e` against the emulated
chserver on port 38123, checks with `InlineViews=false` that the mvworker
container fills a rollup by itself, starts the replicator container
rendered for `GlobalMode=aws` (its `REGIONS` pointed at the two DynamoDB
Local instances) and stops every container with SIGTERM, expecting exit
code 0. It removes the containers and the `ecs_` tables at the end.

What only AWS can verify: the IAM policies against real calls (in
particular the replica operations), the load balancer, listeners, security
groups and Secrets Manager injection, `GlobalMode=aws` with real global
tables, and Fargate's stop sequence.
