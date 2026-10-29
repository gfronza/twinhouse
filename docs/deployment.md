# Deployment

Every deployment runs the same binaries (`cmd/*`, or the image built from
`deploy/Dockerfile`) against the same DynamoDB layout, configured with the
environment variables of [Configuration](configuration.md). The options
differ in where the processes run and what starts them.

> **Not run on real AWS yet.** The automated suites have not run against
> real AWS. The API Lambda passes the e2e suite under `sam local`, and the
> ECS task definitions pass it as local containers (`make ecs-local`), both
> against DynamoDB Local. Read [DESIGN.md §1, §2, §5, §6](../DESIGN.md) before
> you deploy.

## Options

| Option | Directory | What runs | Use it for |
|---|---|---|---|
| Local stack | [`deploy/local`](../deploy/local/docker-compose.yml) | Docker Compose: two DynamoDB Local instances as two regions, `chserver` and `mvworker` per region, the replicator emulating global tables (`GLOBAL_MODE=emulated`). | Development, the test suites, evaluation and demos. Not for production data. |
| AWS serverless | [`deploy/aws-lambda`](../deploy/aws-lambda/README.md) | One AWS SAM stack per region: the API as a Lambda behind a private REST API Gateway, the stream worker and the replicator as Lambdas on DynamoDB-stream event-source mappings, and a reconciler Lambda that manages tables and mappings. | Low or spiky traffic, nothing to keep running, requests and responses under 6 MB, queries under about 25 s. |
| AWS containers | [`deploy/aws-ecs`](../deploy/aws-ecs/README.md) | One CloudFormation stack per region on ECS Fargate in an existing VPC: `chserver` behind an internal Application Load Balancer on port 8123, an `mvworker` service (or the worker in-process), the replicator in the home region. | Steady traffic, large `INSERT`s and results, queries up to about 100 s, HTTP sessions, clients that switch by host and port only. |
| Self-managed containers | [`deploy/Dockerfile`](../deploy/Dockerfile) | The same image and environment variables on Kubernetes, EC2 or any other scheduler, with your own load balancer, secrets and IAM. | A platform you already operate, or load balancer limits you need to control. |
| Hybrids | none | Combinations the binaries support but no template deploys, such as the container API with Lambda stream consumers. | See [Hybrid deployments](deployment-self-managed.md#hybrid-deployments). |

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

Each option has its own page:

- [Local stack](#local-stack-deploylocal), below.
- [AWS serverless](deployment-serverless.md) (`deploy/aws-lambda`).
- [AWS containers on ECS Fargate](deployment-ecs.md) (`deploy/aws-ecs`).
- [Self-managed containers](deployment-self-managed.md), with the
  [hybrid deployments](deployment-self-managed.md#hybrid-deployments).

The [cost model, DynamoDB limits and operations](operations.md) apply to
every option.

## Comparison

| | Serverless (`deploy/aws-lambda`) | Containers (`deploy/aws-ecs`, or self-managed) |
|---|---|---|
| How clients connect | HTTPS on 443 from inside the VPC, through the `execute-api` interface endpoint: `https://<api-id>-<vpce-id>.execute-api.<region>.amazonaws.com/v1` (`ApiVpceEndpoint` output), or the plain invoke URL when the endpoint has private DNS. The stage is a path prefix, so clickhouse-go needs `HttpUrlPath: "v1"`; a private custom domain mapped with an empty base path removes it. Go clients must send `Accept: */*`. | `http://<internal-alb-dns>:8123` (ECS `ClickHouseUrl` output): a ClickHouse client switches by host and port only. HTTPS-only ECS stacks (`CertificateArn` set, `KeepHttpListener=false`) have no 8123 listener and no `ClickHouseUrl` output: clients use `https://<alias the certificate covers>:8443`. Self-managed: the address of your load balancer. |
| TLS termination | Always, at API Gateway. | ECS: optional HTTPS listener on 8443 with an ACM certificate (`CertificateArn`); plain HTTP on 8123 inside the VPC unless `KeepHttpListener=false`. Self-managed: your load balancer or proxy. chserver itself has no TLS. |
| HTTP sessions (`session_id`) | Rejected with `NOT_IMPLEMENTED` (48). | Supported. A session lives in the memory of one process, so with several instances every request of a session must reach the same one. |
| Request size | 6 MB per request (Lambda invocation payload). Request bodies reach the function base64-encoded, which inflates them by about a third toward that limit. | No fixed limit on `INSERT` data: a body over 1 MiB is split at the start of its inline data and streamed into the statement. Query text (not counting inline `INSERT` data) is limited by `max_query_size`, 262144 bytes by default as in ClickHouse; a request may raise it to at most 256 MiB. |
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

## Common to every AWS deployment

**Regions and `GLOBAL_MODE`.**

- **Several regions:** use `GLOBAL_MODE=aws`. Source tables and `__catalog`
  become DynamoDB global tables, `ReplacingMergeTree` tables get one table
  per region kept in sync by the replicator, and rollups are regional
  (see [Consistency model](consistency.md)).
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
  `KmsKeyArn` to restrict who can decrypt it
  ([deploy/aws-lambda/README.md](../deploy/aws-lambda/README.md)
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
in [deploy/aws-ecs/README.md](../deploy/aws-ecs/README.md#teardown).
DynamoDB allows removing a replica, or deleting a table that was the
source of a replica, only 24 hours after the replica was added.

### Moving between options

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

### IAM permissions

The tables below list what each role needs. Each option's page
([AWS serverless](deployment-serverless.md#permissions),
[ECS Fargate](deployment-ecs.md#permissions)) lists what its template
grants, and `go test ./deploy/aws-lambda/ ./deploy/aws-ecs/` checks the
scoping. The resources are
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

## Local stack (`deploy/local`)

`make up` builds the image and starts
[`deploy/local/docker-compose.yml`](../deploy/local/docker-compose.yml) as the
compose project `twinhouse`:

- two DynamoDB Local instances, in memory, standing in for `us-east-1`
  (host port 18000) and `eu-west-1` (18001);
- a `chserver` per region: `us` on `:8123`, `eu` on `:8124`;
- an `mvworker` per region;
- the replicator, copying every global table between the two instances
  with last-writer-wins (`GLOBAL_MODE=emulated`, [DESIGN.md §6](../DESIGN.md#6-multi-region-local-emulation-and-aws-cmdreplicator-deploylocal-deployaws-lambda-deployaws-ecs)).

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
  second stack beside the first (see [Testing](testing.md)).
- **Users:** no `CH_USERS` is set, so the user is `default` without a
  password.
- **Data:** DynamoDB Local keeps everything in memory, and `make down`
  discards it. DynamoDB Local also slows down as state accumulates (see
  [Testing](testing.md)).
- **Upgrades:** `make up` rebuilds the image and recreates the containers
  whose image changed.
- **Not for production:** the stack has no TLS, no real global tables and
  no durability.
