# AWS containers on ECS Fargate (`deploy/aws-ecs`)

Part of [Deployment](deployment.md). The regions, table prefix, users,
credentials and IAM notes in
[Common to every AWS deployment](deployment.md#common-to-every-aws-deployment)
apply here too.

[deploy/aws-ecs](../deploy/aws-ecs/README.md) is a plain CloudFormation
template that runs the long-running binaries on ECS Fargate in an
existing VPC, **one stack per region**. It has no SAM transform; it is
deployed with `sam deploy` for parity with the serverless stack.

```
            application VPC (region A)                     (same stack in region B,
                   │ http :8123 / https :8443                without the replicator)
     ┌─────────────▼──────────────┐
     │ internal Application LB    │  health check /ping
     └─────────────┬──────────────┘
     ┌─────────────▼──────────────┐
     │ chserver service           │  cmd/chserver: HTTP API + reconciler
     └─────────────┬──────────────┘
   ┌───────────────▼─────────────────────────────────────────────┐
   │ DynamoDB (region A)   global tables ◄──replication──────────┼──► region B
   └───────┬──────────────────────────────────────┬──────────────┘
           │ source-table streams                 │ ReplacingMergeTree streams of every region
   ┌───────▼──────────┐                 ┌─────────▼───────────────────┐
   │ mvworker service │                 │ replicator service          │
   │ (or in-process)  │                 │ (home region only)          │
   └──────────────────┘                 └─────────────────────────────┘
```

## What it creates

The ECS cluster (or it uses `ExistingClusterName`), the internal
Application Load Balancer, its listeners and target group, the security
groups, the log groups, a task execution role, one task role per service,
and these services:

- **chserver** runs behind the load balancer, whose listener uses
  ClickHouse's HTTP port **8123**, so clients switch by host and port
  only. An optional HTTPS listener on **8443** uses an ACM certificate.
  The target group checks `/ping`. `CH_USERS` comes from a Secrets
  Manager secret through the task definition's `Secrets`.
- **mvworker** is a separate service (`WorkerMode=service`; shard leases
  split the work between its tasks), or it runs inside every chserver task
  (`WorkerMode=in-process`, `RUN_STREAM_WORKER=true`).
- **replicator** runs once per deployment, by default in the home region
  (`RunReplicator=auto`), with `REGIONS=us-east-1=,eu-west-1=` built from
  `ReplicaRegions`.
- **Access:** the load balancer accepts only `AllowedCidr` and
  `ClientSecurityGroupId`, and the tasks accept only the load balancer.
- **Parameters** set optional CPU target-tracking autoscaling for
  chserver, log retention, task sizes and counts.

## Prerequisites

In every region:

- Private subnets in two or more Availability Zones, with a NAT gateway
  (or other internet egress). mvworker, the in-process worker and the
  replicator read DynamoDB Streams, which the DynamoDB gateway endpoint
  does not cover, and the replicator calls the other regions. VPC
  endpoints for DynamoDB, ECR, CloudWatch Logs and Secrets Manager are
  optional; STS is not needed.
- An ECR repository.
- The `CH_USERS` secret.

## Quick start

```sh
make ecs-validate                                   # sam validate --lint (no credentials needed)
make image-push IMAGE_REPO=<account>.dkr.ecr.us-east-1.amazonaws.com/twinhouse \
  IMAGE_TAG=v1 PLATFORM=linux/arm64                 # after docker login to ECR; per region
# fill in the CHANGE_ME values in deploy/aws-ecs/samconfig.toml (both environments) first
make ecs-deploy ENV=us                              # home region first
make ecs-deploy ENV=eu
make ecs-local                                      # optional: the task definitions as local
                                                    # containers + tests/e2e, no AWS (make up first)
```

The `CHANGE_ME` placeholders are `ImageUri`, `VpcId`, `SubnetIds`,
`AllowedCidr` and `ChUsersSecretArn`, and CloudFormation rejects them, so
a forgotten one cannot deploy. As with `aws-deploy`, `make ecs-deploy`
refuses `--parameter-overrides`. Use a git-ignored copy instead
(`ECS_CONFIG=samconfig.local.toml`).

## Limits

- **Sessions with several tasks.** A session lives in one chserver task.
  The load balancer keeps a client on one task only with target-group
  stickiness and a client that returns its cookie, which most ClickHouse
  clients do not. Prefer URL settings and `database=`, or run one task.
- **Query time.** `MaxExecutionTime` (100 s) must stay below
  `LoadBalancerIdleTimeout` (110 s, at most 115 s), which must stay below
  chserver's fixed 120 s keep-alive. Otherwise the load balancer can reuse
  a connection chserver just closed and return sporadic 502s.
- **DDL.** A global `CREATE TABLE` waits for its replicas (minutes) and
  can outlast the idle timeout (see [Comparison](deployment.md#comparison)).
- **No Lambda permissions.** The template grants none, so event-source
  mappings are not used (`MVWORKER_FUNCTION_ARN` stays unset).

## Permissions

Each service has its own task role:

- **chserver:**
  - the item and table actions (`DeleteTable` included) and the TTL
    actions in its region, and `ListTables`;
  - with `GLOBAL_MODE=aws`, the replica actions, limited by
    `aws:RequestedRegion` to the deployment's regions;
  - the stream reads only with the in-process worker.
- **mvworker:** the item, table (no `DeleteTable`), TTL and stream actions
  in its region, plus the `__catalog` replica actions in the home region.
- **replicator:** the item actions, `DescribeTable`, `CreateTable` and the
  stream actions in every region of the deployment.

The task execution role pulls the image, writes logs and reads the
`CH_USERS` secret.

## Upgrade

1. Push a new tag (`make image-push ... IMAGE_TAG=v2`).
2. Set `ImageUri` to it in each environment of `samconfig.toml`, or of the
   local copy.
3. Run `make ecs-deploy`, home region first when a release changes the
   table layout or the catalog format.

Each service rolls out with 100% minimum healthy, and the deployment
circuit breaker rolls back a version whose tasks keep failing. Stopping
tasks first leave the load balancer for `DeregistrationDelay` (110 s), so
in-flight queries finish.

## Teardown

`sam delete --config-env eu`, then `--config-env us`, run in
`deploy/aws-ecs`. This removes the services, the load balancer, the roles
and the log groups, not the tables.

[deploy/aws-ecs/README.md](../deploy/aws-ecs/README.md) has the full
prerequisites, the image build for `linux/arm64` or `linux/amd64`, every
parameter, how to verify a stack, operations, scaling, upgrades, teardown
with table deletion, and what `make ecs-local` checks.
