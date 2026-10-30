# Benchmark environment on AWS

This directory runs [`cmd/chbench`](../../cmd/chbench) on AWS: twinhouse on
ECS with DynamoDB against ClickHouse on EC2, so the local comparison
([docs/clickhouse-comparison.md](../../docs/clickhouse-comparison.md)) can
be measured on DynamoDB itself. Everything it creates is disposable.

```
  bench VPC (10.40.0.0/16): private subnets in two AZs, one NAT gateway,
  DynamoDB and S3 gateway endpoints
  ┌──────────────────────────────────────────────────────────────────────┐
  │ client EC2 ──► internal ALB ──► chserver (Fargate, 4 vCPU / 8 GiB) ──► DynamoDB (on demand)
  │ (chbench)                        stream worker in-process
  │     └────────► ClickHouse 25.8 EC2 (c7g.xlarge, 4 vCPU / 8 GiB)
  └──────────────────────────────────────────────────────────────────────┘
```

- `template.yaml`: the VPC, the ClickHouse and client instances (reached
  through SSM, no SSH keys or public IPs), the ECR repository for the
  twinhouse image, a bucket for the chbench binary and results, and the
  log group the runs stream to.
- twinhouse is the regular [ECS deployment](../aws-ecs/README.md), as a
  second stack in this VPC: `GlobalMode=none`, `WorkerMode=in-process`,
  one task, table prefix `chbench_`.
- `bench.sh` drives it all; `capacity.py` splits DynamoDB's consumed
  capacity (CloudWatch) by benchmark step.

## Network and access

Nothing accepts connections from the internet. The public subnet holds
only the NAT gateway, which carries outbound connections only; the
instances, the load balancer (`Scheme: internal`) and the twinhouse task
have private addresses only.

| Security group | Inbound | Outbound |
|---|---|---|
| client | none | 443 anywhere (AWS APIs); 8123 within the VPC (both servers) |
| ClickHouse | 8123 from the client's group | 443 anywhere (packages, Docker Hub, AWS APIs) |
| twinhouse load balancer | 8123 from the client's group | the tasks only |
| twinhouse tasks | 8123 from the load balancer | 443 anywhere (AWS APIs) |

There is no SSH: `bench.sh` runs commands on the client through SSM,
whose agent connects out, so access is whoever may call
`ssm:SendCommand`. The instances require IMDSv2 and have encrypted disks;
the bucket blocks public access.

## Running it

Needs the AWS CLI, Docker with buildx, Go and python3. `AWS_PROFILE`
(default `twinhouse`) and `AWS_REGION` (default `us-east-1`) choose the
account and region.

```sh
deploy/aws-bench/bench.sh up          # secret, bench stack, image, twinhouse stack (~15 min)
deploy/aws-bench/bench.sh check       # both servers answer from the client
deploy/aws-bench/bench.sh run -size M # one chbench run, streamed; results in bin/aws/
deploy/aws-bench/bench.sh capacity bin/aws/chbench-aws-<time>.json
deploy/aws-bench/bench.sh down        # everything, the chbench_* tables included
```

`run` passes its arguments to chbench and adds `-settle 2m -align-steps
-timeout 100s`:
- `-settle`: there is no Docker to watch, so after a load chbench waits
  until ClickHouse has no merges and at least 2 minutes for twinhouse's
  stream worker.
- `-align-steps`: each step starts on a minute boundary, so CloudWatch's
  per-minute capacity can be split by step.
- `-timeout`: below the load balancer's 110 s idle timeout.

Both servers use the same login, `bench` with a random password, kept in
the Secrets Manager secret `twinhouse-bench/ch-users`. twinhouse reads it
as `CH_USERS`; the instances read it at start.

## Cost

While the stacks are up, roughly $0.45 an hour: the NAT gateway, the load
balancer, the Fargate task and the two instances. DynamoDB is on demand:
a size M run costs a few dollars, mostly the bulk load through the view
(its transactions bill twice). `capacity` prints each step's units and
cost. Run `down` when done; DynamoDB tables are not part of any stack, and
`down` deletes them by prefix.

## What differs from the local run

- twinhouse is reached through a load balancer; ClickHouse directly.
- The Fargate task and the load balancer may sit in the other
  Availability Zone from the client, which adds a cross-zone hop.
- DynamoDB has no CPU or memory cap: it is the service as billed.
