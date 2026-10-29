# twinhouse

A ClickHouse-compatible HTTP server that stores its data in Amazon DynamoDB,
so a ClickHouse-style workload can run **multi-region active-active**.

ClickHouse Cloud has no multi-region active-active mode. DynamoDB global
tables do. This server speaks ClickHouse's HTTP protocol, so an application
that talks to ClickHouse through clickhouse-go v2 (HTTP protocol), curl or
any other HTTP client can switch to it by changing host and port. The
workload it targets is append-only events (`MergeTree`), latest-state
snapshots (`ReplacingMergeTree`) and materialized-view rollups
(`AggregatingMergeTree` / `SummingMergeTree`). It runs in every region,
writes are accepted everywhere, and all regions converge to the same data.

> **Status: experimental.** The design is stable and well tested against
> emulated global tables, but it has no production track record yet. The
> `GLOBAL_MODE=aws` deployment path (real DynamoDB global tables), the
> serverless deployment ([deploy/aws-lambda](deploy/aws-lambda/README.md),
> Lambda behind a private API Gateway) and the container deployment
> ([deploy/aws-ecs](deploy/aws-ecs/README.md), ECS Fargate behind an
> internal load balancer) are implemented and unit-tested; the API Lambda
> passes the e2e suite under `sam local` and the ECS task definitions pass
> it as local containers, but the automated suites have not run against
> real AWS. Expect rough edges, and benchmark costs
> before relying on it.

## How it works

- Every ClickHouse table is a DynamoDB table. `WHERE` predicates on the
  sorting key become DynamoDB `Query` key conditions; anything else is a
  parallel `Scan`.
- Source tables (`MergeTree`, `ReplacingMergeTree`) are global; rollups
  (`AggregatingMergeTree`, `SummingMergeTree`) are regional, and each
  region rebuilds its own from its replica's stream.
- Materialized views are applied inline during `INSERT` (read-your-writes
  in that region) and again by a stream worker, guarded by markers so
  every source row is counted exactly once per region.
- The schema lives in a replicated catalog, and every region reconciles
  its physical tables against it.

[Architecture](docs/architecture.md) and
[Consistency model](docs/consistency.md) have the details;
[DESIGN.md](DESIGN.md) is the full contract.

## What is verified

On top of the unit and DynamoDB integration tests, three black-box suites
run against a local Docker stack (two DynamoDB Local instances standing in
for two AWS regions):

- `tests/e2e`: an application's view of the server, through clickhouse-go
  v2 over HTTP and raw HTTP.
- `tests/compat`: differential testing against real ClickHouse 25.8.
  Several hundred statements must return the same results, or the same
  error code and HTTP status; every deviation is declared and documented.
- `tests/multiregion`: active-active behaviour on two regions, including
  concurrent writers, exactly-once rollups while processes are killed, and
  region outage and catch-up.

All three suites, `go vet` and `go test ./...` currently pass.
[Testing](docs/testing.md#what-is-verified) describes each suite.

## Quickstart

You need Docker with Compose v2, `make` and `curl`.

```sh
git clone <this repository> && cd twinhouse
make up        # two "regions": us on :8123, eu on :8124
US=http://localhost:8123 EU=http://localhost:8124

curl -sS $US --data-binary "CREATE DATABASE demo"
curl -sS $US --data-binary "
CREATE TABLE demo.page_views (ts DateTime64(3), site String, user_id UInt64)
ENGINE = MergeTree ORDER BY (site, ts)"
curl -sS $US --data-binary "
INSERT INTO demo.page_views VALUES ('2026-10-01 10:00:00.000', 'example.org', 1)"

sleep 3        # the schema and the row replicate to eu
curl -sS $EU --data-binary "SELECT count() FROM demo.page_views"   # 1
make down      # remove the stack and its data
```

[Getting started](docs/getting-started.md) walks through, from a fresh
stack, a materialized-view rollup, writes in both regions, latest-wins snapshots
and clickhouse-go v2.

## Documentation

- [Getting started](docs/getting-started.md): the local stack and a
  walk-through in curl and Go.
- [Architecture](docs/architecture.md): how tables, views, replication and
  the catalog map onto DynamoDB, and the project layout.
- [Consistency model](docs/consistency.md): what clients see in their own
  region and across regions.
- [SQL support](docs/sql.md): what is supported, what is not, and the
  deliberate deviations from ClickHouse.
- [Configuration](docs/configuration.md): the environment variables of
  every binary.
- [Deployment](docs/deployment.md): choosing an option, the comparison,
  what every AWS deployment shares, and the local stack.
  - [AWS serverless](docs/deployment-serverless.md): Lambda behind a
    private API Gateway.
  - [AWS containers on ECS Fargate](docs/deployment-ecs.md): chserver
    behind an internal load balancer.
  - [Self-managed containers](docs/deployment-self-managed.md): any
    scheduler, and hybrid deployments.
- [Costs, limits and operations](docs/operations.md): DynamoDB write
  costs, item and key limits, and monitoring.
- [Testing](docs/testing.md): what the suites verify and how to run them.
- [Limitations and roadmap](docs/roadmap.md): known gaps.
- [DESIGN.md](DESIGN.md): the detailed design contract.

[docs/README.md](docs/README.md) is the same index with a line per page.

## Development

**Prerequisites:** Go **1.27** or newer (see `go.mod`), Docker with
Compose v2 for the local stack and every suite beyond the unit tests, and
[golangci-lint](https://golangci-lint.run/) on `PATH` for `make lint`.
`make lint-install` installs the version pinned in the `Makefile`
(`GOLANGCI_LINT_VERSION`) into `$GOBIN`; `make lint` warns when the one on
`PATH` differs, since its findings may then differ from CI.

Once per clone, install the git hooks:

```sh
make hooks     # git config core.hooksPath .githooks
```

Everyday targets:

| Target | What it does |
|---|---|
| `make build` | `go build ./...` and the binaries into `bin/`. |
| `make test` | `go test ./...`: unit tests, no Docker needed. |
| `make test-race` | `go test -race ./...`. |
| `make fmt` | `gofmt -w` on every Go file; fixes the `gofmt` step of `make verify`. |
| `make vet` | `go vet ./...`, then again with the build tags of the tagged suites, so they compile. |
| `make leaks` | gitleaks over every commit: no secret in the git history. |
| `make lint` | golangci-lint with `.golangci.yml`. |
| `make tidy` | `go mod tidy`; fixes the `go.mod`/`go.sum` step of `make verify`. |
| `make verify` | The full local gate, the same one CI runs: `go.mod`/`go.sum` tidy and verified, `gofmt` check, `make leaks`, `make vet`, `make lint` and `make test`. |

**Hooks.** `pre-commit` scans the staged changes with gitleaks and stops
the commit if they hold a secret, then runs `go mod tidy` and stages
`go.mod` and `go.sum` if it changed them. `pre-push` runs `make verify` on the tip
commit of each ref being pushed (in a temporary worktree unless it is the
current, clean `HEAD`) and blocks the push if it fails;
`git push --no-verify` skips it.

**Test suites.** The unit tests need nothing; the others need Docker.

| Suite | Command | Needs |
|---|---|---|
| Unit | `make test` | Nothing. |
| Integration (storage, streams, replicator, mvworker) | `make test-integration` | Docker; starts two throwaway DynamoDB Local containers on ports 18101 and 18102. |
| End to end | `make e2e` | The local stack (`make up`). |
| ClickHouse compatibility | `make compat` | The local stack (`make up`); `make compat` starts the reference ClickHouse 25.8 on `:18123` itself. |
| Multi-region | `make multiregion` | Its own stack on separate ports, never the development stack. |

[Testing](docs/testing.md) has the exact commands, the deployment-template
tests and what to do when DynamoDB Local slows down.

**CI.** GitHub Actions runs [`ci.yml`](.github/workflows/ci.yml) on pull
requests, pushes to `main`, a weekly schedule and manual dispatch: `make
verify`, the race detector, the DynamoDB Local integration tests, the e2e
suite against the local stack, a linux/amd64 and linux/arm64 build and
govulncheck. The compat and multiregion suites run on demand from
[`suites.yml`](.github/workflows/suites.yml).

Issues and pull requests are welcome. Please keep examples and schemas
generic, and add a compat scenario for any new SQL behaviour.

## License

Apache License 2.0, the same license as ClickHouse. See [LICENSE](LICENSE).
