# Testing

The project has unit tests, DynamoDB integration tests and three
black-box suites that run against the local stack. The everyday make
targets and the git hooks are in the
[Development](../README.md#development) section of the top-level README.

## What is verified

Three black-box test suites run against a local Docker stack (two DynamoDB
Local instances standing in for two AWS regions), on top of the unit and
DynamoDB integration tests:

| Suite | What it checks |
|---|---|
| `tests/e2e` | An application's view of the server: clickhouse-go v2 over HTTP (native API and `database/sql`) and raw HTTP. Covers DDL, parameterised inserts, MV rollups, `FINAL`, key-range probes, latest-wins replays, bulk `JSONEachRow`, formats and error codes. |
| `tests/compat` | **Differential testing against real ClickHouse 25.8.** Over 20 scenario files (`tests/compat/testdata/*.sql`, several hundred statements) run against both servers. Results must match in several output formats (TSV with names and types, JSONCompact, and Native decoded by clickhouse-go). Expected failures must match too: the same ClickHouse error code and HTTP status. Each statement carries a directive (`-- query`, `-- error`, `-- not-implemented`, `-- unsupported`, `-- known-deviation`). A deviation has to be declared and documented; nothing differs silently. |
| `tests/multiregion` | Active-active behaviour on two regions: DDL and DROP propagation, table re-creation, write in one region and read in the other, concurrent writers in both regions on the same keys, exactly-once rollups while workers and the replicator are killed and restarted, region outage and catch-up, replication lag, clickhouse-go parity across regions. |

All three suites, `go vet` and `go test ./...` currently pass.

## The local gate (`make verify`)

`make verify` is the check that the pre-push hook and CI run. It needs no
Docker, and runs these steps in order:

1. `go mod tidy -diff` and `go mod verify`: `go.mod` and `go.sum` are tidy
   and the module cache matches them (`make tidy` fixes the first).
2. `gofmt -l`: every Go file is formatted (`make fmt` fixes it).
3. `make leaks`: gitleaks finds no secret in any commit reachable from
   `HEAD`. It runs through `go run` at `GITLEAKS_VERSION` from the
   Makefile, so it needs no install; the first run downloads and builds
   it.
4. `make vet`: `go vet ./...`, then `go vet` again with the build tags in
   the Makefile's `BUILD_TAGS` (`e2e`, `compat`, `multiregion`,
   `ecslocal`, `samlocal`, `tools`), so the tagged suites at least compile
   without their stack.
5. `make lint`: golangci-lint with `.golangci.yml`.
6. `make test`: `go test ./...`.

A secret that is already committed stays in the history after it is
deleted from the file: rotate it first, then rewrite the history or, for a
false positive, add its fingerprint to a `.gitleaksignore` file.

`make test-race` runs the unit tests under the race detector
(`go test -race ./...`).

## Running the suites

```sh
make test               # go test ./... — unit tests, no Docker needed
make bench              # the Go benchmarks (docs/benchmarks.md)
make bench-compare      # benchmarks of BASE vs the working tree; fails on a >3% exec/server slowdown
make test-integration   # storage, streams, replicator and mvworker against two throwaway DynamoDB Local containers
DYNAMODB_TEST_ENDPOINT=http://localhost:18101 go test ./internal/storage/dynamo/...   # storage integration tests (any DynamoDB Local)

make up PROFILES=compat # local stack + reference ClickHouse 25.8 on :18123
make e2e                # tests/e2e against :8123 (single-region; also works with make up-single)
make compat             # tests/compat: differential tests against the reference ClickHouse

make aws-validate && make aws-build   # SAM template lint + the four Lambda bootstraps
go test ./deploy/aws-lambda/   # template vs. the binaries' contract (env vars, IAM scope, private API, schedule)
make aws-local-api      # the API Lambda under sam local (:3000) against the twinhouse DynamoDB Local
make aws-local-e2e      # tests/e2e against it; the suite detects Lambda mode and skips sessions
go test -tags samlocal -count=1 ./cmd/chserver-lambda/   # API Gateway specifics (base64 bodies, compression) against sam local

make ecs-validate       # ECS template lint (sam validate --lint)
go test ./deploy/aws-ecs/   # template vs. the binaries' contract (env vars, ports, /ping, secrets, IAM scope, conditions)
make ecs-local          # the ECS task definitions as local containers (ecsemu-*) against the twinhouse
                        # DynamoDB Local: tests/e2e, mvworker applying a view, replicator start, SIGTERM
```

The API Lambda's unit tests (`cmd/chserver-lambda`, `go test ./...`) drive
the real server with synthetic API Gateway proxy events and compare every
response byte for byte with the same server over plain HTTP, so the proxy
layer needs no AWS account to be tested.

The **multiregion** suite stops, kills and recreates containers, so it
needs its own compose project on separate ports, never the development
stack:

```sh
COMPOSE_PROJECT=dhmr US_HTTP_PORT=28123 EU_HTTP_PORT=28124 \
  DDB_US_PORT=28000 DDB_EU_PORT=28001 CH_REF_PORT=28125 make up
make multiregion        # ~4 minutes on a fresh stack
```

See `tests/multiregion/doc.go` for its `MR_*` variables and the optional
soak test (`MR_SOAK=1`).

**DynamoDB Local slows down as state accumulates.** It keeps all stream
and item history in memory, and gets markedly slower after a few suite
runs: an insert with an inline view went from about 1 s to about 25 s,
and the multiregion suite from about 3 to about 17 minutes. If tests start
timing out, recreate the stack first (`make down && make up`, or
`docker compose -p dhmr -f deploy/local/docker-compose.yml down -v` for the
multiregion stack). For experiments, prefer a fresh private container:

```sh
docker run -d --rm --name ddb-scratch -p 18100:8000 \
  amazon/dynamodb-local:3.3.2 -jar DynamoDBLocal.jar -sharedDb -inMemory
```
