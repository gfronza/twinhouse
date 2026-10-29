# Developer entry points.
#   deploy/local/docker-compose.yml  the local stack (targets up, down, e2e, ...)
#   deploy/aws-lambda/               serverless AWS deployment, SAM (targets aws-*)
#   deploy/aws-ecs/                  container AWS deployment, ECS Fargate,
#                                    plain CloudFormation (targets ecs-*, image-push)

GO              ?= go
COMPOSE_PROJECT ?= twinhouse
PROFILES        ?=
COMPOSE_YML     ?= deploy/local/docker-compose.yml
COMPOSE         := docker compose -p $(COMPOSE_PROJECT) -f $(COMPOSE_YML)
COMPOSE_UP      := $(COMPOSE) $(foreach p,$(PROFILES),--profile $(p))

# Endpoints of the running stack, exported to the e2e/compat/multiregion tests.
export TWINHOUSE_URL      ?= http://localhost:8123
export TWINHOUSE_EU_URL   ?= http://localhost:8124
export CLICKHOUSE_REF_URL ?= http://localhost:18123
export DDB_US_ENDPOINT    ?= http://localhost:18000
export DDB_EU_ENDPOINT    ?= http://localhost:18001
export TABLE_PREFIX       ?= twinhouse_

# DynamoDB Local containers for the integration tests (make test-integration).
# The image is pinned (tag and digest); keep it in sync with the x-ddb image of
# deploy/local/docker-compose.yml.
ITEST_DDB_A    ?= twinhouse-itest-ddb-a
ITEST_DDB_B    ?= twinhouse-itest-ddb-b
ITEST_PORT_A   ?= 18101
ITEST_PORT_B   ?= 18102
DDB_LOCAL_IMG  ?= amazon/dynamodb-local:3.3.2@sha256:6ada631ec7aa5fc83446855eb17233e3dad184fec47477e8140c6c569cfe0c86

# Quality gate (make verify, run by CI and by the pre-push hook).
# GOLANGCI_LINT_VERSION is the single source of the linter version: CI
# installs exactly this one with `make lint-install`.
GOLANGCI_LINT_VERSION ?= v2.15.0
GOLANGCI_LINT         ?= golangci-lint
# Build tags of the suites outside the default build (tests/e2e, tests/compat,
# tests/multiregion, deploy/aws-ecs ecslocal, cmd/chserver-lambda samlocal,
# tools/deps.go); `make vet` compiles and vets them too. Keep in sync with
# run.build-tags in .golangci.yml.
BUILD_TAGS            ?= e2e,compat,multiregion,ecslocal,samlocal,tools
RACE_TIMEOUT          ?= 30m
GOVULNCHECK_VERSION   ?= v1.9.0
# gitleaks runs through `go run` at this version (make leaks, the pre-commit
# hook); the first run downloads and builds it, later runs use the Go cache.
GITLEAKS_VERSION      ?= v8.30.1
GITLEAKS              ?= $(GO) run github.com/zricethezav/gitleaks/v8@$(GITLEAKS_VERSION)
# The SAM CLI the CI installs (make sam-install) for `sam validate --lint`,
# which the deploy/aws-lambda and deploy/aws-ecs Go tests run when sam is on PATH.
SAM_CLI_VERSION       ?= 1.170.0

.PHONY: all build image test bench bench-compare vet fmt test-integration up up-single down logs ps e2e compat multiregion \
        logs-dump verify tidy mod-check fmt-check leaks leaks-staged lint lint-install sam-install test-race vulncheck hooks \
        aws-validate aws-build aws-local-reconcile aws-local-api aws-local-e2e aws-deploy \
        ecs-validate ecs-local image-push ecs-deploy chbench-up chbench-down chbench

all: build test

build:
	$(GO) build ./...
	$(GO) build -o bin/ ./cmd/...

image:
	$(COMPOSE) build

test:
	$(GO) test ./...

test-race:
	$(GO) test -race -timeout $(RACE_TIMEOUT) ./...

# The Go benchmarks (no tests), with allocations. A full run takes about a
# minute; narrow it with BENCH (a -bench regexp) and BENCH_PKGS, e.g.
#   make bench BENCH=SelectStream BENCH_PKGS=./internal/server/
BENCH         ?= .
BENCH_PKGS    ?= ./...
BENCH_COUNT   ?= 1
BENCH_TIME    ?= 1s
BENCH_TIMEOUT ?= 60m
bench:
	$(GO) test -run '^$$' -bench '$(BENCH)' -benchmem -count $(BENCH_COUNT) \
		-benchtime $(BENCH_TIME) -timeout $(BENCH_TIMEOUT) $(BENCH_PKGS)

# The benchmarks of BASE against the working tree (scripts/bench-compare.sh),
# about 13 minutes with the defaults. Not part of verify: CI runners are too
# noisy for a 3% limit. Details in docs/benchmarks.md.
#   make bench-compare
#   make bench-compare BASE=main~3 BENCH=SelectStream
BENCHSTAT_VERSION    ?= v0.0.0-20261009192801-be2c69fb417e
BENCHSTAT            ?= $(GO) run golang.org/x/perf/cmd/benchstat@$(BENCHSTAT_VERSION)
BASE                 ?=
BENCH_PAIRS          ?= 8
BENCH_MAX_LOAD       ?= 8
BENCH_LOAD_WAIT      ?= 900
BENCH_MAX_REGRESSION ?= 3
BENCH_GATE_PKGS      ?= ./internal/exec ./internal/server
BENCH_REPORT_PKGS    ?= ./internal/format ./internal/sqlparse
BENCH_OUT            ?= bin/bench-compare
bench-compare:
	GO='$(GO)' BENCHSTAT='$(BENCHSTAT)' BASE='$(BASE)' BENCH='$(BENCH)' BENCH_TIME='$(BENCH_TIME)' \
	BENCH_PAIRS='$(BENCH_PAIRS)' BENCH_MAX_LOAD='$(BENCH_MAX_LOAD)' BENCH_LOAD_WAIT='$(BENCH_LOAD_WAIT)' \
	BENCH_MAX_REGRESSION='$(BENCH_MAX_REGRESSION)' BENCH_GATE_PKGS='$(BENCH_GATE_PKGS)' \
	BENCH_REPORT_PKGS='$(BENCH_REPORT_PKGS)' BENCH_OUT='$(BENCH_OUT)' scripts/bench-compare.sh

# The default build, then the tagged suites (they need a running stack to
# run, but must always compile).
vet:
	$(GO) vet ./...
	$(GO) vet -tags $(BUILD_TAGS) ./...

# GO_FILES lists, NUL-separated, the tracked and untracked (not ignored) Go
# files that exist in the working tree (a deleted tracked file is skipped).
GO_FILES := git ls-files -z --cached --others --exclude-standard -- '*.go' | \
	xargs -0 sh -c 'for f; do [ ! -e "$$f" ] || printf "%s\0" "$$f"; done' sh

fmt:
	@$(GO_FILES) | xargs -0 gofmt -l -w

# ---------------------------------------------------------------------------
# Quality gate. `make verify` is what CI runs and what the pre-push hook runs
# (install the hooks once per clone with `make hooks`):
#   make verify      mod-check, fmt-check, leaks, vet, lint and test, in that order
#   make tidy        go mod tidy (the pre-commit hook runs it too)
#   make leaks       gitleaks over the git history (secrets in any commit)
#   make lint        golangci-lint with .golangci.yml
#   make test-race   the unit tests under the race detector
#   make vulncheck   govulncheck (CI job; not in verify: it needs the network and
#                    flags standard-library fixes of newer Go patch releases)
verify: mod-check fmt-check leaks vet lint test
	@echo "verify: OK"

tidy:
	$(GO) mod tidy

# go.mod/go.sum must be tidy and the module cache must match go.sum.
mod-check:
	$(GO) mod tidy -diff
	$(GO) mod verify

# No commit reachable from HEAD may contain a secret. --redact keeps the
# matched values out of the output (and CI logs).
leaks:
	$(GITLEAKS) git --no-banner --redact --verbose --log-level warn .

# The changes staged for the next commit (the pre-commit hook).
leaks-staged:
	$(GITLEAKS) git --pre-commit --staged --no-banner --redact --verbose --log-level warn .

# Tracked and untracked (not ignored) Go files must be gofmt-clean.
fmt-check:
	@files=$$($(GO_FILES) | xargs -0 gofmt -l) || { echo "gofmt failed (see above)" >&2; exit 1; }; \
	if [ -n "$$files" ]; then echo "gofmt: these files need formatting (make fmt):" >&2; echo "$$files" >&2; exit 1; fi

lint:
	@command -v $(GOLANGCI_LINT) >/dev/null 2>&1 || { \
		echo "golangci-lint not found: run 'make lint-install' to install $(GOLANGCI_LINT_VERSION) into \$$GOBIN (default \$$GOPATH/bin, which must be on PATH)," >&2; \
		echo "or see https://golangci-lint.run/docs/welcome/install/local" >&2; exit 1; }
	@v=$$($(GOLANGCI_LINT) version --short 2>/dev/null); \
	if [ "v$${v#v}" != "$(GOLANGCI_LINT_VERSION)" ]; then \
		echo "warning: $(GOLANGCI_LINT) is $${v:-of unknown version}, the pinned version is $(GOLANGCI_LINT_VERSION) (make lint-install); findings may differ from CI" >&2; fi
	$(GOLANGCI_LINT) run

lint-install:
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

vulncheck:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

# The pinned SAM CLI, with pipx (CI; locally any recent sam works). Fails if
# another sam comes first on PATH.
sam-install:
	pipx install --force "aws-sam-cli==$(SAM_CLI_VERSION)"
	@v=$$($(SAM) --version); case "$$v" in *" $(SAM_CLI_VERSION)") echo "$$v";; \
		*) echo "$(SAM) on PATH is '$$v', not $(SAM_CLI_VERSION): put pipx's bin dir (pipx environment --value PIPX_BIN_DIR) first on PATH" >&2; exit 1;; esac

# One-time, per clone: use the versioned hooks in .githooks (pre-commit runs
# go mod tidy, pre-push runs make verify; git push --no-verify skips it).
hooks:
	git config core.hooksPath .githooks

# Integration tests against two throwaway DynamoDB Local instances: the
# streams, replicator and mvworker tests (DDB_TEST_ENDPOINT[_B]), then the
# storage-layer tests of internal/storage/dynamo (DYNAMODB_TEST_ENDPOINT, on
# instance B, so they do not share an instance with the stream tests). The
# containers are removed on success and failure; on a startup failure their
# logs are printed.
ITEST_WAIT_TRIES ?= 240
test-integration:
	@docker rm -f $(ITEST_DDB_A) $(ITEST_DDB_B) >/dev/null 2>&1 || true
	docker run -d --name $(ITEST_DDB_A) -p $(ITEST_PORT_A):8000 $(DDB_LOCAL_IMG) -jar DynamoDBLocal.jar -sharedDb -inMemory >/dev/null
	docker run -d --name $(ITEST_DDB_B) -p $(ITEST_PORT_B):8000 $(DDB_LOCAL_IMG) -jar DynamoDBLocal.jar -sharedDb -inMemory >/dev/null
	@for p in $(ITEST_PORT_A) $(ITEST_PORT_B); do n=0; \
		until curl -s -o /dev/null http://localhost:$$p/; do \
			n=$$((n+1)); if [ $$n -ge $(ITEST_WAIT_TRIES) ]; then \
				echo "DynamoDB Local on :$$p did not start" >&2; \
				docker logs $(ITEST_DDB_A) >&2; docker logs $(ITEST_DDB_B) >&2; \
				docker rm -f $(ITEST_DDB_A) $(ITEST_DDB_B) >/dev/null; exit 1; fi; \
			sleep 0.5; done; done
	@status=0; \
	echo "DDB_TEST_ENDPOINT=http://localhost:$(ITEST_PORT_A) DDB_TEST_ENDPOINT_B=http://localhost:$(ITEST_PORT_B) $(GO) test -count=1 ./internal/streams/... ./internal/replicator/... ./internal/mvworker/..."; \
	DDB_TEST_ENDPOINT=http://localhost:$(ITEST_PORT_A) DDB_TEST_ENDPOINT_B=http://localhost:$(ITEST_PORT_B) \
		$(GO) test -count=1 ./internal/streams/... ./internal/replicator/... ./internal/mvworker/... || status=1; \
	echo "DYNAMODB_TEST_ENDPOINT=http://localhost:$(ITEST_PORT_B) $(GO) test -count=1 ./internal/storage/dynamo/..."; \
	DYNAMODB_TEST_ENDPOINT=http://localhost:$(ITEST_PORT_B) \
		$(GO) test -count=1 ./internal/storage/dynamo/... || status=1; \
	docker rm -f $(ITEST_DDB_A) $(ITEST_DDB_B) >/dev/null; exit $$status

# Local stack. PROFILES=compat adds the reference ClickHouse.
up:
	$(COMPOSE_UP) up -d --build --wait

up-single:
	$(COMPOSE) --profile single up -d --build --wait dynamodb-single chserver-single

down:
	$(COMPOSE) --profile compat --profile single down -v --remove-orphans

logs:
	$(COMPOSE) --profile compat --profile single logs -f --tail=200

# The same logs without following (for CI failure reports).
logs-dump:
	$(COMPOSE) --profile compat --profile single logs --no-color --timestamps --tail=1000

ps:
	$(COMPOSE) --profile compat --profile single ps

# Test suites against the running stack (make up first).
e2e:
	$(GO) test -tags e2e -count=1 -v -timeout 20m ./tests/e2e/...

compat:
	$(COMPOSE) --profile compat up -d --wait clickhouse
	$(GO) test -tags compat -count=1 -v ./tests/compat/...

multiregion:
	$(GO) test -tags multiregion -count=1 -v -timeout 20m ./tests/multiregion/...

# Comparison with ClickHouse (cmd/chbench, docs/clickhouse-comparison.md):
# single-region twinhouse on DynamoDB Local and a ClickHouse, in their own
# compose project and ports so no development stack is shared. Each side is
# capped at 4 CPUs and 4 GiB (deploy/local/bench.yml) unless CHBENCH_UNCAPPED=1.
#   make chbench-up && make chbench
#   make chbench CHBENCH_ARGS='-size M -only R2,R5'
CHBENCH_PROJECT  ?= twinhouse-bench
CHBENCH_TH_PORT  ?= 38125
CHBENCH_CH_PORT  ?= 38123
CHBENCH_DDB_PORT ?= 38002
CHBENCH_UNCAPPED ?=
CHBENCH_ARGS     ?=
CHBENCH_COMPOSE  := SINGLE_HTTP_PORT=$(CHBENCH_TH_PORT) CH_REF_PORT=$(CHBENCH_CH_PORT) \
	DDB_SINGLE_PORT=$(CHBENCH_DDB_PORT) TWINHOUSE_IMAGE=twinhouse:bench \
	docker compose -p $(CHBENCH_PROJECT) -f $(COMPOSE_YML) \
	$(if $(CHBENCH_UNCAPPED),,-f deploy/local/bench.yml) --profile single --profile compat

chbench-up:
	$(CHBENCH_COMPOSE) up -d --build --wait dynamodb-single chserver-single clickhouse

chbench-down:
	$(CHBENCH_COMPOSE) down -v --remove-orphans

chbench:
	$(GO) run ./cmd/chbench -twinhouse http://localhost:$(CHBENCH_TH_PORT) \
		-clickhouse http://localhost:$(CHBENCH_CH_PORT) $(CHBENCH_ARGS)

# ---------------------------------------------------------------------------
# Serverless AWS deployment (deploy/aws-lambda, SAM): the API on Lambda behind a
# private REST API Gateway, stream worker / replicator / reconciler Lambdas.
# Run from the deploy/aws-lambda directory so samconfig.toml and .aws-sam/ live there.
#   make aws-validate                      # sam validate --lint (no credentials needed)
#   make aws-build                         # cross-compiles the four arm64 bootstraps
#   make aws-local-api                     # sam local start-api on :3000 against the
#                                          # twinhouse DynamoDB Local (make up first)
#   make aws-local-e2e                     # tests/e2e against the emulated API (in
#                                          # another terminal; session tests skip)
#   make aws-deploy ENV=us                 # deploy the home region (needs credentials
#   make aws-deploy ENV=eu                 # and the VpcEndpointId in samconfig, see deploy/aws-lambda/README.md)
#   make aws-deploy ENV=us SAM_CONFIG=samconfig.local.toml   # a git-ignored copy with secrets
SAM             ?= sam
SAM_DIR         ?= deploy/aws-lambda
SAM_CONFIG      ?= samconfig.toml
ENV             ?= us
SAM_DEPLOY_ARGS ?=
SAM_LOCAL_URL   ?= http://127.0.0.1:3000

aws-validate:
	cd $(SAM_DIR) && $(SAM) validate --lint

aws-build:
	cd $(SAM_DIR) && $(SAM) build

# Dummy credentials: DynamoDB Local accepts anything, and an expired SSO
# session on the host would otherwise stop the Lambda containers from starting.
# The API Lambda only checks that the system tables exist (the reconciler
# creates them), so one reconciler round runs first against the same
# DynamoDB Local ($(DDB_US_ENDPOINT), from the host). It does not read
# $(SAM_LOCAL_ENV): its settings below repeat those of local-env.json, and
# TABLE_PREFIX comes from make, so an env file with another TABLE_PREFIX
# needs the same TABLE_PREFIX=... on the make command line.
SAM_LOCAL_ENV   ?= local-env.json
aws-local-reconcile:
	env -u AWS_PROFILE -u AWS_SESSION_TOKEN AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local \
		REGION=us-east-1 DYNAMODB_ENDPOINT=$(DDB_US_ENDPOINT) TABLE_PREFIX=$(TABLE_PREFIX) GLOBAL_MODE=emulated \
		MVWORKER_FUNCTION_ARN= REPLICATOR_FUNCTION_ARN= $(GO) run ./cmd/reconciler-lambda -once

aws-local-api: aws-build aws-local-reconcile
	cd $(SAM_DIR) && env -u AWS_PROFILE -u AWS_SESSION_TOKEN AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local \
		AWS_DEFAULT_REGION=us-east-1 $(SAM) local start-api --env-vars $(SAM_LOCAL_ENV)

# The e2e suite against the emulated API (make aws-local-api running). The
# suite detects Lambda mode (session_id answered with NOT_IMPLEMENTED) and
# skips the tests of features the serverless deployment does not offer.
aws-local-e2e:
	TWINHOUSE_URL=$(SAM_LOCAL_URL) $(GO) test -tags e2e -count=1 -v ./tests/e2e/...

# Never run by the test suites: it needs AWS credentials and creates
# resources. Every parameter comes from $(SAM_CONFIG) (VpcEndpointId must be
# filled in there); `--parameter-overrides` is refused because the SAM CLI
# replaces the whole parameter list of the environment with it.
aws-deploy: aws-build
	$(if $(findstring --parameter-overrides,$(SAM_DEPLOY_ARGS)),$(error SAM_DEPLOY_ARGS must not contain --parameter-overrides: it REPLACES every parameter of [$(ENV).deploy.parameters]; edit $(SAM_DIR)/$(SAM_CONFIG) or deploy a git-ignored copy with SAM_CONFIG=samconfig.local.toml))
	cd $(SAM_DIR) && $(SAM) deploy --config-file $(SAM_CONFIG) --config-env $(ENV) $(SAM_DEPLOY_ARGS)

# ---------------------------------------------------------------------------
# Container AWS deployment (deploy/aws-ecs, plain CloudFormation deployed
# with `sam deploy`): chserver on ECS Fargate behind an internal ALB on
# :8123, the mvworker service and the replicator (home region).
#   make ecs-validate                      # sam validate --lint (no credentials needed)
#   make ecs-local                         # the rendered task definitions as local
#                                          # containers against the twinhouse DynamoDB Local,
#                                          # plus tests/e2e against them (make up first)
#   make image-push IMAGE_REPO=<account>.dkr.ecr.<region>.amazonaws.com/<repo> \
#        [IMAGE_TAG=v1] [PLATFORM=linux/arm64|linux/amd64]
#                                          # build and push (docker login to ECR first)
#   make ecs-deploy ENV=us                 # home region first, then ENV=eu; fill in the
#                                          # CHANGE_ME values of deploy/aws-ecs/samconfig.toml
#   make ecs-deploy ENV=us ECS_CONFIG=samconfig.local.toml   # a git-ignored copy
ECS_DIR         ?= deploy/aws-ecs
ECS_CONFIG      ?= samconfig.toml
ECS_DEPLOY_ARGS ?=
IMAGE_REPO      ?=
IMAGE_TAG       ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
# linux/arm64 matches the template's default CpuArchitecture=ARM64;
# linux/amd64 for X86_64.
PLATFORM        ?= linux/arm64

ecs-validate:
	cd $(ECS_DIR) && $(SAM) validate --lint --template-file template.yaml

ecs-local:
	$(GO) test -tags ecslocal -count=1 -v -timeout 30m ./$(ECS_DIR)/

# Never run by the test suites: it pushes to a registry.
image-push:
	$(if $(IMAGE_REPO),,$(error IMAGE_REPO=<account>.dkr.ecr.<region>.amazonaws.com/<repository> is required))
	docker buildx build --platform $(PLATFORM) -f deploy/Dockerfile -t $(IMAGE_REPO):$(IMAGE_TAG) --push .

# Never run by the test suites: it needs AWS credentials and creates
# resources. As for aws-deploy, every parameter comes from $(ECS_CONFIG) and
# `--parameter-overrides` is refused (it would replace the whole list).
ecs-deploy:
	$(if $(findstring --parameter-overrides,$(ECS_DEPLOY_ARGS)),$(error ECS_DEPLOY_ARGS must not contain --parameter-overrides: it REPLACES every parameter of [$(ENV).deploy.parameters]; edit $(ECS_DIR)/$(ECS_CONFIG) or deploy a git-ignored copy with ECS_CONFIG=samconfig.local.toml))
	cd $(ECS_DIR) && $(SAM) deploy --config-file $(ECS_CONFIG) --config-env $(ENV) $(ECS_DEPLOY_ARGS)
