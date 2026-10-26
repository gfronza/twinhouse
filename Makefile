# Developer entry points.
#   deploy/local/docker-compose.yml  the local stack (targets up, down, e2e, ...)
#   deploy/aws-lambda/               serverless AWS deployment, SAM (targets aws-*)
#   deploy/aws-ecs/                  container AWS deployment, ECS Fargate,
#                                    plain CloudFormation (targets ecs-*, image-push)

GO              ?= go
COMPOSE_PROJECT ?= dhdev
PROFILES        ?=
COMPOSE_YML     ?= deploy/local/docker-compose.yml
COMPOSE         := docker compose -p $(COMPOSE_PROJECT) -f $(COMPOSE_YML)
COMPOSE_UP      := $(COMPOSE) $(foreach p,$(PROFILES),--profile $(p))

# Endpoints of the running stack, exported to the e2e/compat/multiregion tests.
export CHDDB_URL          ?= http://localhost:8123
export CHDDB_EU_URL       ?= http://localhost:8124
export CLICKHOUSE_REF_URL ?= http://localhost:18123
export DDB_US_ENDPOINT    ?= http://localhost:18000
export DDB_EU_ENDPOINT    ?= http://localhost:18001
export TABLE_PREFIX       ?= chddb_

# DynamoDB Local containers for the stream/replicator integration tests.
ITEST_DDB_A    ?= chddb-itest-ddb-a
ITEST_DDB_B    ?= chddb-itest-ddb-b
ITEST_PORT_A   ?= 18101
ITEST_PORT_B   ?= 18102
DDB_LOCAL_IMG  ?= amazon/dynamodb-local:latest

.PHONY: all build image test vet fmt test-integration up up-single down logs ps e2e compat multiregion \
        aws-validate aws-build aws-local-reconcile aws-local-api aws-local-e2e aws-deploy \
        ecs-validate ecs-local image-push ecs-deploy

all: build test

build:
	$(GO) build ./...
	$(GO) build -o bin/ ./cmd/...

image:
	$(COMPOSE) build

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -l -w $$(git ls-files '*.go' 2>/dev/null || find . -name '*.go' -not -path './vendor/*')

# Streams + replicator tests against two throwaway DynamoDB Local instances.
test-integration:
	@docker rm -f $(ITEST_DDB_A) $(ITEST_DDB_B) >/dev/null 2>&1 || true
	docker run -d --name $(ITEST_DDB_A) -p $(ITEST_PORT_A):8000 $(DDB_LOCAL_IMG) -jar DynamoDBLocal.jar -sharedDb -inMemory >/dev/null
	docker run -d --name $(ITEST_DDB_B) -p $(ITEST_PORT_B):8000 $(DDB_LOCAL_IMG) -jar DynamoDBLocal.jar -sharedDb -inMemory >/dev/null
	@for p in $(ITEST_PORT_A) $(ITEST_PORT_B); do \
		until curl -s -o /dev/null http://localhost:$$p/; do sleep 0.5; done; done
	DDB_TEST_ENDPOINT=http://localhost:$(ITEST_PORT_A) DDB_TEST_ENDPOINT_B=http://localhost:$(ITEST_PORT_B) \
		$(GO) test -count=1 ./internal/streams/... ./internal/replicator/... ./internal/mvworker/...; \
		status=$$?; docker rm -f $(ITEST_DDB_A) $(ITEST_DDB_B) >/dev/null; exit $$status

# Local stack. PROFILES=compat adds the reference ClickHouse.
up:
	$(COMPOSE_UP) up -d --build --wait

up-single:
	$(COMPOSE) --profile single up -d --build --wait dynamodb-single chserver-single

down:
	$(COMPOSE) --profile compat --profile single down -v --remove-orphans

logs:
	$(COMPOSE) --profile compat --profile single logs -f --tail=200

ps:
	$(COMPOSE) --profile compat --profile single ps

# Test suites against the running stack (make up first).
e2e:
	$(GO) test -tags e2e -count=1 -v ./tests/e2e/...

compat:
	$(COMPOSE) --profile compat up -d --wait clickhouse
	$(GO) test -tags compat -count=1 -v ./tests/compat/...

multiregion:
	$(GO) test -tags multiregion -count=1 -v -timeout 20m ./tests/multiregion/...

# ---------------------------------------------------------------------------
# Serverless AWS deployment (deploy/aws-lambda, SAM): the API on Lambda behind a
# private REST API Gateway, stream worker / replicator / reconciler Lambdas.
# Run from the deploy/aws-lambda directory so samconfig.toml and .aws-sam/ live there.
#   make aws-validate                      # sam validate --lint (no credentials needed)
#   make aws-build                         # cross-compiles the four arm64 bootstraps
#   make aws-local-api                     # sam local start-api on :3000 against the
#                                          # dhdev DynamoDB Local (make up first)
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
	CHDDB_URL=$(SAM_LOCAL_URL) $(GO) test -tags e2e -count=1 -v ./tests/e2e/...

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
#                                          # containers against the dhdev DynamoDB Local,
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
