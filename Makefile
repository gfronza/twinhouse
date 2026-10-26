# Developer entry points. The local stack is deploy/local/docker-compose.yml.

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

.PHONY: all build image test vet fmt test-integration up up-single down logs ps e2e compat multiregion

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
