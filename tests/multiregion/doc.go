//go:build multiregion

// Package multiregion is the multi-region (active-active) test suite of
// dynamodb-eventstream (DESIGN.md §1, §5, §6). It drives two regions of the
// local compose stack — chserver-us / chserver-eu, mvworker-us / mvworker-eu,
// two DynamoDB Local instances and the replicator that emulates global tables
// — and checks that both regions converge to exactly the same data:
//
//   - DDL replication (catalog, eu reconciler, eu mvworker) and DROP propagation
//   - write in one region, read in the other (source rows and rollups)
//   - concurrent writers in both regions on the same keys (no lost / double counts)
//   - exactly-once view application under kill/restart of mvworker-us/eu and
//     the replicator (checkpoint replay + inline application + stream)
//   - inline-vs-stream deduplication in the writer region
//   - ReplacingMergeTree(version) latest-wins across regions
//   - replication lag (REPLICATION_DELAY) and monotonic, never-overshooting reads
//   - region outage (chserver-eu + mvworker-eu stopped) and catch-up
//   - clickhouse-go (native API and database/sql over HTTP) parity across regions
//   - an optional soak / consistency checker (MR_SOAK=1)
//
// The fault-injection tests stop, kill and recreate containers, so the suite
// must run against its OWN compose project, never the shared development
// stack ("dhdev"). Start it with distinct host ports:
//
//	cd <repo root>
//	COMPOSE_PROJECT=dhmr US_HTTP_PORT=28123 EU_HTTP_PORT=28124 \
//	  DDB_US_PORT=28000 DDB_EU_PORT=28001 CH_REF_PORT=28125 make up
//
// and run the suite:
//
//	go test -tags multiregion -count=1 -v -timeout 20m ./tests/multiregion/...
//
// (or `make multiregion` with the MR_* variables below exported). The soak
// checker runs only with MR_SOAK=1 (duration MR_SOAK_DURATION, default 2m):
//
//	MR_SOAK=1 MR_SOAK_DURATION=3m go test -tags multiregion -count=1 -v \
//	  -run TestSoak -timeout 30m ./tests/multiregion/...
//
// Environment (defaults in parentheses):
//
//	MR_US_URL            chserver-us HTTP endpoint (http://localhost:28123)
//	MR_EU_URL            chserver-eu HTTP endpoint (http://localhost:28124)
//	MR_DDB_US            DynamoDB Local of us-east-1 (http://localhost:28000)
//	MR_DDB_EU            DynamoDB Local of eu-west-1 (http://localhost:28001)
//	MR_COMPOSE_PROJECT   compose project driven by fault tests (dhmr)
//	MR_COMPOSE_FILE      compose file (deploy/docker-compose.yml under the repo root)
//	MR_TABLE_PREFIX      physical table prefix of the stack (chddb_)
//	MR_CONVERGE_TIMEOUT  convergence deadline per check, Go duration (60s)
//	MR_PARALLEL          max scenario tests running at once (4)
//	MR_SOAK, MR_SOAK_DURATION   see above
//
// DynamoDB Local gets markedly slower as stream and item history piles up
// (measured: a 150-row insert with an inline view went from ~1s on a fresh
// stack to ~25s after a few suite runs; whole-suite wall time went
// 172s -> 520s -> 1030s over three consecutive runs on one stack, with
// DynamoDB Local pinned at one CPU core during the run and idle otherwise,
// and the same with only the chserver/mvworker/replicator containers
// restarted, i.e. the cost is DynamoDB Local's retained state). Two
// consecutive runs on a freshly created stack pass. If convergence deadlines
// or insert timeouts start failing, recreate the stack first:
//
//	docker compose -p dhmr -f deploy/docker-compose.yml down -v
//	COMPOSE_PROJECT=dhmr US_HTTP_PORT=28123 ... make up
//
// Every test creates its own databases named mr_<scenario>_<random> and drops
// them at the end; tests never touch databases they did not create. Tests
// skip when the endpoints are unreachable; fault-injection tests also skip
// when docker compose cannot reach the project or its published ports do not
// match MR_US_URL / MR_EU_URL, and they refuse to run against "dhdev".
//
// Fault-injection tests are sequential (no t.Parallel) and restore the stack
// (all services running, REPLICATION_DELAY=0s) when they finish; the other
// tests run in parallel afterwards. The suite leaves the stack running.
package multiregion
