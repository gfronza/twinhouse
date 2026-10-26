// Package e2e is the end-to-end suite (build tag e2e): it drives a running
// chserver (TWINHOUSE_URL, default http://localhost:8123 — the us server of the
// twinhouse compose stack) exactly like an application would: clickhouse-go v2
// over HTTP (database/sql and the native API) and plain net/http like a
// Python loader. It relies on a single region only (no replication), so it
// also runs against `make up-single` (TWINHOUSE_URL=http://localhost:8125).
//
// Every test creates its own uniquely named database(s) (e2e_<run>_<name>)
// and drops them at the end.
//
//	go test -tags e2e -count=1 ./tests/e2e/...
//
// Against the serverless deployment (the API Lambda behind API Gateway, or
// `make aws-local-api`, TWINHOUSE_URL=http://127.0.0.1:3000) the suite detects
// Lambda mode (a session_id request answered with NOT_IMPLEMENTED, or
// E2E_LAMBDA=1), sends Accept: */* so API Gateway returns binary responses
// decoded, and skips the tests of features that deployment does not offer
// (HTTP sessions).
package e2e
