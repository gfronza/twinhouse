// Package e2e is the end-to-end suite (build tag e2e): it drives a running
// chserver (CHDDB_URL, default http://localhost:8123 — the us server of the
// dhdev compose stack) exactly like an application would: clickhouse-go v2
// over HTTP (database/sql and the native API) and plain net/http like a
// Python loader. It relies on a single region only (no replication), so it
// also runs against `make up-single` (CHDDB_URL=http://localhost:8125).
//
// Every test creates its own uniquely named database(s) (e2e_<run>_<name>)
// and drops them at the end.
//
//	go test -tags e2e -count=1 ./tests/e2e/...
package e2e
