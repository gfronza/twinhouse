# Benchmarks

The Go benchmarks measure the CPU time and allocations of the SQL parser,
the query engine, the output formats and the HTTP server. They run on the
in-memory store (`internal/storage/memstore`), so they say nothing about
DynamoDB latency or cost: see [Costs, limits and operations](operations.md)
for those. [Comparison with ClickHouse](clickhouse-comparison.md) runs one
workload end to end against twinhouse on DynamoDB Local and against
ClickHouse.

A change must not make the exec or server benchmarks more than 3% slower
than the previous commit. `make bench-compare` checks that.

## Running them (`make bench`)

```sh
make bench                                                  # every benchmark, about a minute
make bench BENCH=SelectStream BENCH_PKGS=./internal/server/ # one benchmark
make bench BENCH='OrderBy|Distinct' BENCH_COUNT=6           # six samples each
```

`make bench` runs `go test -run '^$' -bench ... -benchmem`, so no tests run
and every result includes `B/op` and `allocs/op`. Make expands `$` in
variables, so write a `$` anchor in `BENCH` as `$$` (`BENCH='OrderBy$$'`).

| Variable | Default | Meaning |
|---|---|---|
| `BENCH` | `.` | The `-bench` regexp. |
| `BENCH_PKGS` | `./...` | The packages. |
| `BENCH_COUNT` | `1` | Samples per benchmark (`-count`). |
| `BENCH_TIME` | `1s` | `-benchtime`. |
| `BENCH_TIMEOUT` | `60m` | `-timeout`. |

## What they cover

| Package | Benchmark | What it runs |
|---|---|---|
| `internal/server` | `SelectStream` | A 1M-row `SELECT` served over HTTP as TSV into a discarding response, without a memory limit and with an 8 GiB one. |
| `internal/exec` | `Collect` | Collecting a 1M-row result, with memory accounting off and on. |
| | `InsertWithViews` | `INSERT` of 10k rows into a table with three materialized-view rollups. |
| | `Insert` | `INSERT ... SELECT` of 10k rows into a plain table. |
| | `GroupBy` | `GROUP BY` over 100k rows with `sum`, `uniqExact` and `argMax`. |
| | `OrderBy`, `Distinct` | A full sort of 100k rows; `DISTINCT` keeping 50k of 100k. |
| | `OrderByLimit` | Top-N (`LIMIT 10 OFFSET 5`) over 100k rows. |
| | `OrderByLargeLimit` | Limits near or past the row count: presorted with `OFFSET`, reversed, shuffled, and a limit that keeps every row. |
| | `OrderByLimitStrings` | Keeping half of 200k shuffled rows with long or short strings. |
| | `HashJoin` | `LEFT JOIN` with an extra condition and an inner join, 100k × 20k rows. |
| | `CrossJoinLimit` | `CROSS JOIN` of 1k × 100k rows stopped by `LIMIT 1`. |
| | `ArrayJoin` | `ARRAY JOIN` of two arrays over 4k rows. |
| | `InSubquery` | `IN (SELECT ...)` with a 100k-element set, numbers and strings. |
| | `StringPieces` | `splitByChar`, `substring` and `trim` over 200k rows. |
| | `CaseMapUTF8` | `lowerUTF8` / `upperUTF8` on ASCII and on mappings that keep, shrink or grow the byte length. |
| `internal/format` | `NativeEncode100k`, `NativeDecode100k` | The Native format over 100k rows. |
| `internal/sqlparse` | `ParseInsertValues`, `ParseSelect` | Parsing a 10k-row `INSERT ... VALUES` and a typical `SELECT`. |

Some report `rows/s` as well. The engine benchmarks run with the test
memory limit, so memory accounting is on.

## Comparing two revisions (`make bench-compare`)

```sh
make bench-compare                          # uncommitted changes vs HEAD (or HEAD vs HEAD~1 on a clean tree)
make bench-compare BASE=main                # the working tree vs main
make bench-compare BENCH=SelectStream       # one benchmark, a minute or two
```

`scripts/bench-compare.sh` does the following:

1. Checks out `BASE` in a temporary git worktree. The other side is always
   the working tree. Without `BASE`, a tree with changes is compared with
   `HEAD` and a clean tree with `HEAD~1`.
2. Compiles the benchmark binaries of both sides once.
3. Runs them in alternating pairs (base then new, new then base, ...).
   Each pair waits until the 1-minute load average is below
   `BENCH_MAX_LOAD`, so background load lands on both sides alike.
4. Prints the [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)
   comparison of time, bytes and allocations.
5. Fails if a benchmark of `BENCH_GATE_PKGS` is slower (`sec/op`) by more
   than `BENCH_MAX_REGRESSION` percent and benchstat finds the change
   significant. `BENCH_REPORT_PKGS` are printed and never fail it.

With the defaults a run takes about 13 minutes, plus any wait for the load
to drop. The samples stay in `bin/bench-compare/` (`base-gate.txt`,
`new-gate.txt`, ...), so you can run benchstat on them again. It is not part
of `make verify` or CI: shared CI runners are too noisy for a 3% limit.

| Variable | Default | Meaning |
|---|---|---|
| `BASE` | `HEAD` or `HEAD~1` | The revision to compare against. |
| `BENCH` | `.` | The `-bench` regexp. |
| `BENCH_TIME` | `1s` | `-benchtime` of each sample. |
| `BENCH_PAIRS` | `8` | Base/new pairs, so samples per side. |
| `BENCH_MAX_LOAD` | `8` | A pair starts only below this 1-minute load average. |
| `BENCH_LOAD_WAIT` | `900` | Seconds the whole run may spend waiting for the load; after that, pairs start at any load and a warning is printed. |
| `BENCH_MAX_REGRESSION` | `3` | The slowdown, in percent, that fails the run. |
| `BENCH_GATE_PKGS` | `./internal/exec ./internal/server` | Packages that can fail the run. |
| `BENCH_REPORT_PKGS` | `./internal/format ./internal/sqlparse` | Packages that are only reported. |
| `BENCH_OUT` | `bin/bench-compare` | Where the samples go (emptied first). |
| `BENCHSTAT_VERSION` | pinned in the Makefile | The benchstat version, run with `go run`. |

### Reading the result

In benchstat's table, `~` means the difference is within the noise. A
percentage with `p=` below 0.05 is a significant change. Only rows of the
`sec/op` table can fail the run; the `geomean` row is a summary and is never
gated.

### Noise

On a machine that is busy with other work, timings vary by several percent
from run to run. A single unpaired run can show a false 5% slowdown, and a
real 3–6% slowdown can be lost in the noise. To get a result you can trust:

- Stop other heavy work (Docker stacks, builds) while it runs.
- Compare in pairs, as `make bench-compare` does; never trust two single runs.
- Add samples with `BENCH_PAIRS=12` when a change is close to the limit.
- Treat a run that printed the load warning as a hint, not a verdict.

## Adding a benchmark

- Put it next to the code it measures, in a `*_bench_test.go` file or the
  package's existing test file, and say in a comment what it runs.
- Use `for b.Loop()`, keep setup before the loop, and call `b.ReportAllocs()`
  where allocations matter. Report `rows/s` with `b.ReportMetric` when the
  row count is fixed.
- Benchmarks in `internal/exec` and `internal/server` are gated by
  `make bench-compare`. Keep them deterministic and long enough to time
  (tens of milliseconds per operation or more) so that the noise stays low.
- Add it to the table above.
