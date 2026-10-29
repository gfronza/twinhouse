#!/bin/sh
# Compares the Go benchmarks of a base revision with the current code
# (make bench-compare; the Makefile sets every variable below).
#
# The base revision is checked out in a temporary git worktree. Both sides are
# compiled once, then run in alternating pairs (base/new, new/base, ...), each
# pair started once the 1-minute load average is below BENCH_MAX_LOAD, so
# background load spreads over both sides. benchstat compares the samples.
#
# Fails when a benchmark of BENCH_GATE_PKGS is slower (sec/op) by more than
# BENCH_MAX_REGRESSION percent and benchstat finds the change significant.
# BENCH_REPORT_PKGS are compared and printed, never gated.
set -eu

GO=${GO:-go}
: "${BENCHSTAT:?BENCHSTAT must be set (run through make bench-compare)}"
BASE=${BASE:-}
BENCH=${BENCH:-.}
BENCH_TIME=${BENCH_TIME:-1s}
BENCH_PAIRS=${BENCH_PAIRS:-8}
BENCH_MAX_LOAD=${BENCH_MAX_LOAD:-8}
BENCH_LOAD_WAIT=${BENCH_LOAD_WAIT:-900}
BENCH_MAX_REGRESSION=${BENCH_MAX_REGRESSION:-3}
BENCH_GATE_PKGS=${BENCH_GATE_PKGS:-./internal/exec ./internal/server}
BENCH_REPORT_PKGS=${BENCH_REPORT_PKGS:-}
BENCH_OUT=${BENCH_OUT:-bin/bench-compare}

die() { echo "bench-compare: $*" >&2; exit 2; }

root=$(git rev-parse --show-toplevel)
cd "$root"

# The new side is always the working tree. Without BASE, a dirty tree is
# compared with HEAD and a clean one (which is HEAD) with HEAD~1.
if [ -n "$(git status --porcelain)" ]; then new_desc="working tree"; else new_desc="HEAD"; fi
if [ -z "$BASE" ]; then
	if [ "$new_desc" = HEAD ]; then BASE=HEAD~1; else BASE=HEAD; fi
fi
base_rev=$(git rev-parse --verify --quiet "$BASE^{commit}") || die "unknown revision: $BASE"
echo "bench-compare: $BASE ($(git rev-parse --short "$base_rev")) vs $new_desc ($(git rev-parse --short HEAD)$( [ "$new_desc" = HEAD ] || echo ' + changes'))"

work=$(mktemp -d "${TMPDIR:-/tmp}/bench-compare.XXXXXX")
cleanup() {
	git worktree remove --force "$work/base" >/dev/null 2>&1 || true
	rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

git worktree add --detach --quiet "$work/base" "$base_rev"

# bin_name ./internal/exec -> internal_exec.test
bin_name() { echo "$1" | sed 's|^\./||; s|/|_|g; s|$|.test|'; }

# build <tree> <side>: one test binary per package. A package without test
# files yields no binary; a package missing from the base tree is skipped.
build() {
	mkdir -p "$work/$2"
	for pkg in $BENCH_GATE_PKGS $BENCH_REPORT_PKGS; do
		if [ ! -d "$1/$pkg" ]; then
			echo "bench-compare: $pkg does not exist in $2, skipped there" >&2
			continue
		fi
		(cd "$1" && $GO test -c -o "$work/$2/$(bin_name "$pkg")" "$pkg") ||
			die "building $pkg ($2) failed"
	done
}
echo "bench-compare: building"
build "$work/base" base
build "$root" new

# BENCH_OUT is emptied first, so it must be a subdirectory of the repository.
case "$BENCH_OUT" in
"" | . | ./ | /* | ..* | */..* ) die "BENCH_OUT must be a relative subdirectory, got '$BENCH_OUT'" ;;
esac
out=$root/$BENCH_OUT
rm -rf "$out"
mkdir -p "$out"

# run_group <side> <tree> <group> <pkgs>: one sample of every benchmark, run
# from the package directory as go test does.
run_group() {
	for pkg in $4; do
		bin=$work/$1/$(bin_name "$pkg")
		[ -x "$bin" ] || continue
		(cd "$2/$pkg" && "$bin" -test.run '^$' -test.bench "$BENCH" -test.benchmem \
			-test.count 1 -test.benchtime "$BENCH_TIME" -test.timeout 60m) >>"$out/$1-$3.txt" ||
			die "benchmarks of $pkg ($1) failed; output in $out/$1-$3.txt"
	done
}
run_side() {
	if [ "$1" = base ]; then tree=$work/base; else tree=$root; fi
	run_group "$1" "$tree" gate "$BENCH_GATE_PKGS"
	run_group "$1" "$tree" report "$BENCH_REPORT_PKGS"
}

load1() {
	if [ -r /proc/loadavg ]; then cut -d' ' -f1 /proc/loadavg
	else sysctl -n vm.loadavg | awk '{print $2}'; fi
}
# BENCH_LOAD_WAIT is the waiting budget of the whole run: once it is spent,
# the remaining pairs start at whatever load there is.
noisy=0
budget=$BENCH_LOAD_WAIT
wait_load() {
	while awk -v l="$(load1)" -v m="$BENCH_MAX_LOAD" 'BEGIN { exit !(l >= m) }'; do
		if [ "$budget" -le 0 ]; then
			noisy=1
			return
		fi
		sleep 10
		budget=$((budget - 10))
	done
}

i=1
while [ "$i" -le "$BENCH_PAIRS" ]; do
	wait_load
	echo "bench-compare: pair $i/$BENCH_PAIRS (load $(load1)$( [ "$budget" -gt 0 ] || echo ", load wait budget spent"))"
	if [ $((i % 2)) -eq 1 ]; then run_side base; run_side new; else run_side new; run_side base; fi
	i=$((i + 1))
done

has_samples() { grep -qs '^Benchmark' "$out/base-$1.txt" "$out/new-$1.txt"; }

for group in gate report; do
	has_samples "$group" || continue
	echo
	if [ "$group" = gate ]; then echo "== gated: $BENCH_GATE_PKGS"; else echo "== report only: $BENCH_REPORT_PKGS"; fi
	$BENCHSTAT base="$out/base-$group.txt" new="$out/new-$group.txt"
done

# Gate: rows of the sec/op tables whose change is significant (benchstat
# prints "~" otherwise) and above the limit. The geomean is not tested.
status=0
if has_samples gate; then
	$BENCHSTAT -format csv base="$out/base-gate.txt" new="$out/new-gate.txt" 2>/dev/null >"$out/gate.csv"
	awk -F, -v max="$BENCH_MAX_REGRESSION" '
		/^pkg: / { pkg = substr($0, 6) }
		$6 == "vs base" { metric = $2; next }
		metric == "sec/op" && $1 != "geomean" && $6 ~ /^\+[0-9.]+%$/ {
			v = $6; gsub(/[+%]/, "", v)
			if (v + 0 > max + 0) { printf "  %s %s %s (%s)\n", pkg, $1, $6, $7; bad = 1 }
		}
		END { exit bad }' "$out/gate.csv" >"$out/regressions.txt" || status=1
fi

echo
echo "samples: $out"
if [ "$noisy" -eq 1 ]; then
	echo "warning: some pairs started at load >= $BENCH_MAX_LOAD; results may be noisy" >&2
fi
if [ "$status" -ne 0 ]; then
	echo "bench-compare: FAIL, slower by more than $BENCH_MAX_REGRESSION%:" >&2
	cat "$out/regressions.txt" >&2
	exit 1
fi
echo "bench-compare: OK (no gated benchmark slower by more than $BENCH_MAX_REGRESSION%)"
