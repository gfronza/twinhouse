//go:build compat

// Package compat is the differential compatibility suite: every scenario in
// testdata/*.sql runs against the reference ClickHouse (CLICKHOUSE_REF_URL)
// and against twinhouse (TWINHOUSE_URL), and the outputs must be identical — raw HTTP
// bodies for text formats, decoded values + column names/types for
// clickhouse-go Native, and error codes / HTTP statuses for invalid
// statements. See corpus_test.go for the directive syntax.
//
//	go test -tags compat -count=1 ./tests/compat/...
//	go test -tags compat -count=1 ./tests/compat/... -run 'TestCompat/strings'
package compat

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func TestCompat(t *testing.T) {
	ref, our := requireTargets(t)
	files, err := filepath.Glob("testdata/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("no scenarios: %v", err)
	}
	for _, f := range files {
		stmts, err := parseCorpus(f)
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSuffix(filepath.Base(f), ".sql")
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runScenario(t, ref, our, scenarioDB(name), stmts)
		})
	}
}

var dbSanitize = regexp.MustCompile(`[^a-z0-9_]+`)

// scenarioDB is the per-run, per-scenario database name (identical on both
// servers so that outputs mentioning it compare equal).
func scenarioDB(scenario string) string {
	s := dbSanitize.ReplaceAllString(strings.ToLower(scenario), "_")
	return "cmp_" + runID + "_" + s
}

// createDB creates db on both servers and drops it at the end of the test.
func createDB(t *testing.T, ref, our *target, db string) {
	t.Helper()
	ctx := context.Background()
	for _, tg := range []*target{ref, our} {
		tg.mustExec(t, ctx, "CREATE DATABASE "+db, reqOpts{})
	}
	t.Cleanup(func() {
		if os.Getenv("COMPAT_KEEP") != "" {
			t.Logf("COMPAT_KEEP set: keeping database %s", db)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, tg := range []*target{ref, our} {
			if r := tg.do(ctx, "DROP DATABASE IF EXISTS "+db, reqOpts{}); r.failed() {
				t.Errorf("%s: drop %s: %s", tg.name, db, r.describe())
			}
		}
	})
}

type scenario struct {
	ref, our         *target
	db               string
	refConn, ourConn driver.Conn
}

func runScenario(t *testing.T, ref, our *target, db string, stmts []*stmt) {
	createDB(t, ref, our, db)
	sc := &scenario{ref: ref, our: our, db: db}
	var err error
	if sc.refConn, err = ref.open(db); err != nil {
		t.Fatalf("open reference: %v", err)
	}
	if sc.ourConn, err = our.open(db); err != nil {
		t.Fatalf("open twinhouse: %v", err)
	}
	t.Cleanup(func() { sc.refConn.Close(); sc.ourConn.Close() })

	seen := map[string]int{}
	for _, s := range stmts {
		sql := strings.ReplaceAll(s.sql, "$DB", db)
		s = withDBParams(s, db)
		loc := fmt.Sprintf("%s:%d", filepath.Base(s.file), s.line)
		if s.kind == kindExec {
			sc.exec(t, s, sql, loc)
			continue
		}
		name := s.name
		if seen[name]++; seen[name] > 1 {
			name = fmt.Sprintf("%s#%d", name, seen[name])
		}
		t.Run(name, func(t *testing.T) {
			switch s.kind {
			case kindQuery:
				sc.query(t, s, sql, loc)
			case kindError:
				sc.errorCase(t, s, sql, loc)
			case kindNotImpl:
				sc.notImplemented(t, s, sql, loc, 48)
			case kindUnsupported:
				sc.notImplemented(t, s, sql, loc, 46, 48)
			}
		})
	}
}

func (sc *scenario) opts(s *stmt) reqOpts {
	return reqOpts{db: sc.db, params: s.params, settings: s.settings, gzip: s.gzip, split: s.split}
}

// exec runs a setup/DML statement on both servers; both must succeed, or the
// rest of the scenario is meaningless.
func (sc *scenario) exec(t *testing.T, s *stmt, sql, loc string) {
	t.Helper()
	ctx := context.Background()
	r := sc.ref.do(ctx, sql, sc.opts(s))
	if r.failed() {
		t.Fatalf("%s: setup statement failed on the REFERENCE (harness/corpus bug):\n%s\n  -> %s", loc, sql, r.describe())
	}
	if s.refOnly {
		return
	}
	o := sc.our.do(ctx, sql, sc.opts(s))
	if o.failed() {
		t.Fatalf("%s: setup statement failed on twinhouse (reference succeeded):\n%s\n  -> %s", loc, sql, o.describe())
	}
}

func (sc *scenario) query(t *testing.T, s *stmt, sql, loc string) {
	ctx := context.Background()
	formats := s.formatsOrDefault()
	if m := explicitFormatRe.FindStringSubmatch(sql); m != nil {
		formats = []string{"(explicit " + m[1] + ")"}
	}
	for _, f := range formats {
		f := f
		t.Run(f, func(t *testing.T) {
			var want, got string
			if f == "Native" {
				w := nativeQuery(ctx, sc.refConn, sql, s)
				if w.err != nil {
					t.Fatalf("%s: reference Native query failed (corpus bug?): %v\n%s", loc, w.err, sql)
				}
				g := nativeQuery(ctx, sc.ourConn, sql, s)
				want, got = w.render(), g.render()
				if g.err != nil {
					got += "\n" + g.err.Error()
				}
			} else {
				q := sql
				explicit := strings.HasPrefix(f, "(explicit ")
				if !explicit {
					q = sql + "\nFORMAT " + f
				}
				fmtName := f
				if explicit {
					fmtName = explicitFormatRe.FindStringSubmatch(sql)[1]
				}
				w := sc.ref.do(ctx, q, sc.opts(s))
				if w.failed() {
					t.Fatalf("%s: reference failed (corpus bug?):\n%s\n  -> %s", loc, q, w.describe())
				}
				g := sc.our.do(ctx, q, sc.opts(s))
				want = renderHTTP(fmtName, w, s.unordered)
				got = renderHTTP(fmtName, g, s.unordered)
			}
			if want == got {
				if s.deviation != "" {
					t.Logf("%s: known deviation (%s) did not reproduce", loc, s.deviation)
				}
				return
			}
			msg := fmt.Sprintf("%s: MISMATCH [%s]\nquery:\n%s\n%s", loc, f, indent(sql), unifiedDiff(want, got))
			if s.deviation != "" {
				t.Logf("known deviation (%s):\n%s", s.deviation, msg)
				return
			}
			t.Error(msg)
		})
	}
}

// renderHTTP turns a response into the text that is compared.
func renderHTTP(format string, r httpResult, unordered bool) string {
	if r.failed() {
		return "ERROR " + r.describe() + "\n"
	}
	return fmt.Sprintf("HTTP %d\nContent-Type: %s\nX-ClickHouse-Format: %s\n\n%s",
		r.status, r.ctype, r.xformat, normalizeBody(format, r.body, unordered))
}

func (sc *scenario) errorCase(t *testing.T, s *stmt, sql, loc string) {
	ctx := context.Background()
	w := sc.ref.do(ctx, sql, sc.opts(s))
	if !w.failed() {
		t.Fatalf("%s: expected an error but the REFERENCE succeeded (corpus bug):\n%s\n  -> %s", loc, sql, truncate(string(w.body), 400))
	}
	if s.errCode != 0 && w.code != s.errCode {
		t.Fatalf("%s: reference returned code %d, corpus expects %d (corpus bug): %s", loc, w.code, s.errCode, w.describe())
	}
	g := sc.our.do(ctx, sql, sc.opts(s))
	if !g.failed() {
		t.Errorf("%s: reference fails with code %d (HTTP %d) but twinhouse SUCCEEDED\nquery:\n%s\nreference: %s\ntwinhouse body: %s",
			loc, w.code, w.status, indent(sql), w.describe(), truncate(string(g.body), 400))
		return
	}
	if g.code != w.code || g.status != w.status {
		t.Errorf("%s: error mismatch\nquery:\n%s\nreference: code=%d HTTP %d: %s\ntwinhouse:     code=%d HTTP %d: %s",
			loc, indent(sql), w.code, w.status, firstLine(w.body), g.code, g.status, firstLine(g.body))
	}
	// clickhouse-go must surface the same code.
	if s.split || s.gzip {
		return
	}
	wn := nativeQuery(ctx, sc.refConn, sql, s)
	gn := nativeQuery(ctx, sc.ourConn, sql, s)
	if wn.code != 0 && gn.code != wn.code {
		t.Errorf("%s: clickhouse-go error code mismatch: reference %d (%v), twinhouse %d (%v)", loc, wn.code, wn.err, gn.code, gn.err)
	}
}

// notImplemented checks a documented limitation: twinhouse must refuse the
// statement with one of the given codes instead of returning a wrong result.
func (sc *scenario) notImplemented(t *testing.T, s *stmt, sql, loc string, codes ...int) {
	ctx := context.Background()
	g := sc.our.do(ctx, sql, sc.opts(s))
	if !slices.Contains(codes, g.code) {
		t.Errorf("%s: documented limitation (%s): twinhouse must answer code %v, got %s\nquery:\n%s",
			loc, s.deviation, codes, g.describe(), indent(sql))
	}
}

func indent(s string) string {
	return "    " + strings.ReplaceAll(s, "\n", "\n    ")
}

func firstLine(b []byte) string {
	s, _, _ := strings.Cut(strings.TrimSpace(string(b)), "\n")
	return truncate(s, 300)
}

// withDBParams returns s with $DB in parameter values replaced by db (e.g. a
// {d:Identifier} parameter naming the scenario database).
func withDBParams(s *stmt, db string) *stmt {
	if !strings.Contains(fmt.Sprint(s.params), "$DB") {
		return s
	}
	cp := *s
	cp.params = make([]kv, len(s.params))
	for i, p := range s.params {
		cp.params[i] = kv{p.k, strings.ReplaceAll(p.v, "$DB", db)}
	}
	return &cp
}
