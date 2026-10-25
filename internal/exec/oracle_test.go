package exec

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"dynamodb-eventstream/internal/cherr"
)

// The oracle corpus: every case runs its fixtures and query on this engine
// and the output (TabSeparatedWithNamesAndTypes) is compared with a golden
// recorded from a real ClickHouse server.
//
// Regenerate goldens (needs a ClickHouse server):
//
//	ORACLE_URL=http://localhost:18128 go test ./internal/exec -run TestOracle -update-oracle
var updateOracle = flag.Bool("update-oracle", false, "record oracle goldens from ORACLE_URL")

type oracleCase struct {
	name     string
	fixtures []string
	query    string
	params   map[string]string
	// unordered compares rows as a multiset (query has no total ORDER BY).
	unordered bool
}

func goldenPath(name string) string { return filepath.Join("testdata", "oracle", name+".tsv") }

func TestOracle(t *testing.T) {
	for _, c := range oracleCorpus {
		t.Run(c.name, func(t *testing.T) {
			if *updateOracle {
				recordOracle(t, c)
			}
			want, err := os.ReadFile(goldenPath(c.name))
			if err != nil {
				t.Fatalf("missing golden (run with -update-oracle): %v", err)
			}
			te := newTestEnv(t)
			for _, f := range c.fixtures {
				for _, st := range fixtures[f] {
					te.exec(st)
				}
			}
			var params []string
			for k, v := range c.params {
				params = append(params, k, v)
			}
			res, err := te.run(c.query, params...)
			var got string
			if err != nil {
				got = errorGolden(err)
			} else {
				got = renderTSV(t, res)
			}
			if c.unordered {
				got, want = sortedLines(got), []byte(sortedLines(string(want)))
			}
			if got != string(want) {
				t.Errorf("query: %s\n--- got\n%s--- want\n%s", c.query, got, want)
			}
		})
	}
}

// errorGolden renders an error as "ERROR <code>\n" (the oracle's exception
// code), so error behaviour is part of the corpus.
func errorGolden(err error) string {
	return fmt.Sprintf("ERROR %d\n", cherr.As(err).Code)
}

func sortedLines(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= 2 {
		return s
	}
	head, body := lines[:2], append([]string(nil), lines[2:]...)
	sort.Strings(body)
	return strings.Join(append(head, body...), "\n") + "\n"
}

func recordOracle(t *testing.T, c oracleCase) {
	base := os.Getenv("ORACLE_URL")
	if base == "" {
		t.Skip("ORACLE_URL not set")
	}
	db := "oracle_" + strings.ReplaceAll(c.name, "-", "_")
	post := func(q string, params map[string]string, database string) (string, int, string) {
		v := url.Values{}
		if database != "" {
			v.Set("database", database)
		}
		for k, val := range params {
			v.Set("param_"+k, val)
		}
		resp, err := http.Post(base+"/?"+v.Encode(), "text/plain", strings.NewReader(q))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b), resp.StatusCode, resp.Header.Get("X-ClickHouse-Exception-Code")
	}
	mustOK := func(q, database string) {
		if body, st, _ := post(q, nil, database); st != 200 {
			t.Fatalf("oracle setup %q: %s", q, body)
		}
	}
	mustOK("DROP DATABASE IF EXISTS "+db, "")
	mustOK("CREATE DATABASE "+db, "")
	defer mustOK("DROP DATABASE IF EXISTS "+db, "")
	for _, f := range c.fixtures {
		for _, st := range fixtures[f] {
			mustOK(st, db)
		}
	}
	q := strings.TrimRight(strings.TrimSpace(c.query), ";")
	body, st, code := post(q+" FORMAT TabSeparatedWithNamesAndTypes", c.params, db)
	out := body
	if st != 200 {
		out = "ERROR " + code + "\n"
	}
	if err := os.MkdirAll(filepath.Dir(goldenPath(c.name)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(goldenPath(c.name), []byte(out), 0o644); err != nil {
		t.Fatal(err)
	}
}
