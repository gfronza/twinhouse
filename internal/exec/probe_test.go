package exec

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

// TestProbe is a development aid: it runs every query in $PROBE_FILE (one
// per line; lines starting with "fixture:" name fixtures) against this
// engine and $ORACLE_URL and prints the differences. Skipped by default.
func TestProbe(t *testing.T) {
	file, base := os.Getenv("PROBE_FILE"), os.Getenv("ORACLE_URL")
	if file == "" || base == "" {
		t.Skip("PROBE_FILE and ORACLE_URL not set")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var fx []string
	var queries []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "" || strings.HasPrefix(line, "--"):
		case strings.HasPrefix(line, "fixture:"):
			fx = append(fx, strings.TrimSpace(strings.TrimPrefix(line, "fixture:")))
		default:
			queries = append(queries, line)
		}
	}
	for i, q := range queries {
		c := oracleCase{name: "probe_" + itoa(i), fixtures: fx, query: q}
		db := "oracle_" + c.name
		post := func(sql, database string) string {
			v := url.Values{}
			if database != "" {
				v.Set("database", database)
			}
			resp, err := http.Post(base+"/?"+v.Encode(), "text/plain", strings.NewReader(sql))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				return "ERROR " + resp.Header.Get("X-ClickHouse-Exception-Code") + "\n"
			}
			return string(b)
		}
		post("DROP DATABASE IF EXISTS "+db, "")
		post("CREATE DATABASE "+db, "")
		te := newTestEnv(t)
		for _, f := range fx {
			for _, st := range fixtures[f] {
				post(st, db)
				te.exec(st)
			}
		}
		want := post(q+" FORMAT TabSeparatedWithNamesAndTypes", db)
		post("DROP DATABASE IF EXISTS "+db, "")
		var got string
		if res, err := te.run(q); err != nil {
			got = errorGolden(err)
		} else {
			got = renderTSV(t, res)
		}
		if got != want {
			t.Errorf("DIFF %s\n--- got\n%s--- want\n%s", q, got, want)
		}
	}
}
