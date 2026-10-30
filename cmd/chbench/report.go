package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// results is what a run produces: the Markdown tables on stdout and the
// raw samples as JSON.
type results struct {
	Context runContext    `json:"context"`
	Writes  []writeResult `json:"writes"`
	Reads   []readResult  `json:"reads"`
}

type runContext struct {
	Date        string      `json:"date"`
	Commit      string      `json:"commit"`
	Host        string      `json:"host"`
	Twinhouse   string      `json:"twinhouse_version"`
	ClickHouse  string      `json:"clickhouse_version"`
	Containers  []container `json:"containers"`
	Rows        int         `json:"rows"`
	Seed        uint64      `json:"seed"`
	LoadStart   string      `json:"load_avg_start"`
	LoadEnd     string      `json:"load_avg_end"`
	ViewCatchUp string      `json:"view_catch_up,omitempty"`
	Duration    string      `json:"duration"`
}

type writeResult struct {
	ID    string `json:"id"`
	What  string `json:"what"`
	Side  string `json:"side"`
	Rows  int    `json:"rows,omitempty"`
	MS    int64  `json:"ms"`
	Stats *stats `json:"latency,omitempty"`
	// Requests or rows per second over the side's own time.
	PerSec float64 `json:"per_sec"`
}

type readResult struct {
	ID     string             `json:"id"`
	Shape  string             `json:"shape"`
	SQL    string             `json:"sql_example"`
	Conc   int                `json:"concurrency"`
	Rounds int                `json:"rounds"`
	Sides  map[string]stats   `json:"sides"`
	QPS    map[string]float64 `json:"qps"`
}

type stats struct {
	N       int     `json:"n"`
	P50     float64 `json:"p50_ms"`
	P95     float64 `json:"p95_ms"`
	P99     float64 `json:"p99_ms"`
	Samples []int64 `json:"samples_us"`
}

func summarize(d []time.Duration) stats {
	s := slices.Clone(d)
	slices.Sort(s)
	pct := func(p float64) float64 {
		if len(s) == 0 {
			return 0
		}
		i := int(p*float64(len(s))+0.5) - 1
		return float64(s[min(max(i, 0), len(s)-1)].Microseconds()) / 1000
	}
	st := stats{N: len(s), P50: pct(.50), P95: pct(.95), P99: pct(.99)}
	for _, x := range d {
		st.Samples = append(st.Samples, x.Microseconds())
	}
	return st
}

func (r *results) addThroughput(id, what string, rows int, took map[string]time.Duration) {
	for _, side := range []string{"twinhouse", "clickhouse"} {
		d := took[side]
		r.Writes = append(r.Writes, writeResult{ID: id, What: what, Side: side, Rows: rows, MS: d.Milliseconds(), PerSec: float64(rows) / d.Seconds()})
	}
}

func (r *results) addLatency(id, what, side string, lat []time.Duration, wall time.Duration) {
	st := summarize(lat)
	r.Writes = append(r.Writes, writeResult{ID: id, What: what, Side: side, MS: wall.Milliseconds(), Stats: &st, PerSec: float64(len(lat)) / wall.Seconds()})
}

func (r *results) addRead(rc readCase, conc, rounds int, lat map[string][]time.Duration, wall map[string]time.Duration) {
	rr := readResult{ID: rc.id, Shape: rc.shape, SQL: rc.sql(newParams(0)), Conc: conc, Rounds: rounds,
		Sides: map[string]stats{}, QPS: map[string]float64{}}
	for side, l := range lat {
		rr.Sides[side] = summarize(l)
		rr.QPS[side] = float64(len(l)) / wall[side].Seconds()
	}
	r.Reads = append(r.Reads, rr)
}

func (r *results) writeJSON(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func ratio(a, b float64) string {
	if b == 0 {
		return "–"
	}
	x := a / b
	switch {
	case x >= 10:
		return fmt.Sprintf("%.0f×", x)
	case x >= 1:
		return fmt.Sprintf("%.1f×", x)
	default:
		return fmt.Sprintf("%.2f×", x)
	}
}

func ms(v float64) string {
	switch {
	case v >= 1000:
		return fmt.Sprintf("%.2f s", v/1000)
	case v >= 10:
		return fmt.Sprintf("%.0f ms", v)
	default:
		return fmt.Sprintf("%.1f ms", v)
	}
}

func count(n float64) string {
	switch {
	case n >= 1e6:
		return fmt.Sprintf("%.1fM", n/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1fk", n/1e3)
	default:
		return fmt.Sprintf("%.0f", n)
	}
}

// markdown prints the run as the tables docs/clickhouse-comparison.md uses.
func (r *results) markdown(w io.Writer) {
	c := r.Context
	fmt.Fprintf(w, "Run %s, commit %s, %d events (seed %d), %s.\n", c.Date, c.Commit, c.Rows, c.Seed, c.Duration)
	fmt.Fprintf(w, "Host: %s. Load average %s at the start, %s at the end.\n", c.Host, c.LoadStart, c.LoadEnd)
	fmt.Fprintf(w, "twinhouse %s, ClickHouse %s.\n", c.Twinhouse, c.ClickHouse)
	for _, k := range c.Containers {
		caps := "uncapped"
		if k.CPUs > 0 {
			caps = fmt.Sprintf("%.1f CPUs, %.0f GiB", k.CPUs, k.MemGiB)
		}
		fmt.Fprintf(w, "- `%s` (%s): %s\n", k.Name, k.Image, caps)
	}

	var bulk, small []writeResult
	for _, x := range r.Writes {
		if x.Stats == nil {
			bulk = append(bulk, x)
		} else {
			small = append(small, x)
		}
	}
	if len(bulk) > 0 {
		fmt.Fprintf(w, "\n| Write | Rows | twinhouse rows/s | ClickHouse rows/s | ClickHouse is |\n|---|--:|--:|--:|--:|\n")
		for i := 0; i+1 < len(bulk); i += 2 {
			t, h := bulk[i], bulk[i+1]
			fmt.Fprintf(w, "| %s %s | %s | %s | %s | %s faster |\n", t.ID, t.What, count(float64(t.Rows)), count(t.PerSec), count(h.PerSec), ratio(h.PerSec, t.PerSec))
		}
		if c.ViewCatchUp != "" {
			fmt.Fprintf(w, "\nAfter W2, both servers settled (every container under 10%% CPU) within %s.\n", c.ViewCatchUp)
		}
	}
	if len(small) > 0 {
		fmt.Fprintf(w, "\n| Write | Server | p50 | p95 | p99 | inserts/s |\n|---|---|--:|--:|--:|--:|\n")
		for _, x := range small {
			fmt.Fprintf(w, "| %s %s | %s | %s | %s | %s | %s |\n", x.ID, x.What, x.Side, ms(x.Stats.P50), ms(x.Stats.P95), ms(x.Stats.P99), count(x.PerSec))
		}
	}
	if len(r.Reads) > 0 {
		fmt.Fprintf(w, "\n| Read | Shape | Clients | twinhouse p50 / p95 | ClickHouse p50 / p95 | p50 ratio |\n|---|---|--:|--:|--:|--:|\n")
		for _, x := range r.Reads {
			t, h := x.Sides["twinhouse"], x.Sides["clickhouse"]
			rounds := ""
			if x.Rounds < 20 {
				rounds = fmt.Sprintf(" (%d rounds)", x.Rounds)
			}
			fmt.Fprintf(w, "| %s | %s%s | %d | %s / %s | %s / %s | %s |\n", x.ID, x.Shape, rounds, x.Conc,
				ms(t.P50), ms(t.P95), ms(h.P50), ms(h.P95), ratio(t.P50, h.P50))
		}
		fmt.Fprintln(w, "\nQueries:")
		seen := map[string]bool{}
		for _, x := range r.Reads {
			if !seen[x.ID] {
				seen[x.ID] = true
				fmt.Fprintf(w, "- %s: `%s`\n", x.ID, strings.Join(strings.Fields(x.SQL), " "))
			}
		}
	}
}
