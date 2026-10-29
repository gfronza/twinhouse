//go:build samlocal

// End-to-end check of the API Lambda through API Gateway emulation:
//
//	sam build && sam local start-api --docker-network twinhouse_default ...
//	go test -tags samlocal -count=1 -v ./cmd/chserver-lambda/
//
// SAM_LOCAL_URL (default http://127.0.0.1:3000) is the emulated API; the
// function must point at a DynamoDB (Local) it can reach. Tables are created
// with a unique name and dropped at the end.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/klauspost/compress/zstd"

	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/format"
)

func TestSamLocal(t *testing.T) {
	base := os.Getenv("SAM_LOCAL_URL")
	if base == "" {
		base = "http://127.0.0.1:3000"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}, Timeout: 2 * time.Minute}
	timed := func(what string, f func()) {
		start := time.Now()
		f()
		t.Logf("%-48s %8.1f ms", what, float64(time.Since(start).Microseconds())/1000)
	}
	do := func(method, path string, body []byte, hdr ...string) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		// API Gateway (and its emulator) base64-decodes a binary response
		// only when the request's Accept header matches a binary media type;
		// Go clients send none by default, curl sends */*. See below.
		req.Header.Set("Accept", "*/*")
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		if req.Header.Get("Accept") == "-" {
			req.Header.Del("Accept")
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer res.Body.Close()
		b, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return res, b
	}

	table := fmt.Sprintf("samlocal_%d", time.Now().UnixNano()%1_000_000)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{strings.TrimPrefix(base, "http://")}, Protocol: clickhouse.HTTP,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		HttpHeaders: map[string]string{"Accept": "*/*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table); err != nil {
			t.Errorf("drop %s: %v", table, err)
		}
		conn.Close()
	})

	timed("clickhouse-go Ping", func() {
		if err := conn.Ping(ctx); err != nil {
			t.Fatalf("ping: %v", err)
		}
	})
	sv, err := conn.ServerVersion()
	if err != nil || sv.Revision != 54477 {
		t.Fatalf("server version %+v %v", sv, err)
	}
	t.Logf("server: %s %s", sv.DisplayName, sv.Version)

	timed("CREATE TABLE (clickhouse-go)", func() {
		if err := conn.Exec(ctx, "CREATE TABLE "+table+" (tenant String, id UInt64, ts DateTime64(3), amount Float64, note String) ENGINE = MergeTree ORDER BY (tenant, id)"); err != nil {
			t.Fatalf("create: %v", err)
		}
	})

	// Native batch insert (clickhouse-go: Native body, LZ4 frames because of
	// Compression, i.e. decompress=1), 1000 rows.
	const nativeRows = 1000
	ts := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	timed(fmt.Sprintf("INSERT Native batch, %d rows (clickhouse-go)", nativeRows), func() {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i <= nativeRows; i++ {
			if err := batch.Append(fmt.Sprintf("t%d", i%7), uint64(i), ts.Add(time.Duration(i)*time.Millisecond), float64(i)/4, strings.Repeat("ü", i%13)); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Send(); err != nil {
			t.Fatalf("send: %v", err)
		}
	})

	// JSONEachRow insert, gzip request body (Content-Encoding), 2000 rows.
	const jsonRows = 2000
	var raw, gz bytes.Buffer
	for i := nativeRows + 1; i <= nativeRows+jsonRows; i++ {
		fmt.Fprintf(&raw, `{"tenant":"t%d","id":%d,"ts":"2026-03-04 05:06:07.%03d","amount":%g,"note":"n%d"}`+"\n", i%7, i, i%1000, float64(i)/4, i)
	}
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(raw.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	timed(fmt.Sprintf("INSERT JSONEachRow gzip, %d rows (%d -> %d bytes)", jsonRows, raw.Len(), gz.Len()), func() {
		res, b := do("POST", "/?query="+url.QueryEscape("INSERT INTO "+table+" FORMAT JSONEachRow"), gz.Bytes(), "Content-Encoding", "gzip")
		if res.StatusCode != 200 {
			t.Fatalf("gzip insert: %d %s", res.StatusCode, b)
		}
	})
	// Plain text POST (query + inline data), as curl sends it.
	timed("INSERT inline Values (curl style)", func() {
		res, b := do("POST", "/", []byte("INSERT INTO "+table+" VALUES ('x', 999999, '2026-01-01 00:00:00.000', 1, 'inline')"))
		if res.StatusCode != 200 {
			t.Fatalf("inline insert: %d %s", res.StatusCode, b)
		}
	})
	total := nativeRows + jsonRows + 1

	// clickhouse-go query (Native over HTTP, LZ4).
	timed("SELECT count/sum (clickhouse-go)", func() {
		var n uint64
		var sum float64
		if err := conn.QueryRow(ctx, "SELECT count(), sum(amount) FROM "+table).Scan(&n, &sum); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n != uint64(total) {
			t.Errorf("count %d want %d", n, total)
		}
		t.Logf("rows=%d sum=%g", n, sum)
	})
	timed(fmt.Sprintf("SELECT all rows Native LZ4 (clickhouse-go), %d rows", total), func() {
		rows, err := conn.Query(ctx, "SELECT tenant, id, ts, amount, note FROM "+table+" ORDER BY id")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		n := 0
		for rows.Next() {
			var tenant, note string
			var id uint64
			var at time.Time
			var amount float64
			if err := rows.Scan(&tenant, &id, &at, &amount, &note); err != nil {
				t.Fatal(err)
			}
			if n == 0 && (id != 1 || tenant != "t1" || amount != 0.25 || !at.Equal(ts.Add(time.Millisecond))) {
				t.Errorf("first row %s %d %v %g %q", tenant, id, at, amount, note)
			}
			n++
		}
		rows.Close()
		if n != total {
			t.Errorf("rows %d want %d", n, total)
		}
	})

	// Raw HTTP: JSON, TSV, Native, compress=1, gzip/zstd HTTP compression.
	sel := "SELECT tenant, id, amount, note FROM " + table + " ORDER BY id"
	timed("SELECT FORMAT JSON (curl style)", func() {
		res, b := do("GET", "/?query="+url.QueryEscape(sel+" FORMAT JSON"), nil)
		var js struct {
			Rows int `json:"rows"`
		}
		if res.StatusCode != 200 || json.Unmarshal(b, &js) != nil || js.Rows != total {
			t.Fatalf("json: %d rows %d: %.200s", res.StatusCode, js.Rows, b)
		}
		if ct := res.Header.Get("Content-Type"); ct != "application/json; charset=UTF-8" {
			t.Errorf("json content type %q", ct)
		}
		t.Logf("JSON body %d bytes", len(b))
	})
	timed("SELECT FORMAT TSV", func() {
		res, b := do("GET", "/?query="+url.QueryEscape(sel+" FORMAT TSV"), nil)
		if res.StatusCode != 200 || strings.Count(string(b), "\n") != total || !strings.HasPrefix(string(b), "t1\t1\t0.25\tü\n") {
			t.Fatalf("tsv: %d %.200q", res.StatusCode, b)
		}
		t.Logf("TSV body %d bytes", len(b))
	})
	cols := []format.Column{{Name: "tenant", Type: chtype.TString}, {Name: "id", Type: chtype.TUInt64}, {Name: "amount", Type: chtype.TFloat64}, {Name: "note", Type: chtype.TString}}
	timed("SELECT FORMAT Native (binary response)", func() {
		res, b := do("POST", "/", []byte(sel+" FORMAT Native"))
		if res.StatusCode != 200 || res.Header.Get("Content-Type") != "application/octet-stream" {
			t.Fatalf("native: %d %q %.100q", res.StatusCode, res.Header.Get("Content-Type"), b)
		}
		dec, err := format.NewDecoder("Native", bytes.NewReader(b), cols, format.Options{})
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for {
			row, err := dec.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("native row %d: %v", n, err)
			}
			if n == 0 && (row[0] != "t1" || row[1] != uint64(1) || row[2] != 0.25) {
				t.Errorf("first native row %v", row)
			}
			n++
		}
		if n != total {
			t.Errorf("native rows %d want %d", n, total)
		}
		t.Logf("Native body %d bytes, valid UTF-8: %v", len(b), utf8.Valid(b))
	})
	timed("SELECT FORMAT TSV compress=1 (LZ4 frames)", func() {
		res, b := do("GET", "/?compress=1&query="+url.QueryEscape(sel+" FORMAT TSV"), nil)
		if res.StatusCode != 200 || utf8.Valid(b) {
			t.Fatalf("compress=1: %d valid-utf8=%v %.100q", res.StatusCode, utf8.Valid(b), b)
		}
		// Round trip: the server accepts its own frames as INSERT data.
		res2, b2 := do("POST", "/?decompress=1&query="+url.QueryEscape("INSERT INTO "+table+" (tenant, id, amount, note) FORMAT TSV"), b)
		if res2.StatusCode != 200 {
			t.Fatalf("decompress=1 insert: %d %s", res2.StatusCode, b2)
		}
		total *= 2
		t.Logf("compress=1 body %d bytes", len(b))
	})
	for _, enc := range []string{"gzip", "zstd"} {
		timed("SELECT FORMAT JSONEachRow enable_http_compression "+enc, func() {
			res, b := do("GET", "/?enable_http_compression=1&query="+url.QueryEscape(sel+" FORMAT JSONEachRow"), nil, "Accept-Encoding", enc)
			if res.StatusCode != 200 || res.Header.Get("Content-Encoding") != enc {
				t.Fatalf("%s: %d enc=%q %.100q", enc, res.StatusCode, res.Header.Get("Content-Encoding"), b)
			}
			var plain []byte
			if enc == "gzip" {
				r, err := gzip.NewReader(bytes.NewReader(b))
				if err != nil {
					t.Fatalf("%s: %v", enc, err)
				}
				plain, err = io.ReadAll(r)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				r, err := zstd.NewReader(bytes.NewReader(b))
				if err != nil {
					t.Fatal(err)
				}
				plain, err = io.ReadAll(r)
				r.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if n := strings.Count(string(plain), "\n"); n != total {
				t.Errorf("%s: %d lines want %d", enc, n, total)
			}
			t.Logf("%s body %d -> %d bytes", enc, len(b), len(plain))
		})
	}

	// Sessions are rejected; other errors keep their ClickHouse shape.
	res, b := do("POST", "/?session_id=abc", []byte("SELECT 1"))
	if res.StatusCode != 501 || res.Header.Get("X-ClickHouse-Exception-Code") != "48" ||
		string(b) != "Code: 48. DB::Exception: sessions are not supported by this deployment. (NOT_IMPLEMENTED) (version 25.8.1.1)\n" {
		t.Errorf("session: %d %s", res.StatusCode, b)
	}
	var ex *clickhouse.Exception
	if _, err := conn.Query(ctx, "SELECT * FROM nope_"+table); !errors.As(err, &ex) || ex.Code != 60 {
		t.Errorf("unknown table: %v", err)
	}
	res, b = do("GET", "/?query="+url.QueryEscape("DROP TABLE "+table), nil)
	if res.StatusCode != 500 || res.Header.Get("X-ClickHouse-Exception-Code") != "164" {
		t.Errorf("GET drop: %d %s", res.StatusCode, b)
	}

	// Observation, not a requirement of this code: without an Accept header
	// the emulator returns the Lambda's base64 body undecoded for binary
	// output (text output is unaffected).
	res, b = do("GET", "/?query="+url.QueryEscape("SELECT 1 FORMAT Native"), nil, "Accept", "-")
	t.Logf("Native without Accept header: status %d, body %q (base64 passthrough: %v)", res.StatusCode, b, strings.HasPrefix(string(b), "AQEB"))
	res, b = do("GET", "/?query="+url.QueryEscape("SELECT 1 FORMAT TSV"), nil, "Accept", "-")
	if res.StatusCode != 200 || string(b) != "1\n" {
		t.Errorf("TSV without Accept header: %d %q", res.StatusCode, b)
	}

	// A larger response (~1.5 MB of JSON) to see the base64 and latency cost.
	timed("SELECT ~1.5 MB JSONEachRow (large response)", func() {
		res, b := do("GET", "/?query="+url.QueryEscape("SELECT number, toString(number) AS s, repeat('x', 40) AS pad FROM numbers(20000) FORMAT JSONEachRow"), nil)
		if res.StatusCode != 200 || strings.Count(string(b), "\n") != 20000 {
			t.Fatalf("large: %d %d lines", res.StatusCode, strings.Count(string(b), "\n"))
		}
		t.Logf("large body %d bytes", len(b))
	})
}
