package format

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// fakeServer answers clickhouse-go's HTTP requests using only this package:
// SELECT results are Native-encoded with the client's protocol revision and
// INSERT ... FORMAT Native bodies are decoded with NewDecoder.
type fakeServer struct {
	t        *testing.T
	results  map[string]nativeCase // query text -> result
	table    []Column              // DESCRIBE TABLE t
	mu       sync.Mutex
	inserted [][]any
}

func (s *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	rev, _ := strconv.ParseUint(q.Get("client_protocol_version"), 10, 64)
	if query := q.Get("query"); strings.HasPrefix(query, "INSERT INTO") {
		dec, err := NewDecoder("Native", r.Body, s.table, Options{})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for {
			row, err := dec.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			s.mu.Lock()
			s.inserted = append(s.inserted, row)
			s.mu.Unlock()
		}
		return
	}
	body, _ := io.ReadAll(r.Body)
	query := strings.TrimSpace(string(body))
	var res nativeCase
	switch {
	case query == "SELECT displayName(), version(), revision(), timezone()":
		res = nativeCase{
			cols: []Column{col("displayName()", "String"), col("version()", "String"), col("revision()", "UInt32"), col("timezone()", "String")},
			rows: [][]any{{"fake", "25.8.1.1", uint64(54477), "UTC"}},
		}
	case strings.HasPrefix(query, "DESCRIBE TABLE"):
		res.cols = []Column{col("name", "String"), col("type", "String"), col("default_type", "String"),
			col("default_expression", "String"), col("comment", "String"), col("codec_expression", "String"), col("ttl_expression", "String")}
		for _, c := range s.table {
			res.rows = append(res.rows, []any{c.Name, c.Type.String(), "", "", "", "", ""})
		}
	default:
		var ok bool
		if res, ok = s.results[query]; !ok {
			http.Error(w, "Code: 62. DB::Exception: unexpected query "+query+". (SYNTAX_ERROR)", 400)
			return
		}
	}
	w.Header().Set("Content-Type", ContentType("Native"))
	enc, err := NewEncoder("Native", w, res.cols, Options{ClientProtocolVersion: rev})
	if err != nil {
		s.t.Errorf("encoder: %v", err)
		return
	}
	if err := enc.WriteRows(res.rows); err != nil {
		s.t.Errorf("WriteRows: %v", err)
	}
	enc.Finish(Stats{})
}

func TestClickHouseGoClientOverHTTP(t *testing.T) {
	srv := &fakeServer{t: t, results: map[string]nativeCase{}}
	for _, c := range nativeCases() {
		srv.results[c.query] = c
	}
	srv.table = []Column{col("id", "UInt64"), col("name", "LowCardinality(String)"), col("ts", "DateTime64(9, 'UTC')"),
		col("tags", "Array(String)"), col("score", "Nullable(Float64)")}
	hs := httptest.NewServer(srv)
	defer hs.Close()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{strings.TrimPrefix(hs.URL, "http://")},
		Protocol: clickhouse.HTTP,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()

	for _, c := range nativeCases() {
		t.Run("select/"+c.name, func(t *testing.T) {
			if chgoUnsupported[c.name] {
				t.Skip("clickhouse-go cannot materialise this type generically")
			}
			rows, err := conn.Query(ctx, c.query)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			cts := rows.ColumnTypes()
			if len(cts) != len(c.cols) {
				t.Fatalf("got %d columns, want %d", len(cts), len(c.cols))
			}
			n := 0
			for rows.Next() {
				dest := make([]any, len(cts))
				for i, ct := range cts {
					dest[i] = reflect.New(ct.ScanType()).Interface()
				}
				if err := rows.Scan(dest...); err != nil {
					t.Fatalf("scan row %d: %v", n, err)
				}
				for i := range dest {
					got := fromChGo(c.cols[i].Type, reflect.ValueOf(dest[i]).Elem().Interface())
					want := sortMaps(c.cols[i].Type, c.rows[n][i])
					if !valuesEqual(got, want) {
						t.Fatalf("row %d col %s: got %#v want %#v", n, c.cols[i].Name, got, want)
					}
				}
				n++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if n != len(c.rows) {
				t.Fatalf("got %d rows, want %d", n, len(c.rows))
			}
		})
	}

	t.Run("batch insert", func(t *testing.T) {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO t")
		if err != nil {
			t.Fatal(err)
		}
		ts := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
		score := 0.5
		if err := batch.Append(uint64(1), "a", ts, []string{"x"}, &score); err != nil {
			t.Fatal(err)
		}
		if err := batch.Append(uint64(2), "b", ts.Add(time.Hour), []string{}, nil); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
		assertRows(t, srv.inserted, [][]any{
			{uint64(1), "a", ts, []any{"x"}, 0.5},
			{uint64(2), "b", ts.Add(time.Hour), []any{}, nil},
		})
	})
}
