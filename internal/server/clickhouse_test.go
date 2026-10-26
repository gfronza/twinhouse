package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/format"
)

// miniEngine is a fake Executor answering the statements clickhouse-go and
// the tests send, with canned typed results. Table t has columns
// (id UInt64, name String, score Float64, ts DateTime).
type miniEngine struct {
	mu   sync.Mutex
	rows [][]any
	reqs []string
}

var tCols = []format.Column{
	{Name: "id", Type: chtype.TUInt64},
	{Name: "name", Type: chtype.TString},
	{Name: "score", Type: chtype.TFloat64},
	{Name: "ts", Type: chtype.TDateTime},
}

func (m *miniEngine) Execute(ctx context.Context, req *exec.Request) (*exec.Result, error) {
	q := strings.TrimSpace(req.Query)
	m.mu.Lock()
	m.reqs = append(m.reqs, q)
	m.mu.Unlock()
	str := func(name string) format.Column { return format.Column{Name: name, Type: chtype.TString} }
	switch {
	case q == "SELECT displayName(), version(), revision(), timezone()":
		return &exec.Result{
			Columns: []format.Column{str("displayName()"), str("version()"), {Name: "revision()", Type: chtype.TUInt32}, str("timezone()")},
			Rows:    [][]any{{"test-node", "25.8.1.1", uint64(ServerRevision), "UTC"}},
		}, nil
	case q == "SELECT 1":
		return oneRow(), nil
	case q == "DESCRIBE TABLE t":
		res := &exec.Result{Columns: []format.Column{str("name"), str("type"), str("default_type"), str("default_expression"),
			str("comment"), str("codec_expression"), str("ttl_expression")}}
		for _, c := range tCols {
			res.Rows = append(res.Rows, []any{c.Name, c.Type.String(), "", "", "", "", ""})
		}
		return res, nil
	case strings.HasPrefix(q, "INSERT INTO t") && strings.HasSuffix(q, "FORMAT Native"),
		strings.HasPrefix(q, "INSERT INTO t FORMAT JSONEachRow"):
		name := "Native"
		if strings.Contains(q, "JSONEachRow") {
			name = "JSONEachRow"
		}
		dec, err := format.NewDecoder(name, req.Data, tCols, req.FormatOptions)
		if err != nil {
			return nil, err
		}
		var n uint64
		for {
			row, err := dec.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			m.mu.Lock()
			m.rows = append(m.rows, row)
			m.mu.Unlock()
			n++
		}
		return &exec.Result{WrittenRows: n}, nil
	case q == "INSERT INTO t (id, name) VALUES ({id:UInt64}, {name:String})":
		id, err := chtype.ParseText(chtype.TUInt64, req.Params["id"])
		if err != nil {
			return nil, cherr.New(cherr.BadQueryParameter, "bad id")
		}
		name, ok := req.Params["name"]
		if !ok {
			return nil, cherr.New(cherr.UnknownQueryParameter, "Substitution `name` is not set")
		}
		m.mu.Lock()
		m.rows = append(m.rows, []any{id, name, 0.0, time.Unix(0, 0).UTC()})
		m.mu.Unlock()
		return &exec.Result{WrittenRows: 1}, nil
	case q == "SELECT count() FROM t WHERE name = {name:String}":
		var n uint64
		m.mu.Lock()
		for _, r := range m.rows {
			if r[1] == req.Params["name"] {
				n++
			}
		}
		m.mu.Unlock()
		return &exec.Result{Columns: []format.Column{{Name: "count()", Type: chtype.TUInt64}}, Rows: [][]any{{n}}}, nil
	case q == "SELECT id, name, score, ts FROM t ORDER BY id":
		m.mu.Lock()
		defer m.mu.Unlock()
		return &exec.Result{Columns: tCols, Rows: append([][]any(nil), m.rows...)}, nil
	case q == "SELECT number FROM numbers(70000)":
		res := &exec.Result{Columns: []format.Column{{Name: "number", Type: chtype.TUInt64}}}
		for i := 0; i < 70000; i++ {
			res.Rows = append(res.Rows, []any{uint64(i)})
		}
		return res, nil
	case q == "SELECT broken":
		res := &exec.Result{Columns: []format.Column{{Name: "n", Type: chtype.TUInt64}}}
		for i := 0; i < 100000; i++ {
			res.Rows = append(res.Rows, []any{uint64(i)})
		}
		res.Rows = append(res.Rows, []any{"oops"})
		return res, nil
	case strings.HasPrefix(q, "SELECT * FROM missing"):
		return nil, cherr.New(cherr.UnknownTable, "Table default.missing does not exist")
	case strings.HasPrefix(q, "CREATE TABLE"):
		return &exec.Result{}, nil
	}
	return nil, cherr.New(cherr.NotImplemented, "fake engine: unsupported query %q", q)
}

func requireNative(t *testing.T) {
	t.Helper()
	requireEncoders(t)
	var b strings.Builder
	enc, err := format.NewEncoder("Native", &b, []format.Column{{Name: "x", Type: chtype.TString}}, format.Options{ClientProtocolVersion: 54460})
	if err != nil || enc.WriteRows([][]any{{"a"}}) != nil || enc.Finish(format.Stats{}) != nil {
		t.Skip("Native encoder not ready")
	}
	if _, err := format.NewDecoder("Native", strings.NewReader(""), tCols, format.Options{}); err != nil {
		t.Skip("Native decoder not ready")
	}
}

func newCHServer(t *testing.T) (*miniEngine, string) {
	t.Helper()
	me := &miniEngine{}
	s, err := New(Config{Executor: me, ServerVersion: "25.8.1.1", DisplayName: "test-node", Timezone: "UTC",
		Logger: quietLogger(), ResponseBufferSize: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	ts := httpTestServer(t, s)
	return me, strings.TrimPrefix(ts, "http://")
}

func TestClickHouseGoClient(t *testing.T) {
	requireNative(t)
	compressions := []*clickhouse.Compression{
		nil,
		{Method: clickhouse.CompressionLZ4},
		{Method: clickhouse.CompressionZSTD},
		{Method: clickhouse.CompressionGZIP},
		{Method: clickhouse.CompressionDeflate},
		{Method: clickhouse.CompressionBrotli},
	}
	for _, comp := range compressions {
		name := "none"
		if comp != nil {
			name = comp.Method.String()
		}
		t.Run(name, func(t *testing.T) {
			me, addr := newCHServer(t)
			ctx := context.Background()
			conn, err := clickhouse.Open(&clickhouse.Options{
				Addr:        []string{addr},
				Protocol:    clickhouse.HTTP,
				Auth:        clickhouse.Auth{Database: "default", Username: "default"},
				Compression: comp,
				DialTimeout: 5 * time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			if err := conn.Ping(ctx); err != nil {
				t.Fatalf("ping: %v", err)
			}
			sv, err := conn.ServerVersion()
			if err != nil {
				t.Fatal(err)
			}
			if sv.DisplayName != "test-node" || sv.Revision != ServerRevision || sv.Version.Major != 25 || sv.Version.Minor != 8 ||
				sv.Timezone.String() != "UTC" {
				t.Errorf("server version %+v", sv)
			}

			// Exec with server-side parameters.
			pctx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"id": "7", "name": "seven"}))
			if err := conn.Exec(pctx, "INSERT INTO t (id, name) VALUES ({id:UInt64}, {name:String})"); err != nil {
				t.Fatalf("exec: %v", err)
			}

			// Batch insert (DESCRIBE TABLE + INSERT ... FORMAT Native).
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO t")
			if err != nil {
				t.Fatalf("prepare batch: %v", err)
			}
			ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			for i := 1; i <= 3; i++ {
				if err := batch.Append(uint64(i), fmt.Sprintf("n%d", i), float64(i)/2, ts); err != nil {
					t.Fatal(err)
				}
			}
			if err := batch.Send(); err != nil {
				t.Fatalf("send: %v", err)
			}

			var count uint64
			qctx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"name": "n2"}))
			if err := conn.QueryRow(qctx, "SELECT count() FROM t WHERE name = {name:String}").Scan(&count); err != nil {
				t.Fatalf("query row: %v", err)
			}
			if count != 1 {
				t.Errorf("count %d", count)
			}

			rows, err := conn.Query(ctx, "SELECT id, name, score, ts FROM t ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for rows.Next() {
				var (
					id    uint64
					nm    string
					score float64
					tm    time.Time
				)
				if err := rows.Scan(&id, &nm, &score, &tm); err != nil {
					t.Fatal(err)
				}
				got = append(got, fmt.Sprintf("%d/%s/%g/%s", id, nm, score, tm.UTC().Format(time.DateTime)))
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			want := "[7/seven/0/1970-01-01 00:00:00 1/n1/0.5/2026-01-02 03:04:05 2/n2/1/2026-01-02 03:04:05 3/n3/1.5/2026-01-02 03:04:05]"
			if fmt.Sprint(got) != want {
				t.Errorf("rows %v\nwant %s", got, want)
			}

			// Multi-block result.
			rows, err = conn.Query(ctx, "SELECT number FROM numbers(70000)")
			if err != nil {
				t.Fatal(err)
			}
			var n, sum uint64
			for rows.Next() {
				var v uint64
				if err := rows.Scan(&v); err != nil {
					t.Fatal(err)
				}
				n++
				sum += v
			}
			if rows.Err() != nil || n != 70000 || sum != 70000*69999/2 {
				t.Errorf("numbers: n=%d sum=%d err=%v", n, sum, rows.Err())
			}

			// Server-side exception before any output: typed *clickhouse.Exception.
			_, err = conn.Query(ctx, "SELECT * FROM missing")
			var ex *clickhouse.Exception
			if !errors.As(err, &ex) || ex.Code != 60 || !strings.Contains(ex.Message, "Table default.missing does not exist") {
				t.Errorf("exception: %#v", err)
			}
			var httpErr *clickhouse.HTTPError
			if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusNotFound {
				t.Errorf("http error: %#v", err)
			}
			if err := conn.Exec(ctx, "SELECT * FROM missing"); !errors.As(err, &ex) || ex.Code != 60 {
				t.Errorf("exec exception: %v", err)
			}

			// Exception after the response was committed: in-band block.
			rows, err = conn.Query(ctx, "SELECT broken")
			if err == nil {
				for rows.Next() {
				}
				err = rows.Err()
			}
			if !errors.As(err, &ex) || ex.Code != 49 {
				t.Errorf("mid-stream exception: %v", err)
			}

			me.mu.Lock()
			defer me.mu.Unlock()
			if len(me.rows) != 4 {
				t.Errorf("stored rows %d", len(me.rows))
			}
		})
	}
}

func TestClickHouseGoDatabaseSQL(t *testing.T) {
	requireNative(t)
	me, addr := newCHServer(t)
	db := clickhouse.OpenDB(&clickhouse.Options{Addr: []string{addr}, Protocol: clickhouse.HTTP})
	defer db.Close()
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	pctx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"id": "9", "name": "nine"}))
	if _, err := db.ExecContext(pctx, "INSERT INTO t (id, name) VALUES ({id:UInt64}, {name:String})"); err != nil {
		t.Fatalf("exec: %v", err)
	}
	// database/sql batch: Begin + Prepare + Exec per row + Commit.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO t")
	if err != nil {
		t.Fatal(err)
	}
	for i := 10; i < 12; i++ {
		if _, err := stmt.ExecContext(ctx, uint64(i), "nine", 1.0, time.Unix(100, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var count uint64
	qctx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"name": "nine"}))
	if err := db.QueryRowContext(qctx, "SELECT count() FROM t WHERE name = {name:String}").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("count %d", count)
	}
	var ex *clickhouse.Exception
	if err := db.QueryRowContext(ctx, "SELECT * FROM missing").Scan(new(string)); !errors.As(err, &ex) || ex.Code != 60 {
		t.Errorf("exception %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE x (a UInt8) ENGINE = Memory"); err != nil {
		t.Errorf("ddl: %v", err)
	}
	me.mu.Lock()
	defer me.mu.Unlock()
	if len(me.rows) != 3 {
		t.Errorf("stored rows %d", len(me.rows))
	}
}

// TestRawHTTPJSONEachRowInsert drives the server the way Python's requests
// (or curl) typically does.
func TestRawHTTPJSONEachRowInsert(t *testing.T) {
	requireNative(t)
	me, addr := newCHServer(t)
	base := "http://" + addr
	body := `{"id": 1, "name": "a", "score": 1.5, "ts": "2026-01-02 03:04:05"}` + "\n" +
		`{"id": 2, "name": "b", "score": 2, "ts": "2026-01-02 03:04:06"}` + "\n"
	u := base + "/?query=" + url.QueryEscape("INSERT INTO t FORMAT JSONEachRow")
	r := post(t, u, body, "Content-Type", "application/json")
	if r.Status != 200 || r.Body != "" {
		t.Fatalf("%d %q", r.Status, r.Body)
	}
	if s := r.Header.Get("X-ClickHouse-Summary"); !strings.Contains(s, `"written_rows":"2"`) {
		t.Errorf("summary %s", s)
	}
	r = get(t, base+"/?query="+url.QueryEscape("SELECT id, name, score, ts FROM t ORDER BY id"))
	if r.Status != 200 || r.Body != "1\ta\t1.5\t2026-01-02 03:04:05\n2\tb\t2\t2026-01-02 03:04:06\n" {
		t.Errorf("select: %d %q", r.Status, r.Body)
	}
	// A failing INSERT surfaces as a plain-text exception.
	r = post(t, u, "{not json\n")
	if r.Status == 200 || !strings.HasPrefix(r.Body, "Code: ") {
		t.Errorf("bad insert: %d %q", r.Status, r.Body)
	}
	me.mu.Lock()
	defer me.mu.Unlock()
	if len(me.rows) != 2 {
		t.Errorf("rows %d", len(me.rows))
	}
}
