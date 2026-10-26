//go:build e2e

package e2e

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var trickyStrings = []string{
	"plain",
	"it's",
	`double "quoted"`,
	`back\slash`,
	"tab\tand\nnewline\rcr",
	"ünïcødé — 日本語 ✓ 🚀",
	"semi;colon -- comment /* x */",
	"{curly:String}",
	"?",
	"@name",
	"",
	`'); DROP TABLE x; --`,
}

func TestParams(t *testing.T) {
	t.Parallel()
	db := newDB(t, "params")
	mustExec(t, db, `CREATE TABLE notes (tenant_id String, note_id UInt64, body String, score Int64, ratio Float64,
		created DateTime, created64 DateTime64(3), tags Array(String), flag Bool, opt Nullable(String))
		ENGINE = MergeTree ORDER BY (tenant_id, note_id)`)
	sqlc := openSQL(t, db)
	nconn := openNative(t, db)
	created := time.Date(2024, 2, 29, 23, 59, 58, 0, time.UTC)

	// Insert tricky strings with positional parameters (database/sql).
	for i, s := range trickyStrings {
		var opt *string
		if i%2 == 0 {
			opt = &trickyStrings[len(trickyStrings)-1-i]
		}
		if _, err := sqlc.ExecContext(ctxT(t), "INSERT INTO notes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			"tenant-a", uint64(i), s, int64(-i*1000), float64(i)+0.5, created.Add(time.Duration(i)*time.Second),
			created.Add(time.Duration(i)*time.Second), []string{s, "x"}, i%3 == 0, opt); err != nil {
			t.Fatalf("positional insert %d (%q): %v", i, s, err)
		}
	}

	check := func(t *testing.T, label string, scan func(i int, s string) (string, error)) {
		t.Helper()
		for i, s := range trickyStrings {
			got, err := scan(i, s)
			if err != nil {
				t.Errorf("%s %d (%q): %v", label, i, s, err)
				continue
			}
			if got != s {
				t.Errorf("%s %d: got %q want %q", label, i, got, s)
			}
		}
	}

	t.Run("positional", func(t *testing.T) {
		check(t, "positional", func(i int, s string) (string, error) {
			var body string
			var tags []string
			err := sqlc.QueryRowContext(ctxT(t), "SELECT body, tags FROM notes WHERE tenant_id = ? AND body = ? AND note_id = ?", "tenant-a", s, uint64(i)).Scan(&body, &tags)
			if err == nil && (len(tags) != 2 || tags[0] != s) {
				return "", fmt.Errorf("tags = %q", tags)
			}
			return body, err
		})
		// Typed positional args: negative ints, floats, time, slices (IN), bool, nil.
		var n uint64
		if err := sqlc.QueryRowContext(ctxT(t), "SELECT count() FROM notes WHERE score <= ? AND ratio > ? AND created >= ? AND note_id IN (?) AND flag = ?",
			int64(-3000), 2.0, created.Add(3*time.Second), []uint64{3, 4, 6, 9, 100}, true).Scan(&n); err != nil {
			t.Fatal(err)
		}
		// note ids 3,6,9 have flag=true (i%3==0) and satisfy score<=-3000, ratio>2, created>=+3s.
		if n != 3 {
			t.Errorf("typed positional count = %d, want 3", n)
		}
		if err := sqlc.QueryRowContext(ctxT(t), "SELECT count() FROM notes WHERE body IN (?)", []string{"it's", `back\slash`, "nope"}).Scan(&n); err != nil || n != 2 {
			t.Errorf("IN (?) with []string: n=%d err=%v", n, err)
		}
		var isNull uint8
		if err := sqlc.QueryRowContext(ctxT(t), "SELECT isNull(?)", nil).Scan(&isNull); err != nil || isNull != 1 {
			t.Errorf("isNull(nil) = %d, %v", isNull, err)
		}
	})

	t.Run("named_sql", func(t *testing.T) {
		check(t, "named (database/sql)", func(i int, s string) (string, error) {
			var body string
			err := sqlc.QueryRowContext(ctxT(t), "SELECT body FROM notes WHERE tenant_id = @tenant AND note_id = @id AND body = @body",
				clickhouse.Named("tenant", "tenant-a"), clickhouse.Named("id", uint64(i)), clickhouse.Named("body", s)).Scan(&body)
			return body, err
		})
		var n uint64
		if err := sqlc.QueryRowContext(ctxT(t), "SELECT count() FROM notes WHERE created64 >= @from AND created64 < @to",
			clickhouse.DateNamed("from", created.Add(2*time.Second), clickhouse.MilliSeconds),
			clickhouse.DateNamed("to", created.Add(5*time.Second), clickhouse.MilliSeconds)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Errorf("DateNamed range = %d, want 3", n)
		}
	})

	t.Run("named_native", func(t *testing.T) {
		check(t, "named (native)", func(i int, s string) (string, error) {
			var body string
			err := nconn.QueryRow(ctxT(t), "SELECT body FROM notes WHERE note_id = @id AND body = @body",
				clickhouse.Named("id", uint64(i)), clickhouse.Named("body", s)).Scan(&body)
			return body, err
		})
	})

	t.Run("server_side_with_parameters", func(t *testing.T) {
		check(t, "{name:Type} WithParameters", func(i int, s string) (string, error) {
			// Parameters are TSV-escaped text (like clickhouse-client --param_x).
			esc := strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`).Replace(s)
			ctx := clickhouse.Context(ctxT(t), clickhouse.WithParameters(clickhouse.Parameters{
				"id": fmt.Sprint(i), "body": esc, "tenant": "tenant-a"}))
			var body string
			var echoed string
			err := nconn.QueryRow(ctx, "SELECT body, {body:String} FROM notes WHERE tenant_id = {tenant:String} AND note_id = {id:UInt64} AND body = {body:String}").Scan(&body, &echoed)
			if err == nil && echoed != s {
				return "", fmt.Errorf("echoed param %q", echoed)
			}
			return body, err
		})
	})

	t.Run("server_side_named_args", func(t *testing.T) {
		// clickhouse.Named with {name:Type} placeholders: clickhouse-go sends
		// them as server-side query parameters.
		check(t, "{name:Type} Named args", func(i int, s string) (string, error) {
			var body string
			err := nconn.QueryRow(ctxT(t), "SELECT body FROM notes WHERE tenant_id = {tenant:String} AND note_id = {id:UInt64} AND body = {body:String}",
				clickhouse.Named("tenant", "tenant-a"), clickhouse.Named("id", uint64(i)), clickhouse.Named("body", s)).Scan(&body)
			return body, err
		})
		var n uint64
		err := sqlc.QueryRowContext(ctxT(t), `SELECT count() FROM notes WHERE created BETWEEN {from:DateTime} AND {to:DateTime}
			AND (tenant_id, body) IN {keys:Array(Tuple(String, String))} AND score >= {min:Int64}`,
			clickhouse.Named("from", created.Add(time.Second)),
			clickhouse.Named("to", created.Add(11*time.Second)),
			clickhouse.Named("keys", clickhouse.ArraySet{
				clickhouse.GroupSet{Value: []any{"tenant-a", "it's"}},
				clickhouse.GroupSet{Value: []any{"tenant-a", trickyStrings[5]}},
				clickhouse.GroupSet{Value: []any{"tenant-b", "plain"}},
				clickhouse.GroupSet{Value: []any{"tenant-a", `'); DROP TABLE x; --`}},
			}),
			clickhouse.Named("min", int64(-10000))).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		// it's (1), unicode (5) and the injection string (11, created+11s, score -11000 < min) → 2.
		if n != 2 {
			t.Errorf("Array(Tuple) named param count = %d, want 2", n)
		}
	})

	t.Run("http_params", func(t *testing.T) {
		for i, s := range trickyStrings {
			esc := strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`).Replace(s)
			r := doHTTP(t, req{
				Method: "GET",
				Params: url.Values{"database": {db}, "param_b": {esc}, "param_id": {fmt.Sprint(i)},
					"query": {"SELECT note_id, length(body), body = {b:String} FROM notes WHERE note_id = {id:UInt64} AND body = {b:String} FORMAT TSV"}},
			})
			want := fmt.Sprintf("%d\t%d\t1\n", i, len(s))
			if r.Status != 200 || r.Body != want {
				t.Errorf("http param %d (%q): %d %q, want %q", i, s, r.Status, r.Body, want)
			}
		}
		// DateTime / UInt64 / Array params.
		r := doHTTP(t, req{
			Params: url.Values{"database": {db}, "param_from": {created.Format(time.DateTime)},
				"param_ids": {"[0,2,4,6]"}, "param_big": {"18446744073709551615"}},
			Body: strings.NewReader("SELECT count(), max(note_id), {big:UInt64} FROM notes WHERE created > {from:DateTime} AND note_id IN {ids:Array(UInt64)}"),
		})
		if r.Status != 200 || r.Body != "3\t6\t18446744073709551615\n" {
			t.Errorf("typed http params: %d %q", r.Status, r.Body)
		}
	})

	t.Run("errors", func(t *testing.T) {
		_, err := nconn.Query(ctxT(t), "SELECT {missing:String}")
		wantCode(t, err, 456, "missing parameter")
		ctx := clickhouse.Context(ctxT(t), clickhouse.WithParameters(clickhouse.Parameters{"n": "not-a-number"}))
		_, err = nconn.Query(ctx, "SELECT {n:UInt64}")
		wantCode(t, err, 457, "bad parameter value")
		r := doHTTP(t, req{Params: url.Values{"query": {"SELECT {x:String}"}}, Method: "GET"})
		if r.Status == 200 || r.Header.Get("X-ClickHouse-Exception-Code") != "456" {
			t.Errorf("missing param over HTTP: %d %v %q", r.Status, r.Header.Get("X-ClickHouse-Exception-Code"), r.Body)
		}
	})
}
