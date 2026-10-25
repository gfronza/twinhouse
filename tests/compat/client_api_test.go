//go:build compat

package compat

// Differential tests of the clickhouse-go v2 client surface over HTTP: the
// same calls (database/sql and native API, client-side ? / Named binding,
// server-side {name:Type} parameters, PrepareBatch, scanning into Go types,
// settings, exceptions, compression) run against both servers and their
// observable results must match.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func hostPort(tg *target) string {
	host := tg.base.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "8123")
	}
	return host
}

func openOpts(tg *target, db string, comp *clickhouse.Compression) *clickhouse.Options {
	return &clickhouse.Options{
		Addr:        []string{hostPort(tg)},
		Protocol:    clickhouse.HTTP,
		Auth:        clickhouse.Auth{Database: db, Username: "default"},
		Compression: comp,
		DialTimeout: 10 * time.Second,
		ReadTimeout: 60 * time.Second,
	}
}

// diffBoth runs fn against both servers and fails on any difference.
func diffBoth(t *testing.T, ref, our *target, fn func(t *testing.T, tg *target) []string) {
	t.Helper()
	want := fn(t, ref)
	got := fn(t, our)
	w, g := strings.Join(want, "\n")+"\n", strings.Join(got, "\n")+"\n"
	if os.Getenv("COMPAT_VERBOSE") != "" {
		t.Logf("reference:\n%s", w)
	}
	if w != g {
		t.Errorf("clickhouse-go results differ:\n%s", unifiedDiff(w, g))
	}
}

func errString(err error) string {
	if err == nil {
		return "ok"
	}
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		return fmt.Sprintf("exception code=%d", ex.Code)
	}
	if m := nativeCodeRe.FindStringSubmatch(err.Error()); m != nil {
		return "error code=" + m[1]
	}
	return "error: " + err.Error()
}

func show(v any) string { return canon(reflect.ValueOf(v)) }

// --- database/sql ------------------------------------------------------------

func TestClientDatabaseSQL(t *testing.T) {
	ref, our := requireTargets(t)
	db := scenarioDB("client_sql")
	createDB(t, ref, our, db)
	ctx := context.Background()
	for _, tg := range []*target{ref, our} {
		tg.mustExec(t, ctx, `CREATE TABLE ev (tenant_id String, event_time DateTime, event_id String, n UInt64, f Float64,
			tags Array(String), note Nullable(String), ok Bool, attrs Map(String, UInt8), t3 DateTime64(3))
			ENGINE = MergeTree ORDER BY (tenant_id, event_time, event_id)`, reqOpts{db: db})
		tg.mustExec(t, ctx, `INSERT INTO ev VALUES
			('acme', '2024-05-01 10:00:00', 'e1', 1, 0.5, ['a','b'], NULL, true, {'k':1}, '2024-05-01 10:00:00.123'),
			('acme', '2024-05-01 11:00:00', 'e2', 18446744073709551615, -1.25, [], 'it''s', false, {}, '2024-05-01 11:00:00.000'),
			('globex', '2024-05-02 00:00:00', 'g1', 7, 1e100, ['x'], '', true, {'a':2,'b':3}, '1970-01-01 00:00:00.001')`, reqOpts{db: db})
	}

	withDB := func(t *testing.T, tg *target, comp *clickhouse.Compression, fn func(*sql.DB) []string) []string {
		conn := clickhouse.OpenDB(openOpts(tg, db, comp))
		defer conn.Close()
		if err := conn.PingContext(ctx); err != nil {
			return []string{"ping: " + errString(err)}
		}
		return fn(conn)
	}

	t.Run("scan_types", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withDB(t, tg, nil, func(c *sql.DB) []string {
				rows, err := c.QueryContext(ctx, "SELECT tenant_id, event_time, event_id, n, f, tags, note, ok, attrs, t3 FROM ev ORDER BY tenant_id, event_time")
				if err != nil {
					return []string{errString(err)}
				}
				defer rows.Close()
				var out []string
				cts, _ := rows.ColumnTypes()
				for _, ct := range cts {
					nullable, _ := ct.Nullable()
					out = append(out, fmt.Sprintf("col %s %s nullable=%v scan=%v", ct.Name(), ct.DatabaseTypeName(), nullable, ct.ScanType()))
				}
				for rows.Next() {
					var (
						tenant, id string
						at         time.Time
						n          uint64
						f          float64
						tags       []string
						note       *string
						ok         bool
						attrs      map[string]uint8
						t3         time.Time
					)
					if err := rows.Scan(&tenant, &at, &id, &n, &f, &tags, &note, &ok, &attrs, &t3); err != nil {
						return append(out, "scan: "+err.Error())
					}
					out = append(out, strings.Join([]string{show(tenant), show(at), show(id), show(n), show(f), show(tags), show(note), show(ok), show(attrs), show(t3)}, " | "))
				}
				return append(out, "err: "+errString(rows.Err()))
			})
		})
	})

	t.Run("positional_bind", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withDB(t, tg, nil, func(c *sql.DB) []string {
				var out []string
				rows, err := c.QueryContext(ctx, "SELECT event_id, n FROM ev WHERE tenant_id = ? AND event_time >= ? AND n > ? ORDER BY event_id",
					"acme", time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC), 0)
				if err != nil {
					return []string{errString(err)}
				}
				for rows.Next() {
					var id string
					var n uint64
					_ = rows.Scan(&id, &n)
					out = append(out, id+" "+show(n))
				}
				rows.Close()
				var s string
				var i int64
				var u uint64
				var fl float64
				var arr []string
				err = c.QueryRowContext(ctx, "SELECT ?, ?, ?, ?, ?", "quote ' back \\ tab \t", int64(-9223372036854775808), uint64(18446744073709551615), 0.1, []string{"a", "b'c"}).
					Scan(&s, &i, &u, &fl, &arr)
				out = append(out, errString(err), show(s), show(i), show(u), show(fl), show(arr))
				var cnt uint64
				err = c.QueryRowContext(ctx, "SELECT count() FROM ev WHERE event_id IN (?)", []string{"e1", "g1", "zz"}).Scan(&cnt)
				out = append(out, errString(err), show(cnt))
				return out
			})
		})
	})

	t.Run("named_bind", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withDB(t, tg, nil, func(c *sql.DB) []string {
				var id string
				var at time.Time
				err := c.QueryRowContext(ctx, "SELECT event_id, event_time FROM ev WHERE tenant_id = @tenant AND event_id = @id",
					clickhouse.Named("tenant", "globex"), clickhouse.Named("id", "g1")).Scan(&id, &at)
				out := []string{errString(err), id, show(at)}
				var d time.Time
				err = c.QueryRowContext(ctx, "SELECT toDateTime64(@ts, 3)",
					clickhouse.DateNamed("ts", time.Date(2024, 5, 1, 10, 0, 0, 123_000_000, time.UTC), clickhouse.MilliSeconds)).Scan(&d)
				return append(out, errString(err), show(d))
			})
		})
	})

	t.Run("server_side_params", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withDB(t, tg, nil, func(c *sql.DB) []string {
				pctx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{
					"keys": "[('acme','e1'),('globex','g1'),('acme','nope')]",
					"from": "2024-05-01 00:00:00",
					"to":   "2024-05-03 00:00:00",
				}))
				rows, err := c.QueryContext(pctx, `SELECT tenant_id, event_id FROM ev
					WHERE (tenant_id, event_id) IN {keys:Array(Tuple(String, String))}
					AND event_time BETWEEN {from:DateTime} AND {to:DateTime} ORDER BY tenant_id, event_id`)
				if err != nil {
					return []string{errString(err)}
				}
				defer rows.Close()
				var out []string
				for rows.Next() {
					var a, b string
					_ = rows.Scan(&a, &b)
					out = append(out, a+"/"+b)
				}
				return append(out, errString(rows.Err()))
			})
		})
	})

	t.Run("exec_insert_and_ddl", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withDB(t, tg, nil, func(c *sql.DB) []string {
				var out []string
				_, err := c.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS kv (k String, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k")
				out = append(out, "create "+errString(err))
				_, err = c.ExecContext(ctx, "INSERT INTO kv VALUES (?, ?, ?)", "a", 1, 1)
				out = append(out, "insert1 "+errString(err))
				_, err = c.ExecContext(ctx, "INSERT INTO kv VALUES (@k, @v, @ver)", clickhouse.Named("k", "a"), clickhouse.Named("v", 2), clickhouse.Named("ver", 2))
				out = append(out, "insert2 "+errString(err))
				var v uint64
				err = c.QueryRowContext(clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"p": "a"})),
					"SELECT v FROM kv FINAL WHERE k = {p:String}").Scan(&v)
				out = append(out, errString(err), show(v))
				return out
			})
		})
	})

	t.Run("transaction_batch_insert", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withDB(t, tg, nil, func(c *sql.DB) []string {
				var out []string
				_, err := c.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS sqlbatch (id UInt32, s String, t DateTime, arr Array(UInt8), n Nullable(Int64)) ENGINE = MergeTree ORDER BY id")
				out = append(out, "create "+errString(err))
				tx, err := c.Begin()
				if err != nil {
					return append(out, "begin "+errString(err))
				}
				st, err := tx.Prepare("INSERT INTO sqlbatch")
				if err != nil {
					return append(out, "prepare "+errString(err))
				}
				for i := 0; i < 5; i++ {
					var n *int64
					if i%2 == 0 {
						v := int64(-i)
						n = &v
					}
					if _, err := st.Exec(uint32(i), fmt.Sprintf("s%d\t'", i), time.Unix(int64(1700000000+i), 0).UTC(), []uint8{uint8(i)}, n); err != nil {
						return append(out, "exec "+errString(err))
					}
				}
				out = append(out, "commit "+errString(tx.Commit()))
				rows, err := c.QueryContext(ctx, "SELECT id, s, t, arr, n FROM sqlbatch ORDER BY id")
				if err != nil {
					return append(out, errString(err))
				}
				defer rows.Close()
				for rows.Next() {
					var id uint32
					var s string
					var tm time.Time
					var arr []uint8
					var n *int64
					_ = rows.Scan(&id, &s, &tm, &arr, &n)
					out = append(out, strings.Join([]string{show(id), show(s), show(tm), show(arr), show(n)}, " | "))
				}
				return out
			})
		})
	})

	t.Run("exceptions", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withDB(t, tg, nil, func(c *sql.DB) []string {
				var out []string
				for _, q := range []string{
					"SELECT * FROM no_such_table",
					"SELECT nope FROM ev",
					"SELECT nofn(1)",
					"SELECT 1 +",
					"SELECT 'a' + 1",
					"SELECT {missing:String}",
				} {
					rows, err := c.QueryContext(ctx, q)
					if err == nil {
						for rows.Next() {
						}
						err = rows.Err()
						rows.Close()
					}
					out = append(out, q+" -> "+errString(err))
				}
				_, err := c.ExecContext(ctx, "INSERT INTO no_such_table VALUES (1)")
				out = append(out, "insert -> "+errString(err))
				return out
			})
		})
	})

	for _, comp := range []struct {
		name string
		c    *clickhouse.Compression
	}{
		{"lz4", &clickhouse.Compression{Method: clickhouse.CompressionLZ4}},
		{"zstd", &clickhouse.Compression{Method: clickhouse.CompressionZSTD}},
		{"gzip", &clickhouse.Compression{Method: clickhouse.CompressionGZIP}},
		{"deflate", &clickhouse.Compression{Method: clickhouse.CompressionDeflate}},
	} {
		t.Run("compression_"+comp.name, func(t *testing.T) {
			diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
				return withDB(t, tg, comp.c, func(c *sql.DB) []string {
					var out []string
					rows, err := c.QueryContext(ctx, "SELECT number, toString(number), [number] FROM numbers(2000) ORDER BY number")
					if err != nil {
						return []string{errString(err)}
					}
					var n, sum uint64
					for rows.Next() {
						var x uint64
						var s string
						var a []uint64
						if err := rows.Scan(&x, &s, &a); err != nil {
							return append(out, "scan "+err.Error())
						}
						n++
						sum += x + a[0]
					}
					rows.Close()
					out = append(out, fmt.Sprintf("rows=%d sum=%d err=%s", n, sum, errString(rows.Err())))
					_, err = c.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS comp_"+comp.name+" (id UInt64, s String) ENGINE = MergeTree ORDER BY id")
					out = append(out, "create "+errString(err))
					tx, _ := c.Begin()
					st, err := tx.Prepare("INSERT INTO comp_" + comp.name)
					if err != nil {
						return append(out, "prepare "+errString(err))
					}
					for i := 0; i < 300; i++ {
						if _, err := st.Exec(uint64(i), strings.Repeat("x", i%17)); err != nil {
							return append(out, "exec "+errString(err))
						}
					}
					out = append(out, "commit "+errString(tx.Commit()))
					var cnt, ln uint64
					err = c.QueryRowContext(ctx, "SELECT count(), sum(length(s)) FROM comp_"+comp.name).Scan(&cnt, &ln)
					return append(out, errString(err), show(cnt), show(ln))
				})
			})
		})
	}
}

// --- native API over HTTP ------------------------------------------------------

func TestClientNativeAPI(t *testing.T) {
	ref, our := requireTargets(t)
	db := scenarioDB("client_native")
	createDB(t, ref, our, db)
	ctx := context.Background()
	for _, tg := range []*target{ref, our} {
		tg.mustExec(t, ctx, `CREATE TABLE ev (tenant_id String, event_time DateTime, event_id String, campaign_id UInt64,
			user_id String, kind LowCardinality(String), cost UInt64, bid Float64, note Nullable(String),
			tags Array(String), t3 DateTime64(3), flag UInt8)
			ENGINE = MergeTree ORDER BY (tenant_id, event_time, event_id)`, reqOpts{db: db})
		tg.mustExec(t, ctx, `CREATE TABLE hourly (tenant_id String, campaign_id UInt64, hour DateTime,
			events SimpleAggregateFunction(sum, UInt64), last_seen SimpleAggregateFunction(max, DateTime),
			users AggregateFunction(uniqExact, String))
			ENGINE = AggregatingMergeTree ORDER BY (tenant_id, campaign_id, hour)`, reqOpts{db: db})
		tg.mustExec(t, ctx, `CREATE MATERIALIZED VIEW hourly_mv TO hourly AS
			SELECT tenant_id, campaign_id, toStartOfHour(event_time) AS hour, count() AS events,
			max(event_time) AS last_seen, uniqExactState(user_id) AS users
			FROM ev GROUP BY tenant_id, campaign_id, hour`, reqOpts{db: db})
	}

	withConn := func(tg *target, fn func(driver.Conn) []string) []string {
		conn, err := clickhouse.Open(openOpts(tg, db, nil))
		if err != nil {
			return []string{"open: " + errString(err)}
		}
		defer conn.Close()
		if err := conn.Ping(ctx); err != nil {
			return []string{"ping: " + errString(err)}
		}
		return fn(conn)
	}

	t.Run("prepare_batch_send", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withConn(tg, func(c driver.Conn) []string {
				var out []string
				for round := 0; round < 2; round++ {
					b, err := c.PrepareBatch(ctx, "INSERT INTO ev")
					if err != nil {
						return append(out, "prepare "+errString(err))
					}
					for i := 0; i < 50; i++ {
						var note *string
						if i%3 == 0 {
							s := fmt.Sprintf("n%d \"q\" \\ \t", i)
							note = &s
						}
						tenant := []string{"acme", "globex", "ünï"}[i%3]
						err := b.Append(tenant, time.Date(2024, 5, 1, i%5, i, round, 0, time.UTC), fmt.Sprintf("r%d-%02d", round, i),
							uint64(i%4), fmt.Sprintf("u%d", i%7), []string{"impression", "click"}[i%2], uint64(i)*1000, float64(i)/4,
							note, []string{fmt.Sprint(i), ""}[:1+i%2], time.Date(2024, 5, 1, 0, 0, i, i*1_000_000, time.UTC), uint8(i%2))
						if err != nil {
							return append(out, "append "+errString(err))
						}
					}
					out = append(out, fmt.Sprintf("send %d: %s rows=%d", round, errString(b.Send()), b.Rows()))
				}
				return out
			})
		})
		// Read back in every comparison mode.
		sc := &scenario{ref: ref, our: our, db: db}
		var err error
		if sc.refConn, err = ref.open(db); err != nil {
			t.Fatal(err)
		}
		if sc.ourConn, err = our.open(db); err != nil {
			t.Fatal(err)
		}
		defer sc.refConn.Close()
		defer sc.ourConn.Close()
		for _, q := range []string{
			"SELECT * FROM ev ORDER BY tenant_id, event_time, event_id",
			"SELECT tenant_id, campaign_id, hour, sum(events), max(last_seen), uniqExactMerge(users) FROM hourly GROUP BY tenant_id, campaign_id, hour ORDER BY tenant_id, campaign_id, hour",
			"SELECT tenant_id, count(), sum(cost), avg(bid), uniqExact(user_id), countIf(note IS NULL) FROM ev GROUP BY tenant_id ORDER BY tenant_id",
		} {
			s := &stmt{sql: q, formats: []string{"TSVWithNamesAndTypes", "JSONEachRow", "Native"}}
			t.Run(autoName(q), func(t *testing.T) { sc.query(t, s, q, "client_api_test.go") })
		}
	})

	t.Run("scan_into_go_types", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withConn(tg, func(c driver.Conn) []string {
				var (
					s    string
					u64  uint64
					i64  int64
					f64  float64
					tm   time.Time
					tm3  time.Time
					arr  []string
					ns   *string
					nn   *string
					b    bool
					m    map[string]uint64
					u8   uint8
					i32  int32
					f32  float32
					d    time.Time
					lc   string
					tup  []any
					arr2 [][]uint8
				)
				err := c.QueryRow(ctx, `SELECT 'x', toUInt64(18446744073709551615), toInt64(-42), 0.1 + 0.2,
					toDateTime('2024-05-01 10:00:00'), toDateTime64('2024-05-01 10:00:00.123', 3), ['a', 'b'],
					CAST(NULL AS Nullable(String)), toNullable('y'), true, map('k', toUInt64(1)), toUInt8(255),
					toInt32(-2147483648), toFloat32(1.5), toDate('2024-02-29'), toLowCardinality('lc'),
					tuple('t', toUInt8(1)), [[1], []]`).
					Scan(&s, &u64, &i64, &f64, &tm, &tm3, &arr, &ns, &nn, &b, &m, &u8, &i32, &f32, &d, &lc, &tup, &arr2)
				return []string{errString(err), show(s), show(u64), show(i64), show(f64), show(tm), show(tm3), show(arr),
					show(ns), show(nn), show(b), show(m), show(u8), show(i32), show(f32), show(d), show(lc), show(tup), show(arr2)}
			})
		})
	})

	t.Run("scan_struct", func(t *testing.T) {
		type row struct {
			Tenant string    `ch:"tenant_id"`
			N      uint64    `ch:"n"`
			Last   time.Time `ch:"last"`
		}
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withConn(tg, func(c driver.Conn) []string {
				var rows []row
				err := c.Select(ctx, &rows, "SELECT tenant_id, count() AS n, max(event_time) AS last FROM ev GROUP BY tenant_id ORDER BY tenant_id")
				out := []string{errString(err)}
				for _, r := range rows {
					out = append(out, show(r))
				}
				return out
			})
		})
	})

	t.Run("params_and_settings", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withConn(tg, func(c driver.Conn) []string {
				var out []string
				qctx := clickhouse.Context(ctx,
					clickhouse.WithParameters(clickhouse.Parameters{"t": "acme", "ids": "['r0-00','r1-03','nope']"}),
					clickhouse.WithSettings(clickhouse.Settings{"max_threads": 2, "max_block_size": 7}))
				rows, err := c.Query(qctx, "SELECT event_id FROM ev WHERE tenant_id = {t:String} AND event_id IN {ids:Array(String)} ORDER BY event_id")
				if err != nil {
					return []string{errString(err)}
				}
				for rows.Next() {
					var id string
					_ = rows.Scan(&id)
					out = append(out, id)
				}
				rows.Close()
				var n uint64
				err = c.QueryRow(ctx, "SELECT count() FROM ev WHERE tenant_id = ? AND event_time < ?", "globex", time.Date(2024, 5, 1, 2, 0, 0, 0, time.UTC)).Scan(&n)
				out = append(out, errString(err), show(n))
				err = c.QueryRow(ctx, "SELECT count() FROM ev WHERE tenant_id = @t AND cost >= @c", clickhouse.Named("t", "acme"), clickhouse.Named("c", 10000)).Scan(&n)
				out = append(out, errString(err), show(n))
				return out
			})
		})
	})

	t.Run("exec_and_exceptions", func(t *testing.T) {
		diffBoth(t, ref, our, func(t *testing.T, tg *target) []string {
			return withConn(tg, func(c driver.Conn) []string {
				var out []string
				out = append(out, "exec "+errString(c.Exec(ctx, "CREATE TABLE IF NOT EXISTS st (k String, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k")))
				out = append(out, "ins "+errString(c.Exec(ctx, "INSERT INTO st VALUES (?, ?, ?)", "a", 1, 5)))
				out = append(out, "ins "+errString(c.Exec(ctx, "INSERT INTO st VALUES (?, ?, ?)", "a", 2, 4)))
				var v uint64
				err := c.QueryRow(ctx, "SELECT v FROM st FINAL WHERE k = 'a'").Scan(&v)
				out = append(out, errString(err), show(v))
				out = append(out, "bad "+errString(c.Exec(ctx, "INSERT INTO nope VALUES (1)")))
				out = append(out, "bad "+errString(c.QueryRow(ctx, "SELECT * FROM nope").Scan(&v)))
				_, err = c.Query(ctx, "SELECT x FROM st")
				out = append(out, "bad "+errString(err))
				_, err = c.PrepareBatch(ctx, "INSERT INTO nope")
				out = append(out, "batch "+errString(err))
				return out
			})
		})
	})

	t.Run("server_version_handshake", func(t *testing.T) {
		for _, tg := range []*target{ref, our} {
			conn, err := clickhouse.Open(openOpts(tg, db, nil))
			if err != nil {
				t.Fatalf("%s: open: %v", tg.name, err)
			}
			v, err := conn.ServerVersion()
			if err != nil {
				t.Errorf("%s: ServerVersion: %v", tg.name, err)
			} else if v.Version.Major != 25 || v.Revision == 0 || v.Timezone == nil {
				t.Errorf("%s: unexpected server version %+v", tg.name, v)
			}
			conn.Close()
		}
	})
}
