//go:build compat

package compat

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// target is one server under comparison.
type target struct {
	name string // "reference" | "twinhouse"
	base *url.URL
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	targetsOnce sync.Once
	refTarget   *target
	ourTarget   *target
	targetsErr  error
	runID       string
	httpClient  = &http.Client{Timeout: 60 * time.Second}
)

func loadTargets() {
	ref, err := url.Parse(envOr("CLICKHOUSE_REF_URL", "http://localhost:18123"))
	if err != nil {
		targetsErr = err
		return
	}
	our, err := url.Parse(envOr("TWINHOUSE_URL", "http://localhost:8123"))
	if err != nil {
		targetsErr = err
		return
	}
	refTarget = &target{name: "reference", base: ref}
	ourTarget = &target{name: "twinhouse", base: our}
	for _, tg := range []*target{refTarget, ourTarget} {
		resp, err := httpClient.Get(tg.base.JoinPath("/ping").String())
		if err != nil {
			targetsErr = fmt.Errorf("%s (%s) unreachable: %v", tg.name, tg.base, err)
			return
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			targetsErr = fmt.Errorf("%s (%s) /ping: HTTP %d", tg.name, tg.base, resp.StatusCode)
			return
		}
	}
	var b [3]byte
	_, _ = rand.Read(b[:])
	runID = strconv.FormatInt(time.Now().Unix()%1_000_000, 36) + hex.EncodeToString(b[:])
}

// requireTargets skips the test when either server is unreachable.
func requireTargets(t *testing.T) (*target, *target) {
	t.Helper()
	targetsOnce.Do(loadTargets)
	if targetsErr != nil {
		t.Skipf("compat targets unavailable: %v", targetsErr)
	}
	return refTarget, ourTarget
}

// httpResult is one raw HTTP exchange.
type httpResult struct {
	status  int
	code    int // X-ClickHouse-Exception-Code (0 if none)
	body    []byte
	ctype   string
	xformat string
	summary string // X-ClickHouse-Summary
	err     error  // transport error
}

func (r httpResult) failed() bool { return r.err != nil || r.status != 200 || r.code != 0 }

func (r httpResult) describe() string {
	if r.err != nil {
		return "transport error: " + r.err.Error()
	}
	return fmt.Sprintf("HTTP %d code=%d body=%q", r.status, r.code, truncate(string(r.body), 600))
}

var (
	codeInBody   = regexp.MustCompile(`Code: (\d+)\.`)
	nativeCodeRe = regexp.MustCompile(`(?i)code:\s*(\d+)`)
)

type reqOpts struct {
	db       string
	params   []kv
	settings []kv
	gzip     bool
	split    bool // first line → ?query=, rest → body
	// acceptGzip asks for a gzip response and keeps the body compressed.
	acceptGzip bool
}

// do runs one statement over raw HTTP (POST, query in body).
func (tg *target) do(ctx context.Context, sql string, o reqOpts) httpResult {
	u := *tg.base
	u.Path = "/"
	q := url.Values{}
	if o.db != "" {
		q.Set("database", o.db)
	}
	for _, p := range o.params {
		q.Set("param_"+p.k, p.v)
	}
	for _, s := range o.settings {
		q.Set(s.k, s.v)
	}
	body := []byte(sql)
	if o.split {
		head, rest, _ := strings.Cut(sql, "\n")
		q.Set("query", head)
		body = []byte(rest + "\n")
	}
	u.RawQuery = q.Encode()
	var rd io.Reader = bytes.NewReader(body)
	if o.gzip {
		var zb bytes.Buffer
		zw := gzip.NewWriter(&zb)
		_, _ = zw.Write(body)
		_ = zw.Close()
		rd = &zb
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), rd)
	if err != nil {
		return httpResult{err: err}
	}
	if o.gzip {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if o.acceptGzip {
		req.Header.Set("Accept-Encoding", "gzip")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return httpResult{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	r := httpResult{status: resp.StatusCode, body: b, err: err,
		ctype: resp.Header.Get("Content-Type"), xformat: resp.Header.Get("X-ClickHouse-Format"),
		summary: resp.Header.Get("X-ClickHouse-Summary")}
	if c := resp.Header.Get("X-ClickHouse-Exception-Code"); c != "" {
		r.code, _ = strconv.Atoi(c)
	} else if r.status != 200 {
		if m := codeInBody.FindSubmatch(b); m != nil {
			r.code, _ = strconv.Atoi(string(m[1]))
		}
	}
	// An exception after the response was committed (HTTP 200) shows up as an
	// error trailer in the body.
	if r.status == 200 && r.code == 0 {
		if i := bytes.LastIndex(b, []byte("DB::Exception")); i >= 0 && bytes.Contains(b[max(0, i-40):i], []byte("Code: ")) {
			if m := codeInBody.FindSubmatch(b[max(0, i-40):]); m != nil {
				r.code, _ = strconv.Atoi(string(m[1]))
			}
		}
	}
	return r
}

// mustExec runs a DDL/DML statement and fails the test on error.
func (tg *target) mustExec(t testing.TB, ctx context.Context, sql string, o reqOpts) {
	t.Helper()
	if r := tg.do(ctx, sql, o); r.failed() {
		t.Fatalf("%s: %s\n  -> %s", tg.name, sql, r.describe())
	}
}

// --- clickhouse-go (Native over HTTP) ---------------------------------------

func (tg *target) open(db string) (driver.Conn, error) {
	return clickhouse.Open(openOpts(tg, db, nil))
}

type nativeResult struct {
	cols []string // "name type"
	rows []string // canonical row text
	code int      // exception code
	err  error
}

func (r nativeResult) render() string {
	if r.err != nil {
		if r.code != 0 {
			return fmt.Sprintf("ERROR code=%d", r.code)
		}
		return "ERROR " + r.err.Error()
	}
	var sb strings.Builder
	sb.WriteString(strings.Join(r.cols, "\t"))
	sb.WriteByte('\n')
	for _, row := range r.rows {
		sb.WriteString(row)
		sb.WriteByte('\n')
	}
	return sb.String()
}

func nativeQuery(ctx context.Context, conn driver.Conn, sql string, s *stmt) nativeResult {
	if len(s.params) > 0 {
		p := clickhouse.Parameters{}
		for _, kv := range s.params {
			p[kv.k] = kv.v
		}
		ctx = clickhouse.Context(ctx, clickhouse.WithParameters(p))
	}
	if len(s.settings) > 0 {
		st := clickhouse.Settings{}
		for _, kv := range s.settings {
			st[kv.k] = kv.v
		}
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(st))
	}
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nativeErr(err)
	}
	defer rows.Close()
	var res nativeResult
	cts := rows.ColumnTypes()
	for _, ct := range cts {
		res.cols = append(res.cols, ct.Name()+" "+ct.DatabaseTypeName())
	}
	for rows.Next() {
		ptrs := make([]any, len(cts))
		for i, ct := range cts {
			st := ct.ScanType()
			if st == nil {
				var a any
				ptrs[i] = &a
				continue
			}
			ptrs[i] = reflect.New(st).Interface()
		}
		if err := rows.Scan(ptrs...); err != nil {
			res.err = fmt.Errorf("scan: %w", err)
			return res
		}
		cells := make([]string, len(ptrs))
		for i, p := range ptrs {
			cells[i] = canon(reflect.ValueOf(p).Elem())
		}
		res.rows = append(res.rows, strings.Join(cells, "\t"))
	}
	if err := rows.Err(); err != nil {
		return nativeErr(err)
	}
	if s.unordered {
		sort.Strings(res.rows)
	}
	return res
}

func nativeErr(err error) nativeResult {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		return nativeResult{err: err, code: int(ex.Code)}
	}
	if m := nativeCodeRe.FindStringSubmatch(err.Error()); m != nil {
		n, _ := strconv.Atoi(m[1])
		return nativeResult{err: err, code: n}
	}
	return nativeResult{err: err}
}

var (
	timeType   = reflect.TypeOf(time.Time{})
	bigIntType = reflect.TypeOf(big.Int{})
)

// canon renders a scanned value deterministically, including its Go type.
func canon(v reflect.Value) string {
	if !v.IsValid() {
		return "NULL"
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return "NULL"
		}
		return canon(v.Elem())
	}
	switch v.Type() {
	case timeType:
		tm := v.Interface().(time.Time)
		return "time(" + tm.Format("2006-01-02 15:04:05.999999999") + " " + tm.Location().String() + ")"
	case bigIntType:
		b := v.Interface().(big.Int)
		return "big(" + b.String() + ")"
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		bits := 64
		if v.Kind() == reflect.Float32 {
			bits = 32
		}
		s := strconv.FormatFloat(f, 'g', -1, bits)
		if f == 0 && math.Signbit(f) {
			s = "-0"
		}
		return v.Type().String() + "(" + s + ")"
	case reflect.String:
		return strconv.Quote(v.String())
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return v.Type().String() + "[]"
		}
		parts := make([]string, v.Len())
		for i := range parts {
			parts[i] = canon(v.Index(i))
		}
		return v.Type().String() + "[" + strings.Join(parts, ",") + "]"
	case reflect.Map:
		keys := v.MapKeys()
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = canon(k) + ":" + canon(v.MapIndex(k))
		}
		sort.Strings(parts)
		return v.Type().String() + "{" + strings.Join(parts, ",") + "}"
	case reflect.Struct:
		var parts []string
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				parts = append(parts, v.Type().Field(i).Name+"="+canon(v.Field(i)))
			}
		}
		return v.Type().String() + "{" + strings.Join(parts, ",") + "}"
	}
	return fmt.Sprintf("%s(%v)", v.Type(), v.Interface())
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
