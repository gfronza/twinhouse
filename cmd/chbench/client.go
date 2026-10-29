package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// server is one side of the comparison. Both sides get byte-identical
// requests; only the base URL differs.
type server struct {
	name string
	base string
	db   string
	// timeout is sent as max_execution_time on every request: a ClickHouse
	// query keeps running after its client disconnects.
	timeout time.Duration
	hc      *http.Client
}

func newServer(name, base, db string, timeout time.Duration) *server {
	return &server{
		name: name, base: base, db: db, timeout: timeout,
		hc: &http.Client{
			Timeout:   timeout + 30*time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: 64},
		},
	}
}

// reply is what a request returned. body is kept only when asked for.
type reply struct {
	elapsed time.Duration
	body    []byte
}

// do sends query (in the URL) with an optional request body and reads the
// whole response. keep keeps the body; final asks for the response only
// after the query ends (wait_end_of_query), as statements and checks need.
func (s *server) do(ctx context.Context, query string, data []byte, settings url.Values, keep, final bool) (reply, error) {
	q := url.Values{}
	for k, v := range settings {
		q[k] = v
	}
	q.Set("query", query)
	q.Set("database", s.db)
	q.Set("max_execution_time", strconv.Itoa(int(s.timeout/time.Second)))
	if final {
		q.Set("wait_end_of_query", "1")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+"/?"+q.Encode(), bytes.NewReader(data))
	if err != nil {
		return reply{}, err
	}
	start := time.Now()
	resp, err := s.hc.Do(req)
	if err != nil {
		return reply{}, fmt.Errorf("%s: %w", s.name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var r reply
	if keep || resp.StatusCode != http.StatusOK {
		r.body, err = io.ReadAll(resp.Body)
	} else {
		_, err = io.Copy(io.Discard, resp.Body)
	}
	r.elapsed = time.Since(start)
	if err != nil {
		return reply{}, fmt.Errorf("%s: reading response: %w", s.name, err)
	}
	if resp.StatusCode != http.StatusOK {
		return reply{}, fmt.Errorf("%s: HTTP %d: %s\nquery: %s", s.name, resp.StatusCode, clip(r.body, 400), clip([]byte(query), 400))
	}
	return r, nil
}

// exec runs a statement whose result is not needed.
func (s *server) exec(ctx context.Context, query string) error {
	_, err := s.do(ctx, query, nil, nil, false, true)
	return err
}

// text runs a query and returns its TSV output.
func (s *server) text(ctx context.Context, query string) (string, error) {
	r, err := s.do(ctx, query, nil, nil, true, true)
	return string(bytes.TrimSpace(r.body)), err
}

func clip(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}
