package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
)

// Handler is the Lambda entry point for API Gateway REST (v1) proxy events.
type Handler func(ctx context.Context, ev events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error)

// App is the per-execution-environment state shared by invocations. The
// Lambda has no background goroutines: the work chserver's loops do (system
// tables, catalog reloads) happens at the start of an invocation instead.
type App struct {
	// Catalog is refreshed when its snapshot is older than MaxAge.
	Catalog catalog.Catalog
	// MaxAge is the maximum catalog snapshot age (CATALOG_REFRESH).
	MaxAge time.Duration
	// Ensure, when set, runs once before the first request that reaches the
	// server (Store.CheckSystemTables: the system tables exist; the
	// reconciler creates them, a request never waits for table
	// operations); it is retried on the next request while it fails.
	Ensure func(context.Context) error
	// ServerVersion is shown in the exception text of a failed prepare.
	ServerVersion string
	Logger        *slog.Logger
	// Now overrides the clock (tests).
	Now func() time.Time

	mu        sync.Mutex
	ensured   bool
	refreshed time.Time
}

func (a *App) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func (a *App) log() *slog.Logger {
	if a.Logger != nil {
		return a.Logger
	}
	return slog.Default()
}

// Prepare makes the catalog current for one request: it checks the system
// tables exist (once) and reloads the catalog snapshot when it is older
// than MaxAge. A failed reload is logged and the previous snapshot is served,
// unless there is none yet, in which case the request fails (Unavailable).
func (a *App) Prepare(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.ensured && a.Ensure != nil {
		if err := a.Ensure(ctx); err != nil {
			return cherr.Wrap(cherr.Unavailable, err, "system tables not ready")
		}
	}
	a.ensured = true
	now := a.now()
	if !a.refreshed.IsZero() && now.Sub(a.refreshed) <= a.MaxAge {
		return nil
	}
	if err := a.Catalog.Refresh(ctx); err != nil {
		if a.refreshed.IsZero() {
			return cherr.Wrap(cherr.Unavailable, err, "cannot load catalog")
		}
		a.log().Warn("catalog refresh failed; using last snapshot", "age", now.Sub(a.refreshed), "err", err)
		return nil
	}
	a.refreshed = now
	return nil
}

// Middleware wraps the ClickHouse server for the Lambda: it prepares the
// catalog before each request and restores what API Gateway does not
// forward.
func (a *App) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := a.Prepare(r.Context()); err != nil {
			a.log().Error("request rejected: catalog not ready", "err", err)
			writeException(w, cherr.As(err), a.ServerVersion)
			return
		}
		// The adapter builds the request from the (fully known) event body
		// and sets r.ContentLength; API Gateway does not always forward the
		// Content-Length header the server requires of POST requests.
		if r.Method == http.MethodPost && r.ContentLength >= 0 && r.Header.Get("Content-Length") == "" && len(r.TransferEncoding) == 0 {
			r.Header.Set("Content-Length", strconv.FormatInt(r.ContentLength, 10))
		}
		next.ServeHTTP(w, r)
	})
}

// writeException renders e like the server renders an exception raised
// before any output (plain text, the X-ClickHouse-Exception-Code header).
func writeException(w http.ResponseWriter, e *cherr.Error, version string) {
	msg := e.Msg
	if !strings.HasSuffix(msg, ".") {
		msg += "."
	}
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=UTF-8")
	h.Set("X-ClickHouse-Exception-Code", strconv.Itoa(e.Code))
	w.WriteHeader(e.HTTPStatus())
	fmt.Fprintf(w, "Code: %d. DB::Exception: %s (%s) (version %s)\n", e.Code, msg, e.Name(), version)
}

// NewHandler returns the Lambda handler serving h (the ClickHouse server
// wrapped by App.Middleware) for API Gateway REST proxy events.
//
// Bodies: the adapter decodes base64 request bodies (the API has
// BinaryMediaTypes */*, so API Gateway base64-encodes every request body)
// and base64-encodes a response body that is not valid UTF-8. Binary output
// that happens to be valid UTF-8 (a short Native block, a compressed
// response) would be returned as text and re-encoded by API Gateway, so the
// handler also forces base64 for binary content types and encoded bodies.
func NewHandler(h http.Handler) Handler {
	ad := httpadapter.New(h)
	return func(ctx context.Context, ev events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		resp, err := ad.ProxyWithContext(ctx, ev)
		if err != nil {
			return resp, err
		}
		return finalize(resp), nil
	}
}

// finalize base64-encodes a text response whose content is binary (see
// NewHandler). API Gateway decodes it when returning the body to the client.
func finalize(resp events.APIGatewayProxyResponse) events.APIGatewayProxyResponse {
	if resp.IsBase64Encoded || resp.Body == "" || !binaryResponse(resp) {
		return resp
	}
	resp.Body = base64.StdEncoding.EncodeToString([]byte(resp.Body))
	resp.IsBase64Encoded = true
	return resp
}

// binaryResponse reports whether the response carries bytes rather than
// text: a Content-Encoding (gzip, br, zstd, ...) or a non-text Content-Type.
func binaryResponse(resp events.APIGatewayProxyResponse) bool {
	get := func(name string) string {
		if v := http.Header(resp.MultiValueHeaders).Get(name); v != "" {
			return v
		}
		for k, v := range resp.Headers {
			if strings.EqualFold(k, name) {
				return v
			}
		}
		return ""
	}
	if get("Content-Encoding") != "" {
		return true
	}
	ct := strings.ToLower(get("Content-Type"))
	if ct == "" {
		return false
	}
	mt, _, _ := strings.Cut(ct, ";")
	mt = strings.TrimSpace(mt)
	switch {
	case strings.HasPrefix(mt, "text/"):
		return false
	case mt == "application/json", strings.HasSuffix(mt, "+json"), mt == "application/x-ndjson":
		return false
	}
	return true
}
