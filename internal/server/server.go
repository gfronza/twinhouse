// Package server implements the ClickHouse HTTP interface (DESIGN.md §2) on
// top of the query engine: request parsing exactly like ClickHouse's
// HTTPHandler (query in the URL and/or body, settings and parameters from the
// URL, auth, sessions, compression), output formatting and ClickHouse-style
// error rendering.
package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// Executor runs one statement. *exec.Engine implements it.
type Executor interface {
	Execute(ctx context.Context, req *exec.Request) (*exec.Result, error)
}

var _ Executor = (*exec.Engine)(nil)

// Defaults.
const (
	DefaultServerVersion = "25.8.1.1"
	// ServerRevision is the protocol revision reported by revision().
	ServerRevision = 54477

	defaultResponseBuffer = 1 << 20
	// defaultMaxQuerySize is ClickHouse's max_query_size default.
	defaultMaxQuerySize = 262144
	// defaultQuerySizeCeiling is the largest max_query_size a request may
	// set unless Config.MaxQuerySize says otherwise.
	defaultQuerySizeCeiling = 256 << 20
	// bodySplitThreshold: bodies up to this size are handed to the engine as
	// one query text; larger INSERT bodies are split at the start of their
	// inline data, which is then streamed.
	bodySplitThreshold = 1 << 20
	// defaultBlockSize is ClickHouse's default max_block_size; result rows
	// are passed to encoders (Native: one block each) in batches of it.
	defaultBlockSize = 65409
	maxBlockSize     = 1 << 20
	// defaultSendTimeout is ClickHouse's http_send_timeout default.
	defaultSendTimeout = 30 * time.Second
)

// Config configures a Server.
type Config struct {
	// Executor runs statements (required).
	Executor Executor
	// Catalog, when set, is used to reject requests for unknown databases
	// before execution (like ClickHouse does for the `database` parameter).
	Catalog catalog.Catalog
	// Users maps user -> password. Empty means {"default": ""}.
	Users map[string]string
	// ServerVersion is shown in exception texts ("(version ...)").
	ServerVersion string
	// DisplayName is sent as X-ClickHouse-Server-Display-Name.
	DisplayName string
	// Timezone is sent as X-ClickHouse-Timezone (default "UTC").
	Timezone string
	// Logger receives request logs (default slog.Default()).
	Logger *slog.Logger
	// MaxSessionTimeout bounds session_timeout (default 3600s).
	MaxSessionTimeout time.Duration
	// ResponseBufferSize is how many bytes of output are buffered before the
	// status and headers are sent (default 1 MiB). Errors raised before that
	// get a proper HTTP error status; later ones are appended to the body.
	ResponseBufferSize int
	// BufferResponses buffers every response whole before sending it, so
	// every error is an HTTP error; what a SELECT buffers past
	// ResponseBufferSize counts against its max_memory_usage. The Lambda
	// sets it: its adapter holds the whole response anyway.
	BufferResponses bool
	// MaxQuerySize is the largest max_query_size a request may set
	// (default 256 MiB); larger values are lowered to it.
	MaxQuerySize int
	// MaxExecutionTime is the default max_execution_time of every query
	// (0: unlimited, like ClickHouse). A max_execution_time setting (URL,
	// session or the query's SETTINGS clause) overrides it.
	MaxExecutionTime time.Duration
	// RejectSessions makes every request that uses HTTP sessions
	// (session_id, session_check or session_timeout) fail with
	// NOT_IMPLEMENTED (48): deployments without a long-lived process (the
	// Lambda API) cannot keep per-session state between requests.
	RejectSessions bool
	// Now overrides the clock (tests).
	Now func() time.Time
}

// Server is an http.Handler speaking the ClickHouse HTTP protocol.
type Server struct {
	cfg      Config
	users    map[string]string
	log      *slog.Logger
	now      func() time.Time
	sessions *sessionManager
}

// New creates a Server.
func New(cfg Config) (*Server, error) {
	if cfg.Executor == nil {
		return nil, fmt.Errorf("server: Executor is required")
	}
	if cfg.ServerVersion == "" {
		cfg.ServerVersion = DefaultServerVersion
	}
	if cfg.Timezone == "" {
		cfg.Timezone = "UTC"
	}
	if cfg.ResponseBufferSize <= 0 {
		cfg.ResponseBufferSize = defaultResponseBuffer
	}
	if cfg.MaxQuerySize <= 0 {
		cfg.MaxQuerySize = defaultQuerySizeCeiling
	}
	s := &Server{cfg: cfg, users: cfg.Users, log: cfg.Logger, now: cfg.Now}
	if len(s.users) == 0 {
		s.users = map[string]string{defaultUser: ""}
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.now == nil {
		s.now = time.Now
	}
	s.sessions = newSessionManager(s.now, cfg.MaxSessionTimeout)
	return s, nil
}

const exposeHeaders = "X-ClickHouse-Query-Id,X-ClickHouse-Summary,X-ClickHouse-Server-Display-Name,X-ClickHouse-Format,X-ClickHouse-Timezone,X-ClickHouse-Exception-Code"

const helpText = "\n\nUse / or /ping for health checks.\nOr /replicas_status for more sophisticated health checks.\n\n" +
	"Send queries from your program with POST method or GET /?query=...\n"

// ServeHTTP routes like ClickHouse's default handlers.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/ping":
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			s.writeOk(w, true)
			return
		}
	case "/replicas_status":
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "4")
			_, _ = io.WriteString(w, "Ok.\n")
			return
		}
	case "/", "":
		switch r.Method {
		case http.MethodOptions:
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", "*")
			h.Set("Access-Control-Allow-Headers", "origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization")
			h.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
			h.Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		case http.MethodGet, http.MethodHead:
			if r.URL.RawQuery == "" && !r.URL.ForceQuery {
				s.writeOk(w, true)
				return
			}
			s.serveQuery(w, r)
			return
		case http.MethodPost:
			s.serveQuery(w, r)
			return
		default:
			w.WriteHeader(http.StatusNotImplemented)
			return
		}
	}
	target := r.URL.Path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.WriteHeader(http.StatusNotFound)
	_, _ = io.WriteString(w, "There is no handle "+target+helpText)
}

func (s *Server) writeOk(w http.ResponseWriter, summary bool) {
	h := w.Header()
	if summary {
		h.Set("X-ClickHouse-Summary", summaryJSON(summaryStats{}))
	}
	h.Set("Content-Type", "text/html; charset=UTF-8")
	_, _ = io.WriteString(w, "Ok.\n")
}

// reservedParams are URL parameters that are not settings.
var reservedParams = map[string]bool{
	"query": true, "database": true, "default_format": true, "client_protocol_version": true,
	"query_id": true, "session_id": true, "session_timeout": true, "session_check": true,
	"user": true, "password": true, "quota_key": true, "compress": true, "decompress": true,
	"enable_http_compression": true, "wait_end_of_query": true, "buffer_size": true,
	"stacktrace": true,
}

// call is the state of one query request.
type call struct {
	s      *Server
	w      http.ResponseWriter
	r      *http.Request
	start  time.Time
	params url.Values

	user      string
	queryID   string
	tz        string
	sess      *session
	sessIn    sessionState
	sessDelta sessionDelta
	// settings visible to this query: session settings overridden by URL ones.
	settings map[string]string

	// Response compression; errors are compressed only after authentication
	// (compressReady), like ClickHouse.
	nativeCompress bool
	httpEncoding   string
	compressReady  bool

	query         string
	keyword       string
	defaultFormat string
	data          *countingReader

	// outFormat is the selected output format once a result is written,
	// outCanon its canonical name once its encoder is set up.
	outFormat string
	outCanon  string
	// sendTimeout is http_send_timeout (0: none).
	sendTimeout time.Duration
	// bufferSize is how many response bytes are buffered before the
	// commit; wholeBody buffers the whole response (bufferSize -1)
	// because the client or the server config asked for it;
	// explicitBuffer is set when the client gave the size.
	bufferSize     int
	wholeBody      bool
	explicitBuffer bool
	// outSettings caches outputSettings().
	outSettings map[string]string

	// queryCtx is the running query's context, bounded by queryLimit
	// (max_execution_time; 0 = none).
	queryCtx   context.Context
	queryLimit time.Duration

	// outcome, for logging
	status    int
	errCode   int
	errMsg    string
	result    *exec.Result
	committed bool
}

func (s *Server) serveQuery(w http.ResponseWriter, r *http.Request) {
	c := &call{s: s, w: w, r: r, start: s.now(), params: r.URL.Query(), status: http.StatusOK}
	defer c.logRequest()
	defer func() {
		if p := recover(); p != nil {
			s.log.Error("panic serving query", "query_id", c.queryID, "panic", p, "stack", string(debug.Stack()))
			if !c.committed {
				c.fail(cherr.New(cherr.LogicalError, "Internal error: %v", p))
			}
		}
	}()
	defer func() {
		if c.sess != nil {
			c.releaseSession()
		}
	}()
	h := w.Header()
	h.Set("Access-Control-Expose-Headers", exposeHeaders)
	if s.cfg.DisplayName != "" {
		h.Set("X-ClickHouse-Server-Display-Name", s.cfg.DisplayName)
	}

	user, password, err := credentials(r, c.params)
	if err == nil {
		err = authenticate(s.users, user, password)
	}
	c.user = user
	if err != nil {
		c.fail(err)
		return
	}

	c.queryID = c.params.Get("query_id")
	if c.queryID == "" {
		c.queryID = uuid.NewString()
	}
	h.Set("X-ClickHouse-Query-Id", c.queryID)
	c.tz = s.cfg.Timezone
	h.Set("X-ClickHouse-Timezone", c.tz)

	if err := c.run(); err != nil {
		c.fail(err)
	}
}

func (c *call) run() error {
	r, s := c.r, c.s
	if err := c.openSession(); err != nil {
		return err
	}
	c.buildSettings()
	if tz := c.settings["session_timezone"]; tz != "" {
		c.tz = tz
		c.w.Header().Set("X-ClickHouse-Timezone", tz)
	}

	readonly, err := c.readonlyLevel()
	if err != nil {
		return err
	}

	c.nativeCompress = c.setting("compress") == "1"
	if isTrue(c.setting("enable_http_compression")) {
		c.httpEncoding = chooseResponseEncoding(r.Header.Get("Accept-Encoding"))
	}
	c.compressReady = true
	c.defaultFormat = r.Header.Get("X-ClickHouse-Format")
	if c.defaultFormat == "" {
		c.defaultFormat = c.setting("default_format")
	}

	if r.Method == http.MethodPost && r.ContentLength <= 0 && len(r.TransferEncoding) == 0 && r.Header.Get("Content-Length") == "" {
		return cherr.New(cherr.HTTPLengthRequired, "The Transfer-Encoding is not chunked and there is no Content-Length header for POST request")
	}
	if ct := strings.ToLower(r.Header.Get("Content-Type")); strings.HasPrefix(ct, "multipart/form-data") {
		return cherr.New(cherr.NotImplemented, "multipart/form-data requests (external tables) are not supported")
	}
	body, err := decodeRequestBody(r.Header.Get("Content-Encoding"), r.Body)
	if err != nil {
		return err
	}
	defer body.Close()
	var in io.Reader = body
	if c.setting("decompress") == "1" {
		nr := newNativeReader(body)
		defer nr.Close()
		in = nr
	}
	maxQuerySize, sizeErr := c.maxQuerySize()
	if _, inURL := c.params["max_query_size"]; sizeErr != nil && inURL {
		return sizeErr
	}
	if sizeErr != nil {
		// A bad value in the session must not block the SET replacing it.
		maxQuerySize = min(defaultMaxQuerySize, s.cfg.MaxQuerySize)
	}
	if err := c.assembleQuery(in, maxQuerySize); err != nil {
		return err
	}
	if isEmptyQuery(c.query) {
		return cherr.New(cherr.SyntaxError, "Empty query")
	}
	c.keyword = leadingKeyword(c.query)
	setsSize, err := c.setsMaxQuerySize()
	if err != nil {
		return err
	}
	if sizeErr != nil && !setsSize {
		return sizeErr
	}

	db := c.database()
	if s.cfg.Catalog != nil && !builtinDatabase(db) {
		if _, err := s.cfg.Catalog.Database(db); err != nil {
			return err
		}
	}
	if err := c.checkReadonly(readonly); err != nil {
		return err
	}

	req := &exec.Request{
		Query:    c.query,
		Params:   c.queryParams(),
		Database: db,
		Settings: c.engineSettings(readonly),
		QueryID:  c.queryID,
		User:     c.user,
		// A SELECT's rows are encoded as they are read.
		Stream: true,
		FormatOptions: format.Options{
			// Clients encode INSERT data with revision 0 over HTTP
			// (client_protocol_version only applies to output).
			Settings: c.settings,
			Timezone: c.tz,
		},
	}
	if c.data != nil {
		req.Data = c.data
	}
	limit, err := c.maxExecutionTime()
	if err != nil {
		return err
	}
	if c.sendTimeout, err = c.httpSendTimeout(); err != nil {
		return err
	}
	if err := c.readResponseBuffer(); err != nil {
		return err
	}
	res, cancel, err := c.execute(req, limit)
	// max_execution_time also covers sending the result.
	defer cancel()
	if err != nil {
		return err
	}
	if res == nil {
		res = &exec.Result{}
	}
	if res.Stream != nil {
		defer res.Stream.Close()
	}
	c.result = res
	c.updateSession(res)
	return c.writeResult(res)
}

// execute runs req, bounded by limit (max_execution_time; 0 = none), and
// returns the cancel func of the query's context: a streamed result keeps
// running in it until the caller cancels it.
func (c *call) execute(req *exec.Request, limit time.Duration) (res *exec.Result, cancel context.CancelFunc, err error) {
	c.queryCtx, cancel = c.r.Context(), func() {}
	if limit > 0 {
		c.queryCtx, cancel = context.WithTimeout(c.queryCtx, limit)
	}
	c.queryLimit = limit
	defer func() {
		if p := recover(); p != nil {
			c.s.log.Error("panic in executor", "query_id", c.queryID, "panic", p, "stack", string(debug.Stack()))
			err = cherr.New(cherr.LogicalError, "Internal error: %v", p)
		}
		if err != nil {
			err = c.queryError(err)
		}
	}()
	res, err = c.s.cfg.Executor.Execute(c.queryCtx, req)
	return res, cancel, err
}

// queryError maps an error of the running query: once max_execution_time
// has expired it is TIMEOUT_EXCEEDED (159), whatever error the cancelled
// storage call reported.
func (c *call) queryError(err error) error {
	if c.queryLimit > 0 && c.r.Context().Err() == nil && errors.Is(c.queryCtx.Err(), context.DeadlineExceeded) {
		return timeoutExceeded(c.s.now().Sub(c.start), c.queryLimit)
	}
	return err
}

// timeoutExceeded is ClickHouse 25.8's max_execution_time error, e.g.
// "Timeout exceeded: elapsed 1000.538376 ms, maximum: 1000 ms".
func timeoutExceeded(elapsed, limit time.Duration) error {
	ms := func(d time.Duration) string {
		return strconv.FormatFloat(float64(d.Nanoseconds())/1e6, 'f', -1, 64)
	}
	return cherr.New(cherr.Timeout, "Timeout exceeded: elapsed %s ms, maximum: %s ms", ms(elapsed), ms(limit))
}

// maxExecutionTime returns the query's max_execution_time (seconds, may be
// fractional; 0 or negative = unlimited), from the URL/session settings or
// the query's SETTINGS clause, else the server default. Values are parsed
// and rejected like ClickHouse does.
func (c *call) maxExecutionTime() (time.Duration, error) {
	v, ok := c.outputSettings()["max_execution_time"]
	if !ok {
		return c.s.cfg.MaxExecutionTime, nil
	}
	return parseDuration(v)
}

// httpSendTimeout returns http_send_timeout, how long one write to the
// client may block (seconds; 0 or negative = no limit), from the URL or
// session settings.
func (c *call) httpSendTimeout() (time.Duration, error) {
	v := c.setting("http_send_timeout")
	if v == "" {
		return defaultSendTimeout, nil
	}
	return parseDuration(v)
}

// readResponseBuffer reads how much of the response is buffered before
// its status and headers are sent, as ClickHouse 25.8 reads it: the whole
// body with wait_end_of_query (URL) or http_wait_end_of_query (URL or
// session); else http_response_buffer_size (URL or session; the engine
// reports an invalid value) if set, else the buffer_size URL parameter,
// else ResponseBufferSize. A given size, 0 included, is explicit.
func (c *call) readResponseBuffer() error {
	wait, err := c.waitEndOfQuery()
	if err != nil {
		return err
	}
	c.bufferSize = c.s.cfg.ResponseBufferSize
	c.wholeBody = c.s.cfg.BufferResponses || wait
	if v := c.setting("http_response_buffer_size"); v != "" {
		if n, err := exec.ParseSizeSetting(v); err == nil {
			c.setBufferSize(n)
		}
	} else if v, ok := c.params["buffer_size"]; ok && len(v) > 0 {
		n, err := parseBufferSize(v[0])
		if err != nil {
			return err
		}
		c.setBufferSize(n)
	}
	if c.wholeBody {
		c.bufferSize = -1
	}
	return nil
}

// setBufferSize sets the response buffer size the client gave.
func (c *call) setBufferSize(n uint64) {
	c.bufferSize = int(min(n, math.MaxInt))
	c.explicitBuffer = true
}

// waitEndOfQuery reports whether the client asked for the whole body to be
// buffered. http_wait_end_of_query is a Bool setting (0, 1, true or false
// in any case, else 467); the wait_end_of_query URL parameter is read as
// ClickHouse's readBoolText reads it (parseURLBool).
func (c *call) waitEndOfQuery() (bool, error) {
	if v := c.setting("http_wait_end_of_query"); v != "" {
		switch strings.ToLower(v) {
		case "1", "true":
			return true, nil
		case "0", "false":
		default:
			return false, cherr.New(cherr.CannotParseBool, "Cannot parse bool from string '%s'", v)
		}
	}
	if v, ok := c.params["wait_end_of_query"]; ok && len(v) > 0 {
		return parseURLBool(v[0])
	}
	return false, nil
}

// parseURLBool parses a bool URL parameter as ClickHouse 25.8 does: any
// first character but '0' is true, and a longer value starting with t or
// f must spell true or false (in any case).
func parseURLBool(v string) (bool, error) {
	if v == "" {
		return false, cherr.New(cherr.AttemptToReadAfterEOF, "Attempt to read after eof")
	}
	x, rest := v[0] != '0', v[1:]
	var err error
	if rest != "" {
		switch v[0] {
		case 't', 'T':
			rest, err = expectFold(rest, "rue")
		case 'f', 'F':
			x = false
			rest, err = expectFold(rest, "alse")
		}
	}
	if err != nil {
		return false, err
	}
	if rest != "" {
		return false, cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: '%s'", rest)
	}
	return x, nil
}

// expectFold reads the lower-case word from the start of s, ignoring
// ASCII case, and returns the rest.
func expectFold(s, word string) (string, error) {
	for i := 0; i < len(word); i++ {
		if i == len(s) {
			return "", cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected '%s' at end of stream.", word)
		}
		if ch := s[i] | 0x20; ch != word[i] {
			return "", cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected '%s' before: '%s'", word, s[i:])
		}
	}
	return s[len(word):], nil
}

// parseBufferSize parses the buffer_size URL parameter as ClickHouse 25.8
// does: an optional '+', then decimal digits only, too large a value
// saturating.
func parseBufferSize(v string) (uint64, error) {
	if v == "" {
		return 0, cherr.New(cherr.AttemptToReadAfterEOF, "Attempt to read after eof")
	}
	switch v[0] {
	case '-':
		return 0, cherr.New(cherr.CannotParseNumber, "Unsigned type must not contain '-' symbol")
	case '+':
		v = v[1:]
		switch {
		case v != "" && (v[0] == '+' || v[0] == '-'):
			return 0, cherr.New(cherr.CannotParseNumber, "Cannot parse number with multiple sign (+/-) characters")
		case v == "" || v[0] < '0' || v[0] > '9':
			return 0, cherr.New(cherr.CannotParseNumber, "Cannot parse number without any digits")
		}
	}
	n := 0
	for n < len(v) && v[n] >= '0' && v[n] <= '9' {
		n++
	}
	if n < len(v) {
		return 0, cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: '%s'", v[n:])
	}
	x, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return math.MaxUint64, nil
	}
	return x, nil
}

// parseDuration parses a Seconds setting (may be fractional); 0, negative
// and out-of-range values mean no limit.
func parseDuration(v string) (time.Duration, error) {
	secs, err := parseSecondsSetting(v)
	if err != nil {
		return 0, err
	}
	if secs <= 0 || math.IsNaN(secs) || secs > float64(math.MaxInt64/int64(time.Second)) {
		return 0, nil
	}
	return time.Duration(secs * float64(time.Second)), nil
}

// parseSecondsSetting parses a Seconds setting value the way ClickHouse
// reads it as a float.
func parseSecondsSetting(v string) (float64, error) {
	t := strings.TrimSpace(v)
	if t == "" {
		return 0, cherr.New(cherr.CannotParseNumber, "Cannot read floating point value")
	}
	if f, err := strconv.ParseFloat(t, 64); err == nil {
		return f, nil
	}
	n := len(t) - 1
	for ; n > 0; n-- {
		if _, err := strconv.ParseFloat(t[:n], 64); err == nil {
			break
		}
	}
	return 0, cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: '%s'", t[n:])
}

// setting returns a setting: URL parameter first, then the session.
func (c *call) setting(name string) string {
	if v, ok := c.params[name]; ok && len(v) > 0 {
		return v[0]
	}
	return c.sessIn.Settings[name]
}

func (c *call) buildSettings() {
	c.settings = make(map[string]string, len(c.sessIn.Settings)+len(c.params))
	for k, v := range c.sessIn.Settings {
		c.settings[k] = v
	}
	for k, v := range c.params {
		if len(v) == 0 || reservedParams[k] || strings.HasPrefix(k, "param_") {
			continue
		}
		c.settings[k] = v[0]
	}
}

// engineSettings are the settings passed to the engine: everything but
// HTTP-only parameters, plus readonly=2 for GET requests.
func (c *call) engineSettings(readonly int) map[string]string {
	out := make(map[string]string, len(c.settings)+1)
	for k, v := range c.settings {
		if !reservedParams[k] {
			out[k] = v
		}
	}
	if readonly > 0 {
		out["readonly"] = strconv.Itoa(readonly)
	}
	return out
}

func (c *call) queryParams() map[string]string {
	var out map[string]string
	for k, v := range c.params {
		if name, ok := strings.CutPrefix(k, "param_"); ok && len(v) > 0 {
			if out == nil {
				out = map[string]string{}
			}
			out[name] = v[0]
		}
	}
	return out
}

// database resolves the current database: X-ClickHouse-Database header,
// then the `database` parameter, then the session's, then "default".
func (c *call) database() string {
	if v := c.r.Header.Get("X-ClickHouse-Database"); v != "" {
		return v
	}
	if v := c.params.Get("database"); v != "" {
		return v
	}
	if c.sessIn.Database != "" {
		return c.sessIn.Database
	}
	return "default"
}

// builtinDatabase reports whether db always exists without a catalog entry:
// "default" and ClickHouse's virtual databases "system", "information_schema"
// and "INFORMATION_SCHEMA" (names are case-sensitive, as in ClickHouse).
func builtinDatabase(db string) bool {
	switch db {
	case "default", "system", "information_schema", "INFORMATION_SCHEMA":
		return true
	}
	return false
}

func (c *call) isGet() bool { return c.r.Method == http.MethodGet || c.r.Method == http.MethodHead }

// readonlyLevel returns the effective readonly level. GET implies readonly=2
// and the client may not change it.
func (c *call) readonlyLevel() (int, error) {
	v := c.setting("readonly")
	level := 0
	if v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 2 {
			return 0, cherr.New(cherr.CannotParseText, "Cannot parse readonly setting value '%s'", v)
		}
		level = n
	}
	if c.isGet() {
		if _, ok := c.params["readonly"]; ok && level != 2 {
			return 0, cherr.New(cherr.ReadonlyQuery, "Cannot modify 'readonly' setting in readonly mode")
		}
		level = 2
	}
	return level, nil
}

func (c *call) checkReadonly(level int) error {
	if level == 0 || c.keyword == "" {
		return nil
	}
	if c.keyword == "SET" {
		if level == 1 {
			return cherr.New(cherr.ReadonlyQuery, "Cannot modify '%s' setting in readonly mode", firstSettingName(c.query))
		}
		return nil
	}
	if readonlyKeywords[c.keyword] {
		return nil
	}
	if c.isGet() {
		return cherr.New(cherr.ReadonlyQuery, "%s: Cannot execute query in readonly mode. For queries over HTTP, method GET implies readonly. You should use method POST for modifying queries", c.user)
	}
	return cherr.New(cherr.ReadonlyQuery, "%s: Cannot execute query in readonly mode", c.user)
}

// firstSettingName returns the first setting name of a SET statement.
func firstSettingName(q string) string {
	l := lexer{s: q}
	l.skipSpace()
	l.word() // SET
	l.skipSpace()
	if l.eof() {
		return ""
	}
	if c := l.s[l.pos]; c == '`' || c == '"' {
		start := l.pos
		l.quoted()
		return strings.Trim(l.s[start:l.pos], "`\"")
	}
	return l.word()
}

// assembleQuery resolves the query text and INSERT data like ClickHouse:
// with a non-empty `query` parameter the body is appended to it (as INSERT
// data, or as the rest of the query text otherwise); without it the body is
// the query, possibly followed by inline INSERT data. The query part may be
// at most maxSize bytes (max_query_size).
func (c *call) assembleQuery(body io.Reader, maxSize int) error {
	qp := c.params.Get("query")
	if qp != "" {
		if leadingKeyword(qp) == "INSERT" {
			c.query = qp
			c.data = &countingReader{r: body}
			return checkQuerySize(c.query, maxSize)
		}
		rest, err := readLimited(body, maxSize)
		if err != nil {
			return err
		}
		c.query = qp
		if len(rest) > 0 {
			c.query = qp + "\n" + string(rest)
		}
		return checkQuerySize(c.query, maxSize)
	}
	br := bufio.NewReaderSize(body, 64<<10)
	head, err := readLimited(io.LimitReader(br, bodySplitThreshold+1), bodySplitThreshold+1)
	if err != nil {
		return err
	}
	if len(head) <= bodySplitThreshold {
		c.query = string(head)
		return checkQuerySize(c.query, maxSize)
	}
	if leadingKeyword(string(head[:min(len(head), 4096)])) == "INSERT" {
		if end, isFormat, ok := insertDataSplit(string(head)); ok {
			c.query = string(head[:end])
			var data io.Reader = io.MultiReader(bytes.NewReader(head[end:]), br)
			if isFormat {
				data = trimInlineDataStart(data)
			}
			c.data = &countingReader{r: data}
			return checkQuerySize(c.query, maxSize)
		}
	}
	rest, err := readLimited(br, maxSize-len(head))
	if err != nil {
		return err
	}
	c.query = string(head) + string(rest)
	return checkQuerySize(c.query, maxSize)
}

// maxQuerySize returns the request's max_query_size (URL or session
// setting, default 262144), lowered to Config.MaxQuerySize; 0 means that
// ceiling.
func (c *call) maxQuerySize() (int, error) {
	v := c.setting("max_query_size")
	if v == "" {
		return min(defaultMaxQuerySize, c.s.cfg.MaxQuerySize), nil
	}
	n, err := parseUintSetting(v)
	if err != nil {
		return 0, err
	}
	if n == 0 || n > uint64(c.s.cfg.MaxQuerySize) {
		return c.s.cfg.MaxQuerySize, nil
	}
	return int(n), nil
}

// setsMaxQuerySize reports whether the query is a SET of max_query_size,
// failing as the per-request check would when the value does not parse,
// so that a session never stores it.
func (c *call) setsMaxQuerySize() (bool, error) {
	if c.keyword != "SET" {
		return false, nil
	}
	st, err := c.parseQuery()
	if err != nil {
		return false, nil // the engine reports it
	}
	set, ok := st.(*sqlparse.SetStmt)
	if !ok {
		return false, nil
	}
	sets := false
	for _, kv := range set.Settings {
		if kv.Name == "max_query_size" {
			if _, err := parseUintSetting(settingValueText(kv.Value)); err != nil {
				return false, err
			}
			sets = true
		}
	}
	return sets, nil
}

// parseUintSetting parses an unsigned integer setting value the way
// ClickHouse does (parseWithSizeSuffix): digits, optionally followed by a
// size suffix (K, M, G, T, or Ki, Mi, Gi, Ti for powers of 1024).
func parseUintSetting(v string) (uint64, error) {
	t := strings.TrimSpace(v)
	n := 0
	for n < len(t) && t[n] >= '0' && t[n] <= '9' {
		n++
	}
	if n == 0 {
		return 0, cherr.New(cherr.CannotParseNumber, "Cannot read integer value")
	}
	mult, ok := sizeSuffixes[t[n:]]
	if !ok {
		return 0, cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: '%s'", t[n:])
	}
	x, err := strconv.ParseUint(t[:n], 10, 64)
	if err != nil || x > math.MaxUint64/mult {
		return 0, cherr.New(cherr.CannotParseNumber, "Cannot read integer value: %s", t)
	}
	return x * mult, nil
}

// sizeSuffixes maps the size suffixes of integer settings to multipliers.
var sizeSuffixes = map[string]uint64{
	"": 1, "k": 1e3, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12,
	"ki": 1 << 10, "Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40,
}

// checkQuerySize fails when the query part of text is over maxSize bytes.
// Inline INSERT data (after VALUES or FORMAT <name>) is not counted, as in
// ClickHouse.
func checkQuerySize(text string, maxSize int) error {
	n := len(text)
	if n > maxSize && leadingKeyword(text[:min(n, 4096)]) == "INSERT" {
		if end, _, ok := insertDataSplit(text); ok {
			n = end
		}
	}
	if n > maxSize {
		return errQuerySize()
	}
	return nil
}

func errQuerySize() error {
	return cherr.New(cherr.SyntaxError, "Syntax error: Max query size exceeded (can be increased with the `max_query_size` setting)")
}

// trimInlineDataStart skips what ClickHouse skips between `FORMAT <name>`
// and inline data: spaces and tabs, then one optional (CR)LF.
func trimInlineDataStart(r io.Reader) io.Reader {
	br := bufio.NewReader(r)
	for {
		b, err := br.Peek(1)
		if err != nil || (b[0] != ' ' && b[0] != '\t' && b[0] != '\f') {
			break
		}
		_, _ = br.Discard(1)
	}
	if b, err := br.Peek(1); err == nil && b[0] == '\r' {
		_, _ = br.Discard(1)
	}
	if b, err := br.Peek(1); err == nil && b[0] == '\n' {
		_, _ = br.Discard(1)
	}
	return br
}

// readLimited reads r fully, failing when it is longer than limit bytes.
func readLimited(r io.Reader, limit int) ([]byte, error) {
	if limit < 0 {
		limit = 0
	}
	b, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > limit {
		return nil, errQuerySize()
	}
	return b, nil
}

func (c *call) openSession() error {
	if c.s.cfg.RejectSessions && c.usesSession() {
		return cherr.New(cherr.NotImplemented, rejectedSessionsMsg)
	}
	id, ok := c.params["session_id"]
	if !ok {
		return nil
	}
	if len(id) == 0 || id[0] == "" {
		return cherr.New(cherr.SessionIDEmpty, "Session id query parameter was provided, but it was empty")
	}
	toStr, hasTO := c.params["session_timeout"]
	to, err := c.s.sessions.parseSessionTimeout(first(toStr), hasTO)
	if err != nil {
		return err
	}
	sess, st, err := c.s.sessions.acquire(c.user, id[0], to, c.params.Get("session_check") == "1")
	if err != nil {
		return err
	}
	c.sess, c.sessIn = sess, st
	return nil
}

// rejectedSessionsMsg is the error of a session request when
// Config.RejectSessions is set.
const rejectedSessionsMsg = "sessions are not supported by this deployment"

// usesSession reports whether the request names any session parameter.
func (c *call) usesSession() bool {
	for _, p := range []string{"session_id", "session_check", "session_timeout"} {
		if _, ok := c.params[p]; ok {
			return true
		}
	}
	return false
}

// pending session changes, applied on release.
type sessionDelta struct {
	database *string
	settings map[string]string
	// reset are the settings SET to DEFAULT, dropped after settings are
	// applied: as in ClickHouse, DEFAULT wins within one SET.
	reset []string
}

// set records one SET assignment.
func (d *sessionDelta) set(kv sqlparse.Setting) {
	text, reset, ok := exec.SettingText(kv.Name, kv.Value)
	switch {
	case reset:
		d.reset = append(d.reset, kv.Name)
	case ok:
		d.settings[kv.Name] = text
	default:
		d.settings[kv.Name] = settingValueText(kv.Value)
	}
}

func (c *call) updateSession(res *exec.Result) {
	if c.sess == nil {
		return
	}
	d := sessionDelta{}
	if res.Database != "" {
		db := res.Database
		d.database = &db
	} else if c.keyword == "USE" {
		if st, err := c.parseQuery(); err == nil {
			if u, ok := st.(*sqlparse.UseStmt); ok && u.Database != "" {
				d.database = &u.Database
			}
		}
	}
	if c.keyword == "SET" {
		st, err := c.parseQuery()
		if err != nil {
			c.s.log.Debug("cannot record session settings", "query_id", c.queryID, "error", err)
		} else if set, ok := st.(*sqlparse.SetStmt); ok {
			d.settings = make(map[string]string, len(set.Settings))
			for _, kv := range set.Settings {
				d.set(kv)
			}
		}
	}
	c.sessDelta = d
}

// parseQuery parses the request's query for classification with the same
// inputs exec uses ({name:Identifier} params, union_default_mode).
func (c *call) parseQuery() (sqlparse.Statement, error) {
	return sqlparse.ParseRequest(c.query, c.queryParams(), c.settings)
}

func (c *call) releaseSession() {
	c.s.sessions.release(c.sess, c.sessDelta)
	c.sess = nil
}

// settingValueText renders a SET value the way settings are passed as text.
func settingValueText(e sqlparse.Expr) string {
	switch v := e.(type) {
	case *sqlparse.Literal:
		switch x := v.Value.(type) {
		case string:
			return x
		case bool:
			if x {
				return "1"
			}
			return "0"
		case nil:
			return ""
		}
		if v.Raw != "" {
			return v.Raw
		}
		return fmt.Sprint(v.Value)
	case *sqlparse.UnaryExpr:
		if v.Op == "-" {
			return "-" + settingValueText(v.X)
		}
	case *sqlparse.Ident:
		return strings.Join(v.Parts, ".")
	}
	return sqlparse.String(e)
}

func first(v []string) string {
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func isTrue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true":
		return true
	}
	return false
}
