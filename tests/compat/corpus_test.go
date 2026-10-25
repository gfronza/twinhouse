//go:build compat

package compat

// Scenario corpus parser.
//
// A scenario file (testdata/*.sql) is a sequence of statements, each
// terminated by a line that ends with ';' (the ';' is stripped). Comment
// lines immediately preceding a statement may carry directives:
//
//	-- query[: name]            compare the statement's result on both servers
//	-- error[: code]            the statement must fail on both servers with the
//	                            same ClickHouse error code (and HTTP status);
//	                            with a code, the reference must return it too
//	-- not-implemented[: why]   documented deviation: our server must answer
//	                            NOT_IMPLEMENTED (48); the reference is not checked
//	-- unsupported: why         outside the DESIGN §3 contract: our server must
//	                            answer UNKNOWN_FUNCTION (46) or NOT_IMPLEMENTED
//	                            (48) — never a wrong result
//	-- known-deviation: why     documented deliberate deviation: a diff is logged,
//	                            not failed (only for DESIGN-documented deviations)
//	-- formats: F1,F2           output formats to compare (default: see
//	                            defaultFormats); "Native" = clickhouse-go decoding
//	-- unordered                compare results as multisets of rows
//	-- param: name=value        query parameter (param_name URL param /
//	                            clickhouse.WithParameters)
//	-- setting: name=value      setting (URL param / clickhouse.WithSettings)
//	-- gzip                     (exec) send the body gzip-compressed
//	-- split-data               (exec) send the first line as ?query= and the
//	                            rest of the statement as the request body
//	-- reference-only           (exec) run only on the reference server
//
// Statements without query/error/not-implemented are "exec" statements: they
// run on both servers and must succeed on both (setup DDL, INSERTs ...).
// Other comment lines are ignored. The token $DB (also in param values) is replaced by the
// scenario's unique database name (the same name on both servers, so outputs
// that mention it stay comparable); statements also run with that database
// selected (database= URL param / Auth.Database).

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type stmtKind int

const (
	kindExec stmtKind = iota
	kindQuery
	kindError
	kindNotImpl
	kindUnsupported
)

type kv struct{ k, v string }

type stmt struct {
	file      string
	line      int
	sql       string
	kind      stmtKind
	name      string
	errCode   int // expected reference error code (kindError), 0 = any
	formats   []string
	unordered bool
	params    []kv
	settings  []kv
	deviation string
	gzip      bool
	split     bool
	refOnly   bool
}

var defaultFormats = []string{"TSVWithNamesAndTypes", "JSONCompact", "Native"}

var directiveRe = regexp.MustCompile(`^--\s*(query|error|not-implemented|unsupported|known-deviation|formats|unordered|param|setting|gzip|split-data|reference-only)\s*(?::\s*(.*))?$`)

func parseCorpus(path string) ([]*stmt, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var (
		out     []*stmt
		cur     = &stmt{file: path}
		buf     []string
		lineNo  int
		started bool
	)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		if !started {
			if trimmed == "" {
				continue
			}
			if strings.HasPrefix(trimmed, "--") {
				m := directiveRe.FindStringSubmatch(trimmed)
				if m == nil {
					continue // plain comment
				}
				if err := applyDirective(cur, m[1], strings.TrimSpace(m[2])); err != nil {
					return nil, fmt.Errorf("%s:%d: %v", path, lineNo, err)
				}
				continue
			}
			started = true
			cur.line = lineNo
		}
		buf = append(buf, line)
		if strings.HasSuffix(strings.TrimRight(line, " \t\r"), ";") {
			sql := strings.Join(buf, "\n")
			sql = strings.TrimRight(sql, " \t\r\n")
			sql = strings.TrimSuffix(sql, ";")
			cur.sql = sql
			if cur.name == "" {
				cur.name = autoName(sql)
			}
			if cur.unordered {
				for _, fm := range cur.formats {
					if fm == "Values" || fm == "Pretty" || strings.HasPrefix(fm, "Pretty") {
						return nil, fmt.Errorf("%s:%d: unordered not supported with %s", path, cur.line, fm)
					}
				}
			}
			out = append(out, cur)
			cur = &stmt{file: path}
			buf = nil
			started = false
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if started {
		return nil, fmt.Errorf("%s:%d: unterminated statement (missing ';')", path, cur.line)
	}
	return out, nil
}

func applyDirective(s *stmt, name, arg string) error {
	switch name {
	case "query":
		s.kind = kindQuery
		s.name = arg
	case "error":
		s.kind = kindError
		if arg != "" {
			fields := strings.Fields(arg)
			n, err := strconv.Atoi(fields[0])
			if err != nil {
				return fmt.Errorf("bad error code %q", arg)
			}
			s.errCode = n
		}
	case "not-implemented":
		s.kind = kindNotImpl
		s.deviation = arg
	case "unsupported":
		s.kind = kindUnsupported
		s.deviation = arg
	case "known-deviation":
		if arg == "" {
			return fmt.Errorf("known-deviation needs a reason")
		}
		s.deviation = arg
	case "formats":
		s.formats = nil
		for _, f := range strings.Split(arg, ",") {
			if f = strings.TrimSpace(f); f != "" {
				s.formats = append(s.formats, f)
			}
		}
	case "unordered":
		s.unordered = true
	case "param", "setting":
		k, v, ok := strings.Cut(arg, "=")
		if !ok {
			return fmt.Errorf("%s needs name=value", name)
		}
		p := kv{strings.TrimSpace(k), v}
		if name == "param" {
			s.params = append(s.params, p)
		} else {
			s.settings = append(s.settings, p)
		}
	case "gzip":
		s.gzip = true
	case "split-data":
		s.split = true
	case "reference-only":
		s.refOnly = true
	}
	return nil
}

var nonName = regexp.MustCompile(`[^A-Za-z0-9_]+`)

func autoName(sql string) string {
	s := strings.Join(strings.Fields(sql), " ")
	s = strings.ReplaceAll(s, "$DB.", "")
	s = nonName.ReplaceAllString(s, "_")
	s = strings.Trim(s, "_")
	if len(s) > 60 {
		s = s[:60]
	}
	return s
}

func (s *stmt) formatsOrDefault() []string {
	if len(s.formats) > 0 {
		return s.formats
	}
	return defaultFormats
}

// explicitFormatRe detects a statement that already carries a FORMAT clause.
var explicitFormatRe = regexp.MustCompile(`(?is)\bFORMAT\s+([A-Za-z]+)\s*$`)
