package sqlparse

import (
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

func TestUnaryPlus(t *testing.T) {
	// ClickHouse drops a unary plus: +1 is the literal 1 (UInt8), but it
	// does not fold `+ -1` into a literal (that is negate(1)).
	if e := mustExpr(t, "+1"); !astEqual(e, &Literal{Value: uint64(1), Raw: "1"}) {
		t.Errorf("+1 = %#v", e)
	}
	if e := mustExpr(t, "+ -1"); ColumnName(e) != "negate(1)" {
		t.Errorf("+ -1 named %q", ColumnName(e))
	}
	if e := mustExpr(t, "1 - +x * 2"); ColumnName(e) != "minus(1, multiply(x, 2))" {
		t.Errorf("1 - +x * 2 named %q", ColumnName(e))
	}
}

func TestDoubleParenTupleLiteral(t *testing.T) {
	// ((1, 'a')) is tuple((1, 'a')) in ClickHouse: a parenthesized tuple
	// literal becomes a one-element tuple. Non-literal tuples and
	// tuple(...) calls are not wrapped.
	e := mustExpr(t, "((1, 'a'))")
	tup, ok := e.(*TupleExpr)
	if !ok || len(tup.Elems) != 1 {
		t.Fatalf("((1, 'a')) = %#v, want a one-element tuple", e)
	}
	if _, ok := tup.Elems[0].(*TupleExpr); !ok {
		t.Fatalf("((1, 'a')) element = %#v", tup.Elems[0])
	}
	for _, s := range []string{"((1, x))", "(((1, 'a')))", "((tuple(1, 2)))", "((1, [1]))"} {
		e := mustExpr(t, s)
		if tup, ok := e.(*TupleExpr); ok && len(tup.Elems) == 1 {
			if inner, ok := tup.Elems[0].(*TupleExpr); ok && len(inner.Elems) == 1 {
				t.Errorf("%s wrapped twice", s)
			}
			if s != "(((1, 'a')))" {
				t.Errorf("%s wrapped: %#v", s, e)
			}
		}
	}
	// The wrapped form survives a render/parse round trip.
	if got := String(mustExpr(t, String(e))); got != String(e) {
		t.Errorf("round trip %q -> %q", String(e), got)
	}
}

func TestParamRawType(t *testing.T) {
	// ClickHouse names a bound parameter _CAST(v, '<type as written>'): the
	// text after ':' (leading blanks skipped) up to '}'.
	for _, c := range []struct{ src, typ, raw string }{
		{"{x:UInt8}", "UInt8", "UInt8"},
		{"{x :  UInt8  }", "UInt8", "UInt8  "},
		{"{x:Array(Tuple(String,String))}", "Array(Tuple(String, String))", "Array(Tuple(String,String))"},
		{"{x:Nullable( UInt8 )}", "Nullable(UInt8)", "Nullable( UInt8 )"},
	} {
		p, ok := mustExpr(t, c.src).(*Param)
		if !ok {
			t.Fatalf("%s is not a Param", c.src)
		}
		if p.Type != c.typ || p.RawType != c.raw {
			t.Errorf("%s: Type %q RawType %q, want %q %q", c.src, p.Type, p.RawType, c.typ, c.raw)
		}
	}
}

func TestIdentifierParams(t *testing.T) {
	params := map[string]string{"tbl": "one", "db": "system", "col": "dummy", "a": "x", "odd": "we`ird"}
	parse := func(sql string) *SelectStmt {
		t.Helper()
		st, err := ParseWithParams(sql, params)
		if err != nil {
			t.Fatalf("ParseWithParams(%q): %v", sql, err)
		}
		return st.(*SelectStmt)
	}
	s := parse("SELECT count() FROM system.{tbl:Identifier}")
	if s.From.Database != "system" || s.From.Table != "one" {
		t.Errorf("FROM = %+v", s.From)
	}
	s = parse("SELECT {col:Identifier}, {db:Identifier}.{tbl:Identifier}.{col:Identifier} AS {a:Identifier} FROM {db:Identifier}.{tbl:Identifier} WHERE {col:Identifier} = 0")
	if s.From.Database != "system" || s.From.Table != "one" {
		t.Errorf("FROM = %+v", s.From)
	}
	if !astEqual(s.Columns[0].Expr, id("dummy")) || ColumnName(s.Columns[0].Expr) != "dummy" {
		t.Errorf("col 0 = %#v", s.Columns[0].Expr)
	}
	if !astEqual(s.Columns[1].Expr, id("system", "one", "dummy")) || s.Columns[1].Alias != "x" {
		t.Errorf("col 1 = %#v alias %q", s.Columns[1].Expr, s.Columns[1].Alias)
	}
	// The value is one identifier, even with a dot in it (ClickHouse:
	// UNKNOWN_TABLE `system.one`).
	st, err := ParseWithParams("SELECT * FROM {t:Identifier}", map[string]string{"t": "system.one"})
	if err != nil {
		t.Fatal(err)
	}
	if tr := st.(*SelectStmt).From; tr.Database != "" || tr.Table != "system.one" {
		t.Errorf("FROM = %+v", tr)
	}
	s = parse("SELECT {odd:Identifier} FROM t")
	if !astEqual(s.Columns[0].Expr, id("we`ird")) {
		t.Errorf("odd = %#v", s.Columns[0].Expr)
	}
	// INSERT ... FORMAT data is never substituted.
	ins, err := ParseWithParams("INSERT INTO {db:Identifier}.{tbl:Identifier} FORMAT TSV\n{tbl:Identifier}\n", params)
	if err != nil {
		t.Fatal(err)
	}
	if i := ins.(*InsertStmt); i.Database != "system" || i.Table != "one" || i.Data != "{tbl:Identifier}\n" {
		t.Errorf("insert = %+v", i)
	}
	// Non-Identifier parameters stay Param nodes.
	s = parse("SELECT {n:UInt8} FROM t")
	if _, ok := s.Columns[0].Expr.(*Param); !ok {
		t.Errorf("{n:UInt8} = %#v", s.Columns[0].Expr)
	}

	// A missing value is UNKNOWN_QUERY_PARAMETER, as in ClickHouse.
	_, err = ParseWithParams("SELECT count() FROM {nope:Identifier}", params)
	if ce, ok := err.(*cherr.Error); !ok || ce.Code != cherr.UnknownQueryParameter {
		t.Errorf("missing param: %v", err)
	}
	// Plain Parse cannot bind them: NOT_IMPLEMENTED, never a syntax error.
	expectErr(t, "SELECT count() FROM {tbl:Identifier}", cherr.NotImplemented, "Identifier")
	expectErr(t, "SELECT {c:Identifier} FROM t", cherr.NotImplemented, "Identifier")
}

func TestBareUnion(t *testing.T) {
	// ClickHouse 25.8 (union_default_mode = ''): Code 558.
	expectErr(t, "SELECT 1 UNION SELECT 2", 558, "Expected ALL or DISTINCT in SelectWithUnion query")
	expectErr(t, "SELECT * FROM (SELECT 1 UNION SELECT 2)", 558, "union_default_mode")
	expectErr(t, "SELECT 1 UNION SELECT 2 SETTINGS union_default_mode = 'all'", 557, "Unexpected value of SetOperationMode: 'all'")
	for _, c := range []struct{ sql, op string }{
		{"SELECT 1 UNION SELECT 2 SETTINGS union_default_mode = 'DISTINCT'", "DISTINCT"},
		{"SELECT 1 UNION SELECT 2 UNION ALL SELECT 3 SETTINGS union_default_mode = 'ALL'", "ALL"},
		{"SELECT * FROM (SELECT 1 UNION SELECT 2) SETTINGS union_default_mode = 'DISTINCT'", "DISTINCT"},
	} {
		s := mustSelect(t, c.sql)
		if s.From != nil && s.From.Subquery != nil {
			s = s.From.Subquery
		}
		if len(s.UnionOps) == 0 || s.UnionOps[0] != c.op {
			t.Errorf("%s: ops %v", c.sql, s.UnionOps)
		}
	}
}

func TestBareUnionRequestSettings(t *testing.T) {
	// ClickHouse 25.8 also takes union_default_mode from the HTTP/session
	// settings; the statement's own SETTINGS clause wins over them.
	ops := func(sql string, settings map[string]string) ([]string, error) {
		st, err := ParseRequest(sql, nil, settings)
		if err != nil {
			return nil, err
		}
		return st.(*SelectStmt).UnionOps, nil
	}
	if o, err := ops("SELECT 1 UNION SELECT 1", map[string]string{"union_default_mode": "DISTINCT"}); err != nil || o[0] != "DISTINCT" {
		t.Errorf("http DISTINCT: %v %v", o, err)
	}
	if o, err := ops("SELECT 1 UNION SELECT 1 SETTINGS union_default_mode = 'ALL'", map[string]string{"union_default_mode": "DISTINCT"}); err != nil || o[0] != "ALL" {
		t.Errorf("clause over http: %v %v", o, err)
	}
	for _, c := range []struct {
		mode string
		code int
	}{{"", 558}, {"all", 557}} {
		_, err := ops("SELECT 1 UNION SELECT 1", map[string]string{"union_default_mode": c.mode})
		if ce, ok := err.(*cherr.Error); !ok || ce.Code != c.code {
			t.Errorf("mode %q: %v", c.mode, err)
		}
	}
	if _, err := ops("SELECT 1 UNION SELECT 1", nil); err == nil {
		t.Error("no settings: want 558")
	}
	// Identifier parameters bind as in ParseWithParams.
	st, err := ParseRequest("SELECT count() FROM system.{t:Identifier}", map[string]string{"t": "one"}, nil)
	if err != nil || st.(*SelectStmt).From.Table != "one" {
		t.Errorf("identifier param: %#v %v", st, err)
	}
}
