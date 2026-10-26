package sqlparse

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

func mustParse(t *testing.T, sql string) Statement {
	t.Helper()
	st, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	return st
}

func mustExpr(t *testing.T, s string) Expr {
	t.Helper()
	e, err := ParseExpr(s)
	if err != nil {
		t.Fatalf("ParseExpr(%q): %v", s, err)
	}
	return e
}

func mustSelect(t *testing.T, sql string) *SelectStmt {
	t.Helper()
	st := mustParse(t, sql)
	s, ok := st.(*SelectStmt)
	if !ok {
		t.Fatalf("Parse(%q) = %T, want *SelectStmt", sql, st)
	}
	return s
}

// expectErr asserts Parse fails with the given ClickHouse error code and a
// message containing want.
func expectErr(t *testing.T, sql string, code int, want string) {
	t.Helper()
	_, err := Parse(sql)
	if err == nil {
		t.Fatalf("Parse(%q) succeeded, want error %d", sql, code)
	}
	ce, ok := err.(*cherr.Error)
	if !ok {
		t.Fatalf("Parse(%q) error %T is not *cherr.Error", sql, err)
	}
	if ce.Code != code {
		t.Fatalf("Parse(%q) code = %d (%s), want %d", sql, ce.Code, ce.Msg, code)
	}
	if !strings.Contains(ce.Msg, want) {
		t.Fatalf("Parse(%q) message %q does not contain %q", sql, ce.Msg, want)
	}
}

// AST constructors for compact expectations.
func id(parts ...string) *Ident { return &Ident{Parts: parts} }
func u(v uint64) *Literal       { return &Literal{Value: v, Raw: fmt.Sprint(v)} }
func str(s string) *Literal     { return &Literal{Value: s, Raw: quoteString(s)} }
func bin(op string, l, r Expr) *BinaryExpr {
	return &BinaryExpr{Op: op, Left: l, Right: r}
}
func fn(name string, args ...Expr) *FuncCall { return &FuncCall{Name: name, Args: args} }
func param(name, typ string) *Param          { return &Param{Name: name, Type: typ, RawType: typ} }

func assertAST(t *testing.T, what string, got, want any) {
	t.Helper()
	if !astEqual(got, want) {
		t.Errorf("%s:\n got:  %s\n want: %s", what, dump(got), dump(want))
	}
}

// dump renders AST nodes readably for failure messages.
func dump(v any) string {
	switch x := v.(type) {
	case Expr:
		return fmt.Sprintf("%s  [%s]", String(x), ColumnName(x))
	case []Expr:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = dump(e)
		}
		return "[" + strings.Join(parts, "; ") + "]"
	case *SelectStmt:
		return SelectString(x)
	}
	return fmt.Sprintf("%#v", v)
}
