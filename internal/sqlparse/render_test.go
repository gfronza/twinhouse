package sqlparse

import (
	"math"
	"strings"
	"testing"
)

// Engine-built nodes carry no Raw text; String must still produce SQL that
// parses back to the same value.
func TestStringOfConstructedNodes(t *testing.T) {
	neg := &Literal{Value: int64(-5)}
	tests := []struct {
		e    Expr
		want string
	}{
		{neg, "-5"},
		{&Literal{Value: uint64(7)}, "7"},
		{&Literal{Value: 7}, "7"},
		{&Literal{Value: 2.5}, "2.5"},
		{&Literal{Value: float32(0.5)}, "0.5"},
		{&Literal{Value: 1e21}, "1e21"},
		{&Literal{Value: math.Inf(-1)}, "-inf"},
		{&Literal{Value: math.Copysign(0, -1)}, "-0."},
		{&Literal{Value: "it's"}, `'it\'s'`},
		{&Literal{Value: true}, "true"},
		{&Literal{Value: nil}, "NULL"},
		{&UnaryExpr{Op: "-", X: neg}, "-(-5)"},
		{&UnaryExpr{Op: "-", X: &Literal{Value: uint64(5)}}, "-(5)"},
		{&UnaryExpr{Op: "-", X: &UnaryExpr{Op: "-", X: id("x")}}, "-(-x)"},
		{bin("-", id("a"), neg), "a - -5"},
		{&IndexExpr{X: neg, Index: u(1)}, "(-5)[1]"},
		{&TupleAccessExpr{X: &Literal{Value: uint64(1)}, Index: 1}, "(1).1"},
		{bin("*", bin("+", id("a"), id("b")), id("c")), "(a + b) * c"},
		{bin("+", id("a"), bin("+", id("b"), id("c"))), "a + (b + c)"},
		{bin("AND", id("a"), bin("OR", id("b"), id("c"))), "a AND (b OR c)"},
		{&IsNullExpr{X: bin("AND", id("a"), id("b"))}, "(a AND b) IS NULL"},
		{&BetweenExpr{X: id("x"), Low: bin("AND", id("a"), id("b")), High: u(1)}, "x BETWEEN (a AND b) AND 1"},
		{&IntervalExpr{X: bin("=", id("a"), u(1)), Unit: "DAY"}, "INTERVAL (a = 1) DAY"},
		{&LambdaExpr{Params: []string{"x"}, Body: id("x")}, "x -> x"},
		{bin("=", &LambdaExpr{Params: []string{"x"}, Body: id("x")}, u(1)), "(x -> x) = 1"},
		{id("select", "from"), "`select`.`from`"},
		{id("a`b"), "`a\\`b`"},
		{&Star{Table: "db.t"}, "db.t.*"},
		{&FuncCall{Name: "my func", Args: []Expr{u(1)}}, "`my func`(1)"},
		{&CastExpr{X: id("x"), Type: "DateTime64(9, 'UTC')"}, `CAST(x, 'DateTime64(9, \'UTC\')')`},
		{&AliasExpr{X: id("x"), Alias: "y"}, "(x AS y)"},
		{nil, "NULL"},
	}
	for _, tt := range tests {
		got := String(tt.e)
		if got != tt.want {
			t.Errorf("String(%#v) = %q, want %q", tt.e, got, tt.want)
		}
		if tt.e == nil {
			continue
		}
		back, err := ParseExpr(got)
		if err != nil {
			t.Errorf("ParseExpr(%q): %v", got, err)
			continue
		}
		if ColumnName(back) != ColumnName(tt.e) {
			t.Errorf("%q: name %q, want %q", got, ColumnName(back), ColumnName(tt.e))
		}
	}
}

func TestSelectStringShapes(t *testing.T) {
	s := mustSelect(t, "select a from t where b settings x = 1 format JSON")
	if got, want := SelectString(s), "SELECT a FROM t WHERE b SETTINGS x = 1 FORMAT JSON"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	s = mustSelect(t, "SELECT 1 UNION ALL (SELECT 2 SETTINGS y = 2) UNION ALL SELECT 3 SETTINGS x = 1")
	got := SelectString(s)
	if want := "SELECT 1 UNION ALL (SELECT 2 SETTINGS y = 2) UNION ALL SELECT 3 SETTINGS x = 1"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	assertAST(t, "reparse", mustParse(t, got), s)
	if !strings.Contains(SelectString(mustSelect(t, "SELECT * FROM a ANY LEFT JOIN b USING (k)")), "ANY LEFT JOIN b USING (k)") {
		t.Error("join rendering")
	}
}

func BenchmarkParseInsertValues(b *testing.B) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO db.e (a, b, c, d) VALUES ")
	for i := 0; i < 10000; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("('tenant-1', 12345, -1.5, ['x', 'y'])")
	}
	sql := sb.String()
	b.SetBytes(int64(len(sql)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Parse(sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseSelect(b *testing.B) {
	sql := roundTripSelects[1]
	for i := 0; i < b.N; i++ {
		if _, err := Parse(sql); err != nil {
			b.Fatal(err)
		}
	}
}
