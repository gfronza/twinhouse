package sqlparse

import (
	"math"
	"testing"

	"dynamodb-eventstream/internal/cherr"
)

func TestExprShapes(t *testing.T) {
	tests := []struct {
		in   string
		want Expr
	}{
		{"a", id("a")},
		{"db.t.col", id("db", "t", "col")},
		{"`a b`.\"c\"", id("a b", "c")},
		{"t.*", &Star{Table: "t"}},
		{"db.t.*", &Star{Table: "db.t"}},
		{"*", &Star{}},
		{"1", u(1)},
		{"-1", &Literal{Value: int64(-1), Raw: "-1"}},
		{"- 1", &Literal{Value: int64(-1), Raw: "-1"}},
		{"-0", &Literal{Value: uint64(0), Raw: "-0"}},
		{"-(1)", &UnaryExpr{Op: "-", X: u(1)}},
		{"- -1", &UnaryExpr{Op: "-", X: &Literal{Value: int64(-1), Raw: "-1"}}},
		{"-x", &UnaryExpr{Op: "-", X: id("x")}},
		{"1.50", &Literal{Value: 1.5, Raw: "1.50"}},
		{".5", &Literal{Value: 0.5, Raw: ".5"}},
		{"1.", &Literal{Value: 1.0, Raw: "1."}},
		{"1e3", &Literal{Value: 1000.0, Raw: "1e3"}},
		{"1E-2", &Literal{Value: 0.01, Raw: "1E-2"}},
		{"0x1F", &Literal{Value: uint64(31), Raw: "0x1F"}},
		{"0b101", &Literal{Value: uint64(5), Raw: "0b101"}},
		{"1_000", &Literal{Value: uint64(1000), Raw: "1_000"}},
		{"18446744073709551615", &Literal{Value: uint64(math.MaxUint64), Raw: "18446744073709551615"}},
		{"18446744073709551616", &Literal{Value: 18446744073709551616.0, Raw: "18446744073709551616"}},
		{"-9223372036854775808", &Literal{Value: int64(math.MinInt64), Raw: "-9223372036854775808"}},
		{"-9223372036854775809", &Literal{Value: -9223372036854775809.0, Raw: "-9223372036854775809"}},
		{"inf", &Literal{Value: math.Inf(1), Raw: "inf"}},
		{"-inf", &Literal{Value: math.Inf(-1), Raw: "-inf"}},
		{"NULL", &Literal{Value: nil, Raw: "NULL"}},
		{"null", &Literal{Value: nil, Raw: "null"}},
		{"TRUE", &Literal{Value: true, Raw: "TRUE"}},
		{"false", &Literal{Value: false, Raw: "false"}},
		{"'it''s'", &Literal{Value: "it's", Raw: "'it''s'"}},
		{`'a\'b\\c\n\t\0\x41'`, &Literal{Value: "a'b\\c\n\t\x00A", Raw: `'a\'b\\c\n\t\0\x41'`}},
		{`'\z\%'`, &Literal{Value: `\z\%`, Raw: `'\z\%'`}},
		{"{s:String}", param("s", "String")},
		{"{ keys : Array( Tuple( String,String ,String ) ) }", &Param{Name: "keys", Type: "Array(Tuple(String, String, String))", RawType: "Array( Tuple( String,String ,String ) ) "}},
		{"{d:DateTime64(9, 'UTC')}", param("d", "DateTime64(9, 'UTC')")},
		{"(a)", id("a")},
		{"(a, b)", &TupleExpr{Elems: []Expr{id("a"), id("b")}}},
		{"(a,)", &TupleExpr{Elems: []Expr{id("a")}}},
		{"()", &TupleExpr{Elems: []Expr{}}},
		{"tuple()", &TupleExpr{Elems: []Expr{}, Func: true}},
		{"tuple(1, 2)", &TupleExpr{Elems: []Expr{u(1), u(2)}, Func: true}},
		{"[]", &ArrayExpr{Elems: []Expr{}}},
		{"[1, 'a']", &ArrayExpr{Elems: []Expr{u(1), str("a")}}},
		{"array(1)", &ArrayExpr{Elems: []Expr{u(1)}, Func: true}},
		{"x[1]", &IndexExpr{X: id("x"), Index: u(1)}},
		{"m['k']", &IndexExpr{X: id("m"), Index: str("k")}},
		{"t.1", &TupleAccessExpr{X: id("t"), Index: 1}},
		{"t.1.2", &TupleAccessExpr{X: &TupleAccessExpr{X: id("t"), Index: 1}, Index: 2}},
		{"f(x).name", &TupleAccessExpr{X: fn("f", id("x")), Name: "name"}},
		{"count()", fn("count")},
		{"count(*)", fn("count")},
		{"COUNT(*)", fn("COUNT")},
		{"count(DISTINCT x)", &FuncCall{Name: "count", Args: []Expr{id("x")}, Distinct: true}},
		{"quantile(0.9)(x)", &FuncCall{Name: "quantile", Params: []Expr{&Literal{Value: 0.9, Raw: "0.9"}}, Args: []Expr{id("x")}}},
		{"groupArray(10)()", &FuncCall{Name: "groupArray", Params: []Expr{u(10)}}},
		{"f(x AS y)", fn("f", &AliasExpr{X: id("x"), Alias: "y"})},
		{"CAST(x AS String)", &CastExpr{X: id("x"), Type: "String"}},
		{"cast(x as LowCardinality( Nullable(String) ))", &CastExpr{X: id("x"), Type: "LowCardinality(Nullable(String))"}},
		{"CAST(x, 'DateTime64(9, \\'UTC\\')')", &CastExpr{X: id("x"), Type: "DateTime64(9, 'UTC')"}},
		{"CAST(x, t)", fn("CAST", id("x"), id("t"))},
		{"x::Array(String)", &CastExpr{X: id("x"), Type: "Array(String)"}},
		{"1::UInt8", &CastExpr{X: str("1"), Type: "UInt8"}},
		{"-1::String", &CastExpr{X: str("-1"), Type: "String"}},
		{"[1,2]::Array(UInt8)", &CastExpr{X: str("[1,2]"), Type: "Array(UInt8)"}},
		{"'a'::String", &CastExpr{X: str("a"), Type: "String"}},
		{"(1)::UInt8", &CastExpr{X: str("(1)"), Type: "UInt8"}},
		{"- 1::String", &CastExpr{X: str("- 1"), Type: "String"}},
		{"[ 1 , 2 ]::Array(UInt8)", &CastExpr{X: str("[ 1 , 2 ]"), Type: "Array(UInt8)"}},
		{"(1, 'a')::Tuple(UInt8, String)", &CastExpr{X: str("(1, 'a')"), Type: "Tuple(UInt8, String)"}},
		{"true::String", &CastExpr{X: &Literal{Value: true, Raw: "true"}, Type: "String"}},
		{"inf::Float64", &CastExpr{X: &Literal{Value: math.Inf(1), Raw: "inf"}, Type: "Float64"}},
		{"NULL::Nullable(UInt8)", &CastExpr{X: &Literal{Value: nil, Raw: "NULL"}, Type: "Nullable(UInt8)"}},
		{"[x]::Array(UInt32)", &CastExpr{X: &ArrayExpr{Elems: []Expr{id("x")}}, Type: "Array(UInt32)"}},
		{"INTERVAL 1 DAY", &IntervalExpr{X: u(1), Unit: "DAY"}},
		{"INTERVAL 2 hours", &IntervalExpr{X: u(2), Unit: "HOUR"}},
		{"INTERVAL '3' MINUTE", &IntervalExpr{X: str("3"), Unit: "MINUTE"}},
		{"INTERVAL '3 seconds'", &IntervalExpr{X: &Literal{Value: uint64(3), Raw: "3"}, Unit: "SECOND"}},
		{"INTERVAL x + 1 MONTH", &IntervalExpr{X: bin("+", id("x"), u(1)), Unit: "MONTH"}},
		{"interval", id("interval")},
		{"date", id("date")},
		{"x -> x + 1", &LambdaExpr{Params: []string{"x"}, Body: bin("+", id("x"), u(1))}},
		{"(x, y) -> x", &LambdaExpr{Params: []string{"x", "y"}, Body: id("x")}},
		{"CASE WHEN a THEN 1 ELSE 2 END", &CaseExpr{Whens: []WhenClause{{Cond: id("a"), Result: u(1)}}, Else: u(2)}},
		{"CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END", &CaseExpr{Operand: id("x"), Whens: []WhenClause{{u(1), str("a")}, {u(2), str("b")}}}},
		{"x BETWEEN 1 AND 2", &BetweenExpr{X: id("x"), Low: u(1), High: u(2)}},
		{"x NOT BETWEEN 1 AND 2", &BetweenExpr{X: id("x"), Low: u(1), High: u(2), Not: true}},
		{"x IS NULL", &IsNullExpr{X: id("x")}},
		{"x IS NOT NULL", &IsNullExpr{X: id("x"), Not: true}},
		{"x == 1", bin("=", id("x"), u(1))},
		{"x <> 1", bin("!=", id("x"), u(1))},
		{"x MOD 2", bin("%", id("x"), u(2))},
		{"x DIV 2", fn("intDiv", id("x"), u(2))},
		{"s REGEXP 'a'", fn("match", id("s"), str("a"))},
		{"c ? 1 : 2", fn("if", id("c"), u(1), u(2))},
		{"x NOT IN (1, 2)", bin("NOT IN", id("x"), &TupleExpr{Elems: []Expr{u(1), u(2)}})},
		{"x GLOBAL NOT IN t", bin("GLOBAL NOT IN", id("x"), id("t"))},
		{"x IN {p:Array(String)}", bin("IN", id("x"), param("p", "Array(String)"))},
		{"(a, b) IN {keys:Array(Tuple(String,String))}", bin("IN", &TupleExpr{Elems: []Expr{id("a"), id("b")}}, &Param{Name: "keys", Type: "Array(Tuple(String, String))", RawType: "Array(Tuple(String,String))"})},
		{"s NOT ILIKE 'x%'", bin("NOT ILIKE", id("s"), str("x%"))},
		{"a || b", bin("||", id("a"), id("b"))},
		{"(a AND b)", &BinaryExpr{Op: "AND", Left: id("a"), Right: id("b"), Parens: true}},
		{"(a + b)", bin("+", id("a"), id("b"))},
		{"NOT a", &UnaryExpr{Op: "NOT", X: id("a")}},
		{"not(a)", &UnaryExpr{Op: "NOT", X: id("a")}},
		{"NOT (a) = 1", bin("=", &UnaryExpr{Op: "NOT", X: id("a")}, u(1))},
		{"x IN (SELECT 1)", bin("IN", id("x"), &SubqueryExpr{Select: &SelectStmt{Columns: []SelectItem{{Expr: u(1)}}}})},
		{"exists(SELECT 1)", fn("exists", &SubqueryExpr{Select: &SelectStmt{Columns: []SelectItem{{Expr: u(1)}}}})},
		{"DATE '2024-01-02'", fn("toDate", str("2024-01-02"))},
		{"TIMESTAMP '2024-01-02 03:04:05'", fn("toDateTime", str("2024-01-02 03:04:05"))},
		{"EXTRACT(DAY FROM ts)", fn("toDayOfMonth", id("ts"))},
		{"POSITION('a' IN s)", fn("position", id("s"), str("a"))},
		{"position(s, 'a')", fn("position", id("s"), str("a"))},
		{"substring(s FROM 2 FOR 3)", fn("substring", id("s"), u(2), u(3))},
		{"substring(s, 2)", fn("substring", id("s"), u(2))},
		{"trim(s)", fn("trimBoth", id("s"))},
		{"TRIM(LEADING 'x' FROM s)", fn("trimLeft", id("s"), str("x"))},
		{"trim(TRAILING 'y' FROM s)", fn("trimRight", id("s"), str("y"))},
		{"trim(' ' FROM s)", fn("trimBoth", id("s"), str(" "))},
		{"ltrim(s)", fn("trimLeft", id("s"))},
		{"rtrim(s)", fn("trimRight", id("s"))},
		{"dateDiff(second, a, b)", fn("dateDiff", &Literal{Value: "second", Raw: "'second'"}, id("a"), id("b"))},
		{"DATE_DIFF('day', a, b)", fn("dateDiff", str("day"), id("a"), id("b"))},
		{"date_add(HOUR, 2, ts)", bin("+", id("ts"), &IntervalExpr{X: u(2), Unit: "HOUR"})},
		{"timestamp_sub(ts, INTERVAL 1 DAY)", bin("-", id("ts"), &IntervalExpr{X: u(1), Unit: "DAY"})},
		{"sum(x) OVER (PARTITION BY a ORDER BY b DESC)", &FuncCall{Name: "sum", Args: []Expr{id("x")},
			Over: &WindowSpec{PartitionBy: []Expr{id("a")}, OrderBy: []OrderItem{{Expr: id("b"), Desc: true}}}}},
		{"x AS y", &AliasExpr{X: id("x"), Alias: "y"}},
	}
	for _, tt := range tests {
		assertAST(t, tt.in, mustExpr(t, tt.in), tt.want)
	}
}

// TestPrecedence checks operator binding against ClickHouse (names verified
// with clickhouse-server 25.8, see testdata/colnames.tsv for more).
func TestPrecedence(t *testing.T) {
	tests := []struct{ in, name string }{
		{"a OR b AND c", "or(a, and(b, c))"},
		{"a AND b OR c", "or(and(a, b), c)"},
		{"NOT a AND b", "and(not(a), b)"},
		{"NOT a OR b", "or(not(a), b)"},
		{"NOT a = 1", "not(equals(a, 1))"},
		{"NOT x IN (1)", "not(in(x, 1))"},
		{"NOT n IS NULL", "not(isNull(n))"},
		{"NOT x[1]", "not(arrayElement(x, 1))"},
		{"a = b IS NULL", "isNull(equals(a, b))"},
		{"a IS NULL AND b", "and(isNull(a), b)"},
		{"a BETWEEN 1 AND 2 = 1", "and(greaterOrEquals(a, 1), lessOrEquals(a, equals(2, 1)))"},
		{"x = a BETWEEN 1 AND 2", "and(greaterOrEquals(equals(x, a), 1), lessOrEquals(equals(x, a), 2))"},
		{"a AND b BETWEEN 1 AND 2", "and(a, and(greaterOrEquals(b, 1), lessOrEquals(b, 2)))"},
		{"x BETWEEN 1 AND 2 AND a", "and(and(greaterOrEquals(x, 1), lessOrEquals(x, 2)), a)"},
		{"s LIKE s = c", "equals(like(s, s), c)"},
		{"x IN (1) = 1", "equals(in(x, 1), 1)"},
		{"a + 1 IN (1)", "in(plus(a, 1), 1)"},
		{"a < b < c", "less(less(a, b), c)"},
		{"s || s = s", "equals(concat(s, s), s)"},
		{"a + b || s", "concat(plus(a, b), s)"},
		{"a + b * c", "plus(a, multiply(b, c))"},
		{"a - b - c", "minus(minus(a, b), c)"},
		{"a / b % c", "modulo(divide(a, b), c)"},
		{"-a * 2", "multiply(negate(a), 2)"},
		{"-a - -b", "minus(negate(a), negate(b))"},
		{"-x::String", "negate(CAST(x, 'String'))"},
		{"-arr[1]", "negate(arrayElement(arr, 1))"},
		{"1 + 2::String", "plus(1, CAST('2', 'String'))"},
		{"a::Int8 + 1", "plus(CAST(a, 'Int8'), 1)"},
		{"arr[1][2]", "arrayElement(arrayElement(arr, 1), 2)"},
		{"tup.1 + 1", "plus(tupleElement(tup, 1), 1)"},
		{"INTERVAL 1 DAY * 2", "multiply(toIntervalDay(1), 2)"},
		{"ts - INTERVAL 1 DAY", "minus(ts, toIntervalDay(1))"},
		{"a AND b ? 1 : 2", "if(and(a, b), 1, 2)"},
		{"a OR b ? 1 : 2", "if(or(a, b), 1, 2)"},
		{"a ILIKE 'x' AND NOT s LIKE 'y'", "and(ilike(a, 'x'), not(like(s, 'y')))"},
		{"x -> x AND y", "lambda(tuple(x), and(x, y))"},
		{"a = 1 AND b IN (SELECT 1) OR c", "or(and(equals(a, 1), in(b, _subquery_1)), c)"},
		{"(x IN (SELECT 1)) + (SELECT 2)", "plus(in(x, _subquery_1), _subquery_2)"},
	}
	for _, tt := range tests {
		if got := ColumnName(mustExpr(t, tt.in)); got != tt.name {
			t.Errorf("%s: got %s, want %s", tt.in, got, tt.name)
		}
	}
}

func TestColumnNameExtras(t *testing.T) {
	tests := []struct {
		e    Expr
		name string
	}{
		{&Literal{Value: int64(-5)}, "-5"},
		{&Literal{Value: 2.0}, "2."},
		{&Literal{Value: 1e-7}, "1e-7"},
		{&Literal{Value: 1.2345678901234568e29}, "1.2345678901234568e29"},
		{&Literal{Value: 123456789012345680000.0}, "123456789012345680000."},
		{&Literal{Value: math.Copysign(0, -1)}, "-0."},
		{&Literal{Value: math.NaN()}, "nan"},
		{&Literal{Value: "a\x01\a'\\"}, "'a\x01\a\\'\\\\'"},
		{param("p", "String"), "{p:String}"},
		{&Star{Table: "t"}, "t.*"},
		{fn("tuple", id("a")), "(a)"},
		{fn("array", u(1)), "[1]"},
		{fn("cast", id("x"), str("String")), "CAST(x, 'String')"},
		{&FuncCall{Name: "countIf", Args: []Expr{id("x"), id("c")}, Distinct: true}, "countIfDistinct(x, c)"},
		{&FuncCall{Name: "quantile", Params: []Expr{&Literal{Value: 0.5}}, Args: []Expr{id("x")}, Distinct: true}, "quantileDistinct(0.5)(x)"},
		{&FuncCall{Name: "row_number", Over: &WindowSpec{PartitionBy: []Expr{id("a")}, OrderBy: []OrderItem{{Expr: id("b")}}}}, "row_number() OVER (PARTITION BY a ORDER BY b ASC)"},
		{&FuncCall{Name: "sum", Args: []Expr{id("x")}, Over: &WindowSpec{}}, "sum(x) OVER ()"},
		{&TupleAccessExpr{X: id("t"), Name: "a"}, "tupleElement(t, 'a')"},
		{&BinaryExpr{Op: "AND", Left: &BinaryExpr{Op: "AND", Left: id("a"), Right: id("b"), Parens: true}, Right: id("c")}, "and(and(a, b), c)"},
		{bin("AND", bin("AND", id("a"), id("b")), id("c")), "and(a, b, c)"},
		{nil, "NULL"},
	}
	for _, tt := range tests {
		if got := ColumnName(tt.e); got != tt.name {
			t.Errorf("ColumnName(%#v) = %q, want %q", tt.e, got, tt.name)
		}
	}
}

func TestColumnNameFuncHook(t *testing.T) {
	e := mustExpr(t, "plus({n:UInt8}, t.tup.a)")
	got := ColumnNameFunc(e, func(x Expr) (string, bool) {
		switch v := x.(type) {
		case *Param:
			return "_CAST(3, 'UInt8')", true
		case *Ident:
			if len(v.Parts) == 3 {
				return "tup.a", true
			}
		}
		return "", false
	})
	if want := "plus(_CAST(3, 'UInt8'), tup.a)"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestParseExprErrors(t *testing.T) {
	tests := []struct {
		in   string
		code int
	}{
		{"", cherr.SyntaxError},
		{"1 +", cherr.SyntaxError},
		{"(1", cherr.SyntaxError},
		{"f(1", cherr.SyntaxError},
		{"[1, 2", cherr.SyntaxError},
		{"'abc", cherr.SyntaxError},
		{"`abc", cherr.SyntaxError},
		{"'\\x4'", cherr.SyntaxError},
		{"1a", cherr.SyntaxError},
		{"a b", cherr.SyntaxError},
		{"x BETWEEN 1", cherr.SyntaxError},
		{"CASE END", cherr.SyntaxError},
		{"CASE WHEN a END", cherr.SyntaxError},
		{"INTERVAL 1 FORTNIGHT", cherr.SyntaxError},
		{"INTERVAL 'soon'", cherr.SyntaxError},
		{"{p}", cherr.SyntaxError},
		{"{p:}", cherr.SyntaxError},
		{"{p:String", cherr.SyntaxError},
		{"c ? 1 : c ? 2 : 3", cherr.SyntaxError},
		{"t.0", cherr.SyntaxError},
		{"db.f(x)", cherr.SyntaxError},
		{"EXTRACT(WEEK FROM ts)", cherr.SyntaxError},
		{"trim(LEADING FROM s)", cherr.SyntaxError},
		{"trim(BOTH s)", cherr.SyntaxError},
		{"/* unterminated", cherr.SyntaxError},
		{"a IS NOT DISTINCT FROM b", cherr.NotImplemented},
		{"a <=> b", cherr.NotImplemented},
		{"* EXCEPT (a)", cherr.NotImplemented},
		{"COLUMNS('a.*')", cherr.NotImplemented},
		{"x = ANY (SELECT 1)", cherr.NotImplemented},
		{"sum(x) OVER w", cherr.NotImplemented},
		{"sumIf(x, c) FILTER (WHERE a)", cherr.NotImplemented},
	}
	for _, tt := range tests {
		_, err := ParseExpr(tt.in)
		if !cherr.Is(err, tt.code) {
			t.Errorf("ParseExpr(%q) error = %v, want code %d", tt.in, err, tt.code)
		}
	}
}

func TestFormatFloat(t *testing.T) {
	tests := map[float64]string{
		0: "0.", 1: "1.", 1.5: "1.5", 1000: "1000.", 0.1: "0.1", 1e-5: "0.00001", 1e-6: "0.000001",
		1e-7: "1e-7", 1.5e-7: "1.5e-7", 1e20: "100000000000000000000.", 1e21: "1e21", 1e308: "1e308",
		-1500: "-1500.", 0.30000000000000004: "0.30000000000000004", 123456789.123: "123456789.123",
		18446744073709551616: "18446744073709552000.", math.Inf(1): "inf", math.Inf(-1): "-inf",
	}
	for f, want := range tests {
		if got := formatFloat(f); got != want {
			t.Errorf("formatFloat(%v) = %q, want %q", f, got, want)
		}
	}
}
