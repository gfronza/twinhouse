package sqlparse

import (
	"strings"
	"testing"
)

func lexAll(src string) []token {
	l := lexer{src: src}
	var out []token
	for {
		t := l.next()
		out = append(out, t)
		if t.kind == tEOF || t.kind == tError {
			return out
		}
	}
}

func TestLexer(t *testing.T) {
	type tk struct {
		kind tokKind
		text string
	}
	tests := []struct {
		src  string
		want []tk
	}{
		{"SELECT a,b", []tk{{tIdent, "SELECT"}, {tIdent, "a"}, {tOp, ","}, {tIdent, "b"}}},
		{"x::String", []tk{{tIdent, "x"}, {tOp, "::"}, {tIdent, "String"}}},
		{"a<=b>=c<>d!=e==f<=>g||h->i", []tk{{tIdent, "a"}, {tOp, "<="}, {tIdent, "b"}, {tOp, ">="}, {tIdent, "c"},
			{tOp, "<>"}, {tIdent, "d"}, {tOp, "!="}, {tIdent, "e"}, {tOp, "=="}, {tIdent, "f"}, {tOp, "<=>"},
			{tIdent, "g"}, {tOp, "||"}, {tIdent, "h"}, {tOp, "->"}, {tIdent, "i"}}},
		{"t.1", []tk{{tIdent, "t"}, {tOp, "."}, {tNumber, "1"}}},
		{"(x).1", []tk{{tOp, "("}, {tIdent, "x"}, {tOp, ")"}, {tOp, "."}, {tNumber, "1"}}},
		{"> .5", []tk{{tOp, ">"}, {tNumber, ".5"}}},
		{"SELECT .5", []tk{{tIdent, "SELECT"}, {tNumber, ".5"}}},
		{"1.5e-3 0x1F 0b10 1_000 1. 2E+2", []tk{{tNumber, "1.5e-3"}, {tNumber, "0x1F"}, {tNumber, "0b10"},
			{tNumber, "1_000"}, {tNumber, "1."}, {tNumber, "2E+2"}}},
		{"1e", []tk{{tError, "1e"}}},
		{"a -- comment\n b", []tk{{tIdent, "a"}, {tIdent, "b"}}},
		{"a # comment\n b #!shebang", []tk{{tIdent, "a"}, {tIdent, "b"}}},
		{"a /* x /* nested */ y */ b", []tk{{tIdent, "a"}, {tIdent, "b"}}},
		{"a /* open", []tk{{tIdent, "a"}, {tError, "/* open"}}},
		{"a-1", []tk{{tIdent, "a"}, {tOp, "-"}, {tNumber, "1"}}},
		{"'a''b' `c``d` \"e\\\"f\"", []tk{{tString, "'a''b'"}, {tQIdent, "`c``d`"}, {tQIdent, `"e\"f"`}}},
		{"'-- not a comment'", []tk{{tString, "'-- not a comment'"}}},
		{"ñame", []tk{{tError, "ñ"}}}, // ClickHouse: bare identifiers are ASCII only
		{"a$b", []tk{{tIdent, "a$b"}}},
		{"§", []tk{{tError, "§"}}},
		{"{p:Array(String)}", []tk{{tOp, "{"}, {tIdent, "p"}, {tOp, ":"}, {tIdent, "Array"}, {tOp, "("},
			{tIdent, "String"}, {tOp, ")"}, {tOp, "}"}}},
	}
	for _, tt := range tests {
		got := lexAll(tt.src)
		if got[len(got)-1].kind == tEOF {
			got = got[:len(got)-1]
		}
		if len(got) != len(tt.want) {
			t.Errorf("%q: got %d tokens %+v, want %+v", tt.src, len(got), got, tt.want)
			continue
		}
		for i, w := range tt.want {
			if got[i].kind != w.kind || got[i].text != w.text {
				t.Errorf("%q token %d = (%d %q), want (%d %q)", tt.src, i, got[i].kind, got[i].text, w.kind, w.text)
			}
			if got[i].kind != tError && tt.src[got[i].pos:got[i].end] != got[i].text {
				t.Errorf("%q token %d: bad offsets", tt.src, i)
			}
		}
	}
}

func TestUnescape(t *testing.T) {
	// Expected values verified with ClickHouse 25.8: SELECT hex('...').
	tests := []struct{ body, want string }{
		{`plain`, "plain"},
		{`a''b`, "a'b"},
		{`\a\v\e\N\z\x4A\\\"\` + "`" + `\0\b\f\r\n\t\'`, "\a\v\x1b\\zJ\\\"`\x00\b\f\r\n\t'"},
		{`\x4a`, "J"},
		{`Aé`, "Aé"},
		{`\/\=\%\_\?\Z\1`, `/=\%\_\?\Z\1`},
		{`trailing\`, `trailing\`},
	}
	for _, tt := range tests {
		got, msg := unescape(tt.body, '\'')
		if msg != "" || got != tt.want {
			t.Errorf("unescape(%q) = %q (%s), want %q", tt.body, got, msg, tt.want)
		}
	}
	for _, bad := range []string{`\x4`, `\xZZ`, `\x`} {
		if _, msg := unescape(bad, '\''); msg == "" {
			t.Errorf("unescape(%q) succeeded", bad)
		}
	}
	got, _ := unescape("a``b", '`')
	if got != "a`b" {
		t.Errorf("backquote doubling: %q", got)
	}
}

func TestQuoteRoundTrip(t *testing.T) {
	for _, s := range []string{"", "a", "it's", "\\", "\x00\b\f\n\r\t\a\v", "é ✓", strings.Repeat("'\\", 10)} {
		lit, err := ParseExpr(quoteString(s))
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		if got := lit.(*Literal).Value; got != s {
			t.Errorf("quoteString(%q) parsed back as %q", s, got)
		}
		id, err := ParseExpr(quoteIdent(s + "x"))
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		if got := id.(*Ident).Parts[0]; got != s+"x" {
			t.Errorf("quoteIdent(%q) parsed back as %q", s+"x", got)
		}
	}
}
