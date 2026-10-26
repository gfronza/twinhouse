package sqlparse

import (
	"errors"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// FuzzParse checks that arbitrary input never panics, that every error is a
// *cherr.Error, and that parsed SELECTs and expressions re-render to SQL that
// parses to the same AST.
func FuzzParse(f *testing.F) {
	for _, s := range roundTripSelects {
		f.Add(s)
	}
	for _, s := range roundTripExprs {
		f.Add("SELECT " + s)
	}
	f.Add(createEvents)
	f.Add("INSERT INTO t FORMAT JSONEachRow\n{\"a\":1}")
	f.Add("INSERT INTO t VALUES (1, 'a', {p:String})")
	f.Fuzz(func(t *testing.T, sql string) {
		st, err := Parse(sql)
		if err != nil {
			var ce *cherr.Error
			if !errors.As(err, &ce) {
				t.Fatalf("non-ClickHouse error %T: %v", err, err)
			}
			return
		}
		sel, ok := st.(*SelectStmt)
		if !ok {
			return
		}
		out := SelectString(sel)
		st2, err := Parse(out)
		if err != nil {
			t.Fatalf("re-render of %q is unparseable: %q: %v", sql, out, err)
		}
		if !astEqual(st, st2) {
			t.Fatalf("re-render changed AST: %q -> %q", sql, out)
		}
		for _, c := range sel.Columns {
			_ = ColumnName(c.Expr)
		}
	})
}
