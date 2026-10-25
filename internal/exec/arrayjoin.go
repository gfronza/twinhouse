package exec

import (
	"strings"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/sqlparse"
	"dynamodb-eventstream/internal/storage"
)

const sizesOfArraysDontMatch = 190

// ajStage is one ARRAY JOIN step: its arrays are unrolled together (zipped);
// consecutive stages multiply (each arrayJoin() call is its own stage).
type ajStage struct {
	items []*expr
	slots []int
	elems []chtype.Type
	left  bool
}

// arrayJoinPlan collects ARRAY JOIN clause items and arrayJoin() calls.
type arrayJoinPlan struct {
	stages []*ajStage
	calls  map[string]string // fingerprint of arrayJoin(x) -> generated column
}

// rewriteArrayJoins replaces arrayJoin(x) calls in e with references to
// generated columns, registering one stage per distinct call.
func (p *arrayJoinPlan) rewrite(c *compiler, sc *scope, e sqlparse.Expr, pending *[]sqlparse.Expr) sqlparse.Expr {
	return rewrite(e, func(n sqlparse.Expr) (sqlparse.Expr, bool) {
		fc, ok := n.(*sqlparse.FuncCall)
		if !ok || fc.Name != "arrayJoin" {
			return nil, false
		}
		if len(fc.Args) != 1 {
			return n, true // reported by the function compiler
		}
		inner := p.rewrite(c, sc, fc.Args[0], pending)
		fp := c.fingerprint(inner)
		name, ok := p.calls[fp]
		if !ok {
			name = "__arrayJoin_" + itoa(len(p.calls)+1)
			p.calls[fp] = name
			*pending = append(*pending, inner)
		}
		return &sqlparse.Ident{Parts: []string{name}}, true
	})
}

// planArrayJoin extends src with ARRAY JOIN columns. exprs are the query's
// (alias-expanded) expressions; arrayJoin() calls in them are rewritten in
// place.
func (q *query) planArrayJoin(c *compiler, src *source, clause *sqlparse.ArrayJoinClause, exprs [][]sqlparse.Expr) (*source, error) {
	plan := &arrayJoinPlan{calls: map[string]string{}}
	var pending []sqlparse.Expr
	for _, list := range exprs {
		for i, e := range list {
			if e != nil {
				list[i] = plan.rewrite(c, src.sc, e, &pending)
			}
		}
	}
	if clause == nil && len(pending) == 0 {
		return src, nil
	}
	sc := &scope{cols: append([]scopeCol(nil), src.sc.cols...)}
	ac := &compiler{q: q, env: c.env, sc: src.sc, used: c.used, clause: "ARRAY JOIN"}
	addItem := func(st *ajStage, e sqlparse.Expr, name string, hidden bool) error {
		x, err := ac.compile(e)
		if err != nil {
			return err
		}
		at := x.t.Unwrap()
		if at.Kind != chtype.Array {
			return cherr.New(cherr.IllegalTypeOfArgument, "ARRAY JOIN requires an array argument, got %s", x.t.String())
		}
		if at.Elem.Kind == chtype.Nothing {
			return cherr.New(cherr.IllegalTypeOfArgument, "ARRAY JOIN of an array of type %s", x.t.String())
		}
		if i, _ := sc.lookup([]string{name}); i >= 0 {
			// An unaliased ARRAY JOIN column shadows the array column.
			// Key pushdown resolves names in this scope, so a shadowed key
			// column is no longer matched.
			sc.cols[i].name = "\x00shadowed." + name
		}
		st.items = append(st.items, x)
		st.slots = append(st.slots, len(sc.cols))
		st.elems = append(st.elems, *at.Elem)
		sc.cols = append(sc.cols, scopeCol{name: name, t: *at.Elem, hidden: hidden})
		return nil
	}
	var stages []*ajStage
	if clause != nil {
		st := &ajStage{left: clause.Left}
		for _, it := range clause.Items {
			name := it.Alias
			if name == "" {
				id, ok := it.Expr.(*sqlparse.Ident)
				if !ok {
					return nil, cherr.New(cherr.IncorrectQuery, "ARRAY JOIN expression %s requires an alias", sqlparse.String(it.Expr))
				}
				name = strings.Join(id.Parts, ".")
			}
			if err := addItem(st, it.Expr, name, false); err != nil {
				return nil, err
			}
		}
		stages = append(stages, st)
	}
	for i, e := range pending {
		st := &ajStage{}
		if err := addItem(st, e, "__arrayJoin_"+itoa(i+1), true); err != nil {
			return nil, err
		}
		stages = append(stages, st)
	}
	width := len(sc.cols)
	base := src
	out := &source{sc: sc, table: src.table, tableCols: src.tableCols, inner: src, noLimit: true}
	out.open = func(req *storage.ScanRequest) (rowIter, error) {
		it, err := base.open(req)
		if err != nil {
			return nil, err
		}
		return &arrayJoinIter{in: it, stages: stages, width: width}, nil
	}
	return out, nil
}

type arrayJoinIter struct {
	in     rowIter
	stages []*ajStage
	width  int
	buf    [][]any
}

func (a *arrayJoinIter) next() ([]any, error) {
	for len(a.buf) == 0 {
		row, err := a.in.next()
		if err != nil {
			return nil, err
		}
		r := make([]any, a.width)
		copy(r, row)
		rows := [][]any{r}
		for _, st := range a.stages {
			if rows, err = st.expand(rows); err != nil {
				return nil, err
			}
		}
		a.buf = rows
	}
	r := a.buf[0]
	a.buf = a.buf[1:]
	return r, nil
}

func (a *arrayJoinIter) close() { a.in.close() }

func (st *ajStage) expand(rows [][]any) ([][]any, error) {
	var out [][]any
	for _, r := range rows {
		arrays := make([][]any, len(st.items))
		n := -1
		for i, x := range st.items {
			v, err := x.eval(r)
			if err != nil {
				return nil, err
			}
			arr, _ := v.([]any)
			if n >= 0 && len(arr) != n {
				return nil, cherr.New(sizesOfArraysDontMatch, "Sizes of ARRAY-JOIN-ed arrays do not match")
			}
			n = len(arr)
			arrays[i] = arr
		}
		if n == 0 && st.left {
			nr := append([]any(nil), r...)
			for i, slot := range st.slots {
				nr[slot] = zeroValue(st.elems[i])
			}
			out = append(out, nr)
			continue
		}
		for k := 0; k < n; k++ {
			nr := append([]any(nil), r...)
			for i, slot := range st.slots {
				nr[slot] = arrays[i][k]
			}
			out = append(out, nr)
		}
	}
	return out, nil
}
