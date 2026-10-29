package exec

import (
	"context"
	"strings"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/sqlparse"
	"github.com/gfronza/twinhouse/internal/storage"
)

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
	calls map[string]string // fingerprint of arrayJoin(x) -> generated column
}

// rewrite replaces arrayJoin(x) calls in e with references to
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
	out := &source{sc: sc, table: src.table, tableCols: src.tableCols, inner: src, noLimit: true}
	out.open = func(req *storage.ScanRequest) (rowIter, error) {
		it, err := src.open(req)
		if err != nil {
			return nil, err
		}
		return newArrayJoinIter(q.ctx, it, stages, width), nil
	}
	return out, nil
}

// arrayJoinIter emits one row per combination of stage elements, the last
// stage varying fastest. It holds only the current input row's arrays, never
// their product.
type arrayJoinIter struct {
	ctx    context.Context
	in     rowIter
	stages []*ajStage
	cur    []ajCursor
	width  int
	row    []any // the input row plus the current element of each placed stage
	steps  int
}

// ajCursor is one stage's arrays for the current row and its element index.
type ajCursor struct {
	arrays [][]any
	n, k   int
	zero   bool // LEFT ARRAY JOIN of empty arrays: one row of defaults
}

func newArrayJoinIter(ctx context.Context, in rowIter, stages []*ajStage, width int) *arrayJoinIter {
	cur := make([]ajCursor, len(stages))
	for i, st := range stages {
		cur[i].arrays = make([][]any, len(st.items))
	}
	return &arrayJoinIter{ctx: ctx, in: in, stages: stages, cur: cur, width: width}
}

func (a *arrayJoinIter) next() ([]any, error) {
	last := len(a.stages) - 1
	i := last // the stage to advance; -1 reads the next input row
	if a.row == nil {
		i = -1
	}
	for {
		if err := a.poll(); err != nil {
			return nil, err
		}
		if i < 0 {
			if err := a.load(); err != nil {
				return nil, err
			}
			i = 0
			if err := a.eval(0); err != nil {
				return nil, err
			}
		}
		c := &a.cur[i]
		if c.k++; c.k >= c.n {
			i--
			continue
		}
		a.place(i)
		if i == last {
			return append([]any(nil), a.row...), nil
		}
		i++
		if err := a.eval(i); err != nil {
			return nil, err
		}
	}
}

func (a *arrayJoinIter) close() { a.in.close() }

// poll checks for cancellation every 1024 steps, so a long run of empty
// arrays can still be cancelled.
func (a *arrayJoinIter) poll() error {
	if a.steps++; a.steps&1023 != 0 {
		return nil
	}
	if err := a.ctx.Err(); err != nil {
		return cherr.Wrap(cherr.Timeout, err, "query cancelled")
	}
	return nil
}

// load reads the next input row into the working row.
func (a *arrayJoinIter) load() error {
	a.row = nil
	row, err := a.in.next()
	if err != nil {
		return err
	}
	a.row = make([]any, a.width)
	copy(a.row, row)
	return nil
}

// eval evaluates stage i's arrays on the working row, which holds the
// current elements of the earlier stages, and rewinds its cursor.
func (a *arrayJoinIter) eval(i int) error {
	st, c := a.stages[i], &a.cur[i]
	for j, x := range st.items {
		v, err := x.eval(a.row)
		if err != nil {
			return err
		}
		arr, _ := v.([]any)
		if j > 0 && len(arr) != c.n {
			return cherr.New(cherr.SizesOfArraysDontMatch, "Sizes of ARRAY-JOIN-ed arrays do not match")
		}
		c.n = len(arr)
		c.arrays[j] = arr
	}
	c.zero = c.n == 0 && st.left
	if c.zero {
		c.n = 1
	}
	c.k = -1
	return nil
}

// place writes stage i's current elements into the working row.
func (a *arrayJoinIter) place(i int) {
	st, c := a.stages[i], &a.cur[i]
	for j, slot := range st.slots {
		if c.zero {
			a.row[slot] = zeroValue(st.elems[j])
		} else {
			a.row[slot] = c.arrays[j][c.k]
		}
	}
}
