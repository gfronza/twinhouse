package exec

import (
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/exec/aggstate"
	"dynamodb-eventstream/internal/sqlparse"
	"dynamodb-eventstream/internal/storage"
)

// maxKeyRanges caps the number of ranges produced by pushdown; above it the
// engine falls back to a full scan.
const maxKeyRanges = 10000

// keyConstraint restricts a set of key columns (positions in the full key
// pk ++ sk) to a set of value tuples.
type keyConstraint struct {
	cols   []int
	tuples [][]any
}

type keyBounds struct {
	lower, upper *storage.Bound
}

// pushdown derives storage key ranges from the WHERE clause. It returns nil
// for a full scan and an empty slice when no row can match. Predicates are
// always re-evaluated on the returned rows, so ranges may be a superset.
func (q *query) pushdown(c *compiler, src *source, where sqlparse.Expr) ([]storage.KeyRange, error) {
	t := src.table
	keyNames := append(append([]string{}, t.PartitionKey...), t.SortKey...)
	if len(keyNames) == 0 {
		return nil, nil
	}
	keyPos := map[int]int{} // scope column index -> key position
	keyTypes := make([]chtype.Type, len(keyNames))
	for p, n := range keyNames {
		ci := t.ColumnIndex(n)
		if ci < 0 {
			return nil, nil
		}
		keyPos[ci] = p
		keyTypes[p] = t.Columns[ci].Type.Unwrap()
	}
	keyOf := func(e sqlparse.Expr) (int, bool) {
		for {
			a, ok := e.(*sqlparse.AliasExpr)
			if !ok {
				break
			}
			e = a.X
		}
		id, ok := e.(*sqlparse.Ident)
		if !ok {
			return 0, false
		}
		ci, err := src.sc.lookup(id.Parts)
		if err != nil || ci < 0 {
			return 0, false
		}
		p, ok := keyPos[ci]
		return p, ok
	}
	constOf := func(e sqlparse.Expr) (*expr, bool) {
		cc := c.child()
		cc.used = nil
		cc.sc = &scope{}
		x, err := cc.compile(e)
		if err != nil || !x.konst || x.volatile {
			return nil, false
		}
		return x, true
	}

	var cons []keyConstraint
	bounds := make([]keyBounds, len(keyNames))
	addBound := func(p int, op string, x *expr) {
		v, ok := exactConvert(x.val, x.t, q.zoned(keyTypes[p]))
		if !ok || v == nil {
			return
		}
		b := &storage.Bound{Value: v, Inclusive: op == ">=" || op == "<="}
		kb := &bounds[p]
		switch op {
		case ">", ">=":
			if kb.lower == nil || tighterLower(b, kb.lower, keyTypes[p]) {
				kb.lower = b
			}
		case "<", "<=":
			if kb.upper == nil || tighterUpper(b, kb.upper, keyTypes[p]) {
				kb.upper = b
			}
		}
	}
	flip := map[string]string{"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "="}

	for _, cj := range splitAnd(where) {
		switch n := cj.(type) {
		case *sqlparse.BinaryExpr:
			switch n.Op {
			case "=", "<", "<=", ">", ">=":
				p, ok := keyOf(n.Left)
				other, op := n.Right, n.Op
				if !ok {
					if p, ok = keyOf(n.Right); !ok {
						continue
					}
					other, op = n.Left, flip[n.Op]
				}
				x, ok := constOf(other)
				if !ok {
					continue
				}
				if op == "=" {
					var tuples [][]any
					if v, ok := exactConvert(x.val, x.t, q.zoned(keyTypes[p])); ok && v != nil {
						tuples = [][]any{{v}}
					}
					cons = append(cons, keyConstraint{cols: []int{p}, tuples: tuples})
					continue
				}
				addBound(p, op, x)
			case "IN", "GLOBAL IN":
				con, ok, err := q.inConstraint(c, n, keyOf, keyTypes)
				if err != nil {
					return nil, err
				}
				if ok {
					cons = append(cons, con)
				}
			}
		case *sqlparse.BetweenExpr:
			if n.Not {
				continue
			}
			p, ok := keyOf(n.X)
			if !ok {
				continue
			}
			if lo, ok := constOf(n.Low); ok {
				addBound(p, ">=", lo)
			}
			if hi, ok := constOf(n.High); ok {
				addBound(p, "<=", hi)
			}
		}
	}

	// Walk the key in order, extending assignments while constraints exist.
	assign := [][]any{{}}
	depth := 0
	for p := range keyNames {
		var cover []keyConstraint
		for _, k := range cons {
			for _, col := range k.cols {
				if col == p {
					cover = append(cover, k)
					break
				}
			}
		}
		if len(cover) == 0 {
			break
		}
		next, overflow := extendAssignments(assign, p, cover)
		if overflow {
			if p < len(t.PartitionKey) {
				return nil, nil
			}
			break
		}
		assign, depth = next, p+1
		if len(assign) == 0 {
			return []storage.KeyRange{}, nil
		}
	}
	npk := len(t.PartitionKey)
	if depth < npk {
		return nil, nil
	}
	var lower, upper *storage.Bound
	if depth < len(keyNames) && depth >= npk {
		lower, upper = bounds[depth].lower, bounds[depth].upper
		if lower != nil && upper != nil {
			if c := chtype.CompareTyped(lower.Value, upper.Value, keyTypes[depth]); c > 0 || (c == 0 && !(lower.Inclusive && upper.Inclusive)) {
				return []storage.KeyRange{}, nil
			}
		}
	}
	if depth == 0 && lower == nil && upper == nil {
		return nil, nil
	}
	ranges := make([]storage.KeyRange, len(assign))
	for i, a := range assign {
		ranges[i] = storage.KeyRange{Partition: a[:npk], SortPrefix: a[npk:]}
		if len(ranges[i].SortPrefix) == 0 {
			ranges[i].SortPrefix = nil
		}
		if npk == 0 {
			ranges[i].Partition = nil
		}
		ranges[i].Lower, ranges[i].Upper = lower, upper
	}
	return ranges, nil
}

// extendAssignments extends each partial key assignment (values for key
// positions < p) with the values for position p allowed by every constraint.
func extendAssignments(assign [][]any, p int, cover []keyConstraint) ([][]any, bool) {
	var out [][]any
	seen := map[string]bool{}
	for _, a := range assign {
		var cands []any
		for ci, k := range cover {
			var vals []any
			idx := indexOf(k.cols, p)
			for _, tup := range k.tuples {
				if !consistent(k.cols, tup, a) {
					continue
				}
				vals = append(vals, tup[idx])
			}
			if ci == 0 {
				cands = vals
				continue
			}
			allowed := map[string]bool{}
			for _, v := range vals {
				allowed[aggstate.Key(v)] = true
			}
			var kept []any
			for _, v := range cands {
				if allowed[aggstate.Key(v)] {
					kept = append(kept, v)
				}
			}
			cands = kept
		}
		for _, v := range cands {
			na := append(append(make([]any, 0, len(a)+1), a...), v)
			k := aggstate.Key(na)
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, na)
			if len(out) > maxKeyRanges {
				return nil, true
			}
		}
	}
	return out, false
}

func indexOf(xs []int, v int) int {
	for i, x := range xs {
		if x == v {
			return i
		}
	}
	return -1
}

// consistent reports whether tup agrees with assignment a on assigned positions.
func consistent(cols []int, tup []any, a []any) bool {
	for i, col := range cols {
		if col < len(a) && aggstate.Key(tup[i]) != aggstate.Key(a[col]) {
			return false
		}
	}
	return true
}

// inConstraint turns `key IN set` or `(k1, k2, ...) IN set` into a constraint.
func (q *query) inConstraint(c *compiler, n *sqlparse.BinaryExpr, keyOf func(sqlparse.Expr) (int, bool), keyTypes []chtype.Type) (keyConstraint, bool, error) {
	var positions []int // key position per tuple element, -1 if not a key column
	var leftType chtype.Type
	switch l := n.Left.(type) {
	case *sqlparse.TupleExpr:
		fields := make([]chtype.Field, len(l.Elems))
		any := false
		for i, e := range l.Elems {
			p, ok := keyOf(e)
			if !ok {
				positions = append(positions, -1)
				x, err := c.child().compile(e)
				if err != nil {
					return keyConstraint{}, false, err
				}
				fields[i] = chtype.Field{Type: x.t}
				continue
			}
			any = true
			positions = append(positions, p)
			fields[i] = chtype.Field{Type: keyTypes[p]}
		}
		if !any {
			return keyConstraint{}, false, nil
		}
		leftType = chtype.NewTuple(fields...)
	default:
		p, ok := keyOf(n.Left)
		if !ok {
			return keyConstraint{}, false, nil
		}
		positions = []int{p}
		leftType = keyTypes[p]
	}
	cc := c.child()
	cc.used = nil
	vals, err := cc.inValues(leftType, n.Right)
	if err != nil {
		// Non-constant right sides are evaluated by the filter (or fail there).
		return keyConstraint{}, false, nil
	}
	var con keyConstraint
	var keep []int
	for i, p := range positions {
		if p >= 0 {
			con.cols = append(con.cols, p)
			keep = append(keep, i)
		}
	}
	tuple := leftType.Kind == chtype.Tuple
	for _, v := range vals {
		if !tuple {
			con.tuples = append(con.tuples, []any{v})
			continue
		}
		tv := v.([]any)
		row := make([]any, len(keep))
		for j, i := range keep {
			row[j] = tv[i]
		}
		con.tuples = append(con.tuples, row)
	}
	return con, true, nil
}

// tighterLower reports whether lower bound a (on a key column of type t)
// excludes more than b; bounds compare in t's ClickHouse order.
func tighterLower(a, b *storage.Bound, t chtype.Type) bool {
	c := chtype.CompareTyped(a.Value, b.Value, t)
	return c > 0 || (c == 0 && !a.Inclusive)
}

// tighterUpper is tighterLower for upper bounds.
func tighterUpper(a, b *storage.Bound, t chtype.Type) bool {
	c := chtype.CompareTyped(a.Value, b.Value, t)
	return c < 0 || (c == 0 && !a.Inclusive)
}
