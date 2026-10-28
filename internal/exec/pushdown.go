package exec

import (
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/memtrack"
	"github.com/gfronza/twinhouse/internal/sqlparse"
	"github.com/gfronza/twinhouse/internal/storage"
)

// maxKeyRanges caps the number of ranges produced by pushdown; above it the
// engine falls back to a full scan.
const maxKeyRanges = 10000

// keyConstraint restricts a set of key columns (positions in the full key
// pk ++ sk) to a set of value tuples: tuples, or the values of an IN set,
// read in place so that a large set is not copied.
type keyConstraint struct {
	cols   []int
	tuples [][]any
	in     *valueSet
	// pick is, for a set of tuples, the tuple element of each column.
	pick []int
}

// size is the number of tuples.
func (k *keyConstraint) size() int {
	if k.in != nil {
		return len(k.in.vals)
	}
	return len(k.tuples)
}

// at is the value of tuple i for column j.
func (k *keyConstraint) at(i, j int) any {
	switch {
	case k.in == nil:
		return k.tuples[i][j]
	case k.pick == nil:
		return k.in.vals[i]
	}
	return k.in.vals[i].([]any)[k.pick[j]]
}

// consistent reports whether tuple i agrees with assignment a on assigned
// positions.
func (k *keyConstraint) consistent(i int, a []any) bool {
	for j, col := range k.cols {
		if col < len(a) && aggstate.Key(k.at(i, j)) != aggstate.Key(a[col]) {
			return false
		}
	}
	return true
}

type keyBounds struct {
	lower, upper *storage.Bound
}

// pushdown derives storage key ranges from the WHERE clause. It returns nil
// for a full scan and an empty slice when no row can match. Predicates are
// always re-evaluated on the returned rows, so ranges may be a superset.
func (q *query) pushdown(c *compiler, src *source, where sqlparse.Expr) ([]storage.KeyRange, error) {
	// The constants compiled here are dropped once the ranges are built.
	defer q.plan.consts.release(q.plan.consts.mark())
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
	acct := q.mem.NewAccount()
	defer acct.Close()
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
		next, overflow, err := extendAssignments(acct, assign, p, cover)
		if err != nil {
			return nil, err
		}
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
	if depth < len(keyNames) {
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
// positions < p) with the values for position p allowed by every
// constraint, in the order of the first constraint's tuples. The other
// constraints' allowed values are charged to acct while they are held.
func extendAssignments(acct *memtrack.Account, assign [][]any, p int, cover []keyConstraint) ([][]any, bool, error) {
	var out [][]any
	seen := map[string]bool{}
	first := &cover[0]
	idx := indexOf(first.cols, p)
	for _, a := range assign {
		allowed, held, err := allowedValues(acct, a, p, cover[1:])
		if err != nil {
			return nil, false, err
		}
		for i := range first.size() {
			if !first.consistent(i, a) {
				continue
			}
			v := first.at(i, idx)
			if !allowed.has(v) {
				continue
			}
			na := append(append(make([]any, 0, len(a)+1), a...), v)
			k := aggstate.Key(na)
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, na)
			if len(out) > maxKeyRanges {
				acct.Shrink(held)
				return nil, true, nil
			}
		}
		acct.Shrink(held)
	}
	return out, false, nil
}

// keySets are the values each constraint allows at a key position, by key.
type keySets []map[string]struct{}

func (s keySets) has(v any) bool {
	if len(s) == 0 {
		return true
	}
	k := aggstate.Key(v)
	for _, m := range s {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}

// allowedValues returns the values each constraint allows at position p
// under assignment a, and what they were charged. An IN set of single
// values allows its own keys, so they are not copied.
func allowedValues(acct *memtrack.Account, a []any, p int, cover []keyConstraint) (keySets, int64, error) {
	sets := make(keySets, len(cover))
	held := int64(0)
	for ci := range cover {
		k := &cover[ci]
		if k.in != nil && k.pick == nil {
			sets[ci] = k.in.keys
			continue
		}
		m := map[string]struct{}{}
		idx := indexOf(k.cols, p)
		for i := range k.size() {
			if !k.consistent(i, a) {
				continue
			}
			key := aggstate.Key(k.at(i, idx))
			if _, ok := m[key]; ok {
				continue
			}
			m[key] = struct{}{}
			n := keyEntry(setEntry, len(key))
			held += n
			if err := acct.Grow(n); err != nil {
				acct.Shrink(held)
				return nil, 0, err
			}
		}
		sets[ci] = m
	}
	return sets, held, nil
}

func indexOf(xs []int, v int) int {
	for i, x := range xs {
		if x == v {
			return i
		}
	}
	return -1
}

// inConstraint turns `key IN set` or `(k1, k2, ...) IN set` into a constraint.
func (q *query) inConstraint(c *compiler, n *sqlparse.BinaryExpr, keyOf func(sqlparse.Expr) (int, bool), keyTypes []chtype.Type) (keyConstraint, bool, error) {
	var positions []int // key position per tuple element, -1 if not a key column
	var leftType chtype.Type
	switch l := n.Left.(type) {
	case *sqlparse.TupleExpr:
		fields := make([]chtype.Field, len(l.Elems))
		hasKey := false
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
			hasKey = true
			positions = append(positions, p)
			fields[i] = chtype.Field{Type: keyTypes[p]}
		}
		if !hasKey {
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
	set, err := cc.inSet(leftType, n.Right)
	if cherr.Is(err, cherr.MemoryLimitExceeded) {
		return keyConstraint{}, false, err
	}
	if err != nil {
		// Non-constant right sides are evaluated by the filter (or fail there).
		return keyConstraint{}, false, nil
	}
	con := keyConstraint{in: set}
	for i, p := range positions {
		if p >= 0 {
			con.cols = append(con.cols, p)
			con.pick = append(con.pick, i)
		}
	}
	if leftType.Kind != chtype.Tuple {
		con.pick = nil
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
