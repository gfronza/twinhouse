package exec

import (
	"io"
	"strings"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/sqlparse"
	"github.com/gfronza/twinhouse/internal/storage"
)

// joinSpec is one planned hash join of the current source with a right side.
type joinSpec struct {
	kind       string // INNER, LEFT, RIGHT, FULL, CROSS
	strictness string // "", ALL, ANY, SEMI, ANTI
	leftW      int
	rightW     int
	leftKeys   []*expr // over the combined row
	rightKeys  []*expr // over the combined row (left part empty)
	keyConvL   []func(any) (any, error)
	keyConvR   []func(any) (any, error)
	residual   *expr
	leftZero   []any
	rightZero  []any
	// using maps a left slot to the right slot it is coalesced with.
	using map[int]int
	right *source
	// antiFill lists the key column pairs of an ANTI join: the emitted
	// (unmatched) left row carries its key values in the right key columns
	// too, like ClickHouse, instead of the right side's defaults.
	antiFill []antiFill
}

type antiFill struct {
	left, right int
	conv        func(any) (any, error)
}

// planJoins joins src with every JOIN clause, left to right. where is the
// query's WHERE (equality conjuncts linking a CROSS join's sides turn it
// into a hash join, like ClickHouse's cross-to-inner rewrite).
func (q *query) planJoins(src *source, joins []sqlparse.JoinClause, where sqlparse.Expr, env *planEnv) (*source, error) {
	if len(joins) == 0 {
		return src, nil
	}
	if v := q.settings["join_use_nulls"]; v != "" && v != "0" {
		return nil, cherr.New(cherr.NotImplemented, "join_use_nulls = 1 is not supported")
	}
	pushdown := true
	cur := src
	for i := range joins {
		j := &joins[i]
		right, err := q.planFrom(j.Table, env)
		if err != nil {
			return nil, err
		}
		if cur, err = q.joinTwo(cur, right, j, where, env); err != nil {
			return nil, err
		}
		if j.Kind == "RIGHT" || j.Kind == "FULL" {
			pushdown = false
		}
	}
	if !pushdown {
		cur.table = nil
	}
	return cur, nil
}

func (q *query) joinTwo(left, right *source, j *sqlparse.JoinClause, where sqlparse.Expr, env *planEnv) (*source, error) {
	kind, strict := strings.ToUpper(j.Kind), strings.ToUpper(j.Strictness)
	switch {
	case strict == "ASOF":
		return nil, cherr.New(cherr.NotImplemented, "ASOF JOIN is not supported")
	case (strict == "SEMI" || strict == "ANTI") && kind != "LEFT":
		return nil, cherr.New(cherr.NotImplemented, "%s %s JOIN is not supported (use LEFT %s JOIN)", kind, strict, strict)
	case strict == "ANY" && (kind == "RIGHT" || kind == "FULL"):
		return nil, cherr.New(cherr.NotImplemented, "%s ANY JOIN is not supported", kind)
	}
	switch kind {
	case "INNER", "LEFT", "RIGHT", "FULL", "CROSS":
	default:
		return nil, cherr.New(cherr.NotImplemented, "%s JOIN is not supported", kind)
	}
	L, R := len(left.sc.cols), len(right.sc.cols)
	sc := &scope{cols: append([]scopeCol(nil), left.sc.cols...)}
	leftNames := map[string]bool{}
	for _, c := range left.sc.cols {
		leftNames[baseName(c)] = true
	}
	usingSet := map[string]bool{}
	for _, u := range j.Using {
		usingSet[u] = true
	}
	if left.joined {
		// From the third table on, ClickHouse qualifies clashing columns on
		// every side (and an unqualified clashing name is ambiguous).
		rightNames := map[string]bool{}
		for _, c := range right.sc.cols {
			rightNames[c.name] = true
		}
		for i := range sc.cols {
			c := &sc.cols[i]
			if c.qname == "" && rightNames[c.name] && !usingSet[c.name] && len(c.quals) > 0 {
				c.qname = c.name
				c.name = c.quals[0] + "." + c.name
			}
		}
	}
	for _, c := range right.sc.cols {
		rc := c
		if a := j.Table.Alias; a != "" {
			// An aliased right table answers only to its alias (so self-joins
			// resolve the table name to the left side, like ClickHouse).
			rc.quals = []string{a}
		}
		if leftNames[c.name] || usingSet[c.name] {
			// ClickHouse qualifies right columns that clash with left ones.
			qual := right.displayQual()
			rc.qname = c.name
			rc.name = qual + "." + c.name
			if usingSet[c.name] {
				rc.hidden = true
			}
		}
		sc.cols = append(sc.cols, rc)
	}
	used := map[int]bool{}
	c := &compiler{q: q, env: env, sc: sc, used: used, clause: "JOIN ON"}
	spec := &joinSpec{kind: kind, strictness: strict, leftW: L, rightW: R, right: right, using: map[int]int{}}

	side := func(e sqlparse.Expr) (bool, bool) {
		var l, r bool
		walk(e, func(n sqlparse.Expr) bool {
			if id, ok := n.(*sqlparse.Ident); ok {
				if i, err := sc.lookup(id.Parts); err == nil && i >= 0 {
					if i < L {
						l = true
					} else {
						r = true
					}
				}
			}
			return true
		})
		return l, r
	}
	addKey := func(le, re sqlparse.Expr) error {
		lx, err := c.compile(le)
		if err != nil {
			return err
		}
		rx, err := c.compile(re)
		if err != nil {
			return err
		}
		st, err := chtype.Supertype(removeNullable(lx.t.Unwrap()), removeNullable(rx.t.Unwrap()))
		if err != nil {
			return cherr.Wrap(cherr.TypeMismatch, err, "JOIN key types %s and %s are not compatible", lx.t.String(), rx.t.String())
		}
		cl, err := converter(lx.t, st)
		if err != nil {
			return err
		}
		cr, err := converter(rx.t, st)
		if err != nil {
			return err
		}
		if strict == "ANTI" {
			li, lok := plainSlot(sc, le)
			ri, rok := plainSlot(sc, re)
			if lok && rok && li < L && ri >= L {
				if conv, err := converter(lx.t, rx.t); err == nil {
					spec.antiFill = append(spec.antiFill, antiFill{left: li, right: ri, conv: conv})
				}
			}
		}
		spec.leftKeys = append(spec.leftKeys, lx)
		spec.rightKeys = append(spec.rightKeys, rx)
		spec.keyConvL = append(spec.keyConvL, cl)
		spec.keyConvR = append(spec.keyConvR, cr)
		return nil
	}
	var residual []sqlparse.Expr
	switch {
	case len(j.Using) > 0:
		for _, u := range j.Using {
			li, _ := left.sc.lookup([]string{u})
			ri, _ := right.sc.lookup([]string{u})
			if li < 0 || ri < 0 {
				return nil, cherr.New(cherr.UnknownIdentifier, "Column %s in USING clause does not exist on both sides of the JOIN", u)
			}
			if err := addKey(&sqlparse.Ident{Parts: []string{u}}, &sqlparse.Ident{Parts: []string{sc.cols[L+ri].name}}); err != nil {
				return nil, err
			}
			spec.using[li] = L + ri
		}
	case j.On != nil:
		for _, cj := range splitAnd(j.On) {
			if b, ok := cj.(*sqlparse.BinaryExpr); ok && b.Op == "=" {
				ll, lr := side(b.Left)
				rl, rr := side(b.Right)
				switch {
				case ll && !lr && rr && !rl:
					if err := addKey(b.Left, b.Right); err != nil {
						return nil, err
					}
					continue
				case rl && !rr && lr && !ll:
					if err := addKey(b.Right, b.Left); err != nil {
						return nil, err
					}
					continue
				}
			}
			residual = append(residual, cj)
		}
	case kind == "CROSS" && where != nil:
		// Use WHERE equalities linking both sides as hash keys; the WHERE
		// clause is still applied to the joined rows.
		for _, cj := range splitAnd(where) {
			b, ok := cj.(*sqlparse.BinaryExpr)
			if !ok || b.Op != "=" {
				continue
			}
			ll, lr := side(b.Left)
			rl, rr := side(b.Right)
			switch {
			case ll && !lr && rr && !rl:
				if err := addKey(b.Left, b.Right); err != nil {
					return nil, err
				}
			case rl && !rr && lr && !ll:
				if err := addKey(b.Right, b.Left); err != nil {
					return nil, err
				}
			}
		}
	}
	if kind == "CROSS" && len(residual) > 0 {
		return nil, cherr.New(cherr.IncorrectQuery, "CROSS JOIN cannot have an ON clause")
	}
	if len(residual) > 0 {
		var cond sqlparse.Expr = residual[0]
		for _, r := range residual[1:] {
			cond = &sqlparse.BinaryExpr{Op: "AND", Left: cond, Right: r}
		}
		x, err := c.compile(cond)
		if err != nil {
			return nil, err
		}
		if err := checkCondType("JOIN ON", x, 1); err != nil {
			return nil, cherr.New(cherr.IllegalTypeOfArgument, "Illegal type %s of JOIN ON condition", x.t.String())
		}
		spec.residual = x
	}
	if (kind == "RIGHT" || kind == "FULL") && len(spec.leftKeys) == 0 {
		return nil, cherr.New(cherr.NotImplemented, "%s JOIN requires an equality condition between the two tables", kind)
	}
	spec.leftZero = make([]any, L)
	for i, col := range left.sc.cols {
		spec.leftZero[i] = zeroValue(col.t)
	}
	spec.rightZero = make([]any, R)
	for i, col := range right.sc.cols {
		spec.rightZero[i] = zeroValue(col.t)
	}
	out := &source{sc: sc, table: left.table, tableCols: left.tableCols, noLimit: true, joined: true}
	var rightUsed map[int]bool
	out.finalizeFn = func(fc *compiler) error {
		for i := range used {
			fc.used[i] = true
		}
		if err := left.finalize(fc); err != nil {
			return err
		}
		rightUsed = map[int]bool{}
		for i := range fc.used {
			if i >= L {
				rightUsed[i-L] = true
			}
		}
		return right.finalize(&compiler{q: q, env: env, sc: right.sc, used: rightUsed})
	}
	out.open = func(req *storage.ScanRequest) (rowIter, error) {
		var rreq *storage.ScanRequest
		if right.table != nil {
			rreq = &storage.ScanRequest{Consistent: q.consistent(), Columns: right.scanColumns(rightUsed)}
		}
		rit, err := right.open(rreq)
		if err != nil {
			return nil, err
		}
		build, err := spec.build(q, rit)
		rit.close()
		if err != nil {
			return nil, err
		}
		lit, err := left.open(req)
		if err != nil {
			return nil, err
		}
		return &joinIter{q: q, spec: spec, b: build, in: lit}, nil
	}
	return out, nil
}

func baseName(c scopeCol) string {
	if c.qname != "" {
		return c.qname
	}
	return c.name
}

// displayQual is the qualifier ClickHouse uses for clashing right columns.
func (s *source) displayQual() string {
	for _, c := range s.sc.cols {
		for _, q := range c.quals {
			if q != "" && !strings.Contains(q, ".") {
				return q
			}
		}
		if len(c.quals) > 0 {
			return c.quals[0]
		}
	}
	return "_right"
}

type joinBuild struct {
	rows    [][]any // combined-width rows with the left part empty
	index   map[string][]int
	matched []bool
}

func (spec *joinSpec) keyOf(row []any, keys []*expr, convs []func(any) (any, error)) (string, bool, error) {
	var b []byte
	for i, k := range keys {
		v, err := k.eval(row)
		if err != nil {
			return "", false, err
		}
		if v == nil {
			return "", false, nil // NULL never matches
		}
		if v, err = convs[i](v); err != nil {
			return "", false, err
		}
		b = aggstate.AppendKey(b, v)
	}
	return string(b), true, nil
}

func (spec *joinSpec) build(q *query, it rowIter) (*joinBuild, error) {
	b := &joinBuild{index: map[string][]int{}}
	for {
		r, err := it.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		row := make([]any, spec.leftW+spec.rightW)
		copy(row[spec.leftW:], r)
		k, ok, err := spec.keyOf(row, spec.rightKeys, spec.keyConvR)
		if err != nil {
			return nil, err
		}
		b.rows = append(b.rows, row)
		if ok {
			b.index[k] = append(b.index[k], len(b.rows)-1)
		}
		if len(b.rows)&1023 == 0 {
			if err := q.ctx.Err(); err != nil {
				return nil, cherr.Wrap(cherr.Timeout, err, "query cancelled")
			}
		}
	}
	b.matched = make([]bool, len(b.rows))
	return b, nil
}

type joinIter struct {
	q     *query
	spec  *joinSpec
	b     *joinBuild
	in    rowIter
	buf   [][]any
	done  bool
	tailI int
}

func (j *joinIter) combine(left []any, right []any) []any {
	s := j.spec
	row := make([]any, s.leftW+s.rightW)
	if left != nil {
		copy(row, left[:s.leftW])
	} else {
		copy(row, s.leftZero)
	}
	if right != nil {
		copy(row[s.leftW:], right[s.leftW:])
	} else {
		copy(row[s.leftW:], s.rightZero)
	}
	if left == nil {
		for li, ri := range s.using {
			row[li] = row[ri]
		}
	}
	if right == nil && left != nil {
		for _, f := range s.antiFill {
			if v, err := f.conv(row[f.left]); err == nil {
				row[f.right] = v
			}
		}
	}
	return row
}

// plainSlot resolves a plain column reference to its slot.
func plainSlot(sc *scope, e sqlparse.Expr) (int, bool) {
	id, ok := e.(*sqlparse.Ident)
	if !ok {
		return 0, false
	}
	i, err := sc.lookup(id.Parts)
	return i, err == nil && i >= 0
}

func (j *joinIter) next() ([]any, error) {
	s := j.spec
	for len(j.buf) == 0 {
		if j.done {
			// Unmatched right rows of RIGHT/FULL joins.
			for j.tailI < len(j.b.rows) {
				i := j.tailI
				j.tailI++
				if !j.b.matched[i] {
					return j.combine(nil, j.b.rows[i]), nil
				}
			}
			return nil, io.EOF
		}
		left, err := j.in.next()
		if err == io.EOF {
			j.done = true
			if s.kind != "RIGHT" && s.kind != "FULL" {
				return nil, io.EOF
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		var cands []int
		if len(s.leftKeys) == 0 {
			cands = make([]int, len(j.b.rows))
			for i := range cands {
				cands[i] = i
			}
		} else {
			probe := make([]any, s.leftW+s.rightW)
			copy(probe, left)
			k, ok, err := s.keyOf(probe, s.leftKeys, s.keyConvL)
			if err != nil {
				return nil, err
			}
			if ok {
				cands = j.b.index[k]
			}
		}
		matched := false
		for _, ci := range cands {
			row := j.combine(left, j.b.rows[ci])
			if s.residual != nil {
				v, err := s.residual.eval(row)
				if err != nil {
					return nil, err
				}
				if v == nil {
					continue
				}
				if ok, err := truth(v); err != nil || !ok {
					if err != nil {
						return nil, err
					}
					continue
				}
			}
			matched = true
			j.b.matched[ci] = true
			if s.strictness == "ANTI" {
				break
			}
			j.buf = append(j.buf, row)
			if s.strictness == "ANY" || s.strictness == "SEMI" {
				break
			}
		}
		if !matched && (s.kind == "LEFT" && s.strictness != "SEMI" || s.kind == "FULL") {
			j.buf = append(j.buf, j.combine(left, nil))
		}
	}
	r := j.buf[0]
	j.buf = j.buf[1:]
	return r, nil
}

func (j *joinIter) close() { j.in.close() }
