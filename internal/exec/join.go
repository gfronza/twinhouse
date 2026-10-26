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
		return startJoin(q, spec, rit, func() (rowIter, error) { return left.open(req) })
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

// plainSlot resolves a plain column reference to its slot.
func plainSlot(sc *scope, e sqlparse.Expr) (int, bool) {
	id, ok := e.(*sqlparse.Ident)
	if !ok {
		return 0, false
	}
	i, err := sc.lookup(id.Parts)
	return i, err == nil && i >= 0
}

// startJoin builds the right side from rit, closes it and opens the left
// side to probe. Tests swap it for the previous implementation (an oracle).
var startJoin = startHashJoin

func startHashJoin(q *query, spec *joinSpec, rit rowIter, openLeft func() (rowIter, error)) (rowIter, error) {
	b, err := spec.build(q, rit)
	rit.close()
	if err != nil {
		return nil, err
	}
	lit, err := openLeft()
	if err != nil {
		return nil, err
	}
	return &joinIter{q: q, spec: spec, b: b, in: lit, scratch: make([]any, spec.leftW+spec.rightW)}, nil
}

type joinBuild struct {
	rows    chunkedRows    // right-side rows, as the right input returned them
	index   map[string]int // key → its entry in lists
	lists   [][]int        // the build rows of each key, in input order
	matched []bool
	key     []byte // reused key buffer
}

// lookup returns the build rows whose key is key.
func (b *joinBuild) lookup(key []byte) []int {
	if li, ok := b.index[string(key)]; ok {
		return b.lists[li]
	}
	return nil
}

// chunkRows is the size of every chunk of chunkedRows after the first.
const chunkRows = 1024

// chunkedRows is an append-only list of rows kept in fixed-size chunks, so
// growing it never copies the rows already held.
type chunkedRows struct {
	chunks [][][]any
	n      int
}

func (c *chunkedRows) add(r []any) {
	last := len(c.chunks) - 1
	if last < 0 || len(c.chunks[last]) == chunkRows {
		var chunk [][]any
		if last >= 0 {
			chunk = make([][]any, 0, chunkRows)
		}
		c.chunks = append(c.chunks, chunk)
		last++
	}
	c.chunks[last] = append(c.chunks[last], r)
	c.n++
}

func (c *chunkedRows) at(i int) []any { return c.chunks[i/chunkRows][i%chunkRows] }

// appendKey appends the key of row to dst; ok is false when a part of it
// is NULL, which never matches.
func (spec *joinSpec) appendKey(dst []byte, row []any, keys []*expr, convs []func(any) (any, error)) ([]byte, bool, error) {
	for i, k := range keys {
		v, err := k.eval(row)
		if err != nil {
			return dst, false, err
		}
		if v == nil {
			return dst, false, nil
		}
		if v, err = convs[i](v); err != nil {
			return dst, false, err
		}
		dst = aggstate.AppendKey(dst, v)
	}
	return dst, true, nil
}

// build reads the right side into rows and indexes it by key.
func (spec *joinSpec) build(q *query, it rowIter) (*joinBuild, error) {
	b := &joinBuild{index: map[string]int{}}
	scratch := make([]any, spec.leftW+spec.rightW)
	for {
		r, err := it.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := b.add(spec, scratch, r); err != nil {
			return nil, err
		}
		if b.rows.n&1023 == 0 {
			if err := q.ctx.Err(); err != nil {
				return nil, cherr.Wrap(cherr.Timeout, err, "query cancelled")
			}
		}
	}
	b.matched = make([]bool, b.rows.n)
	return b, nil
}

// add stores r and indexes it by its key, evaluated over scratch: a reused
// combined row whose left part stays empty. Key-less joins need no index.
func (b *joinBuild) add(spec *joinSpec, scratch, r []any) error {
	b.rows.add(r)
	if len(spec.rightKeys) == 0 {
		return nil
	}
	setSlots(scratch[spec.leftW:], r)
	key, ok, err := spec.appendKey(b.key[:0], scratch, spec.rightKeys, spec.keyConvR)
	b.key = key
	if err != nil || !ok {
		return err
	}
	if li, found := b.index[string(key)]; found {
		b.lists[li] = append(b.lists[li], b.rows.n-1)
		return nil
	}
	b.index[string(key)] = len(b.lists)
	b.lists = append(b.lists, []int{b.rows.n - 1})
	return nil
}

// setSlots copies src into dst and clears the slots src does not reach.
func setSlots(dst, src []any) {
	n := copy(dst, src)
	clear(dst[n:])
}

// joinIter probes the build with one left row at a time and emits one
// combined row per next call.
type joinIter struct {
	q    *query
	spec *joinSpec
	b    *joinBuild
	in   rowIter
	// scratch holds the current left row, then the right side of the
	// candidate being tested (empty while the left keys are evaluated).
	scratch []any
	// Probe state of the current left row, while probing is set.
	probing bool
	left    []any
	cands   []int // the matching entry of lists, never copied
	all     bool  // key-less join: every build row is a candidate
	pos     int
	matched bool
	// checked is set once the residual has passed on every entry of cands,
	// which then point into accepted (reused across left rows).
	checked  bool
	accepted []int
	scanned  int // candidates tested, for polling ctx
	done     bool
	tailI    int
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
		copy(row[s.leftW:], right)
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

func (j *joinIter) next() ([]any, error) {
	for !j.done {
		if !j.probing {
			if err := j.readLeft(); err != nil {
				return nil, err
			}
			continue
		}
		row, err := j.probe()
		if err != nil || row != nil {
			return row, err
		}
		j.probing = false
		if !j.matched && j.emitsUnmatchedLeft() {
			return j.combine(j.left, nil), nil
		}
	}
	return j.tail()
}

// readLeft starts probing the next left row, or marks the left side done.
func (j *joinIter) readLeft() error {
	s := j.spec
	left, err := j.in.next()
	if err == io.EOF {
		j.done = true
		return nil
	}
	if err != nil {
		return err
	}
	j.probing, j.left, j.cands, j.all, j.pos, j.matched, j.checked = true, left, nil, false, 0, false, false
	if len(s.leftKeys) == 0 {
		j.all = true
	} else {
		setSlots(j.scratch, left)
		key, ok, err := s.appendKey(j.b.key[:0], j.scratch, s.leftKeys, s.keyConvL)
		j.b.key = key
		if err != nil {
			return err
		}
		if ok {
			j.cands = j.b.lookup(key)
		}
	}
	if s.residual != nil && !j.stopsAtFirstMatch() {
		return j.checkAll()
	}
	return nil
}

// stopsAtFirstMatch reports whether a left row uses at most one match.
func (j *joinIter) stopsAtFirstMatch() bool {
	switch j.spec.strictness {
	case "ANY", "SEMI", "ANTI":
		return true
	}
	return false
}

// checkAll tests the residual on every candidate of the current left row
// before any of its rows is emitted, so a residual error comes first (as it
// did when those rows were buffered). The accepted candidates replace cands.
func (j *joinIter) checkAll() error {
	j.accepted = j.accepted[:0]
	n := j.candidates()
	for pos := range n {
		if err := j.poll(); err != nil {
			return err
		}
		ci := j.candidate(pos)
		ok, err := j.accepts(j.b.rows.at(ci))
		if err != nil {
			return err
		}
		if ok {
			j.accepted = append(j.accepted, ci)
		}
	}
	j.cands, j.all, j.checked = j.accepted, false, true
	return nil
}

// candidates is the number of candidates of the current left row.
func (j *joinIter) candidates() int {
	if j.all {
		return j.b.rows.n
	}
	return len(j.cands)
}

// candidate returns the build row of the candidate at pos.
func (j *joinIter) candidate(pos int) int {
	if j.all {
		return pos
	}
	return j.cands[pos]
}

// probe returns the next combined row for the current left row, or nil once
// its candidates are exhausted. ANY and SEMI stop at the first match; ANTI
// stops there too but emits nothing.
func (j *joinIter) probe() ([]any, error) {
	n := j.candidates()
	for j.pos < n {
		ci := j.candidate(j.pos)
		j.pos++
		if err := j.poll(); err != nil {
			return nil, err
		}
		if !j.checked {
			ok, err := j.accepts(j.b.rows.at(ci))
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		j.matched = true
		j.b.matched[ci] = true
		switch j.spec.strictness {
		case "ANTI":
			j.pos = n
			return nil, nil
		case "ANY", "SEMI":
			j.pos = n
		}
		return j.combine(j.left, j.b.rows.at(ci)), nil
	}
	return nil, nil
}

// accepts reports whether the residual condition holds for the current left
// row joined with right.
func (j *joinIter) accepts(right []any) (bool, error) {
	s := j.spec
	if s.residual == nil {
		return true, nil
	}
	copy(j.scratch, j.left[:s.leftW])
	setSlots(j.scratch[s.leftW:], right)
	v, err := s.residual.eval(j.scratch)
	if err != nil || v == nil {
		return false, err
	}
	return truth(v)
}

// poll checks for cancellation every 1024 tested candidates.
func (j *joinIter) poll() error {
	j.scanned++
	if j.scanned&1023 != 0 {
		return nil
	}
	if err := j.q.ctx.Err(); err != nil {
		return cherr.Wrap(cherr.Timeout, err, "query cancelled")
	}
	return nil
}

func (j *joinIter) emitsUnmatchedLeft() bool {
	s := j.spec
	return s.kind == "LEFT" && s.strictness != "SEMI" || s.kind == "FULL"
}

// tail emits the unmatched right rows of RIGHT and FULL joins.
func (j *joinIter) tail() ([]any, error) {
	if j.spec.kind != "RIGHT" && j.spec.kind != "FULL" {
		return nil, io.EOF
	}
	for j.tailI < j.b.rows.n {
		i := j.tailI
		j.tailI++
		if !j.b.matched[i] {
			return j.combine(nil, j.b.rows.at(i)), nil
		}
	}
	return nil, io.EOF
}

func (j *joinIter) close() { j.in.close() }
