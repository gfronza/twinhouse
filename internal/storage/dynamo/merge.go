package dynamo

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/storage"
)

func (s *Store) merge(ctx context.Context, t *catalog.Table, rows []storage.MergeRow) error {
	l, err := s.impl.layoutOf(t)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	gate, err := s.impl.prepareTarget(ctx, l)
	if err != nil {
		return err
	}
	ms := parseMergeRows(rows, gate)
	if len(ms) == 0 {
		return nil
	}
	switch {
	case l.merging:
		plain := make([]storage.MergeRow, len(ms))
		for i := range ms {
			plain[i] = ms[i].MergeRow
		}
		return s.impl.mergeRows(ctx, l, plain, true)
	case l.appendOnly:
		return s.impl.mergeAppend(ctx, l, ms)
	default:
		return s.impl.mergeReplacing(ctx, l, ms)
	}
}

// mergeReplacing writes view output into a ReplacingMergeTree target in
// source order rather than application order (DESIGN.md §4): the item
// takes the source row's write time as _ts and (source row id, output
// index) as _so, and replaces the stored item only if it is later in that
// order (after the version, if any). The stream worker re-applying rows
// that inline application already applied, or applying records of
// different stream shards in any order, therefore converges to the row
// ClickHouse keeps; re-application of the same output is a no-op rewrite.
// Output whose source write time is unknown (ids without one) is ordered by
// application time, as before.
func (s *storeImpl) mergeReplacing(ctx context.Context, l *layout, ms []mergeSource) error {
	writes := make([]replWrite, len(ms))
	for i := range ms {
		m := &ms[i]
		pk, sk, err := l.rowKey(m.Row, nil)
		if err != nil {
			return err
		}
		ts := m.ts
		if ts == 0 {
			ts = s.stamp(1)
		}
		item, err := s.rowItem(l, m.Row, pk, sk, hex.EncodeToString(mvUniquifier(m.Source, m.Index)), true, ts)
		if err != nil {
			return err
		}
		so := m.Source.RowID + "#" + fmt.Sprintf("%010d", m.Index)
		item[attrSrcOrder] = attrS(so)
		if err := checkItemSize(l, i, item); err != nil {
			return err
		}
		writes[i] = replWrite{idx: i, item: item, ts: ts, so: so}
	}
	err := s.writeReplacing(ctx, l, groupReplacing(writes), true, make([]bool, len(ms)), make([]int64, len(ms)))
	return ddbError(err, "cannot write to DynamoDB table %s", l.physical)
}

// mergeAppend upserts MV output into an append-only table under keys derived
// from (Source, Index), so redelivery overwrites the same item.
func (s *storeImpl) mergeAppend(ctx context.Context, l *layout, rows []mergeSource) error {
	reqs := make([]types.WriteRequest, 0, len(rows))
	seen := map[string]int{}
	base := s.stamp(len(rows))
	for i, mr := range rows {
		uniq := mvUniquifier(mr.Source, mr.Index)
		pk, sk, err := l.rowKey(mr.Row, uniq)
		if err != nil {
			return err
		}
		item, err := s.rowItem(l, mr.Row, pk, sk, hex.EncodeToString(uniq), true, base+int64(i))
		if err != nil {
			return err
		}
		if err := checkItemSize(l, i, item); err != nil {
			return err
		}
		req := types.WriteRequest{PutRequest: &types.PutRequest{Item: item}}
		// A batch must not contain one key twice; the last write wins.
		k := string(pk) + "\x00" + string(sk)
		if i, ok := seen[k]; ok {
			reqs[i] = req
			continue
		}
		seen[k] = len(reqs)
		reqs = append(reqs, req)
	}
	return s.writeChunks(ctx, l.physical, reqs)
}

// mergeRows merges rows into an Aggregating/Summing table (DESIGN.md §4).
// With marked, each row is applied exactly once per (Source, key, Index)
// using a marker item; otherwise (direct INSERT) it is applied
// unconditionally. Rows sharing a key are pre-aggregated: one row update per
// transaction of up to maxMarkersPerTx markers, one extremum write per
// column and one member pass per set column for the whole group. Distinct
// keys are applied in parallel. Every row is planned (and validated) before
// the first write.
func (s *storeImpl) mergeRows(ctx context.Context, l *layout, rows []storage.MergeRow, marked bool) error {
	type group struct {
		pk, sk []byte
		rows   []*rowPlan
	}
	var groups []*group
	byKey := map[string]*group{}
	seenMarker := map[string]bool{}
	for i := range rows {
		mr := &rows[i]
		pk, sk, err := l.rowKey(mr.Row, nil)
		if err != nil {
			return err
		}
		rp, err := s.planRow(ctx, l, mr)
		if err != nil {
			return err
		}
		if marked {
			rp.marker = l.markerID(mr.Source, pk, sk, mr.Index)
			if seenMarker[rp.marker] {
				continue // the same application twice in one call
			}
			seenMarker[rp.marker] = true
		}
		k := string(pk) + "\x00" + string(sk)
		g := byKey[k]
		if g == nil {
			g = &group{pk: pk, sk: sk}
			byKey[k] = g
			groups = append(groups, g)
		}
		g.rows = append(g.rows, rp)
	}
	for _, g := range groups {
		u := groupUpdate(g.rows)
		if n := u.contributionSize(); n > maxItemSize {
			return cherr.New(cherr.BadArguments, "the rows merged into one key of %s add %d bytes; DynamoDB items are limited to %d bytes (400 KB)",
				l.t.FullName(), n, maxItemSize)
		}
	}
	err := parallel(ctx, len(groups), writeParallelism, func(ctx context.Context, i int) error {
		return s.applyGroup(ctx, l, groups[i].pk, groups[i].sk, groups[i].rows, marked)
	})
	return ddbError(err, "cannot merge into DynamoDB table %s", l.physical)
}

// maxMarkersPerTx is the number of rows (markers) applied by one step-1
// transaction: TransactWriteItems takes 100 items, one is the row update.
const maxMarkersPerTx = 99

// extremumOp is an idempotent conditional update (min/max/argMin/argMax).
type extremumOp struct {
	cmpAttr string // attribute compared (value or argMin/argMax key)
	// existAttr marks presence: c#h for AggregateFunction states, the column
	// itself for SimpleAggregateFunction.
	existAttr string
	cmpType   chtype.Type
	cmpVal    any
	cmpAV     types.AttributeValue
	set       map[string]types.AttributeValue
	greater   bool // max/argMax
}

type memberSet struct {
	col     *colSpec
	members []any
}

// opKind is how a step-1 attribute contribution combines with the stored
// value and with the contributions of other rows of the same key.
type opKind uint8

const (
	opSet      opKind = iota // SET a = v: the last row wins
	opIfAbsent               // SET a = if_not_exists(a, v): the first row wins
	opAddInt                 // ADD a n: exact integer delta
	opAddFloat               // a = a + f in IEEE arithmetic (compare-and-swap)
	opAppend                 // list_append of every row's elements, in order
)

// attrOp is one attribute contribution of a row (or of a group of rows).
type attrOp struct {
	attr string
	kind opKind
	av   types.AttributeValue   // opSet, opIfAbsent
	n    *big.Int               // opAddInt
	fs   []float64              // opAddFloat: addends in row order
	bits int                    // opAddFloat: 32 or 64 (Float32 sums round each step)
	list []types.AttributeValue // opAppend
}

// rowPlan is the decomposition of one merge row (DESIGN.md §4).
type rowPlan struct {
	ops     []attrOp // step 1: key SETs, additive parts, any/anyLast/groupArray
	extrema []extremumOp
	sets    []memberSet
	marker  string // exactly-once marker id (marked merges)
}

// finishing reports whether the row has steps 2/3 (extrema, set members).
func (r *rowPlan) finishing() bool { return len(r.extrema) > 0 || len(r.sets) > 0 }

// planRow decomposes a merge row into attribute contributions, extremum
// candidates and set members; it issues no writes (set states loaded
// lazily by the engine are read here).
func (s *storeImpl) planRow(ctx context.Context, l *layout, mr *storage.MergeRow) (*rowPlan, error) {
	p := &rowPlan{}
	for i := range l.cols {
		c := &l.cols[i]
		v := mr.Row[i]
		if c.key {
			av, err := encodeValue(c.typ, v)
			if err != nil {
				return nil, columnErr(l, c.name, err)
			}
			p.ops = append(p.ops, attrOp{attr: c.name, kind: opSet, av: av})
			continue
		}
		if v == nil {
			continue // aggregates skip NULL / absent states
		}
		var err error
		switch c.kind {
		case colAggState:
			err = s.planAggState(ctx, p, c, v)
		case colSimpleAgg:
			err = planSimpleAgg(p, c, v)
		default:
			if c.summed {
				err = p.addSum(c.name, c.typ, v)
			} else {
				var av types.AttributeValue
				if av, err = encodeValue(c.typ, v); err == nil {
					p.ops = append(p.ops, attrOp{attr: c.name, kind: opIfAbsent, av: av})
				}
			}
		}
		if err != nil {
			return nil, columnErr(l, c.name, err)
		}
	}
	return p, nil
}

// floatSum reports whether sums of type t accumulate in IEEE arithmetic
// (Float32/Float64) and with which width. Integer sums use DynamoDB ADD,
// which is exact; float sums cannot: ADD sums decimals exactly and fails
// once the exact sum needs more than 38 significant digits (1e5 + 5.55e-17),
// so they are computed in Go and written by compare-and-swap.
func floatSum(t chtype.Type) (bool, int) {
	switch k := baseType(t).Kind; k {
	case chtype.Float32:
		return true, 32
	case chtype.Float64:
		return true, 64
	}
	return false, 0
}

// addSum adds v to attr, a sum of type t (see floatSum).
func (p *rowPlan) addSum(attr string, t chtype.Type, v any) error {
	if isZeroNumber(v) {
		return nil
	}
	if fl, bits := floatSum(t); fl {
		f, ok := toFloat64(v)
		if !ok {
			return valueMismatch(t, v)
		}
		p.ops = append(p.ops, attrOp{attr: attr, kind: opAddFloat, fs: []float64{f}, bits: bits})
		return nil
	}
	n, ok := toBigInt(v)
	if !ok {
		return valueMismatch(t, v)
	}
	p.ops = append(p.ops, attrOp{attr: attr, kind: opAddInt, n: n})
	return nil
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case uint64:
		return float64(x), true
	case int64:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func toBigInt(v any) (*big.Int, bool) {
	switch x := v.(type) {
	case uint64:
		return new(big.Int).SetUint64(x), true
	case int64:
		return big.NewInt(x), true
	case bool:
		if x {
			return big.NewInt(1), true
		}
		return big.NewInt(0), true
	case float64:
		// avg(Int) states carry their integral sum as a float.
		if math.IsInf(x, 0) || math.IsNaN(x) || x != math.Trunc(x) {
			return nil, false
		}
		n, _ := big.NewFloat(x).Int(nil)
		return n, true
	}
	return nil, false
}

func (s *storeImpl) planAggState(ctx context.Context, p *rowPlan, c *colSpec, v any) error {
	st, ok := v.(*chtype.AggPartial)
	if !ok {
		return cherr.New(cherr.TypeMismatch, "expected an aggregate state for %s, got %T", typeName(c.typ), v)
	}
	if st.Func != "" && st.Func != c.agg.Func {
		return cherr.New(cherr.TypeMismatch, "cannot merge a %s state into AggregateFunction(%s, ...)", st.Func, c.agg.Func)
	}
	trueAV := attrTrue
	switch c.agg.Func {
	case catalog.AggCount:
		if st.Count > 0 {
			p.ops = append(p.ops, attrOp{attr: c.name + sufCount, kind: opAddInt, n: new(big.Int).SetUint64(st.Count)})
		}
	case catalog.AggSum, catalog.AggAvg:
		if st.Sum != nil {
			if err := p.addSum(c.name+sufSum, aggSumType(c.agg), st.Sum); err != nil {
				return err
			}
		}
		if c.agg.Func == catalog.AggAvg && st.Count > 0 {
			p.ops = append(p.ops, attrOp{attr: c.name + sufCount, kind: opAddInt, n: new(big.Int).SetUint64(st.Count)})
		}
	case catalog.AggAny, catalog.AggAnyLast:
		if !st.HasValue {
			return nil
		}
		av, err := encodeValue(c.agg.Args[0], st.Value)
		if err != nil {
			return err
		}
		kind := opIfAbsent
		if c.agg.Func == catalog.AggAnyLast {
			kind = opSet
		}
		p.ops = append(p.ops, attrOp{attr: c.name + sufValue, kind: kind, av: av}, attrOp{attr: c.name + sufHas, kind: opSet, av: trueAV})
	case catalog.AggMin, catalog.AggMax:
		if !st.HasValue || st.Value == nil {
			return nil
		}
		av, err := encodeValue(c.agg.Args[0], st.Value)
		if err != nil {
			return err
		}
		p.extrema = append(p.extrema, extremumOp{
			cmpAttr: c.name + sufValue, existAttr: c.name + sufHas, cmpType: c.agg.Args[0], cmpVal: st.Value, cmpAV: av,
			set:     map[string]types.AttributeValue{c.name + sufValue: av, c.name + sufHas: trueAV},
			greater: c.agg.Func == catalog.AggMax,
		})
	case catalog.AggArgMin, catalog.AggArgMax:
		if !st.HasValue || st.Key == nil {
			return nil
		}
		vav, err := encodeValue(c.agg.Args[0], st.Value)
		if err != nil {
			return err
		}
		kav, err := encodeValue(c.agg.Args[1], st.Key)
		if err != nil {
			return err
		}
		p.extrema = append(p.extrema, extremumOp{
			cmpAttr: c.name + sufKey, existAttr: c.name + sufHas, cmpType: c.agg.Args[1], cmpVal: st.Key, cmpAV: kav,
			set:     map[string]types.AttributeValue{c.name + sufValue: vav, c.name + sufKey: kav, c.name + sufHas: trueAV},
			greater: c.agg.Func == catalog.AggArgMax,
		})
	case catalog.AggUniq, catalog.AggUniqExact, catalog.AggGroupUniqArray:
		members := st.Set
		if !st.SetLoaded && st.LoadSet != nil {
			var err error
			if members, err = st.LoadSet(ctx); err != nil {
				return err
			}
		}
		if len(members) > 0 {
			p.sets = append(p.sets, memberSet{col: c, members: members})
		}
	case catalog.AggGroupArray:
		if len(st.List) > 0 {
			av, err := encodeValue(chtype.NewArray(c.agg.Args[0]), st.List)
			if err != nil {
				return err
			}
			p.ops = append(p.ops, attrOp{attr: c.name + sufList, kind: opAppend, list: av.(*types.AttributeValueMemberL).Value})
		}
	}
	return nil
}

// aggSumType is the type of a sum/avg state's c#s: sum's result type;
// avg sums integers exactly (like ClickHouse's integer numerator) and
// floats as Float64.
func aggSumType(spec catalog.AggregateSpec) chtype.Type {
	if spec.Func == catalog.AggAvg && len(spec.Args) > 0 {
		if fl, _ := floatSum(spec.Args[0]); !fl {
			return chtype.Type{Kind: catalog.SumResultKind(spec.Args[0])}
		}
	}
	return chtype.Type{Kind: spec.SumKind()}
}

func planSimpleAgg(p *rowPlan, c *colSpec, v any) error {
	switch c.saf {
	case catalog.AggSum:
		return p.addSum(c.name, c.typ, v)
	case catalog.AggMin, catalog.AggMax:
		av, err := encodeValue(c.typ, v)
		if err != nil {
			return err
		}
		p.extrema = append(p.extrema, extremumOp{
			cmpAttr: c.name, existAttr: c.name, cmpType: c.typ, cmpVal: v, cmpAV: av,
			set: map[string]types.AttributeValue{c.name: av}, greater: c.saf == catalog.AggMax,
		})
	case catalog.AggAny, catalog.AggAnyLast:
		av, err := encodeValue(c.typ, v)
		if err != nil {
			return err
		}
		kind := opIfAbsent
		if c.saf == catalog.AggAnyLast {
			kind = opSet
		}
		p.ops = append(p.ops, attrOp{attr: c.name, kind: kind, av: av})
	case catalog.AggGroupUniqArray:
		arr, ok := v.([]any)
		if !ok {
			return valueMismatch(c.typ, v)
		}
		if len(arr) > 0 {
			p.sets = append(p.sets, memberSet{col: c, members: arr})
		}
	case catalog.AggGroupArray:
		arr, ok := v.([]any)
		if !ok {
			return valueMismatch(c.typ, v)
		}
		if len(arr) > 0 {
			av, err := encodeValue(c.typ, arr)
			if err != nil {
				return err
			}
			p.ops = append(p.ops, attrOp{attr: c.name, kind: opAppend, list: av.(*types.AttributeValueMemberL).Value})
		}
	}
	return nil
}

func isZeroNumber(v any) bool {
	switch x := v.(type) {
	case uint64:
		return x == 0
	case int64:
		return x == 0
	case float64:
		return x == 0 && !math.Signbit(x)
	}
	return false
}

// rowUpdate is the step-1 update of one item: the folded contributions of
// a group of rows of one key, in first-seen attribute order.
type rowUpdate struct {
	ops    []*attrOp
	byAttr map[string]*attrOp
	floats bool // has opAddFloat: written by compare-and-swap on attrCAS
}

func (u *rowUpdate) put(op attrOp) {
	cur := u.byAttr[op.attr]
	if cur == nil {
		c := op
		c.fs = append([]float64(nil), op.fs...)
		c.list = append([]types.AttributeValue(nil), op.list...)
		if op.n != nil {
			c.n = new(big.Int).Set(op.n)
		}
		u.byAttr[op.attr] = &c
		u.ops = append(u.ops, &c)
		u.floats = u.floats || op.kind == opAddFloat
		return
	}
	switch op.kind {
	case opSet:
		cur.av = op.av
	case opAddInt:
		cur.n.Add(cur.n, op.n)
	case opAddFloat:
		cur.fs = append(cur.fs, op.fs...)
	case opAppend:
		cur.list = append(cur.list, op.list...)
	}
	// opIfAbsent: the first row's value is kept.
}

// groupUpdate folds the step-1 contributions of rows, in order.
func groupUpdate(rows []*rowPlan) *rowUpdate {
	u := &rowUpdate{byAttr: map[string]*attrOp{}}
	for _, r := range rows {
		for _, op := range r.ops {
			u.put(op)
		}
	}
	return u
}

// contributionSize bounds the bytes the update adds to the item.
func (u *rowUpdate) contributionSize() int {
	n := 0
	for _, op := range u.ops {
		n += len(op.attr) + 1
		switch op.kind {
		case opSet, opIfAbsent:
			n += attrSize(op.av)
		case opAddInt, opAddFloat:
			n += 21
		case opAppend:
			for _, e := range op.list {
				n += attrSize(e) + 1
			}
		}
	}
	return n
}

// casState is what the step-1 writer of a group knows about the stored
// float sums: their attribute values and the CAS token (attrCAS), as read
// from the item or as last written by this writer.
type casState struct {
	loaded bool
	item   map[string]types.AttributeValue
}

// render builds the update of u (with the system attributes) over the
// stored float sums in cur. It returns the update, its condition (nil
// without float sums: integer parts use ADD, which needs none) and, with
// float sums, the attributes it writes (the next casState). Float sums are
// added in IEEE arithmetic, rounding each step to the column width like
// ClickHouse, and stored as encodeFloat stores plain columns (NaN, ±Inf
// and magnitudes outside N's range as S).
func (s *storeImpl) render(l *layout, u *rowUpdate, pk, sk []byte, marked bool, cur map[string]types.AttributeValue) (*expr, *string, map[string]types.AttributeValue, error) {
	e := newExpr()
	var next map[string]types.AttributeValue
	rid := e.n(attrRID)
	e.set = append(e.set, rid+"=if_not_exists("+rid+","+e.v(attrS(rowIDForKey(l.physical, pk, sk)))+")")
	for _, op := range u.ops {
		n := e.n(op.attr)
		switch op.kind {
		case opSet:
			e.set = append(e.set, n+"="+e.v(op.av))
		case opIfAbsent:
			e.set = append(e.set, n+"=if_not_exists("+n+","+e.v(op.av)+")")
		case opAddInt:
			if op.n.Sign() != 0 {
				e.add = append(e.add, n+" "+e.v(attrN(op.n.String())))
			}
		case opAddFloat:
			sum := 0.0
			if av := cur[op.attr]; av != nil {
				v, err := decodeValue(chtype.Type{Kind: chtype.Float64}, av)
				if err != nil {
					return nil, nil, nil, columnErr(l, op.attr, err)
				}
				sum = v.(float64)
			}
			for _, f := range op.fs {
				if op.bits == 32 {
					sum = float64(float32(sum) + float32(f))
				} else {
					sum += f
				}
			}
			av := encodeFloat(sum, op.bits)
			if next == nil {
				next = map[string]types.AttributeValue{}
			}
			next[op.attr] = av
			e.set = append(e.set, n+"="+e.v(av))
		case opAppend:
			e.set = append(e.set, n+"=list_append(if_not_exists("+n+","+e.v(&types.AttributeValueMemberL{Value: []types.AttributeValue{}})+"),"+
				e.v(&types.AttributeValueMemberL{Value: op.list})+")")
		}
	}
	e.set = append(e.set, e.n(attrTS)+"="+e.v(attrN(itoa64(s.stamp(1)))), e.n(attrOrigin)+"="+e.v(attrS(s.opts.Region)))
	if marked {
		e.set = append(e.set, e.n(attrMV)+"="+e.v(attrTrue))
	} else {
		e.remove = append(e.remove, e.n(attrMV))
	}
	if !u.floats {
		return e, nil, nil, nil
	}
	tok := attrS(hex.EncodeToString(randomBytes(8)))
	next[attrCAS] = tok
	c := e.n(attrCAS)
	e.set = append(e.set, c+"="+e.v(tok))
	var cond string
	if old := cur[attrCAS]; old != nil {
		cond = c + "=" + e.v(old)
	} else {
		cond = "attribute_not_exists(" + c + ")"
	}
	return e, aws.String(cond), next, nil
}

// readCAS reads the stored float sums and CAS token of an item (strongly
// consistent).
func (s *storeImpl) readCAS(ctx context.Context, l *layout, key map[string]types.AttributeValue, u *rowUpdate) (map[string]types.AttributeValue, error) {
	in := &dynamodb.GetItemInput{TableName: aws.String(l.physical), Key: key, ConsistentRead: aws.Bool(true)}
	e := newExpr()
	parts := []string{e.n(attrCAS)}
	for _, op := range u.ops {
		if op.kind == opAddFloat {
			parts = append(parts, e.n(op.attr))
		}
	}
	if proj := strings.Join(parts, ","); len(proj) <= maxExpressionLen {
		in.ProjectionExpression, in.ExpressionAttributeNames = aws.String(proj), e.nameMap()
	}
	var out *dynamodb.GetItemOutput
	if err := withRetry(ctx, func() (err error) {
		out, err = s.ddb.GetItem(ctx, in)
		return err
	}); err != nil {
		return nil, err
	}
	if out.Item == nil {
		return map[string]types.AttributeValue{}, nil
	}
	return out.Item, nil
}

// casApplied reports whether item already carries the token a write that
// failed its condition was going to set (the write landed, e.g. through an
// SDK-level retry of a request whose response was lost).
func casApplied(item, next map[string]types.AttributeValue) bool {
	a, ok1 := item[attrCAS].(*types.AttributeValueMemberS)
	b, ok2 := next[attrCAS].(*types.AttributeValueMemberS)
	return ok1 && ok2 && a.Value == b.Value
}

// applyGroup applies the rows of one key: (1) marker transactions carrying
// the folded update, (2) one idempotent extremum update per column, (3) the
// union of new set members, then marks the markers done (DESIGN.md §4).
func (s *storeImpl) applyGroup(ctx context.Context, l *layout, pk, sk []byte, rows []*rowPlan, marked bool) error {
	key := itemKey(pk, sk)
	cas := &casState{}
	var finish []*rowPlan // rows whose steps 2/3 must run
	var undone []*rowPlan // markers to mark done after steps 2/3
	if marked {
		for start := 0; start < len(rows); start += maxMarkersPerTx {
			chunk := rows[start:min(start+maxMarkersPerTx, len(rows))]
			applied, existing, err := s.applyMarked(ctx, l, pk, sk, key, chunk, cas)
			if err != nil {
				return err
			}
			for _, r := range applied {
				if r.finishing() {
					finish = append(finish, r)
					undone = append(undone, r)
				}
			}
			// Rows applied before: their steps 2/3 run again unless the
			// marker records that they finished (a crash may have stopped
			// a previous attempt between the steps).
			for _, r := range existing {
				if r.finishing() {
					finish = append(finish, r)
					undone = append(undone, r)
				}
			}
		}
	} else {
		if err := s.applyUnmarked(ctx, l, pk, sk, key, rows, cas); err != nil {
			return err
		}
		finish = rows
	}
	if err := s.finishGroup(ctx, l, pk, sk, key, finish); err != nil {
		return err
	}
	if len(undone) == 0 {
		return nil
	}
	reqs := make([]types.WriteRequest, len(undone))
	for i, r := range undone {
		reqs[i] = types.WriteRequest{PutRequest: &types.PutRequest{Item: s.markerItem(l, r.marker, true)}}
	}
	return s.writeChunks(ctx, s.markersTable(), reqs)
}

// markerItem is the exactly-once marker of a row application; done records
// that its steps 2/3 finished too.
func (s *storeImpl) markerItem(l *layout, id string, done bool) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{
		attrPK:          attrS(id),
		attrExp:         attrN(itoa64(s.now().Add(s.markerTTL()).Unix())),
		attrMarkerTable: attrS(l.markerTag()),
	}
	if done {
		m[attrMarkerDone] = attrTrue
	}
	return m
}

// applyMarked runs step 1 for a chunk of rows: one transaction with each
// row's marker Put (attribute_not_exists) and the update folded from the
// rows whose markers are written with it. A marker that exists cancels the
// transaction: its row was applied before (atomically with its marker) and
// is dropped from the retry, so the folded update always matches the
// markers written with it. A failed CAS condition (float sums changed
// concurrently) is retried over the stored values. It returns the rows
// applied now and the rows applied before whose marker is not done.
func (s *storeImpl) applyMarked(ctx context.Context, l *layout, pk, sk []byte, key map[string]types.AttributeValue, chunk []*rowPlan, cas *casState) (applied, existing []*rowPlan, err error) {
	pending := append([]*rowPlan(nil), chunk...)
	for attempt := 0; len(pending) > 0; {
		u := groupUpdate(pending)
		if u.floats && !cas.loaded {
			if cas.item, err = s.readCAS(ctx, l, key, u); err != nil {
				return nil, nil, err
			}
			cas.loaded = true
		}
		e, cond, next, err := s.render(l, u, pk, sk, true, cas.item)
		if err != nil {
			return nil, nil, err
		}
		items := make([]types.TransactWriteItem, 0, len(pending)+1)
		for _, r := range pending {
			items = append(items, types.TransactWriteItem{Put: &types.Put{
				TableName: aws.String(s.markersTable()), Item: s.markerItem(l, r.marker, !r.finishing()),
				ConditionExpression:                 aws.String("attribute_not_exists(#p)"),
				ExpressionAttributeNames:            map[string]string{"#p": attrPK},
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
			}})
		}
		upd := &types.Update{
			TableName: aws.String(l.physical), Key: key, UpdateExpression: e.updateExpression(),
			ExpressionAttributeNames: e.nameMap(), ExpressionAttributeValues: e.valueMap(), ConditionExpression: cond,
		}
		if cond != nil {
			upd.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
		}
		items = append(items, types.TransactWriteItem{Update: upd})
		err = withRetry(ctx, func() error {
			_, err := s.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
			return err
		})
		if err == nil {
			if next != nil {
				for a, av := range next {
					cas.item[a] = av
				}
			}
			return append(applied, pending...), existing, nil
		}
		var tce *types.TransactionCanceledException
		if !errors.As(err, &tce) || len(tce.CancellationReasons) != len(items) {
			return nil, nil, err
		}
		reasons := tce.CancellationReasons
		rest := pending[:0:0]
		dropped := false
		for i, r := range pending {
			switch aws.ToString(reasons[i].Code) {
			case "ConditionalCheckFailed":
				dropped = true
				if done, _ := reasons[i].Item[attrMarkerDone].(*types.AttributeValueMemberBOOL); done == nil || !done.Value {
					existing = append(existing, r)
				}
			case "None", "":
				rest = append(rest, r)
			default:
				return nil, nil, err
			}
		}
		ur := reasons[len(items)-1]
		switch aws.ToString(ur.Code) {
		case "None", "":
			if !dropped {
				return nil, nil, err
			}
		case "ConditionalCheckFailed":
			if cond == nil {
				return nil, nil, err
			}
			// The float sums changed since they were read: retry over
			// the stored values.
			if ur.Item != nil {
				cas.item = ur.Item
			} else if cas.item, err = s.readCAS(ctx, l, key, u); err != nil {
				return nil, nil, err
			}
			if !dropped {
				if attempt++; attempt >= maxAttempts*2 {
					return nil, nil, cherr.New(cherr.Unavailable, "too much contention updating a row of %s", l.t.FullName())
				}
				if err := sleepBackoff(ctx, attempt); err != nil {
					return nil, nil, err
				}
			}
		default:
			return nil, nil, err
		}
		pending = rest
	}
	return applied, existing, nil
}

// applyUnmarked runs step 1 of a direct INSERT (no markers): one update of
// the folded rows, conditioned on the CAS token when it carries float sums.
func (s *storeImpl) applyUnmarked(ctx context.Context, l *layout, pk, sk []byte, key map[string]types.AttributeValue, rows []*rowPlan, cas *casState) error {
	u := groupUpdate(rows)
	for attempt := 0; ; attempt++ {
		if u.floats && !cas.loaded {
			var err error
			if cas.item, err = s.readCAS(ctx, l, key, u); err != nil {
				return err
			}
			cas.loaded = true
		}
		e, cond, next, err := s.render(l, u, pk, sk, false, cas.item)
		if err != nil {
			return err
		}
		in := &dynamodb.UpdateItemInput{
			TableName: aws.String(l.physical), Key: key, UpdateExpression: e.updateExpression(),
			ExpressionAttributeNames: e.nameMap(), ExpressionAttributeValues: e.valueMap(), ConditionExpression: cond,
		}
		if cond != nil {
			in.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
		}
		err = withRetry(ctx, func() error {
			_, err := s.ddb.UpdateItem(ctx, in)
			return err
		})
		if err == nil {
			return nil
		}
		if cond == nil || !isConditionFailed(err) {
			return err
		}
		var ccf *types.ConditionalCheckFailedException
		errors.As(err, &ccf)
		if ccf.Item != nil {
			cas.item = ccf.Item
		} else if cas.item, err = s.readCAS(ctx, l, key, u); err != nil {
			return err
		}
		if casApplied(cas.item, next) {
			return nil
		}
		if attempt+1 >= maxAttempts*2 {
			return cherr.New(cherr.Unavailable, "too much contention updating a row of %s", l.t.FullName())
		}
		if err := sleepBackoff(ctx, attempt); err != nil {
			return err
		}
	}
}

// finishGroup runs steps 2 and 3 for rows of one key: the extremum
// candidates of each column folded in Go to one conditional write (the
// same rule the stored value is compared with), and the union of every
// row's set members in one member pass per column.
func (s *storeImpl) finishGroup(ctx context.Context, l *layout, pk, sk []byte, key map[string]types.AttributeValue, rows []*rowPlan) error {
	var extrema []*extremumOp
	byAttr := map[string]int{}
	var sets []memberSet
	setIdx := map[*colSpec]int{}
	for _, r := range rows {
		for i := range r.extrema {
			op := &r.extrema[i]
			j, ok := byAttr[op.cmpAttr]
			if !ok {
				byAttr[op.cmpAttr] = len(extrema)
				extrema = append(extrema, op)
				continue
			}
			if aggstate.ReplacesTyped(op.cmpVal, extrema[j].cmpVal, baseType(op.cmpType), !op.greater) {
				extrema[j] = op
			}
		}
		for _, ms := range r.sets {
			j, ok := setIdx[ms.col]
			if !ok {
				setIdx[ms.col] = len(sets)
				sets = append(sets, memberSet{col: ms.col, members: append([]any(nil), ms.members...)})
				continue
			}
			sets[j].members = append(sets[j].members, ms.members...)
		}
	}
	for _, op := range extrema {
		if err := s.applyExtremum(ctx, l, key, op); err != nil {
			return err
		}
	}
	for _, ms := range sets {
		if err := s.addMembers(ctx, l, pk, sk, ms); err != nil {
			return err
		}
	}
	return nil
}

func (s *storeImpl) markerTTL() time.Duration {
	return time.Duration(s.opts.MarkerTTLDays) * 24 * time.Hour
}

// nativeComparable: DynamoDB's own ordering of the stored attribute equals
// ClickHouse's for these kinds (always N). Other kinds may be stored as
// different attribute types (S/B strings, N/S floats) or are not ordered by
// DynamoDB (BOOL, L), so they use compare-and-swap.
func nativeComparable(t chtype.Type) bool {
	switch baseType(t).Kind {
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64,
		chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64,
		chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		return true
	}
	return false
}

func (s *storeImpl) applyExtremum(ctx context.Context, l *layout, key map[string]types.AttributeValue, op *extremumOp) error {
	op2 := " < "
	if !op.greater {
		op2 = " > "
	}
	update := func(e *expr) *string {
		for attr, av := range op.set {
			e.set = append(e.set, e.n(attr)+" = "+e.v(av))
		}
		return e.updateExpression()
	}
	if nativeComparable(op.cmpType) {
		e := newExpr()
		cond := "attribute_not_exists(" + e.n(op.existAttr) + ") OR " + e.n(op.cmpAttr) + op2 + e.v(op.cmpAV)
		in := &dynamodb.UpdateItemInput{TableName: aws.String(l.physical), Key: key, UpdateExpression: update(e),
			ConditionExpression: aws.String(cond), ExpressionAttributeNames: e.nameMap(), ExpressionAttributeValues: e.valueMap()}
		err := withRetry(ctx, func() error {
			_, err := s.ddb.UpdateItem(ctx, in)
			return err
		})
		if err != nil && !isConditionFailed(err) {
			return err
		}
		return nil
	}
	// Compare-and-swap: read the current extremum, compare in Go, write
	// conditioned on the stored value being unchanged.
	for attempt := 0; attempt < maxAttempts*2; attempt++ {
		get := &dynamodb.GetItemInput{
			TableName: aws.String(l.physical), Key: key, ConsistentRead: aws.Bool(true),
			ProjectionExpression:     aws.String("#c"),
			ExpressionAttributeNames: map[string]string{"#c": op.cmpAttr},
		}
		if op.existAttr != op.cmpAttr {
			get.ProjectionExpression = aws.String("#c, #e")
			get.ExpressionAttributeNames["#e"] = op.existAttr
		}
		var out *dynamodb.GetItemOutput
		if err := withRetry(ctx, func() (err error) {
			out, err = s.ddb.GetItem(ctx, get)
			return err
		}); err != nil {
			return err
		}
		e := newExpr()
		var cond string
		cur, present := out.Item[op.cmpAttr]
		if _, isNull := cur.(*types.AttributeValueMemberNULL); present && !isNull && hasMarker(out.Item, op) {
			curVal, err := decodeValue(op.cmpType, cur)
			if err != nil {
				return err
			}
			// Same rule as the in-memory merge (aggstate.ReplacesTyped):
			// Enum values compare by their numeric value, not their name;
			// a top-level NaN never displaces the stored extremum; nested
			// NULL/NaN elements never win (ClickHouse's compareAt hints).
			if !aggstate.ReplacesTyped(op.cmpVal, curVal, baseType(op.cmpType), !op.greater) {
				return nil
			}
			cond = e.n(op.cmpAttr) + " = " + e.v(cur)
		} else {
			cond = "attribute_not_exists(" + e.n(op.existAttr) + ")"
			if op.existAttr == op.cmpAttr {
				cond += " OR attribute_type(" + e.n(op.cmpAttr) + ", " + e.v(attrS("NULL")) + ")"
			}
		}
		in := &dynamodb.UpdateItemInput{TableName: aws.String(l.physical), Key: key, UpdateExpression: update(e),
			ConditionExpression: aws.String(cond), ExpressionAttributeNames: e.nameMap(), ExpressionAttributeValues: e.valueMap()}
		err := withRetry(ctx, func() error {
			_, err := s.ddb.UpdateItem(ctx, in)
			return err
		})
		if err == nil {
			return nil
		}
		if !isConditionFailed(err) {
			return err
		}
		if err := sleepBackoff(ctx, attempt); err != nil {
			return err
		}
	}
	return cherr.New(cherr.Unavailable, "too much contention updating %s of %s", op.cmpAttr, l.t.FullName())
}

// hasMarker reports whether an AggregateFunction state's c#h is set (always
// true for SimpleAggregateFunction columns, whose presence is the value).
func hasMarker(item map[string]types.AttributeValue, op *extremumOp) bool {
	if op.existAttr == op.cmpAttr {
		return true
	}
	b, ok := item[op.existAttr].(*types.AttributeValueMemberBOOL)
	return ok && b.Value
}

// addMembers inserts the set members missing from the members table and
// increments the row's c#z by the number actually inserted, so c#z is the
// exact set size even under concurrent and repeated merges.
func (s *storeImpl) addMembers(ctx context.Context, l *layout, pk, sk []byte, ms memberSet) error {
	c := ms.col
	mpk := memberPK(pk, sk, c.name)
	type member struct {
		sk   []byte
		item map[string]types.AttributeValue
		size int
	}
	var members []member
	seen := map[string]bool{}
	for _, v := range ms.members {
		msk, err := memberSK(c.memberType, v)
		if err != nil {
			return columnErr(l, c.name, err)
		}
		if seen[string(msk)] {
			continue
		}
		seen[string(msk)] = true
		av, err := encodeValue(c.memberType, v)
		if err != nil {
			return columnErr(l, c.name, err)
		}
		members = append(members, member{
			sk:   msk,
			item: map[string]types.AttributeValue{attrPK: attrB(mpk), attrSK: attrB(msk), attrMember: av},
			size: len(mpk) + len(msk) + attrSize(av) + 16,
		})
	}
	mt := membersTable(l.physical)

	// Pre-check which members already exist (cheap reads instead of
	// cancelled transactions).
	sks := make([][]byte, len(members))
	for i, m := range members {
		sks[i] = m.sk
	}
	existing, err := s.existingKeys(ctx, mt, mpk, sks)
	if err != nil {
		return err
	}
	missing := members[:0]
	for _, m := range members {
		if !existing[string(m.sk)] {
			missing = append(missing, m)
		}
	}

	rowKey := itemKey(pk, sk)
	const maxTxBytes = 3 << 20
	for start := 0; start < len(missing); {
		end, bytes := start, 0
		for end < len(missing) && end-start < memberTxChunk && (end == start || bytes+missing[end].size < maxTxBytes) {
			bytes += missing[end].size
			end++
		}
		chunk := append([]member(nil), missing[start:end]...)
		start = end
		for len(chunk) > 0 {
			items := make([]types.TransactWriteItem, 0, len(chunk)+1)
			for _, m := range chunk {
				items = append(items, types.TransactWriteItem{Put: &types.Put{
					TableName: aws.String(mt), Item: m.item,
					ConditionExpression:      aws.String("attribute_not_exists(#p)"),
					ExpressionAttributeNames: map[string]string{"#p": attrPK},
				}})
			}
			items = append(items, types.TransactWriteItem{Update: &types.Update{
				TableName: aws.String(l.physical), Key: rowKey,
				UpdateExpression:          aws.String("ADD #z :n"),
				ExpressionAttributeNames:  map[string]string{"#z": c.name + sufSize},
				ExpressionAttributeValues: map[string]types.AttributeValue{":n": attrN(strconv.Itoa(len(chunk)))},
			}})
			err := withRetry(ctx, func() error {
				_, err := s.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
				return err
			})
			if err == nil {
				break
			}
			// Members added concurrently cancel the transaction: drop them
			// and retry with the rest.
			reasons := cancelReasons(err)
			if len(reasons) != len(items) {
				return err
			}
			rest := chunk[:0]
			dropped := false
			for i, m := range chunk {
				switch reasons[i] {
				case "ConditionalCheckFailed":
					dropped = true
				case "None", "":
					rest = append(rest, m)
				default:
					return err
				}
			}
			if !dropped {
				return err
			}
			chunk = rest
		}
	}
	return nil
}

// existingKeys returns which sort keys exist under partition pk (BatchGetItem,
// strongly consistent).
func (s *storeImpl) existingKeys(ctx context.Context, table string, pk []byte, all [][]byte) (map[string]bool, error) {
	found := map[string]bool{}
	for start := 0; start < len(all); start += batchGetSize {
		var req []map[string]types.AttributeValue
		for _, k := range all[start:min(start+batchGetSize, len(all))] {
			req = append(req, itemKey(pk, k))
		}
		for attempt := 0; len(req) > 0; attempt++ {
			var out *dynamodb.BatchGetItemOutput
			err := withRetry(ctx, func() (err error) {
				out, err = s.ddb.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
					table: {Keys: req, ConsistentRead: aws.Bool(true), ProjectionExpression: aws.String("#s"),
						ExpressionAttributeNames: map[string]string{"#s": attrSK}},
				}})
				return err
			})
			if err != nil {
				return nil, err
			}
			for _, it := range out.Responses[table] {
				if b, ok := it[attrSK].(*types.AttributeValueMemberB); ok {
					found[string(b.Value)] = true
				}
			}
			req = out.UnprocessedKeys[table].Keys
			if len(req) > 0 {
				if attempt >= maxAttempts {
					return nil, cherr.New(cherr.Unavailable, "DynamoDB kept %d keys of %s unprocessed", len(req), table)
				}
				if err := sleepBackoff(ctx, attempt); err != nil {
					return nil, err
				}
			}
		}
	}
	return found, nil
}
