package exec

import (
	"context"
	"strings"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/memtrack"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// Supported aggregate base functions. Case-insensitive ones are listed in
// aggCI (matched only without combinators, like ClickHouse).
var aggBases = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true, "any": true, "anyLast": true,
	"argMin": true, "argMax": true, "uniq": true, "uniqExact": true, "groupArray": true, "groupUniqArray": true,
}

var aggCI = map[string]string{"count": "count", "sum": "sum", "avg": "avg", "min": "min", "max": "max", "any": "any"}

// Aggregates ClickHouse knows that this engine does not implement: they are
// recognized (so errors are NOT_IMPLEMENTED rather than UNKNOWN_FUNCTION).
var aggUnsupported = map[string]bool{
	"quantile": true, "quantiles": true, "quantileExact": true, "quantileTiming": true, "quantileTDigest": true,
	"median": true, "medianExact": true, "stddevPop": true, "stddevSamp": true, "varPop": true, "varSamp": true,
	"topK": true, "uniqCombined": true, "uniqCombined64": true, "uniqHLL12": true, "uniqTheta": true,
	"sumMap": true, "groupBitmap": true, "groupBitAnd": true, "groupBitOr": true, "groupBitXor": true,
	"covarPop": true, "covarSamp": true, "corr": true, "first_value": true, "last_value": true,
	"anyHeavy": true, "sumWithOverflow": true, "sumKahan": true, "avgWeighted": true, "groupArrayInsertAt": true,
	"groupArrayMovingSum": true, "entropy": true, "simpleLinearRegression": true, "retention": true,
	"windowFunnel": true, "sequenceMatch": true, "sequenceCount": true, "histogram": true, "deltaSum": true,
	"stddev": true, "variance": true, "countDistinct": true, "groupConcat": true, "group_concat": true,
}

// aggSpec is a parsed aggregate function name: base plus combinators.
//
// ClickHouse combinators nest: in sumIfState the -If is part of the state
// (type AggregateFunction(sumIf, T, UInt8)), in sumStateIf it filters the
// rows feeding an AggregateFunction(sum, T) state.
type aggSpec struct {
	base string
	// ifComb is an outer -If: a row filter whose condition is the last argument.
	ifComb bool
	// stateIf is an -If inside -State/-Merge: part of the state function name.
	stateIf     bool
	state       bool
	merge       bool
	unsupported string // non-empty: recognized but not implemented (reason)
}

// stateFunc is the AggregateFunction name of the spec's state.
func (s aggSpec) stateFunc() string {
	if s.stateIf {
		return s.base + "If"
	}
	return s.base
}

func (s aggSpec) canonical() string {
	n := s.stateFunc()
	if s.merge {
		n += "Merge"
	}
	if s.state {
		n += "State"
	}
	if s.ifComb {
		n += "If"
	}
	return n
}

func isAggregateName(name string) bool {
	_, ok := parseAggName(name)
	return ok
}

var aggSuffixes = []string{"MergeState", "SimpleState", "OrDefault", "Distinct", "Resample", "ForEach", "OrNull", "State", "Merge", "Array", "Map", "If"}

// parseAggName parses names like "uniqExactMerge", "countIf", "sumIfState",
// stripping combinators from the outermost (last) one inwards.
func parseAggName(name string) (aggSpec, bool) {
	var spec aggSpec
	rest := name
	inState := false // a -State/-Merge combinator encloses what remains
	for {
		if aggBases[rest] {
			spec.base = rest
			return spec, true
		}
		if b, ok := aggCI[strings.ToLower(rest)]; ok && rest == name {
			spec.base = b
			return spec, true
		}
		if aggUnsupported[rest] {
			spec.base = rest
			spec.unsupported = "aggregate function " + rest + " is not supported"
			return spec, true
		}
		suf := ""
		for _, x := range aggSuffixes {
			if strings.HasSuffix(rest, x) && len(rest) > len(x) {
				suf = x
				break
			}
		}
		if suf == "" {
			return aggSpec{}, false
		}
		rest = rest[:len(rest)-len(suf)]
		switch suf {
		case "If":
			switch {
			case inState && !spec.stateIf:
				spec.stateIf = true
			case !inState && !spec.ifComb:
				spec.ifComb = true
			default:
				return aggSpec{}, false
			}
		case "State":
			if spec.state || spec.merge {
				return aggSpec{}, false
			}
			spec.state, inState = true, true
		case "Merge":
			if spec.merge {
				return aggSpec{}, false
			}
			spec.merge, inState = true, true
		case "MergeState":
			if spec.state || spec.merge {
				return aggSpec{}, false
			}
			spec.merge, spec.state, inState = true, true, true
		default:
			if spec.unsupported == "" {
				spec.unsupported = "aggregate combinator -" + suf + " is not supported"
			}
		}
	}
}

// stateBase maps an AggregateFunction name to the canonical function of its
// state ("sumIf" -> "sum": an -If state is the base function's state).
func stateBase(f string) string { return aggstate.StateBase(f) }

// aggResultType returns the finalized result type of base over argTypes.
func aggResultType(base string, argTypes []chtype.Type) (chtype.Type, error) {
	nullable := false
	for _, t := range argTypes {
		if t.IsNullable() {
			nullable = true
		}
	}
	need := func(n int) error {
		if len(argTypes) != n {
			return cherr.New(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires %d argument(s), got %d", base, n, len(argTypes))
		}
		return nil
	}
	switch base {
	case "count":
		return chtype.TUInt64, nil
	case "uniq", "uniqExact":
		if len(argTypes) == 0 {
			return chtype.Type{}, cherr.New(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires at least one argument", base)
		}
		return chtype.TUInt64, nil
	case "sum", "avg":
		if err := need(1); err != nil {
			return chtype.Type{}, err
		}
		u := argTypes[0].Unwrap()
		var rt chtype.Type
		switch {
		case !isNumKind(u.Kind):
			return chtype.Type{}, cherr.New(cherr.IllegalTypeOfArgument, "Illegal type %s of argument for aggregate function %s", argTypes[0].String(), base)
		case base == "avg" || isFloatKind(u.Kind):
			rt = chtype.TFloat64
		case isSignedKind(u.Kind):
			rt = chtype.TInt64
		default:
			rt = chtype.TUInt64
		}
		return nullableIf(rt, nullable), nil
	case "min", "max", "any", "anyLast":
		if err := need(1); err != nil {
			return chtype.Type{}, err
		}
		return nullableIf(valueType(argTypes[0]), nullable), nil
	case "argMin", "argMax":
		if err := need(2); err != nil {
			return chtype.Type{}, err
		}
		return nullableIf(valueType(argTypes[0]), nullable), nil
	case "groupArray", "groupUniqArray":
		if err := need(1); err != nil {
			return chtype.Type{}, err
		}
		return chtype.NewArray(valueType(removeNullable(argTypes[0]))), nil
	}
	return chtype.Type{}, cherr.New(cherr.NotImplemented, "aggregate function %s is not supported", base)
}

// valueType strips LowCardinality but keeps Nullable and
// SimpleAggregateFunction (min/max/any over a SimpleAggregateFunction column
// keep its type in ClickHouse).
func valueType(t chtype.Type) chtype.Type {
	switch t.Kind {
	case chtype.LowCardinality:
		return valueType(*t.Elem)
	case chtype.Nullable:
		return nullableIf(valueType(*t.Elem), true)
	}
	return t
}

// aggCall is one aggregate function application.
type aggCall struct {
	spec     aggSpec
	name     string
	args     []*expr
	cond     *expr
	argTypes []chtype.Type // state argument types
	t        chtype.Type   // output type
	final    chtype.Type   // finalized result type
	// sumType is the canonical kind of sum accumulators.
	sumType chtype.Type
	// sized is set when the values of its arguments differ in size, so
	// replacing a kept value changes the memory its state holds; kept is
	// what a new state is charged for the values it keeps otherwise.
	sized bool
	kept  int64
	ctx   context.Context
}

// aggContext collects GROUP BY keys and aggregate calls of one SELECT.
type aggContext struct {
	keys   []*expr
	keyFP  map[string]int
	calls  []*aggCall
	callFP map[string]int
}

func newAggContext() *aggContext {
	return &aggContext{keyFP: map[string]int{}, callFP: map[string]int{}}
}

func (a *aggContext) register(c *compiler, n *sqlparse.FuncCall, fp string) (int, error) {
	if i, ok := a.callFP[fp]; ok {
		return i, nil
	}
	spec, _ := parseAggName(n.Name)
	if n.Distinct {
		if spec.base != "count" || spec.ifComb || spec.state || spec.merge {
			return 0, cherr.New(cherr.NotImplemented, "DISTINCT is supported only in count(DISTINCT ...)")
		}
		spec.base = "uniqExact"
	}
	if spec.unsupported != "" {
		return 0, cherr.New(cherr.NotImplemented, "%s", spec.unsupported)
	}
	if n.Params != nil {
		return 0, cherr.New(cherr.NotImplemented, "parametric aggregate function %s is not supported", n.Name)
	}
	sub := c.child()
	sub.agg = nil
	sub.clause = "aggregate function arguments"
	var args []*expr
	for _, ae := range n.Args {
		if _, star := ae.(*sqlparse.Star); star && spec.base == "count" {
			continue
		}
		if hasAggregate(ae) {
			return 0, cherr.New(illegalAggregation, "Aggregate function %s is found inside another aggregate function in query", sqlparse.ColumnName(ae))
		}
		x, err := sub.compile(ae)
		if err != nil {
			return 0, err
		}
		args = append(args, x)
	}
	call, err := newAggCall(c.q.ctx, spec, n.Name, args)
	if err != nil {
		return 0, err
	}
	a.calls = append(a.calls, call)
	a.callFP[fp] = len(a.calls) - 1
	return len(a.calls) - 1, nil
}

// newAggCall type-checks an aggregate application.
func newAggCall(ctx context.Context, spec aggSpec, name string, args []*expr) (*aggCall, error) {
	call := &aggCall{spec: spec, name: name, ctx: ctx}
	if spec.ifComb || (spec.stateIf && !spec.merge) {
		if len(args) == 0 {
			return nil, cherr.New(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires a condition argument", name)
		}
		call.cond = args[len(args)-1]
		if err := checkCondType(name, call.cond, len(args)); err != nil {
			return nil, err
		}
		args = args[:len(args)-1]
	}
	call.args = args
	var stateType chtype.Type
	if spec.merge {
		if len(args) != 1 {
			return nil, cherr.New(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires exactly one argument", name)
		}
		at := args[0].t.Unwrap()
		if at.Kind != chtype.AggregateFunction || at.AggFunc != spec.stateFunc() {
			return nil, cherr.New(cherr.IllegalTypeOfArgument, "Illegal type %s of argument for aggregate function %s, expected AggregateFunction(%s, ...)", args[0].t.String(), name, spec.stateFunc())
		}
		stateType = at
		call.argTypes = at.AggArgs
		if spec.stateIf {
			if len(at.AggArgs) == 0 {
				return nil, cherr.New(cherr.IllegalTypeOfArgument, "Illegal type %s of argument for aggregate function %s", args[0].t.String(), name)
			}
			call.argTypes = at.AggArgs[:len(at.AggArgs)-1]
		}
	} else {
		for _, a := range args {
			call.argTypes = append(call.argTypes, removeLowCardinality(a.t))
		}
		if spec.base == "count" && len(args) > 1 {
			return nil, cherr.New(cherr.NumberOfArgsDoesntMatch, "Aggregate function count requires at most one argument")
		}
		stateArgs := append([]chtype.Type(nil), call.argTypes...)
		if spec.stateIf {
			stateArgs = append(stateArgs, removeLowCardinality(call.cond.t))
		}
		stateType = chtype.Type{Kind: chtype.AggregateFunction, AggFunc: spec.stateFunc(), AggArgs: stateArgs}
	}
	argTypes := call.argTypes
	if !spec.merge {
		// Result types follow the original (LowCardinality-free) arguments.
		argTypes = make([]chtype.Type, len(args))
		for i, a := range args {
			argTypes[i] = removeLowCardinality(a.t)
		}
	}
	final, err := aggResultType(spec.base, argTypes)
	if err != nil {
		return nil, err
	}
	call.final = final
	call.t = final
	if spec.state {
		call.t = stateType
	}
	if spec.base == "sum" {
		call.sumType = removeNullable(final)
	}
	call.sized = !fixedSize(call.argTypes)
	call.kept = call.fixedKeptBytes()
	return call, nil
}

// skipsNulls reports whether NULL arguments skip the row (ClickHouse's Null
// combinator); count() without arguments counts every row.
func (a *aggCall) skipsNulls() bool { return !a.spec.merge }

// acc is the running state of one aggregate in one group.
type acc struct {
	p       *chtype.AggPartial
	members map[string]struct{}
	// pending holds the first merged set state while it is the only one, so
	// a single stored state can be finalized from SetSize without loading.
	pending *chtype.AggPartial
	merged  int
	// mem is charged what the state grows by (nil: not tracked); bytes is
	// how much that is so far.
	mem   *memtrack.Account
	bytes int64
}

func (a *aggCall) newAcc() *acc {
	return &acc{p: aggstate.New(a.spec.base, a.argTypes)}
}

// feed adds one input row to the accumulator.
func (a *aggCall) feed(st *acc, row []any) error {
	if a.cond != nil {
		v, err := a.cond.eval(row)
		if err != nil {
			return err
		}
		if v == nil {
			return nil
		}
		ok, err := truth(v)
		if err != nil || !ok {
			return err
		}
	}
	var vals [2]any
	vs := vals[:0]
	if len(a.args) > 2 {
		vs = make([]any, 0, len(a.args))
	}
	for _, x := range a.args {
		v, err := x.eval(row)
		if err != nil {
			return err
		}
		if v == nil && a.skipsNulls() {
			return nil
		}
		vs = append(vs, v)
	}
	if a.spec.merge {
		p, _ := vs[0].(*chtype.AggPartial)
		return a.mergeState(st, p)
	}
	return a.add(st, vs)
}

// argType is the type of the i-th argument (the zero Type when unknown).
func (a *aggCall) argType(i int) chtype.Type {
	if i < len(a.argTypes) {
		return a.argTypes[i]
	}
	if i < len(a.args) {
		return a.args[i].t
	}
	return chtype.Type{}
}

func (a *aggCall) add(st *acc, vs []any) error {
	p := st.p
	switch a.spec.base {
	case "count":
		p.Count++
	case "sum":
		p.Sum = aggstate.Wrap(a.sumType, aggstate.AddNumbers(p.Sum, toSumKind(vs[0], a.sumType)))
		p.HasValue = true
	case "avg":
		p.Sum = asFloat(p.Sum) + asFloat(vs[0])
		p.Count++
	case "min", "max":
		if !p.HasValue || aggstate.ReplacesTyped(vs[0], p.Value, a.argType(0), a.spec.base == "min") {
			return a.keep(st, vs[0], nil)
		}
	case "any":
		if !p.HasValue {
			return a.keep(st, vs[0], nil)
		}
	case "anyLast":
		return a.keep(st, vs[0], nil)
	case "argMin", "argMax":
		if !p.HasValue || aggstate.ReplacesTyped(vs[1], p.Key, a.argType(1), a.spec.base == "argMin") {
			return a.keep(st, vs[0], vs[1])
		}
	case "uniq", "uniqExact", "groupUniqArray":
		var m any = vs[0]
		if len(vs) > 1 {
			m = append([]any(nil), vs...)
		}
		return st.addMember(m)
	case "groupArray":
		return st.appendList(vs[0])
	default:
		return cherr.New(cherr.NotImplemented, "aggregate function %s is not supported", a.spec.base)
	}
	return nil
}

// addMember adds m to the set unless it is a member already.
func (st *acc) addMember(m any) error {
	if st.members == nil {
		st.members = map[string]struct{}{}
		if err := st.charge(newSet); err != nil {
			return err
		}
	}
	k := aggstate.Key(m)
	if _, ok := st.members[k]; ok {
		return nil
	}
	st.members[k] = struct{}{}
	if st.mem == nil {
		st.p.Set = append(st.p.Set, m)
		return nil
	}
	if err := st.charge(keyEntry(setEntry, len(k))); err != nil {
		return err
	}
	var err error
	st.p.Set, err = st.appendValue(st.p.Set, m)
	return err
}

func toSumKind(v any, t chtype.Type) any {
	switch {
	case isFloatKind(t.Kind):
		return asFloat(v)
	case isSignedKind(t.Kind):
		return asInt(v)
	}
	return asUint(v)
}

// mergeState merges an input partial state (the -Merge combinator).
func (a *aggCall) mergeState(st *acc, in *chtype.AggPartial) error {
	if in == nil {
		return nil
	}
	if in.Func != a.spec.base {
		return cherr.New(cherr.TypeMismatch, "cannot merge state of %s in %s", in.Func, a.name)
	}
	if !aggstate.IsSetFunc(a.spec.base) {
		return a.mergeValues(st, in)
	}
	st.merged++
	if st.merged == 1 && !in.SetLoaded {
		st.pending = in
		if st.mem == nil {
			return nil
		}
		return st.charge(memtrack.Value(in))
	}
	if err := st.flushPending(a.ctx); err != nil {
		return err
	}
	m, err := aggstate.Members(a.ctx, in)
	if err != nil {
		return err
	}
	return st.addMembers(m)
}

func (st *acc) flushPending(ctx context.Context) error {
	p := st.pending
	if p == nil {
		return nil
	}
	held := int64(0)
	if st.mem != nil {
		// What mergeState charged, before loading changes it.
		held = memtrack.Value(p)
	}
	m, err := aggstate.Members(ctx, p)
	if err != nil {
		return err
	}
	st.pending = nil
	if err := st.addMembers(m); err != nil {
		return err
	}
	return st.charge(-held)
}

// load loads the stored set members that result will need. It is the part
// of result that can fail.
func (a *aggCall) load(st *acc) error {
	if !aggstate.IsSetFunc(a.spec.base) || a.countsPending(st) {
		return nil
	}
	return st.flushPending(a.ctx)
}

// countsPending reports whether result counts a single stored set state
// without loading its members.
func (a *aggCall) countsPending(st *acc) bool {
	return st.pending != nil && !a.spec.state && a.spec.base != "groupUniqArray"
}

// result returns the aggregate's output value for a group.
func (a *aggCall) result(st *acc) (any, error) {
	p := st.p
	if aggstate.IsSetFunc(a.spec.base) {
		if a.countsPending(st) {
			return aggstate.SetCount(st.pending), nil
		}
		if err := st.flushPending(a.ctx); err != nil {
			return nil, err
		}
		p.SetSize = uint64(len(p.Set))
		p.SetLoaded = true
	}
	if a.spec.state {
		return aggstate.Clone(p), nil
	}
	return aggstate.Finalize(a.ctx, p, a.final)
}

// removeLowCardinality strips LowCardinality wrappers at any depth
// (Array elements, Tuple fields, Map keys/values), keeping Nullable, like
// ClickHouse's recursiveRemoveLowCardinality on aggregate argument types.
func removeLowCardinality(t chtype.Type) chtype.Type {
	switch t.Kind {
	case chtype.LowCardinality:
		return removeLowCardinality(*t.Elem)
	case chtype.Nullable, chtype.Array:
		e := removeLowCardinality(*t.Elem)
		t.Elem = &e
	case chtype.Map:
		k, v := removeLowCardinality(*t.Key), removeLowCardinality(*t.Elem)
		t.Key, t.Elem = &k, &v
	case chtype.Tuple:
		fs := make([]chtype.Field, len(t.Fields))
		for i, f := range t.Fields {
			fs[i] = chtype.Field{Name: f.Name, Type: removeLowCardinality(f.Type)}
		}
		t.Fields = fs
	}
	return t
}
