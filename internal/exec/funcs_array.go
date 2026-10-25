package exec

import (
	"sort"
	"strings"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/exec/aggstate"
)

// maxRangeElements bounds range() results.
const maxRangeElements = 100_000_000

func init() {
	reg(buildRange, "range")
	reg(buildArrayStringConcat, "arrayStringConcat")
	reg(buildArrayDistinct, "arrayDistinct")
	reg(arraySortBuilder(false), "arraySort")
	reg(arraySortBuilder(true), "arrayReverseSort")
	reg(buildArrayReverse, "arrayReverse")
	reg(buildArrayConcat, "arrayConcat")
	reg(buildArraySlice, "arraySlice")
	reg(buildArrayCount, "arrayCount")
	reg(arrayReduceBuilder("sum"), "arraySum")
	reg(arrayReduceBuilder("min"), "arrayMin")
	reg(arrayReduceBuilder("max"), "arrayMax")
	reg(arrayReduceBuilder("avg"), "arrayAvg")
	reg(buildArrayUniq, "arrayUniq")
	regCI(sideBuilder(true, false), "left")
	regCI(sideBuilder(false, false), "right")
	reg(sideBuilder(true, true), "leftUTF8")
	reg(sideBuilder(false, true), "rightUTF8")
	regCI(buildRepeat, "repeat")
	for name, t := range map[string]chtype.Type{
		"emptyArrayString": chtype.TString, "emptyArrayUInt8": chtype.TUInt8, "emptyArrayUInt64": chtype.TUInt64,
		"emptyArrayInt64": chtype.TInt64, "emptyArrayFloat64": chtype.TFloat64,
	} {
		at := chtype.NewArray(t)
		reg(func(c *compiler, name string, args []*expr) (*expr, error) {
			if err := wantArgs(name, args, 0, 0); err != nil {
				return nil, err
			}
			return constant(at, []any{}), nil
		}, name)
	}
}

func requireArray(name string, args []*expr, positions ...int) error {
	for _, p := range positions {
		if p < len(args) && base(args[p]).Kind != chtype.Array && !isNothing(args[p].t) {
			return illegalType(name, args[p].t, p+1)
		}
	}
	return nil
}

func elemType(x *expr) chtype.Type {
	t := base(x)
	if t.Kind == chtype.Array && t.Elem != nil {
		return *t.Elem
	}
	return chtype.TNothing
}

func buildRange(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 3); err != nil {
		return nil, err
	}
	ts := make([]chtype.Type, len(args))
	for i, a := range args {
		if !isIntKind(base(a).Kind) {
			return nil, illegalType(name, a.t, i+1)
		}
		ts[i] = base(a)
	}
	et, err := chtype.Supertype(ts...)
	if err != nil {
		return nil, err
	}
	return strict(args, chtype.NewArray(et), func(v []any) (any, error) {
		start, end, step := int64(0), asInt(v[0]), int64(1)
		if len(v) >= 2 {
			start, end = asInt(v[0]), asInt(v[1])
		}
		if len(v) == 3 {
			step = asInt(v[2])
		}
		if step <= 0 {
			return nil, cherr.New(cherr.BadArguments, "A call to function %s overflows, step must be positive", name)
		}
		if end > start && (uint64(end)-uint64(start))/uint64(step) > maxRangeElements {
			return nil, cherr.New(cherr.BadArguments, "A call to function %s overflows, investigate the values of arguments you are passing", name)
		}
		out := []any{}
		for x := start; x < end; x += step {
			out = append(out, aggstate.Wrap(et, x))
		}
		return out, nil
	}), nil
}

func buildArrayStringConcat(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 2); err != nil {
		return nil, err
	}
	if err := requireArray(name, args, 0); err != nil {
		return nil, err
	}
	if err := requireString(name, args, 1); err != nil {
		return nil, err
	}
	et := elemType(args[0]).Unwrap()
	return strict(args, chtype.TString, func(v []any) (any, error) {
		sep := ""
		if len(v) == 2 {
			sep = v[1].(string)
		}
		var parts []string
		for _, e := range v[0].([]any) {
			if e != nil {
				parts = append(parts, valueToString(c.q, e, et))
			}
		}
		return strings.Join(parts, sep), nil
	}), nil
}

func buildArrayDistinct(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	if err := requireArray(name, args, 0); err != nil {
		return nil, err
	}
	rt := chtype.NewArray(removeNullable(elemType(args[0])))
	return strict(args, rt, func(v []any) (any, error) {
		var out []any
		seen := map[string]bool{}
		for _, e := range v[0].([]any) {
			if e == nil {
				continue
			}
			k := aggstate.Key(e)
			if !seen[k] {
				seen[k] = true
				out = append(out, e)
			}
		}
		if out == nil {
			out = []any{}
		}
		return out, nil
	}), nil
}

func arraySortBuilder(desc bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 1); err != nil {
			return nil, err
		}
		if err := requireArray(name, args, 0); err != nil {
			return nil, err
		}
		var key func(any) any
		if et := base(args[0]).Elem; et != nil {
			key = aggstate.OrderKey(*et)
		}
		return strict(args, base(args[0]), func(v []any) (any, error) {
			out := append([]any{}, v[0].([]any)...)
			if key == nil {
				sort.SliceStable(out, func(i, j int) bool { return compareOrder(out[i], out[j], desc, false) < 0 })
				return out, nil
			}
			// Enum elements sort by value (ClickHouse).
			keys := make([]any, len(out))
			for i, x := range out {
				if x != nil {
					keys[i] = key(x)
				}
			}
			idx := make([]int, len(out))
			for i := range idx {
				idx[i] = i
			}
			sort.SliceStable(idx, func(i, j int) bool { return compareOrder(keys[idx[i]], keys[idx[j]], desc, false) < 0 })
			sorted := make([]any, len(out))
			for i, k := range idx {
				sorted[i] = out[k]
			}
			return sorted, nil
		}), nil
	}
}

func buildArrayReverse(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	if err := requireArray(name, args, 0); err != nil {
		return nil, err
	}
	return strict(args, base(args[0]), func(v []any) (any, error) {
		in := v[0].([]any)
		out := make([]any, len(in))
		for i, e := range in {
			out[len(in)-1-i] = e
		}
		return out, nil
	}), nil
}

func buildArrayConcat(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, -1); err != nil {
		return nil, err
	}
	ts := make([]chtype.Type, len(args))
	for i, a := range args {
		if err := requireArray(name, args, i); err != nil {
			return nil, err
		}
		ts[i] = elemType(a)
	}
	et, err := chtype.Supertype(ts...)
	if err != nil {
		return nil, supertypeError(err, "No common type for arguments of function %s", name)
	}
	convs := make([]func(any) (any, error), len(args))
	for i := range args {
		if convs[i], err = converter(ts[i], et); err != nil {
			return nil, err
		}
	}
	return strict(args, chtype.NewArray(et), func(v []any) (any, error) {
		out := []any{}
		for i, a := range v {
			for _, e := range a.([]any) {
				if e != nil {
					var err error
					if e, err = convs[i](e); err != nil {
						return nil, err
					}
				}
				out = append(out, e)
			}
		}
		return out, nil
	}), nil
}

func buildArraySlice(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 3); err != nil {
		return nil, err
	}
	if err := requireArray(name, args, 0); err != nil {
		return nil, err
	}
	for i := 1; i < len(args); i++ {
		if !isIntKind(base(args[i]).Kind) {
			return nil, illegalType(name, args[i].t, i+1)
		}
	}
	return strict(args, base(args[0]), func(v []any) (any, error) {
		arr := v[0].([]any)
		start, end := substringBounds(len(arr), asInt(v[1]), v)
		return append([]any{}, arr[start:end]...), nil
	}), nil
}

func buildArrayCount(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	if err := requireArray(name, args, 0); err != nil {
		return nil, err
	}
	return strict(args, chtype.TUInt32, func(v []any) (any, error) {
		n := uint64(0)
		for _, e := range v[0].([]any) {
			if e == nil {
				continue
			}
			if ok, err := truth(e); err == nil && ok {
				n++
			}
		}
		return n, nil
	}), nil
}

// arrayReduceBuilder implements arraySum/Min/Max/Avg with the result types
// of the corresponding aggregates.
func arrayReduceBuilder(fn string) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 1); err != nil {
			return nil, err
		}
		if err := requireArray(name, args, 0); err != nil {
			return nil, err
		}
		et := elemType(args[0])
		if et.IsNullable() || (fn == "sum" || fn == "avg") && !isNumKind(et.Unwrap().Kind) {
			return nil, illegalType(name, args[0].t, 1)
		}
		call, err := newAggCall(c.q.ctx, aggSpec{base: fn}, name, []*expr{{t: et}})
		if err != nil {
			return nil, err
		}
		rt := removeNullable(call.final)
		return strict(args, rt, func(v []any) (any, error) {
			st := call.newAcc()
			for _, e := range v[0].([]any) {
				if e == nil {
					continue
				}
				if err := call.add(st, []any{e}); err != nil {
					return nil, err
				}
			}
			r, err := call.result(st)
			if err != nil || r != nil {
				return r, err
			}
			return zeroValue(rt), nil
		}), nil
	}
}

func buildArrayUniq(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	if err := requireArray(name, args, 0); err != nil {
		return nil, err
	}
	return strict(args, chtype.TUInt32, func(v []any) (any, error) {
		seen := map[string]bool{}
		for _, e := range v[0].([]any) {
			if e != nil {
				seen[aggstate.Key(e)] = true
			}
		}
		return uint64(len(seen)), nil
	}), nil
}

// sideBuilder implements left/right (bytes) and leftUTF8/rightUTF8.
func sideBuilder(leftSide, utf bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, 2); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0); err != nil {
			return nil, err
		}
		if !isIntKind(base(args[1]).Kind) {
			return nil, illegalType(name, args[1].t, 2)
		}
		return strict(args, chtype.TString, func(v []any) (any, error) {
			s := v[0].(string)
			var units []string
			if utf {
				for _, r := range s {
					units = append(units, string(r))
				}
			} else {
				for i := 0; i < len(s); i++ {
					units = append(units, s[i:i+1])
				}
			}
			n, l := int(asInt(v[1])), len(units)
			if n < 0 {
				n = max(0, l+n)
			}
			n = min(n, l)
			if leftSide {
				return strings.Join(units[:n], ""), nil
			}
			return strings.Join(units[l-n:], ""), nil
		}), nil
	}
}

func buildRepeat(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	if err := requireString(name, args, 0); err != nil {
		return nil, err
	}
	if !isIntKind(base(args[1]).Kind) {
		return nil, illegalType(name, args[1].t, 2)
	}
	return strict(args, chtype.TString, func(v []any) (any, error) {
		n := asInt(v[1])
		if n <= 0 {
			return "", nil
		}
		s := v[0].(string)
		if int64(len(s))*n > 1<<30 {
			return nil, cherr.New(cherr.BadArguments, "Too large result of function %s", name)
		}
		return strings.Repeat(s, int(n)), nil
	}), nil
}

func init() {
	reg(buildMap, "map")
	reg(mapPartBuilder(0), "mapKeys")
	reg(mapPartBuilder(1), "mapValues")
	reg(buildMapContains, "mapContains")
}

func buildMap(c *compiler, name string, args []*expr) (*expr, error) {
	if len(args)%2 != 0 {
		return nil, argCountError(name, len(args), "an even number")
	}
	if len(args) == 0 {
		return constant(chtype.Type{Kind: chtype.Map, Key: &chtype.TNothing, Elem: &chtype.TNothing}, []any{}), nil
	}
	var keys, vals []*expr
	for i := 0; i < len(args); i += 2 {
		keys = append(keys, args[i])
		vals = append(vals, args[i+1])
	}
	keys, kt, err := unify(name, keys)
	if err != nil {
		return nil, err
	}
	vals, vt, err := unify(name, vals)
	if err != nil {
		return nil, err
	}
	x := &expr{t: chtype.Type{Kind: chtype.Map, Key: &kt, Elem: &vt}, fn: func(row []any) (any, error) {
		out := make([]any, 0, len(keys))
		for i := range keys {
			k, err := keys[i].eval(row)
			if err != nil {
				return nil, err
			}
			v, err := vals[i].eval(row)
			if err != nil {
				return nil, err
			}
			out = append(out, []any{k, v})
		}
		return out, nil
	}}
	return fold(x, args), nil
}

func mapPartBuilder(part int) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 1); err != nil {
			return nil, err
		}
		mt := base(args[0])
		if mt.Kind != chtype.Map {
			return nil, illegalType(name, args[0].t, 1)
		}
		et := *mt.Key
		if part == 1 {
			et = *mt.Elem
		}
		return strict(args, chtype.NewArray(et), func(v []any) (any, error) {
			pairs := v[0].([]any)
			out := make([]any, len(pairs))
			for i, p := range pairs {
				out[i] = p.([]any)[part]
			}
			return out, nil
		}), nil
	}
}

func buildMapContains(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	mt := base(args[0])
	if mt.Kind != chtype.Map {
		return nil, illegalType(name, args[0].t, 1)
	}
	key, err := castExpr(args[1], *mt.Key, false)
	if err != nil {
		return nil, err
	}
	return strict([]*expr{args[0], key}, chtype.TUInt8, func(v []any) (any, error) {
		for _, p := range v[0].([]any) {
			if chtype.Compare(p.([]any)[0], v[1]) == 0 {
				return uint64(1), nil
			}
		}
		return uint64(0), nil
	}), nil
}
