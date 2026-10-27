package chtype

import "context"

// AggPartial is the value of an AggregateFunction(f, ...) column: a partial
// aggregation state. It is the CONTRACT between the query engine (which
// produces and merges states) and storage (which persists them).
//
// Which fields are meaningful depends on Func (canonical, combinator-free
// aggregate name):
//
//	count            Count
//	sum              Sum (uint64 | int64 | float64, matching sum's result type)
//	avg              Sum (float64) and Count
//	min, max         Value (+ HasValue)
//	any, anyLast     Value (+ HasValue)
//	argMin, argMax   Value (the returned arg) and Key (the compared key) (+ HasValue)
//	uniq, uniqExact  Set (distinct members) and/or SetSize + LoadSet
//	groupUniqArray   Set
//	groupArray       List
//
// States produced by the engine for writing (e.g. uniqExactState(rid) over a
// block) carry concrete Set members. States read back from storage may
// instead carry only SetSize (an exact maintained counter) plus a LoadSet
// callback that fetches the members lazily; merging a single loaded state
// therefore needs no member read, merging several does.
type AggPartial struct {
	Func string
	// ArgTypes are the aggregate's argument types (AggregateFunction(f, T...)).
	ArgTypes []Type

	Count    uint64
	Sum      any
	Value    any
	Key      any
	HasValue bool

	Set     []any
	SetSize uint64
	// SetLoaded is true when Set holds every member (always true for
	// engine-produced states). When false, call LoadSet to obtain members.
	SetLoaded bool
	// LoadSetBytes is the heap LoadSet holds while the set is unloaded:
	// the closure and the copies it captures. It fits in SetLoaded's
	// padding.
	LoadSetBytes int32
	LoadSet      func(ctx context.Context) ([]any, error)

	List []any
}
