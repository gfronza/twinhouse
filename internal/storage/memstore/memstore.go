// Package memstore is an in-memory storage.Store. It implements every engine
// semantic of the storage contract (MergeTree append, ReplacingMergeTree
// version rules, SummingMergeTree, AggregatingMergeTree exactly-once merges
// with separately stored set members, key-range filtering) and is used as the
// test double for the query engine and the stream worker.
package memstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/memtrack"
	"github.com/gfronza/twinhouse/internal/storage"
)

// Store is an in-memory storage.Store. It is safe for concurrent use.
type Store struct {
	mu      sync.Mutex
	tables  map[string]*table // by physical name
	markers map[string]struct{}
	seq     uint64
}

var _ storage.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{tables: map[string]*table{}, markers: map[string]struct{}{}}
}

// item is one stored row (a DynamoDB item). Values are kept by column name so
// that ALTER ADD/DROP COLUMN behaves like attribute-based storage.
type item struct {
	key  string
	pk   []any
	sk   []any
	seq  uint64
	rid  string
	mv   bool
	vals map[string]any
	// members holds set members of set-valued aggregate columns.
	members map[string]*memberSet
}

type memberSet struct {
	keys map[string]struct{}
	vals []any
}

func (m *memberSet) add(vs []any) {
	for _, v := range vs {
		k := aggstate.Key(v)
		if _, ok := m.keys[k]; ok {
			continue
		}
		m.keys[k] = struct{}{}
		m.vals = append(m.vals, v)
	}
}

type table struct {
	items map[string]*item
}

func physical(t *catalog.Table) string {
	if t.PhysicalName != "" {
		return t.PhysicalName
	}
	return t.Database + "." + t.Name
}

func (s *Store) table(t *catalog.Table) (*table, error) {
	tb, ok := s.tables[physical(t)]
	if !ok {
		return nil, cherr.New(cherr.UnknownTable, "physical table %s does not exist", physical(t))
	}
	return tb, nil
}

// EnsureTable implements storage.Store.
func (s *Store) EnsureTable(_ context.Context, t *catalog.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[physical(t)]; !ok {
		s.tables[physical(t)] = &table{items: map[string]*item{}}
	}
	return nil
}

// DropTable implements storage.Store.
func (s *Store) DropTable(_ context.Context, t *catalog.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tables, physical(t))
	return nil
}

// TruncateTable implements storage.Store.
func (s *Store) TruncateTable(_ context.Context, t *catalog.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tb, err := s.table(t)
	if err != nil {
		return err
	}
	tb.items = map[string]*item{}
	return nil
}

// HasTable reports whether the physical table of t exists.
func (s *Store) HasTable(t *catalog.Table) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.tables[physical(t)]
	return ok
}

// ItemCount returns the number of stored items of t (0 if missing).
func (s *Store) ItemCount(t *catalog.Table) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tb, ok := s.tables[physical(t)]; ok {
		return len(tb.items)
	}
	return 0
}

// MarkerCount returns the number of exactly-once markers recorded.
func (s *Store) MarkerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.markers)
}

func isKeyed(engine string) bool {
	switch engine {
	case catalog.EngineReplacingMergeTree, catalog.EngineSummingMergeTree, catalog.EngineAggregatingMergeTree:
		return true
	}
	return false
}

func isMerging(engine string) bool {
	return engine == catalog.EngineSummingMergeTree || engine == catalog.EngineAggregatingMergeTree
}

// stored reports whether column c is persisted (ALIAS/EPHEMERAL are not).
func stored(c *catalog.Column) bool {
	return c.DefaultKind != "ALIAS" && c.DefaultKind != "EPHEMERAL"
}

func keyValues(t *catalog.Table, row storage.Row, names []string) ([]any, error) {
	out := make([]any, len(names))
	for i, n := range names {
		ci := t.ColumnIndex(n)
		if ci < 0 {
			return nil, cherr.New(cherr.NoSuchColumnInTable, "sorting key column %s is missing in table %s", n, t.FullName())
		}
		if ci >= len(row) {
			return nil, cherr.New(cherr.LogicalError, "row has %d values, table %s has %d columns", len(row), t.FullName(), len(t.Columns))
		}
		out[i] = row[ci]
	}
	return out, nil
}

func encodeKey(pk, sk []any) string {
	b := aggstate.AppendKey(nil, pk)
	b = append(b, '|')
	return string(aggstate.AppendKey(b, sk))
}

func (s *Store) nextSeq() uint64 {
	s.seq++
	return s.seq
}

// Insert implements storage.Store.
func (s *Store) Insert(ctx context.Context, t *catalog.Table, rows []storage.Row) (*storage.InsertResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tb, err := s.table(t)
	if err != nil {
		return nil, err
	}
	res := &storage.InsertResult{RowIDs: make([]string, len(rows)), Skipped: make([]bool, len(rows))}
	for i, row := range rows {
		if len(row) != len(t.Columns) {
			return nil, cherr.New(cherr.LogicalError, "row has %d values, table %s has %d columns", len(row), t.FullName(), len(t.Columns))
		}
		if err := checkRow(t, row); err != nil {
			return nil, err
		}
		rid := uuid.NewString()
		res.RowIDs[i] = rid
		pk, err := keyValues(t, row, t.PartitionKey)
		if err != nil {
			return nil, err
		}
		sk, err := keyValues(t, row, t.SortKey)
		if err != nil {
			return nil, err
		}
		switch {
		case isMerging(t.Engine):
			// Direct inserts merge with a fresh source ref: not idempotent.
			if err := s.mergeOne(ctx, t, tb, row, pk, sk, storage.SourceRef{View: "", RowID: rid}, 0); err != nil {
				return nil, err
			}
		case t.Engine == catalog.EngineReplacingMergeTree:
			if !s.putReplacing(t, tb, row, pk, sk, rid, false) {
				res.Skipped[i] = true
			}
		default:
			seq := s.nextSeq()
			key := encodeKey(pk, sk) + "#" + strconv.FormatUint(seq, 10)
			tb.items[key] = &item{key: key, pk: pk, sk: sk, seq: seq, rid: rid, vals: rowValues(t, row)}
		}
	}
	return res, nil
}

func rowValues(t *catalog.Table, row storage.Row) map[string]any {
	m := make(map[string]any, len(t.Columns))
	for i := range t.Columns {
		if stored(&t.Columns[i]) {
			m[t.Columns[i].Name] = row[i]
		}
	}
	return m
}

// putReplacing stores row if it is not older than the stored version.
func (s *Store) putReplacing(t *catalog.Table, tb *table, row storage.Row, pk, sk []any, rid string, mv bool) bool {
	key := encodeKey(pk, sk)
	if old, ok := tb.items[key]; ok && t.VersionColumn != "" {
		vi := t.ColumnIndex(t.VersionColumn)
		if vi >= 0 && chtype.Compare(row[vi], old.vals[t.VersionColumn]) < 0 {
			return false
		}
	}
	tb.items[key] = &item{key: key, pk: pk, sk: sk, seq: s.nextSeq(), rid: rid, mv: mv, vals: rowValues(t, row)}
	return true
}

// Merge implements storage.Store.
func (s *Store) Merge(ctx context.Context, t *catalog.Table, rows []storage.MergeRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tb, err := s.table(t)
	if err != nil {
		return err
	}
	for _, mr := range rows {
		if len(mr.Row) != len(t.Columns) {
			return cherr.New(cherr.LogicalError, "merge row has %d values, table %s has %d columns", len(mr.Row), t.FullName(), len(t.Columns))
		}
		if err := checkRow(t, mr.Row); err != nil {
			return err
		}
		pk, err := keyValues(t, mr.Row, t.PartitionKey)
		if err != nil {
			return err
		}
		sk, err := keyValues(t, mr.Row, t.SortKey)
		if err != nil {
			return err
		}
		switch {
		case isMerging(t.Engine):
			if err := s.mergeOne(ctx, t, tb, mr.Row, pk, sk, mr.Source, mr.Index); err != nil {
				return err
			}
		case t.Engine == catalog.EngineReplacingMergeTree:
			s.putReplacing(t, tb, mr.Row, pk, sk, derivedID(mr.Source, mr.Index), true)
		default:
			// Deterministic item key: redelivery overwrites the same item.
			id := derivedID(mr.Source, mr.Index)
			key := encodeKey(pk, sk) + "#mv:" + id
			if old, ok := tb.items[key]; ok {
				old.vals = rowValues(t, mr.Row)
				continue
			}
			tb.items[key] = &item{key: key, pk: pk, sk: sk, seq: s.nextSeq(), rid: id, mv: true, vals: rowValues(t, mr.Row)}
		}
	}
	return nil
}

func derivedID(src storage.SourceRef, index int) string {
	h := sha256.Sum256([]byte(src.View + "\x00" + src.RowID + "\x00" + strconv.Itoa(index)))
	return hex.EncodeToString(h[:16])
}

// mergeOne merges row into the keyed item exactly once per
// (source, table, key, index).
func (s *Store) mergeOne(ctx context.Context, t *catalog.Table, tb *table, row storage.Row, pk, sk []any, src storage.SourceRef, index int) error {
	key := encodeKey(pk, sk)
	marker := strings.Join([]string{src.View, src.RowID, physical(t), key, strconv.Itoa(index)}, "\x00")
	if _, done := s.markers[marker]; done {
		return nil
	}
	it, ok := tb.items[key]
	fresh := !ok
	if fresh {
		it = &item{key: key, pk: pk, sk: sk, seq: s.nextSeq(), rid: derivedID(src, index), mv: src.View != "", vals: map[string]any{}, members: map[string]*memberSet{}}
	}
	// Compute the merged values first so a failure leaves the item untouched.
	newVals := make(map[string]any, len(it.vals))
	for k, v := range it.vals {
		newVals[k] = v
	}
	type memberAdd struct {
		col  string
		vals []any
	}
	var adds []memberAdd
	isKey := map[string]bool{}
	for _, n := range t.PartitionKey {
		isKey[n] = true
	}
	for _, n := range t.SortKey {
		isKey[n] = true
	}
	for i := range t.Columns {
		c := &t.Columns[i]
		if !stored(c) {
			continue
		}
		v := row[i]
		old, had := it.vals[c.Name]
		switch {
		case isKey[c.Name]:
			newVals[c.Name] = v
		case c.Type.Kind == chtype.AggregateFunction:
			p, ok := v.(*chtype.AggPartial)
			if v == nil {
				if !had {
					newVals[c.Name] = aggstate.New(aggstate.StateBase(c.Type.AggFunc), c.Type.AggArgs)
				}
				continue
			}
			if !ok {
				return cherr.New(cherr.TypeMismatch, "column %s of type %s expects an aggregate state, got %T", c.Name, c.TypeText, v)
			}
			if p.Func != aggstate.StateBase(c.Type.AggFunc) {
				return cherr.New(cherr.TypeMismatch, "cannot merge state of %s into column %s of type %s", p.Func, c.Name, c.TypeText)
			}
			var cur *chtype.AggPartial
			if had && old != nil {
				cur = aggstate.Clone(old.(*chtype.AggPartial))
			} else {
				cur = aggstate.New(aggstate.StateBase(c.Type.AggFunc), c.Type.AggArgs)
			}
			if aggstate.IsSetFunc(p.Func) {
				m, err := aggstate.Members(ctx, p)
				if err != nil {
					return err
				}
				adds = append(adds, memberAdd{c.Name, m})
			} else if err := aggstate.Merge(ctx, cur, p); err != nil {
				return err
			}
			newVals[c.Name] = cur
		case c.Type.Kind == chtype.SimpleAggregateFunction:
			nv, err := mergeSimple(c.Type, old, had, v)
			if err != nil {
				return err
			}
			newVals[c.Name] = nv
		case t.Engine == catalog.EngineSummingMergeTree && summed(t, c, isKey):
			if had {
				newVals[c.Name] = aggstate.Wrap(c.Type, aggstate.AddNumbers(old, v))
			} else {
				newVals[c.Name] = v
			}
		default:
			// Non-aggregated columns: Summing keeps the first value; Aggregating
			// keeps the last written one.
			if !had || t.Engine == catalog.EngineAggregatingMergeTree {
				newVals[c.Name] = v
			}
		}
	}
	it.vals = newVals
	for _, a := range adds {
		ms := it.members[a.col]
		if ms == nil {
			ms = &memberSet{keys: map[string]struct{}{}}
			it.members[a.col] = ms
		}
		ms.add(a.vals)
	}
	if fresh {
		tb.items[key] = it
	}
	s.markers[marker] = struct{}{}
	return nil
}

func summed(t *catalog.Table, c *catalog.Column, isKey map[string]bool) bool {
	if isKey[c.Name] {
		return false
	}
	if len(t.SumColumns) > 0 {
		for _, n := range t.SumColumns {
			if n == c.Name {
				return true
			}
		}
		return false
	}
	return c.Type.IsNumeric() && !c.Type.IsNullable()
}

func mergeSimple(t chtype.Type, old any, had bool, v any) (any, error) {
	if !had || old == nil {
		return v, nil
	}
	if v == nil {
		return old, nil
	}
	switch t.AggFunc {
	case "sum", "sumWithOverflow":
		return aggstate.Wrap(*t.Elem, aggstate.AddNumbers(old, v)), nil
	case "min", "max":
		if aggstate.ReplacesTyped(v, old, *t.Elem, t.AggFunc == "min") {
			return v, nil
		}
		return old, nil
	case "any":
		return old, nil
	case "anyLast":
		return v, nil
	case "groupUniqArrayArray":
		a, _ := old.([]any)
		b, _ := v.([]any)
		return aggstate.Union(a, b), nil
	case "groupArrayArray":
		a, _ := old.([]any)
		b, _ := v.([]any)
		return append(append([]any{}, a...), b...), nil
	}
	return nil, cherr.New(cherr.NotImplemented, "SimpleAggregateFunction(%s) is not supported", t.AggFunc)
}

// Scan implements storage.Store. Rows are returned ordered by sorting key
// (then insertion order); columns not requested are nil.
func (s *Store) Scan(_ context.Context, t *catalog.Table, req storage.ScanRequest) (storage.RowIterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tb, err := s.table(t)
	if err != nil {
		return nil, err
	}
	want := make([]bool, len(t.Columns))
	for i := range want {
		want[i] = req.Columns == nil
	}
	for _, ci := range req.Columns {
		if ci >= 0 && ci < len(want) {
			want[ci] = true
		}
	}
	// ReplacingMergeTree(ver, is_deleted): reads behave like FINAL, so a key
	// whose latest row is a deletion (is_deleted = 1) is not returned.
	deleted := t.IsDeletedColumn()
	pkT, skT := keyTypes(t)
	var items []*item
	for _, it := range tb.items {
		if deleted != "" && isDeletedRow(it.vals[deleted]) {
			continue
		}
		if req.Ranges == nil || matchesAny(it, req.Ranges, pkT, skT) {
			items = append(items, it)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if c := compareTuples(items[i].pk, items[j].pk, pkT); c != 0 {
			return c < 0
		}
		c := compareTuples(items[i].sk, items[j].sk, skT)
		if req.Reverse {
			c = -c
		}
		if c != 0 {
			return c < 0
		}
		return items[i].seq < items[j].seq
	})
	if req.Limit > 0 && len(items) > req.Limit {
		items = items[:req.Limit]
	}
	rows := make([]storage.Row, len(items))
	rids := make([]string, len(items))
	for n, it := range items {
		row := make(storage.Row, len(t.Columns))
		rids[n] = it.rid
		for i := range t.Columns {
			c := &t.Columns[i]
			if !want[i] || !stored(c) {
				continue
			}
			v, ok := it.vals[c.Name]
			if !ok {
				// Item written before the column was added (ALTER ADD
				// COLUMN): storage.Absent when asked for (the engine
				// computes the default), the zero value otherwise.
				if req.MarkAbsent && storage.MarksAbsent(c) && !isKeyColumn(t, c.Name) {
					row[i] = storage.Absent
				} else {
					row[i] = chtype.Zero(c.Type)
				}
				continue
			}
			if p, isP := v.(*chtype.AggPartial); isP {
				v = readPartial(p, it.members[c.Name])
			}
			row[i] = v
		}
		rows[n] = row
	}
	return &sliceIter{rows: rows, rids: rids}, nil
}

func isKeyColumn(t *catalog.Table, name string) bool {
	for _, k := range t.OrderBy {
		if k == name {
			return true
		}
	}
	return false
}

func isDeletedRow(v any) bool {
	switch x := v.(type) {
	case uint64:
		return x == 1
	case int64:
		return x == 1
	case bool:
		return x
	}
	return false
}

// readPartial returns a copy of a stored state the way the DynamoDB store
// rebuilds it: set members are not inlined; SetSize is exact and LoadSet
// fetches them on demand.
func readPartial(p *chtype.AggPartial, ms *memberSet) *chtype.AggPartial {
	c := aggstate.Clone(p)
	if !aggstate.IsSetFunc(c.Func) {
		return c
	}
	var members []any
	if ms != nil {
		members = append([]any(nil), ms.vals...)
	}
	c.Set = nil
	c.SetSize = uint64(len(members))
	c.SetLoaded = c.SetSize == 0
	c.LoadSet = func(context.Context) ([]any, error) { return append([]any(nil), members...), nil }
	// The closure (code pointer and slice header) and the members' array;
	// the members themselves are the stored ones.
	c.LoadSetBytes = int32(memtrack.Alloc(32) + memtrack.Slice(cap(members)) - 24)
	return c
}

// keyTypes returns the types of t's partition-key and sort-key columns, so
// that keys order (and range-match) in ClickHouse's order for each type
// (UUIDs as UInt128, enums by value), not by their canonical text.
func keyTypes(t *catalog.Table) (pk, sk []chtype.Type) {
	of := func(names []string) []chtype.Type {
		out := make([]chtype.Type, len(names))
		for i, n := range names {
			if ci := t.ColumnIndex(n); ci >= 0 {
				out[i] = t.Columns[ci].Type
			}
		}
		return out
	}
	return of(t.PartitionKey), of(t.SortKey)
}

// compareTyped compares a and b with the i-th of types (untyped beyond).
func compareTyped(a, b any, types []chtype.Type, i int) int {
	if i < len(types) && types[i].Kind != chtype.Invalid {
		return chtype.CompareTyped(a, b, types[i])
	}
	return chtype.Compare(a, b)
}

func compareTuples(a, b []any, types []chtype.Type) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := compareTyped(a[i], b[i], types, i); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

func matchesAny(it *item, ranges []storage.KeyRange, pkT, skT []chtype.Type) bool {
	for i := range ranges {
		if matches(it, &ranges[i], pkT, skT) {
			return true
		}
	}
	return false
}

func matches(it *item, r *storage.KeyRange, pkT, skT []chtype.Type) bool {
	if len(r.Partition) != len(it.pk) {
		return false
	}
	for i, v := range r.Partition {
		if compareTyped(v, it.pk[i], pkT, i) != 0 {
			return false
		}
	}
	if len(r.SortPrefix) > len(it.sk) {
		return false
	}
	for i, v := range r.SortPrefix {
		if compareTyped(v, it.sk[i], skT, i) != 0 {
			return false
		}
	}
	if r.Lower == nil && r.Upper == nil {
		return true
	}
	n := len(r.SortPrefix)
	if n >= len(it.sk) {
		return false
	}
	v := it.sk[n]
	if r.Lower != nil {
		c := compareTyped(v, r.Lower.Value, skT, n)
		if c < 0 || (c == 0 && !r.Lower.Inclusive) {
			return false
		}
	}
	if r.Upper != nil {
		c := compareTyped(v, r.Upper.Value, skT, n)
		if c > 0 || (c == 0 && !r.Upper.Inclusive) {
			return false
		}
	}
	return true
}

// DeleteRows implements storage.Store. Keyed engines delete by sorting key;
// MergeTree deletes every item whose stored values equal the row.
func (s *Store) DeleteRows(_ context.Context, t *catalog.Table, rows []storage.Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tb, err := s.table(t)
	if err != nil {
		return err
	}
	for _, row := range rows {
		pk, err := keyValues(t, row, t.PartitionKey)
		if err != nil {
			return err
		}
		sk, err := keyValues(t, row, t.SortKey)
		if err != nil {
			return err
		}
		if isKeyed(t.Engine) {
			delete(tb.items, encodeKey(pk, sk))
			continue
		}
		for k, it := range tb.items {
			if sameValues(t, it, row) {
				delete(tb.items, k)
			}
		}
	}
	return nil
}

func sameValues(t *catalog.Table, it *item, row storage.Row) bool {
	for i := range t.Columns {
		c := &t.Columns[i]
		if !stored(c) {
			continue
		}
		v, ok := it.vals[c.Name]
		if !ok {
			// Attribute absent (column added by ALTER later): it matches any
			// engine value, like the DynamoDB store.
			continue
		}
		if aggstate.Key(v) != aggstate.Key(row[i]) {
			return false
		}
	}
	return true
}

// sliceIter returns scanned rows. It implements exec.RowIDIterator (the
// stored row id of the row last returned by Next).
type sliceIter struct {
	rows []storage.Row
	rids []string
	pos  int
}

// RowID returns the stored row id (_rid) of the last returned row.
func (it *sliceIter) RowID() string {
	if it.pos == 0 || it.pos > len(it.rids) {
		return ""
	}
	return it.rids[it.pos-1]
}

func (it *sliceIter) Next(context.Context) (storage.Row, error) {
	if it.pos >= len(it.rows) {
		return nil, io.EOF
	}
	r := it.rows[it.pos]
	it.pos++
	return r, nil
}

func (it *sliceIter) Close() error { return nil }
