package dynamo

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"time"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/storage"
	"github.com/gfronza/twinhouse/internal/storage/keyenc"
)

// System attribute names (DESIGN.md §4).
const (
	attrPK     = "_pk"
	attrSK     = "_sk"
	attrRID    = "_rid"
	attrTS     = "_ts"
	attrOrigin = "_origin"
	attrMV     = "_mv"
	attrExp    = "_exp"
	// attrMember holds a set member's value in the members table.
	attrMember = "m"
	// attrMarkerTable tags a marker with its target table instance and reset
	// epoch (see layout.markerTag), for inspection.
	attrMarkerTable = "t"
	// attrSrcOrder is the source-order tie-break of MV output written into a
	// ReplacingMergeTree target (see replacingOrder).
	attrSrcOrder = "_so"
	// attrCAS is the compare-and-swap token of a merged row's float sums
	// (see storeImpl.render): every write of a float sum replaces it and is
	// conditioned on the token it read. Column names cannot contain '#'.
	attrCAS = "#cas"
	// attrMarkerDone (BOOL) marks a marker whose row application finished
	// steps 2/3 as well, so a re-application skips the row entirely.
	attrMarkerDone = "d"
)

// Flattened AggregateFunction state attribute suffixes.
const (
	sufCount = "#n"
	sufSum   = "#s"
	sufValue = "#v"
	sufKey   = "#k"
	sufHas   = "#h"
	sufSize  = "#z"
	sufList  = "#l"
)

type colKind uint8

const (
	colPlain     colKind = iota
	colAggState          // AggregateFunction: flattened attributes
	colSimpleAgg         // SimpleAggregateFunction(f, T): stored as T, merged with f
)

type colSpec struct {
	idx  int
	name string
	typ  chtype.Type
	kind colKind
	key  bool
	// agg is the AggregateFunction spec (colAggState).
	agg catalog.AggregateSpec
	// saf is the canonical SimpleAggregateFunction merge function.
	saf string
	// summed: SummingMergeTree column merged with ADD.
	summed bool
	// set: members live in the members table; memberType is their type.
	set        bool
	memberType chtype.Type
	// prev are the types the column had before MODIFY COLUMN, oldest first
	// (catalog.Column.PrevTypes): stored values may still be encoded for one
	// of them.
	prev []chtype.Type
}

// attrs returns every attribute name a column may occupy in an item.
func (c *colSpec) attrs() []string {
	if c.kind == colAggState {
		switch c.agg.Func {
		case catalog.AggCount:
			return []string{c.name + sufCount}
		case catalog.AggSum:
			return []string{c.name + sufSum}
		case catalog.AggAvg:
			return []string{c.name + sufSum, c.name + sufCount}
		case catalog.AggArgMin, catalog.AggArgMax:
			return []string{c.name + sufValue, c.name + sufKey, c.name + sufHas}
		case catalog.AggUniq, catalog.AggUniqExact, catalog.AggGroupUniqArray:
			return []string{c.name + sufSize}
		case catalog.AggGroupArray:
			return []string{c.name + sufList}
		}
		return []string{c.name + sufValue, c.name + sufHas}
	}
	if c.set {
		return []string{c.name + sufSize}
	}
	return []string{c.name}
}

// layout is the physical mapping of one catalog table.
type layout struct {
	t        *catalog.Table
	physical string
	cols     []colSpec
	pkCols   []int
	skCols   []int
	pkTypes  []chtype.Type
	skTypes  []chtype.Type

	// appendOnly: MergeTree/Memory/Log (one item per row, uniquified _sk).
	appendOnly bool
	// merging: Aggregating/Summing (one item per key, merged in place).
	merging    bool
	versionIdx int // Replacing version column, -1 if none
	deletedIdx int // Replacing is_deleted column, -1 if none
	hasMembers bool
	summedIdx  []int

	// created identifies the table instance (CreatedAt unix nanos; a table
	// dropped and recreated under the same name is a new instance). Merge
	// replaces it with the instance recorded in the region's reset item
	// when the caller's catalog snapshot is older (see prepareTarget).
	created int64
	// cut is the region's reset epoch of the instance (unix nanos of the
	// latest TRUNCATE; 0 = never), set by prepareTarget.
	cut int64
}

func (s *storeImpl) layoutOf(t *catalog.Table) (*layout, error) {
	l := &layout{t: t, physical: s.physical(t), versionIdx: -1, deletedIdx: -1, created: t.CreatedAt.UnixNano()}
	switch t.Engine {
	case catalog.EngineMergeTree, catalog.EngineMemory, catalog.EngineLog:
		l.appendOnly = true
	case catalog.EngineAggregatingMergeTree, catalog.EngineSummingMergeTree:
		l.merging = true
	case catalog.EngineReplacingMergeTree:
	default:
		return nil, cherr.New(cherr.UnknownStorage, "unknown table engine %s", t.Engine)
	}
	keySet := map[string]bool{}
	for _, k := range t.OrderBy {
		keySet[k] = true
	}
	sumSet := map[string]bool{}
	for _, c := range t.SumColumns {
		sumSet[c] = true
	}
	l.cols = make([]colSpec, len(t.Columns))
	for i, c := range t.Columns {
		cs := colSpec{idx: i, name: c.Name, typ: c.Type, key: keySet[c.Name]}
		for _, text := range c.PrevTypes {
			pt, err := s.parse(text)
			if err != nil {
				return nil, cherr.Wrap(cherr.As(err).Code, err, "column %s of %s: previous type %s", c.Name, t.FullName(), text)
			}
			cs.prev = append(cs.prev, pt)
		}
		switch c.Type.Kind {
		case chtype.AggregateFunction:
			spec, err := catalog.ResolveAggregate(c.Type)
			if err != nil {
				return nil, err
			}
			cs.kind, cs.agg = colAggState, spec
			if spec.IsSet() {
				cs.set, cs.memberType = true, spec.MemberType()
			}
		case chtype.SimpleAggregateFunction:
			fn, err := catalog.ResolveSimpleAggregate(c.Type)
			if err != nil {
				return nil, err
			}
			cs.kind, cs.saf = colSimpleAgg, fn
			if fn == catalog.AggGroupUniqArray && l.merging {
				cs.set, cs.memberType = true, *baseType(*c.Type.Elem).Elem
			}
		}
		if t.Engine == catalog.EngineSummingMergeTree && !cs.key && cs.kind == colPlain {
			if t.SumColumns != nil {
				cs.summed = sumSet[c.Name]
			} else {
				cs.summed = summableKind(baseType(c.Type).Kind)
			}
			if cs.summed {
				l.summedIdx = append(l.summedIdx, i)
			}
		}
		if cs.set {
			l.hasMembers = true
		}
		l.cols[i] = cs
	}
	if !l.merging {
		// Aggregate column types only merge in merging engines.
		for i := range l.cols {
			if l.cols[i].kind == colAggState {
				return nil, cherr.New(cherr.NotImplemented, "AggregateFunction columns require AggregatingMergeTree")
			}
			l.cols[i].kind = colPlain
		}
	}
	for _, name := range t.PartitionKey {
		i := t.ColumnIndex(name)
		if i < 0 {
			return nil, cherr.New(cherr.LogicalError, "partition key column %s missing in %s", name, t.FullName())
		}
		l.pkCols = append(l.pkCols, i)
		l.pkTypes = append(l.pkTypes, t.Columns[i].Type)
	}
	for _, name := range t.SortKey {
		i := t.ColumnIndex(name)
		if i < 0 {
			return nil, cherr.New(cherr.LogicalError, "sort key column %s missing in %s", name, t.FullName())
		}
		l.skCols = append(l.skCols, i)
		l.skTypes = append(l.skTypes, t.Columns[i].Type)
	}
	if t.Engine == catalog.EngineReplacingMergeTree {
		if t.VersionColumn != "" {
			if l.versionIdx = t.ColumnIndex(t.VersionColumn); l.versionIdx < 0 {
				return nil, cherr.New(cherr.LogicalError, "version column %s missing in %s", t.VersionColumn, t.FullName())
			}
		}
		if name := t.IsDeletedColumn(); name != "" {
			l.deletedIdx = t.ColumnIndex(name)
		}
	}
	return l, nil
}

func summableKind(k chtype.Kind) bool {
	switch k {
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64,
		chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64, chtype.Float32, chtype.Float64:
		return true
	}
	return false
}

// constantKey is the key of an empty partition/sort key tuple.
var constantKey = []byte{0x01}

func (l *layout) partitionKey(vals []any) ([]byte, error) {
	if len(l.pkTypes) == 0 {
		return constantKey, nil
	}
	pk, err := keyenc.AppendTuple(nil, l.pkTypes, vals)
	if err != nil {
		return nil, err
	}
	if len(pk) > maxPartitionKeyLen {
		return nil, cherr.New(cherr.BadArguments, "partition key of %s is %d bytes encoded; DynamoDB allows %d", l.t.FullName(), len(pk), maxPartitionKeyLen)
	}
	return pk, nil
}

// rowKey computes (_pk, _sk) of a row; uniq is appended to the sort key of
// append-only tables (nil otherwise).
func (l *layout) rowKey(row storage.Row, uniq []byte) (pk, sk []byte, err error) {
	if len(row) != len(l.cols) {
		return nil, nil, cherr.New(cherr.LogicalError, "row has %d values, table %s has %d columns", len(row), l.t.FullName(), len(l.cols))
	}
	pkVals := make([]any, len(l.pkCols))
	for i, c := range l.pkCols {
		pkVals[i] = row[c]
	}
	if pk, err = l.partitionKey(pkVals); err != nil {
		return nil, nil, err
	}
	skVals := make([]any, len(l.skCols))
	for i, c := range l.skCols {
		skVals[i] = row[c]
	}
	if sk, err = keyenc.AppendTuple(nil, l.skTypes, skVals); err != nil {
		return nil, nil, err
	}
	sk = append(sk, uniq...)
	if len(sk) == 0 {
		sk = constantKey
	}
	if len(sk) > maxSortKeyLen {
		return nil, nil, cherr.New(cherr.BadArguments, "sort key of %s is %d bytes encoded; DynamoDB allows %d", l.t.FullName(), len(sk), maxSortKeyLen)
	}
	return pk, sk, nil
}

// newUniquifier returns a fresh 16-byte MergeTree row uniquifier: 8-byte
// big-endian unix nanos + 8 random bytes. Its first byte is < 0xFF for any
// instant before year 2262 (required by keyenc's range invariant).
func newUniquifier(now time.Time) []byte {
	u := binary.BigEndian.AppendUint64(make([]byte, 0, 16), uint64(now.UnixNano()))
	return append(u, randomBytes(8)...)
}

// mvUniquifier is the deterministic uniquifier of MV output rows into
// append-only targets: the first 16 bytes of sha256(view, rowID, index), with
// the top bit of the first byte cleared to keep it < 0xFF.
func mvUniquifier(src storage.SourceRef, index int) []byte {
	h := sha256.New()
	writeField(h, []byte(src.View))
	writeField(h, []byte(src.RowID))
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(index))
	h.Write(b[:])
	u := h.Sum(nil)[:16]
	u[0] &= 0x7F
	return u
}

// writeField writes a length-prefixed field so concatenations are unambiguous.
func writeField(h io.Writer, b []byte) {
	var n [binary.MaxVarintLen64]byte
	h.Write(n[:binary.PutUvarint(n[:], uint64(len(b)))])
	h.Write(b)
}

// markerTag identifies the table instance and reset epoch a marker belongs
// to: a table dropped and recreated under the same name, or truncated, must
// not inherit markers (POPULATE and rebuilds re-apply the same sources).
// Markers are never deleted; old ones expire through the TTL.
func (l *layout) markerTag() string {
	tag := l.physical + "@" + itoa64(l.created)
	if l.cut > 0 {
		tag += "@" + itoa64(l.cut)
	}
	return tag
}

// markerID is hex(sha256(view, rowID, table instance, _pk, _sk, index)).
func (l *layout) markerID(src storage.SourceRef, pk, sk []byte, index int) string {
	h := sha256.New()
	writeField(h, []byte(src.View))
	writeField(h, []byte(src.RowID))
	writeField(h, []byte(l.markerTag()))
	writeField(h, pk)
	writeField(h, sk)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(index))
	h.Write(b[:])
	return hex.EncodeToString(h.Sum(nil))
}

// memberPK is the members-table partition key of (row, column).
func memberPK(pk, sk []byte, col string) []byte {
	h := sha256.New()
	writeField(h, pk)
	writeField(h, sk)
	writeField(h, []byte(col))
	return h.Sum(nil)
}

// memberSK is the members-table sort key of a member: its key encoding, or
// 0xFF ‖ sha256(encoding) when that exceeds the sort-key limit (no encoding
// starts with 0xFF).
func memberSK(t chtype.Type, v any) ([]byte, error) {
	enc, err := keyenc.Append(nil, t, v)
	if err != nil {
		return nil, err
	}
	if len(enc) <= maxSortKeyLen {
		return enc, nil
	}
	sum := sha256.Sum256(enc)
	return append([]byte{0xFF}, sum[:]...), nil
}

// rowIDForKey is the deterministic _rid of a merged (one item per key) row.
func rowIDForKey(physical string, pk, sk []byte) string {
	h := sha256.New()
	writeField(h, []byte(physical))
	writeField(h, pk)
	writeField(h, sk)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

func newRowID() string { return hex.EncodeToString(randomBytes(16)) }
