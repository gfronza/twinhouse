package dynamo

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/storage"
	"dynamodb-eventstream/internal/storage/keyenc"
)

// interval is an inclusive _sk byte range; nil bounds are unbounded.
type interval struct{ lo, hi []byte }

// partitionRanges are the disjoint, ordered _sk intervals to query in one
// partition.
type partitionRanges struct {
	pk  []byte
	ivs []interval
}

// keyInterval translates a KeyRange into a partition key and a _sk interval
// that contains every matching row. ok is false when no row can match.
// Bounds that cannot be translated safely (Enum columns, values that do not
// encode for the column type) are dropped: the range only grows.
func (l *layout) keyInterval(kr storage.KeyRange) (pk []byte, iv interval, ok bool, err error) {
	if len(kr.Partition) != len(l.pkCols) {
		return nil, iv, false, cherr.New(cherr.LogicalError, "key range has %d partition values, table %s has %d partition key columns",
			len(kr.Partition), l.t.FullName(), len(l.pkCols))
	}
	if len(kr.SortPrefix) > len(l.skCols) {
		return nil, iv, false, cherr.New(cherr.LogicalError, "key range sort prefix longer than the sort key of %s", l.t.FullName())
	}
	if pk, err = keyenc.AppendTuple(nil, l.pkTypes, kr.Partition); err != nil {
		return nil, iv, false, err
	}
	if len(l.pkTypes) == 0 {
		pk = constantKey
	}
	if len(pk) > maxPartitionKeyLen {
		return nil, iv, false, nil
	}
	prefix, err := keyenc.AppendTuple(nil, l.skTypes[:len(kr.SortPrefix)], kr.SortPrefix)
	if err != nil {
		return nil, iv, false, err
	}
	if len(prefix) > maxSortKeyLen {
		return nil, iv, false, nil
	}
	if len(prefix) > 0 {
		iv = interval{lo: prefix, hi: keyenc.PrefixUpper(prefix, maxSortKeyLen)}
	}
	if n := len(kr.SortPrefix); n < len(l.skCols) && keyenc.RangeSafe(l.skTypes[n]) {
		next := l.skTypes[n]
		if kr.Lower != nil {
			// Inclusive even for '>' (the engine re-filters): an exclusive
			// translation would be wrong for strings continued by 0x00.
			if b, err := keyenc.Append(append([]byte(nil), prefix...), next, kr.Lower.Value); err == nil {
				iv.lo = truncate(b)
			}
		}
		if kr.Upper != nil {
			if b, err := keyenc.Append(append([]byte(nil), prefix...), next, kr.Upper.Value); err == nil {
				if kr.Upper.Inclusive {
					iv.hi = keyenc.PrefixUpper(b, maxSortKeyLen)
				} else {
					// Every value below the bound sorts strictly below its
					// encoding, whatever follows it.
					iv.hi = truncate(b)
				}
			}
		}
	}
	if iv.lo != nil && iv.hi != nil && bytes.Compare(iv.lo, iv.hi) > 0 {
		return nil, iv, false, nil
	}
	return pk, iv, true, nil
}

// truncate clips a bound to the sort-key size limit. For lower bounds this
// only widens the range; for upper bounds no key longer than the limit
// exists, so nothing is lost.
func truncate(b []byte) []byte {
	if len(b) > maxSortKeyLen {
		return b[:maxSortKeyLen]
	}
	return b
}

// planRanges groups key ranges by partition (first-appearance order) and
// merges overlapping intervals so no item is read twice.
func (l *layout) planRanges(ranges []storage.KeyRange, reverse bool) ([]partitionRanges, error) {
	var out []partitionRanges
	idx := map[string]int{}
	for _, kr := range ranges {
		pk, iv, ok, err := l.keyInterval(kr)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		i, seen := idx[string(pk)]
		if !seen {
			i = len(out)
			idx[string(pk)] = i
			out = append(out, partitionRanges{pk: pk})
		}
		out[i].ivs = append(out[i].ivs, iv)
	}
	for i := range out {
		out[i].ivs = mergeIntervals(out[i].ivs)
		if reverse {
			for a, b := 0, len(out[i].ivs)-1; a < b; a, b = a+1, b-1 {
				out[i].ivs[a], out[i].ivs[b] = out[i].ivs[b], out[i].ivs[a]
			}
		}
	}
	return out, nil
}

// mergeIntervals sorts intervals and unions overlapping ones.
func mergeIntervals(ivs []interval) []interval {
	sort.Slice(ivs, func(i, j int) bool {
		a, b := ivs[i].lo, ivs[j].lo
		if a == nil || b == nil {
			return a == nil && b != nil
		}
		return bytes.Compare(a, b) < 0
	})
	var out []interval
	for _, iv := range ivs {
		if n := len(out); n > 0 {
			cur := &out[n-1]
			if cur.hi == nil || iv.lo == nil || bytes.Compare(iv.lo, cur.hi) <= 0 {
				if cur.hi != nil && (iv.hi == nil || bytes.Compare(iv.hi, cur.hi) > 0) {
					cur.hi = iv.hi
				}
				continue
			}
		}
		out = append(out, iv)
	}
	return out
}

func queryInput(table string, pk []byte, iv interval) *dynamodb.QueryInput {
	names := map[string]string{"#kp": attrPK}
	values := map[string]types.AttributeValue{":kp": attrB(pk)}
	cond := "#kp = :kp"
	if iv.lo != nil || iv.hi != nil {
		names["#ks"] = attrSK
	}
	switch {
	case iv.lo != nil && iv.hi != nil && bytes.Equal(iv.lo, iv.hi):
		cond += " AND #ks = :klo"
		values[":klo"] = attrB(iv.lo)
	case iv.lo != nil && iv.hi != nil:
		cond += " AND #ks BETWEEN :klo AND :khi"
		values[":klo"], values[":khi"] = attrB(iv.lo), attrB(iv.hi)
	case iv.lo != nil:
		cond += " AND #ks >= :klo"
		values[":klo"] = attrB(iv.lo)
	case iv.hi != nil:
		cond += " AND #ks <= :khi"
		values[":khi"] = attrB(iv.hi)
	}
	return &dynamodb.QueryInput{
		TableName: aws.String(table), KeyConditionExpression: aws.String(cond),
		ExpressionAttributeNames: names, ExpressionAttributeValues: values,
	}
}

// readPlan holds what every page reader needs.
type readPlan struct {
	l          *layout
	want       []bool // nil = all columns
	projection *string
	projNames  map[string]string
	consistent bool
	pageLimit  int32
	filter     func(storage.Row) bool // false = skip row
	// absent marks the requested columns that decode to storage.Absent when
	// missing (ScanRequest.MarkAbsent); nil = none.
	absent []bool
}

func (s *storeImpl) newReadPlan(l *layout, req storage.ScanRequest) *readPlan {
	p := &readPlan{l: l, consistent: req.Consistent}
	if req.MarkAbsent {
		// Only the caller's columns: extra columns read for the filters
		// below keep decoding to zero values.
		var requested []bool
		if req.Columns != nil {
			requested = make([]bool, len(l.cols))
			for _, c := range req.Columns {
				if c >= 0 && c < len(l.cols) {
					requested[c] = true
				}
			}
		}
		p.absent = absentMask(l, requested)
	}
	var extra []int
	if l.deletedIdx >= 0 {
		extra = append(extra, l.deletedIdx)
		di := l.deletedIdx
		p.filter = func(r storage.Row) bool { u, _ := r[di].(uint64); return u != 1 }
	}
	if l.t.Engine == catalog.EngineSummingMergeTree && len(l.summedIdx) > 0 {
		// ClickHouse drops SummingMergeTree rows whose summed columns are all
		// zero when parts merge; reads here always see the merged state.
		extra = append(extra, l.summedIdx...)
		idx := l.summedIdx
		p.filter = func(r storage.Row) bool {
			for _, i := range idx {
				if r[i] != nil && !isZeroNumber(r[i]) {
					return true
				}
			}
			return false
		}
	}
	if req.Limit > 0 && p.filter == nil {
		p.pageLimit = int32(min(req.Limit, 1<<20))
	}
	if req.Columns == nil {
		return p
	}
	p.want = make([]bool, len(l.cols))
	for _, c := range req.Columns {
		if c >= 0 && c < len(l.cols) {
			p.want[c] = true
		}
	}
	for _, c := range extra {
		p.want[c] = true
	}
	names := map[string]string{"#p0": attrPK, "#p1": attrSK, "#p2": attrRID, "#p3": attrTS}
	parts := []string{"#p0", "#p1", "#p2", "#p3"}
	for i, w := range p.want {
		if !w {
			continue
		}
		for _, a := range l.cols[i].attrs() {
			ph := "#p" + itoa64(int64(len(names)))
			names[ph] = a
			parts = append(parts, ph)
		}
	}
	// DynamoDB caps the expression at 4 KB: a projection naming more
	// attributes reads whole items (decodeRow only decodes p.want).
	if proj := strings.Join(parts, ","); len(proj) <= maxExpressionLen {
		p.projection, p.projNames = aws.String(proj), names
	}
	return p
}

func (p *readPlan) decodePage(ctx context.Context, s *storeImpl, items []map[string]types.AttributeValue) (page, error) {
	pg := page{rows: make([]storage.Row, 0, len(items)), ids: make([]string, 0, len(items))}
	for _, it := range items {
		row, err := s.decodeRow(ctx, p.l, it, p.want, p.absent)
		if err != nil {
			return page{}, err
		}
		if p.filter != nil && !p.filter(row) {
			continue
		}
		pg.rows = append(pg.rows, row)
		pg.ids = append(pg.ids, sourceID(strAttr(it, attrRID), numAttr(it, attrTS), false))
	}
	return pg, nil
}

func (s *Store) scan(ctx context.Context, t *catalog.Table, req storage.ScanRequest) (storage.RowIterator, error) {
	l, err := s.impl.layoutOf(t)
	if err != nil {
		return nil, err
	}
	plan := s.impl.newReadPlan(l, req)
	it := newIterator(ctx, req.Limit)
	if req.Ranges == nil {
		s.impl.startFullScan(it, plan)
		return it, nil
	}
	parts, err := l.planRanges(req.Ranges, req.Reverse)
	if err != nil {
		it.Close()
		return nil, err
	}
	var queries []*dynamodb.QueryInput
	for _, pr := range parts {
		for _, iv := range pr.ivs {
			q := queryInput(l.physical, pr.pk, iv)
			q.ScanIndexForward = aws.Bool(!req.Reverse)
			queries = append(queries, q)
		}
	}
	s.impl.startQueries(it, plan, queries)
	return it, nil
}

type page struct {
	rows []storage.Row
	ids  []string // source row ids of rows (see RowID)
	err  error
}

// iterator streams pages produced by background readers. Memory is bounded
// by the channel capacities (a few pages per reader).
type iterator struct {
	ctx    context.Context
	cancel context.CancelFunc
	out    chan page
	wg     sync.WaitGroup

	buf     []storage.Row
	ids     []string
	lastID  string
	limit   int
	emitted int
	err     error
	done    bool
	closed  bool
}

func newIterator(ctx context.Context, limit int) *iterator {
	ctx, cancel := context.WithCancel(ctx)
	return &iterator{ctx: ctx, cancel: cancel, out: make(chan page, 4), limit: limit}
}

// send delivers a page unless the iterator was closed.
func (it *iterator) send(ch chan<- page, p page) bool {
	select {
	case ch <- p:
		return true
	case <-it.ctx.Done():
		return false
	}
}

func (it *iterator) Next(ctx context.Context) (storage.Row, error) {
	if it.closed {
		return nil, io.EOF
	}
	for len(it.buf) == 0 {
		switch {
		case it.err != nil:
			return nil, it.err
		case it.done || it.closed:
			return nil, io.EOF
		}
		select {
		case p, ok := <-it.out:
			switch {
			case !ok:
				it.done = true
				if err := it.ctx.Err(); err != nil && !it.closed {
					it.err = cherr.Wrap(cherr.Timeout, err, "scan cancelled")
				}
			case p.err != nil:
				it.err = p.err
				it.cancel()
			default:
				it.buf, it.ids = p.rows, p.ids
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if it.limit > 0 && it.emitted >= it.limit {
		it.done = true
		it.cancel()
		return nil, io.EOF
	}
	row := it.buf[0]
	it.buf[0] = nil
	it.buf = it.buf[1:]
	it.lastID = ""
	if len(it.ids) > 0 {
		it.lastID, it.ids = it.ids[0], it.ids[1:]
	}
	it.emitted++
	return row, nil
}

// RowID returns the source row id of the row last returned by Next (the
// stored _rid with its write time, in the never-gated form of sourceID):
// exec's POPULATE uses it so its exactly-once markers coincide with those
// of inline application and the stream worker (exec.RowIDIterator).
func (it *iterator) RowID() string { return it.lastID }

// rowIDIterator mirrors exec.RowIDIterator (asserted against it in the
// tests) without making the storage layer import the engine.
type rowIDIterator interface{ RowID() string }

var _ rowIDIterator = (*iterator)(nil)

func (it *iterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.buf = nil
	it.cancel()
	for range it.out {
	}
	it.wg.Wait()
	return nil
}

// startFullScan reads all segments in parallel into it.out.
func (s *storeImpl) startFullScan(it *iterator, plan *readPlan) {
	segments := s.opts.ScanSegments
	var readers sync.WaitGroup
	for seg := 0; seg < segments; seg++ {
		readers.Add(1)
		it.wg.Add(1)
		go func(seg int) {
			defer it.wg.Done()
			defer readers.Done()
			in := &dynamodb.ScanInput{
				TableName: aws.String(plan.l.physical), Segment: aws.Int32(int32(seg)), TotalSegments: aws.Int32(int32(segments)),
				ConsistentRead: aws.Bool(plan.consistent), ProjectionExpression: plan.projection, ExpressionAttributeNames: plan.projNames,
			}
			if plan.pageLimit > 0 {
				in.Limit = aws.Int32(plan.pageLimit)
			}
			err := s.scanPages(it.ctx, in, func(items []map[string]types.AttributeValue) error {
				pg, err := plan.decodePage(it.ctx, s, items)
				if err != nil {
					return err
				}
				if len(pg.rows) > 0 && !it.send(it.out, pg) {
					return it.ctx.Err()
				}
				return nil
			})
			if err != nil && it.ctx.Err() == nil {
				it.send(it.out, page{err: ddbError(err, "cannot scan DynamoDB table %s", plan.l.physical)})
			}
		}(seg)
	}
	it.wg.Add(1)
	go func() {
		defer it.wg.Done()
		readers.Wait()
		close(it.out)
	}()
}

// startQueries runs the queries with bounded parallelism while delivering
// their pages strictly in query order (each range stays sorted by _sk).
func (s *storeImpl) startQueries(it *iterator, plan *readPlan, queries []*dynamodb.QueryInput) {
	slots := make(chan chan page, rangeParallelism)
	it.wg.Add(2)
	go func() { // dispatcher
		defer it.wg.Done()
		defer close(slots)
		for _, q := range queries {
			ch := make(chan page, 2)
			select {
			case slots <- ch:
			case <-it.ctx.Done():
				return
			}
			it.wg.Add(1)
			go func(q *dynamodb.QueryInput) {
				defer it.wg.Done()
				defer close(ch)
				s.runQuery(it, plan, q, ch)
			}(q)
		}
	}()
	go func() { // collector
		defer it.wg.Done()
		defer close(it.out)
		for ch := range slots {
			for p := range ch {
				if !it.send(it.out, p) {
					return
				}
			}
		}
	}()
}

func (s *storeImpl) runQuery(it *iterator, plan *readPlan, q *dynamodb.QueryInput, ch chan<- page) {
	q.ConsistentRead = aws.Bool(plan.consistent)
	if plan.projection != nil {
		q.ProjectionExpression = plan.projection
		for k, v := range plan.projNames {
			q.ExpressionAttributeNames[k] = v
		}
	}
	if plan.pageLimit > 0 {
		q.Limit = aws.Int32(plan.pageLimit)
	}
	for {
		var out *dynamodb.QueryOutput
		err := withRetry(it.ctx, func() (err error) {
			out, err = s.ddb.Query(it.ctx, q)
			return err
		})
		if err != nil {
			if it.ctx.Err() == nil {
				it.send(ch, page{err: ddbError(err, "cannot query DynamoDB table %s", plan.l.physical)})
			}
			return
		}
		pg, err := plan.decodePage(it.ctx, s, out.Items)
		if err != nil {
			it.send(ch, page{err: err})
			return
		}
		if len(pg.rows) > 0 && !it.send(ch, pg) {
			return
		}
		if len(out.LastEvaluatedKey) == 0 {
			return
		}
		q.ExclusiveStartKey = out.LastEvaluatedKey
	}
}
