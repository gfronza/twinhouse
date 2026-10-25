package dynamo

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/config"
	"dynamodb-eventstream/internal/storage"
)

func (s *Store) insert(ctx context.Context, t *catalog.Table, rows []storage.Row) (*storage.InsertResult, error) {
	l, err := s.impl.layoutOf(t)
	if err != nil {
		return nil, err
	}
	res := &storage.InsertResult{RowIDs: make([]string, len(rows)), Skipped: make([]bool, len(rows))}
	if len(rows) == 0 {
		return res, nil
	}
	switch {
	case l.appendOnly:
		err = s.impl.insertAppend(ctx, l, rows, res)
	case l.merging:
		mrows := make([]storage.MergeRow, len(rows))
		for i, r := range rows {
			res.RowIDs[i] = newRowID()
			mrows[i] = storage.MergeRow{Row: r, Source: storage.SourceRef{RowID: res.RowIDs[i]}}
		}
		err = s.impl.mergeRows(ctx, l, mrows, false)
	default:
		rids := make([]string, len(rows))
		for i := range rows {
			rids[i] = newRowID()
		}
		var ts []int64
		if ts, err = s.impl.putReplacing(ctx, l, rows, rids, res.Skipped); err == nil {
			for i := range rows {
				res.RowIDs[i] = sourceID(rids[i], ts[i], true)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

// insertAppend stores every row as a new item with a fresh uniquifier.
func (s *storeImpl) insertAppend(ctx context.Context, l *layout, rows []storage.Row, res *storage.InsertResult) error {
	reqs := make([]types.WriteRequest, len(rows))
	now := s.now()
	base := s.stamp(len(rows))
	for i, row := range rows {
		uniq := newUniquifier(now)
		pk, sk, err := l.rowKey(row, uniq)
		if err != nil {
			return err
		}
		rid := hex.EncodeToString(uniq)
		ts := base + int64(i)
		item, err := s.rowItem(l, row, pk, sk, rid, false, ts)
		if err != nil {
			return err
		}
		if err := checkItemSize(l, i, item); err != nil {
			return err
		}
		res.RowIDs[i] = sourceID(rid, ts, true)
		reqs[i] = types.WriteRequest{PutRequest: &types.PutRequest{Item: item}}
	}
	return s.writeChunks(ctx, l.physical, reqs)
}

// writeChunks writes requests with bounded parallelism over 25-item batches.
func (s *storeImpl) writeChunks(ctx context.Context, table string, reqs []types.WriteRequest) error {
	n := (len(reqs) + batchWriteSize - 1) / batchWriteSize
	err := parallel(ctx, n, writeParallelism, func(ctx context.Context, i int) error {
		return s.batchWrite(ctx, table, reqs[i*batchWriteSize:min((i+1)*batchWriteSize, len(reqs))])
	})
	return ddbError(err, "cannot write to DynamoDB table %s", table)
}

// replWrite is one ReplacingMergeTree item write.
type replWrite struct {
	idx  int
	item map[string]types.AttributeValue
	// ts is the write's position in the table's total order (_ts): the
	// write time of a direct INSERT, the source row's write time for view
	// output.
	ts int64
	// so is the source-order tie-break of view output ("" for direct
	// writes): source row id and output index.
	so string
}

// groupReplacing groups writes by item key, keeping input order per key.
func groupReplacing(writes []replWrite) [][]replWrite {
	var groups [][]replWrite
	byKey := map[string]int{}
	for _, w := range writes {
		k := string(w.item[attrPK].(*types.AttributeValueMemberB).Value) + "\x00" + string(w.item[attrSK].(*types.AttributeValueMemberB).Value)
		g, ok := byKey[k]
		if !ok {
			g = len(groups)
			byKey[k] = g
			groups = append(groups, nil)
		}
		groups[g] = append(groups[g], w)
	}
	return groups
}

// putReplacing writes ReplacingMergeTree rows of a direct INSERT: one item
// per sorting key, replaced iff the new version is higher, or equal (no
// version column: always) and the write is later in (_ts, _origin) order —
// the order cmd/replicator resolves conflicts with, so every region keeps
// the same row (DESIGN.md §4). When only the clock loses (the stored item
// was written later by a clock that runs ahead, e.g. replicated from
// another region), the row is re-stamped just after the stored _ts and
// written again, so a later write still replaces an earlier one with the
// same version, like ClickHouse. Rows sharing a key are applied in input
// order with increasing _ts (equal versions: the last one wins); distinct
// keys are written in parallel. It returns each row's final _ts.
func (s *storeImpl) putReplacing(ctx context.Context, l *layout, rows []storage.Row, rids []string, skipped []bool) ([]int64, error) {
	base := s.stamp(len(rows))
	writes := make([]replWrite, len(rows))
	for i, row := range rows {
		pk, sk, err := l.rowKey(row, nil)
		if err != nil {
			return nil, err
		}
		ts := base + int64(i)
		item, err := s.rowItem(l, row, pk, sk, rids[i], false, ts)
		if err != nil {
			return nil, err
		}
		if err := checkItemSize(l, i, item); err != nil {
			return nil, err
		}
		writes[i] = replWrite{idx: i, item: item, ts: ts}
	}
	ts := make([]int64, len(rows))
	err := s.writeReplacing(ctx, l, groupReplacing(writes), false, skipped, ts)
	return ts, ddbError(err, "cannot write to DynamoDB table %s", l.physical)
}

// maxRestamps bounds the re-stamping of one direct Replacing write.
const maxRestamps = 4

// writeReplacing applies grouped Replacing writes (see putReplacing; mv:
// view output, see mergeReplacing). skipped and ts are indexed by
// replWrite.idx.
func (s *storeImpl) writeReplacing(ctx context.Context, l *layout, groups [][]replWrite, mv bool, skipped []bool, ts []int64) error {
	// Real AWS global tables (GLOBAL_MODE=aws) resolve concurrent writes
	// with their own last-writer-wins: keep the plain version rule there.
	lww := mv || s.opts.GlobalMode != config.GlobalAWS || !s.awsReplicas(l.t)
	return parallel(ctx, len(groups), writeParallelism, func(ctx context.Context, g int) error {
		var prev int64
		for _, w := range groups[g] {
			if !mv && w.ts <= prev {
				w.ts = prev + 1
				w.item[attrTS] = attrN(itoa64(w.ts))
			}
			for attempt := 0; ; attempt++ {
				in := &dynamodb.PutItemInput{TableName: aws.String(l.physical), Item: w.item}
				if lww || l.versionIdx >= 0 {
					e := newExpr()
					if lww {
						in.ConditionExpression = aws.String(replacingCondition(e, l, w, mv))
					} else {
						in.ConditionExpression = aws.String(versionCondition(e, &l.cols[l.versionIdx], w.item[l.cols[l.versionIdx].name]))
					}
					in.ExpressionAttributeNames, in.ExpressionAttributeValues = e.nameMap(), e.valueMap()
				}
				restamp := lww && !mv && attempt < maxRestamps
				if restamp {
					in.ReturnValuesOnConditionCheckFailure = types.ReturnValuesOnConditionCheckFailureAllOld
				}
				err := withRetry(ctx, func() error {
					_, err := s.ddb.PutItem(ctx, in)
					return err
				})
				if err == nil {
					break
				}
				if !isConditionFailed(err) {
					return err
				}
				if !restamp {
					skipped[w.idx] = true
					break
				}
				stored, err := s.storedTS(ctx, l, w.item, err)
				if err != nil {
					return err
				}
				if stored < w.ts {
					// The clock did not lose: a higher version is stored.
					skipped[w.idx] = true
					break
				}
				s.bumpStamp(stored)
				w.ts = s.stamp(1)
				w.item[attrTS] = attrN(itoa64(w.ts))
			}
			ts[w.idx] = w.ts
			if !skipped[w.idx] {
				prev = w.ts
			}
		}
		return nil
	})
}

// storedTS returns the _ts of the item a conditional put failed against
// (from the ALL_OLD values of the failure, else read back).
func (s *storeImpl) storedTS(ctx context.Context, l *layout, item map[string]types.AttributeValue, condErr error) (int64, error) {
	var ccf *types.ConditionalCheckFailedException
	old := map[string]types.AttributeValue(nil)
	if errors.As(condErr, &ccf) {
		old = ccf.Item
	}
	if old == nil {
		var out *dynamodb.GetItemOutput
		err := withRetry(ctx, func() (err error) {
			out, err = s.ddb.GetItem(ctx, &dynamodb.GetItemInput{
				TableName: aws.String(l.physical), ConsistentRead: aws.Bool(true),
				Key: map[string]types.AttributeValue{attrPK: item[attrPK], attrSK: item[attrSK]},
			})
			return err
		})
		if err != nil {
			return 0, err
		}
		old = out.Item
	}
	return numAttr(old, attrTS), nil
}

// replacingCondition is the write condition of a ReplacingMergeTree item
// (cmd/replicator applies the same order): the item is absent; or, with a
// version column, the stored version is lower (NULL, or absent because
// the column was added by ALTER, is the lowest), or equal and the write is
// later in the table's order; without one, the write is later. The order
// of a direct write is (_ts, _origin), strictly later; view output is
// ordered by its source row: (_ts = source write time, _so = source row id
// and output index), and re-applying the same output (equal order) writes
// the same item again.
func replacingCondition(e *expr, l *layout, w replWrite, mv bool) string {
	ts := e.n(attrTS)
	tsv := e.v(w.item[attrTS])
	order := "(attribute_not_exists(" + ts + ") OR " + ts + " < " + tsv + " OR (" + ts + " = " + tsv + " AND "
	if mv {
		so := e.n(attrSrcOrder)
		order += "(attribute_not_exists(" + so + ") OR " + so + " <= " + e.v(attrS(w.so)) + ")))"
	} else {
		order += e.n(attrOrigin) + " < " + e.v(w.item[attrOrigin]) + "))"
	}
	pk := e.n(attrPK)
	if l.versionIdx < 0 {
		return "attribute_not_exists(" + pk + ") OR " + order
	}
	c := &l.cols[l.versionIdx]
	v := e.n(c.name)
	lowest := "attribute_not_exists(" + v + ")"
	if isNullable(c.typ) {
		lowest += " OR attribute_type(" + v + ", " + e.v(attrS("NULL")) + ")"
	}
	ver := w.item[c.name]
	if _, isNull := ver.(*types.AttributeValueMemberNULL); isNull || ver == nil {
		return "attribute_not_exists(" + pk + ") OR ((" + lowest + ") AND " + order + ")"
	}
	vv := e.v(ver)
	return "attribute_not_exists(" + pk + ") OR " + lowest + " OR " + v + " < " + vv + " OR (" + v + " = " + vv + " AND " + order + ")"
}

// versionCondition builds the plain ReplacingMergeTree(ver) write condition
// used for real AWS global tables: the item is absent, or the stored
// version is <= the new one. Versions are N (integers, unix nanos for
// dates), so DynamoDB compares them numerically. NULL (Nullable version)
// is the smallest version; a missing version attribute (column added by
// ALTER) counts as NULL.
func versionCondition(e *expr, c *colSpec, ver types.AttributeValue) string {
	pk, v := e.n(attrPK), e.n(c.name)
	cond := "attribute_not_exists(" + pk + ") OR attribute_not_exists(" + v + ")"
	if isNullable(c.typ) {
		cond += " OR attribute_type(" + v + ", " + e.v(attrS("NULL")) + ")"
	}
	if _, isNull := ver.(*types.AttributeValueMemberNULL); !isNull {
		cond += " OR " + v + " <= " + e.v(ver)
	}
	return cond
}

// deleteKeys deletes items by key.
func (s *storeImpl) deleteKeys(ctx context.Context, table string, keys []map[string]types.AttributeValue) error {
	reqs := make([]types.WriteRequest, len(keys))
	for i, k := range keys {
		reqs[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: k}}
	}
	return s.writeChunks(ctx, table, reqs)
}
