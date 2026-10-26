package dynamo

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/storage"
	"github.com/gfronza/twinhouse/internal/storage/keyenc"
)

// deleteRows deletes the items of rows previously read with Scan. Rows must
// be complete (Scan with Columns == nil): key columns locate the items, and
// for MergeTree tables — whose items carry a uniquifier the row does not
// expose — every item under the row's sorting key whose column values all
// equal the row's is deleted (identical rows are indistinguishable to any
// WHERE clause, so they all match).
func (s *Store) deleteRows(ctx context.Context, t *catalog.Table, rows []storage.Row) error {
	l, err := s.impl.layoutOf(t)
	if err != nil {
		return err
	}
	if s.impl.multiRegion() && ownsCopy(t) {
		// Nothing carries the deletion to the other regions' copies, which
		// would keep the rows for good (DESIGN.md §1).
		return cherr.New(cherr.NotImplemented,
			"ALTER TABLE ... DELETE / DELETE FROM on %s is not supported with GLOBAL_MODE=%s: the table is regional "+
				"(every region keeps its own copy) and the deletion would only apply to this region's copy; "+
				"use TRUNCATE, or DROP and re-create the table and re-populate it", t.FullName(), s.impl.opts.GlobalMode)
	}
	if len(rows) == 0 {
		return nil
	}
	if l.appendOnly {
		return s.impl.deleteAppend(ctx, l, rows)
	}
	var keys []map[string]types.AttributeValue
	var rowKeys [][2][]byte
	seen := map[string]bool{}
	for _, r := range rows {
		pk, sk, err := l.rowKey(r, nil)
		if err != nil {
			return err
		}
		if k := string(pk) + "\x00" + string(sk); !seen[k] {
			seen[k] = true
			keys = append(keys, itemKey(pk, sk))
			rowKeys = append(rowKeys, [2][]byte{pk, sk})
		}
	}
	if l.hasMembers {
		// Members first: a row recreated later must not inherit them.
		err := parallel(ctx, len(rowKeys), writeParallelism, func(ctx context.Context, i int) error {
			for c := range l.cols {
				if l.cols[c].set {
					if err := s.impl.deleteMembers(ctx, l, memberPK(rowKeys[i][0], rowKeys[i][1], l.cols[c].name)); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return ddbError(err, "cannot delete set members of %s", l.t.FullName())
		}
	}
	return s.impl.deleteKeys(ctx, l.physical, keys)
}

func (s *storeImpl) deleteMembers(ctx context.Context, l *layout, mpk []byte) error {
	in := &dynamodb.QueryInput{
		TableName:                 aws.String(membersTable(l.physical)),
		KeyConditionExpression:    aws.String("#p = :p"),
		ExpressionAttributeNames:  map[string]string{"#p": attrPK, "#s": attrSK},
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": attrB(mpk)},
		ProjectionExpression:      aws.String("#p, #s"),
		ConsistentRead:            aws.Bool(true),
	}
	for {
		var out *dynamodb.QueryOutput
		if err := withRetry(ctx, func() (err error) {
			out, err = s.ddb.Query(ctx, in)
			return err
		}); err != nil {
			return err
		}
		if len(out.Items) > 0 {
			reqs := make([]types.WriteRequest, len(out.Items))
			for i, it := range out.Items {
				reqs[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: it}}
			}
			if err := s.batchWrite(ctx, membersTable(l.physical), reqs); err != nil {
				return err
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			return nil
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (s *storeImpl) deleteAppend(ctx context.Context, l *layout, rows []storage.Row) error {
	type group struct {
		pk   []byte
		iv   interval
		rows []storage.Row
	}
	var groups []*group
	byKey := map[string]*group{}
	for _, r := range rows {
		pk, sk, err := l.rowKey(r, nil)
		if err != nil {
			return err
		}
		if len(l.skCols) == 0 {
			sk = nil // the whole partition
		}
		k := string(pk) + "\x00" + string(sk)
		g := byKey[k]
		if g == nil {
			g = &group{pk: pk}
			if sk != nil {
				g.iv = interval{lo: sk, hi: keyenc.PrefixUpper(sk, maxSortKeyLen)}
			}
			byKey[k] = g
			groups = append(groups, g)
		}
		g.rows = append(g.rows, r)
	}
	plan := &readPlan{l: l, consistent: true}
	absent := absentMask(l, nil) // columns added by ALTER match any engine value
	var keys []map[string]types.AttributeValue
	for _, g := range groups {
		q := queryInput(l.physical, g.pk, g.iv)
		q.ConsistentRead = aws.Bool(true)
		for {
			var out *dynamodb.QueryOutput
			if err := withRetry(ctx, func() (err error) {
				out, err = s.ddb.Query(ctx, q)
				return err
			}); err != nil {
				return ddbError(err, "cannot query DynamoDB table %s", l.physical)
			}
			for _, item := range out.Items {
				got, err := s.decodeRow(ctx, plan.l, item, nil, absent)
				if err != nil {
					return err
				}
				for _, want := range g.rows {
					if sameRow(got, want) {
						keys = append(keys, map[string]types.AttributeValue{attrPK: item[attrPK], attrSK: item[attrSK]})
						break
					}
				}
			}
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			q.ExclusiveStartKey = out.LastEvaluatedKey
		}
	}
	return s.deleteKeys(ctx, l.physical, keys)
}

// sameRow compares a stored row with a row the engine read. A column whose
// attribute is missing from the item (storage.Absent: added by ALTER after
// the item was written) matches whatever value the engine computed for it.
func sameRow(a, b storage.Row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if storage.IsAbsent(a[i]) || storage.IsAbsent(b[i]) {
			continue
		}
		if !sameValue(a[i], b[i]) {
			return false
		}
	}
	return true
}

// decodeTimeout bounds the member reads DecodeRecord may need.
const decodeTimeout = 30 * time.Second

func (s *Store) decodeRecord(t *catalog.Table, eventName string, image map[string]types.AttributeValue) (*storage.StreamRecord, error) {
	if _, ok := image[attrPK].(*types.AttributeValueMemberB); !ok {
		return nil, nil
	}
	if _, ok := image[attrSK].(*types.AttributeValueMemberB); !ok {
		return nil, nil
	}
	l, err := s.impl.layoutOf(t)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), decodeTimeout)
	defer cancel()
	row, err := s.impl.decodeRow(ctx, l, image, nil, nil)
	if err != nil {
		return nil, err
	}
	rec := &storage.StreamRecord{Table: t, EventName: eventName, Row: row}
	if v, ok := image[attrRID].(*types.AttributeValueMemberS); ok {
		// The id carries the row's write time (see sourceID): view output
		// of rows written before the target's reset epoch is dropped, and
		// output into Replacing targets is ordered by it.
		rec.RowID = sourceID(v.Value, numAttr(image, attrTS), true)
	}
	if v, ok := image[attrOrigin].(*types.AttributeValueMemberS); ok {
		rec.Origin = v.Value
	}
	if v, ok := image[attrMV].(*types.AttributeValueMemberBOOL); ok {
		rec.IsMVOutput = v.Value
	}
	return rec, nil
}
