package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
)

// Catalog item attributes (DESIGN.md §4): _pk, kind, def, plus ver (table
// version for optimistic ALTER) and _ts/_origin (replication LWW).
const (
	catAttrKind    = "kind"
	catAttrDef     = "def"
	catAttrVersion = "ver"
)

// catalogBackend persists catalog entries in the __catalog table.
type catalogBackend struct {
	s     *storeImpl
	table string
}

var _ catalog.Backend = (*catalogBackend)(nil)

func (b *catalogBackend) LoadAll(ctx context.Context) ([]catalog.Entry, error) {
	var out []catalog.Entry
	in := &dynamodb.ScanInput{TableName: aws.String(b.table), ConsistentRead: aws.Bool(true)}
	err := b.s.scanPages(ctx, in, func(items []map[string]types.AttributeValue) error {
		for _, it := range items {
			e, err := decodeEntry(it)
			if err != nil {
				// Quarantined by the catalog, which keeps loading the
				// other entries (one bad item must not halt refresh).
				e = catalog.Entry{Key: malformedKey(it), Kind: catalog.KindMalformed}
			}
			out = append(out, e)
		}
		return nil
	})
	if err != nil {
		return nil, ddbError(err, "cannot read catalog table %s", b.table)
	}
	return out, nil
}

// malformedKey names an unreadable catalog item for the quarantine.
func malformedKey(it map[string]types.AttributeValue) string {
	if pk, ok := it[attrPK].(*types.AttributeValueMemberS); ok {
		return pk.Value
	}
	return fmt.Sprintf("malformed:%v", it[attrPK])
}

func decodeEntry(it map[string]types.AttributeValue) (catalog.Entry, error) {
	var e catalog.Entry
	pk, ok1 := it[attrPK].(*types.AttributeValueMemberS)
	kind, ok2 := it[catAttrKind].(*types.AttributeValueMemberS)
	def, ok3 := it[catAttrDef].(*types.AttributeValueMemberS)
	if !ok1 || !ok2 || !ok3 {
		return e, cherr.New(cherr.IncorrectData, "malformed catalog item %v", it[attrPK])
	}
	e.Key, e.Kind, e.Def = pk.Value, kind.Value, []byte(def.Value)
	if v, ok := it[catAttrVersion].(*types.AttributeValueMemberN); ok {
		n, err := decodeInt(chtype.Int64, v.Value)
		if err != nil {
			return e, err
		}
		e.Version = n.(int64)
	}
	return e, nil
}

func (b *catalogBackend) item(e catalog.Entry) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK:         attrS(e.Key),
		catAttrKind:    attrS(e.Kind),
		catAttrDef:     attrS(string(e.Def)),
		catAttrVersion: attrN(itoa64(e.Version)),
		attrTS:         attrN(itoa64(b.s.now().UnixNano())),
		attrOrigin:     attrS(b.s.opts.Region),
	}
}

// condExpr renders one precondition on the item it is attached to.
func condExpr(c catalog.Cond, e *expr) string {
	pk := e.n(attrPK)
	if !c.Exists {
		return "attribute_not_exists(" + pk + ")"
	}
	s := "attribute_exists(" + pk + ")"
	if c.Version > 0 {
		s += " AND " + e.n(catAttrVersion) + " = " + e.v(attrN(itoa64(c.Version)))
	}
	return s
}

// Put writes e with its conditions: conditions on e's own key go into the
// Put; others become ConditionChecks of one transaction.
func (b *catalogBackend) Put(ctx context.Context, en catalog.Entry, conds []catalog.Cond) error {
	put := &types.Put{TableName: aws.String(b.table), Item: b.item(en)}
	pe := newExpr()
	var own []string
	ownIdx := -1
	type check struct {
		idx int
		cc  *types.ConditionCheck
	}
	var checks []check
	for i, c := range conds {
		if c.Key == en.Key {
			own = append(own, condExpr(c, pe))
			ownIdx = i
			continue
		}
		ce := newExpr()
		checks = append(checks, check{i, &types.ConditionCheck{
			TableName: aws.String(b.table), Key: map[string]types.AttributeValue{attrPK: attrS(c.Key)},
			ConditionExpression: aws.String(condExpr(c, ce)), ExpressionAttributeNames: ce.nameMap(),
			ExpressionAttributeValues: ce.valueMap(),
		}})
	}
	if len(own) > 0 {
		cond := own[0]
		for _, o := range own[1:] {
			cond = "(" + cond + ") AND (" + o + ")"
		}
		put.ConditionExpression = aws.String(cond)
		put.ExpressionAttributeNames, put.ExpressionAttributeValues = pe.nameMap(), pe.valueMap()
	}
	if len(checks) == 0 {
		err := withRetry(ctx, func() error {
			_, err := b.s.ddb.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: put.TableName, Item: put.Item, ConditionExpression: put.ConditionExpression,
				ExpressionAttributeNames: put.ExpressionAttributeNames, ExpressionAttributeValues: put.ExpressionAttributeValues,
			})
			return err
		})
		if isConditionFailed(err) {
			return &catalog.CondFailedError{Index: ownIdx}
		}
		return ddbError(err, "cannot write catalog entry %s", en.Key)
	}
	items := []types.TransactWriteItem{{Put: put}}
	for _, c := range checks {
		items = append(items, types.TransactWriteItem{ConditionCheck: c.cc})
	}
	err := withRetry(ctx, func() error {
		_, err := b.s.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		return err
	})
	if reasons := cancelReasons(err); len(reasons) == len(items) {
		for i, r := range reasons {
			if r != "ConditionalCheckFailed" {
				continue
			}
			if i == 0 {
				return &catalog.CondFailedError{Index: ownIdx}
			}
			return &catalog.CondFailedError{Index: checks[i-1].idx}
		}
	}
	return ddbError(err, "cannot write catalog entry %s", en.Key)
}

func (b *catalogBackend) Delete(ctx context.Context, keys []string) error {
	ks := make([]map[string]types.AttributeValue, len(keys))
	for i, k := range keys {
		ks[i] = map[string]types.AttributeValue{attrPK: attrS(k)}
	}
	return b.s.deleteKeys(ctx, b.table, ks)
}
