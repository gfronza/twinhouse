package dynamo

import (
	"context"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/storage"
)

func itoa64(n int64) string { return strconv.FormatInt(n, 10) }

func attrB(b []byte) types.AttributeValue  { return &types.AttributeValueMemberB{Value: b} }
func attrS(s string) types.AttributeValue  { return &types.AttributeValueMemberS{Value: s} }
func attrN(s string) types.AttributeValue  { return &types.AttributeValueMemberN{Value: s} }
func attrBool(b bool) types.AttributeValue { return &types.AttributeValueMemberBOOL{Value: b} }

// expr accumulates an expression's placeholders and clauses. Placeholders
// are short (base-36 indexes) and clauses are joined without spaces:
// DynamoDB caps every expression string at 4 KB, which bounds how many
// columns one update or projection can name (see checkExpressionLimits).
type expr struct {
	names  map[string]string
	byAttr map[string]string // attribute name -> placeholder
	values map[string]types.AttributeValue
	set    []string
	add    []string
	remove []string
}

func newExpr() *expr {
	return &expr{names: map[string]string{}, byAttr: map[string]string{}, values: map[string]types.AttributeValue{}}
}

// n returns the placeholder of an attribute name.
func (e *expr) n(attr string) string {
	if p, ok := e.byAttr[attr]; ok {
		return p
	}
	p := "#" + strconv.FormatInt(int64(len(e.names)), 36)
	e.names[p] = attr
	e.byAttr[attr] = p
	return p
}

// v returns a fresh placeholder bound to av.
func (e *expr) v(av types.AttributeValue) string {
	p := ":" + strconv.FormatInt(int64(len(e.values)), 36)
	e.values[p] = av
	return p
}

func (e *expr) updateExpression() *string {
	var parts []string
	if len(e.set) > 0 {
		parts = append(parts, "SET "+strings.Join(e.set, ","))
	}
	if len(e.add) > 0 {
		parts = append(parts, "ADD "+strings.Join(e.add, ","))
	}
	if len(e.remove) > 0 {
		parts = append(parts, "REMOVE "+strings.Join(e.remove, ","))
	}
	if len(parts) == 0 {
		return nil
	}
	return aws.String(strings.Join(parts, " "))
}

func (e *expr) nameMap() map[string]string {
	if len(e.names) == 0 {
		return nil
	}
	return e.names
}

func (e *expr) valueMap() map[string]types.AttributeValue {
	if len(e.values) == 0 {
		return nil
	}
	return e.values
}

// attrSize is the size DynamoDB bills and limits for an attribute value
// (its name excluded): strings and binaries by length, numbers by
// significant digits, lists and maps with 3 bytes of overhead plus 1 per
// element.
func attrSize(av types.AttributeValue) int {
	switch x := av.(type) {
	case *types.AttributeValueMemberS:
		return len(x.Value)
	case *types.AttributeValueMemberB:
		return len(x.Value)
	case *types.AttributeValueMemberN:
		return numberSize(x.Value)
	case *types.AttributeValueMemberL:
		n := 3
		for _, e := range x.Value {
			n += attrSize(e) + 1
		}
		return n
	case *types.AttributeValueMemberM:
		n := 3
		for k, e := range x.Value {
			n += len(k) + attrSize(e) + 1
		}
		return n
	case *types.AttributeValueMemberSS:
		n := 0
		for _, e := range x.Value {
			n += len(e)
		}
		return n
	case *types.AttributeValueMemberNS:
		n := 0
		for _, e := range x.Value {
			n += numberSize(e)
		}
		return n
	case *types.AttributeValueMemberBS:
		n := 0
		for _, e := range x.Value {
			n += len(e)
		}
		return n
	}
	return 1 // BOOL, NULL
}

// numberSize is a number's size: 1 byte per two significant digits plus 1.
func numberSize(s string) int {
	digits := 0
	lead := true
	trail := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == 'e' || c == 'E' {
			break
		}
		if c < '0' || c > '9' {
			continue
		}
		if lead && c == '0' {
			continue
		}
		lead = false
		digits++
		if c == '0' {
			trail++
		} else {
			trail = 0
		}
	}
	digits -= trail
	return min((digits+1)/2+1, 21)
}

// itemSize is an item's size as DynamoDB limits it (names included).
func itemSize(item map[string]types.AttributeValue) int {
	n := 0
	for k, av := range item {
		n += len(k) + attrSize(av)
	}
	return n
}

// checkItemSize rejects an item over DynamoDB's 400 KB limit before any
// write of the statement is sent (a rejected batch would leave the rows of
// the other batches written).
func checkItemSize(l *layout, row int, item map[string]types.AttributeValue) error {
	if n := itemSize(item); n > maxItemSize {
		return cherr.New(cherr.BadArguments, "row %d written to %s is %d bytes as a DynamoDB item; DynamoDB items are limited to %d bytes (400 KB, attribute names included)",
			row+1, l.t.FullName(), n, maxItemSize)
	}
	return nil
}

func itemKey(pk, sk []byte) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{attrPK: attrB(pk), attrSK: attrB(sk)}
}

// rowItem builds the full item of a row of a non-merging table written at
// ts (unix nanos, see storeImpl.stamp).
func (s *storeImpl) rowItem(l *layout, row storage.Row, pk, sk []byte, rid string, mv bool, ts int64) (map[string]types.AttributeValue, error) {
	item := make(map[string]types.AttributeValue, len(row)+7)
	item[attrPK], item[attrSK] = attrB(pk), attrB(sk)
	item[attrRID] = attrS(rid)
	item[attrTS] = attrN(itoa64(ts))
	item[attrOrigin] = attrS(s.opts.Region)
	if mv {
		item[attrMV] = attrTrue
	}
	for i := range l.cols {
		av, err := encodeValue(l.cols[i].typ, row[i])
		if err != nil {
			return nil, columnErr(l, l.cols[i].name, err)
		}
		item[l.cols[i].name] = av
	}
	return item, nil
}

// decodeColumn decodes a stored column value. A value the column's type
// cannot decode was written before ALTER TABLE MODIFY COLUMN changed the
// type (MODIFY is catalog-only): it is decoded with the newest previous
// type that accepts it and converted like CAST, as ClickHouse's MODIFY
// mutation converts stored parts (Int64 5 -> String '5', Float64 2.7 ->
// Int32 2, String '7' -> UInt8 7, DateTime -> String
// '2024-01-01 13:00:00'). Values the new type decodes directly need no
// conversion: integer width changes wrap like ClickHouse, and dates and
// times are truncated to the type's precision by decodeValue.
func decodeColumn(c *colSpec, av types.AttributeValue) (any, error) {
	v, err := decodeValue(c.typ, av)
	if err == nil || len(c.prev) == 0 {
		return v, err
	}
	for j := len(c.prev) - 1; j >= 0; j-- {
		pv, perr := decodeValue(c.prev[j], av)
		if perr != nil {
			continue
		}
		cv, cerr := chtype.Convert(pv, c.prev[j], c.typ)
		if cerr != nil {
			return nil, cherr.Wrap(cherr.As(cerr).Code, cerr, "cannot convert a value stored as %s to %s", typeName(c.prev[j]), typeName(c.typ))
		}
		return cv, nil
	}
	return nil, err
}

// columnErr prefixes err with the table and column it concerns.
func columnErr(l *layout, col string, err error) error {
	ce := cherr.As(err)
	return &cherr.Error{Code: ce.Code, Msg: "table " + l.t.FullName() + " column " + col + ": " + ce.Msg, Cause: err}
}

// decodeRow rebuilds a row from an item; want selects columns (nil = all).
// absent (nil = none) selects the columns that decode to storage.Absent,
// instead of their zero value, when their attribute is missing from the item
// (see absentMask).
func (s *storeImpl) decodeRow(ctx context.Context, l *layout, item map[string]types.AttributeValue, want, absent []bool) (storage.Row, error) {
	row := make(storage.Row, len(l.cols))
	var pk, sk []byte
	if b, ok := item[attrPK].(*types.AttributeValueMemberB); ok {
		pk = b.Value
	}
	if b, ok := item[attrSK].(*types.AttributeValueMemberB); ok {
		sk = b.Value
	}
	for i := range l.cols {
		if want != nil && !want[i] {
			continue
		}
		c := &l.cols[i]
		var err error
		switch {
		case c.kind == colAggState:
			row[i], err = s.decodeAggState(l, c, item, pk, sk)
		case c.set:
			row[i], err = s.loadMembers(ctx, l, pk, sk, c)
		default:
			av := item[c.name]
			if av == nil && absent != nil && absent[i] {
				row[i] = storage.Absent
				continue
			}
			row[i], err = decodeColumn(c, av)
		}
		if err != nil {
			return nil, columnErr(l, c.name, err)
		}
	}
	return row, nil
}

// absentMask returns which columns of l decode to storage.Absent when their
// attribute is missing (columns added by ALTER after the item was written):
// stored-value columns (not AggregateFunction states or member sets) for
// which storage.MarksAbsent holds, restricted to want (nil = all). It
// returns nil when no column qualifies.
func absentMask(l *layout, want []bool) []bool {
	var m []bool
	for i := range l.cols {
		c := &l.cols[i]
		if (want != nil && !want[i]) || c.kind == colAggState || c.set || c.key || !storage.MarksAbsent(&l.t.Columns[i]) {
			continue
		}
		if m == nil {
			m = make([]bool, len(l.cols))
		}
		m[i] = true
	}
	return m
}

func (s *storeImpl) decodeAggState(l *layout, c *colSpec, item map[string]types.AttributeValue, pk, sk []byte) (*chtype.AggPartial, error) {
	p := &chtype.AggPartial{Func: c.agg.Func, ArgTypes: c.agg.Args}
	num := func(suffix string, kind chtype.Kind) (any, error) {
		t := chtype.Type{Kind: kind}
		return decodeValue(t, item[c.name+suffix])
	}
	hasValue := func() bool {
		b, ok := item[c.name+sufHas].(*types.AttributeValueMemberBOOL)
		return ok && b.Value
	}
	switch c.agg.Func {
	case catalog.AggCount:
		n, err := num(sufCount, chtype.UInt64)
		if err != nil {
			return nil, err
		}
		p.Count = n.(uint64)
	case catalog.AggSum, catalog.AggAvg:
		sum, err := num(sufSum, c.agg.SumKind())
		if err != nil {
			return nil, err
		}
		p.Sum = sum
		if c.agg.Func == catalog.AggAvg {
			n, err := num(sufCount, chtype.UInt64)
			if err != nil {
				return nil, err
			}
			p.Count = n.(uint64)
		}
	case catalog.AggMin, catalog.AggMax, catalog.AggAny, catalog.AggAnyLast:
		if p.HasValue = hasValue(); p.HasValue {
			v, err := decodeValue(c.agg.Args[0], item[c.name+sufValue])
			if err != nil {
				return nil, err
			}
			p.Value = v
		}
	case catalog.AggArgMin, catalog.AggArgMax:
		if p.HasValue = hasValue(); p.HasValue {
			v, err := decodeValue(c.agg.Args[0], item[c.name+sufValue])
			if err != nil {
				return nil, err
			}
			k, err := decodeValue(c.agg.Args[1], item[c.name+sufKey])
			if err != nil {
				return nil, err
			}
			p.Value, p.Key = v, k
		}
	case catalog.AggUniq, catalog.AggUniqExact, catalog.AggGroupUniqArray:
		n, err := num(sufSize, chtype.UInt64)
		if err != nil {
			return nil, err
		}
		p.SetSize = n.(uint64)
		if p.SetSize == 0 {
			p.Set, p.SetLoaded = []any{}, true
		} else {
			pkc, skc := append([]byte(nil), pk...), append([]byte(nil), sk...)
			p.LoadSet = func(ctx context.Context) ([]any, error) { return s.loadMembers(ctx, l, pkc, skc, c) }
		}
	case catalog.AggGroupArray:
		p.List = []any{}
		if av := item[c.name+sufList]; av != nil {
			v, err := decodeValue(chtype.NewArray(c.agg.Args[0]), av)
			if err != nil {
				return nil, err
			}
			p.List = v.([]any)
		}
	}
	return p, nil
}

// loadMembers reads every member of (row, column) from the members table.
func (s *storeImpl) loadMembers(ctx context.Context, l *layout, pk, sk []byte, c *colSpec) ([]any, error) {
	in := &dynamodb.QueryInput{
		TableName:                 aws.String(membersTable(l.physical)),
		KeyConditionExpression:    aws.String("#p = :p"),
		ExpressionAttributeNames:  map[string]string{"#p": attrPK, "#m": attrMember},
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": attrB(memberPK(pk, sk, c.name))},
		ProjectionExpression:      aws.String("#m"),
		ConsistentRead:            aws.Bool(true),
	}
	out := []any{}
	for {
		var page *dynamodb.QueryOutput
		err := withRetry(ctx, func() (err error) {
			page, err = s.ddb.Query(ctx, in)
			return err
		})
		if err != nil {
			return nil, ddbError(err, "cannot read set members of %s.%s", l.t.FullName(), c.name)
		}
		for _, it := range page.Items {
			v, err := decodeValue(c.memberType, it[attrMember])
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		if len(page.LastEvaluatedKey) == 0 {
			return out, nil
		}
		in.ExclusiveStartKey = page.LastEvaluatedKey
	}
}
