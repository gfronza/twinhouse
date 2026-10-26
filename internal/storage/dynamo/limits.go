package dynamo

import (
	"math/big"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

var bigOne = big.NewInt(1)

// checkTable is the catalog's storage check of a table definition
// (catalog.Options.CheckTable), run at CREATE TABLE and ALTER TABLE.
func (s *storeImpl) checkTable(t *catalog.Table) error {
	if t.Engine != catalog.EngineAggregatingMergeTree && t.Engine != catalog.EngineSummingMergeTree {
		return nil
	}
	l, err := s.layoutOf(t)
	if err != nil {
		return err
	}
	return s.checkExpressionLimits(l)
}

// checkExpressionLimits rejects a merging table whose row update cannot be
// expressed: one merge writes every column of the row in a single
// UpdateExpression (inside the exactly-once marker transaction, which
// allows one action per item, so it cannot be split), and DynamoDB caps
// each expression at 4 KB. The bound is the longest update the layout can
// produce (every column present, with placeholders of its real width).
// Reads are not limited: a projection that would be too long reads whole
// items instead (see newReadPlan).
func (s *storeImpl) checkExpressionLimits(l *layout) error {
	if !l.merging {
		return nil
	}
	n, merged := worstCaseUpdateLen(s, l)
	if n > maxExpressionLen {
		return cherr.New(cherr.NotImplemented,
			"table %s has too many merged columns for one DynamoDB update (%d columns need a %d-byte UpdateExpression; DynamoDB allows %d bytes): "+
				"about 500 summed/count, 250 avg or 150 any/anyLast/groupArray columns fit; use fewer columns or split the table", l.t.FullName(), merged, n, maxExpressionLen)
	}
	return nil
}

// worstCaseUpdateLen returns the length of the longest step-1 update of l
// (and the number of non-key columns it writes).
func worstCaseUpdateLen(s *storeImpl, l *layout) (int, int) {
	dummy := attrN("0")
	p := &rowPlan{}
	add := func(attr string, t chtype.Type) {
		if fl, bits := floatSum(t); fl {
			p.ops = append(p.ops, attrOp{attr: attr, kind: opAddFloat, fs: []float64{1}, bits: bits})
		} else {
			p.ops = append(p.ops, attrOp{attr: attr, kind: opAddInt, n: bigOne})
		}
	}
	merged := 0
	for i := range l.cols {
		c := &l.cols[i]
		if c.key {
			p.ops = append(p.ops, attrOp{attr: c.name, kind: opSet, av: dummy})
			continue
		}
		merged++
		switch c.kind {
		case colAggState:
			switch c.agg.Func {
			case catalog.AggCount:
				add(c.name+sufCount, chtype.Type{Kind: chtype.UInt64})
			case catalog.AggSum, catalog.AggAvg:
				add(c.name+sufSum, aggSumType(c.agg))
				if c.agg.Func == catalog.AggAvg {
					add(c.name+sufCount, chtype.Type{Kind: chtype.UInt64})
				}
			case catalog.AggAny, catalog.AggAnyLast:
				p.ops = append(p.ops, attrOp{attr: c.name + sufValue, kind: opIfAbsent, av: dummy},
					attrOp{attr: c.name + sufHas, kind: opSet, av: attrTrue})
			case catalog.AggGroupArray:
				p.ops = append(p.ops, attrOp{attr: c.name + sufList, kind: opAppend})
			}
		case colSimpleAgg:
			switch c.saf {
			case catalog.AggSum:
				add(c.name, c.typ)
			case catalog.AggAny, catalog.AggAnyLast:
				p.ops = append(p.ops, attrOp{attr: c.name, kind: opIfAbsent, av: dummy})
			case catalog.AggGroupArray:
				p.ops = append(p.ops, attrOp{attr: c.name, kind: opAppend})
			}
		default:
			if c.summed {
				add(c.name, c.typ)
			} else {
				p.ops = append(p.ops, attrOp{attr: c.name, kind: opIfAbsent, av: dummy})
			}
		}
	}
	u := groupUpdate([]*rowPlan{p})
	cur := map[string]types.AttributeValue{attrCAS: attrS("0")}
	worst := 0
	for _, marked := range []bool{true, false} {
		e, cond, _, err := s.render(l, u, constantKey, constantKey, marked, cur)
		if err != nil {
			continue
		}
		worst = max(worst, len(aws.ToString(e.updateExpression())), len(aws.ToString(cond)))
	}
	return worst, merged
}
