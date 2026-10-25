package dynamo

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/storage"
)

// Columns added by ALTER ADD COLUMN are missing from older items. With
// ScanRequest.MarkAbsent the store must say so (storage.Absent) for columns
// that have a DEFAULT/MATERIALIZED expression, so the engine can compute the
// default like ClickHouse; other columns keep decoding to the zero value.
func TestDecodeRowMarksAbsentDefaults(t *testing.T) {
	tbl := &catalog.Table{
		Database: "default", Name: "tenants", Engine: catalog.EngineMergeTree,
		Columns: cols("tenant_id", "String", "seats", "UInt64", "region", "String", "seat_cap", "UInt64", "note", "String", "m", "UInt64"),
		OrderBy: []string{"tenant_id"},
	}
	tbl.Columns[2].DefaultKind, tbl.Columns[2].DefaultExpr = "DEFAULT", "'us-east-1'"
	tbl.Columns[3].DefaultKind, tbl.Columns[3].DefaultExpr = "DEFAULT", "seats * 2"
	tbl.Columns[5].DefaultKind, tbl.Columns[5].DefaultExpr = "MATERIALIZED", "seats + 1"
	l := testLayout(t, tbl)
	s := &storeImpl{opts: Options{Prefix: "x_", Region: "r1"}}
	// An item written before region/seat_cap/note/m existed.
	item := map[string]any{"tenant_id": "acme", "seats": uint64(10)}
	old := itemFor(t, l, item)

	// Default: zero values (callers that do not handle Absent).
	row, err := s.decodeRow(context.Background(), l, old, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if row[2] != "" || row[3] != uint64(0) || row[4] != "" || row[5] != uint64(0) {
		t.Fatalf("without mask: %#v", row)
	}

	plan := s.newReadPlan(l, storage.ScanRequest{MarkAbsent: true})
	row, err = s.decodeRow(context.Background(), l, old, plan.want, plan.absent)
	if err != nil {
		t.Fatal(err)
	}
	if row[0] != "acme" || row[1] != uint64(10) {
		t.Fatalf("stored values: %#v", row)
	}
	if !storage.IsAbsent(row[2]) || !storage.IsAbsent(row[3]) || !storage.IsAbsent(row[5]) {
		t.Fatalf("DEFAULT/MATERIALIZED columns missing from the item must be Absent: %#v", row)
	}
	if row[4] != "" {
		t.Fatalf("column without default must decode to its zero value: %#v", row[4])
	}

	// Projection: only requested columns are marked.
	plan = s.newReadPlan(l, storage.ScanRequest{MarkAbsent: true, Columns: []int{0, 3}})
	row, err = s.decodeRow(context.Background(), l, old, plan.want, plan.absent)
	if err != nil {
		t.Fatal(err)
	}
	if row[2] != nil || !storage.IsAbsent(row[3]) {
		t.Fatalf("projected: %#v", row)
	}

	// Present attributes (rows written after the ALTER) are never Absent,
	// whatever their value.
	item = map[string]any{"tenant_id": "hooli", "seats": uint64(100), "region": "", "seat_cap": uint64(0), "note": "x", "m": uint64(0)}
	plan = s.newReadPlan(l, storage.ScanRequest{MarkAbsent: true})
	row, err = s.decodeRow(context.Background(), l, itemFor(t, l, item), plan.want, plan.absent)
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range row {
		if storage.IsAbsent(v) {
			t.Fatalf("column %d of a full item decoded as Absent", i)
		}
	}
}

func TestSameRowIgnoresAbsent(t *testing.T) {
	stored := storage.Row{"acme", uint64(10), storage.Absent}
	if !sameRow(stored, storage.Row{"acme", uint64(10), "us-east-1"}) {
		t.Fatal("Absent stored column must match the engine's computed default")
	}
	if sameRow(stored, storage.Row{"acme", uint64(11), "us-east-1"}) {
		t.Fatal("other columns must still be compared")
	}
}

// itemFor encodes the given column values (by name) into an item; columns
// not in vals are left out, like items written before an ALTER ADD COLUMN.
func itemFor(t *testing.T, l *layout, vals map[string]any) map[string]types.AttributeValue {
	t.Helper()
	item := map[string]types.AttributeValue{attrPK: attrB([]byte{1}), attrSK: attrB([]byte{1})}
	for i := range l.cols {
		v, ok := vals[l.cols[i].name]
		if !ok {
			continue
		}
		av, err := encodeValue(l.cols[i].typ, v)
		if err != nil {
			t.Fatal(err)
		}
		item[l.cols[i].name] = av
	}
	return item
}
