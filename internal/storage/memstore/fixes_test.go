package memstore

import (
	"context"
	"io"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/storage"
)

// TestMarkAbsent: an attribute missing from an item (column added by ALTER
// after the write) is storage.Absent only when requested and only for columns
// with a stored default; otherwise it reads as the zero value.
func TestMarkAbsent(t *testing.T) {
	tb := mkTable(catalog.EngineMergeTree, [][2]string{{"k", "String"}}, []string{"k"}, nil)
	s := newStore(t, tb)
	if _, err := s.Insert(context.Background(), tb, []storage.Row{{"a"}}); err != nil {
		t.Fatal(err)
	}
	alt := *tb
	alt.Columns = append(append([]catalog.Column{}, tb.Columns...),
		catalog.Column{Name: "lvl", Type: chtype.TUInt8, TypeText: "UInt8", DefaultKind: "DEFAULT", DefaultExpr: "3"},
		catalog.Column{Name: "plain", Type: chtype.TUInt8, TypeText: "UInt8"})
	rows := scan(t, s, &alt, storage.ScanRequest{})
	eq(t, rows, []storage.Row{{"a", uint64(0), uint64(0)}})
	rows = scan(t, s, &alt, storage.ScanRequest{MarkAbsent: true})
	if len(rows) != 1 || !storage.IsAbsent(rows[0][1]) || rows[0][2] != uint64(0) {
		t.Errorf("MarkAbsent scan: %#v", rows)
	}
	// Absent attributes match any value on DeleteRows.
	if err := s.DeleteRows(context.Background(), &alt, []storage.Row{{"a", uint64(3), uint64(0)}}); err != nil {
		t.Fatal(err)
	}
	eq(t, len(scan(t, s, &alt, storage.ScanRequest{})), 0)
}

func TestScanRowIDs(t *testing.T) {
	tb := mkTable(catalog.EngineMergeTree, [][2]string{{"k", "String"}}, []string{"k"}, nil)
	s := newStore(t, tb)
	res, err := s.Insert(context.Background(), tb, []storage.Row{{"a"}, {"b"}})
	if err != nil {
		t.Fatal(err)
	}
	it, err := s.Scan(context.Background(), tb, storage.ScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	ri := it.(interface{ RowID() string })
	var got []string
	for {
		if _, err := it.Next(context.Background()); err == io.EOF {
			break
		}
		got = append(got, ri.RowID())
	}
	eq(t, got, res.RowIDs)
}

func TestReplacingIsDeletedHidden(t *testing.T) {
	tb := mkTable(catalog.EngineReplacingMergeTree, [][2]string{{"k", "String"}, {"v", "UInt64"}, {"d", "UInt8"}}, []string{"k"}, nil)
	tb.VersionColumn = "v"
	tb.EngineArgs = []string{"v", "d"}
	s := newStore(t, tb)
	ins := func(rows ...storage.Row) {
		if _, err := s.Insert(context.Background(), tb, rows); err != nil {
			t.Fatal(err)
		}
	}
	ins(storage.Row{"a", uint64(1), uint64(0)}, storage.Row{"b", uint64(1), uint64(0)})
	ins(storage.Row{"a", uint64(2), uint64(1)})
	eq(t, scan(t, s, tb, storage.ScanRequest{}), []storage.Row{{"b", uint64(1), uint64(0)}})
	ins(storage.Row{"a", uint64(1), uint64(0)}) // older than the deletion: ignored
	eq(t, len(scan(t, s, tb, storage.ScanRequest{})), 1)
	ins(storage.Row{"a", uint64(3), uint64(0)})
	eq(t, len(scan(t, s, tb, storage.ScanRequest{Limit: 1, Ranges: []storage.KeyRange{{Partition: []any{"a"}}}})), 1)
}
