package catalog

import (
	"reflect"
	"testing"
	"time"
)

// TestHomeRegionResetAtAndPrevTypes covers the catalog fields the store
// relies on for GLOBAL_MODE=aws table creation, TRUNCATE propagation and
// reads after MODIFY COLUMN.
func TestHomeRegionResetAtAndPrevTypes(t *testing.T) {
	b := newMemBackend()
	c := New(b, Options{ParseType: testParse, Region: "eu-west-1"})
	src := eventsTable("default", "ev")
	src.HomeRegion, src.ResetAt = "us-east-1", time.Now() // e.g. copied by CREATE TABLE ... AS
	must(t, c.CreateTable(ctx, src, false))
	got, _ := c.Table("default", "ev")
	if got.HomeRegion != "eu-west-1" || !got.ResetAt.IsZero() {
		t.Fatalf("create: home %q reset %v", got.HomeRegion, got.ResetAt)
	}

	// TRUNCATE: MarkTruncated + UpdateTable persists a strictly newer ResetAt.
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tr := MarkTruncated(got, at)
	if !tr.ResetAt.Equal(at) || !got.ResetAt.IsZero() {
		t.Fatal("MarkTruncated must stamp a copy")
	}
	must(t, c.UpdateTable(ctx, tr))
	if again := MarkTruncated(tr, at); !again.ResetAt.After(at) {
		t.Fatal("ResetAt must increase")
	}
	got, _ = c.Table("default", "ev")
	if !got.ResetAt.Equal(at) || got.HomeRegion != "eu-west-1" {
		t.Fatalf("after truncate: %+v", got)
	}
	// An ALTER built from an older snapshot does not undo it; MODIFY COLUMN
	// records the previous type.
	alter := cloneTable(got)
	alter.ResetAt, alter.HomeRegion = time.Time{}, "x"
	alter.Columns[3] = col("body", "String")
	must(t, c.UpdateTable(ctx, alter))
	alter = cloneTable(mustTable(t, c))
	alter.Columns[3] = col("body", "UInt64")
	must(t, c.UpdateTable(ctx, alter))
	got = mustTable(t, c)
	if !got.ResetAt.Equal(at) || got.HomeRegion != "eu-west-1" {
		t.Fatalf("after ALTER: reset %v home %q", got.ResetAt, got.HomeRegion)
	}
	if want := []string{"Nullable(String)", "String"}; !reflect.DeepEqual(got.Columns[3].PrevTypes, want) {
		t.Fatalf("prev types %v, want %v", got.Columns[3].PrevTypes, want)
	}
	if got.Columns[0].PrevTypes != nil {
		t.Fatalf("unchanged column has prev types %v", got.Columns[0].PrevTypes)
	}
	// Back to an earlier type: it is no longer a previous type.
	alter = cloneTable(got)
	alter.Columns[3] = col("body", "String")
	must(t, c.UpdateTable(ctx, alter))
	if p := mustTable(t, c).Columns[3].PrevTypes; !reflect.DeepEqual(p, []string{"Nullable(String)", "UInt64"}) {
		t.Fatalf("prev types after revert %v", p)
	}

	// Everything survives a reload from the backend.
	c2 := New(b, Options{ParseType: testParse})
	must(t, c2.Refresh(ctx))
	re, _ := c2.Table("default", "ev")
	want := mustTable(t, c)
	if re.HomeRegion != want.HomeRegion || !re.ResetAt.Equal(want.ResetAt) ||
		!reflect.DeepEqual(re.Columns[3].PrevTypes, want.Columns[3].PrevTypes) {
		t.Fatalf("reloaded %+v", re)
	}
}

func mustTable(t *testing.T, c Catalog) *Table {
	t.Helper()
	tb, err := c.Table("default", "ev")
	if err != nil {
		t.Fatal(err)
	}
	return tb
}
