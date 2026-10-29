package format

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// prettyRowsOf returns n rows of (UInt64, String), counting from first.
func prettyRowsOf(first, n int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{uint64(first + i), fmt.Sprintf("value-%d", first+i)}
	}
	return rows
}

// Pretty charges the rows it keeps (up to output_format_pretty_max_rows)
// through Options.Charge after each block, keeps the charge level while
// the last rows replace each other, and releases it all in Finish.
func TestPrettyChargesTheRowsItKeeps(t *testing.T) {
	for _, name := range []string{"Pretty", "PrettyCompact"} {
		var held int64
		calls := 0
		opts := Options{
			Settings: map[string]string{"output_format_pretty_max_rows": "100"},
			Charge:   func(n int64) error { held += n; calls++; return nil },
		}
		var buf bytes.Buffer
		enc, err := NewEncoder(name, &buf, []Column{col("n", "UInt64"), col("s", "String")}, opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteRows(prettyRowsOf(0, 50)); err != nil {
			t.Fatal(err)
		}
		half := held
		if half < 50*64 {
			t.Errorf("%s: 50 kept rows charged %d bytes", name, half)
		}
		if err := enc.WriteRows(prettyRowsOf(50, 50)); err != nil {
			t.Fatal(err)
		}
		full := held
		if full < 2*half-half/4 {
			t.Errorf("%s: 100 kept rows charged %d bytes, 50 rows %d", name, full, half)
		}
		for i := range 20 {
			if err := enc.WriteRows(prettyRowsOf(100+i*1000, 1000)); err != nil {
				t.Fatal(err)
			}
		}
		if held < full*9/10 || held > full*11/10 {
			t.Errorf("%s: after 20100 rows %d bytes charged, %d with the first 100", name, held, full)
		}
		if calls > 2+20 {
			t.Errorf("%s: charged %d times for 22 blocks", name, calls)
		}
		if err := enc.Finish(Stats{}); err != nil {
			t.Fatal(err)
		}
		if held != 0 {
			t.Errorf("%s: %d bytes still charged after Finish", name, held)
		}
	}
}

// A failing charge fails the block, and nothing is charged without kept
// rows (output_format_pretty_max_rows = 0).
func TestPrettyChargeFailureFailsTheWrite(t *testing.T) {
	limit := errors.New("memory limit")
	var buf bytes.Buffer
	enc, err := NewEncoder("PrettyCompact", &buf, []Column{col("n", "UInt64"), col("s", "String")},
		Options{Charge: func(n int64) error { return limit }})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteRows(prettyRowsOf(0, 10)); !errors.Is(err, limit) {
		t.Errorf("WriteRows: %v", err)
	}
	calls := 0
	enc, err = NewEncoder("PrettyCompact", &buf, []Column{col("n", "UInt64")},
		Options{Settings: map[string]string{"output_format_pretty_max_rows": "0"}, Charge: func(int64) error { calls++; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteRows([][]any{{uint64(1)}, {uint64(2)}}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(Stats{}); err != nil || calls != 0 {
		t.Errorf("Finish %v, %d charges", err, calls)
	}
}
