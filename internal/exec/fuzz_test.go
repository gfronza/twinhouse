package exec

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// FuzzExecute checks that no statement makes the engine panic.
func FuzzExecute(f *testing.F) {
	for _, c := range oracleCorpus {
		f.Add(c.query)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		te := newTestEnv(t)
		for _, name := range []string{"events", "nums", "joins"} {
			for _, st := range fixtures[name] {
				te.exec(st)
			}
		}
		// Bounded: some valid queries never end (system.numbers without LIMIT).
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := te.e.Execute(ctx, &Request{Query: sql, Settings: map[string]string{"max_result_rows": "10000"}})
		if ce := cherr.As(err); ce != nil && ce.Code == cherr.LogicalError && strings.HasPrefix(ce.Msg, "internal error") {
			t.Fatalf("panic for %q: %v\n%v", sql, ce.Msg, ce.Cause)
		}
	})
}
