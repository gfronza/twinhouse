//go:build !race

package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/storage"
)

// copyingStore decodes fresh values on every scan, as DynamoDB does, so
// the rows a statement batches hold memory of their own.
type copyingStore struct{ storage.Store }

func (s copyingStore) Scan(ctx context.Context, t *catalog.Table, req storage.ScanRequest) (storage.RowIterator, error) {
	it, err := s.Store.Scan(ctx, t, req)
	if err != nil {
		return nil, err
	}
	return copyingIter{it}, nil
}

type copyingIter struct{ storage.RowIterator }

func (c copyingIter) Next(ctx context.Context) (storage.Row, error) {
	r, err := c.RowIterator.Next(ctx)
	if err != nil {
		return nil, err
	}
	out := make(storage.Row, len(r))
	for i, v := range r {
		if s, ok := v.(string); ok {
			v = strings.Clone(s)
		}
		out[i] = v
	}
	return out, nil
}

// POPULATE and DELETE batch the rows they read for the store. Charged,
// and ended at a quarter of the limit like an INSERT block, a batch of
// large rows stays well under the limit (1000 rows of these, the row
// bound, held 100 MB).
func TestStoreBatchesStayUnderTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("reads 100 MB of rows")
	}
	guardFunctionTest(t)
	te := newTestEnvConfig(t, func(c *Config) {
		c.Store, c.MaxMemoryUsage = copyingStore{c.Store}, testMaxMemoryUsage
	})
	te.exec("CREATE TABLE big (id UInt64, s String) ENGINE = MergeTree ORDER BY id")
	te.exec("INSERT INTO big SELECT number, repeat(toString(number % 10), 100000) FROM numbers(1000)")
	te.exec("CREATE TABLE sink (id UInt64, n UInt64) ENGINE = MergeTree ORDER BY id")
	const limit = 32000000
	settings := map[string]string{"max_memory_usage": "32000000"}
	for _, sql := range []string{
		"CREATE MATERIALIZED VIEW mvp TO sink POPULATE AS SELECT id, length(s) AS n FROM big",
		"DELETE FROM big WHERE id >= 0",
	} {
		peak, samples, err := syncPeakLive(te, &Request{Query: sql, Settings: settings})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if samples < 10 {
			t.Errorf("%s: %d samples: the batches are not charged", sql, samples)
		}
		t.Logf("%s: live heap grew by %.1f MiB", sql, float64(peak)/(1<<20))
		if peak > limit/2 {
			t.Errorf("%s: live heap grew by %d B, over half the limit of %d B", sql, peak, limit)
		}
	}
	te.expectRows("SELECT count(), sum(n) FROM sink", "1000\t100000000")
	te.expectRows("SELECT count() FROM big", "0")
}
