package exec

import "testing"

// BenchmarkCollect collects a 1M-row result without a memory limit (no
// accounting) and with an 8 GiB one (accounting on, never reached).
func BenchmarkCollect(b *testing.B) {
	for _, c := range []struct{ name, settings string }{
		{"limit=0", ""},
		{"limit=8GiB", " SETTINGS max_memory_usage = 8589934592"},
	} {
		b.Run(c.name, func(b *testing.B) {
			te := newTestEnvConfig(b, func(*Config) {})
			sql := "SELECT number, toString(number) FROM numbers(1000000)" + c.settings
			for b.Loop() {
				te.exec(sql)
			}
		})
	}
}
