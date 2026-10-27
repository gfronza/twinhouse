package memtrack

import (
	"fmt"
	"math/bits"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// LimitExceeded is ClickHouse 25.8's per-query MEMORY_LIMIT_EXCEEDED (241),
// e.g. "Query memory limit exceeded: would use 21.37 MiB (attempt to
// allocate chunk of 4.17 MiB bytes), maximum: 19.07 MiB". ClickHouse also
// names the processor that allocated (": While executing ..."), which
// twinhouse does not have.
func LimitExceeded(wouldUse, chunk, limit int64) *cherr.Error {
	return cherr.New(cherr.MemoryLimitExceeded,
		"Query memory limit exceeded: would use %s (attempt to allocate chunk of %s bytes), maximum: %s",
		ReadableSize(wouldUse), ReadableSize(chunk), ReadableSize(limit))
}

var sizeUnits = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

// ReadableSize formats n bytes like ClickHouse's
// formatReadableSizeWithBinarySuffix: two decimals, rounded half up, in the
// largest binary unit not above n ("0.00 B", "1.50 KiB", "1.00 GiB").
func ReadableSize(n int64) string {
	b := uint64(max(n, 0))
	k := 0
	for k+1 < len(sizeUnits) && b>>(10*(k+1)) > 0 {
		k++
	}
	unit := uint64(1) << (10 * k)
	// hundredths = round(b * 100 / unit), computed in 128 bits.
	hi, lo := bits.Mul64(b, 200)
	lo, carry := bits.Add64(lo, unit, 0)
	hundredths, _ := bits.Div64(hi+carry, lo, 2*unit)
	return fmt.Sprintf("%d.%02d %s", hundredths/100, hundredths%100, sizeUnits[k])
}
