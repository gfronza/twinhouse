//go:build !race

package exec

import (
	"fmt"
	"strings"
	"testing"
)

// Planned constants give 241 before the live heap they take passes the
// limit by more than 8 MiB.
func TestPlannedConstantsStopAtTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("plans constants of about 64 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	var terms []string
	for i := range 8 {
		terms = append(terms, fmt.Sprintf("has(range(%d), number)", 250000+i))
	}
	wantLimitBeforeHeap(t, te, "SELECT count() FROM numbers(10) WHERE "+strings.Join(terms, " AND "), 32000000)
}
