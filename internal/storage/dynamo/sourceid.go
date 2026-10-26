package dynamo

import (
	"strconv"
	"strings"

	"github.com/gfronza/twinhouse/internal/storage"
)

// Source row ids (storage.SourceRef.RowID) handed out by this store carry
// the source row's write time (_ts), so Merge can order view output and
// honour reset epochs without changing the callers (DESIGN.md §5):
//
//   - "<rid>@<ts>": returned by Insert and DecodeRecord. View output of the
//     row is subject to the target's reset epoch: a row written before the
//     target was created or last truncated is not applied (inline
//     application racing a TRUNCATE, and stream backlog replayed after it).
//   - "<rid>@=<ts>": returned by the scan iterator's RowID (POPULATE).
//     Never gated: POPULATE deliberately applies old rows.
//
// Any other id (no suffix) is used as is, with no write time. Exactly-once
// markers and MV-output keys use only <rid>, so every form of a row's id
// names the same source.
const (
	srcSep       = "@"
	srcUngatedOp = "="
)

func sourceID(rid string, ts int64, gated bool) string {
	if ts == 0 {
		return rid
	}
	if gated {
		return rid + srcSep + itoa64(ts)
	}
	return rid + srcSep + srcUngatedOp + itoa64(ts)
}

// parseSourceID splits a source row id into the stored row id and the
// write time it carries (0 if none); gated reports the "<rid>@<ts>" form.
func parseSourceID(id string) (rid string, ts int64, gated bool) {
	i := strings.LastIndex(id, srcSep)
	if i <= 0 {
		return id, 0, false
	}
	num := id[i+1:]
	gated = true
	if strings.HasPrefix(num, srcUngatedOp) {
		num, gated = num[1:], false
	}
	if num == "" || num[0] < '0' || num[0] > '9' {
		return id, 0, false
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n <= 0 {
		return id, 0, false
	}
	return id[:i], n, gated
}

// mergeSource is a MergeRow with its source id parsed.
type mergeSource struct {
	storage.MergeRow
	ts    int64 // source write time (0: unknown)
	gated bool
}

// parseMergeRows normalises the rows' SourceRef.RowID to the stored row id
// and drops rows whose gated source write time is before gate (unix nanos;
// 0 = no gate).
func parseMergeRows(rows []storage.MergeRow, gate int64) []mergeSource {
	out := make([]mergeSource, 0, len(rows))
	for _, mr := range rows {
		rid, ts, gated := parseSourceID(mr.Source.RowID)
		if gated && gate > 0 && ts < gate {
			continue
		}
		mr.Source.RowID = rid
		out = append(out, mergeSource{MergeRow: mr, ts: ts, gated: gated})
	}
	return out
}
