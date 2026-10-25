package catalog

import (
	"encoding/json"
	"sort"
)

// KindMalformed marks a persisted item a Backend could not read as an Entry
// at all (missing key, kind or definition). Refresh quarantines it.
const KindMalformed = "malformed"

// Quarantined describes a persisted catalog entry Refresh could not decode
// (e.g. written by a newer build with a type this build cannot parse). It is
// left out of the snapshot; every other entry still loads, so one bad entry
// cannot halt the catalog, the stream workers or startup. The identity
// fields are best effort (empty when even they cannot be read).
type Quarantined struct {
	Key  string
	Kind string
	// Database and Name of the object.
	Database, Name string
	// PhysicalName of a table ("" unknown).
	PhysicalName string
	// SourceDatabase and SourceTable of a view ("" unknown).
	SourceDatabase, SourceTable string
	Err                         error
}

// Quarantiner is implemented by catalogs that quarantine undecodable
// entries (the catalog returned by New).
type Quarantiner interface {
	// Quarantined lists the entries the last Refresh could not decode,
	// sorted by key.
	Quarantined() []Quarantined
}

// QuarantinedEntries returns c's quarantined entries (nil when c does not
// quarantine).
func QuarantinedEntries(c Catalog) []Quarantined {
	if q, ok := c.(Quarantiner); ok {
		return q.Quarantined()
	}
	return nil
}

// quarantine builds the record of an entry that failed to decode.
func quarantine(e Entry, err error) Quarantined {
	q := Quarantined{Key: e.Key, Kind: e.Kind, Err: err}
	var id struct {
		Database       string `json:"database"`
		Name           string `json:"name"`
		PhysicalName   string `json:"physical_name"`
		SourceDatabase string `json:"source_database"`
		SourceTable    string `json:"source_table"`
	}
	if json.Unmarshal(e.Def, &id) == nil {
		q.Database, q.Name, q.PhysicalName = id.Database, id.Name, id.PhysicalName
		q.SourceDatabase, q.SourceTable = id.SourceDatabase, id.SourceTable
	}
	return q
}

func (s *store) Quarantined() []Quarantined {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.snap.quarantine) == 0 {
		return nil
	}
	out := make([]Quarantined, 0, len(s.snap.quarantine))
	for _, q := range s.snap.quarantine {
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

var _ Quarantiner = (*store)(nil)
