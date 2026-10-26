package catalog

import (
	"context"
	"fmt"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// Entry kinds and key prefixes of persisted catalog items (DESIGN.md §4).
const (
	KindDatabase = "database"
	KindTable    = "table"
	KindView     = "view"
)

// DatabaseKey / TableKey / ViewKey return the backing-store key of an entry.
func DatabaseKey(name string) string       { return "db#" + name }
func TableKey(db, name string) string      { return "table#" + db + "." + name }
func ViewKey(db, name string) string       { return "view#" + db + "." + name }
func objectKeys(db, name string) [2]string { return [2]string{TableKey(db, name), ViewKey(db, name)} }

// Entry is one persisted catalog item.
type Entry struct {
	Key  string
	Kind string // KindDatabase | KindTable | KindView
	// Def is the JSON definition (see codec.go).
	Def []byte
	// Version is the table version (0 for databases and views); stored so the
	// backend can enforce optimistic concurrency on UpdateTable.
	Version int64
}

// Cond is a precondition of Backend.Put.
type Cond struct {
	Key string
	// Exists requires the item to exist (true) or to be absent (false).
	Exists bool
	// Version, when > 0 (and Exists), requires the stored version to equal it.
	Version int64
}

// CondFailedError reports that Backend.Put's Index-th condition failed (-1
// when the backend cannot tell which one).
type CondFailedError struct{ Index int }

func (e *CondFailedError) Error() string {
	return fmt.Sprintf("catalog: condition %d failed", e.Index)
}

// Backend persists catalog entries. Implementations must be safe for
// concurrent use; the DynamoDB one lives in storage/dynamo.
type Backend interface {
	// LoadAll returns every entry (strongly consistent).
	LoadAll(ctx context.Context) ([]Entry, error)
	// Put writes e atomically iff every condition holds; otherwise it returns
	// *CondFailedError and writes nothing.
	Put(ctx context.Context, e Entry, conds []Cond) error
	// Delete removes the entries (missing keys are ignored).
	Delete(ctx context.Context, keys []string) error
}

// Options configures a catalog.
type Options struct {
	// PhysicalName maps a table to its DynamoDB table name
	// (default: "<db>.<table>").
	PhysicalName func(db, table string) string
	// ParseType parses persisted column type text (default chtype.ParseStored).
	ParseType func(string) (chtype.Type, error)
	// Region is stamped as Table.HomeRegion by CreateTable ("" keeps the
	// caller's value).
	Region string
	// CheckTable, when set, is the storage layer's check of a table
	// definition (e.g. DynamoDB expression-size limits), run by CreateTable
	// and by UpdateTable after validation. UpdateTable only enforces it when
	// the previous definition passes, so an existing table that predates a
	// check can still be truncated or altered.
	CheckTable func(*Table) error
}
