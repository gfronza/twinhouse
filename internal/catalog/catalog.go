package catalog

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// DefaultDatabase always exists, whether or not it has a persisted entry.
const DefaultDatabase = "default"

// snapshot is an immutable-object view of the catalog: stored *Table/*View/
// *Database values are never mutated after insertion (updates replace them),
// so callers may keep pointers they obtained.
type snapshot struct {
	dbs      map[string]*Database
	tables   map[string]map[string]*Table
	views    map[string]map[string]*View
	physical map[string]*Table
	// quarantine holds the entries the last Refresh could not decode, by
	// key (see Quarantined).
	quarantine map[string]Quarantined
}

func newSnapshot() *snapshot {
	return &snapshot{
		dbs:        map[string]*Database{},
		tables:     map[string]map[string]*Table{},
		views:      map[string]map[string]*View{},
		physical:   map[string]*Table{},
		quarantine: map[string]Quarantined{},
	}
}

func (s *snapshot) putTable(t *Table) {
	if old := s.tables[t.Database][t.Name]; old != nil {
		delete(s.physical, old.PhysicalName)
	}
	m := s.tables[t.Database]
	if m == nil {
		m = map[string]*Table{}
		s.tables[t.Database] = m
	}
	m[t.Name] = t
	s.physical[t.PhysicalName] = t
}

// putNewer is putTable unless the snapshot holds a newer version of the same
// incarnation (a replayed local mutation must not undo a later ALTER).
func (s *snapshot) putNewer(t *Table) {
	if old := s.tables[t.Database][t.Name]; old != nil && old.CreatedAt.Equal(t.CreatedAt) && old.Version > t.Version {
		return
	}
	s.putTable(t)
}

func (s *snapshot) deleteTable(db, name string) {
	if old := s.tables[db][name]; old != nil {
		delete(s.physical, old.PhysicalName)
		delete(s.tables[db], name)
	}
}

func (s *snapshot) putView(v *View) {
	m := s.views[v.Database]
	if m == nil {
		m = map[string]*View{}
		s.views[v.Database] = m
	}
	m[v.Name] = v
}

func (s *snapshot) hasDB(name string) bool {
	return name == DefaultDatabase || s.dbs[name] != nil
}

func (s *snapshot) hasObject(db, name string) bool {
	return s.tables[db][name] != nil || s.views[db][name] != nil
}

// store implements Catalog over a Backend (nil backend: memory only).
type store struct {
	backend     Backend
	physicalFor func(db, table string) string
	parse       func(string) (chtype.Type, error)
	now         func() time.Time
	region      string
	checkTable  func(*Table) error

	mu   sync.RWMutex
	snap *snapshot
	gen  uint64 // bumped by every local mutation
	// pending are the recent local mutations, replayed onto a snapshot
	// loaded by a Refresh that started before they were made (the load may
	// have missed them), so a concurrent Refresh never loses local DDL.
	pending []localMutation
}

// localMutation is one local change of the snapshot (see store.pending).
type localMutation struct {
	gen   uint64
	at    time.Time
	apply func(*snapshot)
}

// pendingKeep is how long local mutations stay replayable: longer than any
// Refresh can run (callers bound it, e.g. the reconciler's 1m timeout).
const pendingKeep = 5 * time.Minute

// mutateLocked applies f to the snapshot and records it for replay; s.mu
// must be held for writing.
func (s *store) mutateLocked(f func(*snapshot)) {
	f(s.snap)
	s.gen++
	now := s.now()
	s.pending = append(s.pending, localMutation{gen: s.gen, at: now, apply: f})
	i := 0
	for i < len(s.pending) && now.Sub(s.pending[i].at) > pendingKeep {
		i++
	}
	s.pending = s.pending[i:]
}

// New returns a Catalog persisted in b. Lookups are served from an in-memory
// snapshot; call Refresh to load entries written elsewhere.
func New(b Backend, opts Options) Catalog {
	s := &store{
		backend:     b,
		physicalFor: opts.PhysicalName,
		parse:       opts.ParseType,
		now:         time.Now,
		region:      opts.Region,
		checkTable:  opts.CheckTable,
		snap:        newSnapshot(),
	}
	if s.physicalFor == nil {
		s.physicalFor = func(db, table string) string { return db + "." + table }
	}
	if s.parse == nil {
		s.parse = chtype.Parse
	}
	return s
}

var _ Catalog = (*store)(nil)

// incarnationIDLen is the length of the per-incarnation physical-name
// suffix (lowercase base32, 40 random bits).
const incarnationIDLen = 8

// incarnationID returns a random physical-name suffix for a new table.
func incarnationID() string {
	var b [5]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("catalog: crypto/rand failed: " + err.Error())
	}
	return strings.ToLower(base32.StdEncoding.EncodeToString(b[:]))
}

// StripIncarnation removes the per-incarnation suffix (see CreateTable)
// from a physical table name relative to its "<prefix><db>." prefix, whose
// first component is the table name (which cannot contain '.'):
// "t.abcdefgh" -> "t", "t.abcdefgh.members" -> "t.members"; names without
// a suffix ("t", "t.members", from older builds) are returned unchanged.
func StripIncarnation(rel string) string {
	parts := strings.Split(rel, ".")
	if len(parts) > 1 && isIncarnationID(parts[1]) {
		parts = append(parts[:1], parts[2:]...)
	}
	return strings.Join(parts, ".")
}

// IsIncarnationID reports whether s is a per-incarnation physical-name
// suffix as CreateTable generates it (8 lowercase base32 characters).
func IsIncarnationID(s string) bool { return isIncarnationID(s) }

func isIncarnationID(s string) bool {
	if len(s) != incarnationIDLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= 'a' && c <= 'z' || c >= '2' && c <= '7') {
			return false
		}
	}
	return true
}

func errUnknownDB(name string) error {
	return cherr.New(cherr.UnknownDatabase, "Database %s does not exist", name)
}

func errUnknownTable(db, name string) error {
	return cherr.New(cherr.UnknownTable, "Table %s.%s does not exist", db, name)
}

func errTableExists(db, name string) error {
	return cherr.New(cherr.TableAlreadyExists, "Table %s.%s already exists", db, name)
}

// Refresh reloads the snapshot from the backend. Entries are decoded
// independently: one that fails (e.g. written by a newer build) is
// quarantined (see Quarantined) and every other entry still loads. Callers
// that act on absence (the reconciler dropping physical tables, the stream
// worker skipping records) must consult the quarantine first.
func (s *store) Refresh(ctx context.Context) error {
	if s.backend == nil {
		return nil
	}
	s.mu.RLock()
	gen := s.gen
	s.mu.RUnlock()

	entries, err := s.backend.LoadAll(ctx)
	if err != nil {
		return cherr.Wrap(cherr.Unavailable, err, "cannot load catalog")
	}
	snap := newSnapshot()
	for _, e := range entries {
		switch e.Kind {
		case KindDatabase:
			db, err := DecodeDatabase(e.Def)
			if err != nil {
				snap.quarantine[e.Key] = quarantine(e, err)
				continue
			}
			snap.dbs[db.Name] = db
		case KindTable:
			t, err := DecodeTable(e.Def, s.parse)
			if err != nil {
				snap.quarantine[e.Key] = quarantine(e, err)
				continue
			}
			snap.putTable(t)
		case KindView:
			v, err := DecodeView(e.Def)
			if err != nil {
				snap.quarantine[e.Key] = quarantine(e, err)
				continue
			}
			snap.putView(v)
		case KindMalformed:
			snap.quarantine[e.Key] = quarantine(e, cherr.New(cherr.IncorrectData, "malformed catalog item %s", e.Key))
		}
	}
	s.mu.Lock()
	// Local mutations made while the load ran may be missing from it
	// (committed after the scan passed their key): replay them.
	for _, m := range s.pending {
		if m.gen > gen {
			m.apply(snap)
		}
	}
	s.snap = snap
	s.mu.Unlock()
	return nil
}

func (s *store) Databases() []*Database {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Database, 0, len(s.snap.dbs)+1)
	if s.snap.dbs[DefaultDatabase] == nil {
		out = append(out, &Database{Name: DefaultDatabase, Engine: "Atomic"})
	}
	for _, db := range s.snap.dbs {
		out = append(out, db)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *store) Database(name string) (*Database, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if db := s.snap.dbs[name]; db != nil {
		return db, nil
	}
	if name == DefaultDatabase {
		return &Database{Name: DefaultDatabase, Engine: "Atomic"}, nil
	}
	return nil, errUnknownDB(name)
}

func (s *store) CreateDatabase(ctx context.Context, db *Database, ifNotExists bool) error {
	if err := ValidateName("Database", db.Name); err != nil {
		return err
	}
	s.mu.RLock()
	exists := s.snap.hasDB(db.Name) || reservedDatabases[db.Name]
	s.mu.RUnlock()
	if exists {
		if ifNotExists {
			return nil
		}
		return cherr.New(cherr.DatabaseAlreadyExists, "Database %s already exists", db.Name)
	}
	d := *db
	if d.Engine == "" {
		d.Engine = "Atomic"
	}
	if d.CreatedAt.IsZero() {
		d.CreatedAt = s.now().UTC()
	}
	if s.backend != nil {
		def, err := EncodeDatabase(&d)
		if err != nil {
			return err
		}
		err = s.backend.Put(ctx, Entry{Key: DatabaseKey(d.Name), Kind: KindDatabase, Def: def},
			[]Cond{{Key: DatabaseKey(d.Name), Exists: false}})
		if err != nil {
			if isCondFailed(err) {
				if ifNotExists {
					return nil
				}
				return cherr.New(cherr.DatabaseAlreadyExists, "Database %s already exists", d.Name)
			}
			return cherr.Wrap(cherr.Unavailable, err, "cannot write catalog")
		}
	}
	s.mu.Lock()
	s.mutateLocked(func(sn *snapshot) { sn.dbs[d.Name] = &d })
	s.mu.Unlock()
	return nil
}

func (s *store) DropDatabase(ctx context.Context, name string, ifExists bool) error {
	if s.backend != nil {
		// Pick up objects created elsewhere so none is left orphaned.
		if err := s.Refresh(ctx); err != nil {
			return err
		}
	}
	s.mu.RLock()
	exists := s.snap.hasDB(name)
	var keys []string
	if s.snap.dbs[name] != nil {
		keys = append(keys, DatabaseKey(name))
	}
	for tn := range s.snap.tables[name] {
		keys = append(keys, TableKey(name, tn))
	}
	for vn := range s.snap.views[name] {
		keys = append(keys, ViewKey(name, vn))
	}
	for k, q := range s.snap.quarantine {
		if q.Database == name && (q.Kind == KindTable || q.Kind == KindView) {
			keys = append(keys, k)
		}
	}
	s.mu.RUnlock()
	if !exists {
		if ifExists {
			return nil
		}
		return errUnknownDB(name)
	}
	if s.backend != nil && len(keys) > 0 {
		if err := s.backend.Delete(ctx, keys); err != nil {
			return cherr.Wrap(cherr.Unavailable, err, "cannot write catalog")
		}
	}
	s.mu.Lock()
	s.mutateLocked(func(sn *snapshot) {
		for _, t := range sn.tables[name] {
			delete(sn.physical, t.PhysicalName)
		}
		delete(sn.dbs, name)
		delete(sn.tables, name)
		delete(sn.views, name)
	})
	s.mu.Unlock()
	return nil
}

func (s *store) Tables(db string) []*Table {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Table, 0, len(s.snap.tables[db]))
	for _, t := range s.snap.tables[db] {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *store) Table(db, name string) (*Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t := s.snap.tables[db][name]; t != nil {
		return t, nil
	}
	if q, ok := s.snap.quarantine[TableKey(db, name)]; ok {
		return nil, errQuarantined(q)
	}
	if !s.snap.hasDB(db) {
		return nil, errUnknownDB(db)
	}
	return nil, errUnknownTable(db, name)
}

// errQuarantined reports a lookup of an entry that cannot be decoded.
func errQuarantined(q Quarantined) error {
	return cherr.Wrap(cherr.IncorrectData, q.Err, "catalog entry %s cannot be decoded by this server version", q.Key)
}

func (s *store) TableByPhysicalName(physical string) (*Table, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.snap.physical[physical]
	return t, ok
}

func (s *store) CreateTable(ctx context.Context, t *Table, ifNotExists bool) error {
	nt := cloneTable(t)
	if err := validateTable(nt, s.parse); err != nil {
		return err
	}
	nt.PhysicalName = s.physicalFor(nt.Database, nt.Name)
	if s.backend != nil {
		// Every incarnation of a persisted table gets its own physical
		// table: after DROP + CREATE or CREATE OR REPLACE under the same
		// name, rows of the old incarnation still in flight (replication,
		// stream workers, other regions not yet reconciled, stale servers)
		// land in the old physical table, which is dropped, never in the
		// new one. Entries of older builds keep their stored name.
		nt.PhysicalName += "." + incarnationID()
	}
	if len(nt.PhysicalName) > MaxPhysicalNameLen {
		return cherr.New(cherr.BadArguments, "DynamoDB table name for %s is too long (%d > %d characters)",
			nt.FullName(), len(nt.PhysicalName), MaxPhysicalNameLen)
	}
	if s.checkTable != nil {
		if err := s.checkTable(nt); err != nil {
			return err
		}
	}
	if nt.CreatedAt.IsZero() {
		nt.CreatedAt = s.now().UTC()
	}
	if nt.Version == 0 {
		nt.Version = 1
	}
	if s.region != "" {
		nt.HomeRegion = s.region
	}
	// A new table has never been truncated (CREATE TABLE ... AS copies the
	// source's entry).
	nt.ResetAt = time.Time{}

	s.mu.RLock()
	hasDB, exists := s.snap.hasDB(nt.Database), s.snap.hasObject(nt.Database, nt.Name)
	s.mu.RUnlock()
	if !hasDB {
		return errUnknownDB(nt.Database)
	}
	if exists {
		if ifNotExists {
			return nil
		}
		return errTableExists(nt.Database, nt.Name)
	}
	if s.backend != nil {
		def, err := EncodeTable(nt)
		if err != nil {
			return err
		}
		keys := objectKeys(nt.Database, nt.Name)
		conds := []Cond{{Key: keys[0]}, {Key: keys[1]}}
		if nt.Database != DefaultDatabase {
			conds = append(conds, Cond{Key: DatabaseKey(nt.Database), Exists: true})
		}
		err = s.backend.Put(ctx, Entry{Key: keys[0], Kind: KindTable, Def: def, Version: nt.Version}, conds)
		if err != nil {
			var cf *CondFailedError
			if errors.As(err, &cf) {
				if cf.Index == 2 {
					return errUnknownDB(nt.Database)
				}
				if ifNotExists {
					return nil
				}
				return errTableExists(nt.Database, nt.Name)
			}
			return cherr.Wrap(cherr.Unavailable, err, "cannot write catalog")
		}
	}
	s.mu.Lock()
	s.mutateLocked(func(sn *snapshot) { sn.putNewer(nt) })
	s.mu.Unlock()
	return nil
}

func (s *store) UpdateTable(ctx context.Context, t *Table) error {
	s.mu.RLock()
	old := s.snap.tables[t.Database][t.Name]
	s.mu.RUnlock()
	if old == nil {
		return errUnknownTable(t.Database, t.Name)
	}
	nt := cloneTable(t)
	if err := validateTable(nt, s.parse); err != nil {
		return err
	}
	nt.PhysicalName = old.PhysicalName
	nt.CreatedAt = old.CreatedAt
	nt.HomeRegion = old.HomeRegion
	if s.checkTable != nil && s.checkTable(old) == nil {
		if err := s.checkTable(nt); err != nil {
			return err
		}
	}
	if nt.ResetAt.Before(old.ResetAt) {
		// An ALTER built from an older snapshot must not undo a TRUNCATE.
		nt.ResetAt = old.ResetAt
	}
	trackTypeChanges(old, nt)
	nt.Version = old.Version + 1
	if s.backend != nil {
		def, err := EncodeTable(nt)
		if err != nil {
			return err
		}
		key := TableKey(nt.Database, nt.Name)
		err = s.backend.Put(ctx, Entry{Key: key, Kind: KindTable, Def: def, Version: nt.Version},
			[]Cond{{Key: key, Exists: true, Version: old.Version}})
		if err != nil {
			if isCondFailed(err) {
				return cherr.New(cherr.LogicalError,
					"table %s was modified concurrently (expected version %d); retry the ALTER", nt.FullName(), old.Version)
			}
			return cherr.Wrap(cherr.Unavailable, err, "cannot write catalog")
		}
	}
	s.mu.Lock()
	s.mutateLocked(func(sn *snapshot) { sn.putNewer(nt) })
	s.mu.Unlock()
	return nil
}

func (s *store) DropTable(ctx context.Context, db, name string, ifExists bool) error {
	s.mu.RLock()
	exists, hasDB := s.snap.tables[db][name] != nil, s.snap.hasDB(db)
	s.mu.RUnlock()
	if !exists {
		if ifExists {
			return nil
		}
		if !hasDB {
			return errUnknownDB(db)
		}
		return errUnknownTable(db, name)
	}
	if s.backend != nil {
		if err := s.backend.Delete(ctx, []string{TableKey(db, name)}); err != nil {
			return cherr.Wrap(cherr.Unavailable, err, "cannot write catalog")
		}
	}
	s.mu.Lock()
	s.mutateLocked(func(sn *snapshot) { sn.deleteTable(db, name) })
	s.mu.Unlock()
	return nil
}

func (s *store) Views(db string) []*View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*View, 0, len(s.snap.views[db]))
	for _, v := range s.snap.views[db] {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *store) View(db, name string) (*View, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v := s.snap.views[db][name]; v != nil {
		return v, nil
	}
	if !s.snap.hasDB(db) {
		return nil, errUnknownDB(db)
	}
	return nil, errUnknownTable(db, name)
}

func (s *store) CreateView(ctx context.Context, v *View, ifNotExists bool) error {
	nv := *v
	if err := ValidateName("Database", nv.Database); err != nil {
		return err
	}
	if err := ValidateName("View", nv.Name); err != nil {
		return err
	}
	if nv.SelectSQL == "" {
		return cherr.New(cherr.IncorrectQuery, "view %s has no SELECT query", nv.FullName())
	}
	if nv.CreatedAt.IsZero() {
		nv.CreatedAt = s.now().UTC()
	}
	s.mu.RLock()
	hasDB, exists := s.snap.hasDB(nv.Database), s.snap.hasObject(nv.Database, nv.Name)
	var targetOK bool
	if nv.Materialized {
		targetOK = s.snap.tables[nv.ToDatabase][nv.ToTable] != nil
	}
	s.mu.RUnlock()
	if !hasDB {
		return errUnknownDB(nv.Database)
	}
	if exists {
		if ifNotExists {
			return nil
		}
		return errTableExists(nv.Database, nv.Name)
	}
	if nv.Materialized {
		if nv.ToTable == "" {
			return cherr.New(cherr.NotImplemented, "materialized views without TO <table> are not supported")
		}
		if !targetOK {
			return errUnknownTable(nv.ToDatabase, nv.ToTable)
		}
	}
	if s.backend != nil {
		def, err := EncodeView(&nv)
		if err != nil {
			return err
		}
		keys := objectKeys(nv.Database, nv.Name)
		conds := []Cond{{Key: keys[1]}, {Key: keys[0]}}
		if nv.Database != DefaultDatabase {
			conds = append(conds, Cond{Key: DatabaseKey(nv.Database), Exists: true})
		}
		err = s.backend.Put(ctx, Entry{Key: keys[1], Kind: KindView, Def: def}, conds)
		if err != nil {
			var cf *CondFailedError
			if errors.As(err, &cf) {
				if cf.Index == 2 {
					return errUnknownDB(nv.Database)
				}
				if ifNotExists {
					return nil
				}
				return errTableExists(nv.Database, nv.Name)
			}
			return cherr.Wrap(cherr.Unavailable, err, "cannot write catalog")
		}
	}
	s.mu.Lock()
	s.mutateLocked(func(sn *snapshot) { sn.putView(&nv) })
	s.mu.Unlock()
	return nil
}

func (s *store) DropView(ctx context.Context, db, name string, ifExists bool) error {
	s.mu.RLock()
	exists, hasDB := s.snap.views[db][name] != nil, s.snap.hasDB(db)
	s.mu.RUnlock()
	if !exists {
		if ifExists {
			return nil
		}
		if !hasDB {
			return errUnknownDB(db)
		}
		return errUnknownTable(db, name)
	}
	if s.backend != nil {
		if err := s.backend.Delete(ctx, []string{ViewKey(db, name)}); err != nil {
			return cherr.Wrap(cherr.Unavailable, err, "cannot write catalog")
		}
	}
	s.mu.Lock()
	s.mutateLocked(func(sn *snapshot) { delete(sn.views[db], name) })
	s.mu.Unlock()
	return nil
}

func (s *store) ViewsFrom(db, table string) []*View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*View
	for _, m := range s.snap.views {
		for _, v := range m {
			if v.Materialized && v.SourceDatabase == db && v.SourceTable == table {
				out = append(out, v)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FullName() < out[j].FullName() })
	return out
}

func isCondFailed(err error) bool {
	var cf *CondFailedError
	return errors.As(err, &cf)
}

// cloneTable deep-copies the slices and maps of t so the stored entry is
// independent of the caller's value.
func cloneTable(t *Table) *Table {
	nt := *t
	nt.Columns = append([]Column(nil), t.Columns...)
	for i := range nt.Columns {
		nt.Columns[i].PrevTypes = cloneStrings(nt.Columns[i].PrevTypes)
	}
	nt.EngineArgs = cloneStrings(t.EngineArgs)
	nt.OrderBy = cloneStrings(t.OrderBy)
	nt.PartitionKey = cloneStrings(t.PartitionKey)
	nt.SortKey = cloneStrings(t.SortKey)
	nt.SumColumns = cloneStrings(t.SumColumns)
	nt.PrimaryKey = cloneStrings(t.PrimaryKey)
	nt.SettingsSQL = cloneStrings(t.SettingsSQL)
	if t.Settings != nil {
		nt.Settings = make(map[string]string, len(t.Settings))
		for k, v := range t.Settings {
			nt.Settings[k] = v
		}
	}
	return &nt
}

// trackTypeChanges records in nt's columns the types their values may still
// be stored with: a column whose type differs from the same-named column of
// old inherits old's PrevTypes plus old's type (MODIFY COLUMN is
// catalog-only; see Column.PrevTypes). A column that keeps its type keeps
// old's history; a column that is new (or was dropped and re-added) has
// none.
func trackTypeChanges(old, nt *Table) {
	for i := range nt.Columns {
		c := &nt.Columns[i]
		oi := old.ColumnIndex(c.Name)
		if oi < 0 {
			c.PrevTypes = nil
			continue
		}
		o := old.Columns[oi]
		prev := cloneStrings(o.PrevTypes)
		if o.TypeText != c.TypeText {
			prev = append(prev, o.TypeText)
		}
		// The current type needs no entry; keep the newest occurrence of
		// every other type, bounded.
		var out []string
		seen := map[string]bool{c.TypeText: true}
		for j := len(prev) - 1; j >= 0 && len(out) < maxPrevTypes; j-- {
			if !seen[prev[j]] {
				seen[prev[j]] = true
				out = append([]string{prev[j]}, out...)
			}
		}
		c.PrevTypes = out
	}
}

// maxPrevTypes bounds Column.PrevTypes.
const maxPrevTypes = 8

func cloneStrings(s []string) []string {
	if s == nil {
		return nil
	}
	return append([]string{}, s...)
}
