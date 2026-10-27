package server

import (
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// HTTP sessions (session_id): best-effort, in-memory, per server process.
// A session remembers the current database (USE) and settings (SET) between
// requests of the same user, like ClickHouse. Only one request may use a
// session at a time (SESSION_IS_LOCKED otherwise).

const (
	defaultSessionTimeout = 60 * time.Second
	maxSessionTimeout     = 3600 * time.Second
	sessionSweepInterval  = time.Second
)

type sessionKey struct{ user, id string }

type session struct {
	key      sessionKey
	busy     bool
	database string
	settings map[string]string
	timeout  time.Duration
	expires  time.Time
}

type sessionManager struct {
	mu        sync.Mutex
	sessions  map[sessionKey]*session
	now       func() time.Time
	maxTO     time.Duration
	lastSweep time.Time
}

func newSessionManager(now func() time.Time, maxTimeout time.Duration) *sessionManager {
	if maxTimeout <= 0 {
		maxTimeout = maxSessionTimeout
	}
	return &sessionManager{sessions: map[sessionKey]*session{}, now: now, maxTO: maxTimeout}
}

// parseSessionTimeout validates the session_timeout URL parameter (seconds).
func (m *sessionManager) parseSessionTimeout(v string, present bool) (time.Duration, error) {
	if !present {
		return defaultSessionTimeout, nil
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return 0, cherr.New(cherr.InvalidSessionTimeout, "Invalid session timeout: '%s'", v)
	}
	d := time.Duration(n) * time.Second
	if d > m.maxTO {
		return 0, cherr.New(cherr.InvalidSessionTimeout,
			"Session timeout '%s' is larger than max_session_timeout: %d. Maximum session timeout could be modified in configuration file",
			v, int64(m.maxTO/time.Second))
	}
	return d, nil
}

// sessionState is the snapshot of a session a request works with.
type sessionState struct {
	Database string
	Settings map[string]string
}

// acquire locks the session (user, id) for one request, creating it unless
// check is set. The returned snapshot is a copy.
func (m *sessionManager) acquire(user, id string, timeout time.Duration, check bool) (*session, sessionState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	m.sweepLocked(now)
	k := sessionKey{user, id}
	s := m.sessions[k]
	if s != nil && !s.busy && now.After(s.expires) {
		delete(m.sessions, k)
		s = nil
	}
	if s == nil {
		if check {
			return nil, sessionState{}, cherr.New(cherr.SessionNotFound, "Session %s not found", id)
		}
		s = &session{key: k, settings: map[string]string{}}
		m.sessions[k] = s
	}
	if s.busy {
		return nil, sessionState{}, cherr.New(cherr.SessionIsLocked, "Session %s is locked by a concurrent client", id)
	}
	s.busy = true
	s.timeout = timeout
	return s, sessionState{Database: s.database, Settings: maps.Clone(s.settings)}, nil
}

// release unlocks s, applying the changes the request made, and restarts
// its inactivity timer.
func (m *sessionManager) release(s *session, d sessionDelta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d.database != nil {
		s.database = *d.database
	}
	maps.Copy(s.settings, d.settings)
	for _, name := range d.reset {
		delete(s.settings, name)
	}
	s.busy = false
	s.expires = m.now().Add(s.timeout)
	if s.timeout == 0 {
		delete(m.sessions, s.key)
	}
}

func (m *sessionManager) sweepLocked(now time.Time) {
	if now.Sub(m.lastSweep) < sessionSweepInterval {
		return
	}
	m.lastSweep = now
	for k, s := range m.sessions {
		if !s.busy && now.After(s.expires) {
			delete(m.sessions, k)
		}
	}
}
