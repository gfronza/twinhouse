package format

import (
	"strconv"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// dtInMode is the date_time_input_format setting.
type dtInMode uint8

const (
	dtInBasic        dtInMode = iota // YYYY-MM-DD hh:mm:ss[.f] / unix timestamp (chtype text rules)
	dtInBestEffort                   // parseDateTimeBestEffort: ISO 8601, zones, DD/MM/YYYY, ...
	dtInBestEffortUS                 // as best_effort, but ambiguous NN/NN/YYYY is MM/DD/YYYY
)

// dateTimeInputMode reads date_time_input_format (case-sensitive, as in
// ClickHouse; anything else is BAD_ARGUMENTS).
func dateTimeInputMode(s map[string]string) (dtInMode, error) {
	v, ok := s["date_time_input_format"]
	if !ok {
		return dtInBasic, nil
	}
	switch v {
	case "basic":
		return dtInBasic, nil
	case "best_effort":
		return dtInBestEffort, nil
	case "best_effort_us":
		return dtInBestEffortUS, nil
	}
	return dtInBasic, cherr.New(cherr.BadArguments,
		"Unexpected value of DateTimeInputFormat: '%s'. Must be one of ['best_effort_us', 'best_effort', 'basic']", v)
}

// bestEffortTarget returns the DateTime/DateTime64 type a text field of
// column type t is parsed as with best-effort parsing (Nullable,
// LowCardinality and SimpleAggregateFunction unwrapped), or false.
func bestEffortTarget(t chtype.Type) (chtype.Type, bool) {
	u := unwrapAll(t)
	return u, u.Kind == chtype.DateTime || u.Kind == chtype.DateTime64
}

// bestEffortValue parses s for a DateTime/DateTime64 column of type t with
// date_time_input_format=best_effort[_us]. ok=false means the best-effort
// parser did not recognise s; the caller then applies the basic rules (so
// best_effort never accepts less than basic).
func (s *inputSettings) bestEffortValue(t chtype.Type, str string) (any, bool, error) {
	target, isTime := bestEffortTarget(t)
	if !isTime || s.dtIn == dtInBasic {
		return nil, false, nil
	}
	loc := time.UTC
	tz := target.TZ
	if tz == "" {
		tz = s.tz
	}
	if tz != "" && tz != "UTC" {
		if l, err := time.LoadLocation(tz); err == nil {
			loc = l
		}
	}
	tm, ok := parseDateTimeBestEffort(str, loc, s.dtIn == dtInBestEffortUS, target.Kind == chtype.DateTime64)
	if !ok {
		return nil, false, nil
	}
	v, err := chtype.Normalize(target, tm)
	return v, true, err
}

// parseDateTimeBestEffort implements the commonly used subset of
// ClickHouse's parseDateTimeBestEffort:
//
//   - digits only: YYYY, YYYYMM, YYYYMMDD, YYYYMMDDhhmmss, a unix timestamp
//     of 9-10 digits (DateTime64: optionally with ".fraction"), or unix
//     milliseconds (13 digits);
//   - a date YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD (1-2 digit month/day),
//     DD/MM/YYYY, DD.MM.YYYY, DD-MM-YYYY (MM/DD/YYYY with us), YYYYMMDD;
//     followed optionally by 'T' or spaces and a time hh:mm[:ss[.frac]]
//     (or hhmmss / hhmm right after 'T'), then optionally a zone: Z, UTC,
//     GMT, +hh, +hhmm, +hh:mm (or the same with '-');
//   - textual forms: RFC 1123 / RFC 822 / ANSIC, "Jan 2 2006 15:04:05",
//     "2 Jan 2006 ...", "January 2, 2006".
//
// Without a zone the wall clock is taken in loc.
func parseDateTimeBestEffort(s string, loc *time.Location, us, dt64 bool) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	if tm, ok := bestEffortNumeric(s, loc, dt64); ok {
		return tm, true
	}
	if tm, ok := bestEffortISO(s, loc, us); ok {
		return tm, true
	}
	for _, l := range bestEffortLayouts {
		if tm, err := time.ParseInLocation(l, s, loc); err == nil {
			return tm, true
		}
	}
	return time.Time{}, false
}

var bestEffortLayouts = []string{
	time.RFC1123Z,
	time.RFC1123,
	"Mon, 2 Jan 2006 15:04:05 MST",
	"Mon, 2 Jan 2006 15:04:05 -0700",
	time.RFC850,
	time.RFC822Z,
	time.RFC822,
	time.ANSIC,
	time.UnixDate,
	"Jan 2 2006 15:04:05",
	"Jan 2 2006 15:04",
	"Jan 2 2006",
	"Jan 2, 2006 15:04:05",
	"Jan 2, 2006",
	"January 2 2006 15:04:05",
	"January 2 2006",
	"January 2, 2006 15:04:05",
	"January 2, 2006",
	"2 Jan 2006 15:04:05",
	"2 Jan 2006 15:04",
	"2 Jan 2006",
	"2 January 2006 15:04:05",
	"2 January 2006",
	"Mon Jan 2 2006 15:04:05",
}

// bestEffortNumeric handles all-digit inputs and "secs.frac" timestamps.
func bestEffortNumeric(s string, loc *time.Location, dt64 bool) (time.Time, bool) {
	whole, frac, hasDot := strings.Cut(s, ".")
	if !allDigits(whole) || hasDot && (!dt64 || !allDigits(frac)) {
		return time.Time{}, false
	}
	n := len(whole)
	if hasDot && n != 9 && n != 10 {
		return time.Time{}, false
	}
	atoi := func(a, b int) int { v, _ := strconv.Atoi(whole[a:b]); return v }
	switch n {
	case 4:
		return civilTime(atoi(0, 4), 1, 1, 0, 0, 0, 0, loc)
	case 6:
		return civilTime(atoi(0, 4), atoi(4, 6), 1, 0, 0, 0, 0, loc)
	case 8:
		return civilTime(atoi(0, 4), atoi(4, 6), atoi(6, 8), 0, 0, 0, 0, loc)
	case 14:
		return civilTime(atoi(0, 4), atoi(4, 6), atoi(6, 8), atoi(8, 10), atoi(10, 12), atoi(12, 14), 0, loc)
	case 9, 10:
		sec, _ := strconv.ParseInt(whole, 10, 64)
		return time.Unix(sec, fracNanos(frac)).UTC(), true
	case 13:
		ms, _ := strconv.ParseInt(whole, 10, 64)
		return time.UnixMilli(ms).UTC(), true
	}
	return time.Time{}, false
}

// fracNanos converts fractional-second digits to nanoseconds (truncating
// beyond 9 digits).
func fracNanos(f string) int64 {
	var n int64
	for i := 0; i < 9; i++ {
		n *= 10
		if i < len(f) {
			n += int64(f[i] - '0')
		}
	}
	return n
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func civilTime(y, mo, d, h, mi, sec int, nsec int64, loc *time.Location) (time.Time, bool) {
	if mo < 1 || mo > 12 || d < 1 || d > 31 || h > 23 || mi > 59 || sec > 59 {
		return time.Time{}, false
	}
	tm := time.Date(y, time.Month(mo), d, h, mi, sec, int(nsec), loc)
	if tm.Day() != d { // e.g. Feb 30
		return time.Time{}, false
	}
	return tm, true
}

// beScanner is a tiny cursor over the input.
type beScanner struct {
	s string
	i int
}

func (p *beScanner) digits(maxN int) (int, int) {
	start := p.i
	v := 0
	for p.i < len(p.s) && p.i-start < maxN && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
		v = v*10 + int(p.s[p.i]-'0')
		p.i++
	}
	return v, p.i - start
}

func (p *beScanner) peek() byte {
	if p.i < len(p.s) {
		return p.s[p.i]
	}
	return 0
}

func (p *beScanner) eat(c byte) bool {
	if p.peek() == c {
		p.i++
		return true
	}
	return false
}

func (p *beScanner) spaces() {
	for p.i < len(p.s) && p.s[p.i] == ' ' {
		p.i++
	}
}

// bestEffortISO parses numeric date[time][zone] forms.
func bestEffortISO(s string, loc *time.Location, us bool) (time.Time, bool) {
	p := &beScanner{s: s}
	var y, mo, d int
	first, n := p.digits(8)
	switch {
	case n == 4:
		sep := p.peek()
		if sep != '-' && sep != '/' && sep != '.' {
			return time.Time{}, false
		}
		p.i++
		var k int
		if mo, k = p.digits(2); k == 0 || !p.eat(sep) {
			return time.Time{}, false
		}
		if d, k = p.digits(2); k == 0 {
			return time.Time{}, false
		}
		y = first
	case n == 8:
		y, mo, d = first/10000, first/100%100, first%100
	case n == 1 || n == 2:
		sep := p.peek()
		if sep != '-' && sep != '/' && sep != '.' {
			return time.Time{}, false
		}
		p.i++
		second, k := p.digits(2)
		if k == 0 || !p.eat(sep) {
			return time.Time{}, false
		}
		if y, k = p.digits(4); k != 4 {
			return time.Time{}, false
		}
		d, mo = first, second
		if us && sep == '/' {
			d, mo = second, first
		}
	default:
		return time.Time{}, false
	}
	var h, mi, sec int
	var nsec int64
	hasT := p.eat('T')
	if !hasT {
		p.spaces()
	}
	if c := p.peek(); c >= '0' && c <= '9' {
		v, k := p.digits(6)
		switch {
		case p.peek() == ':' && (k == 1 || k == 2):
			h = v
			p.i++
			var kk int
			if mi, kk = p.digits(2); kk == 0 {
				return time.Time{}, false
			}
			if p.eat(':') {
				if sec, kk = p.digits(2); kk == 0 {
					return time.Time{}, false
				}
			}
		case hasT && k == 6:
			h, mi, sec = v/10000, v/100%100, v%100
		case hasT && k == 4:
			h, mi = v/100, v%100
		default:
			return time.Time{}, false
		}
		if p.eat('.') || p.eat(',') {
			fs := p.i
			for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
				p.i++
			}
			if p.i == fs {
				return time.Time{}, false
			}
			nsec = fracNanos(p.s[fs:p.i])
		}
	} else if hasT {
		return time.Time{}, false
	}
	p.spaces()
	zone, ok := bestEffortZone(p)
	if !ok {
		return time.Time{}, false
	}
	p.spaces()
	if p.i != len(p.s) {
		return time.Time{}, false
	}
	if zone != nil {
		loc = zone
	}
	return civilTime(y, mo, d, h, mi, sec, nsec, loc)
}

// bestEffortZone parses an optional zone suffix; nil means none.
func bestEffortZone(p *beScanner) (*time.Location, bool) {
	rest := p.s[p.i:]
	switch {
	case rest == "":
		return nil, true
	case rest == "Z" || rest == "z":
		p.i++
		return time.UTC, true
	case strings.HasPrefix(rest, "UTC") || strings.HasPrefix(rest, "GMT"):
		p.i += 3
		if c := p.peek(); c != '+' && c != '-' {
			return time.UTC, true
		}
	}
	c := p.peek()
	if c != '+' && c != '-' {
		return nil, false
	}
	p.i++
	hh, k := p.digits(2)
	if k != 2 {
		return nil, false
	}
	mm := 0
	if p.eat(':') {
		if mm, k = p.digits(2); k != 2 {
			return nil, false
		}
	} else if v, k := p.digits(2); k == 2 {
		mm = v
	} else if k != 0 {
		return nil, false
	}
	if hh > 14 || mm > 59 {
		return nil, false
	}
	off := hh*3600 + mm*60
	if c == '-' {
		off = -off
	}
	return time.FixedZone("", off), true
}
