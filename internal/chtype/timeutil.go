package chtype

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	_ "time/tzdata" // explicit DateTime('tz') must work without system zoneinfo

	"dynamodb-eventstream/internal/cherr"
)

var locCache sync.Map // tz name -> *time.Location

// loadLocation resolves an IANA timezone name; "" means UTC.
func loadLocation(tz string) (*time.Location, error) {
	if tz == "" || tz == "UTC" {
		return time.UTC, nil
	}
	if l, ok := locCache.Load(tz); ok {
		return l.(*time.Location), nil
	}
	if tz == "Local" {
		return nil, errf(cherr.BadArguments, "Cannot load time zone %s", tz)
	}
	l, err := time.LoadLocation(tz)
	if err != nil || !zoneNameCaseMatches(tz) {
		return nil, errf(cherr.BadArguments, "Cannot load time zone %s", tz)
	}
	locCache.Store(tz, l)
	return l, nil
}

func checkTimezone(tz string) error {
	_, err := loadLocation(tz)
	return err
}

// locationOf returns the rendering/parsing location of a time type
// (UTC when the type has no explicit timezone).
func locationOf(t Type) *time.Location {
	l, err := loadLocation(t.TZ)
	if err != nil {
		return time.UTC
	}
	return l
}

// Value ranges. DateTime64 and Date32 are additionally capped to what an
// int64 count of unix nanoseconds can hold (storage encodes times that way),
// so 2262-04-11 23:47:16.854775807 is the latest representable instant.
var (
	minDate       = Epoch
	maxDate       = time.Date(2149, 6, 6, 0, 0, 0, 0, time.UTC)
	minDate32     = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	maxTime64     = time.Unix(0, math.MaxInt64).UTC()
	maxDate32     = time.Date(maxTime64.Year(), maxTime64.Month(), maxTime64.Day(), 0, 0, 0, 0, time.UTC)
	chMaxDate32   = time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC)
	minDateTime   = Epoch
	maxDateTime   = time.Unix(math.MaxUint32, 0).UTC()
	minDateTime64 = minDate32
	// minStoredTime64 is the lowest DateTime64 instant accepted: 1900-01-01
	// 00:00 local time in a zone east of UTC is still on 1899-12-31 in UTC
	// (CAST(toDate32('1900-01-01') AS DateTime64(0, 'Asia/Tokyo'))).
	minStoredTime64 = minDateTime64.Add(-24 * time.Hour)
)

// dateOf truncates an instant to its calendar date in loc, as a Date value.
func dateOf(t time.Time, loc *time.Location) time.Time {
	l := t.In(loc)
	return time.Date(l.Year(), l.Month(), l.Day(), 0, 0, 0, 0, time.UTC)
}

// truncPrecision truncates t to p fractional-second digits (toward the past,
// matching DateTime64's fixed-point floor semantics for positive scale).
func truncPrecision(t time.Time, p int) time.Time {
	unit := time.Duration(pow10[9-p])
	return t.Truncate(unit)
}

var pow10 = [...]int64{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
	10000000000, 100000000000, 1000000000000, 10000000000000, 100000000000000,
	1000000000000000, 10000000000000000, 100000000000000000, 1000000000000000000}

func checkDateRange(t time.Time, k Kind) error {
	lo, hi := minDate, maxDate
	if k == Date32 {
		lo, hi = minDate32, maxDate32
	}
	if t.Before(lo) || t.After(hi) {
		return errf(codeArgumentOutOfBound, "Value %s is out of range of %s", t.Format("2006-01-02"), kindName(k))
	}
	return nil
}

// clampDate saturates a date to ClickHouse's range for k (Date
// [1970-01-01, 2149-06-06], Date32 [1900-01-01, 2299-12-31]); a Date32
// beyond the representable cap is an error rather than a silent clamp.
func clampDate(t time.Time, k Kind) (time.Time, error) {
	lo, hi := minDate, maxDate
	if k == Date32 {
		lo, hi = minDate32, chMaxDate32
	}
	if t.Before(lo) {
		t = lo
	}
	if t.After(hi) {
		t = hi
	}
	if k == Date32 && t.After(maxDate32) {
		return time.Time{}, errf(codeArgumentOutOfBound, "Value %s is out of the supported range of Date32 (up to %s)", t.Format("2006-01-02"), maxDate32.Format("2006-01-02"))
	}
	return t, nil
}

func checkDateTimeRange(t time.Time) error {
	if t.Before(minDateTime) || t.After(maxDateTime) {
		return errf(codeArgumentOutOfBound, "Value %s is out of range of DateTime [1970-01-01 00:00:00, 2106-02-07 06:28:15]", t.UTC().Format("2006-01-02 15:04:05"))
	}
	return nil
}

func checkDateTime64Range(t time.Time) error {
	if t.Before(minStoredTime64) || t.After(maxTime64) {
		return errf(codeDecimalOverflow, "Value %s is out of range of DateTime64 [1900-01-01 00:00:00, 2262-04-11 23:47:16.854775807]", t.UTC().Format("2006-01-02 15:04:05.999999999"))
	}
	return nil
}

// clampDateTime64Date clamps a DateTime64 parsed from date/time text whose
// calendar date in loc is before 1900-01-01 to that date, keeping the time
// of day, like ClickHouse ('1899-12-31 23:59:59.999' -> '1900-01-01
// 23:59:59.999').
func clampDateTime64Date(t time.Time, loc *time.Location) time.Time {
	if !dateOf(t, loc).Before(minDate32) {
		return t
	}
	l := t.In(loc)
	return time.Date(1900, 1, 1, l.Hour(), l.Minute(), l.Second(), l.Nanosecond(), loc).UTC()
}

// civil builds a wall-clock time in loc with ClickHouse's leniency: hours,
// minutes and seconds overflow into the next unit and a day past the end of
// the month rolls over (2006-02-30 -> 2006-03-02), but month must be 1..12
// and day 1..31. Year 0 is ClickHouse's "zero date" and maps to the epoch.
func civil(year, month, day, hour, min, sec int, loc *time.Location) (time.Time, bool, error) {
	if year == 0 {
		return Epoch, true, nil
	}
	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, false, errf(cherr.CannotParseDateTime, "Cannot parse date: invalid month or day in %04d-%02d-%02d", year, month, day)
	}
	return time.Date(year, time.Month(month), day, hour, min, sec, 0, loc).UTC(), false, nil
}

// zoneinfoDirs are where time.LoadLocation looks before its embedded copy.
var zoneinfoDirs = []string{"/usr/share/zoneinfo/", "/usr/share/lib/zoneinfo/", "/usr/lib/locale/TZ/"}

// zoneNameCaseMatches rejects names that only resolve because the system
// zoneinfo directory lives on a case-insensitive filesystem (macOS loads
// "utc" or "europe/berlin"; ClickHouse and Linux do not).
func zoneNameCaseMatches(tz string) bool {
	if os.Getenv("ZONEINFO") != "" {
		return true
	}
	for _, dir := range zoneinfoDirs {
		if _, err := os.Stat(dir); err != nil {
			continue
		}
		cur := dir
		for _, part := range strings.Split(tz, "/") {
			entries, err := os.ReadDir(cur)
			if err != nil {
				return false
			}
			found := false
			for _, e := range entries {
				if e.Name() == part {
					found = true
					break
				}
			}
			if !found {
				return false
			}
			cur = filepath.Join(cur, part)
		}
		return true
	}
	return true // embedded tzdata lookup is case-sensitive
}
