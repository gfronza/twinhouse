package exec

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

func init() {
	regCI(buildNow, "now", "current_timestamp")
	reg(buildNow64, "now64")
	regCI(dayBuilder(0), "today", "curdate", "current_date")
	reg(dayBuilder(-1), "yesterday")
	regCI(buildToDate, "toDate", "toDate32")
	reg(buildToDateTime, "toDateTime")
	reg(buildToDateTime64, "toDateTime64")
	reg(buildToTimeZone, "toTimeZone", "toTimezone")
	reg(bestEffortBuilder(false, convPlain), "parseDateTimeBestEffort")
	reg(bestEffortBuilder(false, convOrNull), "parseDateTimeBestEffortOrNull")
	reg(bestEffortBuilder(false, convOrZero), "parseDateTimeBestEffortOrZero")
	reg(bestEffortBuilder(true, convPlain), "parseDateTime64BestEffort")
	reg(bestEffortBuilder(true, convOrNull), "parseDateTime64BestEffortOrNull")
	reg(bestEffortBuilder(true, convOrZero), "parseDateTime64BestEffortOrZero")

	partFuncs := map[string]struct {
		t chtype.Type
		f func(time.Time) uint64
	}{
		"toYYYYMM":     {chtype.TUInt32, func(t time.Time) uint64 { return uint64(t.Year()*100 + int(t.Month())) }},
		"toYYYYMMDD":   {chtype.TUInt32, func(t time.Time) uint64 { return uint64(t.Year()*10000 + int(t.Month())*100 + t.Day()) }},
		"toYear":       {chtype.TUInt16, func(t time.Time) uint64 { return uint64(t.Year()) }},
		"toQuarter":    {chtype.TUInt8, func(t time.Time) uint64 { return uint64((int(t.Month())-1)/3 + 1) }},
		"toMonth":      {chtype.TUInt8, func(t time.Time) uint64 { return uint64(t.Month()) }},
		"toDayOfMonth": {chtype.TUInt8, func(t time.Time) uint64 { return uint64(t.Day()) }},
		"toDayOfYear":  {chtype.TUInt16, func(t time.Time) uint64 { return uint64(t.YearDay()) }},
		"toDayOfWeek":  {chtype.TUInt8, func(t time.Time) uint64 { return uint64(isoWeekday(t)) }},
		"toHour":       {chtype.TUInt8, func(t time.Time) uint64 { return uint64(t.Hour()) }},
		"toMinute":     {chtype.TUInt8, func(t time.Time) uint64 { return uint64(t.Minute()) }},
		"toSecond":     {chtype.TUInt8, func(t time.Time) uint64 { return uint64(t.Second()) }},
	}
	for n, p := range partFuncs {
		reg(datePartBuilder(p.t, p.f), n)
	}
	aliasCI := map[string]string{"YEAR": "toYear", "MONTH": "toMonth", "QUARTER": "toQuarter", "DAY": "toDayOfMonth",
		"DAYOFMONTH": "toDayOfMonth", "DAYOFYEAR": "toDayOfYear", "DAYOFWEEK": "toDayOfWeek", "HOUR": "toHour",
		"MINUTE": "toMinute", "SECOND": "toSecond"}
	for alias, target := range aliasCI {
		regCI(funcs[target], alias)
	}

	reg(startOfBuilder(chtype.TDateTime, func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	}), "toStartOfDay")
	reg(startOfBuilder(chtype.TDateTime, func(t time.Time) time.Time { return t.Truncate(time.Hour) }), "toStartOfHour")
	reg(startOfBuilder(chtype.TDateTime, func(t time.Time) time.Time { return t.Truncate(time.Minute) }), "toStartOfMinute")
	reg(buildToStartOfSecond, "toStartOfSecond")
	reg(startOfBuilder(chtype.TDateTime, func(t time.Time) time.Time { return truncMinutes(t, 5) }), "toStartOfFiveMinutes", "toStartOfFiveMinute")
	reg(startOfBuilder(chtype.TDateTime, func(t time.Time) time.Time { return truncMinutes(t, 10) }), "toStartOfTenMinutes")
	reg(startOfBuilder(chtype.TDateTime, func(t time.Time) time.Time { return truncMinutes(t, 15) }), "toStartOfFifteenMinutes")
	reg(startOfBuilder(chtype.TDate, func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	}), "toStartOfMonth")
	reg(startOfBuilder(chtype.TDate, func(t time.Time) time.Time {
		return time.Date(t.Year(), time.Month((int(t.Month())-1)/3*3+1), 1, 0, 0, 0, 0, t.Location())
	}), "toStartOfQuarter")
	reg(startOfBuilder(chtype.TDate, func(t time.Time) time.Time {
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	}), "toStartOfYear")
	reg(startOfBuilder(chtype.TDate, func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day()-(isoWeekday(t)-1), 0, 0, 0, 0, t.Location())
	}), "toMonday")
	reg(buildToStartOfWeek, "toStartOfWeek")
	reg(buildToUnixTimestamp, "toUnixTimestamp")
	regCI(buildFormatDateTime, "formatDateTime", "DATE_FORMAT")
	regCI(buildDateDiff, "dateDiff", "date_diff", "timestampDiff", "timestamp_diff")

	for unit, fn := range intervalFuncs {
		reg(addIntervalBuilder(unit, false), fn)
		reg(addIntervalBuilder(unit, true), "subtract"+strings.TrimPrefix(fn, "add"))
	}
}

func isoWeekday(t time.Time) int {
	wd := int(t.Weekday())
	if wd == 0 {
		return 7
	}
	return wd
}

func truncMinutes(t time.Time, n int) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()/n*n, 0, 0, t.Location())
}

// dateOnly returns midnight UTC of the calendar day of t (Date values are
// midnight UTC by contract).
func dateOnly(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// localTime returns the wall-clock time of a time value of type t: Date
// values are calendar days (no time zone); DateTime values are shown in the
// column or server zone.
func (q *query) localTime(v time.Time, t chtype.Type) time.Time {
	switch t.Kind {
	case chtype.Date, chtype.Date32:
		return v.UTC()
	}
	return v.In(q.location(t))
}

func tzArg(name string, args []*expr, pos int) (string, error) {
	if len(args) <= pos {
		return "", nil
	}
	a := args[pos]
	s, ok := a.val.(string)
	if !a.konst || !ok {
		return "", cherr.New(cherr.IllegalTypeOfArgument, "Argument %d of function %s must be a constant time zone string", pos+1, name)
	}
	if _, err := time.LoadLocation(s); err != nil {
		return "", cherr.New(cherr.BadArguments, "Invalid time zone: %s", s)
	}
	return s, nil
}

func buildNow(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 0, 1); err != nil {
		return nil, err
	}
	tz, err := tzArg(name, args, 0)
	if err != nil {
		return nil, err
	}
	t := chtype.Type{Kind: chtype.DateTime, TZ: tz}
	return constant(t, c.q.now.Truncate(time.Second)), nil
}

func buildNow64(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 0, 2); err != nil {
		return nil, err
	}
	p := 3
	if len(args) > 0 {
		var err error
		if p, err = precisionArg(name, args[0]); err != nil {
			return nil, err
		}
	}
	tz, err := tzArg(name, args, 1)
	if err != nil {
		return nil, err
	}
	return constant(chtype.NewDateTime64(p, tz), truncPrecision(c.q.now, p)), nil
}

func precisionArg(name string, a *expr) (int, error) {
	if !a.konst || !isIntKind(base(a).Kind) {
		return 0, cherr.New(cherr.IllegalTypeOfArgument, "Precision argument of function %s must be a constant integer", name)
	}
	p := int(asInt(a.val))
	if p < 0 || p > 9 {
		return 0, cherr.New(cherr.BadArguments, "Illegal precision %d for function %s, must be in [0, 9]", p, name)
	}
	return p, nil
}

func truncPrecision(t time.Time, p int) time.Time {
	d := time.Duration(math.Pow10(9 - p))
	return t.Truncate(d)
}

func dayBuilder(offset int) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 0, 0); err != nil {
			return nil, err
		}
		d := dateOnly(c.q.now.In(c.q.loc)).AddDate(0, 0, offset)
		return constant(chtype.TDate, d), nil
	}
}

// convertToTime converts a non-NULL value to a Date/DateTime/DateTime64 type
// the way ClickHouse's toDate/toDateTime/toDateTime64 do: time values wrap
// to the target range (Date modulo 2^16 days, DateTime modulo 2^32 seconds),
// strings are parsed like CAST (the basic text parser, not
// parseDateTimeBestEffort) and numbers saturate at the target range.
func convertToTime(q *query, v any, from, to chtype.Type) (any, error) {
	switch x := v.(type) {
	case time.Time:
		switch to.Kind {
		case chtype.Date, chtype.Date32:
			return wrapTime(dateOnly(q.localTime(x, from)), to), nil
		case chtype.DateTime:
			if from.Kind == chtype.Date || from.Kind == chtype.Date32 {
				l := q.location(to)
				return wrapTime(time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, l), to), nil
			}
			return wrapTime(x.Truncate(time.Second), to), nil
		case chtype.DateTime64:
			if from.Kind == chtype.Date || from.Kind == chtype.Date32 {
				l := q.location(to)
				return time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, l).UTC(), nil
			}
			return truncPrecision(x, to.Precision), nil
		}
	case string:
		s := strings.TrimRight(x, "\x00")
		pt := to
		if (pt.Kind == chtype.DateTime || pt.Kind == chtype.DateTime64) && pt.TZ == "" {
			if l := q.location(to); l != time.UTC {
				pt.TZ = l.String()
			}
		}
		out, err := chtype.Convert(s, chtype.TString, pt)
		if err != nil {
			// chtype caps Date32/DateTime64 at 2262-04-11 (the int64-nanosecond
			// storage limit); expressions keep ClickHouse's wider range.
			if pt.Kind != chtype.Date32 && pt.Kind != chtype.DateTime64 || !cherr.Is(err, cherr.ArgumentOutOfBound) && !cherr.Is(err, decimalOverflow) {
				return nil, err
			}
			tm, perr := parseBestEffort(s, q.location(to))
			if perr != nil {
				return nil, err
			}
			out = tm
			if pt.Kind == chtype.Date32 {
				out = wrapTime(dateOnly(tm), to)
			} else if tm.After(maxDateTime64) {
				return nil, err
			}
		}
		if tm, ok := out.(time.Time); ok {
			return fromParsed(tm, to), nil
		}
		return out, nil
	case uint64, int64, float64:
		return numberToTime(q, x, to), nil
	}
	return chtype.Convert(v, from, to)
}

// DateTime64 range limits (ClickHouse saturates numeric conversions to them).
var (
	minDateTime64 = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDateTime64 = time.Date(2299, 12, 31, 23, 59, 59, 0, time.UTC)
	maxDate       = time.Date(2149, 6, 6, 0, 0, 0, 0, time.UTC)
)

// numberToTime converts a number like toDate/toDateTime/toDateTime64:
// Date takes day numbers up to 65535 and unix seconds above; Date32 takes
// day numbers in its range and unix seconds above; DateTime and
// DateTime64 take unix seconds. Out-of-range values saturate.
func numberToTime(q *query, x any, to chtype.Type) any {
	var f float64
	switch n := x.(type) {
	case uint64:
		if n > math.MaxInt64 {
			f = float64(int64(n))
			if to.Kind == chtype.Date || to.Kind == chtype.DateTime || to.Kind == chtype.Date32 {
				f = float64(n) // unsigned: saturates high
			}
		} else {
			f = float64(n)
		}
	case int64:
		f = float64(n)
	case float64:
		f = n
	}
	if math.IsNaN(f) {
		f = 0
	}
	switch to.Kind {
	case chtype.Date:
		switch {
		case f < 0:
			return chtype.Epoch
		case f <= 65535:
			return chtype.Epoch.AddDate(0, 0, int(f))
		}
		if f >= float64(maxDate.Unix()+86400) {
			return maxDate
		}
		d := dateOnly(time.Unix(int64(f), 0).In(q.location(to)))
		if d.After(maxDate) {
			return maxDate
		}
		return d
	case chtype.Date32:
		minDays, maxDays := float64(dayNum(minDate32)), float64(dayNum(maxDate32))
		switch {
		case f < minDays:
			return minDate32
		case f <= maxDays:
			return chtype.Epoch.AddDate(0, 0, int(f))
		}
		if f >= float64(maxDate32.Unix()+86400) {
			return maxDate32
		}
		return wrapTime(dateOnly(time.Unix(int64(f), 0).In(q.location(to))), to)
	case chtype.DateTime:
		switch {
		case f <= 0:
			return chtype.Epoch
		case f >= math.MaxUint32:
			return time.Unix(math.MaxUint32, 0).UTC()
		}
		return time.Unix(int64(f), 0).UTC()
	case chtype.DateTime64:
		switch {
		case f < float64(minDateTime64.Unix()):
			return minDateTime64
		case f > float64(maxDateTime64.Unix()):
			return maxDateTime64
		}
		if _, isFloat := x.(float64); !isFloat {
			return time.Unix(int64(f), 0).UTC()
		}
		sec, frac := math.Modf(f)
		return truncPrecision(time.Unix(int64(sec), int64(math.Round(frac*1e9))).UTC(), to.Precision)
	}
	return chtype.Epoch
}

func fromParsed(tm time.Time, to chtype.Type) any {
	switch to.Kind {
	case chtype.Date, chtype.Date32:
		return dateOnly(tm)
	case chtype.DateTime:
		return tm.UTC().Truncate(time.Second)
	}
	return truncPrecision(tm.UTC(), to.Precision)
}

// parseBestEffort implements a practical subset of
// parseDateTimeBestEffort: ISO 8601 / RFC 3339 (with T or space, optional
// fraction and zone), YYYY-MM-DD, YYYY/MM/DD, DD/MM/YYYY, DD.MM.YYYY,
// YYYYMMDD[hhmmss], unix timestamps, and RFC 1123 / RFC 822 forms.
func parseBestEffort(s string, loc *time.Location) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty")
	}
	if isDigits(s) {
		switch len(s) {
		case 8:
			return time.ParseInLocation("20060102", s, loc)
		case 14:
			return time.ParseInLocation("20060102150405", s, loc)
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		return time.Unix(n, 0).UTC(), nil
	}
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999Z0700",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999Z0700",
		"2006-01-02 15:04:05.999999999 MST",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04Z07:00",
		"2006-01-02T15:04",
		"2006-01-02 15:04",
		"2006-01-02",
		"2006/01/02 15:04:05.999999999",
		"2006/01/02",
		"02/01/2006 15:04:05",
		"02/01/2006",
		"02.01.2006 15:04:05",
		"02.01.2006",
		time.RFC1123Z,
		time.RFC1123,
		time.RFC850,
		time.RFC822Z,
		time.RFC822,
		time.ANSIC,
		"2 Jan 2006 15:04:05",
		"2 January 2006",
		"Jan 2 2006",
		"January 2, 2006",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q", s)
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}

func buildToDate(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 2); err != nil {
		return nil, err
	}
	to := chtype.TDate
	if name == "toDate32" {
		to = chtype.Type{Kind: chtype.Date32}
	}
	return timeConv(c, name, args, to, 1)
}

func buildToDateTime(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 2); err != nil {
		return nil, err
	}
	tz, err := tzArg(name, args, 1)
	if err != nil {
		return nil, err
	}
	return timeConv(c, name, args, chtype.Type{Kind: chtype.DateTime, TZ: tz}, 1)
}

func buildToDateTime64(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 3); err != nil {
		return nil, err
	}
	p, err := precisionArg(name, args[1])
	if err != nil {
		return nil, err
	}
	tz, err := tzArg(name, args, 2)
	if err != nil {
		return nil, err
	}
	return timeConv(c, name, args, chtype.NewDateTime64(p, tz), 1)
}

// timeConv converts args[0] to a time type; extra args (precision/tz) are
// already folded into to.
func timeConv(c *compiler, name string, args []*expr, to chtype.Type, nargs int) (*expr, error) {
	a := args[0]
	from := base(a)
	k := from.Kind
	if !isTimeKind(k) && !isStringKind(k) && !isNumKind(k) && !isNothing(a.t) {
		return nil, illegalType(name, a.t, 1)
	}
	if name == "toDate" && len(args) == 2 {
		tz, err := tzArg(name, args, 1)
		if err != nil {
			return nil, err
		}
		from = withTZ(from, tz)
	}
	return strict(args[:nargs], to, func(v []any) (any, error) {
		return convertToTime(c.q, v[0], from, to)
	}), nil
}

func buildToTimeZone(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	tz, err := tzArg(name, args, 1)
	if err != nil {
		return nil, err
	}
	t := base(args[0])
	if t.Kind != chtype.DateTime && t.Kind != chtype.DateTime64 {
		return nil, illegalType(name, args[0].t, 1)
	}
	t.TZ = tz
	return strict(args[:1], t, func(v []any) (any, error) { return v[0], nil }), nil
}

func bestEffortBuilder(dt64 bool, mode convMode) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		maxArgs := 2
		if dt64 {
			maxArgs = 3
		}
		if err := wantArgs(name, args, 1, maxArgs); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0); err != nil {
			return nil, err
		}
		to := chtype.TDateTime
		tzPos := 1
		if dt64 {
			p := 3
			if len(args) > 1 {
				var err error
				if p, err = precisionArg(name, args[1]); err != nil {
					return nil, err
				}
			}
			to = chtype.NewDateTime64(p, "")
			tzPos = 2
		}
		tz, err := tzArg(name, args, tzPos)
		if err != nil {
			return nil, err
		}
		to.TZ = tz
		rt := to
		if mode == convOrNull {
			rt = chtype.NewNullable(to)
		}
		return strict(args[:1], rt, func(v []any) (any, error) {
			tm, err := parseBestEffort(v[0].(string), c.q.location(to))
			if err != nil {
				switch mode {
				case convOrNull:
					return nil, nil
				case convOrZero:
					return chtype.Epoch, nil
				}
				return nil, cherr.New(cherr.CannotParseDateTime, "Cannot read DateTime: unexpected input '%s'", v[0])
			}
			return fromParsed(saturateBestEffort(tm, to), to), nil
		}), nil
	}
}

// saturateBestEffort clamps a parsed instant to the result type's range as
// ClickHouse's parseDateTime[64]BestEffort do (verified on 25.8):
// DateTime to [1970-01-01 00:00:00, 2106-02-07 06:28:15] UTC, DateTime64 to
// 1900-01-01 00:00:00 at the low end. (DateTime64 values after 2262 cannot be
// represented here; see DESIGN.md.)
func saturateBestEffort(tm time.Time, to chtype.Type) time.Time {
	switch to.Kind {
	case chtype.DateTime:
		if tm.Before(chtype.Epoch) {
			return chtype.Epoch
		}
		if maxDT := time.Unix(math.MaxUint32, 0); tm.After(maxDT) {
			return maxDT.UTC()
		}
	case chtype.DateTime64:
		if low := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC); tm.Before(low) {
			return low
		}
	}
	return tm
}

func requireTime(name string, args []*expr, pos int) error {
	if pos < len(args) && !isTimeKind(base(args[pos]).Kind) && !isNothing(args[pos].t) {
		return illegalType(name, args[pos].t, pos+1)
	}
	return nil
}

func datePartBuilder(rt chtype.Type, f func(time.Time) uint64) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 2); err != nil {
			return nil, err
		}
		if err := requireTime(name, args, 0); err != nil {
			return nil, err
		}
		t := base(args[0])
		if len(args) == 2 {
			tz, err := tzArg(name, args, 1)
			if err != nil {
				return nil, err
			}
			t = withTZ(t, tz)
		}
		return strict(args[:1], rt, func(v []any) (any, error) {
			return f(c.q.localTime(v[0].(time.Time), t)), nil
		}), nil
	}
}

// startOfBuilder builds toStartOf* functions. Results of DateTime kind keep
// the argument's time zone.
func startOfBuilder(rt chtype.Type, f func(time.Time) time.Time) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 2); err != nil {
			return nil, err
		}
		if err := requireTime(name, args, 0); err != nil {
			return nil, err
		}
		t := base(args[0])
		if len(args) == 2 {
			tz, err := tzArg(name, args, 1)
			if err != nil {
				return nil, err
			}
			t = withTZ(t, tz)
		}
		out := rt
		if rt.Kind == chtype.DateTime && (t.Kind == chtype.DateTime || t.Kind == chtype.DateTime64) {
			out.TZ = t.TZ
		}
		return strict(args[:1], out, func(v []any) (any, error) {
			lt := c.q.localTime(v[0].(time.Time), t)
			if t.Kind == chtype.Date || t.Kind == chtype.Date32 {
				lt = time.Date(lt.Year(), lt.Month(), lt.Day(), 0, 0, 0, 0, c.q.location(out))
			}
			r := f(lt)
			if out.Kind == chtype.Date {
				return saturateDate(dateOnly(r)), nil
			}
			return r.UTC(), nil
		}), nil
	}
}

func buildToStartOfWeek(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 3); err != nil {
		return nil, err
	}
	if err := requireTime(name, args, 0); err != nil {
		return nil, err
	}
	mode := int64(0)
	if len(args) > 1 {
		if !args[1].konst || !isIntKind(base(args[1]).Kind) {
			return nil, cherr.New(cherr.IllegalTypeOfArgument, "Mode argument of %s must be a constant integer", name)
		}
		mode = asInt(args[1].val)
	}
	t := base(args[0])
	if len(args) == 3 {
		tz, err := tzArg(name, args, 2)
		if err != nil {
			return nil, err
		}
		t = withTZ(t, tz)
	}
	mondayFirst := mode&1 == 1
	return strict(args[:1], chtype.TDate, func(v []any) (any, error) {
		lt := c.q.localTime(v[0].(time.Time), t)
		back := int(lt.Weekday()) // Sunday-first
		if mondayFirst {
			back = isoWeekday(lt) - 1
		}
		return saturateDate(dateOnly(lt).AddDate(0, 0, -back)), nil
	}), nil
}

// saturateDate clamps a Date result of a start-of function to the Date
// range: ClickHouse returns 1970-01-01 when the start of the period lies
// before the epoch (toMonday(toDate('1970-01-02')) = 1970-01-01).
func saturateDate(d time.Time) time.Time {
	if d.Before(chtype.Epoch) {
		return chtype.Epoch
	}
	return d
}

func buildToUnixTimestamp(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 2); err != nil {
		return nil, err
	}
	t := base(args[0])
	if !isTimeKind(t.Kind) && !isStringKind(t.Kind) {
		return nil, illegalType(name, args[0].t, 1)
	}
	tz, err := tzArg(name, args, 1)
	if err != nil {
		return nil, err
	}
	loc := c.q.location(chtype.Type{Kind: chtype.DateTime, TZ: tz})
	return strict(args[:1], chtype.TUInt32, func(v []any) (any, error) {
		switch x := v[0].(type) {
		case time.Time:
			if t.Kind == chtype.Date || t.Kind == chtype.Date32 {
				return uint64(time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, loc).Unix()), nil
			}
			return uint64(uint32(x.Unix())), nil
		case string:
			out, err := chtype.Convert(x, chtype.TString, chtype.Type{Kind: chtype.DateTime, TZ: loc.String()})
			if err != nil {
				return nil, err
			}
			return uint64(uint32(out.(time.Time).Unix())), nil
		}
		return nil, illegalType(name, args[0].t, 1)
	}), nil
}

func buildFormatDateTime(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 3); err != nil {
		return nil, err
	}
	if err := requireTime(name, args, 0); err != nil {
		return nil, err
	}
	f, ok := args[1].val.(string)
	if !args[1].konst || !ok {
		return nil, cherr.New(cherr.IllegalColumn, "Format argument of function %s must be a constant string", name)
	}
	t := base(args[0])
	if len(args) == 3 {
		tz, err := tzArg(name, args, 2)
		if err != nil {
			return nil, err
		}
		t = withTZ(t, tz)
	}
	if err := validateDateFormat(f); err != nil {
		return nil, err
	}
	return strict(args[:1], chtype.TString, func(v []any) (any, error) {
		return formatDateTime(c.q.localTime(v[0].(time.Time), t), f), nil
	}), nil
}

const dateFormatSpecs = "abcCdDefFgGhHiIjklmMnpQrRsStTuVwWyYz%"

func validateDateFormat(f string) error {
	for i := 0; i < len(f); i++ {
		if f[i] != '%' {
			continue
		}
		if i+1 >= len(f) || !strings.ContainsRune(dateFormatSpecs, rune(f[i+1])) {
			return cherr.New(cherr.BadArguments, "Incorrect syntax '%s', symbol is not supported for formatDateTime", f[i:])
		}
		i++
	}
	return nil
}

// formatDateTime renders t with ClickHouse's formatDateTime specifiers.
func formatDateTime(t time.Time, f string) string {
	var b strings.Builder
	two := func(n int) string { return fmt.Sprintf("%02d", n) }
	hour12 := func() int {
		h := t.Hour() % 12
		if h == 0 {
			h = 12
		}
		return h
	}
	for i := 0; i < len(f); i++ {
		if f[i] != '%' || i+1 >= len(f) {
			b.WriteByte(f[i])
			continue
		}
		i++
		switch f[i] {
		case 'a':
			b.WriteString(t.Format("Mon"))
		case 'A', 'W':
			b.WriteString(t.Weekday().String())
		case 'b':
			b.WriteString(t.Format("Jan"))
		case 'B', 'M':
			b.WriteString(t.Month().String())
		case 'c', 'm':
			b.WriteString(two(int(t.Month())))
		case 'C':
			b.WriteString(two(t.Year() / 100))
		case 'd':
			b.WriteString(two(t.Day()))
		case 'D':
			b.WriteString(t.Format("01/02/06"))
		case 'e':
			b.WriteString(strconv.Itoa(t.Day()))
		case 'E':
			b.WriteString(t.Format("Mon"))
		case 'f':
			b.WriteString(fmt.Sprintf("%06d", t.Nanosecond()/1000))
		case 'F':
			b.WriteString(t.Format("2006-01-02"))
		case 'g':
			y, _ := t.ISOWeek()
			b.WriteString(two(y % 100))
		case 'G':
			y, _ := t.ISOWeek()
			b.WriteString(fmt.Sprintf("%04d", y))
		case 'h', 'I', 'l':
			b.WriteString(two(hour12()))
		case 'H', 'k':
			b.WriteString(two(t.Hour()))
		case 'i':
			b.WriteString(two(t.Minute()))
		case 'j':
			b.WriteString(fmt.Sprintf("%03d", t.YearDay()))
		case 'n':
			b.WriteByte('\n')
		case 'p':
			if t.Hour() < 12 {
				b.WriteString("AM")
			} else {
				b.WriteString("PM")
			}
		case 'Q':
			b.WriteString(strconv.Itoa((int(t.Month())-1)/3 + 1))
		case 'r':
			b.WriteString(two(hour12()) + ":" + two(t.Minute()))
			if t.Hour() < 12 {
				b.WriteString(" AM")
			} else {
				b.WriteString(" PM")
			}
		case 'R':
			b.WriteString(two(t.Hour()) + ":" + two(t.Minute()))
		case 's', 'S':
			b.WriteString(two(t.Second()))
		case 't':
			b.WriteByte('\t')
		case 'T':
			b.WriteString(t.Format("15:04:05"))
		case 'u':
			b.WriteString(strconv.Itoa(isoWeekday(t)))
		case 'V':
			_, w := t.ISOWeek()
			b.WriteString(two(w))
		case 'w':
			b.WriteString(strconv.Itoa(int(t.Weekday())))
		case 'y':
			b.WriteString(two(t.Year() % 100))
		case 'Y':
			b.WriteString(fmt.Sprintf("%04d", t.Year()))
		case 'z':
			b.WriteString(t.Format("-0700"))
		case 'Z':
			b.WriteString(t.Format("MST"))
		case '%':
			b.WriteByte('%')
		}
	}
	return b.String()
}

var dateDiffUnits = map[string]string{
	"nanosecond": "ns", "nanoseconds": "ns", "ns": "ns",
	"microsecond": "us", "microseconds": "us", "us": "us", "u": "us",
	"millisecond": "ms", "milliseconds": "ms", "ms": "ms",
	"second": "s", "seconds": "s", "ss": "s", "s": "s",
	"minute": "mi", "minutes": "mi", "mi": "mi", "n": "mi",
	"hour": "h", "hours": "h", "hh": "h", "h": "h",
	"day": "d", "days": "d", "dd": "d", "d": "d",
	"week": "w", "weeks": "w", "wk": "w", "ww": "w",
	"month": "m", "months": "m", "mm": "m", "m": "m",
	"quarter": "q", "quarters": "q", "qq": "q", "q": "q",
	"year": "y", "years": "y", "yyyy": "y", "yy": "y",
}

func buildDateDiff(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 3, 4); err != nil {
		return nil, err
	}
	us, ok := args[0].val.(string)
	if !args[0].konst || !ok {
		return nil, cherr.New(cherr.IllegalColumn, "First argument of function %s (unit) must be a constant string", name)
	}
	unit, ok := dateDiffUnits[strings.ToLower(us)]
	if !ok {
		return nil, cherr.New(cherr.BadArguments, "Function %s does not support '%s' unit", name, us)
	}
	if err := requireTime(name, args, 1); err != nil {
		return nil, err
	}
	if err := requireTime(name, args, 2); err != nil {
		return nil, err
	}
	ta, tb := base(args[1]), base(args[2])
	if len(args) == 4 {
		tz, err := tzArg(name, args, 3)
		if err != nil {
			return nil, err
		}
		ta, tb = withTZ(ta, tz), withTZ(tb, tz)
	}
	return strict(args[1:3], chtype.TInt64, func(v []any) (any, error) {
		a := c.q.localTime(v[0].(time.Time), ta)
		b := c.q.localTime(v[1].(time.Time), tb)
		return dateDiff(unit, a, b), nil
	}), nil
}

// dayNum is the number of days since 1970-01-01 of t's wall-clock date.
func dayNum(t time.Time) int64 {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// dateDiff counts unit boundaries crossed from a to b (ClickHouse semantics).
func dateDiff(unit string, a, b time.Time) int64 {
	switch unit {
	case "ns":
		return b.Sub(a).Nanoseconds()
	case "us":
		return b.Sub(a).Microseconds()
	case "ms":
		return b.UnixMilli() - a.UnixMilli()
	case "s":
		return b.Unix() - a.Unix()
	case "mi":
		return floorDiv(wallSeconds(b), 60) - floorDiv(wallSeconds(a), 60)
	case "h":
		return floorDiv(wallSeconds(b), 3600) - floorDiv(wallSeconds(a), 3600)
	case "d":
		return dayNum(b) - dayNum(a)
	case "w":
		return floorDiv(dayNum(b)+3, 7) - floorDiv(dayNum(a)+3, 7)
	case "m":
		return int64(b.Year()*12+int(b.Month())) - int64(a.Year()*12+int(a.Month()))
	case "q":
		return int64(b.Year()*4+(int(b.Month())-1)/3) - int64(a.Year()*4+(int(a.Month())-1)/3)
	case "y":
		return int64(b.Year() - a.Year())
	}
	return 0
}

// wallSeconds is seconds since epoch of the wall clock (zone offset applied).
func wallSeconds(t time.Time) int64 {
	_, off := t.Zone()
	return t.Unix() + int64(off)
}

// addMonthsClamped adds n months keeping the day clamped to the month end.
func addMonthsClamped(t time.Time, n int) time.Time {
	y, m, d := t.Date()
	total := int(m) - 1 + n
	ny := y + floorDivInt(total, 12)
	nm := time.Month(total - floorDivInt(total, 12)*12 + 1)
	last := time.Date(ny, nm+1, 0, 0, 0, 0, 0, t.Location()).Day()
	if d > last {
		d = last
	}
	return time.Date(ny, nm, d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
}

func floorDivInt(a, b int) int {
	return int(floorDiv(int64(a), int64(b)))
}

// addIntervalBuilder builds addX/subtractX(time, n).
func addIntervalBuilder(unit string, subtract bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, 2); err != nil {
			return nil, err
		}
		if err := requireTime(name, args, 0); err != nil {
			return nil, err
		}
		if !isIntKind(base(args[1]).Kind) && !isFloatKind(base(args[1]).Kind) {
			return nil, illegalType(name, args[1].t, 2)
		}
		t := base(args[0])
		rt := t
		calendar := unit == "DAY" || unit == "WEEK" || unit == "MONTH" || unit == "QUARTER" || unit == "YEAR"
		if (t.Kind == chtype.Date || t.Kind == chtype.Date32) && !calendar {
			rt = chtype.TDateTime
			if unit == "MILLISECOND" || unit == "MICROSECOND" || unit == "NANOSECOND" {
				rt = chtype.NewDateTime64(map[string]int{"MILLISECOND": 3, "MICROSECOND": 6, "NANOSECOND": 9}[unit], "")
			}
		}
		if t.Kind == chtype.DateTime && (unit == "MILLISECOND" || unit == "MICROSECOND" || unit == "NANOSECOND") {
			rt = chtype.NewDateTime64(map[string]int{"MILLISECOND": 3, "MICROSECOND": 6, "NANOSECOND": 9}[unit], t.TZ)
		}
		return strict(args, rt, func(v []any) (any, error) {
			n := asInt(v[1])
			if subtract {
				n = -n
			}
			tm := v[0].(time.Time)
			var lt time.Time
			if t.Kind == chtype.Date || t.Kind == chtype.Date32 {
				lt = time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, c.q.location(rt))
			} else {
				lt = tm.In(c.q.location(t))
			}
			var r time.Time
			switch unit {
			case "NANOSECOND":
				r = lt.Add(time.Duration(n))
			case "MICROSECOND":
				r = lt.Add(time.Duration(n) * time.Microsecond)
			case "MILLISECOND":
				r = lt.Add(time.Duration(n) * time.Millisecond)
			case "SECOND":
				r = lt.Add(time.Duration(n) * time.Second)
			case "MINUTE":
				r = lt.Add(time.Duration(n) * time.Minute)
			case "HOUR":
				r = lt.Add(time.Duration(n) * time.Hour)
			case "DAY":
				r = lt.AddDate(0, 0, int(n))
			case "WEEK":
				r = lt.AddDate(0, 0, int(n)*7)
			case "MONTH":
				r = addMonthsClamped(lt, int(n))
			case "QUARTER":
				r = addMonthsClamped(lt, int(n)*3)
			case "YEAR":
				r = addMonthsClamped(lt, int(n)*12)
			}
			return wrapTime(r, rt), nil
		}), nil
	}
}

// Date32 range limits.
var (
	minDate32 = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDate32 = time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC)
)

// wrapTime normalizes a date/time arithmetic result to the range of rt the
// way ClickHouse does: Date wraps modulo 2^16 days, DateTime modulo 2^32
// seconds, Date32 saturates.
func wrapTime(r time.Time, rt chtype.Type) time.Time {
	switch rt.Kind {
	case chtype.Date:
		d := ((dayNum(r) % 65536) + 65536) % 65536
		return chtype.Epoch.AddDate(0, 0, int(d))
	case chtype.Date32:
		d := dateOnly(r)
		switch {
		case d.Before(minDate32):
			return minDate32
		case d.After(maxDate32):
			return maxDate32
		}
		return d
	case chtype.DateTime:
		s := r.Unix() % (1 << 32)
		if s < 0 {
			s += 1 << 32
		}
		return time.Unix(s, 0).UTC()
	}
	return r.UTC()
}

// timeArith implements time ± number and time - time.
func timeArith(c *compiler, op string, args []*expr) (*expr, error) {
	a, b := base(args[0]), base(args[1])
	switch {
	case isTimeKind(a.Kind) && isTimeKind(b.Kind):
		if op != "minus" {
			return nil, cherr.New(cherr.IllegalTypeOfArgument, "Illegal types %s and %s of arguments of function plus", a.String(), b.String())
		}
		if a.Kind == chtype.DateTime64 || b.Kind == chtype.DateTime64 {
			return nil, cherr.New(cherr.NotImplemented, "DateTime64 - DateTime64 (Decimal result) is not supported; use dateDiff")
		}
		days := (a.Kind == chtype.Date || a.Kind == chtype.Date32) && (b.Kind == chtype.Date || b.Kind == chtype.Date32)
		return strict(args, chtype.TInt32, func(v []any) (any, error) {
			x, y := v[0].(time.Time), v[1].(time.Time)
			if days {
				return int64(int32(dayNum(x) - dayNum(y))), nil
			}
			return int64(int32(x.Unix() - y.Unix())), nil
		}), nil
	case isTimeKind(a.Kind) && isNumKind(b.Kind):
	case isNumKind(a.Kind) && isTimeKind(b.Kind) && op == "plus":
		args = []*expr{args[1], args[0]}
		a = b
	default:
		return nil, cherr.New(cherr.IllegalTypeOfArgument, "Illegal types %s and %s of arguments of function %s", a.String(), b.String(), op)
	}
	unit := "SECOND"
	if a.Kind == chtype.Date || a.Kind == chtype.Date32 {
		unit = "DAY"
	}
	return addIntervalBuilder(unit, op == "minus")(c, op, args)
}

func buildToStartOfSecond(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 2); err != nil {
		return nil, err
	}
	t := base(args[0])
	if t.Kind != chtype.DateTime64 {
		return nil, illegalType(name, args[0].t, 1)
	}
	return strict(args[:1], t, func(v []any) (any, error) { return v[0].(time.Time).Truncate(time.Second), nil }), nil
}
