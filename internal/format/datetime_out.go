package format

import (
	"strconv"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// dtOutMode is the date_time_output_format setting.
type dtOutMode uint8

const (
	dtSimple dtOutMode = iota // 2024-01-02 03:04:05[.fff] in the column's zone
	dtISO                     // 2024-01-02T03:04:05[.fff]Z, always UTC
	dtUnix                    // 1704164645[.fff]
)

// dateTimeOutputMode reads date_time_output_format (values are
// case-sensitive in ClickHouse; anything else is BAD_ARGUMENTS).
func dateTimeOutputMode(s map[string]string) (dtOutMode, error) {
	v, ok := s["date_time_output_format"]
	if !ok {
		return dtSimple, nil
	}
	switch v {
	case "simple":
		return dtSimple, nil
	case "iso":
		return dtISO, nil
	case "unix_timestamp":
		return dtUnix, nil
	}
	return dtSimple, cherr.New(cherr.BadArguments,
		"Unexpected value of DateTimeOutputFormat: '%s'. Must be one of ['unix_timestamp', 'iso', 'simple']", v)
}

// renderTypes returns the types the text encoders render values with: the
// display types (server timezone applied) and, when date_time_output_format
// is not simple, every DateTime/DateTime64 replaced by String — the
// dtEncoder wrapper converts those values to their iso/unix text first, and
// ClickHouse quotes them exactly like strings (JSON "…", CSV "…", ['…']).
func renderTypes(cols []Column, opts Options) []chtype.Type {
	ts := displayTypes(cols, opts.Timezone)
	if m, _ := dateTimeOutputMode(opts.Settings); m != dtSimple {
		for i := range ts {
			ts[i] = dtAsString(ts[i])
		}
	}
	return ts
}

// hasDateTime reports whether t contains a DateTime or DateTime64 anywhere.
func hasDateTime(t chtype.Type) bool {
	switch t.Kind {
	case chtype.DateTime, chtype.DateTime64:
		return true
	case chtype.Tuple:
		for _, f := range t.Fields {
			if hasDateTime(f.Type) {
				return true
			}
		}
		return false
	}
	return t.Elem != nil && hasDateTime(*t.Elem) || t.Key != nil && hasDateTime(*t.Key)
}

// dtAsString replaces DateTime/DateTime64 inside t by String.
func dtAsString(t chtype.Type) chtype.Type {
	if !hasDateTime(t) {
		return t
	}
	switch t.Kind {
	case chtype.DateTime, chtype.DateTime64:
		return chtype.TString
	case chtype.Tuple:
		fs := make([]chtype.Field, len(t.Fields))
		for i, f := range t.Fields {
			fs[i] = chtype.Field{Name: f.Name, Type: dtAsString(f.Type)}
		}
		t.Fields = fs
		return t
	}
	if t.Elem != nil {
		e := dtAsString(*t.Elem)
		t.Elem = &e
	}
	if t.Key != nil {
		k := dtAsString(*t.Key)
		t.Key = &k
	}
	return t
}

// convertDT rewrites the DateTime/DateTime64 values inside v (of display
// type t) to their date_time_output_format text.
func convertDT(t chtype.Type, v any, m dtOutMode) any {
	if v == nil {
		return nil
	}
	switch t.Kind {
	case chtype.DateTime, chtype.DateTime64:
		tm, ok := v.(time.Time)
		if !ok {
			return v
		}
		prec := 0
		if t.Kind == chtype.DateTime64 {
			prec = t.Precision
		}
		return string(appendDTOutput(nil, tm, prec, m))
	case chtype.Nullable, chtype.LowCardinality, chtype.SimpleAggregateFunction:
		if t.Elem == nil {
			return v
		}
		return convertDT(*t.Elem, v, m)
	case chtype.Array:
		arr, ok := v.([]any)
		if !ok || t.Elem == nil {
			return v
		}
		out := make([]any, len(arr))
		for i, e := range arr {
			out[i] = convertDT(*t.Elem, e, m)
		}
		return out
	case chtype.Tuple:
		tup, ok := v.([]any)
		if !ok {
			return v
		}
		out := make([]any, len(tup))
		for i, e := range tup {
			if i < len(t.Fields) {
				out[i] = convertDT(t.Fields[i].Type, e, m)
			} else {
				out[i] = e
			}
		}
		return out
	case chtype.Map:
		pairs, ok := v.([]any)
		if !ok || t.Key == nil || t.Elem == nil {
			return v
		}
		out := make([]any, len(pairs))
		for i, p := range pairs {
			kv, ok := p.([]any)
			if !ok || len(kv) != 2 {
				out[i] = p
				continue
			}
			out[i] = []any{convertDT(*t.Key, kv[0], m), convertDT(*t.Elem, kv[1], m)}
		}
		return out
	}
	return v
}

var pow10i = [...]int64{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000}

// appendDTOutput renders an instant with prec fractional digits in the iso
// or unix_timestamp form, as ClickHouse does:
//
//	iso:  2024-01-02T03:04:05Z, 2024-01-02T03:04:05.500Z (UTC)
//	unix: 1704164645, 1704164645.500, -315521754.500
func appendDTOutput(b []byte, tm time.Time, prec int, m dtOutMode) []byte {
	prec = max(0, min(prec, 9))
	if m == dtISO {
		u := tm.UTC()
		b = u.AppendFormat(b, "2006-01-02T15:04:05")
		if prec > 0 {
			b = append(b, '.')
			b = appendPadded(b, int64(u.Nanosecond())/pow10i[9-prec], prec)
		}
		return append(b, 'Z')
	}
	sec, nsec := tm.Unix(), int64(tm.Nanosecond())
	if prec == 0 {
		return strconv.AppendInt(b, sec, 10)
	}
	// Decimal rendering: sign, |whole|, '.', |fraction| (the instant is
	// sec + nsec/1e9 with 0 <= nsec < 1e9).
	if sec < 0 && nsec > 0 {
		b = append(b, '-')
		b = strconv.AppendInt(b, -sec-1, 10)
		nsec = 1e9 - nsec
	} else {
		b = strconv.AppendInt(b, sec, 10)
	}
	b = append(b, '.')
	return appendPadded(b, nsec/pow10i[9-prec], prec)
}

func appendPadded(b []byte, v int64, width int) []byte {
	var tmp [20]byte
	s := strconv.AppendInt(tmp[:0], v, 10)
	for i := len(s); i < width; i++ {
		b = append(b, '0')
	}
	return append(b, s...)
}

// dtEncoder converts DateTime values to the date_time_output_format text
// before handing rows to a text encoder built with renderTypes.
type dtEncoder struct {
	inner Encoder
	types []chtype.Type // display types (with DateTime)
	conv  []bool
	mode  dtOutMode
}

func wrapDateTimeOutput(inner Encoder, cols []Column, opts Options, m dtOutMode) Encoder {
	if m == dtSimple {
		return inner
	}
	types := displayTypes(cols, opts.Timezone)
	conv := make([]bool, len(types))
	anyConv := false
	for i, t := range types {
		conv[i] = hasDateTime(t)
		anyConv = anyConv || conv[i]
	}
	if !anyConv {
		return inner
	}
	return &dtEncoder{inner: inner, types: types, conv: conv, mode: m}
}

func (e *dtEncoder) WriteRows(rows [][]any) error {
	if err := checkRowWidth(rows, len(e.types)); err != nil {
		return err
	}
	out := make([][]any, len(rows))
	for r, row := range rows {
		nr := make([]any, len(row))
		for i, v := range row {
			if e.conv[i] {
				v = convertDT(e.types[i], v, e.mode)
			}
			nr[i] = v
		}
		out[r] = nr
	}
	return e.inner.WriteRows(out)
}

func (e *dtEncoder) Finish(s Stats) error { return e.inner.Finish(s) }
