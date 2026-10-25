package memstore

import (
	"time"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/storage"
)

// checkRow verifies that every stored value uses the canonical Go
// representation of its column type (the chtype contract), like the
// DynamoDB codec does, so engine bugs surface in tests.
func checkRow(t *catalog.Table, row storage.Row) error {
	for i := range t.Columns {
		c := &t.Columns[i]
		if !stored(c) {
			continue
		}
		if err := checkValue(c.Type, row[i]); err != nil {
			return cherr.New(cherr.TypeMismatch, "column %s of %s: %v", c.Name, t.FullName(), err)
		}
	}
	return nil
}

func checkValue(t chtype.Type, v any) error {
	bad := func() error {
		return cherr.New(cherr.TypeMismatch, "value %#v is not a canonical %s", v, t.String())
	}
	switch t.Kind {
	case chtype.Nullable:
		if v == nil {
			return nil
		}
		return checkValue(*t.Elem, v)
	case chtype.LowCardinality, chtype.SimpleAggregateFunction:
		return checkValue(*t.Elem, v)
	}
	if v == nil {
		return cherr.New(cherr.TypeMismatch, "NULL value for non-Nullable type %s", t.String())
	}
	ok := true
	switch t.Kind {
	case chtype.Bool:
		_, ok = v.(bool)
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64:
		var u uint64
		if u, ok = v.(uint64); ok {
			ok = t.Kind == chtype.UInt64 || u < 1<<(8*intWidth(t.Kind))
		}
	case chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		var n int64
		if n, ok = v.(int64); ok && t.Kind != chtype.Int64 {
			lim := int64(1) << (8*intWidth(t.Kind) - 1)
			ok = n >= -lim && n < lim
		}
	case chtype.Float32, chtype.Float64:
		_, ok = v.(float64)
	case chtype.String, chtype.UUID, chtype.Enum8, chtype.Enum16:
		_, ok = v.(string)
	case chtype.FixedString:
		var s string
		if s, ok = v.(string); ok {
			ok = len(s) == t.N
		}
	case chtype.Date, chtype.Date32:
		var tm time.Time
		if tm, ok = v.(time.Time); ok {
			ok = tm.Location() == time.UTC && tm.Hour() == 0 && tm.Minute() == 0 && tm.Second() == 0 && tm.Nanosecond() == 0
		}
	case chtype.DateTime, chtype.DateTime64:
		_, ok = v.(time.Time)
	case chtype.Array:
		arr, isArr := v.([]any)
		if !isArr {
			return bad()
		}
		for _, e := range arr {
			if err := checkValue(*t.Elem, e); err != nil {
				return err
			}
		}
	case chtype.Tuple:
		tup, isTup := v.([]any)
		if !isTup || len(tup) != len(t.Fields) {
			return bad()
		}
		for i, f := range t.Fields {
			if err := checkValue(f.Type, tup[i]); err != nil {
				return err
			}
		}
	case chtype.Map:
		pairs, isMap := v.([]any)
		if !isMap {
			return bad()
		}
		for _, p := range pairs {
			kv, isKV := p.([]any)
			if !isKV || len(kv) != 2 {
				return bad()
			}
			if err := checkValue(*t.Key, kv[0]); err != nil {
				return err
			}
			if err := checkValue(*t.Elem, kv[1]); err != nil {
				return err
			}
		}
	case chtype.AggregateFunction:
		_, ok = v.(*chtype.AggPartial)
	}
	if !ok {
		return bad()
	}
	return nil
}

func intWidth(k chtype.Kind) uint {
	switch k {
	case chtype.UInt8, chtype.Int8:
		return 1
	case chtype.UInt16, chtype.Int16:
		return 2
	case chtype.UInt32, chtype.Int32:
		return 4
	}
	return 8
}
