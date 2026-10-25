package chtype

import (
	"testing"

	"dynamodb-eventstream/internal/cherr"
)

type typeCase struct {
	in, want string
	code     int
}

// typeDeviations maps inputs whose outcome differs from ClickHouse on
// purpose to the error code Parse returns instead (0: accepted although
// ClickHouse rejects it).
var typeDeviations = map[string]int{
	// Known ClickHouse types this server does not implement.
	"Decimal(10,2)": cherr.NotImplemented, "Decimal32(2)": cherr.NotImplemented,
	"IPv4": cherr.NotImplemented, "IPv6": cherr.NotImplemented, "INET4": cherr.NotImplemented,
	"Int128": cherr.NotImplemented, "UInt256": cherr.NotImplemented, "JSON": cherr.NotImplemented,
	"Nested(a UInt8)": cherr.NotImplemented, "Variant(UInt8, String)": cherr.NotImplemented,
	"Dynamic": cherr.NotImplemented, "Point": cherr.NotImplemented, "BFloat16": cherr.NotImplemented,
	"Object('json')": cherr.NotImplemented,
	// ClickHouse silently drops an unquoted timezone; we reject it.
	"DateTime64(3, UTC)": cherr.IllegalTypeOfArgument,
	// Implicit enum numbering after an implicit first element is rejected
	// (ClickHouse accepts this two-element form only).
	"Enum('a', 'b' = 5)": codeUnexpectedASTStructure,
	// Accepted although ClickHouse requires allow_suspicious_low_cardinality_types=1.
	"LowCardinality(UInt8)": 0, "LowCardinality(Date)": 0, "LowCardinality(Bool)": 0, "LowCardinality(Float64)": 0,
	// Unknown aggregate functions are left for the engine to reject.
	"AggregateFunction(foo, UInt8)": 0,
	// ClickHouse prints a state version for sumMap ("AggregateFunction(1, sumMap, ...)").
	"AggregateFunction(sumMap, Array(UInt8), Array(UInt64))": 0,
}

// TestTypeGolden checks Parse and String against ClickHouse's own type
// factory (toTypeName(CAST(... AS t)) with print_pretty_type_names=0).
func TestTypeGolden(t *testing.T) {
	for _, c := range typeGolden {
		t.Run(c.in, func(t *testing.T) {
			typ, err := Parse(c.in)
			if want, ok := typeDeviations[c.in]; ok {
				switch {
				case want == 0 && err != nil:
					t.Fatalf("expected acceptance, got %v", err)
				case want != 0 && !cherr.Is(err, want):
					t.Fatalf("expected error code %d, got %v (%v)", want, err, typ)
				}
				return
			}
			if c.code != 0 {
				if err == nil {
					t.Fatalf("ClickHouse rejects this (code %d); got %s", c.code, typ)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v (ClickHouse: %q)", err, c.want)
			}
			if got := typ.String(); got != c.want {
				t.Fatalf("String() = %q, want %q", got, c.want)
			}
			again, err := Parse(typ.String())
			if err != nil || !again.Equal(typ) {
				t.Fatalf("re-parse of %q: %v, equal=%v", typ.String(), err, again.Equal(typ))
			}
		})
	}
}
