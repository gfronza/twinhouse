package chtype

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
)

// uuidRefOrder is `SELECT toUUID(s) u ... ORDER BY u` on ClickHouse 25.8:
// a UUID orders as UInt128 with the last 8 bytes as the high half.
var uuidRefOrder = []string{
	"00000000-0000-0001-0000-000000000000",
	"00000000-0001-0000-0000-000000000000",
	"10000000-0000-0000-0000-000000000000",
	"ffffffff-ffff-ffff-0000-000000000000",
	"00000000-0000-0000-0000-000000000001",
	"00000000-0000-0000-0000-000000000100",
	"00000000-0000-0000-0000-010000000000",
	"00000000-0000-0000-0100-000000000000",
	"00000000-0000-0000-ff00-000000000000",
}

func TestCompareTypedUUIDOrder(t *testing.T) {
	shuffled := slices.Clone(uuidRefOrder)
	rand.New(rand.NewSource(1)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	slices.SortFunc(shuffled, func(a, b string) int { return CompareTyped(a, b, TUUID) })
	if !slices.Equal(shuffled, uuidRefOrder) {
		t.Fatalf("ORDER BY u:\n got %v\nwant %v", shuffled, uuidRefOrder)
	}
	for i := range uuidRefOrder {
		for j := range uuidRefOrder {
			want := cmpInt(i, j)
			if got := CompareTyped(uuidRefOrder[i], uuidRefOrder[j], TUUID); got != want {
				t.Errorf("CompareTyped(%s, %s) = %d, want %d", uuidRefOrder[i], uuidRefOrder[j], got, want)
			}
			for _, h := range []int{1, -1} {
				if got := CompareTypedHint(uuidRefOrder[i], uuidRefOrder[j], TUUID, h); got != want {
					t.Errorf("CompareTypedHint(%s, %s, %d) = %d, want %d", uuidRefOrder[i], uuidRefOrder[j], h, got, want)
				}
			}
		}
	}
	// toUUID('00000000-0000-0001-0000-000000000000') <
	// toUUID('00000000-0000-0000-0000-000000000001') is 1 on ClickHouse.
	if c := CompareTyped("00000000-0000-0001-0000-000000000000", "00000000-0000-0000-0000-000000000001", TUUID); c >= 0 {
		t.Fatalf("scalar UUID < = %d, want < 0", c)
	}
	// Non-canonical spellings compare by value.
	if c := CompareTyped("00000000000000010000000000000000", "00000000-0000-0000-0000-00000000000A", TUUID); c >= 0 {
		t.Fatalf("non-canonical UUID compare = %d, want < 0", c)
	}
	if c := CompareTyped("00000000-0000-0000-0000-00000000000A", "00000000-0000-0000-0000-00000000000a", TUUID); c != 0 {
		t.Fatalf("case-insensitive UUID equality = %d", c)
	}
}

func TestCompareTypedNested(t *testing.T) {
	lo, hi := "00000000-0000-0001-0000-000000000000", "00000000-0000-0000-0000-000000000001"
	cases := []struct {
		typ  string
		a, b any
		want int // ClickHouse 25.8 a <=> b
	}{
		{"Nullable(UUID)", lo, hi, -1},
		{"LowCardinality(Nullable(UUID))", hi, lo, 1},
		// [lo] < [hi, hi] = 1
		{"Array(UUID)", []any{lo}, []any{hi, hi}, -1},
		{"Array(UUID)", []any{hi}, []any{lo, lo}, 1},
		{"Array(Nullable(UUID))", []any{lo, nil}, []any{lo, hi}, 1},
		// (lo, 1) < (hi, 1) = 1
		{"Tuple(UUID, UInt8)", []any{lo, uint64(1)}, []any{hi, uint64(1)}, -1},
		{"Tuple(a UInt8, b Array(UUID))", []any{uint64(1), []any{hi}}, []any{uint64(1), []any{lo}}, 1},
		// map(lo,1) < map(hi,1) = 1
		{"Map(UUID, UInt8)", []any{[]any{lo, uint64(1)}}, []any{[]any{hi, uint64(1)}}, -1},
		{"Map(String, UUID)", []any{[]any{"k", hi}}, []any{[]any{"k", lo}}, 1},
		// CAST('b','Enum8(\'b\'=1,\'a\'=2)') < CAST('a', ...) = 1
		{"Enum8('b' = 1, 'a' = 2)", "b", "a", -1},
		{"Array(Enum8('b' = 1, 'a' = 2))", []any{"a"}, []any{"b", "b"}, 1},
		// Types without UUID/Enum keep Compare's order.
		{"String", hi, lo, -1},
		{"Array(String)", []any{"b"}, []any{"a", "z"}, 1},
	}
	for _, c := range cases {
		typ := MustParse(c.typ)
		if got := CompareTyped(c.a, c.b, typ); got != c.want {
			t.Errorf("CompareTyped(%v, %v, %s) = %d, want %d", c.a, c.b, c.typ, got, c.want)
		}
		if got := CompareTyped(c.b, c.a, typ); got != -c.want {
			t.Errorf("CompareTyped(%v, %v, %s) = %d, want %d", c.b, c.a, c.typ, got, -c.want)
		}
		k := OrderKey(typ)
		if k == nil {
			if got := Compare(c.a, c.b); got != c.want {
				t.Errorf("OrderKey(%s) is nil but Compare = %d, want %d", c.typ, got, c.want)
			}
			continue
		}
		if got := Compare(k(c.a), k(c.b)); got != c.want {
			t.Errorf("Compare(OrderKey(%s)) = %d, want %d", c.typ, got, c.want)
		}
	}
}

// TestCompareTypedNullHint checks that CompareTyped keeps Compare's NULL
// placement and CompareTypedHint keeps CompareHint's.
func TestCompareTypedNullHint(t *testing.T) {
	u := MustParse("Nullable(UUID)")
	a := "00000000-0000-0000-0000-000000000001"
	if CompareTyped(nil, a, u) != -1 || CompareTyped(a, nil, u) != 1 || CompareTyped(nil, nil, u) != 0 {
		t.Fatal("CompareTyped: top-level NULL must sort first")
	}
	if CompareTypedHint(nil, a, u, 1) != 1 || CompareTypedHint(nil, a, u, -1) != -1 {
		t.Fatal("CompareTypedHint: NULL must follow the hint")
	}
	arr := MustParse("Array(Nullable(UUID))")
	if CompareTyped([]any{nil}, []any{a}, arr) != 1 {
		t.Fatal("nested NULL must compare greater with Compare semantics")
	}
	if CompareTypedHint([]any{nil}, []any{a}, arr, -1) != -1 {
		t.Fatal("nested NULL must follow the hint")
	}
}

func randUUID(r *rand.Rand) string {
	const hexd = "0123456789abcdef"
	var b strings.Builder
	for i := 0; i < 32; i++ {
		if i == 8 || i == 12 || i == 16 || i == 20 {
			b.WriteByte('-')
		}
		// Few distinct digits so that ties and shared prefixes are common.
		b.WriteByte(hexd[[]int{0, 1, 15}[r.Intn(3)]])
	}
	return b.String()
}

// TestUUIDOrderKeyAgrees checks that UUIDOrderKey (fixed width, for key
// encodings) and OrderKey order bytewise / under Compare exactly as
// CompareTyped orders, i.e. as ClickHouse's UInt128 order.
func TestUUIDOrderKeyAgrees(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	tup := MustParse("Tuple(Nullable(UUID), Array(UUID))")
	tk := OrderKey(tup)
	for i := 0; i < 20000; i++ {
		a, b := randUUID(r), randUUID(r)
		want := CompareTyped(a, b, TUUID)
		ka, kb := UUIDOrderKey(a), UUIDOrderKey(b)
		if len(ka) != 32 || len(kb) != 32 {
			t.Fatalf("UUIDOrderKey width: %q %q", ka, kb)
		}
		if got := strings.Compare(ka, kb); got != want {
			t.Fatalf("UUIDOrderKey(%s)=%s vs UUIDOrderKey(%s)=%s: %d, want %d", a, ka, b, kb, got, want)
		}
		ta := []any{a, []any{b}}
		tb := []any{b, []any{a}}
		if r.Intn(4) == 0 {
			ta[0] = nil
		}
		if got, want := Compare(tk(ta), tk(tb)), CompareTyped(ta, tb, tup); got != want {
			t.Fatalf("OrderKey tuple %v vs %v: %d, want %d", ta, tb, got, want)
		}
	}
	// Reference values: big-endian UInt128 text with the halves swapped.
	if got := UUIDOrderKey("01234567-89ab-cdef-0011-223344556677"); got != "00112233445566770123456789abcdef" {
		t.Fatalf("UUIDOrderKey = %s", got)
	}
	if got := UUIDOrderKey("0123456789ABCDEF0011223344556677"); got != "00112233445566770123456789abcdef" {
		t.Fatalf("UUIDOrderKey(undashed upper) = %s", got)
	}
	if got := UUIDOrderKey("not a uuid"); got != "not a uuid" {
		t.Fatalf("UUIDOrderKey(non-uuid) = %s", got)
	}
}

// TestOrderKeyNilForPlainTypes: types without UUID/Enum need no key.
func TestOrderKeyNilForPlainTypes(t *testing.T) {
	for _, s := range []string{"String", "UInt64", "Array(Tuple(String, Float64))", "Map(String, Array(DateTime))", "Nullable(Date)"} {
		if OrderKey(MustParse(s)) != nil {
			t.Errorf("OrderKey(%s) should be nil", s)
		}
	}
	for _, s := range []string{"UUID", "LowCardinality(Nullable(UUID))", "Array(UUID)", "Map(String, UUID)", "Tuple(UInt8, Enum8('a' = 1))"} {
		if OrderKey(MustParse(s)) == nil {
			t.Errorf("OrderKey(%s) should not be nil", s)
		}
	}
}

// TestNormalizeGoMapSortsUUIDKeys: Go maps normalize to entries sorted by
// key in ClickHouse order.
func TestNormalizeGoMapSortsUUIDKeys(t *testing.T) {
	lo, hi := "00000000-0000-0001-0000-000000000000", "00000000-0000-0000-0000-000000000001"
	v, err := Normalize(MustParse("Map(UUID, UInt8)"), map[string]int{hi: 2, lo: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(v); got != fmt.Sprint([]any{[]any{lo, uint64(1)}, []any{hi, uint64(2)}}) {
		t.Fatalf("got %s", got)
	}
}
