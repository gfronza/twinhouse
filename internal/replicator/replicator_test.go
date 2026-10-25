package replicator

import (
	"context"
	"strings"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"dynamodb-eventstream/internal/awsx"
)

func TestNewValidates(t *testing.T) {
	c := &awsx.Clients{}
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"one region", Config{Regions: []Region{{"a", c}}, Owner: "o"}, false},
		{"duplicate", Config{Regions: []Region{{"a", c}, {"a", c}}, Owner: "o"}, false},
		{"unnamed", Config{Regions: []Region{{"", c}, {"b", c}}, Owner: "o"}, false},
		{"no clients", Config{Regions: []Region{{"a", nil}, {"b", c}}, Owner: "o"}, false},
		{"no owner", Config{Regions: []Region{{"a", c}, {"b", c}}}, false},
		{"ok", Config{Regions: []Region{{"a", c}, {"b", c}}, Owner: "o", Prefix: "p_"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := New(tc.cfg)
			if (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok=%v", err, tc.ok)
			}
			if tc.ok {
				if r.CatalogTable() != "p___catalog" || r.CheckpointTable() != "p___checkpoints" {
					t.Fatalf("names: %s %s", r.CatalogTable(), r.CheckpointTable())
				}
				if tables, err := r.cfg.Tables(context.Background()); err != nil || tables != nil {
					t.Fatalf("default table source: %v %v", tables, err)
				}
			}
		})
	}
}

func rec(keys map[string]sttypes.AttributeValue) sttypes.Record {
	return sttypes.Record{Dynamodb: &sttypes.StreamRecord{Keys: keys}}
}

func TestLaneOfIsDeterministicPerKey(t *testing.T) {
	k1 := map[string]sttypes.AttributeValue{
		"_pk": &sttypes.AttributeValueMemberB{Value: []byte{1, 0, 2}},
		"_sk": &sttypes.AttributeValueMemberB{Value: []byte{3}},
	}
	k2 := map[string]sttypes.AttributeValue{
		"_sk": &sttypes.AttributeValueMemberB{Value: []byte{3}},
		"_pk": &sttypes.AttributeValueMemberB{Value: []byte{1, 0, 2}},
	}
	for range 20 {
		if laneOf(rec(k1), 8) != laneOf(rec(k2), 8) {
			t.Fatal("same key mapped to different lanes")
		}
	}
	if laneOf(rec(k1), 1) != 0 || laneOf(sttypes.Record{}, 8) != 0 {
		t.Fatal("degenerate lanes")
	}
	lanes := map[int]bool{}
	for i := range 64 {
		k := map[string]sttypes.AttributeValue{"_pk": &sttypes.AttributeValueMemberS{Value: string(rune('a' + i))}}
		lanes[laneOf(rec(k), 8)] = true
	}
	if len(lanes) < 4 {
		t.Fatalf("poor key distribution: %v", lanes)
	}
}

func TestAttrHelpers(t *testing.T) {
	img := map[string]sttypes.AttributeValue{
		"_origin": &sttypes.AttributeValueMemberS{Value: "us"},
		"_ts":     &sttypes.AttributeValueMemberN{Value: "12"},
		"bad":     &sttypes.AttributeValueMemberB{Value: []byte("x")},
	}
	if v, ok := stringAttr(img, "_origin"); !ok || v != "us" {
		t.Fatal("stringAttr")
	}
	if v, ok := numberAttr(img, "_ts"); !ok || v != "12" {
		t.Fatal("numberAttr")
	}
	if _, ok := stringAttr(img, "bad"); ok {
		t.Fatal("stringAttr accepted B")
	}
	if _, ok := numberAttr(nil, "_ts"); ok {
		t.Fatal("numberAttr on nil image")
	}
}

func TestPutCondition(t *testing.T) {
	n := func(v string) *ddbtypes.AttributeValueMemberN { return &ddbtypes.AttributeValueMemberN{Value: v} }
	cases := []struct {
		name, ver string
		item      map[string]ddbtypes.AttributeValue
		cond      string
		verValue  string // expected :ver ("" = absent)
	}{
		{"lww", "", map[string]ddbtypes.AttributeValue{"ver": n("5")},
			"attribute_not_exists(#pk) OR (#ts < :ts OR (#ts = :ts AND #o < :o))", ""},
		{"version", "ver", map[string]ddbtypes.AttributeValue{"ver": n("5")},
			"attribute_not_exists(#pk) OR (attribute_not_exists(#ver) OR attribute_type(#ver, :null)) OR #ver < :ver OR (#ver = :ver AND (#ts < :ts OR (#ts = :ts AND #o < :o)))", "5"},
		{"null version", "ver", map[string]ddbtypes.AttributeValue{"ver": &ddbtypes.AttributeValueMemberNULL{Value: true}},
			"attribute_not_exists(#pk) OR ((attribute_not_exists(#ver) OR attribute_type(#ver, :null)) AND (#ts < :ts OR (#ts = :ts AND #o < :o)))", ""},
		{"missing version", "ver", map[string]ddbtypes.AttributeValue{},
			"attribute_not_exists(#pk) OR ((attribute_not_exists(#ver) OR attribute_type(#ver, :null)) AND (#ts < :ts OR (#ts = :ts AND #o < :o)))", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cond, names, values := putCondition(c.item, "100", "us", c.ver)
			if cond != c.cond {
				t.Fatalf("cond = %s", cond)
			}
			if names["#ts"] != "_ts" || names["#o"] != "_origin" || names["#pk"] != "_pk" {
				t.Fatalf("names = %v", names)
			}
			if c.ver != "" && names["#ver"] != c.ver {
				t.Fatalf("#ver = %q", names["#ver"])
			}
			if c.ver == "" && (names["#ver"] != "" || values[":null"] != nil) {
				t.Fatalf("unexpected version placeholders: %v %v", names, values)
			}
			got, ok := values[":ver"].(*ddbtypes.AttributeValueMemberN)
			if (c.verValue == "") != !ok || (ok && got.Value != c.verValue) {
				t.Fatalf(":ver = %#v", values[":ver"])
			}
			// Every placeholder used is defined, and every defined one is used
			// (DynamoDB rejects unused expression values).
			for k := range values {
				if !strings.Contains(cond, k) {
					t.Errorf("unused value %s", k)
				}
			}
			for k := range names {
				if !strings.Contains(cond, k) {
					t.Errorf("unused name %s", k)
				}
			}
		})
	}
}

// Every pipe's stream reader polls DescribeStream (~10 TPS per account), so
// its interval is floored however fast table discovery runs.
func TestStreamDiscoveryIntervalFloor(t *testing.T) {
	c := &awsx.Clients{}
	for _, tc := range []struct{ in, want time.Duration }{
		{0, MinStreamDiscoveryInterval},
		{time.Second, MinStreamDiscoveryInterval},
		{30 * time.Second, 30 * time.Second},
	} {
		r, err := New(Config{Regions: []Region{{"a", c}, {"b", c}}, Owner: "o",
			DiscoveryInterval: time.Second, StreamDiscoveryInterval: tc.in})
		if err != nil {
			t.Fatal(err)
		}
		if r.cfg.StreamDiscoveryInterval != tc.want {
			t.Errorf("StreamDiscoveryInterval(%v) = %v, want %v", tc.in, r.cfg.StreamDiscoveryInterval, tc.want)
		}
		if r.cfg.DiscoveryInterval != time.Second {
			t.Errorf("table discovery interval changed to %v", r.cfg.DiscoveryInterval)
		}
	}
}
