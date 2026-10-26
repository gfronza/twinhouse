package main

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/config"
	"github.com/gfronza/twinhouse/internal/replicator"
)

func TestParseRegions(t *testing.T) {
	cases := []struct {
		in   string
		want []regionSpec
		ok   bool
	}{
		{"us-east-1=http://a:8000,eu-west-1=http://b:8000", []regionSpec{{"us-east-1", "http://a:8000"}, {"eu-west-1", "http://b:8000"}}, true},
		{" us = http://a , eu= ,", []regionSpec{{"us", "http://a"}, {"eu", ""}}, true},
		{"us=http://a", nil, false},
		{"", nil, false},
		{"us=http://a,us=http://b", nil, false},
		{"us,eu=http://b", nil, false},
		{"=http://a,eu=http://b", nil, false},
	}
	for _, c := range cases {
		got, err := parseRegions(c.in)
		if (err == nil) != c.ok || !reflect.DeepEqual(got, c.want) {
			t.Errorf("parseRegions(%q) = %v, %v", c.in, got, err)
		}
	}
}

func TestGlobalTablesCarryVersionColumns(t *testing.T) {
	ctx := context.Background()
	mk := func(cat catalog.Catalog, name, engine, ver, repl string) {
		t.Helper()
		col := func(n string) catalog.Column { return catalog.Column{Name: n, Type: chtype.MustParse("UInt64")} }
		tb := &catalog.Table{Database: "db", Name: name, Engine: engine, Columns: []catalog.Column{col("k"), col("v")},
			OrderBy: []string{"k"}, PartitionKey: []string{"k"}, VersionColumn: ver, Replication: repl}
		if err := cat.CreateTable(ctx, tb, false); err != nil {
			t.Fatal(err)
		}
	}
	a, b := catalog.NewMemory(), catalog.NewMemory()
	for _, c := range []catalog.Catalog{a, b} {
		if err := c.CreateDatabase(ctx, &catalog.Database{Name: "db"}, true); err != nil {
			t.Fatal(err)
		}
	}
	mk(a, "events", catalog.EngineMergeTree, "", catalog.ReplicationGlobal)
	mk(a, "state", catalog.EngineReplacingMergeTree, "v", catalog.ReplicationGlobal)
	mk(a, "rollup", catalog.EngineAggregatingMergeTree, "", catalog.ReplicationRegional)
	mk(b, "plain", catalog.EngineReplacingMergeTree, "", catalog.ReplicationGlobal)
	mk(b, "state", catalog.EngineReplacingMergeTree, "v", catalog.ReplicationGlobal) // catalog replicated to b too

	got, err := globalTables([]regionCatalog{{"a", a}, {"b", b}}, false)(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// (the memory catalog assigns physical names "db.table")
	want := []replicator.TableSpec{{Name: "db.events"}, {Name: "db.plain"}, {Name: "db.state", VersionColumn: "v"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v want %+v", got, want)
	}
	// GLOBAL_MODE=aws: only ReplacingMergeTree tables (with or without a
	// version; the others are AWS global tables).
	got, err = globalTables([]regionCatalog{{"a", a}, {"b", b}}, true)(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := []replicator.TableSpec{{Name: "db.plain"}, {Name: "db.state", VersionColumn: "v"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("versioned only: got %+v want %+v", got, want)
	}
}

func TestEnvBool(t *testing.T) {
	for in, want := range map[string]bool{"": true, "true": true, "1": true, "FALSE": false, "off": false} {
		t.Setenv("X_BOOL", in)
		if got, err := envBool("X_BOOL", true); err != nil || got != want {
			t.Errorf("envBool(%q) = %v, %v", in, got, err)
		}
	}
	t.Setenv("X_BOOL", "maybe")
	if _, err := envBool("X_BOOL", true); err == nil {
		t.Error("invalid boolean accepted")
	}
}

func TestValidateLambda(t *testing.T) {
	aws := config.Config{Region: "us-east-1", GlobalMode: config.GlobalAWS, ReplicaRegions: []string{"eu-west-1"}}
	if err := validateLambda(aws); err != nil {
		t.Fatalf("valid aws config rejected: %v", err)
	}
	cases := map[string]func(c *config.Config){
		"emulated":       func(c *config.Config) { c.GlobalMode = config.GlobalEmulated },
		"none":           func(c *config.Config) { c.GlobalMode = config.GlobalNone },
		"local endpoint": func(c *config.Config) { c.DynamoDBEndpoint = "http://localhost:8000" },
		"no replicas":    func(c *config.Config) { c.ReplicaRegions = nil },
		"region local":   func(c *config.Config) { c.Region = "local" },
		"foreign home":   func(c *config.Config) { c.HomeRegion = "ap-south-1" },
	}
	for name, mutate := range cases {
		c := aws
		mutate(&c)
		err := validateLambda(c)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if (name == "emulated" || name == "none") && !strings.Contains(err.Error(), "GLOBAL_MODE=aws") {
			t.Errorf("%s: unexpected error %v", name, err)
		}
	}
}
