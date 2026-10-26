package esm

import (
	"context"
	"slices"
	"testing"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/config"
)

func table(engine, repl, ver string) *catalog.Table {
	return &catalog.Table{Database: "default", Name: "t", PhysicalName: "chddb_default.t", Engine: engine, Replication: repl, VersionColumn: ver}
}

func TestTargets(t *testing.T) {
	cases := []struct {
		name  string
		mode  string
		repl  string // replicator ARN
		t     *catalog.Table
		want  []string
		warns bool
	}{
		{"MergeTree, aws", config.GlobalAWS, replicatr, table(catalog.EngineMergeTree, catalog.ReplicationGlobal, ""), []string{mvworker}, false},
		{"Memory", config.GlobalAWS, replicatr, table(catalog.EngineMemory, "", ""), []string{mvworker}, false},
		{"Log", config.GlobalNone, "", table(catalog.EngineLog, "", ""), []string{mvworker}, false},
		{"regional Summing rollup", config.GlobalAWS, replicatr, table(catalog.EngineSummingMergeTree, catalog.ReplicationRegional, ""), nil, false},
		{"global Aggregating (not a view source)", config.GlobalAWS, replicatr, table(catalog.EngineAggregatingMergeTree, catalog.ReplicationGlobal, ""), nil, false},
		{"Replacing(ver), aws", config.GlobalAWS, replicatr, table(catalog.EngineReplacingMergeTree, catalog.ReplicationGlobal, "v"), []string{mvworker, replicatr}, false},
		{"Replacing without version, aws", config.GlobalAWS, replicatr, table(catalog.EngineReplacingMergeTree, "", ""), []string{mvworker, replicatr}, false},
		{"regional Replacing, aws", config.GlobalAWS, replicatr, table(catalog.EngineReplacingMergeTree, catalog.ReplicationRegional, "v"), []string{mvworker}, false},
		{"Replacing, emulated", config.GlobalEmulated, replicatr, table(catalog.EngineReplacingMergeTree, catalog.ReplicationGlobal, "v"), []string{mvworker}, false},
		{"Replacing, none", config.GlobalNone, replicatr, table(catalog.EngineReplacingMergeTree, catalog.ReplicationGlobal, "v"), []string{mvworker}, false},
		{"Replacing, aws, no replicator function", config.GlobalAWS, "", table(catalog.EngineReplacingMergeTree, catalog.ReplicationGlobal, "v"), []string{mvworker}, true},
		{"nil", config.GlobalAWS, replicatr, nil, nil, false},
	}
	for _, tc := range cases {
		m := newManager(t, newFake())
		c, err := NewConsumers(ConsumersConfig{Manager: m, MVWorkerARN: mvworker, ReplicatorARN: tc.repl, GlobalMode: tc.mode, Logger: quiet()})
		if err != nil {
			t.Fatal(err)
		}
		if got := c.Targets(tc.t); !slices.Equal(got, tc.want) {
			t.Errorf("%s: targets %v, want %v", tc.name, got, tc.want)
		}
		c.Targets(tc.t)
		if warned := len(c.warned) > 0; warned != tc.warns {
			t.Errorf("%s: warned %v, want %v", tc.name, warned, tc.warns)
		}
		if tc.warns && len(c.warned) != 1 {
			t.Errorf("%s: warning not deduplicated: %v", tc.name, c.warned)
		}
	}
}

func TestEnsureAndRemoveConsumers(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	m := newManager(t, f)
	c, err := NewConsumers(ConsumersConfig{Manager: m, MVWorkerARN: mvworker, ReplicatorARN: replicatr, GlobalMode: config.GlobalAWS,
		Options: Options{BatchSize: 50}, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	// No stream: nothing to do.
	if err := c.EnsureConsumers(ctx, table(catalog.EngineMergeTree, "", ""), ""); err != nil || f.count("List") != 0 {
		t.Fatalf("no stream: %v, %d calls", err, f.count("List"))
	}
	rt := table(catalog.EngineReplacingMergeTree, catalog.ReplicationGlobal, "v")
	rt.PhysicalName = TableOfStreamARN(streamA)
	if err := c.EnsureConsumers(ctx, rt, streamA); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, g := range f.snapshot() {
		got = append(got, *g.FunctionArn)
		if *g.BatchSize != 50 {
			t.Errorf("batch size %d", *g.BatchSize)
		}
	}
	if !slices.Equal(got, []string{mvworker, replicatr}) {
		t.Fatalf("mappings %v", got)
	}
	// A rollup gets none.
	if err := c.EnsureConsumers(ctx, table(catalog.EngineSummingMergeTree, catalog.ReplicationRegional, ""), streamB); err != nil || len(f.snapshot()) != 2 {
		t.Errorf("rollup: %v, %d mappings", err, len(f.snapshot()))
	}
	// Removal by stream ARN removes both functions' mappings.
	f.add(streamB, mvworker, "Enabled", 50)
	if err := c.RemoveConsumers(ctx, rt.PhysicalName, streamA); err != nil {
		t.Fatal(err)
	}
	if ms := f.snapshot(); len(ms) != 1 || *ms[0].EventSourceArn != streamB {
		t.Fatalf("after remove: %+v", ms)
	}
	// Removal by table name (stream ARN unknown: the table is gone).
	if err := c.RemoveConsumers(ctx, TableOfStreamARN(streamB), ""); err != nil {
		t.Fatal(err)
	}
	if ms := f.snapshot(); len(ms) != 0 {
		t.Fatalf("after remove by table: %+v", ms)
	}
	// Errors of one target do not hide the other.
	f.failNext("Create", &listErr{})
	if err := c.EnsureConsumers(ctx, rt, streamA); err == nil {
		t.Error("create error swallowed")
	}
	if ms := f.snapshot(); len(ms) != 1 || *ms[0].FunctionArn != replicatr {
		t.Errorf("second target not attempted: %+v", ms)
	}
}

type listErr struct{}

func (listErr) Error() string { return "boom" }

func TestNewConsumersValidation(t *testing.T) {
	m := newManager(t, newFake())
	if _, err := NewConsumers(ConsumersConfig{MVWorkerARN: mvworker}); err == nil {
		t.Error("nil manager accepted")
	}
	if _, err := NewConsumers(ConsumersConfig{Manager: m}); err == nil {
		t.Error("empty worker ARN accepted")
	}
}

// TestConsumedTables lists the tables whose streams the stream worker or
// the replicator consume (the reconciler's garbage collection of mappings
// whose table vanished while no reconciler observed it).
func TestConsumedTables(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	f.pageSize = 1
	m := newManager(t, f)
	c, err := NewConsumers(ConsumersConfig{Manager: m, MVWorkerARN: mvworker, ReplicatorARN: replicatr, GlobalMode: config.GlobalAWS, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.ConsumedTables(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty: %v %v", got, err)
	}
	f.add(streamA, mvworker, "Enabled", 100)
	f.add(streamA, replicatr, "Enabled", 100)
	f.add(streamB, mvworker, "Disabled", 100)
	f.add("arn:aws:kinesis:eu-west-1:123456789012:stream/not-dynamodb", mvworker, "Enabled", 100)
	got, err = c.ConsumedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	if want := []string{TableOfStreamARN(streamA), TableOfStreamARN(streamB)}; !slices.Equal(got, want) {
		t.Fatalf("consumed %v, want %v", got, want)
	}
	// Without a replicator only the worker's mappings are listed.
	c2, _ := NewConsumers(ConsumersConfig{Manager: m, MVWorkerARN: replicatr, GlobalMode: config.GlobalNone, Logger: quiet()})
	if got, err = c2.ConsumedTables(ctx); err != nil || !slices.Equal(got, []string{TableOfStreamARN(streamA)}) {
		t.Fatalf("worker only: %v %v", got, err)
	}
	f.failNext("List", &listErr{})
	if _, err := c.ConsumedTables(ctx); err == nil {
		t.Fatal("list error swallowed")
	}
}
