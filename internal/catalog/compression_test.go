package catalog

import (
	"reflect"
	"testing"

	"dynamodb-eventstream/internal/chtype"
)

func TestNormalizeCodec(t *testing.T) {
	ty := func(s string) chtype.Type {
		x, err := chtype.Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		return x
	}
	// Expectations from ClickHouse 25.8 system.columns.compression_codec.
	cases := []struct{ codec, typ, want string }{
		{"Delta, ZSTD(1)", "UInt64", "Delta(8), ZSTD(1)"},
		{"ZSTD(3)", "UInt32", "ZSTD(3)"},
		{"CODEC(Delta, ZSTD(1))", "UInt64", "Delta(8), ZSTD(1)"},
		{"Delta", "UInt32", "Delta(4)"},
		{"Delta, LZ4", "UInt8", "Delta(1), LZ4"},
		{"Gorilla", "Float32", "Gorilla(4)"},
		{"Gorilla", "Float64", "Gorilla(8)"},
		{"Delta,ZSTD", "DateTime64(3)", "Delta(8), ZSTD(1)"},
		{"T64", "Int16", "T64"},
		{"ZSTD", "String", "ZSTD(1)"},
		{"LZ4HC", "String", "LZ4HC(0)"},
		{"NONE", "String", "NONE"},
		{"Delta", "Nullable(UInt64)", "Delta(8)"},
		{"Delta", "Date", "Delta(2)"},
		{"FPC", "Float64", "FPC(12)"},
		{"DoubleDelta", "UInt64", "DoubleDelta"},
		{"ZSTD(2)", "LowCardinality(String)", "ZSTD(2)"},
		{"Delta(2)", "UInt64", "Delta(2)"},
		{"Gorilla,  LZ4", "DateTime", "Gorilla(4), LZ4"},
		{"T64, ZSTD", "Int64", "T64, ZSTD(1)"},
		{"Delta, LZ4", "Array(UInt32)", "Delta(4), LZ4"},
		{"Delta", "Bool", "Delta(1)"},
		{"Delta", "Date32", "Delta(4)"},
		{"", "UInt64", ""},
	}
	for _, c := range cases {
		if got := NormalizeCodec(c.codec, ty(c.typ)); got != c.want {
			t.Errorf("NormalizeCodec(%q, %s) = %q, want %q", c.codec, c.typ, got, c.want)
		}
	}
}

func TestShowSettings(t *testing.T) {
	mt := &Table{Engine: EngineMergeTree}
	if got := mt.ShowSettings(); !reflect.DeepEqual(got, []string{"index_granularity = 8192"}) {
		t.Errorf("default MergeTree settings: %q", got)
	}
	mem := &Table{Engine: EngineMemory}
	if got := mem.ShowSettings(); len(got) != 0 {
		t.Errorf("Memory settings: %q", got)
	}
	decl := &Table{
		Engine:      EngineReplacingMergeTree,
		Settings:    map[string]string{"min_bytes_for_wide_part": "0", "storage_policy": "default"},
		SettingsSQL: []string{SettingSQL("min_bytes_for_wide_part", "0", true), SettingSQL("storage_policy", "default", false)},
	}
	want := []string{"min_bytes_for_wide_part = 0", "storage_policy = 'default'", "index_granularity = 8192"}
	if got := decl.ShowSettings(); !reflect.DeepEqual(got, want) {
		t.Errorf("declared settings: %q", got)
	}
	explicit := &Table{Engine: EngineMergeTree, Settings: map[string]string{"index_granularity": "1024"},
		SettingsSQL: []string{"index_granularity = 1024"}}
	if got := explicit.ShowSettings(); !reflect.DeepEqual(got, []string{"index_granularity = 1024"}) {
		t.Errorf("explicit index_granularity: %q", got)
	}
	legacy := &Table{Engine: EngineMergeTree, Settings: map[string]string{"index_granularity": "1024", "dynamodb_partition_key": "a, b"}}
	if got := legacy.ShowSettings(); !reflect.DeepEqual(got, []string{"dynamodb_partition_key = 'a, b'", "index_granularity = 1024"}) {
		t.Errorf("legacy settings: %q", got)
	}
}

func TestCodecPersistsShowCreateFields(t *testing.T) {
	tbl := eventsTable("default", "t")
	tbl.Columns[0].Codec = "ZSTD(3)"
	tbl.PartitionBy = "toYYYYMM(ts)"
	tbl.PrimaryKey = []string{"tenant"}
	tbl.Settings = map[string]string{"index_granularity": "1024"}
	tbl.SettingsSQL = []string{"index_granularity = 1024"}
	b, err := EncodeTable(tbl)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeTable(b, testParse)
	if err != nil {
		t.Fatal(err)
	}
	if got.Columns[0].Codec != "ZSTD(3)" || got.PartitionBy != "toYYYYMM(ts)" ||
		!reflect.DeepEqual(got.PrimaryKey, []string{"tenant"}) || !reflect.DeepEqual(got.SettingsSQL, []string{"index_granularity = 1024"}) {
		t.Fatalf("show-create fields lost in round trip: %+v", got)
	}
	c := cloneTable(tbl)
	c.PrimaryKey[0], c.SettingsSQL[0] = "x", "y"
	if tbl.PrimaryKey[0] != "tenant" || tbl.SettingsSQL[0] != "index_granularity = 1024" {
		t.Fatal("cloneTable must deep-copy PrimaryKey and SettingsSQL")
	}
}

func TestSetSetting(t *testing.T) {
	tbl := &Table{Engine: EngineMergeTree}
	tbl.SetSetting("min_bytes_for_wide_part", "0", true)
	tbl.SetSetting("storage_policy", "default", false)
	shared := tbl.SettingsSQL
	cp := *tbl
	cp.SetSetting("min_bytes_for_wide_part", "10", true)
	if !reflect.DeepEqual(cp.SettingsSQL, []string{"min_bytes_for_wide_part = 10", "storage_policy = 'default'"}) || cp.Settings["min_bytes_for_wide_part"] != "10" {
		t.Fatalf("replace: %q %v", cp.SettingsSQL, cp.Settings)
	}
	if shared[0] != "min_bytes_for_wide_part = 0" || tbl.Settings["min_bytes_for_wide_part"] != "0" {
		t.Fatal("SetSetting must not modify the original slice/map")
	}
	cp.SetSetting("index_granularity", "256", true)
	if got := cp.ShowSettings(); !reflect.DeepEqual(got, []string{"min_bytes_for_wide_part = 10", "storage_policy = 'default'", "index_granularity = 256"}) {
		t.Fatalf("append: %q", got)
	}
	legacy := &Table{Engine: EngineMergeTree, Settings: map[string]string{"b": "x", "a": "1"}}
	legacy.SetSetting("b", "y", false)
	if !reflect.DeepEqual(legacy.SettingsSQL, []string{"a = 1", "b = 'y'"}) {
		t.Fatalf("legacy: %q", legacy.SettingsSQL)
	}
	legacy = &Table{Engine: EngineMergeTree, Settings: map[string]string{"a": "1"}}
	legacy.SetSetting("c", "z", false)
	if !reflect.DeepEqual(legacy.SettingsSQL, []string{"a = 1", "c = 'z'"}) {
		t.Fatalf("legacy append: %q", legacy.SettingsSQL)
	}
}
