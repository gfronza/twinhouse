package config

import (
	"errors"
	"testing"
	"time"
)

func stubHostname(t *testing.T, name string, err error) {
	t.Helper()
	old := hostname
	hostname = func() (string, error) { return name, err }
	t.Cleanup(func() { hostname = old })
}

func TestLoadDefaults(t *testing.T) {
	stubHostname(t, "box", nil)
	for _, k := range []string{"RUN_STREAM_WORKER", "DISPLAY_NAME", "CATALOG_REFRESH", "STREAM_VIEWS"} {
		t.Setenv(k, "") // empty means default for these
	}
	t.Setenv("REGION", "local")
	t.Setenv("GLOBAL_MODE", GlobalNone)
	t.Setenv("LISTEN_ADDR", ":8123")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.RunStreamWorker {
		t.Error("RunStreamWorker defaults to false")
	}
	if !c.StreamViews {
		t.Error("StreamViews defaults to true")
	}
	if c.DisplayName != "box" {
		t.Errorf("DisplayName %q, want box (no suffix for REGION=local)", c.DisplayName)
	}
	if c.CatalogRefresh != 2*time.Second || c.GlobalMode != GlobalNone || c.ListenAddr == "" {
		t.Errorf("defaults: %+v", c)
	}
}

func TestLoadServerSettings(t *testing.T) {
	stubHostname(t, "box", nil)
	t.Setenv("REGION", "eu-west-1")
	t.Setenv("RUN_STREAM_WORKER", "true")
	t.Setenv("DISPLAY_NAME", "")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.RunStreamWorker {
		t.Error("RUN_STREAM_WORKER=true not honoured")
	}
	t.Setenv("STREAM_VIEWS", "false")
	if c, err = Load(); err != nil || c.StreamViews {
		t.Errorf("STREAM_VIEWS=false not honoured: %v %v", c.StreamViews, err)
	}
	t.Setenv("STREAM_VIEWS", "")
	if c.DisplayName != "box-eu-west-1" {
		t.Errorf("DisplayName %q, want box-eu-west-1", c.DisplayName)
	}

	t.Setenv("DISPLAY_NAME", "node-a")
	if c, err = Load(); err != nil || c.DisplayName != "node-a" {
		t.Errorf("DISPLAY_NAME override: %q %v", c.DisplayName, err)
	}

	stubHostname(t, "", errors.New("no hostname"))
	t.Setenv("DISPLAY_NAME", "")
	if c, err = Load(); err != nil || c.DisplayName != "chserver-eu-west-1" {
		t.Errorf("hostname fallback: %q %v", c.DisplayName, err)
	}

	t.Setenv("RUN_STREAM_WORKER", "maybe")
	if _, err := Load(); err == nil {
		t.Error("invalid RUN_STREAM_WORKER accepted")
	}
}

func TestRegionFallsBackToAWSRegion(t *testing.T) {
	stubHostname(t, "box", nil)
	t.Setenv("DISPLAY_NAME", "")
	t.Setenv("REGION", "")
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_DEFAULT_REGION", "us-west-2")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Region != "eu-west-1" || c.DisplayName != "box-eu-west-1" {
		t.Errorf("AWS_REGION fallback: region %q display %q", c.Region, c.DisplayName)
	}
	t.Setenv("AWS_REGION", "")
	if c, _ = Load(); c.Region != "us-west-2" {
		t.Errorf("AWS_DEFAULT_REGION fallback: %q", c.Region)
	}
	t.Setenv("AWS_DEFAULT_REGION", "")
	if c, _ = Load(); c.Region != "local" {
		t.Errorf("default: %q", c.Region)
	}
	t.Setenv("REGION", "ap-south-1")
	t.Setenv("AWS_REGION", "eu-west-1")
	if c, _ = Load(); c.Region != "ap-south-1" {
		t.Errorf("REGION must win over AWS_REGION: %q", c.Region)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		c    Config
		ok   bool
	}{
		{"local endpoint", Config{Region: "local", DynamoDBEndpoint: "http://localhost:8000", GlobalMode: GlobalNone}, true},
		{"aws region", Config{Region: "eu-west-1", GlobalMode: GlobalNone}, true},
		{"no region on aws", Config{Region: "local", GlobalMode: GlobalNone}, false},
		{"empty region on aws", Config{GlobalMode: GlobalNone}, false},
		{"aws mode", Config{Region: "eu-west-1", GlobalMode: GlobalAWS, ReplicaRegions: []string{"us-east-1"}}, true},
		{"aws mode without replicas", Config{Region: "eu-west-1", GlobalMode: GlobalAWS}, false},
		{"aws mode with endpoint", Config{Region: "eu-west-1", DynamoDBEndpoint: "http://ddb:8000", GlobalMode: GlobalAWS, ReplicaRegions: []string{"us-east-1"}}, false},
		{"emulated", Config{Region: "eu-west-1", DynamoDBEndpoint: "http://ddb:8000", GlobalMode: GlobalEmulated}, true},
		{"home region is a replica", Config{Region: "eu-west-1", GlobalMode: GlobalAWS, ReplicaRegions: []string{"us-east-1"}, HomeRegion: "us-east-1"}, true},
		{"home region is this region", Config{Region: "eu-west-1", GlobalMode: GlobalAWS, ReplicaRegions: []string{"us-east-1"}, HomeRegion: "eu-west-1"}, true},
		{"unknown home region", Config{Region: "eu-west-1", GlobalMode: GlobalAWS, ReplicaRegions: []string{"us-east-1"}, HomeRegion: "ap-south-1"}, false},
	}
	for _, tc := range cases {
		if err := tc.c.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: err %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
	// REPLICA_REGIONS that only names REGION itself leaves no replica.
	t.Setenv("REGION", "eu-west-1")
	t.Setenv("DYNAMODB_ENDPOINT", "")
	t.Setenv("GLOBAL_MODE", GlobalAWS)
	t.Setenv("REPLICA_REGIONS", "eu-west-1")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Validate() == nil {
		t.Error("aws mode with only the own region accepted")
	}
}

func TestMaxExecutionTimeEnv(t *testing.T) {
	t.Setenv("REGION", "local")
	for v, want := range map[string]time.Duration{"": 0, "30": 30 * time.Second, "1.5": 1500 * time.Millisecond, "250ms": 250 * time.Millisecond} {
		t.Setenv("MAX_EXECUTION_TIME", v)
		c, err := Load()
		if err != nil || c.MaxExecutionTime != want {
			t.Errorf("%q: %v %v", v, c.MaxExecutionTime, err)
		}
	}
	for _, v := range []string{"x", "-1", "-2s"} {
		t.Setenv("MAX_EXECUTION_TIME", v)
		if _, err := Load(); err == nil {
			t.Errorf("%q accepted", v)
		}
	}
}

func TestEventSourceMappingSettings(t *testing.T) {
	t.Setenv("REGION", "eu-west-1")
	for _, k := range []string{"MVWORKER_FUNCTION_ARN", "REPLICATOR_FUNCTION_ARN", "RECONCILER_FUNCTION_ARN", "AWS_LAMBDA_FUNCTION_NAME", "ESM_BATCH_SIZE"} {
		t.Setenv(k, "")
	}
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.ManagesEventSourceMappings() || c.MVWorkerFunctionARN != "" || c.ReplicatorFunctionARN != "" || c.ReconcilerFunctionARN != "" {
		t.Errorf("defaults: %+v", c)
	}
	if c.ESMBatchSize != DefaultESMBatchSize {
		t.Errorf("ESMBatchSize %d, want %d", c.ESMBatchSize, DefaultESMBatchSize)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("defaults must validate: %v", err)
	}

	t.Setenv("MVWORKER_FUNCTION_ARN", " arn:aws:lambda:eu-west-1:123456789012:function:mvworker ")
	t.Setenv("REPLICATOR_FUNCTION_ARN", "replicator")
	t.Setenv("AWS_LAMBDA_FUNCTION_NAME", "chddb-reconciler")
	t.Setenv("ESM_BATCH_SIZE", "250")
	if c, err = Load(); err != nil {
		t.Fatal(err)
	}
	if !c.ManagesEventSourceMappings() || c.MVWorkerFunctionARN != "arn:aws:lambda:eu-west-1:123456789012:function:mvworker" ||
		c.ReplicatorFunctionARN != "replicator" || c.ESMBatchSize != 250 {
		t.Errorf("settings not honoured: %+v", c)
	}
	if c.ReconcilerFunctionARN != "chddb-reconciler" {
		t.Errorf("RECONCILER_FUNCTION_ARN must default to AWS_LAMBDA_FUNCTION_NAME, got %q", c.ReconcilerFunctionARN)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("aws settings must validate: %v", err)
	}
	t.Setenv("RECONCILER_FUNCTION_ARN", "explicit")
	if c, _ = Load(); c.ReconcilerFunctionARN != "explicit" {
		t.Errorf("RECONCILER_FUNCTION_ARN must win over AWS_LAMBDA_FUNCTION_NAME, got %q", c.ReconcilerFunctionARN)
	}

	for _, v := range []string{"0", "-1", "10001", "many"} {
		t.Setenv("ESM_BATCH_SIZE", v)
		if _, err := Load(); err == nil {
			t.Errorf("ESM_BATCH_SIZE=%q accepted", v)
		}
	}
	t.Setenv("ESM_BATCH_SIZE", "")

	// ESM management is for real AWS; the replicator target needs the worker target.
	cases := []struct {
		name string
		c    Config
		ok   bool
	}{
		{"esm with local endpoint", Config{Region: "local", DynamoDBEndpoint: "http://ddb:8000", GlobalMode: GlobalNone, MVWorkerFunctionARN: "mvworker"}, false},
		{"esm on aws", Config{Region: "eu-west-1", GlobalMode: GlobalNone, MVWorkerFunctionARN: "mvworker"}, true},
		{"replicator target without worker target", Config{Region: "eu-west-1", GlobalMode: GlobalNone, ReplicatorFunctionARN: "replicator"}, false},
		{"reconciler target alone (Lambda default)", Config{Region: "eu-west-1", GlobalMode: GlobalNone, ReconcilerFunctionARN: "self"}, true},
		{"batch size too large", Config{Region: "eu-west-1", GlobalMode: GlobalNone, ESMBatchSize: MaxESMBatchSize + 1}, false},
		{"batch size default (zero)", Config{Region: "eu-west-1", GlobalMode: GlobalNone}, true},
	}
	for _, tc := range cases {
		if err := tc.c.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: err %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}
