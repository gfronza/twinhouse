//go:build ecslocal

package deployawsecs_test

// TestECSLocal runs the container deployment without AWS: it renders the
// task definitions of template.yaml with concrete parameter values, starts
// each container definition with exactly its Environment (plus what ECS
// itself or the emulation adds, listed in ecsInjected), image, command,
// read-only root filesystem, stop timeout, CPU/memory and health check, on
// the network of the local Docker stack against its DynamoDB Local, and
// then:
//
//  1. runs tests/e2e against the emulated chserver task (as the load
//     balancer would reach it, on a host port);
//  2. re-renders chserver with InlineViews=false and checks that the
//     mvworker container applies a materialized view by itself;
//  3. starts the replicator container (rendered for GLOBAL_MODE=aws, with
//     its REGIONS endpoints pointed at the two DynamoDB Local instances)
//     and checks it starts in ReplacingMergeTree-only mode.
//
// The containers are named ecsemu-* and removed at the end, as are the
// TABLE_PREFIX tables in both DynamoDB Local instances. Needs Docker and
// the dhdev stack (make up):
//
//	go test -tags ecslocal -count=1 -v -timeout 30m ./deploy/aws-ecs/
//	make ecs-local
//
// Settings (environment): ECSLOCAL_NETWORK (dhdev_default),
// ECSLOCAL_DDB_US / ECSLOCAL_DDB_EU (http://dynamodb-us:8000 /
// http://dynamodb-eu:8000, as seen from the network), ECSLOCAL_DDB_US_HOST /
// ECSLOCAL_DDB_EU_HOST (http://localhost:18000 / :18001, for the table
// cleanup), ECSLOCAL_PORT (38123), ECSLOCAL_PREFIX (ecs_), ECSLOCAL_IMAGE
// (built from deploy/Dockerfile as ecsemu-image:local when unset).

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	ecsNetwork = envDefault("ECSLOCAL_NETWORK", "dhdev_default")
	ecsDDBUS   = envDefault("ECSLOCAL_DDB_US", "http://dynamodb-us:8000")
	ecsDDBEU   = envDefault("ECSLOCAL_DDB_EU", "http://dynamodb-eu:8000")
	ecsHostUS  = envDefault("ECSLOCAL_DDB_US_HOST", "http://localhost:18000")
	ecsHostEU  = envDefault("ECSLOCAL_DDB_EU_HOST", "http://localhost:18001")
	ecsPort    = envDefault("ECSLOCAL_PORT", "38123")
	ecsPrefix  = envDefault("ECSLOCAL_PREFIX", "ecs_")
)

// ecsInjected is what the emulation adds to a rendered container: what the
// Fargate agent injects (AWS_REGION, AWS_DEFAULT_REGION), the resolved
// Secrets, and the DynamoDB Local endpoint (on AWS the SDK's default).
func ecsInjected(region string, c container) map[string]string {
	m := map[string]string{"AWS_REGION": region, "AWS_DEFAULT_REGION": region}
	if c.Name != "replicator" { // the replicator's endpoints are in REGIONS
		m["DYNAMODB_ENDPOINT"] = ecsDDBUS
	}
	for name := range c.Secrets {
		if name != "CH_USERS" {
			panic("unexpected secret " + name)
		}
		m[name] = "default:" // the secret's value: user default, no password
	}
	return m
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// runTask starts one rendered container definition as ecsemu-<name>.
func runTask(t *testing.T, s *stack, taskDef, name, image string, extra map[string]string, hostPort string) {
	t.Helper()
	c := s.containers(t, taskDef)[0]
	props := s.eval(t, get(s.tp.resource(t, taskDef), "Properties"))
	cpu, _ := strconv.Atoi(fmt.Sprint(get(props, "Cpu")))
	mem := fmt.Sprint(get(props, "Memory"))
	_ = exec.Command("docker", "rm", "-f", name).Run()
	args := []string{"run", "-d", "--name", name, "--network", ecsNetwork,
		"--cpus", strconv.FormatFloat(float64(cpu)/1024, 'f', 2, 64), "--memory", mem + "m",
		"--stop-timeout", c.StopTimeout}
	if c.ReadOnly {
		args = append(args, "--read-only")
	}
	if len(c.Health) == 2 && c.Health[0] == "CMD-SHELL" {
		args = append(args, "--health-cmd", c.Health[1], "--health-interval", "2s", "--health-retries", "30")
	}
	if hostPort != "" {
		args = append(args, "-p", hostPort+":"+c.Ports[0])
	}
	env := map[string]string{}
	for k, v := range c.Env {
		env[k] = v
	}
	for k, v := range ecsInjected(s.region, c) {
		if _, clash := env[k]; clash {
			t.Fatalf("%s: the template sets %s, which the emulation injects", taskDef, k)
		}
		env[k] = v
	}
	for k, v := range extra {
		env[k] = v
	}
	for k, v := range env {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, image)
	args = append(args, c.Command...)
	docker(t, args...)
	t.Logf("started %s (%s %v): template env %v", name, image, c.Command, c.EnvOrder)
}

func ddbClient(endpoint, region string) *dynamodb.Client {
	return dynamodb.New(dynamodb.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider("local", "local", ""),
	})
}

func prefixedTables(ctx context.Context, c *dynamodb.Client) ([]string, error) {
	var out []string
	in := &dynamodb.ListTablesInput{}
	for {
		page, err := c.ListTables(ctx, in)
		if err != nil {
			return nil, err
		}
		for _, n := range page.TableNames {
			if strings.HasPrefix(n, ecsPrefix) {
				out = append(out, n)
			}
		}
		if page.LastEvaluatedTableName == nil {
			return out, nil
		}
		in.ExclusiveStartTableName = page.LastEvaluatedTableName
	}
}

func query(t *testing.T, q string) (string, error) {
	t.Helper()
	resp, err := http.Post("http://localhost:"+ecsPort+"/?"+url.Values{"query": {q}}.Encode(), "text/plain", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("%s: %d %s", q, resp.StatusCode, b)
	}
	return strings.TrimSpace(string(b)), nil
}

func mustQuery(t *testing.T, q string) string {
	t.Helper()
	out, err := query(t, q)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func waitHealthy(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		state := docker(t, "inspect", "-f", "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}", name)
		if state == "running healthy" {
			if out, err := query(t, "SELECT 1"); err == nil && out == "1" {
				return
			}
		}
		if strings.HasPrefix(state, "exited") {
			t.Fatalf("%s exited:\n%s", name, docker(t, "logs", "--tail", "50", name))
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s not healthy:\n%s", name, docker(t, "logs", "--tail", "50", name))
}

// logLines returns the JSON log lines of a container with msg.
func logLines(t *testing.T, name, msg string) []map[string]any {
	t.Helper()
	out, _ := exec.Command("docker", "logs", name).CombinedOutput()
	var lines []map[string]any
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if json.Unmarshal(sc.Bytes(), &m) == nil && m["msg"] == msg {
			lines = append(lines, m)
		}
	}
	return lines
}

func TestECSLocal(t *testing.T) {
	ctx := context.Background()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed")
	}
	if err := exec.Command("docker", "network", "inspect", ecsNetwork).Run(); err != nil {
		t.Skipf("network %s missing: start the local stack (make up)", ecsNetwork)
	}
	us, eu := ddbClient(ecsHostUS, "us-east-1"), ddbClient(ecsHostEU, "eu-west-1")
	for _, c := range []*dynamodb.Client{us, eu} {
		existing, err := prefixedTables(ctx, c)
		if err != nil {
			t.Fatalf("DynamoDB Local: %v", err)
		}
		if len(existing) > 0 {
			t.Fatalf("tables with prefix %s exist already (%v): choose another ECSLOCAL_PREFIX", ecsPrefix, existing)
		}
	}
	names := []string{"ecsemu-chserver", "ecsemu-mvworker", "ecsemu-replicator"}
	t.Cleanup(func() {
		for _, n := range names {
			_ = exec.Command("docker", "rm", "-f", n).Run()
		}
		for _, c := range []*dynamodb.Client{us, eu} {
			tables, err := prefixedTables(ctx, c)
			if err != nil {
				t.Errorf("cleanup: %v", err)
				continue
			}
			for _, n := range tables {
				if _, err := c.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(n)}); err != nil {
					t.Errorf("cleanup: delete %s: %v", n, err)
				}
			}
			t.Logf("cleanup: deleted %d %s* tables", len(tables), ecsPrefix)
		}
	})

	image := os.Getenv("ECSLOCAL_IMAGE")
	if image == "" {
		image = "ecsemu-image:local"
		out, err := exec.Command("docker", "build", "-q", "-f", "../Dockerfile", "-t", image, "../..").CombinedOutput()
		if err != nil {
			t.Fatalf("docker build: %v\n%s", err, out)
		}
	}

	tp := loadTemplate(t)
	// One region of the container path with GLOBAL_MODE=none (GLOBAL_MODE=aws
	// needs real global tables; the binaries reject DYNAMODB_ENDPOINT with it).
	params := map[string]string{
		"GlobalMode": "none", "TablePrefix": ecsPrefix, "CatalogRefresh": "1s", "WorkerMode": "service", "AllowedCidr": "10.0.0.0/16",
		"ChserverCpu": "1024", "ChserverMemory": "2048", "MvWorkerCpu": "512", "MvWorkerMemory": "1024",
	}
	s := tp.instantiate(t, "us-east-1", withRequired(params))
	if f := s.failedRules(t); len(f) > 0 {
		t.Fatalf("the parameters break the stack rules: %q", f)
	}
	if s.exists(t, "ReplicatorService") || !s.exists(t, "MvWorkerService") {
		t.Fatal("GlobalMode=none, WorkerMode=service: mvworker without replicator expected")
	}
	runTask(t, s, "ChserverTaskDefinition", "ecsemu-chserver", image, nil, ecsPort)
	runTask(t, s, "MvWorkerTaskDefinition", "ecsemu-mvworker", image, nil, "")
	waitHealthy(t, "ecsemu-chserver")

	t.Run("e2e", func(t *testing.T) {
		cmd := exec.Command("go", "test", "-tags", "e2e", "-count=1", "-timeout", "20m", "./tests/e2e/...")
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "CHDDB_URL=http://localhost:"+ecsPort, "TABLE_PREFIX="+ecsPrefix)
		out, err := cmd.CombinedOutput()
		t.Logf("tests/e2e against ecsemu-chserver:\n%s", out)
		if err != nil {
			t.Fatalf("tests/e2e: %v", err)
		}
	})

	t.Run("mvworker applies views", func(t *testing.T) {
		// Without inline views only the mvworker container can fill the
		// rollup.
		s2 := tp.instantiate(t, "us-east-1", withRequired(merge(params, map[string]string{"InlineViews": "false"})))
		if s2.containers(t, "ChserverTaskDefinition")[0].Env["INLINE_VIEWS"] != "false" {
			t.Fatal("InlineViews=false not rendered")
		}
		runTask(t, s2, "ChserverTaskDefinition", "ecsemu-chserver", image, nil, ecsPort)
		waitHealthy(t, "ecsemu-chserver")
		before := 0.0
		if l := logLines(t, "ecsemu-mvworker", "mvworker progress"); len(l) > 0 {
			before, _ = l[len(l)-1]["applied"].(float64)
		}
		mustQuery(t, "CREATE DATABASE IF NOT EXISTS ecsemu")
		mustQuery(t, "CREATE TABLE ecsemu.events (site String, n UInt64) ENGINE = MergeTree ORDER BY site")
		mustQuery(t, "CREATE TABLE ecsemu.totals (site String, n UInt64) ENGINE = SummingMergeTree ORDER BY site")
		mustQuery(t, "CREATE MATERIALIZED VIEW ecsemu.mv TO ecsemu.totals AS SELECT site, n FROM ecsemu.events")
		mustQuery(t, "INSERT INTO ecsemu.events VALUES ('a', 1), ('a', 2), ('b', 5)")
		deadline := time.Now().Add(2 * time.Minute)
		var got string
		for time.Now().Before(deadline) {
			got, _ = query(t, "SELECT site, sum(n) FROM ecsemu.totals GROUP BY site ORDER BY site")
			if got == "a\t3\nb\t5" {
				break
			}
			time.Sleep(time.Second)
		}
		if got != "a\t3\nb\t5" {
			t.Fatalf("rollup = %q, want a 3 / b 5 (applied by the mvworker container)\n%s", got, docker(t, "logs", "--tail", "40", "ecsemu-mvworker"))
		}
		// The worker's own progress log (every 30 s, STATS_INTERVAL's
		// default) shows the records it applied.
		var after float64
		for logWait := time.Now().Add(90 * time.Second); time.Now().Before(logWait); {
			if l := logLines(t, "ecsemu-mvworker", "mvworker progress"); len(l) > 0 {
				after, _ = l[len(l)-1]["applied"].(float64)
				if after >= before+3 {
					break
				}
			}
			time.Sleep(time.Second)
		}
		if after < before+3 {
			t.Errorf("mvworker progress applied %v -> %v, want +3", before, after)
		}
		t.Logf("rollup filled by mvworker; its progress log: applied %v -> %v", before, after)
		mustQuery(t, "DROP DATABASE ecsemu")
	})

	t.Run("replicator starts", func(t *testing.T) {
		rs := tp.instantiate(t, "us-east-1", withRequired(map[string]string{"GlobalMode": "aws", "RunReplicator": "true", "TablePrefix": ecsPrefix, "AllowedCidr": "10.0.0.0/16"}))
		if f := rs.failedRules(t); len(f) > 0 {
			t.Fatalf("the replicator parameters break the stack rules: %q", f)
		}
		c := rs.containers(t, "ReplicatorTaskDefinition")[0]
		if c.Env["REGIONS"] != "us-east-1=,eu-west-1=" {
			t.Fatalf("rendered REGIONS = %q", c.Env["REGIONS"])
		}
		// The only change to the rendered environment: the empty (AWS)
		// endpoints of REGIONS become the DynamoDB Local containers.
		local := "us-east-1=" + ecsDDBUS + ",eu-west-1=" + ecsDDBEU
		runTask(t, rs, "ReplicatorTaskDefinition", "ecsemu-replicator", image, map[string]string{"REGIONS": local}, "")
		deadline := time.Now().Add(time.Minute)
		for time.Now().Before(deadline) {
			if l := logLines(t, "ecsemu-replicator", "replicator started"); len(l) > 0 {
				if l[0]["versioned_only"] != true || l[0]["regions"] != local || l[0]["prefix"] != ecsPrefix {
					t.Errorf("replicator started with %v", l[0])
				}
				time.Sleep(5 * time.Second)
				if st := docker(t, "inspect", "-f", "{{.State.Status}}", "ecsemu-replicator"); st != "running" {
					t.Errorf("replicator %s:\n%s", st, docker(t, "logs", "--tail", "30", "ecsemu-replicator"))
				}
				return
			}
			time.Sleep(time.Second)
		}
		t.Fatalf("replicator did not start:\n%s", docker(t, "logs", "--tail", "30", "ecsemu-replicator"))
	})

	t.Run("graceful stop", func(t *testing.T) {
		for _, n := range names {
			start := time.Now()
			docker(t, "stop", n)
			code := docker(t, "inspect", "-f", "{{.State.ExitCode}}", n)
			t.Logf("%s stopped in %v, exit code %s", n, time.Since(start).Round(time.Millisecond), code)
			if code != "0" {
				t.Errorf("%s exited with %s after SIGTERM:\n%s", n, code, docker(t, "logs", "--tail", "20", n))
			}
		}
	})
}

func merge(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func withRequired(m map[string]string) map[string]string { return merge(required, m) }
