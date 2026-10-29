package deployawsecs_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// repoRoot is the module root (deploy/aws-ecs is two levels down).
var repoRoot = filepath.Join("..", "..")

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// required are the parameters without a default: account-specific values
// the operator supplies (samconfig.toml placeholders).
var required = map[string]string{
	"ImageUri":         "123456789012.dkr.ecr.us-east-1.amazonaws.com/twinhouse:v1",
	"VpcId":            "vpc-0123456789abcdef0",
	"SubnetIds":        "subnet-0123456789abcdef0,subnet-0123456789abcdef1",
	"ChUsersSecretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:twinhouse-users-AbCdEf",
}

// render instantiates the template in region with the required
// parameters plus overrides.
func render(t *testing.T, tp *template, region string, overrides map[string]string) *stack {
	t.Helper()
	all := map[string]string{}
	for k, v := range required {
		all[k] = v
	}
	for k, v := range overrides {
		all[k] = v
	}
	return tp.instantiate(t, region, all)
}

// taskDefs maps each service to its task definition and container.
var taskDefs = map[string]struct{ service, taskDef, container, condition string }{
	"chserver":   {"ChserverService", "ChserverTaskDefinition", "chserver", ""},
	"mvworker":   {"MvWorkerService", "MvWorkerTaskDefinition", "mvworker", "SeparateWorker"},
	"replicator": {"ReplicatorService", "ReplicatorTaskDefinition", "replicator", "RunsReplicator"},
}

func TestTemplateShape(t *testing.T) {
	tp := loadTemplate(t)
	if _, ok := tp.root["Transform"]; ok {
		t.Error("the container template is plain CloudFormation: no Transform")
	}
	for name, p := range tp.params {
		if get(p, "Description") == nil && !strings.HasSuffix(name, "Cpu") && !strings.HasSuffix(name, "Memory") &&
			name != "LogLevel" && name != "LogRetentionDays" {
			t.Errorf("parameter %s has no Description", name)
		}
		if get(p, "NoEcho") != nil {
			t.Errorf("parameter %s is NoEcho: secrets come from Secrets Manager, not parameters", name)
		}
	}
	for name := range required {
		if get(tp.params[name], "Default") != nil {
			t.Errorf("%s should have no default (account-specific)", name)
		}
	}
	var missing []string
	for name, p := range tp.params {
		if get(p, "Default") == nil {
			if _, ok := required[name]; !ok {
				missing = append(missing, name)
			}
		}
	}
	if len(missing) > 0 {
		t.Errorf("parameters without default not covered by the tests: %v", missing)
	}
	if got := tp.resourcesOfType("AWS::DynamoDB::Table"); len(got) > 0 {
		t.Errorf("the template must not declare DynamoDB tables (created at runtime): %v", got)
	}
	for _, typ := range []string{"AWS::ECS::Service", "AWS::ECS::TaskDefinition"} {
		if got := tp.resourcesOfType(typ); len(got) != 3 {
			t.Errorf("%s resources = %v, want chserver, mvworker, replicator", typ, got)
		}
	}
	// Every default stack renders (every Ref, If and Sub resolves).
	s := render(t, tp, "us-east-1", nil)
	for name := range tp.resources {
		if s.exists(t, name) {
			s.eval(t, tp.resources[name])
		}
	}
}

// TestEnvironmentVariablesAreReadByTheCode fails on a variable a container
// sets (in any branch of a condition) but no binary reads, i.e. a typo or
// a stale name.
func TestEnvironmentVariablesAreReadByTheCode(t *testing.T) {
	tp := loadTemplate(t)
	names := map[string]bool{}
	var walk func(v node)
	walk = func(v node) {
		switch x := v.(type) {
		case map[string]any:
			if n, ok := x["Name"].(string); ok && (x["Value"] != nil || x["ValueFrom"] != nil) {
				names[n] = true
			}
			for _, e := range x {
				walk(e)
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		}
	}
	for _, td := range tp.resourcesOfType("AWS::ECS::TaskDefinition") {
		for _, c := range get(tp.resource(t, td), "Properties", "ContainerDefinitions").([]any) {
			walk(get(c, "Environment"))
			walk(get(c, "Secrets"))
		}
	}
	if len(names) < 15 {
		t.Fatalf("found only %d environment variables, parsing is off: %v", len(names), names)
	}
	var sources []byte
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.Walk(filepath.Join(repoRoot, dir), func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			b, err := os.ReadFile(path)
			sources = append(sources, b...)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for k := range names {
		if !regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`).MatchString(k) {
			t.Errorf("environment variable %q is not UPPER_SNAKE", k)
		}
		if !bytes.Contains(sources, []byte(`"`+k+`"`)) {
			t.Errorf("environment variable %s is set by the deployment but no binary reads it", k)
		}
	}
	// Lambda-only settings do not belong in a container deployment.
	for _, k := range []string{"MVWORKER_FUNCTION_ARN", "REPLICATOR_FUNCTION_ARN", "RECONCILER_FUNCTION_ARN", "ESM_BATCH_SIZE", "DYNAMODB_ENDPOINT", "AWS_LAMBDA_RUNTIME_API"} {
		if names[k] {
			t.Errorf("%s must not be set on ECS (Lambda or DynamoDB Local only)", k)
		}
	}
}

func TestContainers(t *testing.T) {
	tp := loadTemplate(t)
	s := render(t, tp, "us-east-1", map[string]string{"RunReplicator": "true"})
	for svc, d := range taskDefs {
		cs := s.containers(t, d.taskDef)
		if len(cs) != 1 {
			t.Fatalf("%s: %d containers, want 1", d.taskDef, len(cs))
		}
		c := cs[0]
		if c.Name != d.container || strings.Join(c.Command, " ") != svc {
			t.Errorf("%s: container %s command %v, want %s running %s", d.taskDef, c.Name, c.Command, d.container, svc)
		}
		if c.Image != required["ImageUri"] {
			t.Errorf("%s: image %s, want the ImageUri parameter", d.taskDef, c.Image)
		}
		if !c.ReadOnly {
			t.Errorf("%s: ReadonlyRootFilesystem should be true (the binaries write no files)", d.taskDef)
		}
		// The binaries stop on SIGTERM (signal.NotifyContext); chserver
		// drains for up to 15 s. Fargate allows at most 120 s.
		main := readFile(t, filepath.Join(repoRoot, "cmd", svc, "main.go"))
		if !strings.Contains(main, "syscall.SIGTERM") {
			t.Errorf("cmd/%s does not handle SIGTERM", svc)
		}
		if n, err := strconv.Atoi(c.StopTimeout); err != nil || n < 20 || n > 120 {
			t.Errorf("%s: StopTimeout %q, want 20..120 seconds", d.taskDef, c.StopTimeout)
		}
		if svc == "chserver" && !strings.Contains(main, "shutdownTimeout = 15 * time.Second") {
			t.Error("chserver's shutdown timeout changed: re-check StopTimeout")
		}
		props := get(tp.resource(t, d.taskDef), "Properties")
		if get(props, "NetworkMode") != "awsvpc" || fmt.Sprint(get(props, "RequiresCompatibilities")) != "[FARGATE]" {
			t.Errorf("%s: want awsvpc on FARGATE", d.taskDef)
		}
		if fmt.Sprint(s.eval(t, get(props, "RuntimePlatform", "CpuArchitecture"))) != "ARM64" {
			t.Errorf("%s: RuntimePlatform.CpuArchitecture must follow the CpuArchitecture parameter", d.taskDef)
		}
		want := map[string]string{"TABLE_PREFIX": "twinhouse_", "GLOBAL_MODE": "aws", "LOG_LEVEL": "info", "CATALOG_REFRESH": "2s"}
		if svc != "replicator" {
			want["REGION"] = "us-east-1"
			want["REPLICA_REGIONS"] = "us-east-1,eu-west-1"
			want["GLOBAL_HOME_REGION"] = "us-east-1"
		}
		for k, v := range want {
			if c.Env[k] != v {
				t.Errorf("%s: %s = %q, want %q", d.container, k, c.Env[k], v)
			}
		}
		if lg := get(s.eval(t, get(props, "ContainerDefinitions", 0, "LogConfiguration")), "Options", "awslogs-group"); lg != "<"+strings.TrimSuffix(d.taskDef, "TaskDefinition")+"LogGroup>" {
			t.Errorf("%s: logs go to %v, want its own log group", d.taskDef, lg)
		}
	}
	rep := s.containers(t, "ReplicatorTaskDefinition")[0]
	// cmd/replicator parses "name=endpoint,..." (empty endpoint = AWS),
	// at least two regions.
	if rep.Env["REGIONS"] != "us-east-1=,eu-west-1=" {
		t.Errorf("replicator REGIONS = %q, want us-east-1=,eu-west-1=", rep.Env["REGIONS"])
	}
	s3 := render(t, tp, "us-east-1", map[string]string{"ReplicaRegions": "us-east-1,eu-west-1,ap-southeast-2", "RunReplicator": "true"})
	if got := s3.containers(t, "ReplicatorTaskDefinition")[0].Env["REGIONS"]; got != "us-east-1=,eu-west-1=,ap-southeast-2=" {
		t.Errorf("three-region REGIONS = %q", got)
	}
	if rep.Env["GLOBAL_MODE"] != "aws" || rep.Env["APPLY_CONFLICT_VIEWS"] != "true" {
		t.Errorf("replicator must run in GLOBAL_MODE=aws with conflict views: %v", rep.Env)
	}
	ch := s.containers(t, "ChserverTaskDefinition")[0]
	if ch.Env["RUN_STREAM_WORKER"] != "false" || ch.Env["STREAM_VIEWS"] != "true" {
		t.Errorf("chserver with the mvworker service: RUN_STREAM_WORKER=false STREAM_VIEWS=true, got %v", ch.Env)
	}
	inproc := render(t, tp, "us-east-1", map[string]string{"WorkerMode": "in-process"})
	if got := inproc.containers(t, "ChserverTaskDefinition")[0].Env["RUN_STREAM_WORKER"]; got != "true" {
		t.Errorf("WorkerMode=in-process: RUN_STREAM_WORKER = %q", got)
	}
	// GLOBAL_MODE=none: no REPLICA_REGIONS / GLOBAL_HOME_REGION, which
	// config.Validate would check against REGION (a stack in eu-west-1
	// with the default HomeRegion would fail to start).
	none := render(t, tp, "eu-west-1", map[string]string{"GlobalMode": "none"})
	for _, svc := range []string{"chserver", "mvworker"} {
		env := none.containers(t, taskDefs[svc].taskDef)[0].Env
		if _, ok := env["REPLICA_REGIONS"]; ok {
			t.Errorf("%s with GlobalMode=none sets REPLICA_REGIONS", svc)
		}
		if _, ok := env["GLOBAL_HOME_REGION"]; ok {
			t.Errorf("%s with GlobalMode=none sets GLOBAL_HOME_REGION", svc)
		}
		if env["GLOBAL_MODE"] != "none" || env["REGION"] != "eu-west-1" {
			t.Errorf("%s with GlobalMode=none: %v", svc, env)
		}
	}
}

// TestChUsersFromSecretsManager: CH_USERS reaches chserver only through
// ECS Secrets (resolved by the execution role at task start), never as a
// plaintext environment variable or parameter.
func TestChUsersFromSecretsManager(t *testing.T) {
	tp := loadTemplate(t)
	s := render(t, tp, "us-east-1", map[string]string{"RunReplicator": "true"})
	for svc, d := range taskDefs {
		c := s.containers(t, d.taskDef)[0]
		if _, ok := c.Env["CH_USERS"]; ok {
			t.Errorf("%s sets CH_USERS in plaintext", svc)
		}
		if svc == "chserver" {
			if c.Secrets["CH_USERS"] != required["ChUsersSecretArn"] {
				t.Errorf("chserver Secrets CH_USERS = %q, want the ChUsersSecretArn parameter", c.Secrets["CH_USERS"])
			}
		} else if len(c.Secrets) > 0 {
			t.Errorf("%s needs no secrets, has %v", svc, c.Secrets)
		}
	}
	for name := range tp.params {
		if strings.EqualFold(name, "ChUsers") || strings.Contains(strings.ToLower(name), "password") {
			t.Errorf("parameter %s would carry a secret in plaintext", name)
		}
	}
	found := false
	for _, st := range statements(t, s.eval(t, tp.resource(t, "TaskExecutionRole"))) {
		if slices.Contains(st.actions, "secretsmanager:GetSecretValue") {
			found = true
			if len(st.resources) != 1 || st.resources[0] != required["ChUsersSecretArn"]+"*" {
				t.Errorf("GetSecretValue should be scoped to the secret, got %v", st.resources)
			}
		}
	}
	if !found {
		t.Error("the execution role cannot read the CH_USERS secret")
	}
	// A JSON key goes only into ValueFrom (<arn>:<key>::). IAM matches
	// GetSecretValue against the secret's own ARN, so the grant must stay
	// <arn>* : <arn>:<key>::* could never match it.
	keyed := render(t, tp, "us-east-1", map[string]string{"ChUsersSecretJsonKey": "ch_users"})
	if got, want := keyed.containers(t, "ChserverTaskDefinition")[0].Secrets["CH_USERS"], required["ChUsersSecretArn"]+":ch_users::"; got != want {
		t.Errorf("with ChUsersSecretJsonKey: ValueFrom = %q, want %q", got, want)
	}
	for _, st := range statements(t, keyed.eval(t, tp.resource(t, "TaskExecutionRole"))) {
		if slices.Contains(st.actions, "secretsmanager:GetSecretValue") &&
			(len(st.resources) != 1 || st.resources[0] != required["ChUsersSecretArn"]+"*") {
			t.Errorf("with ChUsersSecretJsonKey: GetSecretValue resources %v, want the secret ARN + *", st.resources)
		}
	}
	// The ARN parameter refuses the selector form, which would break that
	// grant; it accepts full and partial ARNs.
	arnPattern := regexp.MustCompile(get(tp.params["ChUsersSecretArn"], "AllowedPattern").(string))
	for v, ok := range map[string]bool{
		required["ChUsersSecretArn"]: true,
		"arn:aws:secretsmanager:us-east-1:123456789012:secret:twinhouse/ch-users":            true,
		required["ChUsersSecretArn"] + ":ch_users::":                                         false,
		"arn:aws:secretsmanager:us-east-1:123456789012:secret:twinhouse/ch-users:ch_users::": false,
	} {
		if arnPattern.MatchString(v) != ok {
			t.Errorf("ChUsersSecretArn AllowedPattern accepts %q = %v, want %v", v, !ok, ok)
		}
	}
	if regexp.MustCompile(get(tp.params["ChUsersSecretJsonKey"], "AllowedPattern").(string)).MatchString("a:b") {
		t.Error("ChUsersSecretJsonKey should refuse ':'")
	}
	for _, role := range []string{"ChserverTaskRole", "MvWorkerTaskRole", "ReplicatorTaskRole"} {
		for _, st := range statements(t, s.eval(t, tp.resource(t, role))) {
			for _, a := range st.actions {
				if strings.HasPrefix(a, "secretsmanager:") {
					t.Errorf("%s (the application's role) should not read secrets: %s", role, a)
				}
			}
		}
	}
}

// TestRules: parameter mistakes that would only show up as crash-looping
// tasks (config.Validate, the replicator's parseRegions) or an unreachable
// load balancer fail when the stack is created instead.
func TestRules(t *testing.T) {
	tp := loadTemplate(t)
	cidr := map[string]string{"AllowedCidr": "10.0.0.0/16"}
	withCidr := func(m map[string]string) map[string]string {
		m["AllowedCidr"] = cidr["AllowedCidr"]
		return m
	}
	for _, tc := range []struct {
		name, region string
		params       map[string]string
		fail         []string // substrings of the failing assertions
	}{
		{"home", "us-east-1", cidr, nil},
		{"other region", "eu-west-1", cidr, nil},
		{"client security group only", "us-east-1", map[string]string{"ClientSecurityGroupId": "sg-0123456789abcdef0"}, nil},
		{"no client access", "us-east-1", nil, []string{"AllowedCidr or ClientSecurityGroupId"}},
		{"region not listed", "ap-southeast-2", cidr, []string{"include this stack's region"}},
		{"one region", "us-east-1", withCidr(map[string]string{"ReplicaRegions": "us-east-1"}), []string{"at least one region other"}},
		{"home not listed", "eu-west-1", withCidr(map[string]string{"ReplicaRegions": "eu-west-1,ap-southeast-2"}), []string{"HomeRegion must be one of"}},
		{"single region without global mode", "ap-southeast-2", withCidr(map[string]string{"GlobalMode": "none", "ReplicaRegions": "us-east-1"}), nil},
	} {
		got := render(t, tp, tc.region, tc.params).failedRules(t)
		if len(got) != len(tc.fail) {
			t.Errorf("%s: failed rules %q, want %q", tc.name, got, tc.fail)
			continue
		}
		for _, want := range tc.fail {
			if !slices.ContainsFunc(got, func(g string) bool { return strings.Contains(g, want) }) {
				t.Errorf("%s: failed rules %q, want one containing %q", tc.name, got, want)
			}
		}
	}
}

// TestLoadBalancerAndPorts: clients switch to the server by host and port
// only, so everything uses ClickHouse's 8123 (HTTPS: 8443), and health
// checks use /ping.
func TestLoadBalancerAndPorts(t *testing.T) {
	tp := loadTemplate(t)
	s := render(t, tp, "us-east-1", map[string]string{"CertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc"})
	ch := s.containers(t, "ChserverTaskDefinition")[0]
	if ch.Env["LISTEN_ADDR"] != ":8123" || strings.Join(ch.Ports, ",") != "8123" {
		t.Errorf("chserver listens on %q, port mappings %v; want :8123", ch.Env["LISTEN_ADDR"], ch.Ports)
	}
	if !strings.Contains(strings.Join(ch.Health, " "), "http://127.0.0.1:8123/ping") {
		t.Errorf("container health check = %v, want /ping on 8123", ch.Health)
	}
	tg := s.eval(t, tp.resource(t, "TargetGroup"))
	for path, want := range map[string]string{"Port": "8123", "Protocol": "HTTP", "TargetType": "ip", "HealthCheckPath": "/ping", "Matcher.HttpCode": "200"} {
		got := get(tg, "Properties")
		for _, p := range strings.Split(path, ".") {
			got = get(got, p)
		}
		if fmt.Sprint(got) != want {
			t.Errorf("TargetGroup %s = %v, want %s", path, got, want)
		}
	}
	svc := s.eval(t, tp.resource(t, "ChserverService"))
	lb := get(svc, "Properties", "LoadBalancers", 0)
	if fmt.Sprint(get(lb, "ContainerPort")) != "8123" || get(lb, "ContainerName") != "chserver" || get(lb, "TargetGroupArn") != "<TargetGroup>" {
		t.Errorf("ChserverService load balancer = %v", lb)
	}
	if get(svc, "Properties", "HealthCheckGracePeriodSeconds") == nil {
		t.Error("ChserverService needs a health check grace period (startup waits for the system tables)")
	}
	// The first start in the home region adds the __catalog replicas and
	// waits for them (up to the store's tableWait) before listening.
	if !strings.Contains(readFile(t, filepath.Join(repoRoot, "internal", "storage", "dynamo", "store.go")), "tableWait: 5 * time.Minute") {
		t.Error("tableWait changed: revisit the HealthCheckGracePeriod default")
	}
	if g := get(tp.params["HealthCheckGracePeriod"], "Default"); g != "600" {
		t.Errorf("HealthCheckGracePeriod default %v, want 600 (tableWait 300 s plus startup)", g)
	}
	http := s.eval(t, tp.resource(t, "HttpListener"))
	https := s.eval(t, tp.resource(t, "HttpsListener"))
	if fmt.Sprint(get(http, "Properties", "Port")) != "8123" || get(http, "Properties", "Protocol") != "HTTP" {
		t.Errorf("HttpListener = %v", get(http, "Properties"))
	}
	if fmt.Sprint(get(https, "Properties", "Port")) != "8443" || get(https, "Properties", "Protocol") != "HTTPS" {
		t.Errorf("HttpsListener = %v", get(https, "Properties"))
	}
	if get(tp.resource(t, "LoadBalancer"), "Properties", "Scheme") != "internal" {
		t.Error("the load balancer must be internal")
	}
	attrs := map[string]string{}
	for _, a := range s.eval(t, get(tp.resource(t, "LoadBalancer"), "Properties", "LoadBalancerAttributes")).([]any) {
		attrs[fmt.Sprint(get(a, "Key"))] = fmt.Sprint(get(a, "Value"))
	}
	if attrs["idle_timeout.timeout_seconds"] != "110" {
		t.Errorf("idle timeout = %q", attrs["idle_timeout.timeout_seconds"])
	}
	// The default MAX_EXECUTION_TIME must end a query before the load
	// balancer's idle timeout cuts it, and the idle timeout must stay below
	// chserver's 2-minute keep-alive (http.Server.IdleTimeout).
	if def := get(tp.params["MaxExecutionTime"], "Default"); def != "100" {
		t.Errorf("MaxExecutionTime default %v, want 100 (< idle timeout 110)", def)
	}
	if mx := get(tp.params["LoadBalancerIdleTimeout"], "MaxValue"); mx != "115" ||
		!strings.Contains(readFile(t, filepath.Join(repoRoot, "cmd", "chserver", "main.go")), "IdleTimeout:       2 * time.Minute") {
		t.Errorf("LoadBalancerIdleTimeout MaxValue %v must stay below chserver's keep-alive idle timeout", mx)
	}
	// HTTP only when no certificate; KeepHttpListener=false drops it.
	httpsOnly := render(t, tp, "us-east-1", map[string]string{"CertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc", "KeepHttpListener": "false"})
	if httpsOnly.exists(t, "HttpListener") || !httpsOnly.exists(t, "HttpsListener") {
		t.Error("KeepHttpListener=false with a certificate: HTTPS only")
	}
	plain := render(t, tp, "us-east-1", map[string]string{"KeepHttpListener": "false"})
	if !plain.exists(t, "HttpListener") || plain.exists(t, "HttpsListener") {
		t.Error("without a certificate the HTTP listener is always created")
	}
	// The service must wait for a listener (the target group needs a load
	// balancer before tasks register): its tag references one in every case.
	for _, st := range []*stack{s, httpsOnly, plain} {
		tags := st.eval(t, get(tp.resource(t, "ChserverService"), "Properties", "Tags")).([]any)
		ok := false
		for _, tag := range tags {
			if get(tag, "Key") == "Listener" && strings.HasSuffix(fmt.Sprint(get(tag, "Value")), "Listener>") {
				ok = true
			}
		}
		if !ok {
			t.Error("ChserverService does not reference an existing listener")
		}
	}
}

func TestSecurityGroups(t *testing.T) {
	tp := loadTemplate(t)
	s := render(t, tp, "us-east-1", map[string]string{
		"AllowedCidr": "10.0.0.0/16", "ClientSecurityGroupId": "sg-0123456789abcdef0",
		"CertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc",
	})
	for _, name := range tp.resourcesOfType("AWS::EC2::SecurityGroupIngress") {
		if !s.exists(t, name) {
			t.Errorf("%s should exist with a CIDR, a client group and a certificate", name)
			continue
		}
		p := get(s.eval(t, tp.resource(t, name)), "Properties")
		port := fmt.Sprint(get(p, "FromPort"))
		if port != "8123" && port != "8443" {
			t.Errorf("%s opens port %s", name, port)
		}
		if name == "TaskIngressFromLoadBalancer" {
			if get(p, "GroupId") != "<TaskSecurityGroup>" || get(p, "SourceSecurityGroupId") != "<LoadBalancerSecurityGroup>" || port != "8123" {
				t.Errorf("tasks must accept only the load balancer on 8123: %v", p)
			}
		} else if get(p, "GroupId") != "<LoadBalancerSecurityGroup>" {
			t.Errorf("%s should open the load balancer's group", name)
		}
	}
	none := render(t, tp, "us-east-1", nil)
	for _, name := range tp.resourcesOfType("AWS::EC2::SecurityGroupIngress") {
		if name != "TaskIngressFromLoadBalancer" && none.exists(t, name) {
			t.Errorf("%s exists without AllowedCidr / ClientSecurityGroupId", name)
		}
	}
	egress := get(s.eval(t, tp.resource(t, "TaskSecurityGroup")), "Properties", "SecurityGroupEgress").([]any)
	if len(egress) != 1 || fmt.Sprint(get(egress[0], "FromPort")) != "443" {
		t.Errorf("tasks egress = %v, want HTTPS only", egress)
	}
	if svcSG := get(s.eval(t, tp.resource(t, "ChserverService")), "Properties", "NetworkConfiguration", "AwsvpcConfiguration"); get(svcSG, "AssignPublicIp") != "DISABLED" {
		t.Error("tasks must not get public IPs")
	}
}

// statement is one IAM policy statement (rendered).
type statement struct {
	sid       string
	actions   []string
	resources []string
	condition node
}

// statements collects the statements of a rendered IAM role's inline
// policies.
func statements(t *testing.T, role node) []statement {
	t.Helper()
	var out []statement
	pols, _ := get(role, "Properties", "Policies").([]any)
	for _, p := range pols {
		sts, _ := get(p, "PolicyDocument", "Statement").([]any)
		for _, st := range sts {
			s := statement{sid: fmt.Sprint(get(st, "Sid")), condition: get(st, "Condition")}
			s.actions = toStrings(t, get(st, "Action"))
			s.resources = toStrings(t, get(st, "Resource"))
			out = append(out, s)
		}
	}
	return out
}

func actionsOf(sts []statement) map[string][]statement {
	out := map[string][]statement{}
	for _, s := range sts {
		for _, a := range s.actions {
			out[a] = append(out[a], s)
		}
	}
	return out
}

// TestIAMIsScopedToTablePrefix checks the task roles: every DynamoDB ARN
// names the prefix; only ListTables uses Resource '*'; each role holds
// exactly the operations its binary makes (DESIGN.md §2, §5, §6).
func TestIAMIsScopedToTablePrefix(t *testing.T) {
	tp := loadTemplate(t)
	check := func(label string, s *stack) map[string]map[string][]statement {
		byRole := map[string]map[string][]statement{}
		for _, r := range []string{"ChserverTaskRole", "MvWorkerTaskRole", "ReplicatorTaskRole"} {
			if !s.exists(t, r) {
				continue
			}
			sts := statements(t, s.eval(t, tp.resource(t, r)))
			for _, st := range sts {
				for _, res := range st.resources {
					switch {
					case res == "*":
						if strings.Join(st.actions, ",") != "dynamodb:ListTables" {
							t.Errorf("%s %s/%s: Resource '*' for %v", label, r, st.sid, st.actions)
						}
					case strings.Contains(res, ":dynamodb:"):
						if !strings.Contains(res, ":table/twinhouse_") {
							t.Errorf("%s %s/%s: DynamoDB ARN not scoped to the prefix: %s", label, r, st.sid, res)
						}
						if strings.Contains(res, ":dynamodb:*:") && st.condition == nil {
							t.Errorf("%s %s/%s: cross-region ARN without an aws:RequestedRegion condition", label, r, st.sid)
						}
					case strings.HasPrefix(res, "arn:aws:iam::"):
						if !strings.Contains(res, "replication.dynamodb.amazonaws.com") {
							t.Errorf("%s %s/%s: unexpected IAM resource %s", label, r, st.sid, res)
						}
					default:
						t.Errorf("%s %s/%s: unexpected resource %s", label, r, st.sid, res)
					}
				}
				for _, a := range st.actions {
					if strings.HasSuffix(a, ":*") || a == "*" {
						t.Errorf("%s %s/%s: wildcard action %s", label, r, st.sid, a)
					}
					if a == "dynamodb:ListStreams" || strings.HasPrefix(a, "lambda:") {
						t.Errorf("%s %s/%s: %s is never called by the long-running binaries", label, r, st.sid, a)
					}
				}
			}
			byRole[r] = actionsOf(sts)
		}
		return byRole
	}
	streams := []string{"dynamodb:DescribeStream", "dynamodb:GetShardIterator", "dynamodb:GetRecords"}
	replicas := []string{"dynamodb:CreateTableReplica", "iam:CreateServiceLinkedRole"}

	home := check("home", render(t, tp, "us-east-1", map[string]string{"RunReplicator": "true"}))
	ch, mv, rep := home["ChserverTaskRole"], home["MvWorkerTaskRole"], home["ReplicatorTaskRole"]
	for _, a := range []string{"dynamodb:CreateTable", "dynamodb:DeleteTable", "dynamodb:UpdateTable", "dynamodb:UpdateTimeToLive",
		"dynamodb:ListTables", "dynamodb:CreateTableReplica", "dynamodb:DeleteTableReplica", "iam:CreateServiceLinkedRole",
		"dynamodb:ConditionCheckItem", "dynamodb:BatchGetItem"} {
		if len(ch[a]) == 0 {
			t.Errorf("chserver (DDL, reconciler, replicas in GLOBAL_MODE=aws) lacks %s", a)
		}
	}
	for _, a := range streams {
		if len(ch[a]) > 0 {
			t.Errorf("chserver without the in-process worker should not read streams (%s)", a)
		}
		if len(mv[a]) == 0 || len(rep[a]) == 0 {
			t.Errorf("stream consumers need %s", a)
		}
	}
	for _, a := range replicas {
		if len(mv[a]) == 0 {
			t.Errorf("home-region mvworker (EnsureSystemTables adds the __catalog replicas) lacks %s", a)
		}
		if len(rep[a]) > 0 {
			t.Errorf("the replicator never adds replicas (%s)", a)
		}
	}
	for _, st := range mv["dynamodb:CreateTableReplica"] {
		for _, r := range st.resources {
			if !strings.HasSuffix(r, ":table/twinhouse___catalog") {
				t.Errorf("mvworker's replica permission must be limited to __catalog, got %s", r)
			}
		}
	}
	for _, a := range []string{"dynamodb:DeleteTable", "dynamodb:DeleteTableReplica", "dynamodb:ListTables"} {
		if len(mv[a]) > 0 || len(rep[a]) > 0 {
			t.Errorf("only chserver drops tables or lists them (%s)", a)
		}
	}
	for _, a := range []string{"dynamodb:DescribeTable", "dynamodb:CreateTable", "dynamodb:PutItem", "dynamodb:DeleteItem", "dynamodb:UpdateItem"} {
		if len(rep[a]) == 0 {
			t.Errorf("replicator (checkpoints, dead letters, conditional copies, conflict views) lacks %s", a)
		}
	}
	// Cross-region access is limited to the deployment's regions.
	for _, st := range append(rep["dynamodb:PutItem"], ch["dynamodb:CreateTableReplica"]...) {
		if !strings.Contains(fmt.Sprint(st.condition), "aws:RequestedRegion:[us-east-1 eu-west-1]") {
			t.Errorf("%s: condition %v, want aws:RequestedRegion in ReplicaRegions", st.sid, st.condition)
		}
	}

	other := check("replica region", render(t, tp, "eu-west-1", nil))
	for _, a := range replicas {
		if len(other["MvWorkerTaskRole"][a]) > 0 {
			t.Errorf("mvworker outside the home region only joins __catalog (%s)", a)
		}
		if len(other["ChserverTaskRole"][a]) == 0 {
			t.Errorf("chserver in every region adds the replicas of the tables created there (%s)", a)
		}
	}
	if _, ok := other["ReplicatorTaskRole"]; ok {
		t.Error("RunReplicator=auto must not run the replicator outside the home region")
	}

	single := check("GlobalMode=none", render(t, tp, "eu-west-1", map[string]string{"GlobalMode": "none", "WorkerMode": "in-process"}))
	for _, a := range replicas {
		if len(single["ChserverTaskRole"][a]) > 0 {
			t.Errorf("GlobalMode=none: chserver needs no %s", a)
		}
	}
	for _, a := range streams {
		if len(single["ChserverTaskRole"][a]) == 0 {
			t.Errorf("chserver with the in-process worker needs %s", a)
		}
	}
	if _, ok := single["MvWorkerTaskRole"]; ok {
		t.Error("WorkerMode=in-process must not create the mvworker role")
	}
	// The execution role is the only one ECS itself uses.
	for _, r := range []string{"TaskExecutionRole", "ChserverTaskRole", "MvWorkerTaskRole", "ReplicatorTaskRole"} {
		trust := fmt.Sprint(get(tp.resource(t, r), "Properties", "AssumeRolePolicyDocument"))
		if !strings.Contains(trust, "ecs-tasks.amazonaws.com") || !strings.Contains(trust, "aws:SourceArn") {
			t.Errorf("%s: trust policy should name ecs-tasks.amazonaws.com with an aws:SourceArn condition", r)
		}
	}
}

func TestOptionalServices(t *testing.T) {
	tp := loadTemplate(t)
	for _, name := range []string{"MvWorkerService", "MvWorkerTaskDefinition", "MvWorkerTaskRole", "MvWorkerLogGroup"} {
		if get(tp.resource(t, name), "Condition") != "SeparateWorker" {
			t.Errorf("%s must have Condition SeparateWorker", name)
		}
	}
	for _, name := range []string{"ReplicatorService", "ReplicatorTaskDefinition", "ReplicatorTaskRole", "ReplicatorLogGroup"} {
		if get(tp.resource(t, name), "Condition") != "RunsReplicator" {
			t.Errorf("%s must have Condition RunsReplicator", name)
		}
	}
	cases := []struct {
		region string
		params map[string]string
		mv     bool
		rep    bool
		auto   bool
	}{
		{"us-east-1", nil, true, true, false},
		{"eu-west-1", nil, true, false, false},
		{"eu-west-1", map[string]string{"RunReplicator": "true"}, true, true, false},
		{"us-east-1", map[string]string{"RunReplicator": "false"}, true, false, false},
		{"us-east-1", map[string]string{"GlobalMode": "none", "RunReplicator": "true"}, true, false, false},
		{"us-east-1", map[string]string{"WorkerMode": "in-process"}, false, true, false},
		{"us-east-1", map[string]string{"ChserverMaxCount": "6"}, true, true, true},
	}
	for _, c := range cases {
		s := render(t, tp, c.region, c.params)
		label := fmt.Sprintf("%s %v", c.region, c.params)
		if got := s.exists(t, "MvWorkerService"); got != c.mv {
			t.Errorf("%s: mvworker service = %v, want %v", label, got, c.mv)
		}
		if got := s.exists(t, "ReplicatorService"); got != c.rep {
			t.Errorf("%s: replicator service = %v, want %v", label, got, c.rep)
		}
		if got := s.exists(t, "ChserverScalableTarget") && s.exists(t, "ChserverScalingPolicy"); got != c.auto {
			t.Errorf("%s: autoscaling = %v, want %v", label, got, c.auto)
		}
		if !s.exists(t, "Cluster") {
			t.Errorf("%s: a cluster is created when ExistingClusterName is empty", label)
		}
	}
	s := render(t, tp, "us-east-1", map[string]string{"ExistingClusterName": "shared", "ChserverMaxCount": "4"})
	if s.exists(t, "Cluster") {
		t.Error("ExistingClusterName: no cluster")
	}
	for _, svc := range []string{"ChserverService", "MvWorkerService", "ReplicatorService"} {
		if c := get(s.eval(t, tp.resource(t, svc)), "Properties", "Cluster"); c != "shared" {
			t.Errorf("%s runs in cluster %v, want shared", svc, c)
		}
		dc := get(s.eval(t, tp.resource(t, svc)), "Properties", "DeploymentConfiguration")
		if fmt.Sprint(get(dc, "DeploymentCircuitBreaker", "Rollback")) != "true" {
			t.Errorf("%s: deployments should roll back on failure", svc)
		}
	}
	if id := get(s.eval(t, tp.resource(t, "ChserverScalableTarget")), "Properties", "ResourceId"); id != "service/shared/<ChserverService.Name>" {
		t.Errorf("scalable target ResourceId = %v", id)
	}
	pol := s.eval(t, tp.resource(t, "ChserverScalingPolicy"))
	if get(pol, "Properties", "TargetTrackingScalingPolicyConfiguration", "PredefinedMetricSpecification", "PredefinedMetricType") != "ECSServiceAverageCPUUtilization" {
		t.Error("chserver autoscaling should track average CPU")
	}
}

func TestOutputs(t *testing.T) {
	tp := loadTemplate(t)
	for _, name := range []string{"LoadBalancerDnsName", "ClickHouseUrl", "ClusterName", "ChserverServiceName", "MvWorkerServiceName", "ReplicatorServiceName"} {
		if _, ok := tp.outputs[name]; !ok {
			t.Errorf("missing output %s", name)
		}
	}
	s := render(t, tp, "us-east-1", nil)
	if got := s.eval(t, get(tp.outputs["ClickHouseUrl"], "Value")); got != "http://<LoadBalancer.DNSName>:8123" {
		t.Errorf("ClickHouseUrl = %v", got)
	}
	for out, cond := range map[string]string{"MvWorkerServiceName": "SeparateWorker", "ReplicatorServiceName": "RunsReplicator", "ClickHouseUrl": "CreateHttpListener"} {
		if get(tp.outputs[out], "Condition") != cond {
			t.Errorf("output %s must have Condition %s", out, cond)
		}
	}
}

func TestSamconfigEnvironments(t *testing.T) {
	tp := loadTemplate(t)
	cfg := readFile(t, "samconfig.toml")
	section := func(name string) string {
		_, rest, ok := strings.Cut(cfg, "\n["+name+"]\n")
		if !ok {
			t.Fatalf("samconfig.toml lacks [%s]", name)
		}
		if i := strings.Index(rest, "\n["); i >= 0 {
			rest = rest[:i]
		}
		return rest
	}
	paramRe := regexp.MustCompile(`(?m)^  "([A-Za-z0-9]+)=([^"]*)",`)
	for env, region := range map[string]string{"us": "us-east-1", "eu": "eu-west-1"} {
		s := section(env + ".deploy.parameters")
		if !strings.Contains(s, `region = "`+region+`"`) || !strings.Contains(s, `capabilities = "CAPABILITY_IAM"`) || !strings.Contains(s, "resolve_s3 = true") {
			t.Errorf("[%s.deploy.parameters] needs region %s, CAPABILITY_IAM and resolve_s3 (S3 upload leaves room above the 51,200-byte inline limit)", env, region)
		}
		params := map[string]string{}
		for _, m := range paramRe.FindAllStringSubmatch(s, -1) {
			if _, ok := tp.params[m[1]]; !ok {
				t.Errorf("[%s] sets unknown parameter %s", env, m[1])
			}
			params[m[1]] = m[2]
		}
		for k, want := range map[string]string{"GlobalMode": "aws", "HomeRegion": "us-east-1", "ReplicaRegions": "us-east-1,eu-west-1", "RunReplicator": "auto"} {
			if params[k] != want {
				t.Errorf("[%s] %s = %q, want %q", env, k, params[k], want)
			}
		}
		// Account-specific values are placeholders CloudFormation rejects
		// (pattern or AWS-specific parameter type), never real ids.
		for _, k := range []string{"ImageUri", "VpcId", "SubnetIds", "AllowedCidr", "ChUsersSecretArn"} {
			if !strings.Contains(params[k], "CHANGE_ME") {
				t.Errorf("[%s] %s = %q, want a CHANGE_ME placeholder", env, k, params[k])
			}
			if pat, ok := get(tp.params[k], "AllowedPattern").(string); ok && regexp.MustCompile(pat).MatchString(params[k]) {
				t.Errorf("[%s] placeholder %s=%q passes the parameter's AllowedPattern", env, k, params[k])
			}
		}
		if regexp.MustCompile(`[0-9]{12}`).MatchString(s) {
			t.Errorf("[%s] commits an account id", env)
		}
		if strings.Contains(strings.ToLower(s), "ch_users") || strings.Contains(s, "password") {
			t.Errorf("[%s] must not carry CH_USERS", env)
		}
	}
	if !strings.Contains(cfg, `template_file = "template.yaml"`) {
		t.Error("samconfig.toml should pin template_file")
	}
	gi := readFile(t, filepath.Join(repoRoot, ".gitignore"))
	if !strings.Contains(gi, "/deploy/aws-ecs/samconfig.local.toml") {
		t.Error(".gitignore must ignore deploy/aws-ecs/samconfig.local.toml")
	}
}

func TestMakefileTargets(t *testing.T) {
	mk := readFile(t, filepath.Join(repoRoot, "Makefile"))
	for _, target := range []string{"ecs-validate:", "image-push:", "ecs-deploy:"} {
		if !strings.Contains(mk, "\n"+target) {
			t.Errorf("Makefile lacks target %s", target)
		}
	}
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make not installed")
	}
	run := func(args ...string) (string, error) {
		out, err := exec.Command("make", append([]string{"-n", "-C", repoRoot}, args...)...).CombinedOutput()
		return string(out), err
	}
	if out, err := run("ecs-validate"); err != nil || !strings.Contains(out, "validate --lint") || !strings.Contains(out, "deploy/aws-ecs") {
		t.Errorf("make -n ecs-validate: %v\n%s", err, out)
	}
	if out, err := run("ecs-deploy"); err != nil || !strings.Contains(out, "--config-env us") || !strings.Contains(out, "deploy/aws-ecs") {
		t.Errorf("make -n ecs-deploy: %v\n%s", err, out)
	}
	if out, err := run("ecs-deploy", "ECS_DEPLOY_ARGS=--parameter-overrides ImageUri=x"); err == nil || !strings.Contains(out, "parameter-overrides") {
		t.Errorf("make ecs-deploy must refuse --parameter-overrides, got err=%v:\n%s", err, out)
	}
	if out, err := run("image-push"); err == nil || !strings.Contains(out, "IMAGE_REPO") {
		t.Errorf("make image-push must require IMAGE_REPO, got err=%v:\n%s", err, out)
	}
	out, err := run("image-push", "IMAGE_REPO=123456789012.dkr.ecr.us-east-1.amazonaws.com/twinhouse", "IMAGE_TAG=v1", "PLATFORM=linux/amd64")
	if err != nil || !strings.Contains(out, "buildx build --platform linux/amd64") || !strings.Contains(out, "-f deploy/Dockerfile") ||
		!strings.Contains(out, "123456789012.dkr.ecr.us-east-1.amazonaws.com/twinhouse:v1") || !strings.Contains(out, "--push") {
		t.Errorf("make -n image-push: %v\n%s", err, out)
	}
	// The serverless targets run from deploy/aws-lambda.
	if out, err := run("aws-validate"); err != nil || !strings.Contains(out, "deploy/aws-lambda") {
		t.Errorf("make -n aws-validate: %v\n%s", err, out)
	}
}

// TestDockerfileArchitectures: the image the template runs builds for
// either CpuArchitecture (ARM64 = linux/arm64, X86_64 = linux/amd64): the
// Dockerfile pins no platform or GOARCH, so `docker buildx build
// --platform` decides, and the static (CGO_ENABLED=0) binaries need no
// target libraries.
func TestDockerfileArchitectures(t *testing.T) {
	tp := loadTemplate(t)
	df := readFile(t, filepath.Join(repoRoot, "deploy", "Dockerfile"))
	if !strings.Contains(df, "CGO_ENABLED=0") {
		t.Error("deploy/Dockerfile should build static binaries (CGO_ENABLED=0)")
	}
	if regexp.MustCompile(`GOARCH=(amd64|arm64)|--platform=linux/`).MatchString(df) {
		t.Error("deploy/Dockerfile pins an architecture; the CpuArchitecture parameter offers both")
	}
	for _, bin := range []string{"./cmd/chserver", "./cmd/mvworker", "./cmd/replicator"} {
		if !strings.Contains(df, bin) {
			t.Errorf("deploy/Dockerfile does not build %s", bin)
		}
	}
	archs := toStrings(t, get(tp.params["CpuArchitecture"], "AllowedValues"))
	sort.Strings(archs)
	if strings.Join(archs, ",") != "ARM64,X86_64" {
		t.Errorf("CpuArchitecture values %v", archs)
	}
}

// TestSamValidateLint lints the template with the SAM CLI (cfn-lint); it
// needs no AWS credentials. Skipped when sam is not installed.
func TestSamValidateLint(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	sam, err := exec.LookPath("sam")
	if err != nil {
		t.Skip("sam CLI not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, sam, "validate", "--lint", "-t", "template.yaml")
	cmd.Env = append(os.Environ(), "SAM_CLI_TELEMETRY=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sam validate --lint: %v\n%s", err, out)
	}
}
