package deployawslambda_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// repoRoot is the module root (deploy/aws-lambda is two levels down).
var repoRoot = filepath.Join("..", "..")

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// functionCodeUris are the contract's binaries per SAM function.
var functionCodeUris = map[string]string{
	"ApiFunction":        "../../cmd/chserver-lambda",
	"MvWorkerFunction":   "../../cmd/mvworker",
	"ReconcilerFunction": "../../cmd/reconciler-lambda",
	"ReplicatorFunction": "../../cmd/replicator",
}

// resources splits the template's Resources section into per-resource
// bodies, keyed by logical id (two-space indented keys).
func resources(t *testing.T, tmpl string) map[string]string {
	t.Helper()
	_, rest, ok := strings.Cut(tmpl, "\nResources:\n")
	if !ok {
		t.Fatal("template has no Resources section")
	}
	if i := strings.Index(rest, "\nOutputs:\n"); i >= 0 {
		rest = rest[:i]
	}
	out := map[string]string{}
	var name string
	var body strings.Builder
	flush := func() {
		if name != "" {
			out[name] = body.String()
		}
		body.Reset()
	}
	resourceKey := regexp.MustCompile(`^  ([A-Za-z0-9]+):\s*$`)
	for _, line := range strings.Split(rest, "\n") {
		if m := resourceKey.FindStringSubmatch(line); m != nil {
			flush()
			name = m[1]
			continue
		}
		body.WriteString(line)
		body.WriteByte('\n')
	}
	flush()
	return out
}

func TestFunctionsMatchContract(t *testing.T) {
	tmpl := readFile(t, "template.yaml")
	res := resources(t, tmpl)
	var functions []string
	for name, body := range res {
		if strings.Contains(body, "Type: AWS::Serverless::Function") {
			functions = append(functions, name)
		}
	}
	sort.Strings(functions)
	var want []string
	for name := range functionCodeUris {
		want = append(want, name)
	}
	sort.Strings(want)
	if strings.Join(functions, ",") != strings.Join(want, ",") {
		t.Fatalf("functions = %v, want %v", functions, want)
	}
	codeURI := regexp.MustCompile(`CodeUri:\s*(\S+)`)
	for _, name := range functions {
		body := res[name]
		m := codeURI.FindStringSubmatch(body)
		if m == nil {
			t.Errorf("%s: no CodeUri", name)
			continue
		}
		if m[1] != functionCodeUris[name] {
			t.Errorf("%s: CodeUri = %s, want %s", name, m[1], functionCodeUris[name])
		}
		if !strings.Contains(body, "BuildMethod: go1.x") {
			t.Errorf("%s: Metadata BuildMethod go1.x missing (sam build compiles each cmd dir in place)", name)
		}
		if !strings.Contains(body, "LoggingConfig:") || !strings.Contains(body, "LogGroup: !Ref "+name[:len(name)-len("Function")]+"LogGroup") {
			t.Errorf("%s: should log to its own retention-managed log group", name)
		}
		// The directory must be a Go main package so sam build yields a bootstrap.
		dir := filepath.Clean(m[1])
		entries, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil || len(entries) == 0 {
			t.Errorf("%s: CodeUri %s has no Go files (err=%v)", name, dir, err)
			continue
		}
		isMain := false
		for _, f := range entries {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			if strings.Contains(readFile(t, f), "\npackage main\n") {
				isMain = true
				break
			}
		}
		if !isMain {
			t.Errorf("%s: CodeUri %s is not a package main", name, dir)
		}
	}
	for _, want := range []string{"Runtime: provided.al2023", "Architectures: [arm64]", "Handler: bootstrap"} {
		if !strings.Contains(tmpl, want) {
			t.Errorf("template lacks %q in Globals", want)
		}
	}
}

// envVarRe matches environment variable keys (UPPER_SNAKE) inside the
// template's Variables blocks; CloudFormation keys are CamelCase so the
// shape alone tells them apart.
var envVarRe = regexp.MustCompile(`(?m)^\s+([A-Z][A-Z0-9_]+):\s`)

// TestEnvironmentVariablesAreReadByTheCode fails on a variable the template
// (or the sam local env file) sets but no binary reads, i.e. a typo or a
// stale name.
func TestEnvironmentVariablesAreReadByTheCode(t *testing.T) {
	keys := map[string]bool{}
	for _, m := range envVarRe.FindAllStringSubmatch(readFile(t, "template.yaml"), -1) {
		keys[m[1]] = true
	}
	var local struct {
		Parameters map[string]string `json:"Parameters"`
	}
	if err := json.Unmarshal([]byte(readFile(t, "local-env.json")), &local); err != nil {
		t.Fatalf("local-env.json: %v", err)
	}
	for k := range local.Parameters {
		keys[k] = true
	}
	if len(keys) < 10 {
		t.Fatalf("found only %d environment variables, parsing is off: %v", len(keys), keys)
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
	for k := range keys {
		if !bytes.Contains(sources, []byte(`"`+k+`"`)) {
			t.Errorf("environment variable %s is set by the deployment but no binary reads it", k)
		}
	}
	// The contract's reconciler variables must reach the reconciler.
	res := resources(t, readFile(t, "template.yaml"))
	for _, k := range []string{"MVWORKER_FUNCTION_ARN", "REPLICATOR_FUNCTION_ARN", "ESM_BATCH_SIZE"} {
		if !strings.Contains(res["ReconcilerFunction"], k+":") {
			t.Errorf("ReconcilerFunction does not set %s", k)
		}
	}
	if !strings.Contains(res["ApiFunction"], "CH_USERS:") {
		t.Error("ApiFunction does not set CH_USERS")
	}
	// sam local runs against DynamoDB Local (a parameter override, since
	// sam local only overrides declared variables) in emulated mode with
	// event-source mapping management disabled
	// (config.ManagesEventSourceMappings).
	if v, ok := local.Parameters["MVWORKER_FUNCTION_ARN"]; !ok || v != "" || local.Parameters["GLOBAL_MODE"] != "emulated" {
		t.Errorf("local-env.json must set GLOBAL_MODE=emulated and an empty MVWORKER_FUNCTION_ARN: %v", local.Parameters)
	}
	tmpl := readFile(t, "template.yaml")
	for k := range local.Parameters {
		if !strings.Contains(tmpl, k+":") {
			t.Errorf("local-env.json sets %s, which the template does not declare: sam local would ignore it", k)
		}
	}
	samcfg := readFile(t, "samconfig.toml")
	if !strings.Contains(samcfg, "DynamoDBEndpoint=http://") || !strings.Contains(tmpl, "DYNAMODB_ENDPOINT: !If [HasDynamoDBEndpoint") {
		t.Error("sam local must get DYNAMODB_ENDPOINT through the DynamoDBEndpoint parameter (absent on AWS)")
	}
	if !strings.Contains(samcfg, `template_file = ".aws-sam/build/template.yaml"`) {
		t.Error("sam local must serve the built template (compiled bootstraps), not the source directories")
	}
}

// TestIAMIsScopedToTablePrefix checks every DynamoDB ARN names the prefix,
// and that only the Lambda event-source-mapping statements, which the API
// cannot scope by resource, use Resource '*' (they carry a Condition).
func TestIAMIsScopedToTablePrefix(t *testing.T) {
	tmpl := readFile(t, "template.yaml")
	wildcardOK := map[string]bool{"ListEventSourceMappings": true, "CreateEventSourceMapping": true, "ListTables": true}
	sidRe := regexp.MustCompile(`^\s*- Sid: (\S+)`)
	var sid string
	statements := map[string]string{}
	for _, line := range strings.Split(tmpl, "\n") {
		if m := sidRe.FindStringSubmatch(line); m != nil {
			sid = m[1]
		}
		if sid != "" {
			statements[sid] += line + "\n"
		}
		if strings.Contains(line, ":dynamodb:") && !strings.Contains(line, "table/${TablePrefix}") {
			t.Errorf("DynamoDB ARN not scoped to TablePrefix: %s", strings.TrimSpace(line))
		}
		if strings.Contains(line, "Resource: '*'") && !wildcardOK[sid] {
			t.Errorf("statement %s uses Resource '*'", sid)
		}
	}
	for s := range wildcardOK {
		if s == "CreateEventSourceMapping" && !strings.Contains(statements[s], "Condition:") {
			t.Errorf("statement %s has a wildcard resource without a Condition", s)
		}
	}
	// Stream reads are needed by every stream consumer, and by nobody else.
	res := resources(t, tmpl)
	for name, body := range res {
		if !strings.Contains(body, "Type: AWS::Serverless::Function") {
			continue
		}
		hasStreams := strings.Contains(body, "dynamodb:GetRecords")
		if (name == "ApiFunction") == hasStreams {
			t.Errorf("%s: stream read permissions = %v", name, hasStreams)
		}
		// SAM attaches AWSLambdaBasicExecutionRole (CloudWatch Logs on *)
		// to every generated role; a scoped logs statement would only
		// overstate the roles' tightness.
		if strings.Contains(body, "logs:") {
			t.Errorf("%s: redundant logs statement (SAM adds AWSLambdaBasicExecutionRole)", name)
		}
	}
	// Replica operations belong to the reconciler alone: the API runs DDL
	// with dynamo.Options.AsyncDDL and never adds or removes replicas.
	for _, action := range []string{"dynamodb:CreateTableReplica", "dynamodb:DeleteTableReplica", "iam:CreateServiceLinkedRole", "dynamodb:DeleteTable", "dynamodb:ListTables"} {
		if !strings.Contains(res["ReconcilerFunction"], action) {
			t.Errorf("ReconcilerFunction (EnsureTable/DropTable with GLOBAL_MODE=aws, orphan discovery) lacks %s", action)
		}
	}
	for _, action := range []string{"dynamodb:CreateTableReplica", "dynamodb:DeleteTableReplica", "iam:CreateServiceLinkedRole", "dynamodb:ListTables"} {
		if strings.Contains(res["ApiFunction"], action) {
			t.Errorf("ApiFunction must not hold %s (replica operations and discovery are the reconciler's)", action)
		}
	}
	for _, action := range []string{"dynamodb:CreateTable", "dynamodb:DeleteTable", "dynamodb:DescribeTable"} {
		if !strings.Contains(res["ApiFunction"], action) {
			t.Errorf("ApiFunction (CREATE/DROP TABLE) lacks %s", action)
		}
	}
	for _, action := range []string{"lambda:CreateEventSourceMapping", "lambda:UpdateEventSourceMapping", "lambda:DeleteEventSourceMapping", "lambda:ListEventSourceMappings"} {
		if !strings.Contains(res["ReconcilerFunction"], action) {
			t.Errorf("ReconcilerFunction lacks %s", action)
		}
	}
	if strings.Contains(tmpl, "lambda:GetEventSourceMapping") {
		t.Error("lambda:GetEventSourceMapping is granted but esm.Manager never calls it")
	}
	// CH_USERS can be protected with a customer managed key.
	if !strings.Contains(res["ApiFunction"], "KmsKeyArn: !If [HasKmsKey, !Ref KmsKeyArn") || !strings.Contains(res["ApiFunction"], "kms:Decrypt") {
		t.Error("ApiFunction should encrypt its environment with KmsKeyArn when set and be allowed to decrypt it")
	}
}

// TestApiGatewayAccountAndReconcilerInvokeConfig: access logging needs the
// region's API Gateway CloudWatch role, which the stack manages (behind a
// parameter) and orders before the stage; the reconciler drops throttled
// schedule ticks instead of retrying them for hours.
func TestApiGatewayAccountAndReconcilerInvokeConfig(t *testing.T) {
	tmpl := readFile(t, "template.yaml")
	res := resources(t, tmpl)
	if strings.Contains(res["Api"], "AccessLogSetting:") {
		if !strings.Contains(res["ApiGatewayAccount"], "Type: AWS::ApiGateway::Account") ||
			!strings.Contains(res["ApiGatewayCloudWatchRole"], "AmazonAPIGatewayPushToCloudWatchLogs") {
			t.Error("AccessLogSetting needs an AWS::ApiGateway::Account with a CloudWatch Logs role")
		}
		if !strings.Contains(res["ApiAccessLogGroup"], "!Ref ApiGatewayAccount") {
			t.Error("the access log group (and so the stage) must depend on the account setting")
		}
		for _, name := range []string{"ApiGatewayAccount", "ApiGatewayCloudWatchRole"} {
			if !strings.Contains(res[name], "Condition: ManageApiGwAccount") {
				t.Errorf("%s must be optional (ManageApiGatewayAccount)", name)
			}
		}
	}
	rec := res["ReconcilerFunction"]
	if !strings.Contains(rec, "EventInvokeConfig:") || !strings.Contains(rec, "MaximumRetryAttempts: 0") || !strings.Contains(rec, "MaximumEventAgeInSeconds: 60") {
		t.Error("ReconcilerFunction should drop throttled asynchronous invocations (EventInvokeConfig retries 0, age 60s)")
	}
}

func TestPrivateApiAndSchedule(t *testing.T) {
	tmpl := readFile(t, "template.yaml")
	res := resources(t, tmpl)
	api := res["Api"]
	for _, want := range []string{"Type: PRIVATE", "VPCEndpointIds: [!Ref VpcEndpointId]", "IntrinsicVpceWhitelist: [!Ref VpcEndpointId]", "BinaryMediaTypes: ['*~1*']"} {
		if !strings.Contains(api, want) {
			t.Errorf("Api lacks %q", want)
		}
	}
	apiFn := res["ApiFunction"]
	for _, want := range []string{"Path: /\n", "Path: /{proxy+}", "Method: ANY", "TimeoutInMillis: !Ref ApiIntegrationTimeoutMillis"} {
		if strings.Count(apiFn, want) == 0 {
			t.Errorf("ApiFunction events lack %q", want)
		}
	}
	if strings.Count(apiFn, "Method: ANY") != 2 {
		t.Errorf("ApiFunction should have ANY on / and /{proxy+}")
	}
	if !strings.Contains(res["ReconcilerFunction"], "Schedule: rate(1 minute)") {
		t.Error("ReconcilerFunction lacks the rate(1 minute) schedule")
	}
	for _, fn := range []string{"MvWorkerFunction", "ReplicatorFunction"} {
		if strings.Contains(res[fn], "Events:") {
			t.Errorf("%s must have no template events: the reconciler creates its event-source mappings", fn)
		}
	}
	if strings.Contains(tmpl, "AWS::DynamoDB::") {
		t.Error("the template must not declare DynamoDB tables; the functions create them at runtime")
	}
	for _, out := range []string{"ApiEndpoint:", "ApiVpceEndpoint:", "ApiFunctionArn:", "MvWorkerFunctionArn:", "ReconcilerFunctionArn:", "ReplicatorFunctionArn:"} {
		if !strings.Contains(tmpl, "\n  "+out) {
			t.Errorf("missing output %s", out)
		}
	}
}

func TestSamconfigEnvironments(t *testing.T) {
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
	us, eu := section("us.deploy.parameters"), section("eu.deploy.parameters")
	if !strings.Contains(us, `region = "us-east-1"`) || !strings.Contains(eu, `region = "eu-west-1"`) {
		t.Error("us/eu environments must deploy to us-east-1 / eu-west-1")
	}
	for name, s := range map[string]string{"us": us, "eu": eu} {
		for _, want := range []string{`"HomeRegion=us-east-1"`, `"ReplicaRegions=us-east-1,eu-west-1"`, `"GlobalMode=aws"`, `capabilities = "CAPABILITY_IAM"`} {
			if !strings.Contains(s, want) {
				t.Errorf("[%s.deploy.parameters] lacks %s", name, want)
			}
		}
		// VpcEndpointId is account-specific: the committed file carries a
		// placeholder the operator must replace (CloudFormation rejects it),
		// never a real id; ChUsers is never committed either.
		if !strings.Contains(s, "\n  \"VpcEndpointId=vpce-CHANGE_ME\"") || regexp.MustCompile(`"VpcEndpointId=vpce-[0-9a-f]{8,17}"`).MatchString(s) {
			t.Errorf("[%s.deploy.parameters] must carry the VpcEndpointId=vpce-CHANGE_ME placeholder", name)
		}
		if strings.Contains(s, "\n  \"ChUsers=") {
			t.Errorf("[%s.deploy.parameters] commits ChUsers; it belongs in a git-ignored copy", name)
		}
	}
	if !strings.Contains(cfg, `template_file = "template.yaml"`) {
		t.Error("samconfig.toml should pin template_file")
	}
	// The go1.x builder's cache only hashes cmd/<fn>: a stale bootstrap
	// would be deployed after a change under internal/.
	if !strings.Contains(cfg, "\ncached = false\n") {
		t.Error("samconfig.toml must disable the build cache (it does not cover internal/)")
	}
}

func TestMakefileTargets(t *testing.T) {
	mk := readFile(t, filepath.Join(repoRoot, "Makefile"))
	if !strings.Contains(mk, "deploy/local/docker-compose.yml") || strings.Contains(mk, "deploy/docker-compose.yml") {
		t.Error("Makefile must use deploy/local/docker-compose.yml")
	}
	for _, target := range []string{"aws-validate:", "aws-build:", "aws-local-api:", "aws-deploy:"} {
		if !strings.Contains(mk, "\n"+target) {
			t.Errorf("Makefile lacks target %s", target)
		}
	}
	if !strings.Contains(mk, "--config-env $(ENV)") {
		t.Error("aws-deploy must select the environment with --config-env $(ENV)")
	}
	gi := readFile(t, filepath.Join(repoRoot, ".gitignore"))
	if !strings.Contains(gi, "\n.aws-sam/\n") || !strings.Contains(gi, "samconfig.local.toml") {
		t.Error(".gitignore must ignore .aws-sam/ and the operator's samconfig.local.toml")
	}
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make not installed")
	}
	for _, target := range []string{"aws-validate", "aws-build", "aws-deploy"} {
		cmd := exec.Command("make", "-n", "-C", repoRoot, target)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("make -n %s: %v\n%s", target, err, out)
		}
		if target == "aws-deploy" && !strings.Contains(string(out), "--config-env us") {
			t.Errorf("make -n aws-deploy should run sam deploy --config-env us, got:\n%s", out)
		}
	}
	// --parameter-overrides replaces the whole parameter list of the
	// environment: the Makefile refuses it.
	cmd := exec.Command("make", "-n", "-C", repoRoot, "aws-deploy", "SAM_DEPLOY_ARGS=--parameter-overrides ChUsers=x")
	if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "parameter-overrides") {
		t.Errorf("make aws-deploy must refuse --parameter-overrides, got err=%v:\n%s", err, out)
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

// TestLocalComposeConfig checks the moved compose file still resolves (build
// context ../.., dockerfile deploy/Dockerfile) with every profile.
func TestLocalComposeConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	compose := filepath.Join("..", "local", "docker-compose.yml")
	cmd := exec.CommandContext(ctx, "docker", "compose", "-p", "dhconfigtest", "-f", compose,
		"--profile", "compat", "--profile", "single", "config")
	out, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "Cannot connect to the Docker daemon") {
			t.Skip("docker daemon not running")
		}
		t.Fatalf("docker compose config: %v\n%s", err, out)
	}
	root, _ := filepath.Abs(repoRoot)
	if !strings.Contains(string(out), "context: "+root+"\n") || !strings.Contains(string(out), "dockerfile: deploy/Dockerfile") {
		t.Errorf("compose build context should be the repository root with deploy/Dockerfile:\n%s", out)
	}
	if !strings.Contains(string(out), "twinhouse:dev") {
		t.Errorf("compose should default the image to twinhouse:dev")
	}
}

// TestBuiltBootstrapsAreArm64 checks the artifacts of a previous `sam build`
// (when present) are 64-bit AArch64 ELF executables named bootstrap, as
// provided.al2023/arm64 requires.
func TestBuiltBootstrapsAreArm64(t *testing.T) {
	if _, err := os.Stat(filepath.Join(".aws-sam", "build", "template.yaml")); err != nil {
		t.Skip("no sam build output")
	}
	for fn := range functionCodeUris {
		path := filepath.Join(".aws-sam", "build", fn, "bootstrap")
		f, err := os.Open(path)
		if err != nil {
			t.Errorf("%s: %v (run make aws-build)", fn, err)
			continue
		}
		hdr := make([]byte, 20)
		_, err = f.Read(hdr)
		f.Close()
		if err != nil || string(hdr[:4]) != "\x7fELF" {
			t.Errorf("%s: not an ELF file", path)
			continue
		}
		const elfClass64, emAArch64 = 2, 0xB7
		if hdr[4] != elfClass64 {
			t.Errorf("%s: not a 64-bit ELF", path)
		}
		if m := binary.LittleEndian.Uint16(hdr[18:20]); m != emAArch64 {
			t.Errorf("%s: e_machine = %#x, want AArch64 (%#x)", path, m, emAArch64)
		}
	}
}
