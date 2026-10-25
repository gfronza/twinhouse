//go:build multiregion

package multiregion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// ---------------------------------------------------------------------------
// docker compose (fault injection)
// ---------------------------------------------------------------------------

// App services the suite may restart. DynamoDB Local runs -inMemory, so its
// containers are never touched (that would wipe the region).
var appServices = []string{"chserver-us", "chserver-eu", "mvworker-us", "mvworker-eu", "replicator"}

var (
	composeOnce sync.Once
	composeErr  error
)

func portOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Port()
}

// composeEnv pins the stack's host ports so that `up` never re-publishes a
// service on the defaults of the shared development stack.
func composeEnv(extra ...string) []string {
	env := os.Environ()
	for k, raw := range map[string]string{
		"US_HTTP_PORT": cfg.usURL, "EU_HTTP_PORT": cfg.euURL,
		"DDB_US_PORT": cfg.ddbUS, "DDB_EU_PORT": cfg.ddbEU,
	} {
		if p := portOf(raw); p != "" {
			env = append(env, k+"="+p)
		}
	}
	return append(env, extra...)
}

func composeCmd(ctx context.Context, env []string, args ...string) (string, error) {
	full := append([]string{"compose", "-p", cfg.project, "-f", cfg.compose}, args...)
	cmd := exec.CommandContext(ctx, "docker", full...)
	cmd.Dir = cfg.repoRoot
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker %s: %w\n%s", strings.Join(full, " "), err, out)
	}
	return string(out), nil
}

// requireCompose skips the test unless docker compose controls the project
// whose published ports are the ones the suite talks to.
func requireCompose(t testing.TB) {
	t.Helper()
	requireStack(t)
	if cfg.project == "dhdev" {
		t.Skip("refusing to inject faults into the shared development stack (MR_COMPOSE_PROJECT=dhdev)")
	}
	composeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for svc, raw := range map[string]string{"chserver-us": cfg.usURL, "chserver-eu": cfg.euURL} {
			out, err := composeCmd(ctx, composeEnv(), "port", svc, "8123")
			if err != nil {
				composeErr = err
				return
			}
			if p := portOf("http://" + strings.TrimSpace(out)); p != portOf(raw) {
				composeErr = fmt.Errorf("project %s publishes %s on %q, but the suite uses %s", cfg.project, svc, strings.TrimSpace(out), raw)
				return
			}
		}
	})
	if composeErr != nil {
		t.Skipf("fault injection unavailable: %v", composeErr)
	}
}

// compose runs a docker compose command against the project and fails the
// test on error.
func compose(t testing.TB, args ...string) {
	t.Helper()
	composeWithEnv(t, nil, args...)
}

func composeWithEnv(t testing.TB, extraEnv []string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	start := time.Now()
	if _, err := composeCmd(ctx, composeEnv(extraEnv...), args...); err != nil {
		t.Fatalf("%v", err)
	}
	t.Logf("docker compose %s (%s)", strings.Join(args, " "), time.Since(start).Round(time.Millisecond))
}

// restoreStack brings every app service up with REPLICATION_DELAY=0s
// (recreating only services whose configuration changed) and waits until
// both servers answer.
func restoreStack(t testing.TB) {
	t.Helper()
	args := append([]string{"up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "120"}, appServices...)
	composeWithEnv(t, []string{"REPLICATION_DELAY=0s"}, args...)
	waitPing(t, us())
	waitPing(t, eu())
}

// faultTest prepares a fault-injection test: compose must be usable, the
// stack starts in its normal state and is restored when the test ends.
func faultTest(t *testing.T) {
	t.Helper()
	requireCompose(t)
	restoreStack(t)
	t.Cleanup(func() { restoreStack(t) })
}

func waitPing(t testing.TB, r *region) {
	t.Helper()
	ok := eventually(t, 90*time.Second, r.name+": /ping", func() (bool, string) {
		if err := ping(r.base); err != nil {
			return false, err.Error()
		}
		return true, ""
	})
	if !ok {
		t.FailNow()
	}
}

// ---------------------------------------------------------------------------
// Direct DynamoDB access
// ---------------------------------------------------------------------------

type ddb struct {
	name string
	db   *dynamodb.Client
	st   *dynamodbstreams.Client
}

func newDDB(name, endpoint, awsRegion string) *ddb {
	creds := credentials.NewStaticCredentialsProvider("local", "local", "")
	return &ddb{
		name: name,
		db: dynamodb.New(dynamodb.Options{Region: awsRegion, Credentials: creds, BaseEndpoint: aws.String(endpoint),
			RetryMaxAttempts: 5}),
		st: dynamodbstreams.New(dynamodbstreams.Options{Region: awsRegion, Credentials: creds, BaseEndpoint: aws.String(endpoint),
			RetryMaxAttempts: 5}),
	}
}

func ddbUS() *ddb { return newDDB("us", cfg.ddbUS, "us-east-1") }
func ddbEU() *ddb { return newDDB("eu", cfg.ddbEU, "eu-west-1") }

// physical returns the DynamoDB table name of db.table in the region, read
// from the region's catalog entry (each table incarnation has its own
// physical name, "<prefix><db>.<table>.<id>"; entries of older builds have
// none).
func (d *ddb) physical(ctx context.Context, db, table string) (string, error) {
	out, err := d.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(cfg.prefix + "__catalog"), ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{"_pk": &ddbtypes.AttributeValueMemberS{Value: "table#" + db + "." + table}},
	})
	if err != nil {
		return "", err
	}
	def, ok := out.Item["def"].(*ddbtypes.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("%s: no catalog entry for %s.%s", d.name, db, table)
	}
	var e struct {
		PhysicalName string `json:"physical_name"`
	}
	if err := json.Unmarshal([]byte(def.Value), &e); err != nil || e.PhysicalName == "" {
		return "", fmt.Errorf("%s: catalog entry of %s.%s has no physical name (%v)", d.name, db, table, err)
	}
	return e.PhysicalName, nil
}

// tableExists reports whether a physical table exists (ACTIVE or being created).
func (d *ddb) tableExists(ctx context.Context, name string) (bool, error) {
	_, err := d.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	var nf *ddbtypes.ResourceNotFoundException
	if errors.As(err, &nf) {
		return false, nil
	}
	return err == nil, err
}

// streamPosition returns, per shard of the table's latest stream, the
// sequence number of the last record currently in the shard (shards
// without records are omitted).
func (d *ddb) streamPosition(ctx context.Context, table string) (arn string, last map[string]string, err error) {
	dt, err := d.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		return "", nil, err
	}
	if dt.Table.LatestStreamArn == nil {
		return "", nil, fmt.Errorf("table %s has no stream", table)
	}
	arn = *dt.Table.LatestStreamArn
	last = map[string]string{}
	var start *string
	for {
		ds, err := d.st.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(arn), ExclusiveStartShardId: start})
		if err != nil {
			return "", nil, err
		}
		for _, sh := range ds.StreamDescription.Shards {
			seq, err := d.lastSeq(ctx, arn, *sh.ShardId)
			if err != nil {
				return "", nil, err
			}
			if seq != "" {
				last[*sh.ShardId] = seq
			}
		}
		start = ds.StreamDescription.LastEvaluatedShardId
		if start == nil {
			return arn, last, nil
		}
	}
}

func (d *ddb) lastSeq(ctx context.Context, arn, shard string) (string, error) {
	it, err := d.st.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn: aws.String(arn), ShardId: aws.String(shard), ShardIteratorType: sttypes.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		return "", err
	}
	iter := it.ShardIterator
	last := ""
	for empty := 0; iter != nil && empty < 2; {
		out, err := d.st.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: iter})
		if err != nil {
			return "", err
		}
		if len(out.Records) == 0 {
			empty++
		} else {
			empty = 0
			last = *out.Records[len(out.Records)-1].Dynamodb.SequenceNumber
		}
		iter = out.NextShardIterator
	}
	return last, nil
}

// consumerCaughtUp reports whether consumer's checkpoints (in the region's
// checkpoints table) cover every record currently in the table's stream.
func (d *ddb) consumerCaughtUp(ctx context.Context, table, consumer string) (bool, string, error) {
	arn, last, err := d.streamPosition(ctx, table)
	if err != nil {
		return false, "", err
	}
	for shard, seq := range last {
		key := consumer + "|" + arn + "|" + shard
		out, err := d.db.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(cfg.prefix + "__checkpoints"), ConsistentRead: aws.Bool(true),
			Key: map[string]ddbtypes.AttributeValue{"_pk": &ddbtypes.AttributeValueMemberS{Value: key}},
		})
		if err != nil {
			return false, "", err
		}
		if done, ok := out.Item["done"].(*ddbtypes.AttributeValueMemberBOOL); ok && done.Value {
			continue
		}
		cp, _ := out.Item["seq"].(*ddbtypes.AttributeValueMemberS)
		if cp == nil || !seqGE(cp.Value, seq) {
			got := "<none>"
			if cp != nil {
				got = cp.Value
			}
			return false, fmt.Sprintf("%s shard %s: checkpoint %s < last record %s", consumer, shard, got, seq), nil
		}
	}
	return true, "", nil
}

func seqGE(a, b string) bool {
	x, ok1 := new(big.Int).SetString(a, 10)
	y, ok2 := new(big.Int).SetString(b, 10)
	if !ok1 || !ok2 {
		return a >= b
	}
	return x.Cmp(y) >= 0
}

// waitCaughtUp waits until the region's mvworker has checkpointed every
// record of the table's stream.
func waitCaughtUp(t testing.TB, d *ddb, db, table string) bool {
	t.Helper()
	return eventually(t, cfg.converge, d.name+": mvworker caught up on "+db+"."+table, func() (bool, string) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		name, err := d.physical(ctx, db, table)
		if err != nil {
			return false, err.Error()
		}
		ok, why, err := d.consumerCaughtUp(ctx, name, "mvworker")
		if err != nil {
			return false, err.Error()
		}
		return ok, why
	})
}
