package dynamo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"dynamodb-eventstream/internal/awsx"
	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/storage"
)

// Integration tests run against DynamoDB Local when DYNAMODB_TEST_ENDPOINT is
// set (e.g. http://localhost:18100) and are skipped otherwise.

var prefixSeq atomic.Int64

type env struct {
	t       *testing.T
	ctx     context.Context
	clients *awsx.Clients
	store   *Store
	cat     catalog.Catalog
	prefix  string
}

func newEnv(t *testing.T) *env {
	return newEnvWith(t, func(o *Options) {}, nil)
}

func newEnvWith(t *testing.T, opt func(*Options), wrap func(ddbAPI) ddbAPI) *env {
	t.Helper()
	ep := os.Getenv("DYNAMODB_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("DYNAMODB_TEST_ENDPOINT not set")
	}
	ctx := context.Background()
	c, err := awsx.New(ctx, "us-east-1", ep)
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("it%d_%d_", time.Now().UnixMilli()%1e8, prefixSeq.Add(1))
	opts := Options{Prefix: prefix, Region: "us-east-1"}
	opt(&opts)
	var api ddbAPI = c.DynamoDB
	if wrap != nil {
		api = wrap(api)
	}
	s, err := newStoreWithAPI(api, c.Region, opts)
	if err != nil {
		t.Fatal(err)
	}
	s.impl.tableWait = time.Minute
	e := &env{t: t, ctx: ctx, clients: c, store: s, prefix: prefix}
	e.cat = catalog.New(nil, catalog.Options{PhysicalName: s.PhysicalName})
	if err := s.EnsureSystemTables(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.dropAll() })
	return e
}

// dropAll deletes every table created under the env's prefix.
func (e *env) dropAll() {
	var start *string
	for {
		out, err := e.clients.DynamoDB.ListTables(e.ctx, &dynamodb.ListTablesInput{ExclusiveStartTableName: start})
		if err != nil {
			e.t.Logf("cleanup: %v", err)
			return
		}
		for _, n := range out.TableNames {
			if strings.HasPrefix(n, e.prefix) {
				_, _ = e.clients.DynamoDB.DeleteTable(e.ctx, &dynamodb.DeleteTableInput{TableName: aws.String(n)})
			}
		}
		if out.LastEvaluatedTableName == nil {
			return
		}
		start = out.LastEvaluatedTableName
	}
}

func (e *env) table(tbl *catalog.Table) *catalog.Table {
	e.t.Helper()
	if err := e.cat.CreateTable(e.ctx, tbl, false); err != nil {
		e.t.Fatal(err)
	}
	stored, err := e.cat.Table(tbl.Database, tbl.Name)
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.store.EnsureTable(e.ctx, stored); err != nil {
		e.t.Fatal(err)
	}
	return stored
}

func (e *env) insert(t *catalog.Table, rows ...storage.Row) *storage.InsertResult {
	e.t.Helper()
	res, err := e.store.Insert(e.ctx, t, rows)
	if err != nil {
		e.t.Fatal(err)
	}
	return res
}

func (e *env) merge(t *catalog.Table, rows ...storage.MergeRow) {
	e.t.Helper()
	if err := e.store.Merge(e.ctx, t, rows); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) scan(t *catalog.Table, req storage.ScanRequest) []storage.Row {
	e.t.Helper()
	rows, err := e.scanErr(t, req)
	if err != nil {
		e.t.Fatal(err)
	}
	return rows
}

func (e *env) scanErr(t *catalog.Table, req storage.ScanRequest) ([]storage.Row, error) {
	req.Consistent = true
	it, err := e.store.Scan(e.ctx, t, req)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var rows []storage.Row
	for {
		r, err := it.Next(e.ctx)
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
}

func (e *env) all(t *catalog.Table) []storage.Row { return e.scan(t, storage.ScanRequest{}) }

// itemCount counts the items of a physical table.
func (e *env) itemCount(table string) int {
	e.t.Helper()
	n := 0
	in := &dynamodb.ScanInput{TableName: aws.String(table), Select: "COUNT", ConsistentRead: aws.Bool(true)}
	for {
		out, err := e.clients.DynamoDB.Scan(e.ctx, in)
		if err != nil {
			e.t.Fatal(err)
		}
		n += int(out.Count)
		if out.LastEvaluatedKey == nil {
			return n
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func rowStrings(rows []storage.Row) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%v", []any(r))
	}
	sort.Strings(out)
	return out
}

func eqRows(t *testing.T, got, want []storage.Row) {
	t.Helper()
	g, w := rowStrings(got), rowStrings(want)
	if len(g) != len(w) {
		t.Fatalf("got %d rows, want %d\ngot:  %v\nwant: %v", len(g), len(w), g, w)
	}
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("row mismatch\ngot:  %v\nwant: %v", g, w)
		}
	}
}
