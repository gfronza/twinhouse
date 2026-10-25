package awsx

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"dynamodb-eventstream/internal/cherr"
)

func TestCustomEndpointDisablesResponseChecksum(t *testing.T) {
	ctx := context.Background()
	local, err := New(ctx, "us-east-1", "http://localhost:8000")
	if err != nil {
		t.Fatal(err)
	}
	o := local.DynamoDB.Options()
	if !o.DisableValidateResponseChecksum {
		t.Error("DynamoDB Local: response CRC32 validation should be disabled")
	}
	if o.BaseEndpoint == nil || *o.BaseEndpoint != "http://localhost:8000" {
		t.Errorf("BaseEndpoint = %v", o.BaseEndpoint)
	}
	if so := local.Streams.Options(); so.BaseEndpoint == nil || *so.BaseEndpoint != "http://localhost:8000" {
		t.Errorf("streams BaseEndpoint = %v", so.BaseEndpoint)
	}

	t.Setenv("AWS_ACCESS_KEY_ID", "x")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "y")
	aws, err := New(ctx, "us-east-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if aws.DynamoDB.Options().DisableValidateResponseChecksum {
		t.Error("real AWS: response CRC32 validation must stay enabled")
	}
	if aws.DynamoDB.Options().BaseEndpoint != nil {
		t.Error("real AWS: no BaseEndpoint expected")
	}
}

func TestHTTPTimeout(t *testing.T) {
	t.Setenv("DDB_HTTP_TIMEOUT", "")
	if d, err := HTTPTimeout(); err != nil || d != DefaultHTTPTimeout {
		t.Fatalf("default: %v %v", d, err)
	}
	t.Setenv("DDB_HTTP_TIMEOUT", "5s")
	if d, err := HTTPTimeout(); err != nil || d != 5*time.Second {
		t.Fatalf("5s: %v %v", d, err)
	}
	t.Setenv("DDB_HTTP_TIMEOUT", "0")
	if d, err := HTTPTimeout(); err != nil || d != 0 {
		t.Fatalf("0: %v %v", d, err)
	}
	t.Setenv("DDB_HTTP_TIMEOUT", "soon")
	if _, err := HTTPTimeout(); err == nil {
		t.Fatal("invalid duration accepted")
	}
	if _, err := New(context.Background(), "us-east-1", "http://localhost:8000"); err == nil {
		t.Fatal("New with an invalid DDB_HTTP_TIMEOUT succeeded")
	}
}

func TestPermanentError(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{&smithy.GenericAPIError{Code: "ValidationException"}, true},
		{fmt.Errorf("wrapped: %w", &smithy.GenericAPIError{Code: "UnrecognizedClientException"}), true},
		{&smithy.GenericAPIError{Code: "ThrottlingException"}, false},
		{cherr.New(cherr.BadArguments, "layout mismatch"), true},
		{errors.New("connection refused"), false},
	} {
		if got := PermanentError(c.err); got != c.want {
			t.Errorf("PermanentError(%v) = %v", c.err, got)
		}
	}
}

func TestDeterministicCancel(t *testing.T) {
	tce := func(codes ...string) error {
		e := &types.TransactionCanceledException{Message: aws.String("Transaction cancelled")}
		for _, c := range codes {
			r := types.CancellationReason{Code: aws.String(c)}
			if c == "ValidationError" {
				r.Message = aws.String("Item size to update has exceeded the maximum allowed size")
			}
			e.CancellationReasons = append(e.CancellationReasons, r)
		}
		return e
	}
	wrap := func(err error) error {
		return cherr.Wrap(cherr.StdException, &smithy.OperationError{ServiceID: "DynamoDB", OperationName: "TransactWriteItems", Err: err}, "merge")
	}
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"item too large", tce("None", "ValidationError"), true},
		{"item too large, wrapped", wrap(tce("None", "ValidationError")), true},
		{"validation with condition", tce("ConditionalCheckFailed", "ValidationError"), true},
		{"validation with conflict", tce("ValidationError", "TransactionConflict"), false},
		{"validation with throttling", tce("ValidationError", "ThrottlingError"), false},
		{"validation with capacity", wrap(tce("ProvisionedThroughputExceeded", "ValidationError")), false},
		{"validation with request limit", tce("ValidationError", "RequestLimitExceeded"), false},
		{"conflict", tce("None", "TransactionConflict"), false},
		{"condition", tce("ConditionalCheckFailed", "None"), false},
		{"plain validation", &smithy.GenericAPIError{Code: "ValidationException"}, false},
		{"other", errors.New("boom"), false},
		{"nil", nil, false},
	} {
		reason, got := DeterministicCancel(c.err)
		if got != c.want {
			t.Errorf("%s: DeterministicCancel = %v, want %v", c.name, got, c.want)
		}
		if got && reason != "Item size to update has exceeded the maximum allowed size" {
			t.Errorf("%s: reason = %q", c.name, reason)
		}
	}
}
