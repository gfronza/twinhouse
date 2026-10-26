package dynamo

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// A transaction cancelled for a ValidationError is a client error (36), with
// DynamoDB's reason in the message, not STD_EXCEPTION.
func TestDDBErrorDeterministicCancel(t *testing.T) {
	tce := &types.TransactionCanceledException{Message: aws.String("Transaction cancelled"), CancellationReasons: []types.CancellationReason{
		{Code: aws.String("None")},
		{Code: aws.String("ValidationError"), Message: aws.String("Item size to update has exceeded the maximum allowed size")},
	}}
	err := ddbError(tce, "merge into %s", "db.t")
	var ce *cherr.Error
	if !errors.As(err, &ce) || ce.Code != cherr.BadArguments {
		t.Fatalf("got %v, want BAD_ARGUMENTS", err)
	}
	if !strings.Contains(err.Error(), "merge into db.t: Item size to update has exceeded") {
		t.Errorf("message %q lacks the reason", err.Error())
	}
	// Condition failures alone stay what they were (not BAD_ARGUMENTS).
	cond := &types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: aws.String("ConditionalCheckFailed")}}}
	if errors.As(ddbError(cond, "x"), &ce) && ce.Code == cherr.BadArguments {
		t.Errorf("condition-only cancellation became BAD_ARGUMENTS")
	}
}
