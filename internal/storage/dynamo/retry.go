package dynamo

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"github.com/gfronza/twinhouse/internal/awsx"
	"github.com/gfronza/twinhouse/internal/cherr"
)

const (
	maxAttempts = 12
	backoffBase = 20 * time.Millisecond
	backoffMax  = 2 * time.Second
)

// sleepBackoff waits a full-jitter exponential backoff for attempt (0-based).
func sleepBackoff(ctx context.Context, attempt int) error {
	d := backoffBase << min(attempt, 10)
	if d > backoffMax {
		d = backoffMax
	}
	d = time.Duration(rand.Int64N(int64(d)) + 1)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// withRetry runs fn until it succeeds, fails permanently, or attempts run out.
// Retryable: throttling, transaction conflicts, transient server errors.
func withRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err = fn(); err == nil || !isRetryable(err) {
			return err
		}
		if serr := sleepBackoff(ctx, attempt); serr != nil {
			return serr
		}
	}
	return err
}

// isRetryable classifies transient DynamoDB errors.
func isRetryable(err error) bool {
	var tce *types.TransactionCanceledException
	if errors.As(err, &tce) {
		for _, r := range tce.CancellationReasons {
			switch aws.ToString(r.Code) {
			case "TransactionConflict", "ThrottlingError", "ProvisionedThroughputExceeded", "RequestLimitExceeded":
				return true
			}
		}
		return false
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "ProvisionedThroughputExceededException", "ThrottlingException", "RequestLimitExceeded",
			"TransactionConflictException", "TransactionInProgressException", "InternalServerError",
			"InternalFailure", "ServiceUnavailable", "LimitExceededException":
			return true
		}
	}
	return false
}

// cancelReasons returns the cancellation reason codes of a canceled
// transaction (nil when err is not a TransactionCanceledException).
func cancelReasons(err error) []string {
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) {
		return nil
	}
	out := make([]string, len(tce.CancellationReasons))
	for i, r := range tce.CancellationReasons {
		out[i] = aws.ToString(r.Code)
	}
	return out
}

func isConditionFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

func isNotFound(err error) bool {
	var nf *types.ResourceNotFoundException
	return errors.As(err, &nf)
}

func isInUse(err error) bool {
	var iu *types.ResourceInUseException
	return errors.As(err, &iu)
}

// ddbError converts a DynamoDB error into a ClickHouse-style error.
func ddbError(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	var ce *cherr.Error
	if errors.As(err, &ce) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return cherr.Wrap(cherr.Timeout, err, format, args...)
	}
	if isNotFound(err) {
		return cherr.Wrap(cherr.UnknownTable, err, format, args...)
	}
	if isRetryable(err) {
		return cherr.Wrap(cherr.Unavailable, err, format, args...)
	}
	// A transaction cancelled for a ValidationError (e.g. an item over
	// 400 KB) is the transactional form of a ValidationException: report it
	// as BAD_ARGUMENTS with DynamoDB's reason in the message.
	if reason, ok := awsx.DeterministicCancel(err); ok {
		return cherr.Wrap(cherr.BadArguments, err, format+": %s", append(args, reason)...)
	}
	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() == "ValidationException" {
		return cherr.Wrap(cherr.BadArguments, err, format, args...)
	}
	return cherr.Wrap(cherr.StdException, err, format, args...)
}
