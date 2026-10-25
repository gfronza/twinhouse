// Package awsx builds AWS SDK clients for DynamoDB and DynamoDB Streams,
// honouring a custom endpoint (DynamoDB Local).
package awsx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/smithy-go"

	"dynamodb-eventstream/internal/cherr"
)

// DefaultHTTPTimeout bounds how long an SDK call waits for the response
// headers of one attempt, so a stalled endpoint cannot hang a background
// loop (the SDK retries the attempt). DDB_HTTP_TIMEOUT (a Go duration)
// overrides it; 0 disables the bound.
const DefaultHTTPTimeout = 30 * time.Second

// Clients bundles the clients for one region.
type Clients struct {
	Region   string
	DynamoDB *dynamodb.Client
	Streams  *dynamodbstreams.Client
}

// HTTPTimeout returns the per-attempt response-header timeout: DDB_HTTP_TIMEOUT
// when set, DefaultHTTPTimeout otherwise.
func HTTPTimeout() (time.Duration, error) {
	v := os.Getenv("DDB_HTTP_TIMEOUT")
	if v == "" {
		return DefaultHTTPTimeout, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("DDB_HTTP_TIMEOUT: invalid duration %q", v)
	}
	return d, nil
}

// New creates clients for region. When endpoint is non-empty (DynamoDB
// Local) static dummy credentials are used and the DynamoDB response CRC32
// validation is disabled (DynamoDB Local's checksums are unreliable).
func New(ctx context.Context, region, endpoint string) (*Clients, error) {
	timeout, err := HTTPTimeout()
	if err != nil {
		return nil, err
	}
	opts := []func(*awscfg.LoadOptions) error{awscfg.WithRegion(region)}
	if timeout > 0 {
		opts = append(opts, awscfg.WithHTTPClient(awshttp.NewBuildableClient().
			WithTransportOptions(func(t *http.Transport) { t.ResponseHeaderTimeout = timeout })))
	}
	if endpoint != "" {
		opts = append(opts, awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("local", "local", "")))
	}
	cfg, err := awscfg.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			// DynamoDB Local sends X-Amz-Crc32 values that do not match some
			// response bodies (e.g. "0"), which makes the SDK log "failed to
			// close HTTP response body ... response did not match expected
			// checksum" on every such call. TCP/TLS already protects the
			// local hop; real AWS endpoints keep the validation.
			o.DisableValidateResponseChecksum = true
		}
	})
	st := dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	return &Clients{Region: region, DynamoDB: ddb, Streams: st}, nil
}

// permanentAPIErrors are DynamoDB/AWS error codes that retrying cannot fix:
// invalid requests (e.g. a TABLE_PREFIX DynamoDB rejects) and missing or
// wrong credentials/permissions.
var permanentAPIErrors = map[string]bool{
	"ValidationException":                 true,
	"AccessDeniedException":               true,
	"UnrecognizedClientException":         true,
	"InvalidSignatureException":           true,
	"MissingAuthenticationToken":          true,
	"MissingAuthenticationTokenException": true,
	"IncompleteSignatureException":        true,
	"InvalidClientTokenId":                true,
}

// PermanentError reports whether a startup error will not go away by
// retrying. Connection errors, DNS failures, throttling and 5xx stay
// retryable (DynamoDB may start after the process, e.g. under compose).
func PermanentError(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) && permanentAPIErrors[ae.ErrorCode()] {
		return true
	}
	// Layout mismatches the storage layer reports (an existing table with a
	// different key schema or stream view, a rejected replica setup).
	return cherr.Is(err, cherr.BadArguments)
}

// retryableCancelReasons are TransactionCanceledException reason codes a
// retry can clear (contention and throttling).
var retryableCancelReasons = map[string]bool{
	"TransactionConflict":           true,
	"ThrottlingError":               true,
	"ProvisionedThroughputExceeded": true,
	"RequestLimitExceeded":          true,
}

// DeterministicCancel reports whether err is a canceled transaction
// (TransactionCanceledException) that retrying cannot fix: at least one
// reason is ValidationError (e.g. "Item size to update has exceeded the
// maximum allowed size", the transactional form of a ValidationException)
// and none is a retryable one (contention, throttling). Cancellations made
// only of None/ConditionalCheckFailed reasons are not deterministic in
// this sense: callers handle condition failures themselves. reason is the
// message of the first ValidationError (its code when it has none).
func DeterministicCancel(err error) (reason string, ok bool) {
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) {
		return "", false
	}
	for _, r := range tce.CancellationReasons {
		code := aws.ToString(r.Code)
		if retryableCancelReasons[code] {
			return "", false
		}
		if code == "ValidationError" && !ok {
			ok = true
			reason = aws.ToString(r.Message)
			if reason == "" {
				reason = code
			}
		}
	}
	return reason, ok
}
