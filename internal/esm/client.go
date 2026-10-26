package esm

import (
	"context"
	"net/http"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"

	"dynamodb-eventstream/internal/awsx"
)

// NewClient builds the Lambda client of region with the default credential
// chain and the same per-attempt response-header timeout as the DynamoDB
// clients (awsx.HTTPTimeout).
func NewClient(ctx context.Context, region string) (*lambda.Client, error) {
	timeout, err := awsx.HTTPTimeout()
	if err != nil {
		return nil, err
	}
	opts := []func(*awscfg.LoadOptions) error{awscfg.WithRegion(region)}
	if timeout > 0 {
		opts = append(opts, awscfg.WithHTTPClient(awshttp.NewBuildableClient().
			WithTransportOptions(func(t *http.Transport) { t.ResponseHeaderTimeout = timeout })))
	}
	cfg, err := awscfg.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return lambda.NewFromConfig(cfg), nil
}
