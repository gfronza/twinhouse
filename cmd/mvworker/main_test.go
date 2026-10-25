package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/smithy-go"
)

// The startup retry stops at once on permanent errors (bad credentials,
// invalid requests) and keeps retrying transient ones.
func TestRetryStopsOnPermanentError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	err := retry(ctx, "op", func(context.Context) error {
		calls++
		return &smithy.GenericAPIError{Code: "UnrecognizedClientException"}
	})
	if err == nil || calls != 1 {
		t.Fatalf("permanent: err=%v calls=%d", err, calls)
	}
	calls = 0
	err = retry(ctx, "op", func(context.Context) error {
		if calls++; calls < 3 {
			return errors.New("connection refused")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("transient: err=%v calls=%d", err, calls)
	}
}
