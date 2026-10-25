package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/smithy-go"

	"dynamodb-eventstream/internal/cherr"
)

func TestRetryStopsOnPermanentErrors(t *testing.T) {
	old := retryInitialDelay
	retryInitialDelay = time.Millisecond
	t.Cleanup(func() { retryInitialDelay = old })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	validation := &smithy.GenericAPIError{Code: "ValidationException", Message: "Invalid table/index name"}
	for name, perm := range map[string]error{
		"validation (wrapped like the store does)": cherr.Wrap(cherr.BadArguments, validation, "cannot describe DynamoDB table"),
		"access denied":   fmt.Errorf("op: %w", &smithy.GenericAPIError{Code: "AccessDeniedException"}),
		"bad credentials": &smithy.GenericAPIError{Code: "UnrecognizedClientException"},
		"layout mismatch": cherr.New(cherr.BadArguments, "DynamoDB table x exists with an incompatible key schema"),
	} {
		calls := 0
		err := retry(context.Background(), log, "ensure system tables", func() error { calls++; return perm })
		if err == nil || calls != 1 || !errors.Is(err, perm) {
			t.Errorf("%s: calls %d err %v", name, calls, err)
		}
	}

	// Transient errors are retried until they clear.
	for name, transient := range map[string]error{
		"connection refused": errors.New(`Post "http://localhost:8000/": dial tcp [::1]:8000: connect: connection refused`),
		"throttling":         &smithy.GenericAPIError{Code: "ThrottlingException"},
		"unavailable":        cherr.Wrap(cherr.Unavailable, errors.New("503"), "cannot describe"),
	} {
		calls := 0
		err := retry(context.Background(), log, "load catalog", func() error {
			if calls++; calls < 3 {
				return transient
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Errorf("%s: calls %d err %v", name, calls, err)
		}
	}
}
