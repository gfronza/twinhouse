// Package esm manages the Lambda event-source mappings (ESMs) of the
// serverless deployment (DESIGN.md §2, §5): the reconciler wires every
// source table's DynamoDB stream to the stream-worker Lambda (and, with
// GLOBAL_MODE=aws, every ReplacingMergeTree table's stream to the
// replicator Lambda), the {prefix}__catalog stream to itself, and removes
// the mappings of dropped tables.
//
// Manager is the idempotent primitive (Ensure/Remove one mapping);
// Consumers decides, per catalog table, which functions consume its stream
// and implements the reconciler's StreamConsumers hook.
package esm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// LambdaAPI is the subset of the Lambda API the manager uses (the SDK
// client implements it; tests use a fake).
type LambdaAPI interface {
	ListEventSourceMappings(ctx context.Context, in *lambda.ListEventSourceMappingsInput, optFns ...func(*lambda.Options)) (*lambda.ListEventSourceMappingsOutput, error)
	CreateEventSourceMapping(ctx context.Context, in *lambda.CreateEventSourceMappingInput, optFns ...func(*lambda.Options)) (*lambda.CreateEventSourceMappingOutput, error)
	UpdateEventSourceMapping(ctx context.Context, in *lambda.UpdateEventSourceMappingInput, optFns ...func(*lambda.Options)) (*lambda.UpdateEventSourceMappingOutput, error)
	DeleteEventSourceMapping(ctx context.Context, in *lambda.DeleteEventSourceMappingInput, optFns ...func(*lambda.Options)) (*lambda.DeleteEventSourceMappingOutput, error)
}

// Options are the settings of one event-source mapping. The zero value is
// the deployment's contract: StartingPosition TRIM_HORIZON (a table's
// stream is read from the start, exactly-once markers make replays
// no-ops), BatchSize DefaultBatchSize, FunctionResponseTypes
// ReportBatchItemFailures (the worker reports the first failed record so
// Lambda retries from there), BisectBatchOnFunctionError false and
// MaximumRetryAttempts -1 (retry until the record expires from the stream).
type Options struct {
	// BatchSize is the mapping's BatchSize (0: DefaultBatchSize).
	BatchSize int32
	// StartingPosition applies to a mapping being created (it cannot be
	// changed later); "" is TRIM_HORIZON.
	StartingPosition types.EventSourcePosition
	// MaximumRetryAttempts is the mapping's retry limit; nil is -1 (retry
	// until the record expires, as the exactly-once markers allow).
	MaximumRetryAttempts *int32
}

// DefaultBatchSize is the BatchSize of a mapping when Options leaves it 0.
const DefaultBatchSize = 100

func (o Options) batchSize() int32 {
	if o.BatchSize <= 0 {
		return DefaultBatchSize
	}
	return o.BatchSize
}

func (o Options) startingPosition() types.EventSourcePosition {
	if o.StartingPosition == "" {
		return types.EventSourcePositionTrimHorizon
	}
	return o.StartingPosition
}

func (o Options) maximumRetryAttempts() int32 {
	if o.MaximumRetryAttempts == nil {
		return -1
	}
	return *o.MaximumRetryAttempts
}

// ErrInTransition reports that a mapping is being created, updated or
// deleted by Lambda (or by a concurrent reconciler) and could not be
// brought to the wanted state now; the caller retries on its next round.
var ErrInTransition = errors.New("event-source mapping is in transition")

// ManagerConfig configures a Manager.
type ManagerConfig struct {
	API    LambdaAPI
	Logger *slog.Logger
	// RetryAttempts bounds the retries of a throttled call
	// (TooManyRequestsException; default 5) and RetryBase is the first
	// backoff (default 200ms, doubled with jitter up to RetryMax, default 5s).
	RetryAttempts int
	RetryBase     time.Duration
	RetryMax      time.Duration
}

// Manager ensures and removes event-source mappings idempotently.
type Manager struct {
	api LambdaAPI
	log *slog.Logger
	cfg ManagerConfig
}

// NewManager creates a Manager.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if cfg.API == nil {
		return nil, errors.New("esm: API is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RetryAttempts <= 0 {
		cfg.RetryAttempts = 5
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = 200 * time.Millisecond
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 5 * time.Second
	}
	return &Manager{api: cfg.API, log: cfg.Logger.With("component", "esm"), cfg: cfg}, nil
}

// Ensure makes functionARN (a Lambda function ARN or name) consume
// streamARN with the given Options: it creates the mapping when it does not
// exist, enables it when it is disabled and updates it when its settings
// differ; a mapping that already matches costs one ListEventSourceMappings
// call. A concurrent creation (ResourceConflictException) is tolerated;
// a mapping in transition yields ErrInTransition.
func (m *Manager) Ensure(ctx context.Context, streamARN, functionARN string, o Options) error {
	if streamARN == "" || functionARN == "" {
		return errors.New("esm: stream ARN and function ARN are required")
	}
	existing, err := m.list(ctx, streamARN, functionARN)
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		err := m.call(ctx, func() error {
			_, err := m.api.CreateEventSourceMapping(ctx, &lambda.CreateEventSourceMappingInput{
				EventSourceArn:             aws.String(streamARN),
				FunctionName:               aws.String(functionARN),
				Enabled:                    aws.Bool(true),
				StartingPosition:           o.startingPosition(),
				BatchSize:                  aws.Int32(o.batchSize()),
				BisectBatchOnFunctionError: aws.Bool(false),
				MaximumRetryAttempts:       aws.Int32(o.maximumRetryAttempts()),
				FunctionResponseTypes:      []types.FunctionResponseType{types.FunctionResponseTypeReportBatchItemFailures},
			})
			return err
		})
		if err == nil {
			m.log.Info("event-source mapping created", "stream", streamARN, "function", functionARN, "batch_size", o.batchSize())
			return nil
		}
		var conflict *types.ResourceConflictException
		if !errors.As(err, &conflict) {
			return fmt.Errorf("esm: create mapping %s -> %s: %w", streamARN, functionARN, err)
		}
		// Another reconciler created it meanwhile: converge on what exists.
		m.log.Debug("event-source mapping created concurrently", "stream", streamARN, "function", functionARN)
		var lerr error
		if existing, lerr = m.list(ctx, streamARN, functionARN); lerr != nil {
			return lerr
		}
		if len(existing) == 0 {
			return fmt.Errorf("esm: create mapping %s -> %s: %w", streamARN, functionARN, err)
		}
	}
	// Lambda allows one mapping per (stream, function); extra ones can
	// only come from manual changes. Keep the first, remove the rest.
	for _, extra := range existing[1:] {
		if err := m.delete(ctx, extra); err != nil {
			return err
		}
	}
	return m.converge(ctx, existing[0], o)
}

// converge enables and/or updates an existing mapping so it matches o.
func (m *Manager) converge(ctx context.Context, cur types.EventSourceMappingConfiguration, o Options) error {
	uuid := aws.ToString(cur.UUID)
	state := aws.ToString(cur.State)
	settingsDiffer := aws.ToInt32(cur.BatchSize) != o.batchSize() ||
		aws.ToBool(cur.BisectBatchOnFunctionError) ||
		retryAttempts(cur.MaximumRetryAttempts) != o.maximumRetryAttempts() ||
		!slices.Contains(cur.FunctionResponseTypes, types.FunctionResponseTypeReportBatchItemFailures)
	var enable bool
	switch state {
	case "Enabled", "Enabling", "Creating":
		if !settingsDiffer {
			return nil
		}
		if state != "Enabled" {
			return fmt.Errorf("esm: mapping %s is %s and its settings differ: %w", uuid, state, ErrInTransition)
		}
	case "Disabled":
		enable = true
	case "Disabling", "Updating", "Deleting":
		return fmt.Errorf("esm: mapping %s is %s: %w", uuid, state, ErrInTransition)
	default:
		// An unknown (new) state: treat like Enabled when the settings match.
		if !settingsDiffer {
			return nil
		}
		return fmt.Errorf("esm: mapping %s is in unknown state %q and its settings differ: %w", uuid, state, ErrInTransition)
	}
	err := m.call(ctx, func() error {
		_, err := m.api.UpdateEventSourceMapping(ctx, &lambda.UpdateEventSourceMappingInput{
			UUID:                       aws.String(uuid),
			Enabled:                    aws.Bool(true),
			BatchSize:                  aws.Int32(o.batchSize()),
			BisectBatchOnFunctionError: aws.Bool(false),
			MaximumRetryAttempts:       aws.Int32(o.maximumRetryAttempts()),
			FunctionResponseTypes:      []types.FunctionResponseType{types.FunctionResponseTypeReportBatchItemFailures},
		})
		return err
	})
	switch {
	case err == nil:
		m.log.Info("event-source mapping updated", "uuid", uuid, "stream", aws.ToString(cur.EventSourceArn),
			"function", aws.ToString(cur.FunctionArn), "enabled", enable, "settings_changed", settingsDiffer)
		return nil
	case isInUse(err):
		return fmt.Errorf("esm: update mapping %s: %w", uuid, ErrInTransition)
	case isNotFound(err):
		// Deleted meanwhile (a concurrent drop?): the next round decides.
		return fmt.Errorf("esm: update mapping %s: mapping disappeared: %w", uuid, err)
	}
	return fmt.Errorf("esm: update mapping %s: %w", uuid, err)
}

// retryAttempts normalises a mapping's MaximumRetryAttempts: Lambda reports
// -1 for "no limit", which is also what an unset value means.
func retryAttempts(v *int32) int32 {
	if v == nil {
		return -1
	}
	return *v
}

// Remove deletes the mapping(s) of streamARN -> functionARN; a missing
// mapping is not an error, one already being deleted is left alone.
func (m *Manager) Remove(ctx context.Context, streamARN, functionARN string) error {
	if streamARN == "" || functionARN == "" {
		return errors.New("esm: stream ARN and function ARN are required")
	}
	existing, err := m.list(ctx, streamARN, functionARN)
	if err != nil {
		return err
	}
	for _, e := range existing {
		if err := m.delete(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// RemoveByTable deletes every mapping of functionARN whose event source is
// a stream of the DynamoDB table physicalName (any stream ARN, so it also
// works once the table, and with it the stream ARN, is gone). It lists all
// of the function's mappings, so it costs more than Remove.
func (m *Manager) RemoveByTable(ctx context.Context, physicalName, functionARN string) error {
	if physicalName == "" || functionARN == "" {
		return errors.New("esm: table name and function ARN are required")
	}
	existing, err := m.list(ctx, "", functionARN)
	if err != nil {
		return err
	}
	for _, e := range existing {
		if TableOfStreamARN(aws.ToString(e.EventSourceArn)) != physicalName {
			continue
		}
		if err := m.delete(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// TableOfStreamARN returns the table name of a DynamoDB stream ARN
// (arn:aws:dynamodb:<region>:<acct>:table/<name>/stream/<label>), or "".
func TableOfStreamARN(arn string) string {
	_, rest, ok := strings.Cut(arn, ":table/")
	if !ok {
		return ""
	}
	name, _, ok := strings.Cut(rest, "/stream/")
	if !ok {
		return ""
	}
	return name
}

func (m *Manager) delete(ctx context.Context, e types.EventSourceMappingConfiguration) error {
	uuid := aws.ToString(e.UUID)
	if aws.ToString(e.State) == "Deleting" {
		return nil
	}
	err := m.call(ctx, func() error {
		_, err := m.api.DeleteEventSourceMapping(ctx, &lambda.DeleteEventSourceMappingInput{UUID: aws.String(uuid)})
		return err
	})
	switch {
	case err == nil:
		m.log.Info("event-source mapping deleted", "uuid", uuid, "stream", aws.ToString(e.EventSourceArn), "function", aws.ToString(e.FunctionArn))
		return nil
	case isNotFound(err):
		return nil // already gone
	case isInUse(err):
		return fmt.Errorf("esm: delete mapping %s: %w", uuid, ErrInTransition)
	}
	return fmt.Errorf("esm: delete mapping %s: %w", uuid, err)
}

// list returns the mappings of functionARN, restricted to streamARN when
// non-empty, following pagination.
func (m *Manager) list(ctx context.Context, streamARN, functionARN string) ([]types.EventSourceMappingConfiguration, error) {
	var out []types.EventSourceMappingConfiguration
	in := &lambda.ListEventSourceMappingsInput{FunctionName: aws.String(functionARN), MaxItems: aws.Int32(100)}
	if streamARN != "" {
		in.EventSourceArn = aws.String(streamARN)
	}
	for {
		var page *lambda.ListEventSourceMappingsOutput
		err := m.call(ctx, func() (err error) {
			page, err = m.api.ListEventSourceMappings(ctx, in)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("esm: list mappings of %s: %w", functionARN, err)
		}
		out = append(out, page.EventSourceMappings...)
		if page.NextMarker == nil || *page.NextMarker == "" {
			return out, nil
		}
		next := *in
		next.Marker = page.NextMarker
		in = &next
	}
}

// call runs f, retrying throttled attempts with capped exponential backoff
// (the SDK's own retries come first; this bounds longer throttling spells).
func (m *Manager) call(ctx context.Context, f func() error) error {
	delay := m.cfg.RetryBase
	for attempt := 1; ; attempt++ {
		err := f()
		if err == nil || ctx.Err() != nil || !isThrottled(err) || attempt > m.cfg.RetryAttempts {
			return err
		}
		m.log.Debug("lambda api throttled; retrying", "attempt", attempt, "in", delay, "error", err)
		t := time.NewTimer(delay + time.Duration(rand.Int64N(int64(delay)/2+1)))
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
		delay = min(2*delay, m.cfg.RetryMax)
	}
}

func isThrottled(err error) bool {
	var e *types.TooManyRequestsException
	return errors.As(err, &e)
}

func isNotFound(err error) bool {
	var e *types.ResourceNotFoundException
	return errors.As(err, &e)
}

func isInUse(err error) bool {
	var e *types.ResourceInUseException
	return errors.As(err, &e)
}
