package mvworker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"dynamodb-eventstream/internal/awsx"
	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/storage"
	"dynamodb-eventstream/internal/streams"
)

// Poison policies (Config.PoisonPolicy, MV_POISON_POLICY).
const (
	// PoisonDeadLetter: a record whose view application keeps failing with
	// a deterministic engine error for PoisonAfter is written to the
	// dead-letter table and skipped, so its shard keeps advancing. This is
	// what the writer region does too: the INSERT that produced the record
	// got the error and the view output was not applied (DESIGN.md §5).
	PoisonDeadLetter = "deadletter"
	// PoisonBlock retries such a record until it succeeds or an operator
	// intervenes; the shard does not advance (records still pending when
	// the stream trims them after 24h are lost).
	PoisonBlock = "block"
)

// Defaults of the poison handling.
const (
	defaultPoisonAfter      = 30 * time.Second
	defaultCatalogMinReload = time.Second
)

// DeadLetterTableName returns the regional dead-letter table of a store
// whose checkpoints table is checkpoints ({prefix}__checkpoints →
// {prefix}__dead_letters).
func DeadLetterTableName(checkpoints string) string {
	return strings.TrimSuffix(checkpoints, "__checkpoints") + "__dead_letters"
}

// DeadLetterAPI is the part of *dynamodb.Client the dead-letter table needs.
type DeadLetterAPI interface {
	streams.TableAdmin
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// deadLetter is one record set aside.
type deadLetter struct {
	Table  string // source table (db.name)
	View   string // view whose application failed ("" = decoding or every view)
	Seq    string // stream sequence number (or replication source)
	RowID  string
	Origin string // "stream", "lambda" or "replicator"
	Err    error
	Image  map[string]ddbtypes.AttributeValue
}

// deadLetterSink stores dead letters.
type deadLetterSink interface {
	put(ctx context.Context, d deadLetter) error
}

// dynamoDeadLetters writes dead letters to a regional DynamoDB table
// (_pk S, created on first use).
type dynamoDeadLetters struct {
	ddb   DeadLetterAPI
	table string

	mu      sync.Mutex
	ensured bool
}

func (s *dynamoDeadLetters) ensure(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ensured {
		return nil
	}
	if err := streams.EnsureCheckpointTable(ctx, s.ddb, s.table); err != nil {
		return fmt.Errorf("create dead-letter table %s: %w", s.table, err)
	}
	s.ensured = true
	return nil
}

// maxErrorText bounds the stored error message.
const maxErrorText = 4096

func (s *dynamoDeadLetters) put(ctx context.Context, d deadLetter) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	ce := cherr.As(d.Err)
	msg := d.Err.Error()
	if len(msg) > maxErrorText {
		msg = msg[:maxErrorText]
	}
	item := map[string]ddbtypes.AttributeValue{
		"_pk":        &ddbtypes.AttributeValueMemberS{Value: d.Table + "|" + d.Seq + "|" + d.View},
		"table":      &ddbtypes.AttributeValueMemberS{Value: d.Table},
		"seq":        &ddbtypes.AttributeValueMemberS{Value: d.Seq},
		"origin":     &ddbtypes.AttributeValueMemberS{Value: d.Origin},
		"error_code": &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(ce.Code)},
		"error_name": &ddbtypes.AttributeValueMemberS{Value: ce.Name()},
		"error":      &ddbtypes.AttributeValueMemberS{Value: msg},
		"at":         &ddbtypes.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339Nano)},
	}
	if d.View != "" {
		item["view"] = &ddbtypes.AttributeValueMemberS{Value: d.View}
	}
	if d.RowID != "" {
		item["row_id"] = &ddbtypes.AttributeValueMemberS{Value: d.RowID}
	}
	if d.Image != nil {
		item["image"] = &ddbtypes.AttributeValueMemberM{Value: d.Image}
	}
	_, err := s.ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.table), Item: item})
	if err != nil && d.Image != nil && hasAPICode(err, "ValidationException") {
		// The image plus metadata exceeds the item size limit: keep the
		// metadata (the source row itself is still in its table).
		delete(item, "image")
		item["image_dropped"] = &ddbtypes.AttributeValueMemberBOOL{Value: true}
		_, err = s.ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(s.table), Item: item})
	}
	if err != nil {
		return fmt.Errorf("write dead letter to %s: %w", s.table, err)
	}
	return nil
}

func hasAPICode(err error, code string) bool {
	var ae smithy.APIError
	return errors.As(err, &ae) && ae.ErrorCode() == code
}

// isTransient reports whether a view-application error may go away by
// itself (throttling, network, backend unavailability, a table still being
// created, cancellation), so the record must be retried rather than treated
// as poison. Errors that cannot be classified count as transient: a record
// is only ever set aside for an error known to be deterministic, i.e. an
// engine error about the data or the view (CANNOT_PARSE_*, TYPE_MISMATCH,
// UNKNOWN_TABLE for a dropped target, ...) or a DynamoDB ValidationException
// (e.g. an item over 400 KB), including its transactional form, a
// TransactionCanceledException with a ValidationError reason and no
// retryable one (awsx.DeterministicCancel).
//
// pure marks errors of operations without I/O (decoding a stream image):
// those are deterministic unless they say otherwise.
func isTransient(err error, pure bool) bool {
	if err == nil {
		return false
	}
	var de *decodeError
	if errors.As(err, &de) {
		pure = true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if _, ok := awsx.DeterministicCancel(err); ok {
		return false
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode() != "ValidationException"
	}
	var oe *smithy.OperationError
	var ne net.Error
	if errors.As(err, &oe) || errors.As(err, &ne) {
		return true
	}
	// A missing DynamoDB table (e.g. a view target the reconciler is still
	// creating) surfaces as UNKNOWN_TABLE; the engine may have flattened
	// the cause into the message.
	if strings.Contains(err.Error(), "ResourceNotFoundException") {
		return true
	}
	var ce *cherr.Error
	if !errors.As(err, &ce) {
		return !pure
	}
	switch ce.Code {
	case cherr.Unavailable, cherr.Timeout, cherr.QueryWasCancelled:
		return true
	case cherr.StdException:
		return !pure
	}
	return false
}

// decodeError marks a failure to decode a stream image (no I/O involved).
type decodeError struct{ err error }

func (e *decodeError) Error() string { return e.err.Error() }
func (e *decodeError) Unwrap() error { return e.err }

// isolation is the state of a batch being applied record by record after a
// deterministic failure (kept across retries of the batch).
type isolation struct {
	since time.Time       // first deterministic failure
	done  map[string]bool // record seq, or seq+"\x00"+view, fully handled
}

func newIsolation(now time.Time) *isolation {
	return &isolation{since: now, done: map[string]bool{}}
}

// poisonRec is a record (or one of its views) that failed deterministically.
type poisonRec struct {
	c     change
	rowID string
	view  string // "" = the record as a whole
	err   error
}

// handle applies changes like process, but a deterministic failure does not
// hold the other records back: the batch is then applied record by record
// (and view by view when the engine supports it), and records that keep
// failing for PoisonAfter are dead-lettered (PoisonDeadLetter) so that the
// shard advances. *scratch carries the isolation state across retries of
// the same batch (streams.Batch.Scratch).
func (p *processor) handle(ctx context.Context, physical string, changes []change, scratch *any) error {
	st, _ := (*scratch).(*isolation)
	if st == nil {
		err := p.processOnce(ctx, physical, changes)
		if err == nil {
			return nil
		}
		p.errs.Add(1)
		if isTransient(err, false) {
			return err
		}
		st = newIsolation(p.clock())
		*scratch = st
		p.log.Warn("deterministic materialized view error; applying the batch record by record to isolate it",
			"table", physical, "records", len(changes), "err", err)
	}
	err := p.processIsolated(ctx, physical, changes, st)
	if err != nil {
		p.errs.Add(1)
	}
	return err
}

func (p *processor) processIsolated(ctx context.Context, physical string, changes []change, st *isolation) error {
	var newestWrite time.Time
	for _, c := range changes {
		if (c.EventName == eventInsert || c.EventName == eventModify) && c.WriteTime.After(newestWrite) {
			newestWrite = c.WriteTime
		}
	}
	if err := p.ensureFresh(ctx, newestWrite); err != nil {
		return err
	}
	t, err := p.table(ctx, physical)
	if err != nil {
		return err
	}
	if t == nil {
		p.skipped.Add(uint64(len(changes)))
		return nil
	}
	views := p.cat.ViewsFrom(t.Database, t.Name)
	if len(views) == 0 {
		p.skipped.Add(uint64(len(changes)))
		return nil
	}
	la, perView := p.app.(viewListApplier)
	var poison []poisonRec
	var applied, skipped uint64
	for _, c := range changes {
		if st.done[c.Seq] {
			continue
		}
		if c.EventName != eventInsert && c.EventName != eventModify {
			st.done[c.Seq] = true
			skipped++
			continue
		}
		rec, err := p.dec.DecodeRecord(t, c.EventName, c.NewImage)
		if err != nil {
			err = fmt.Errorf("decode %s record %s of %s: %w", c.EventName, c.Seq, t.FullName(), err)
			if isTransient(err, true) {
				return err
			}
			poison = append(poison, poisonRec{c: c, err: err})
			continue
		}
		if rec == nil || rec.IsMVOutput {
			st.done[c.Seq] = true
			skipped++
			continue
		}
		m := eligibleViews(views, c.WriteTime)
		if m.none() {
			st.done[c.Seq] = true
			skipped++
			continue
		}
		rows, ids := []storage.Row{rec.Row}, []string{rec.RowID}
		if !perView {
			if err := p.apply(ctx, t, views, m, rows, ids); err != nil {
				if isTransient(err, false) {
					return fmt.Errorf("apply views of %s: %w", t.FullName(), err)
				}
				poison = append(poison, poisonRec{c: c, rowID: rec.RowID, err: err})
				continue
			}
			st.done[c.Seq] = true
			applied++
			continue
		}
		targets := views
		if !m.all() {
			targets = m.selectFrom(views)
		}
		ok := true
		for _, v := range targets {
			key := c.Seq + "\x00" + v.FullName()
			if st.done[key] {
				continue
			}
			if err := la.ApplyViewList(ctx, t, []*catalog.View{v}, rows, ids); err != nil {
				if isTransient(err, false) {
					return fmt.Errorf("apply view %s of %s: %w", v.FullName(), t.FullName(), err)
				}
				poison = append(poison, poisonRec{c: c, rowID: rec.RowID, view: v.FullName(), err: err})
				ok = false
				continue
			}
			st.done[key] = true
		}
		if ok {
			st.done[c.Seq] = true
			applied++
		}
	}
	p.applied.Add(applied)
	p.skipped.Add(skipped)
	if len(poison) == 0 {
		return nil
	}
	failing := p.clock().Sub(st.since)
	if p.policy == PoisonBlock || failing < p.poisonAfter {
		first := poison[0]
		return fmt.Errorf("%d poison record(s) of %s (first: seq %s view %q; failing for %s, set aside after %s unless MV_POISON_POLICY=block): %w",
			len(poison), t.FullName(), first.c.Seq, first.view, failing.Round(time.Millisecond), p.poisonAfter, first.err)
	}
	for _, pr := range poison {
		d := deadLetter{Table: t.FullName(), View: pr.view, Seq: pr.c.Seq, RowID: pr.rowID, Origin: p.origin, Err: pr.err, Image: pr.c.NewImage}
		if p.dlq != nil {
			if err := p.dlq.put(ctx, d); err != nil {
				return err
			}
		}
		p.deadLettered.Add(1)
		p.log.Error("DEAD LETTER: materialized view application failed deterministically; record skipped",
			"table", d.Table, "view", d.View, "seq", d.Seq, "row_id", d.RowID, "failing_for", failing.Round(time.Millisecond),
			"stored", p.dlq != nil, "err", pr.err)
		if pr.view != "" {
			st.done[pr.c.Seq+"\x00"+pr.view] = true
		} else {
			st.done[pr.c.Seq] = true
		}
	}
	return nil
}

// scratchTracker keeps isolation state for callers whose retries arrive as
// new invocations (Lambda, the replicator hook), keyed by the first record.
type scratchTracker struct {
	mu sync.Mutex
	m  map[string]any
}

// maxTracked bounds the tracked batches; on overflow the state restarts
// (poison then simply waits PoisonAfter again).
const maxTracked = 4096

func (s *scratchTracker) get(key string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key]
}

func (s *scratchTracker) set(key string, v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v == nil {
		delete(s.m, key)
		return
	}
	if s.m == nil || len(s.m) >= maxTracked {
		s.m = map[string]any{}
	}
	s.m[key] = v
}

// resolvePoison fills defaults; unset values come from MV_POISON_POLICY,
// MV_POISON_AFTER and CATALOG_MIN_RELOAD (so the stream worker embedded in
// chserver honours them too).
func resolvePoison(policy string, after, minReload time.Duration) (string, time.Duration, time.Duration, error) {
	if policy == "" {
		policy = os.Getenv("MV_POISON_POLICY")
	}
	switch policy {
	case "":
		policy = PoisonDeadLetter
	case PoisonDeadLetter, PoisonBlock:
	default:
		return "", 0, 0, fmt.Errorf("mvworker: MV_POISON_POLICY must be %q or %q, got %q", PoisonDeadLetter, PoisonBlock, policy)
	}
	var err error
	if after <= 0 {
		if after, err = envDuration("MV_POISON_AFTER", defaultPoisonAfter); err != nil {
			return "", 0, 0, err
		}
	}
	if minReload <= 0 {
		if minReload, err = envDuration("CATALOG_MIN_RELOAD", defaultCatalogMinReload); err != nil {
			return "", 0, 0, err
		}
	}
	return policy, after, minReload, nil
}

func envDuration(k string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("mvworker: %s must be a positive Go duration, got %q", k, v)
	}
	return d, nil
}
