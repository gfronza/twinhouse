package dynamo

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
)

// Reset epochs (DESIGN.md §5 "Resets").
//
// Every region keeps, per physical row table, a reset item in its regional
// __mv_markers table (key "reset#<physical>", no TTL) describing its copy:
//
//   - created: the table instance (CreatedAt unix nanos);
//   - cut: the region's reset epoch — the unix-nanos time of the latest
//     TRUNCATE applied to the copy (0: never);
//   - applied: the newest catalog ResetAt applied (catalog.Table.ResetAt);
//   - state/owner/until/del/cut_set: an in-progress reset holding a lease.
//
// Merge reads the item (strongly consistent) before applying view output:
// it waits while a reset is in progress (like ClickHouse, where TRUNCATE
// holds the table lock), applies a catalog reset this region has not
// applied yet, drops gated source rows written before max(created, cut)
// and tags its exactly-once markers with the cut, so a rebuild or POPULATE
// after a TRUNCATE is not blocked by old markers (which are never deleted:
// they expire through their TTL).
//
// A reset claims the item, then stamps the cut — after the claim landed, so
// every Merge that read the item before the claim only carries rows written
// before the cut, which the stream worker later drops — deletes the copy's
// rows and members (when this region owns them) and releases the item.
// The lease is renewed while deleting; a crashed reset is taken over once
// its lease expires.

const (
	resetKeyPrefix = "reset#"
	attrRCreated   = "created"
	attrRCut       = "cut"
	attrRApplied   = "applied"
	attrRState     = "state"
	attrROwner     = "owner"
	attrRUntil     = "until"
	attrRDel       = "del"
	attrRCutSet    = "cut_set"
	attrRPending   = "pending"
	attrRVer       = "v"

	resetStateResetting = "resetting"
)

// resetLease is how long a reset owns the item without renewing it.
const resetLease = 30 * time.Second

var errResetLost = errors.New("reset lease lost")

type resetItem struct {
	exists    bool
	ver       int64
	created   int64
	cut       int64
	applied   int64
	resetting bool
	owner     string
	until     int64
	del       bool
	cutSet    bool
	pending   int64 // catalog ResetAt the running reset applies (0: none)
}

func resetKey(physical string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{attrPK: attrS(resetKeyPrefix + physical)}
}

func numAttr(item map[string]types.AttributeValue, name string) int64 {
	if n, ok := item[name].(*types.AttributeValueMemberN); ok {
		v, _ := strconv.ParseInt(n.Value, 10, 64)
		return v
	}
	return 0
}

func boolAttr(item map[string]types.AttributeValue, name string) bool {
	b, ok := item[name].(*types.AttributeValueMemberBOOL)
	return ok && b.Value
}

func strAttr(item map[string]types.AttributeValue, name string) string {
	if v, ok := item[name].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

// getReset reads the reset item of a physical table (strongly consistent).
// A missing item, or a missing markers table, reads as the zero item.
func (s *storeImpl) getReset(ctx context.Context, physical string) (resetItem, error) {
	var out *dynamodb.GetItemOutput
	err := withRetry(ctx, func() (err error) {
		out, err = s.ddb.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(s.markersTable()), Key: resetKey(physical), ConsistentRead: aws.Bool(true),
		})
		return err
	})
	if isNotFound(err) {
		return resetItem{}, nil
	}
	if err != nil {
		return resetItem{}, ddbError(err, "cannot read the reset state of DynamoDB table %s", physical)
	}
	if len(out.Item) == 0 {
		return resetItem{}, nil
	}
	it := out.Item
	return resetItem{
		exists: true, ver: numAttr(it, attrRVer), created: numAttr(it, attrRCreated), cut: numAttr(it, attrRCut),
		applied: numAttr(it, attrRApplied), resetting: strAttr(it, attrRState) == resetStateResetting,
		owner: strAttr(it, attrROwner), until: numAttr(it, attrRUntil), del: boolAttr(it, attrRDel),
		cutSet: boolAttr(it, attrRCutSet), pending: numAttr(it, attrRPending),
	}, nil
}

// casReset replaces the reset item prev (as read) with next. It reports
// false when the item changed in between.
func (s *storeImpl) casReset(ctx context.Context, physical string, prev resetItem, next *resetItem) (bool, error) {
	next.exists, next.ver = true, prev.ver+1
	item := resetKey(physical)
	item[attrRVer] = attrN(itoa64(next.ver))
	item[attrRCreated] = attrN(itoa64(next.created))
	item[attrRCut] = attrN(itoa64(next.cut))
	item[attrRApplied] = attrN(itoa64(next.applied))
	if next.resetting {
		item[attrRState] = attrS(resetStateResetting)
		item[attrROwner] = attrS(next.owner)
		item[attrRUntil] = attrN(itoa64(next.until))
		item[attrRDel] = attrBool(next.del)
		item[attrRCutSet] = attrBool(next.cutSet)
		item[attrRPending] = attrN(itoa64(next.pending))
	}
	in := &dynamodb.PutItemInput{TableName: aws.String(s.markersTable()), Item: item}
	if prev.exists {
		in.ConditionExpression = aws.String("#v = :v")
		in.ExpressionAttributeNames = map[string]string{"#v": attrRVer}
		in.ExpressionAttributeValues = map[string]types.AttributeValue{":v": attrN(itoa64(prev.ver))}
	} else {
		in.ConditionExpression = aws.String("attribute_not_exists(#p)")
		in.ExpressionAttributeNames = map[string]string{"#p": attrPK}
	}
	err := withRetry(ctx, func() error {
		_, err := s.ddb.PutItem(ctx, in)
		return err
	})
	switch {
	case err == nil:
		return true, nil
	case isConditionFailed(err):
		return false, nil
	}
	return false, ddbError(err, "cannot write the reset state of DynamoDB table %s", physical)
}

// instanceView returns the reset item as it applies to table instance
// created: an item of an older instance (the table was dropped and
// recreated) reads as a fresh item; stale reports an item of a newer
// instance (the caller's catalog snapshot is out of date).
func instanceView(it resetItem, created int64) (view resetItem, stale bool) {
	switch {
	case !it.exists:
		return resetItem{created: created}, false
	case it.created < created:
		return resetItem{exists: true, ver: it.ver, created: created}, false
	case it.created > created:
		return it, true
	}
	return it, false
}

// catalogReset is t's catalog reset epoch (unix nanos; 0 = never).
func catalogReset(t *catalog.Table) int64 {
	if t.ResetAt.IsZero() {
		return 0
	}
	return t.ResetAt.UnixNano()
}

// ownsCopy reports whether this region must delete its own copy's rows
// when it applies a catalog reset: regional tables are never replicated.
// For global tables the issuing region's deletes replicate, so other
// regions only record the epoch.
func ownsCopy(t *catalog.Table) bool { return t.Replication == catalog.ReplicationRegional }

// reset resets this region's copy of l's table: it records a new cut (at or
// after catE, the catalog ResetAt being applied; 0 if none) and, with del,
// deletes the copy's rows and members. Without force it is a no-op when
// catE was already applied. Concurrent resets of one table serialise on
// the reset item.
func (s *storeImpl) reset(ctx context.Context, l *layout, catE int64, del, force bool) error {
	me := newRowID()
	for attempt := 0; ; attempt++ {
		raw, err := s.getReset(ctx, l.physical)
		if err != nil {
			return err
		}
		it, stale := instanceView(raw, l.created)
		if stale {
			return nil // a newer instance of the table exists
		}
		if !force && !it.resetting && catE <= it.applied {
			return nil
		}
		now := s.now().UnixNano()
		if it.resetting && it.until > now {
			if err := pollSleep(ctx, attempt); err != nil {
				return cherr.Wrap(cherr.Timeout, err, "timed out waiting for the reset of DynamoDB table %s", l.physical)
			}
			continue
		}
		claim := it
		claim.created = l.created
		claim.owner, claim.until = me, now+int64(resetLease)
		if it.resetting {
			// Take over an expired reset: finish it as it was started.
			claim.del = claim.del || del
			catE = max(catE, it.pending)
		} else {
			claim.resetting, claim.del, claim.cutSet = true, del, false
		}
		claim.pending = catE
		ok, err := s.casReset(ctx, l.physical, raw, &claim)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		err = s.runReset(ctx, l, claim, catE, force)
		if errors.Is(err, errResetLost) {
			continue
		}
		if err != nil {
			return err
		}
		if force {
			return nil
		}
		// Re-check: a newer catalog reset may have been claimed meanwhile.
	}
}

// runReset performs a claimed reset (cur is the item as written).
//
// The cut of a TRUNCATE issued here (local) is stamped after the claim
// landed: a Merge that read the item before the claim only carries rows
// written before that, which are therefore gated, so it can never count a
// row the worker applies again under the new epoch. A reset applied from
// the catalog (issued in another region) uses the catalog's ResetAt as its
// cut, so every region drops the same source rows; only a Merge already in
// flight when this region claims the reset, with a row written between the
// remote TRUNCATE and the claim, could be applied under both epochs.
func (s *storeImpl) runReset(ctx context.Context, l *layout, cur resetItem, catE int64, local bool) error {
	if !cur.cutSet {
		next := cur
		next.cut = max(catE, cur.cut+1)
		if local || catE == 0 {
			next.cut = max(next.cut, s.stamp(1))
		}
		next.cutSet = true
		ok, err := s.casReset(ctx, l.physical, cur, &next)
		if err != nil {
			return err
		}
		if !ok {
			return errResetLost
		}
		cur = next
	}
	if cur.del {
		var (
			mu   sync.Mutex
			lost bool
		)
		dctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		renewed := make(chan struct{})
		go func() { // lease renewal
			defer close(renewed)
			t := time.NewTicker(resetLease / 3)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
				}
				mu.Lock()
				next := cur
				next.until = s.now().UnixNano() + int64(resetLease)
				ok, err := s.casReset(dctx, l.physical, cur, &next)
				if ok {
					cur = next
				} else if err == nil {
					lost = true
					cancel()
				}
				mu.Unlock()
				if lost {
					return
				}
			}
		}()
		err := s.deleteCopy(dctx, l)
		close(done)
		<-renewed
		cancel()
		if lost {
			return errResetLost
		}
		if err != nil {
			return err
		}
	}
	final := cur
	final.resetting, final.owner, final.until, final.del, final.cutSet, final.pending = false, "", 0, false, false, 0
	final.applied = max(cur.applied, catE)
	ok, err := s.casReset(ctx, l.physical, cur, &final)
	if err != nil {
		return err
	}
	if !ok {
		return errResetLost
	}
	return nil
}

// deleteCopy deletes every row (and set member) of this region's copy.
func (s *storeImpl) deleteCopy(ctx context.Context, l *layout) error {
	if err := s.deleteAll(ctx, l.physical, true); err != nil {
		return err
	}
	if err := s.deleteAll(ctx, membersTable(l.physical), true); err != nil && !cherr.Is(err, cherr.UnknownTable) {
		return err
	}
	return nil
}

// prepareTarget brings l in line with this region's reset item before view
// output is merged into it: it waits for an in-progress reset, applies a
// catalog reset not yet applied here, sets l.created/l.cut (marker tags)
// and returns the gate: gated source rows written before it are dropped
// (unix nanos; 0 = none).
func (s *storeImpl) prepareTarget(ctx context.Context, l *layout) (int64, error) {
	catE := catalogReset(l.t)
	for attempt := 0; ; attempt++ {
		raw, err := s.getReset(ctx, l.physical)
		if err != nil {
			return 0, err
		}
		it, stale := instanceView(raw, l.created)
		switch {
		case it.resetting && stale:
			if err := pollSleep(ctx, attempt); err != nil {
				return 0, cherr.Wrap(cherr.Timeout, err, "timed out waiting for the reset of DynamoDB table %s", l.physical)
			}
			continue
		case it.resetting:
			if err := s.reset(ctx, l, 0, it.del, false); err != nil {
				return 0, err
			}
			continue
		case !stale && catE > it.applied:
			if err := s.reset(ctx, l, catE, ownsCopy(l.t), false); err != nil {
				return 0, err
			}
			continue
		}
		gate := it.cut
		if stale {
			l.created = it.created
			gate = max(gate, it.created)
		} else if !l.t.CreatedAt.IsZero() {
			// Like a view, a target only receives rows written after it
			// was created (DROP + CREATE starts a new instance).
			gate = max(gate, l.created)
		}
		l.cut = it.cut
		return gate, nil
	}
}

// syncResetOnEnsure records a new table instance in this region's reset
// item (taking over the catalog's reset epoch, so a copy created later in
// another region applies the same cut) and applies a catalog reset this
// region has not applied yet (the reconciler calls EnsureTable whenever the
// catalog entry changes, so a TRUNCATE issued in another region resets
// this region's copy of a regional table).
func (s *storeImpl) syncResetOnEnsure(ctx context.Context, l *layout) error {
	catE := catalogReset(l.t)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		raw, err := s.getReset(ctx, l.physical)
		if err != nil {
			return err
		}
		it, stale := instanceView(raw, l.created)
		if stale {
			return nil
		}
		if !raw.exists || raw.created < l.created {
			next := resetItem{created: l.created, cut: catE, applied: catE}
			ok, err := s.casReset(ctx, l.physical, raw, &next)
			if isNotFound(err) {
				return nil // no markers table (system tables not set up)
			}
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
			continue
		}
		if catE > it.applied {
			return s.reset(ctx, l, catE, ownsCopy(l.t), false)
		}
		return nil
	}
	return cherr.New(cherr.Unavailable, "the reset state of DynamoDB table %s keeps changing", l.physical)
}
