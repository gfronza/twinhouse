package dynamo

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/config"
	"github.com/gfronza/twinhouse/internal/storage"
)

// tableSpec describes a physical table to create.
type tableSpec struct {
	name string
	// keyType is the type of _pk (and _sk when rangeKey).
	keyType  types.ScalarAttributeType
	rangeKey bool
	stream   bool
	// global tables get replicas in GLOBAL_MODE=aws.
	global  bool
	ttlAttr string
	// home is the region that creates a global table and adds its replicas
	// in GLOBAL_MODE=aws. Every other region only joins: it waits for the
	// replica the home region adds and never creates the table itself (AWS
	// refuses to add a replica in a region where a table of that name
	// already exists, so a table created there first would never be
	// replicated).
	home string
	// waitReplica makes a joining region wait (up to tableWait) for its
	// replica instead of reporting it pending at once.
	waitReplica bool
}

func (s *storeImpl) rowTableSpec(l *layout) tableSpec {
	home := l.t.HomeRegion
	if home == "" {
		home = s.homeRegion()
	}
	return tableSpec{
		name: l.physical, keyType: types.ScalarAttributeTypeB, rangeKey: true, stream: hasStreamConsumer(l),
		global: s.awsReplicas(l.t), home: home,
	}
}

// hasStreamConsumer reports whether a row table's stream can have a
// reader: every table except regional Aggregating/Summing tables (rollups),
// which cannot be view sources (rejected at CREATE VIEW) and are neither
// replicated by cmd/replicator nor AWS global tables. Rollups are the
// busiest writers (one or more updates per source row), and DynamoDB Local
// keeps stream records, even of dropped tables, for 24 hours, slowing every
// later write. Streams of existing tables are never disabled.
func hasStreamConsumer(l *layout) bool {
	repl := l.t.Replication
	if repl == "" {
		repl = catalog.DefaultReplication(l.t.Engine)
	}
	return !(l.merging && repl == catalog.ReplicationRegional)
}

// homeRegion is the region that creates the system global tables
// (__catalog) and the global tables whose catalog entry names no home
// region: Options.HomeRegion when set; otherwise the first entry of
// ReplicaRegions when it lists this region (every region is configured
// with the same full list), else the alphabetically first of this region
// and ReplicaRegions (every region lists the others). Both rules give
// every region the same answer when they are configured consistently.
func (s *storeImpl) homeRegion() string {
	if s.opts.HomeRegion != "" {
		return s.opts.HomeRegion
	}
	var listed []string
	self := false
	for _, r := range s.opts.ReplicaRegions {
		if r == "" {
			continue
		}
		listed = append(listed, r)
		self = self || r == s.opts.Region
	}
	if self {
		return listed[0]
	}
	all := append([]string{s.opts.Region}, listed...)
	sort.Strings(all)
	return all[0]
}

// awsReplicas reports whether t's physical table gets AWS global-table
// replicas (GLOBAL_MODE=aws). ReplacingMergeTree tables do not, even when
// their catalog replication is global: AWS resolves concurrent writes to an
// item by its own last-writer-wins, which could let a lower version win
// and silently drops the loser, which then never reaches the losing
// region's stream (and views). They exist in every region (the reconciler
// creates them) and cmd/replicator copies them, with its version-aware
// conditional put for ReplacingMergeTree(ver) and its conflict hook for
// both (catalog.CopiedByReplicator).
func (s *storeImpl) awsReplicas(t *catalog.Table) bool {
	return t.Replication == catalog.ReplicationGlobal && !catalog.CopiedByReplicator(t)
}

func membersSpec(l *layout) tableSpec {
	return tableSpec{name: membersTable(l.physical), keyType: types.ScalarAttributeTypeB, rangeKey: true}
}

func (s *storeImpl) systemSpecs() []tableSpec {
	p := s.opts.Prefix
	return []tableSpec{
		{name: p + "__catalog", keyType: types.ScalarAttributeTypeS, stream: true, global: true,
			home: s.homeRegion(), waitReplica: true},
		{name: p + "__mv_markers", keyType: types.ScalarAttributeTypeS, ttlAttr: attrExp},
		{name: p + "__checkpoints", keyType: types.ScalarAttributeTypeS},
	}
}

func (s *Store) ensureSystemTables(ctx context.Context) error {
	for _, spec := range s.impl.systemSpecs() {
		if err := s.impl.ensurePhysical(ctx, spec); err != nil {
			return err
		}
	}
	return nil
}

// checkSystemTables is CheckSystemTables: DescribeTable only. A table that
// is UPDATING (a replica being added) accepts item operations, so it is
// ready; CREATING or missing is not.
func (s *Store) checkSystemTables(ctx context.Context) error {
	for _, spec := range s.impl.systemSpecs() {
		desc, err := s.impl.describe(ctx, spec.name)
		if err != nil {
			return ddbError(err, "cannot describe DynamoDB table %s", spec.name)
		}
		switch {
		case desc == nil:
			return cherr.New(cherr.Unavailable, "system table %s does not exist in %s yet: the reconciler (or chserver) creates it; "+
				"deploy the home region first and wait for its first reconcile", spec.name, s.impl.opts.Region)
		case desc.TableStatus == types.TableStatusCreating:
			return cherr.New(cherr.Unavailable, "system table %s is still being created", spec.name)
		}
		if err := s.impl.checkKeySchema(desc, spec); err != nil {
			return err
		}
	}
	return nil
}

// systemTable reports a system table name ({prefix}__catalog, __mv_markers,
// __checkpoints, the stream worker's __dead_letters, ...).
func (s *storeImpl) systemTable(name string) bool {
	return strings.HasPrefix(name, s.opts.Prefix+"__")
}

// ownRowTable reports a row table of this deployment by name shape:
// "<prefix><db>.<table>.<incarnation>" as catalog.CreateTable names them
// (identifiers without dots, the 8-character incarnation suffix). System
// tables, ".members" side tables and tables of other deployments (a
// prefix that starts with this one's does not produce this shape unless
// the rest of it is itself "<db>." of a valid name, so prefixes sharing an
// account should not be prefixes of one another) are excluded; tables of
// older builds without the suffix are only dropped when their catalog
// entry was observed.
func (s *storeImpl) ownRowTable(name string) bool {
	if !strings.HasPrefix(name, s.opts.Prefix) || s.systemTable(name) || strings.HasSuffix(name, membersSuffix) {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(name, s.opts.Prefix), ".")
	if len(parts) != 3 || !catalog.IsIncarnationID(parts[2]) {
		return false
	}
	return catalog.ValidateName("database", parts[0]) == nil && catalog.ValidateName("table", parts[1]) == nil
}

// listPhysicalTables is ListPhysicalTables.
func (s *storeImpl) listPhysicalTables(ctx context.Context) ([]string, error) {
	var out []string
	in := &dynamodb.ListTablesInput{Limit: aws.Int32(100)}
	for {
		var page *dynamodb.ListTablesOutput
		err := withRetry(ctx, func() (err error) {
			page, err = s.ddb.ListTables(ctx, in)
			return err
		})
		if err != nil {
			return nil, ddbError(err, "cannot list DynamoDB tables")
		}
		for _, name := range page.TableNames {
			if s.ownRowTable(name) {
				out = append(out, name)
			}
		}
		if page.LastEvaluatedTableName == nil || *page.LastEvaluatedTableName == "" {
			return out, nil
		}
		next := *in
		next.ExclusiveStartTableName = page.LastEvaluatedTableName
		in = &next
	}
}

// dropPhysicalTable is DropPhysicalTable.
func (s *Store) dropPhysicalTable(ctx context.Context, physical string) error {
	if physical == "" || !strings.HasPrefix(physical, s.impl.opts.Prefix) || s.impl.systemTable(physical) {
		return cherr.New(cherr.BadArguments, "refusing to drop DynamoDB table %q: not a row table of prefix %q", physical, s.impl.opts.Prefix)
	}
	if err := s.impl.dropPhysical(ctx, physical, true); err != nil {
		return err
	}
	return s.impl.dropPhysical(ctx, membersTable(physical), false)
}

func (s *Store) ensureTable(ctx context.Context, t *catalog.Table) error {
	l, err := s.impl.layoutOf(t)
	if err != nil {
		return err
	}
	if err := s.impl.ensurePhysical(ctx, s.impl.rowTableSpec(l)); err != nil {
		return err
	}
	if l.hasMembers {
		if err := s.impl.ensurePhysical(ctx, membersSpec(l)); err != nil {
			return err
		}
	}
	return s.impl.syncResetOnEnsure(ctx, l)
}

func (s *Store) dropTable(ctx context.Context, t *catalog.Table) error {
	l, err := s.impl.layoutOf(t)
	if err != nil {
		return err
	}
	rowErr := s.impl.dropPhysical(ctx, l.physical, s.impl.awsReplicas(t))
	if rowErr != nil && !errors.Is(rowErr, storage.ErrDeleteDeferred) {
		return rowErr
	}
	// Always attempt the side table: an ALTER may have changed the layout.
	if err := s.impl.dropPhysical(ctx, membersTable(l.physical), false); err != nil {
		return err
	}
	return rowErr
}

// truncateTable deletes this region's rows and members of t and starts a
// new reset epoch (DESIGN.md §5 "Resets"): exactly-once markers are kept
// (they are tagged with the epoch, so rebuilds and POPULATE are not blocked
// by them) and view output of source rows written before the epoch is no
// longer applied, so stream records not yet processed by the worker, and
// inline applications racing the TRUNCATE, cannot bring old rows back or
// count them twice. When the caller persisted t.ResetAt (catalog
// MarkTruncated + UpdateTable) first, the other regions apply the same
// reset to their copies of a regional table.
func (s *Store) truncateTable(ctx context.Context, t *catalog.Table) error {
	l, err := s.impl.layoutOf(t)
	if err != nil {
		return err
	}
	catE := catalogReset(t)
	if s.impl.multiRegion() && ownsCopy(t) {
		raw, err := s.impl.getReset(ctx, l.physical)
		if err != nil {
			return err
		}
		if it, _ := instanceView(raw, l.created); catE <= it.applied {
			// Without a new catalog ResetAt the other regions would keep
			// their copies (each region keeps its own copy of a regional
			// table): refuse rather than diverge.
			return cherr.New(cherr.NotImplemented,
				"TRUNCATE of %s is not supported with GLOBAL_MODE=%s unless the reset is recorded in the catalog first "+
					"(the table is regional and only this region's copy would be reset)", t.FullName(), s.impl.opts.GlobalMode)
		}
	}
	return s.impl.reset(ctx, l, catE, true, true)
}

// multiRegion reports a multi-region deployment (GLOBAL_MODE aws or
// emulated).
func (s *storeImpl) multiRegion() bool {
	return s.opts.GlobalMode == config.GlobalAWS || s.opts.GlobalMode == config.GlobalEmulated
}

func (s *Store) streamARN(ctx context.Context, physical string) (string, error) {
	desc, err := s.impl.describe(ctx, physical)
	if err != nil {
		return "", ddbError(err, "cannot describe DynamoDB table %s", physical)
	}
	if desc == nil {
		return "", cherr.New(cherr.UnknownTable, "DynamoDB table %s does not exist", physical)
	}
	if desc.StreamSpecification == nil || !aws.ToBool(desc.StreamSpecification.StreamEnabled) {
		return "", nil
	}
	return aws.ToString(desc.LatestStreamArn), nil
}

// describe returns the table description, or nil if the table does not exist.
func (s *storeImpl) describe(ctx context.Context, name string) (*types.TableDescription, error) {
	var out *dynamodb.DescribeTableOutput
	err := withRetry(ctx, func() (err error) {
		out, err = s.ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		return err
	})
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out.Table, nil
}

func createTableInput(spec tableSpec) *dynamodb.CreateTableInput {
	in := &dynamodb.CreateTableInput{
		TableName:   aws.String(spec.name),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: spec.keyType},
		},
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash}},
	}
	if spec.rangeKey {
		in.AttributeDefinitions = append(in.AttributeDefinitions,
			types.AttributeDefinition{AttributeName: aws.String(attrSK), AttributeType: spec.keyType})
		in.KeySchema = append(in.KeySchema, types.KeySchemaElement{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange})
	}
	if spec.stream {
		in.StreamSpecification = &types.StreamSpecification{
			StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewAndOldImages,
		}
	}
	return in
}

// ensurePhysical creates the table if missing and waits until it is ACTIVE
// with the requested stream, TTL and (GLOBAL_MODE=aws) replicas. Idempotent
// and safe to run concurrently from several processes.
func (s *storeImpl) ensurePhysical(ctx context.Context, spec tableSpec) error {
	if spec.global && s.opts.GlobalMode == config.GlobalAWS && spec.home != "" && spec.home != s.opts.Region {
		return s.joinReplica(ctx, spec)
	}
	desc, err := s.waitActive(ctx, spec.name, func() error {
		_, err := s.ddb.CreateTable(ctx, createTableInput(spec))
		return err
	})
	if err != nil {
		return ddbError(err, "cannot create DynamoDB table %s", spec.name)
	}
	if err := s.checkKeySchema(desc, spec); err != nil {
		return err
	}
	if spec.stream {
		ss := desc.StreamSpecification
		switch {
		case ss == nil || !aws.ToBool(ss.StreamEnabled):
			err := withRetry(ctx, func() error {
				_, err := s.ddb.UpdateTable(ctx, &dynamodb.UpdateTableInput{
					TableName: aws.String(spec.name),
					StreamSpecification: &types.StreamSpecification{
						StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewAndOldImages,
					},
				})
				return err
			})
			if err != nil {
				return ddbError(err, "cannot enable streams on DynamoDB table %s", spec.name)
			}
			if _, err := s.waitActive(ctx, spec.name, nil); err != nil {
				return ddbError(err, "waiting for DynamoDB table %s", spec.name)
			}
		case ss.StreamViewType != types.StreamViewTypeNewAndOldImages:
			return cherr.New(cherr.BadArguments, "DynamoDB table %s has stream view type %s; NEW_AND_OLD_IMAGES is required",
				spec.name, ss.StreamViewType)
		}
	}
	if spec.ttlAttr != "" {
		if err := s.ensureTTL(ctx, spec.name, spec.ttlAttr); err != nil {
			return err
		}
	}
	if spec.global && s.opts.GlobalMode == config.GlobalAWS && !s.opts.AsyncDDL {
		// With AsyncDDL the home region's reconciler adds the replicas
		// (it runs EnsureTable for every catalog entry): adding one takes
		// minutes, far beyond an API request's budget.
		return s.ensureReplicas(ctx, spec.name)
	}
	return nil
}

// joinReplica is ensurePhysical of a global table in a region other than
// its home (GLOBAL_MODE=aws): the home region creates the table and adds
// this region as a replica, so this region only waits for the replica to
// become ACTIVE. Without spec.waitReplica a missing replica is reported at
// once (cherr.Unavailable) and the caller (the reconciler) retries later.
func (s *storeImpl) joinReplica(ctx context.Context, spec tableSpec) error {
	wctx, cancel := context.WithTimeout(ctx, s.tableWait)
	defer cancel()
	for attempt := 0; ; attempt++ {
		desc, err := s.describe(wctx, spec.name)
		if err != nil {
			return ddbError(err, "cannot describe DynamoDB table %s", spec.name)
		}
		if desc != nil {
			if desc.TableStatus != types.TableStatusActive {
				if desc, err = s.waitActive(ctx, spec.name, nil); err != nil {
					return ddbError(err, "waiting for the replica of DynamoDB table %s", spec.name)
				}
			}
			return s.checkKeySchema(desc, spec)
		}
		pending := &cherr.Error{Code: cherr.Unavailable, Cause: storage.ErrAwaitingReplica,
			Msg: fmt.Sprintf("DynamoDB table %s does not exist in %s yet: waiting for its home region %s to add this region as a replica",
				spec.name, s.opts.Region, spec.home)}
		if !spec.waitReplica {
			return pending
		}
		if err := pollSleep(wctx, attempt); err != nil {
			return pending
		}
	}
}

func (s *storeImpl) checkKeySchema(desc *types.TableDescription, spec tableSpec) error {
	want := 1
	if spec.rangeKey {
		want = 2
	}
	if len(desc.KeySchema) != want {
		return cherr.New(cherr.BadArguments, "DynamoDB table %s exists with an incompatible key schema", spec.name)
	}
	for _, d := range desc.AttributeDefinitions {
		if n := aws.ToString(d.AttributeName); (n == attrPK || n == attrSK) && d.AttributeType != spec.keyType {
			return cherr.New(cherr.BadArguments, "DynamoDB table %s exists with key %s of type %s, want %s",
				spec.name, n, d.AttributeType, spec.keyType)
		}
	}
	return nil
}

// waitActive polls until the table is ACTIVE. When it does not exist, create
// (if non-nil) is called; a table being deleted is awaited and recreated.
func (s *storeImpl) waitActive(ctx context.Context, name string, create func() error) (*types.TableDescription, error) {
	ctx, cancel := context.WithTimeout(ctx, s.tableWait)
	defer cancel()
	for attempt := 0; ; attempt++ {
		desc, err := s.describe(ctx, name)
		if err != nil {
			return nil, err
		}
		switch {
		case desc == nil:
			if create == nil {
				return nil, &types.ResourceNotFoundException{Message: aws.String("table " + name + " not found")}
			}
			if err := create(); err != nil && !isInUse(err) {
				return nil, err
			}
			continue
		case desc.TableStatus == types.TableStatusActive:
			return desc, nil
		}
		if err := pollSleep(ctx, attempt); err != nil {
			return nil, fmt.Errorf("table %s stuck in status %s: %w", name, desc.TableStatus, err)
		}
	}
}

// pollSleep waits between table-status polls (100ms growing to 2s).
func pollSleep(ctx context.Context, attempt int) error {
	d := 100 * time.Millisecond << min(attempt, 4)
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *storeImpl) ensureTTL(ctx context.Context, name, attr string) error {
	out, err := s.ddb.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(name)})
	if err != nil {
		return ddbError(err, "cannot describe TTL of %s", name)
	}
	if d := out.TimeToLiveDescription; d != nil &&
		(d.TimeToLiveStatus == types.TimeToLiveStatusEnabled || d.TimeToLiveStatus == types.TimeToLiveStatusEnabling) {
		return nil
	}
	_, err = s.ddb.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName:               aws.String(name),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String(attr), Enabled: aws.Bool(true)},
	})
	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() == "ValidationException" && strings.Contains(ae.ErrorMessage(), "already enabled") {
		return nil
	}
	return ddbError(err, "cannot enable TTL on %s", name)
}

// replicasToAdd returns the regions of wanted that are neither self nor
// already replicas of the table, in order and without duplicates.
func replicasToAdd(desc *types.TableDescription, self string, wanted []string) []string {
	have := map[string]bool{self: true}
	if desc != nil {
		for _, r := range desc.Replicas {
			have[aws.ToString(r.RegionName)] = true
		}
	}
	var out []string
	for _, r := range wanted {
		if r == "" || have[r] {
			continue
		}
		have[r] = true
		out = append(out, r)
	}
	return out
}

func replicaCreateInput(table, region string) *dynamodb.UpdateTableInput {
	return &dynamodb.UpdateTableInput{
		TableName: aws.String(table),
		ReplicaUpdates: []types.ReplicationGroupUpdate{
			{Create: &types.CreateReplicationGroupMemberAction{RegionName: aws.String(region)}},
		},
	}
}

func replicaDeleteInput(table, region string) *dynamodb.UpdateTableInput {
	return &dynamodb.UpdateTableInput{
		TableName: aws.String(table),
		ReplicaUpdates: []types.ReplicationGroupUpdate{
			{Delete: &types.DeleteReplicationGroupMemberAction{RegionName: aws.String(region)}},
		},
	}
}

// ensureReplicas adds the configured replica regions to a global table, one
// UpdateTable call per region (DynamoDB accepts one replica change at a
// time), waiting for the table to return to ACTIVE in between.
func (s *storeImpl) ensureReplicas(ctx context.Context, name string) error {
	desc, err := s.describe(ctx, name)
	if err != nil {
		return ddbError(err, "cannot describe DynamoDB table %s", name)
	}
	for _, region := range replicasToAdd(desc, s.opts.Region, s.opts.ReplicaRegions) {
		if err := s.updateReplicas(ctx, replicaCreateInput(name, region)); err != nil {
			if replicaExistsAsTable(err) {
				return cherr.Wrap(cherr.BadArguments, err,
					"cannot add replica %s to DynamoDB table %s: a table of that name already exists in %s outside the "+
						"replication group (created by a process of that region that did not wait for its replica). "+
						"Delete that table in %s (it is not replicated, so it only holds rows written there) and the "+
						"replica will be added on the next attempt", region, name, region, region)
			}
			return ddbError(err, "cannot add replica %s to DynamoDB table %s", region, name)
		}
	}
	return nil
}

// replicaExistsAsTable reports AWS's refusal to add a replica in a region
// that already has a table of that name.
func replicaExistsAsTable(err error) bool {
	var ae smithy.APIError
	return errors.As(err, &ae) && ae.ErrorCode() == "ValidationException" &&
		strings.Contains(ae.ErrorMessage(), "already exist")
}

// isDeleteRestricted reports DynamoDB's refusal to delete the source table
// (or replica) of a global table within 24 hours of adding a replica, a
// ValidationException whose message mentions the 24 hour window. The exact
// wording is not documented; the match is kept loose (fix on a real
// deployment if the service's text differs).
func isDeleteRestricted(err error) bool {
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "ValidationException" {
		return false
	}
	msg := strings.ToLower(ae.ErrorMessage())
	return strings.Contains(msg, "24 hours") || strings.Contains(msg, "24 hrs") || strings.Contains(msg, "twenty-four hours")
}

// deleteDeferred is the error of a physical deletion the reconciler
// completes later (storage.ErrDeleteDeferred in the cause chain).
func deleteDeferred(name, why string) error {
	return &cherr.Error{Code: cherr.Unavailable, Cause: storage.ErrDeleteDeferred,
		Msg: fmt.Sprintf("DynamoDB table %s cannot be deleted now: %s", name, why)}
}

// updateReplicas applies one replica update, retrying while the table is busy.
func (s *storeImpl) updateReplicas(ctx context.Context, in *dynamodb.UpdateTableInput) error {
	for attempt := 0; ; attempt++ {
		_, err := s.ddb.UpdateTable(ctx, in)
		if err == nil {
			_, err = s.waitActive(ctx, aws.ToString(in.TableName), nil)
			return err
		}
		if (!isInUse(err) && !isRetryable(err)) || attempt >= maxAttempts {
			return err
		}
		if _, werr := s.waitActive(ctx, aws.ToString(in.TableName), nil); werr != nil {
			return werr
		}
	}
}

// dropPhysical deletes a table (idempotent) and waits until it is gone. With
// removeReplicas in GLOBAL_MODE=aws, the other regions' replicas are removed
// first, so DROP removes the global table everywhere. With Options.AsyncDDL
// the deletion is issued but not awaited, and a table that still has
// replicas is left to the reconciler (ErrDeleteDeferred).
func (s *storeImpl) dropPhysical(ctx context.Context, name string, removeReplicas bool) error {
	desc, err := s.describe(ctx, name)
	if err != nil {
		return ddbError(err, "cannot describe DynamoDB table %s", name)
	}
	if desc == nil {
		return nil
	}
	if removeReplicas && s.opts.GlobalMode == config.GlobalAWS {
		for _, r := range desc.Replicas {
			region := aws.ToString(r.RegionName)
			if region == s.opts.Region {
				continue
			}
			if s.opts.AsyncDDL {
				return deleteDeferred(name, fmt.Sprintf("it has a replica in %s; the reconciler removes the replicas and the table", region))
			}
			if err := s.updateReplicas(ctx, replicaDeleteInput(name, region)); err != nil && !isNotFound(err) {
				if isDeleteRestricted(err) {
					return deleteDeferred(name, err.Error())
				}
				return ddbError(err, "cannot remove replica %s of DynamoDB table %s", region, name)
			}
		}
	}
	wctx, cancel := context.WithTimeout(ctx, s.tableWait)
	defer cancel()
	for attempt := 0; ; attempt++ {
		_, err := s.ddb.DeleteTable(wctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)})
		switch {
		case err == nil && s.opts.AsyncDDL:
			return nil // deletion accepted; not awaited
		case err == nil, isInUse(err) && !s.opts.AsyncDDL:
			// Deleting (or busy): poll until it disappears.
		case isInUse(err):
			return deleteDeferred(name, "it is busy (CREATING or UPDATING); the reconciler deletes it")
		case isNotFound(err):
			return nil
		case isDeleteRestricted(err):
			return deleteDeferred(name, err.Error())
		case !isRetryable(err):
			return ddbError(err, "cannot delete DynamoDB table %s", name)
		}
		desc, derr := s.describe(wctx, name)
		if derr != nil {
			return ddbError(derr, "cannot describe DynamoDB table %s", name)
		}
		if desc == nil {
			return nil
		}
		if err := pollSleep(wctx, attempt); err != nil {
			return ddbError(err, "timed out deleting DynamoDB table %s", name)
		}
	}
}

// scanFilter is an optional Scan FilterExpression.
type scanFilter struct {
	expr   string
	names  map[string]string
	values map[string]types.AttributeValue
}

// deleteAll deletes every item (matching filter) of a table using a parallel
// keys-only Scan. rangeKey tells whether the table has _sk.
func (s *storeImpl) deleteAll(ctx context.Context, table string, rangeKey bool, filter *scanFilter) error {
	segments := s.opts.ScanSegments
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for seg := 0; seg < segments; seg++ {
		wg.Add(1)
		go func(seg int) {
			defer wg.Done()
			names := map[string]string{"#p": attrPK}
			proj := "#p"
			if rangeKey {
				names["#s"] = attrSK
				proj = "#p, #s"
			}
			in := &dynamodb.ScanInput{
				TableName: aws.String(table), Segment: aws.Int32(int32(seg)), TotalSegments: aws.Int32(int32(segments)),
				ProjectionExpression: aws.String(proj), ExpressionAttributeNames: names, ConsistentRead: aws.Bool(true),
			}
			if filter != nil {
				in.FilterExpression = aws.String(filter.expr)
				for k, v := range filter.names {
					names[k] = v
				}
				in.ExpressionAttributeValues = filter.values
			}
			err := s.scanPages(ctx, in, func(items []map[string]types.AttributeValue) error {
				reqs := make([]types.WriteRequest, len(items))
				for i, it := range items {
					reqs[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: it}}
				}
				return s.batchWrite(ctx, table, reqs)
			})
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
			}
		}(seg)
	}
	wg.Wait()
	if firstErr != nil {
		return ddbError(firstErr, "cannot delete items of DynamoDB table %s", table)
	}
	return nil
}

// scanPages runs a paginated Scan, calling fn per page.
func (s *storeImpl) scanPages(ctx context.Context, in *dynamodb.ScanInput, fn func([]map[string]types.AttributeValue) error) error {
	for {
		var out *dynamodb.ScanOutput
		err := withRetry(ctx, func() (err error) {
			out, err = s.ddb.Scan(ctx, in)
			return err
		})
		if err != nil {
			return err
		}
		if len(out.Items) > 0 {
			if err := fn(out.Items); err != nil {
				return err
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			return nil
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// batchWrite writes requests in BatchWriteItem chunks (sequentially per
// call; callers parallelise), retrying unprocessed items with backoff.
func (s *storeImpl) batchWrite(ctx context.Context, table string, reqs []types.WriteRequest) error {
	for start := 0; start < len(reqs); start += batchWriteSize {
		pending := reqs[start:min(start+batchWriteSize, len(reqs))]
		for attempt := 0; len(pending) > 0; attempt++ {
			var out *dynamodb.BatchWriteItemOutput
			err := withRetry(ctx, func() (err error) {
				out, err = s.ddb.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{table: pending},
				})
				return err
			})
			if err != nil {
				return err
			}
			pending = out.UnprocessedItems[table]
			if len(pending) == 0 {
				break
			}
			if attempt >= maxAttempts {
				return cherr.New(cherr.Unavailable, "DynamoDB kept %d items of %s unprocessed after %d attempts", len(pending), table, attempt+1)
			}
			if err := sleepBackoff(ctx, attempt); err != nil {
				return err
			}
		}
	}
	return nil
}

// parallel runs fn(0..n-1) with at most limit concurrent calls and returns
// the first error (cancelling the context passed to the remaining calls).
func parallel(ctx context.Context, n, limit int, fn func(ctx context.Context, i int) error) error {
	if n == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	sem := make(chan struct{}, limit)
	for i := 0; i < n; i++ {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			if err := fn(ctx, i); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	return firstErr
}
