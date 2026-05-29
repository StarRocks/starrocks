// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.alter.reshard.presplit;

import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.planner.LoadScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StatementBase.ExplainLevel;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * StmtExecutor → coordinator bridge for Sample-Based Tablet Pre-Split on the
 * INSERT-from-FILES path.
 *
 * <p>The hook is invoked from {@code StmtExecutor.executeStmt} BEFORE the
 * statement is planned. Running here, outside the planner's
 * {@code PlannerMetaLocker}-scoped read lock, avoids deadlock with the
 * reshard daemon's write lock on the same table — the daemon needs the
 * write lock to transition the table to {@code TABLET_RESHARD}.
 *
 * <h2>Sync-await semantics</h2>
 * <p>The hook submits the reshard job, then synchronously waits for it to
 * reach {@code FINISHED} (bounded by
 * {@code tablet_pre_split_post_submit_wait_seconds}) so the triggering
 * {@code INSERT}'s plan sees the post-split tablet layout — not just
 * subsequent loads. The Broker Load path takes the fire-and-forget route
 * instead; see {@link #awaitFinishedAllowingFallback} for why sync-await is
 * deadlock-safe for {@code INSERT INTO ... SELECT * FROM FILES(...)}
 * specifically and what happens on timeout.
 *
 * <h2>Detection</h2>
 * <p>The hook only matches the strict {@code INSERT INTO target SELECT *
 * FROM FILES(...)} shape: a single {@code FileTableFunctionRelation} as the
 * {@code SelectRelation}'s FROM, a bare {@code *} as the projection, no
 * target column list, and no WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/DISTINCT.
 *
 * <p>The reason is that the sampler executes a derived
 * {@code SELECT <sort_key> FROM FILES(<verbatim properties>)} that ignores
 * the user's projection, filter, grouping, and aliasing. Anything other
 * than a bare {@code SELECT *} would make the sampler observe a different
 * row-set or column-set than the load actually writes, producing boundaries
 * that do not match the inserted data. Joins, unions, subqueries, CTEs,
 * value lists, and multi-source variants are likewise rejected.
 *
 * <p>After the FILES schema is inferred, a final
 * {@link #schemasAlignForByPositionInsert} gate verifies that target column
 * N has the same name as FILES column N. Otherwise the load's by-position
 * mapping (file column N is written into target column N) and the sampler's
 * by-name read of the target sort-key name would resolve to different
 * columns. By-name INSERT mapping skips this check.
 *
 * <p>Sampler-executor selection is delegated to
 * {@link DefaultPreSplitPipeline#forLoadKind}: meta tier uses
 * {@link InsertFromFilesRowGroupStatisticsProvider}, data tier uses
 * {@link InsertFromFilesSampleSubqueryExecutor}. The per-path Config flag
 * {@code enable_tablet_pre_split_for_insert_from_files} defaults to
 * {@code true} as of v4.1.0 (GA flip); set it to {@code false} to disable
 * cluster-wide. The session variable {@code enable_tablet_pre_split} (also
 * default {@code true}) provides a per-session opt-out checked early in
 * this hook so a session-opt-out load does not pay the FILES schema
 * resolution.
 */
public final class InsertFromFilesPreSplitHook {

    private static final Logger LOG = LogManager.getLogger(InsertFromFilesPreSplitHook.class);

    private InsertFromFilesPreSplitHook() {
    }

    /**
     * Entry point invoked from {@code StmtExecutor.executeStmt} just before
     * {@code StatementPlanner.plan(parsedStmt, context)}.
     *
     * <p>The method is fully self-contained: any throw is swallowed and the
     * load proceeds without pre-split. The hook never propagates an exception
     * because it runs before the planner; failing here must not abort an
     * INSERT that would otherwise plan and run correctly.
     */
    public static void maybeRunPreSplit(StatementBase parsedStmt, ConnectContext context) {
        try {
            tryRunPreSplit(parsedStmt, context);
        } catch (Throwable unexpected) {
            LOG.warn("Sample-Based Tablet Pre-Split hook failed; proceeding without pre-split", unexpected);
        }
    }

    private static void tryRunPreSplit(StatementBase parsedStmt, ConnectContext context)
            throws AccessDeniedException {
        InsertStmt insertStmt = qualifyingInsertStmt(parsedStmt, context);
        if (insertStmt == null) {
            return;
        }
        FileTableFunctionRelation filesRelation = extractSingleFilesSource(insertStmt);
        if (filesRelation == null) {
            return;
        }
        // Honor the per-session opt-out before target resolution + FILES schema
        // inference. The helper bumps the disabled_by_session bvar — the
        // coordinator never sees this skip, but operators still need the bvar.
        if (PreSplitMetrics.shortCircuitOnSessionOptOut(context.getSessionVariable())) {
            return;
        }
        ResolvedTable resolvedTable = resolveEligibleTable(insertStmt, context);
        if (resolvedTable == null) {
            return;
        }
        // Authorize the side effects this hook is about to trigger: INSERT on
        // the resolved target (gates the journaled reshard job) and USAGE on
        // the non-default active warehouse (gates the sampler sub-query that
        // runs in context.currentComputeResource — the planner's later check
        // applies the same default-warehouse exemption). The planner's full
        // Authorizer.check still covers everything else (FILES function,
        // catalog, etc.); we do not call it here because the AST is not yet
        // analyzed and `Authorizer.check(stmt, ctx)` on an unnormalized
        // TableRef would throw on unqualified `INSERT INTO t`. The outer
        // try/catch in maybeRunPreSplit swallows the throw; the planner
        // re-runs its full check and surfaces the actual auth error.
        if (!context.isBypassAuthorizerCheck()) {
            Authorizer.checkTableAction(context,
                    resolvedTable.database().getFullName(),
                    resolvedTable.olapTable().getName(),
                    PrivilegeType.INSERT);
            Warehouse currentWarehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getWarehouse(context.getCurrentComputeResource().getWarehouseId());
            if (currentWarehouse.getId() != WarehouseManager.DEFAULT_WAREHOUSE_ID) {
                Authorizer.checkWarehouseAction(context, currentWarehouse.getName(), PrivilegeType.USAGE);
            }
        }
        TableFunctionTable sourceTable = resolveSourceTable(insertStmt, filesRelation, context);
        if (sourceTable == null) {
            return;
        }
        if (!schemasAlignForByPositionInsert(insertStmt, resolvedTable.olapTable(), sourceTable)) {
            return;
        }
        // Branch on partitioned vs unpartitioned. Partitioned tables go through the
        // multi-partition flow (sampler → grouper → submitForPartitionsCombined),
        // which forces the data tier sub-query sampler (the meta tier's per-column min/max
        // is fundamentally lossy under expression-based partitioning) and submits ONE
        // combined reshard spanning every predicted partition. Unpartitioned tables
        // keep the legacy single-partition path.
        if (resolvedTable.olapTable().getPartitionInfo().isPartitioned()) {
            runMultiPartitionFlow(resolvedTable.database(), resolvedTable.olapTable(), sourceTable, context);
        } else {
            runSinglePartitionFlow(resolvedTable.database(), resolvedTable.olapTable(), sourceTable, context);
        }
    }

    /**
     * Legacy single-partition path: resolve the unique partition + base tablet,
     * then go through {@link DefaultPreSplitPipeline} + {@link TabletPreSplitCoordinator#submitAsynchronously}
     * + {@link #awaitFinishedAllowingFallback}. Unpartitioned-table behavior is
     * unchanged.
     */
    private static void runSinglePartitionFlow(
            Database database, OlapTable table, TableFunctionTable sourceTable, ConnectContext context) {
        PreSplitTargets.EligibleTarget target = PreSplitTargets.findEligibleTarget(database, table);
        if (target == null) {
            return;
        }
        submitToCoordinator(target, sourceTable, context);
    }

    /**
     * Multi-partition path: sample the load's input via the data tier,
     * group sample rows by predicted partition value, pre-create missing
     * partitions, and submit ONE combined reshard via
     * {@link TabletPreSplitCoordinator#submitForPartitionsCombined}.
     *
     * <p>{@link #awaitCombinedJobAllowingFallback} waits once on the combined
     * job before the planner runs; timeout / abort proceeds against the
     * currently visible layout without aborting the INSERT.
     */
    private static void runMultiPartitionFlow(
            Database database, OlapTable table, TableFunctionTable sourceTable, ConnectContext context) {
        ComputeResource computeResource = context.getCurrentComputeResource();
        int activeComputeNodeCount = Math.max(1,
                LoadScanNode.getAvailableComputeNodes(computeResource).size());
        long fileTotalBytes = sumFileBytes(sourceTable);

        SampleSet samples = runDataTierSampler(table, sourceTable, computeResource);
        if (samples == null) {
            return;
        }

        List<PartitionSamples> groups = PartitionSampleGrouper.group(
                samples, table, context, database.getId(), fileTotalBytes);
        if (groups.isEmpty()) {
            // Grouper already recorded the skip reason bvar.
            return;
        }

        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                database, table, groups, activeComputeNodeCount, context);
        LOG.info("Sample-Based Tablet Pre-Split (multi-partition) outcome for table {}: {}",
                table.getName(), outcome);

        if (outcome instanceof PreSplitOutcome.SubmittedCombined submittedCombined) {
            // Single call site: awaitCombinedJobAllowingFallback is invoked ONCE per
            // combined-job submission, NOT once per PartitionSamples. The coordinator
            // batches every contributing partition's boundaries into ONE TabletReshardJob
            // (see SplitTabletJobFactory.forExternalBoundariesMultiTablet), so the hook
            // only blocks on a single reshard before plan() — bounded by
            // tablet_pre_split_post_submit_wait_seconds regardless of partition count.
            awaitCombinedJobAllowingFallback(table, submittedCombined.combinedJob());
        }
    }

    /**
     * Run the data-tier sampler directly (no {@link DefaultPreSplitPipeline}).
     * The pipeline orchestrates plan + submit for a single partition; the
     * multi-partition flow plans + submits per-partition inside
     * {@link TabletPreSplitCoordinator#submitForPartitionsCombined}, so we only
     * need the sample step here. Sort-key columns drive boundary planning;
     * partition-source columns let the grouper project per-row partition values
     * for bucketing.
     *
     * @return the sampled rows, or {@code null} when the sampler failed
     *         (caller no-ops; bvar recorded inline).
     */
    private static SampleSet runDataTierSampler(
            OlapTable table, TableFunctionTable sourceTable, ComputeResource computeResource) {
        try {
            List<Column> sortKey = MetaUtils.getRangeDistributionColumns(table);
            List<Column> partitionSourceColumns =
                    table.getPartitionInfo().getPartitionColumns(table.getIdToColumn());
            InsertFromFilesScanContext scanContext =
                    new InsertFromFilesScanContext(sourceTable, computeResource);
            SampleRequest request = new SampleRequest(
                    scanContext, sortKey, partitionSourceColumns,
                    Config.tablet_pre_split_sample_byte_limit, /*seed*/ 0L);
            Sampler sampler = new ReservoirSampler(new InsertFromFilesSampleSubqueryExecutor());
            return sampler.sample(request);
        } catch (StarRocksException sampleFailure) {
            LOG.info("Pre-split skipped for table {}: data-tier sampling failed — {}",
                    table.getName(), sampleFailure.getMessage());
            PreSplitMetrics.recordSamplerFailed(SkipReason.SAMPLE_FAILED);
            return null;
        } catch (RuntimeException sampleFailure) {
            LOG.warn("Pre-split skipped for table {}: data-tier sampling errored — {}",
                    table.getName(), sampleFailure.getMessage());
            PreSplitMetrics.recordSamplerFailed(SkipReason.SAMPLE_FAILED);
            return null;
        }
    }

    /**
     * Centralizes the cheap "no-mutating-side-effects" pre-filters so the rest
     * of {@link #tryRunPreSplit} reads as a resolve-and-submit pipeline.
     *
     * @return the {@link InsertStmt} when {@code parsedStmt} is an
     *         INSERT-from-FILES candidate that should reach the eligibility
     *         gate, or {@code null} when any pre-filter rejects (caller no-ops).
     */
    private static InsertStmt qualifyingInsertStmt(StatementBase parsedStmt, ConnectContext context) {
        if (!Config.enable_tablet_pre_split_for_insert_from_files) {
            return null;
        }
        if (!(parsedStmt instanceof InsertStmt insertStmt)) {
            return null;
        }
        // Read-only EXPLAIN (anything except EXPLAIN ANALYZE) must not mutate
        // tablet metadata. Mirrors StatementPlanner.beginTransaction's skip.
        if (insertStmt.isExplain() && !ExplainLevel.ANALYZE.equals(insertStmt.getExplainLevel())) {
            return null;
        }
        // INSERT OVERWRITE's first planning pass intentionally has no txn yet —
        // the overwrite handler creates a separate overwrite job and re-plans.
        // Submitting a split during the first pass would race the overwrite's
        // own table-state changes.
        if (insertStmt.isOverwrite() && !insertStmt.hasOverwriteJob()) {
            return null;
        }
        // Skip when the session already holds an open transaction (explicit
        // BEGIN ... INSERT ... COMMIT) or the InsertOverwriteJobRunner has
        // pre-set the stmt txn id; in either case the reshard daemon's
        // cleanup-phase prev-txn wait would block on that transaction.
        if (context.getTxnId() != 0 || insertStmt.getTxnId() != DmlStmt.INVALID_TXN_ID) {
            return null;
        }
        return insertStmt;
    }

    /**
     * @return the FROM relation when the INSERT shape is exactly
     *         {@code INSERT INTO target SELECT * FROM FILES(...)} with no
     *         target column list, no joins/unions/subqueries/CTEs, and no
     *         projection/filter/grouping/ordering/limit/distinct that would
     *         decouple the sampled row-set from the inserted row-set;
     *         {@code null} otherwise.
     */
    private static FileTableFunctionRelation extractSingleFilesSource(InsertStmt insertStmt) {
        if (insertStmt.getTargetColumnNames() != null) {
            return null;
        }
        if (insertStmt.getQueryStatement() == null) {
            return null;
        }
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        if (queryRelation == null || !queryRelation.getCteRelations().isEmpty()) {
            return null;
        }
        if (!(queryRelation instanceof SelectRelation selectRelation)) {
            return null;
        }
        if (!isStraightStarProjection(selectRelation)) {
            return null;
        }
        Relation from = selectRelation.getRelation();
        return from instanceof FileTableFunctionRelation filesRelation ? filesRelation : null;
    }

    /**
     * Verifies that, under by-position INSERT mapping, the target column at
     * every ordinal has the same name as the FILES column at the same ordinal.
     *
     * <p>Required because the load and the sampler resolve the source column
     * differently: the load writes FILES column N into target column N (by
     * position), while the sampler reads the source by name (it issues
     * {@code SELECT <target_sort_key_name> FROM FILES(...)}). When FILES has
     * the columns in a different order than the target, the two resolutions
     * diverge — the sampler computes boundaries from a different column than
     * the load actually writes, producing wrong split points.
     *
     * <p>The check is skipped when the INSERT uses by-name mapping
     * ({@link InsertStmt#isColumnMatchByName()}): in that mode the load also
     * pairs columns by name, so the sampler's by-name read matches.
     *
     * <p>Package-private (not private) so the unit test can drive it without
     * mocking the full eligibility chain that precedes it.
     */
    static boolean schemasAlignForByPositionInsert(
            InsertStmt insertStmt, OlapTable targetTable, TableFunctionTable sourceTable) {
        if (insertStmt.isColumnMatchByName()) {
            return true;
        }
        List<Column> targetColumns = targetTable.getBaseSchemaWithoutGeneratedColumn();
        List<Column> sourceColumns = sourceTable.getFullSchema();
        if (targetColumns.size() != sourceColumns.size()) {
            return false;
        }
        for (int ordinal = 0; ordinal < targetColumns.size(); ordinal++) {
            String targetName = targetColumns.get(ordinal).getName();
            String sourceName = sourceColumns.get(ordinal).getName();
            if (!targetName.equalsIgnoreCase(sourceName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Verifies the SelectRelation is exactly {@code SELECT * FROM <from>}: a
     * single bare star projection with no qualifier, no {@code EXCLUDE}, no
     * alias, no {@code DISTINCT}, and no WHERE/GROUP BY/HAVING/ORDER BY/LIMIT.
     *
     * <p>The sampler synthesizes its own {@code SELECT <sort_key> FROM
     * FILES(<verbatim properties>)} and ignores any wrapper. Any projection
     * transform, filter, or row-changing clause here would make the sampled
     * boundaries diverge from what the load actually writes.
     */
    private static boolean isStraightStarProjection(SelectRelation selectRelation) {
        SelectList selectList = selectRelation.getSelectList();
        if (selectList == null || selectList.isDistinct()) {
            return false;
        }
        List<SelectListItem> items = selectList.getItems();
        if (items.size() != 1) {
            return false;
        }
        SelectListItem onlyItem = items.get(0);
        if (!onlyItem.isStar()) {
            return false;
        }
        if (onlyItem.getTblName() != null) {
            return false;
        }
        if (!onlyItem.getExcludedColumns().isEmpty()) {
            return false;
        }
        if (onlyItem.getAlias() != null) {
            return false;
        }
        return !selectRelation.hasWhereClause()
                && !selectRelation.hasGroupByClause()
                && !selectRelation.hasHavingClause()
                && !selectRelation.hasOrderByClause()
                && !selectRelation.hasLimit();
    }

    /**
     * Walks the catalog and applies the table-level eligibility gate
     * ({@link PreSplitTargets#findEligibleTable}). Per-partition checks
     * (single physical partition, single base tablet, empty partition) are
     * deferred so the partitioned multi-partition flow can run them per-bucket
     * after pre-create; the legacy single-partition flow continues to apply
     * them via {@link PreSplitTargets#findEligibleTarget} downstream.
     *
     * @return the resolved {@link ResolvedTable}, or {@code null} when target
     *         resolution or the table-level eligibility check fails (caller
     *         no-ops; the table-level helper records the eligibility-skip
     *         bvar).
     */
    private static ResolvedTable resolveEligibleTable(InsertStmt insertStmt, ConnectContext context) {
        TableRef normalizedTableRef = normalizeTableRefOrNull(insertStmt, context);
        if (normalizedTableRef == null) {
            return null;
        }
        Database database = resolveDatabase(normalizedTableRef, context);
        if (database == null) {
            return null;
        }
        OlapTable olapTable = resolveOlapTarget(normalizedTableRef, database, context);
        if (olapTable == null) {
            return null;
        }
        SkipReason tableLevelSkip = PreSplitTargets.findEligibleTable(database, olapTable);
        if (tableLevelSkip != null) {
            PreSplitMetrics.recordEligibilitySkip(tableLevelSkip);
            return null;
        }
        return new ResolvedTable(database, olapTable);
    }

    /** Database + table bundle returned by {@link #resolveEligibleTable}. */
    private record ResolvedTable(Database database, OlapTable olapTable) { }

    /**
     * Triggers FILES() schema inference via the analyzer's lock-free path and
     * returns the resolved {@link TableFunctionTable}. The same call site is
     * used inside {@link com.starrocks.sql.StatementPlanner} for INSERT plans
     * that mix FILES() with normal tables.
     */
    private static TableFunctionTable resolveSourceTable(
            InsertStmt insertStmt, FileTableFunctionRelation filesRelation, ConnectContext context) {
        try {
            new QueryAnalyzer(context).analyzeFilesOnly(insertStmt.getQueryStatement());
        } catch (Throwable failure) {
            LOG.info("Sample-Based Tablet Pre-Split: lock-free FILES() analyze failed for table {}; skipping: {}",
                    targetNameForLog(insertStmt), failure.getMessage());
            return null;
        }
        Table boundTable = filesRelation.getTable();
        return boundTable instanceof TableFunctionTable resolved ? resolved : null;
    }

    private static void submitToCoordinator(
            PreSplitTargets.EligibleTarget target, TableFunctionTable sourceTable, ConnectContext context) {
        ComputeResource computeResource = context.getCurrentComputeResource();
        InsertFromFilesScanContext scanContext = new InsertFromFilesScanContext(sourceTable, computeResource);
        int activeComputeNodeCount = Math.max(1,
                LoadScanNode.getAvailableComputeNodes(computeResource).size());
        long fileTotalBytes = sumFileBytes(sourceTable);

        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                target.database(), target.olapTable(), target.oldTabletId(), fileTotalBytes,
                LoadKind.INSERT_FROM_FILES);

        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitAsynchronously(
                target.database(), target.olapTable(), target.partitionId(), scanContext,
                LoadKind.INSERT_FROM_FILES, pipeline, activeComputeNodeCount);
        LOG.info("Sample-Based Tablet Pre-Split outcome for table {}: {}",
                target.olapTable().getName(), outcome);

        if (outcome instanceof PreSplitOutcome.Submitted submitted) {
            awaitFinishedAllowingFallback(target.olapTable(), pipeline, submitted.preparedJob());
        }
    }

    /**
     * Fail-safe variant of {@link TabletPreSplitCoordinator#awaitFinishedAndRecordMetrics}:
     * any timeout or wait failure is logged and the INSERT proceeds without
     * abort — its plan sees whatever tablet layout is currently visible
     * (could be the original layout if the daemon hasn't progressed, or
     * partially / fully post-split if it raced past the wait giving up).
     * {@code tablet_pre_split_load_abort} is intentionally not incremented
     * (the INSERT itself is not aborted) — the shared helper updates the
     * latency histogram and hard-cap counter.
     *
     * <p>Sync-await is deadlock-safe here specifically because this hook runs
     * in {@code StmtExecutor.executeStmt} <b>before</b>
     * {@code StatementPlanner.plan()} opens the load transaction. The reshard
     * daemon's cleaning-phase {@code isPreviousTransactionsFinished} wait
     * therefore does not include the not-yet-allocated load txn, so the AB-BA
     * cycle the Broker Load path has to dodge with fire-and-forget does not
     * exist on this path.
     */
    private static void awaitFinishedAllowingFallback(
            OlapTable olapTable, DefaultPreSplitPipeline pipeline,
            PreSplitPipeline.PreparedReshardJob preparedJob) {
        Duration postSubmitTimeout = Duration.ofSeconds(Config.tablet_pre_split_post_submit_wait_seconds);
        try {
            TabletPreSplitCoordinator.awaitFinishedAndRecordMetrics(pipeline, preparedJob, postSubmitTimeout);
        } catch (PreSplitPostSubmitTimeoutException timeout) {
            LOG.warn("Pre-split awaitFinished timed out for table {} after {}s; "
                            + "INSERT will proceed without abort against the currently visible layout: {}",
                    olapTable.getName(), postSubmitTimeout.toSeconds(), timeout.getMessage());
        } catch (StarRocksException waitFailure) {
            LOG.warn("Pre-split awaitFinished failed for table {}; "
                            + "INSERT will proceed without abort against the currently visible layout: {}",
                    olapTable.getName(), waitFailure.getMessage());
        }
    }

    /**
     * Multi-partition variant of {@link #awaitFinishedAllowingFallback}. Polls
     * {@link TabletReshardJobMgr} for the combined job's terminal state,
     * bounded by {@code tablet_pre_split_post_submit_wait_seconds}.
     *
     * <p>The combined-job path bypasses {@link PreSplitPipeline}'s plan/submit
     * stages (the coordinator's {@code submitForPartitionsCombined} builds and
     * admits the {@link TabletReshardJob} directly), so there is no
     * {@link PreSplitPipeline.PreparedReshardJob} wrapper to feed into the
     * pipeline's polling helper. Polling logic here mirrors
     * {@link DefaultPreSplitPipeline#awaitFinished}'s loop: poll
     * {@code TabletReshardJobMgr.getTabletReshardJob} on a fixed interval,
     * return when the job reaches a final state or the deadline expires.
     *
     * <p>Same fail-safe semantics as {@link #awaitFinishedAllowingFallback}:
     * timeout / abort / wait failure is logged and the INSERT proceeds against
     * whatever tablet layout is currently visible — never aborts the load. The
     * post-submit latency histogram is updated unconditionally; the hard-cap
     * counter is bumped on timeout, mirroring
     * {@link TabletPreSplitCoordinator#awaitFinishedAndRecordMetrics}.
     */
    // Package-private for tests: drives the post-submit await loop without
    // routing through the full hook (which depends on Authorizer + catalog state).
    static void awaitCombinedJobAllowingFallback(OlapTable olapTable, TabletReshardJob combinedJob) {
        Duration postSubmitTimeout = Duration.ofSeconds(Config.tablet_pre_split_post_submit_wait_seconds);
        long jobId = combinedJob.getJobId();
        TabletReshardJobMgr tabletReshardJobManager =
                GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Instant deadline = Instant.now().plus(postSubmitTimeout);
        long postSubmitStartMillis = System.currentTimeMillis();
        try {
            while (true) {
                TabletReshardJob latest = tabletReshardJobManager.getTabletReshardJob(jobId);
                if (latest == null) {
                    LOG.warn("Pre-split combined job {} disappeared for table {}; "
                                    + "INSERT will proceed against the currently visible layout",
                            jobId, olapTable.getName());
                    return;
                }
                TabletReshardJob.JobState state = latest.getJobState();
                if (state == TabletReshardJob.JobState.FINISHED) {
                    return;
                }
                if (state.isFinalState()) {
                    LOG.warn("Pre-split combined job {} aborted for table {}: {}; "
                                    + "INSERT will proceed against the currently visible layout",
                            jobId, olapTable.getName(), latest.getErrorMessage());
                    return;
                }
                if (Instant.now().isAfter(deadline)) {
                    if (MetricRepo.hasInit) {
                        MetricRepo.COUNTER_TABLET_PRE_SPLIT_POST_SUBMIT_HARD_CAP.increase(1L);
                    }
                    LOG.warn("Pre-split combined job {} did not finish in {}s for table {}; "
                                    + "INSERT will proceed without abort against the currently visible layout",
                            jobId, postSubmitTimeout.toSeconds(), olapTable.getName());
                    return;
                }
                try {
                    Thread.sleep(DefaultPreSplitPipeline.DEFAULT_POLL_INTERVAL.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Pre-split combined-job await interrupted for table {}; "
                                    + "INSERT will proceed against the currently visible layout",
                            olapTable.getName());
                    return;
                }
            }
        } finally {
            if (MetricRepo.hasInit) {
                MetricRepo.HISTO_TABLET_PRE_SPLIT_POST_SUBMIT_WAIT_MS.update(
                        System.currentTimeMillis() - postSubmitStartMillis);
            }
        }
    }

    /**
     * Normalizes the InsertStmt's tableRef so the catalog/db/table parts are
     * fully qualified — {@code TableRef.getDbName()} returns null for
     * unqualified {@code INSERT INTO t} until the session's current
     * catalog/db is resolved. Returns {@code null} on any failure; the hook
     * then no-ops.
     */
    private static TableRef normalizeTableRefOrNull(InsertStmt insertStmt, ConnectContext context) {
        if (insertStmt.getTableRef() == null) {
            return null;
        }
        try {
            return AnalyzerUtils.normalizedTableRef(insertStmt.getTableRef(), context);
        } catch (Throwable failure) {
            LOG.info("Sample-Based Tablet Pre-Split: tableRef normalization failed for {}; skipping: {}",
                    targetNameForLog(insertStmt), failure.getMessage());
            return null;
        }
    }

    private static Database resolveDatabase(TableRef normalizedTableRef, ConnectContext context) {
        String catalogName = normalizedTableRef.getCatalogName();
        String databaseName = normalizedTableRef.getDbName();
        if (catalogName == null || databaseName == null) {
            return null;
        }
        return GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, catalogName, databaseName);
    }

    private static OlapTable resolveOlapTarget(
            TableRef normalizedTableRef, Database database, ConnectContext context) {
        TableName qualifiedTableName = TableName.fromTableRef(normalizedTableRef);
        Table table = MetaUtils.getSessionAwareTable(context, database, qualifiedTableName);
        return table instanceof OlapTable olapTable ? olapTable : null;
    }

    private static long sumFileBytes(TableFunctionTable sourceTable) {
        long total = 0L;
        for (TBrokerFileStatus fileStatus : sourceTable.loadFileList()) {
            if (fileStatus != null) {
                total += fileStatus.size;
            }
        }
        return total;
    }

    private static String targetNameForLog(InsertStmt insertStmt) {
        TableRef tableRef = insertStmt.getTableRef();
        return tableRef == null ? "<unknown>" : tableRef.getTableName();
    }
}
