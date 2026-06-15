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

import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StatementBase.ExplainLevel;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * StmtExecutor &rarr; coordinator bridge for Sample-Based Tablet Pre-Split on
 * the {@code INSERT INTO target SELECT ... FROM <olap source>} path.
 *
 * <p>The hook is invoked from {@code StmtExecutor.executeStmt} BEFORE the
 * statement is planned. Running here, outside the planner's
 * {@code PlannerMetaLocker}-scoped read lock, avoids deadlock with the reshard
 * daemon's write lock on the same table.
 *
 * <h2>Sync-await semantics</h2>
 * <p>The hook submits the reshard job, then synchronously waits for it to reach
 * {@code FINISHED} (bounded by {@code tablet_pre_split_post_submit_wait_seconds})
 * so the triggering {@code INSERT}'s plan sees the post-split tablet layout. The
 * shared fail-safe wait helpers live on
 * {@link TabletPreSplitCoordinator#awaitFinishedAllowingFallback} and
 * {@link TabletPreSplitCoordinator#awaitCombinedJobAllowingFallback}; both paths
 * log + proceed on timeout, never abort the load.
 *
 * <h2>Detection</h2>
 * <p>Unlike the FILES path, the OLAP source admits a richer (but still tightly
 * constrained) shape: the projection may be a bare {@code SELECT *} OR a list of
 * bare column references, and a deterministic WHERE clause is allowed because the
 * sampler copies it verbatim into the sampling sub-query. The source must be a
 * single plain {@code TableRelation} (no PARTITION/TABLET/TABLESAMPLE/temporal
 * modifier), and CTEs, joins, unions, subqueries, DISTINCT, GROUP BY, HAVING,
 * ORDER BY, LIMIT, and an explicit target column list are all rejected so the
 * sampled row-set and column-set match what the load writes.
 *
 * <h2>Source authorization and policy gate</h2>
 * <p>The sampler runs as {@code UserIdentity.ROOT} in a statistics
 * {@code ConnectContext}, so the user's SELECT privilege on the source is not
 * otherwise enforced and row-access / column-masking policies would not apply to
 * the ROOT sample. The hook therefore re-checks the user's SELECT privilege on
 * the source and skips pre-split entirely when a row-access or column-masking
 * policy is attached, since a policy-filtered load diverges from a ROOT
 * full-scan sample.
 *
 * <p>Sampler-executor selection is delegated to
 * {@link DefaultPreSplitPipeline#forLoadKind}, which forces the data tier for
 * {@link LoadKind#INSERT_FROM_TABLE} (an OLAP source has no Parquet/ORC footer
 * for the meta tier to read). The per-path Config flag
 * {@code enable_tablet_pre_split_for_insert_from_table} gates the path
 * cluster-wide; the session variable {@code enable_tablet_pre_split} provides a
 * per-session opt-out checked early in this hook.
 */
public final class InsertFromTablePreSplitHook {

    private static final Logger LOG = LogManager.getLogger(InsertFromTablePreSplitHook.class);

    private InsertFromTablePreSplitHook() {
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
            LOG.warn("Sample-Based Tablet Pre-Split (INSERT-from-table) hook failed; "
                    + "proceeding without pre-split", unexpected);
        }
    }

    private static void tryRunPreSplit(StatementBase parsedStmt, ConnectContext context)
            throws AccessDeniedException {
        InsertStmt insertStmt = qualifyingInsertStmt(parsedStmt, context);
        if (insertStmt == null) {
            return;
        }
        SingleTableSource singleTableSource = extractSingleTableSource(insertStmt);
        if (singleTableSource == null) {
            return;
        }
        // Honor the per-session opt-out before target / source resolution.
        if (PreSplitMetrics.shortCircuitOnSessionOptOut(context.getSessionVariable())) {
            return;
        }
        ResolvedTable resolvedTable = resolveEligibleTable(insertStmt, context);
        if (resolvedTable == null) {
            return;
        }
        // Authorize the side effects this hook is about to trigger on the
        // target: INSERT (gates the journaled reshard job) and USAGE on the
        // non-default active warehouse (gates the sampler sub-query that runs in
        // context.currentComputeResource). The outer try/catch swallows the
        // throw; the planner re-runs its full check and surfaces the actual
        // auth error.
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
        ResolvedSource resolvedSource = resolveSourceTable(singleTableSource.sourceRelation(), context);
        if (resolvedSource == null) {
            return;
        }
        // Re-check the user's SELECT privilege on the source and reject row /
        // column policies: the ROOT sample would otherwise read rows or values
        // the policy-filtered load never sees, diverging the boundaries.
        if (!sourceAuthorizedAndPolicyFree(resolvedSource, context)) {
            return;
        }
        // WHERE gate: the predicate is copied verbatim into the ROOT sampling
        // sub-query, so reject anything that resolves differently in that
        // context (non-deterministic / information functions, context
        // variables, subqueries, foreign-qualified columns).
        Expr where = singleTableSource.selectRelation().getWhereClause();
        if (!SamplingPredicateGate.isDeterministicAndSafe(
                where, resolvedSource.normalizedName(), resolvedSource.sourceAlias())) {
            return;
        }
        String wherePredicateSql = where == null ? null : SamplingPredicateGate.toSql(where);

        OlapTable target = resolvedTable.olapTable();
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(target);
        List<Column> partitionColumns =
                target.getPartitionInfo().getPartitionColumns(target.getIdToColumn());
        InsertSelectSourceColumns mapping = InsertSelectSourceColumns.resolve(
                insertStmt, singleTableSource.selectRelation(), target, resolvedSource.sourceTable(),
                resolvedSource.normalizedName(), resolvedSource.sourceAlias(),
                sortKeyColumns, partitionColumns);
        if (mapping == null) {
            return;
        }

        // The scan context depends only on the SOURCE table + resolved mapping —
        // it is independent of which eligible TARGET partition the flow picks —
        // so build it once here and share it across both partition branches.
        ComputeResource computeResource = context.getCurrentComputeResource();
        InsertFromTableScanContext scanContext = new InsertFromTableScanContext(
                resolvedSource.sourceTable(), resolvedSource.sourceFromSql(),
                mapping.sortKeySourceColumnNames(), mapping.partitionSourceColumnNames(),
                wherePredicateSql, computeResource);

        if (target.getPartitionInfo().isPartitioned()) {
            runMultiPartitionFlow(resolvedTable.database(), target, resolvedSource.sourceTable(),
                    scanContext, sortKeyColumns, partitionColumns, context);
        } else {
            runSinglePartitionFlow(resolvedTable.database(), target, resolvedSource.sourceTable(),
                    scanContext, context);
        }
    }

    /**
     * Legacy single-partition path: resolve the unique partition + base tablet,
     * then go through {@link DefaultPreSplitPipeline}
     * + {@link TabletPreSplitCoordinator#submitAsynchronously}
     * + {@link TabletPreSplitCoordinator#awaitFinishedAllowingFallback} using the
     * shared {@link InsertFromTableScanContext}.
     */
    private static void runSinglePartitionFlow(
            Database database, OlapTable table, OlapTable sourceTable,
            InsertFromTableScanContext scanContext, ConnectContext context) {
        PreSplitTargets.EligibleTarget target = PreSplitTargets.findEligibleTarget(database, table);
        if (target == null) {
            return;
        }
        int activeComputeNodeCount = TabletReshardUtils.computeNodeCount(context.getCurrentComputeResource());
        long estimatedBytes = Math.max(0L, sourceTable.getDataSize());

        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.forLoadKind(
                target.database(), target.olapTable(), target.oldTabletId(), estimatedBytes,
                LoadKind.INSERT_FROM_TABLE);

        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitAsynchronously(
                target.database(), target.olapTable(), target.partitionId(), scanContext,
                LoadKind.INSERT_FROM_TABLE, pipeline, activeComputeNodeCount);
        LOG.info("Sample-Based Tablet Pre-Split (INSERT-from-table) outcome for table {}: {}",
                target.olapTable().getName(), outcome);

        if (outcome instanceof PreSplitOutcome.Submitted submitted) {
            // shouldAbort = context::isKilled: KILL <query_id> during the wait
            // releases the session thread promptly.
            TabletPreSplitCoordinator.awaitFinishedAllowingFallback(
                    LoadKind.INSERT_FROM_TABLE, target.olapTable(), pipeline, submitted.preparedJob(),
                    context::isKilled);
        }
    }

    /**
     * Multi-partition path: sample the load's input via the data tier, group
     * sample rows by predicted partition value, pre-create missing partitions,
     * and submit ONE combined reshard via
     * {@link TabletPreSplitCoordinator#submitForPartitionsCombined}.
     *
     * <p>{@link TabletPreSplitCoordinator#awaitCombinedJobAllowingFallback}
     * waits once on the combined job before the planner runs; timeout / abort
     * proceeds against the currently visible layout without aborting the INSERT.
     */
    private static void runMultiPartitionFlow(
            Database database, OlapTable table, OlapTable sourceTable,
            InsertFromTableScanContext scanContext,
            List<Column> sortKeyColumns, List<Column> partitionColumns, ConnectContext context) {
        int activeComputeNodeCount = TabletReshardUtils.computeNodeCount(context.getCurrentComputeResource());
        long estimatedBytes = Math.max(0L, sourceTable.getDataSize());

        SampleSet samples = runDataTierSampler(table, scanContext, sortKeyColumns, partitionColumns);
        if (samples == null) {
            return;
        }

        List<PartitionSamples> groups = PartitionSampleGrouper.group(
                samples, table, context, database.getId(), estimatedBytes);
        if (groups.isEmpty()) {
            // Grouper already recorded the skip reason bvar.
            return;
        }

        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitForPartitionsCombined(
                database, table, groups, activeComputeNodeCount, context);
        LOG.info("Sample-Based Tablet Pre-Split (INSERT-from-table, multi-partition) outcome for table {}: {}",
                table.getName(), outcome);

        if (outcome instanceof PreSplitOutcome.SubmittedCombined submittedCombined) {
            TabletPreSplitCoordinator.awaitCombinedJobAllowingFallback(
                    LoadKind.INSERT_FROM_TABLE, table, submittedCombined.combinedJob(), context::isKilled);
        }
    }

    /**
     * Run the data-tier sampler directly (no {@link DefaultPreSplitPipeline}).
     * The multi-partition flow plans + submits per-partition inside
     * {@link TabletPreSplitCoordinator#submitForPartitionsCombined}, so only the
     * sample step is needed here. Sort-key columns drive boundary planning;
     * partition-source columns let the grouper project per-row partition values.
     *
     * @return the sampled rows, or {@code null} when the sampler failed
     *         (caller no-ops; bvar recorded inline).
     */
    private static SampleSet runDataTierSampler(
            OlapTable table, InsertFromTableScanContext scanContext,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        try {
            // Cap the BE sample at the pre-submit budget — the same bound the
            // single-partition path applies via DefaultPreSplitPipeline. This
            // path bypasses the pipeline, so without the cap the sample would
            // run until the default query_timeout.
            SampleRequest request = new SampleRequest(
                    scanContext, sortKeyColumns, partitionColumns,
                    Config.tablet_pre_split_sample_byte_limit, /*seed*/ 0L)
                    .withQueryTimeoutSeconds((int) Config.tablet_pre_split_pre_submit_timeout_seconds);
            Sampler sampler = new ReservoirSampler(new InsertFromTableSampleSubqueryExecutor());
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
     *         INSERT-from-table candidate that should reach the eligibility
     *         gate, or {@code null} when any pre-filter rejects (caller no-ops).
     */
    private static InsertStmt qualifyingInsertStmt(StatementBase parsedStmt, ConnectContext context) {
        if (!Config.enable_tablet_pre_split_for_insert_from_table) {
            // Record here: the coordinator's checkConfigAndSession is never
            // reached on this early return, so it can't bump the bucket itself.
            PreSplitMetrics.recordEligibilitySkip(SkipReason.DISABLED_BY_CONFIG);
            return null;
        }
        if (!(parsedStmt instanceof InsertStmt insertStmt)) {
            return null;
        }
        // Read-only EXPLAIN (anything except EXPLAIN ANALYZE) must not mutate
        // tablet metadata.
        if (insertStmt.isExplain() && !ExplainLevel.ANALYZE.equals(insertStmt.getExplainLevel())) {
            return null;
        }
        // INSERT OVERWRITE's first planning pass intentionally has no txn yet —
        // the overwrite handler creates a separate overwrite job and re-plans.
        if (insertStmt.isOverwrite() && !insertStmt.hasOverwriteJob()) {
            return null;
        }
        // Skip when the session already holds an open transaction or the
        // InsertOverwriteJobRunner has pre-set the stmt txn id; in either case
        // the reshard daemon's cleanup-phase prev-txn wait would block on that
        // transaction.
        if (context.getTxnId() != 0 || insertStmt.getTxnId() != DmlStmt.INVALID_TXN_ID) {
            return null;
        }
        return insertStmt;
    }

    /**
     * @return the SELECT body + source relation when the INSERT shape is
     *         {@code INSERT INTO target SELECT * | <bare cols> FROM <single
     *         plain table>} with no target column list, no CTE/join/union/
     *         subquery, and no DISTINCT/GROUP BY/HAVING/ORDER BY/LIMIT;
     *         {@code null} otherwise. A deterministic WHERE clause is allowed —
     *         it is gated downstream and copied verbatim into the sampler.
     */
    private static SingleTableSource extractSingleTableSource(InsertStmt insertStmt) {
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
        if (!isMappableProjection(selectRelation)) {
            return null;
        }
        Relation from = selectRelation.getRelation();
        if (!(from instanceof TableRelation sourceRelation)) {
            return null;
        }
        if (!isPlainTableReference(sourceRelation)) {
            return null;
        }
        return new SingleTableSource(selectRelation, sourceRelation);
    }

    /**
     * Verifies the projection is a bare {@code SELECT *} (single star, no
     * qualifier / EXCLUDE / alias) OR a list of bare column references
     * ({@code SlotRef}), with no DISTINCT and no
     * GROUP BY / HAVING / ORDER BY / LIMIT. A WHERE clause is allowed (gated
     * separately). Any expression projection, qualified / excluded star, or
     * row-changing clause would decouple the sampled row-set from what the load
     * writes.
     */
    private static boolean isMappableProjection(SelectRelation selectRelation) {
        SelectList selectList = selectRelation.getSelectList();
        if (selectList == null || selectList.isDistinct()) {
            return false;
        }
        List<SelectListItem> items = selectList.getItems();
        if (items.isEmpty()) {
            return false;
        }
        SelectListItem first = items.get(0);
        if (items.size() == 1 && first.isStar()) {
            if (first.getTblName() != null || !first.getExcludedColumns().isEmpty() || first.getAlias() != null) {
                return false;
            }
        } else {
            for (SelectListItem item : items) {
                if (item.isStar() || !(item.getExpr() instanceof SlotRef)) {
                    return false;
                }
            }
        }
        return !selectRelation.hasGroupByClause()
                && !selectRelation.hasHavingClause()
                && !selectRelation.hasOrderByClause()
                && !selectRelation.hasLimit();
    }

    /**
     * Rejects any source-slice modifier on the FROM-clause table relation.
     * Each modifier (explicit partition / tablet / replica selection, table
     * hints such as {@code _META_}/{@code _BINLOG_}/{@code _SYNC_MV_}, table
     * sampling, time-travel, and GTID) would make the sampler observe a
     * different row-set than a plain full scan, so pre-split is skipped.
     * Over-rejection is safe: every accessor that exposes a modifier is checked.
     */
    private static boolean isPlainTableReference(TableRelation relation) {
        return relation.getPartitionNames() == null
                && (relation.getTabletIds() == null || relation.getTabletIds().isEmpty())
                && (relation.getReplicaIds() == null || relation.getReplicaIds().isEmpty())
                && (relation.getTableHints() == null || relation.getTableHints().isEmpty())
                && relation.getSampleClause() == null
                && relation.getQueryPeriod() == null
                && relation.getQueryPeriodString() == null
                && relation.getTvrVersionRange() == null
                && relation.getGtid() == 0;
    }

    /**
     * Walks the catalog and applies the table-level eligibility gate
     * ({@link PreSplitTargets#findEligibleTable}). Per-partition checks are
     * deferred so the partitioned multi-partition flow can run them per-bucket.
     *
     * @return the resolved {@link ResolvedTable}, or {@code null} when target
     *         resolution or the table-level eligibility check fails (caller
     *         no-ops; the table-level helper records the eligibility-skip bvar).
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

    /** Database + target table bundle returned by {@link #resolveEligibleTable}. */
    private record ResolvedTable(Database database, OlapTable olapTable) { }

    /** Parsed SELECT body + source relation returned by {@link #extractSingleTableSource}. */
    private record SingleTableSource(SelectRelation selectRelation, TableRelation sourceRelation) { }

    /**
     * Resolves the OLAP source table referenced by the FROM clause and the
     * SQL bits the sampler needs to re-issue a scan against it.
     *
     * <p>The source {@link TableName} is cloned before normalization so the
     * AST's own {@code TableName} is never mutated in place.
     *
     * @return the resolved source bundle, or {@code null} when the source db /
     *         table cannot be resolved or the source is not an OLAP table.
     */
    private static ResolvedSource resolveSourceTable(TableRelation sourceRelation, ConnectContext context) {
        TableName sourceName = sourceRelation.getName();
        TableName normalized = new TableName(
                sourceName.getCatalog(), sourceName.getDb(), sourceName.getTbl());
        normalized.normalization(context);
        Database sourceDb = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(context, normalized.getCatalog(), normalized.getDb());
        if (sourceDb == null) {
            return null;
        }
        Table table = MetaUtils.getSessionAwareTable(context, sourceDb, normalized);
        if (!(table instanceof OlapTable sourceTable)) {
            return null;
        }
        String sourceAlias = sourceRelation.getAlias() == null ? null : sourceRelation.getAlias().getTbl();
        String sourceFromSql = SqlUtils.getIdentSql(normalized.getDb())
                + "." + SqlUtils.getIdentSql(normalized.getTbl())
                + (sourceAlias != null ? " " + SqlUtils.getIdentSql(sourceAlias) : "");
        return new ResolvedSource(sourceTable, normalized, sourceAlias, sourceFromSql);
    }

    /** Resolved OLAP source + the qualifier / SQL bits the sampler needs. */
    private record ResolvedSource(OlapTable sourceTable, TableName normalizedName,
                                  String sourceAlias, String sourceFromSql) { }

    /**
     * Re-checks the user's SELECT privilege on the source and rejects sources
     * carrying a row-access or column-masking policy. The sampler runs as ROOT,
     * so without these checks the sample would observe rows / values the
     * policy-filtered load never writes, diverging the boundaries.
     *
     * @return {@code true} when the source is authorized and policy-free;
     *         {@code false} when a policy is attached. An auth failure throws
     *         {@link AccessDeniedException}, which the outer try/catch swallows
     *         (skip, safe).
     */
    private static boolean sourceAuthorizedAndPolicyFree(ResolvedSource source, ConnectContext context)
            throws AccessDeniedException {
        TableName normalized = source.normalizedName();
        if (!context.isBypassAuthorizerCheck()) {
            Authorizer.checkTableAction(context, normalized.getCatalog(), normalized.getDb(),
                    normalized.getTbl(), PrivilegeType.SELECT);
        }
        if (Authorizer.getRowAccessPolicy(context, normalized) != null) {
            return false;
        }
        Map<String, Expr> masking = Authorizer.getColumnMaskingPolicy(
                context, normalized, source.sourceTable().getBaseSchema());
        return masking == null || masking.isEmpty();
    }

    /**
     * Normalizes the InsertStmt's tableRef so the catalog/db/table parts are
     * fully qualified. Returns {@code null} on any failure; the hook then no-ops.
     */
    private static TableRef normalizeTableRefOrNull(InsertStmt insertStmt, ConnectContext context) {
        if (insertStmt.getTableRef() == null) {
            return null;
        }
        try {
            return AnalyzerUtils.normalizedTableRef(insertStmt.getTableRef(), context);
        } catch (Throwable failure) {
            LOG.info("Sample-Based Tablet Pre-Split (INSERT-from-table): tableRef normalization "
                    + "failed for {}; skipping: {}", targetNameForLog(insertStmt), failure.getMessage());
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

    private static String targetNameForLog(InsertStmt insertStmt) {
        TableRef tableRef = insertStmt.getTableRef();
        return tableRef == null ? "<unknown>" : tableRef.getTableName();
    }
}
