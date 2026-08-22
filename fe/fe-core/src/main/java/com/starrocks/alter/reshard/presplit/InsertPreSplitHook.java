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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StatementBase.ExplainLevel;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Single {@code StmtExecutor} &rarr; {@link PreSplitFlow} bridge for Sample-Based
 * Tablet Pre-Split on the {@code INSERT ... SELECT} paths (FILES, internal OLAP,
 * and external Iceberg sources). The hook runs pre-plan, outside the planner's
 * {@code PlannerMetaLocker}-scoped read lock, so it cannot deadlock with the
 * reshard daemon's write lock on the same target table.
 *
 * <p>The hook owns the parts of the flow that are common to both source kinds:
 * the statement-shape pre-filters, {@link SelectRelation} extraction, the
 * mutually-exclusive strategy selection, the per-path config gate, the
 * per-session opt-out, and target resolve + authorization. The conservative-skip
 * statement gates live alongside them: the statement-shape and load-properties
 * gates in {@link #passesCommonPreFilters}, and the materialized-view gate in
 * {@link #resolveEligibleTable}. Each {@link InsertPreSplitSource} supplies the
 * source-specific detection + resolve, and the submit flow (plus the
 * automatic-partition gate) lives in {@link PreSplitFlow}.
 *
 * <p>The entry point is fail-safe: any throw is swallowed and the load proceeds
 * without pre-split, because the hook runs before the planner and must never
 * abort an INSERT that would otherwise plan and run correctly.
 */
public final class InsertPreSplitHook {

    private static final Logger LOG = LogManager.getLogger(InsertPreSplitHook.class);

    /** Order matters only for reporting; matches() is mutually exclusive. */
    private static final List<InsertPreSplitSource> SOURCES =
            List.of(new FilesPreSplitSource(), new TablePreSplitSource());

    private InsertPreSplitHook() {
    }

    public static void maybeRunPreSplit(StatementBase parsedStmt, ConnectContext context) {
        try {
            tryRunPreSplit(parsedStmt, context);
        } catch (Throwable unexpected) {
            LOG.warn("Sample-Based Tablet Pre-Split (INSERT) hook failed; proceeding without pre-split", unexpected);
        }
    }

    /**
     * Runs the dynamic-overwrite variant after its transaction has been opened but before the
     * load is replanned and starts writing. Fail-safe like {@link #maybeRunPreSplit}: any failure
     * leaves the runtime auto-partition path to create or reuse the temporary partitions.
     */
    public static void maybeRunDynamicOverwritePreSplit(
            InsertStmt insertStmt, ConnectContext context, long overwriteTransactionId) {
        try {
            if (!passesDynamicOverwritePreFilters(insertStmt, context, overwriteTransactionId)) {
                return;
            }
            tryRunEligibleInsert(insertStmt, context, overwriteTransactionId,
                    PreSplitPartitionScope.unrestricted(), false);
        } catch (Throwable unexpected) {
            LOG.warn("Sample-Based Tablet Pre-Split (dynamic INSERT OVERWRITE) hook failed; "
                    + "proceeding without pre-split", unexpected);
        }
    }

    /**
     * Runs after a static overwrite job has cloned its source partitions to temporary partitions
     * and before the INSERT is replanned to write those temporary partitions.
     *
     * @param estimates the statement's estimated output, taken from the plan the optimizer already
     *                  built. Only the derived materialized-view route below reads it; the sampled
     *                  routes learn their input size from the sample itself.
     */
    public static void maybeRunStaticOverwritePreSplit(
            InsertStmt insertStmt, ConnectContext context,
            List<String> sourcePartitionNames, List<String> temporaryPartitionNames, Estimates estimates) {
        try {
            if (!passesStaticOverwritePreFilters(insertStmt, context)
                    || sourcePartitionNames == null || sourcePartitionNames.isEmpty()
                    || temporaryPartitionNames == null || temporaryPartitionNames.isEmpty()) {
                return;
            }
            PreSplitPartitionScope partitionScope =
                    PreSplitPartitionScope.staticOverwrite(sourcePartitionNames, temporaryPartitionNames);
            // Classify the target BEFORE tryRunEligibleInsert, which returns on a set-operation or CTE
            // query shape before it ever looks at the target. A materialized view refreshed by one of
            // those shapes would otherwise be dropped with nothing recorded, even though the derived
            // tier does not care what the refresh query looks like.
            ResolvedTable resolvedTable = resolveTarget(insertStmt, context);
            if (resolvedTable != null && resolvedTable.olapTable() instanceof MaterializedView) {
                // Establish that the derived tier can carve this view BEFORE the feature flag gets a
                // say: an ordinary async view's full refresh is also an INSERT OVERWRITE and lands
                // here, and consulting the flag first would count every such refresh against this
                // feature's own config gate, describing a view pre-split was never a candidate for.
                // The skip reason itself is the same one resolveEligibleTable would record -- that
                // resolver has always declined materialized-view targets -- so this only reports it
                // earlier, and spares those refreshes a table lock they gain nothing from.
                if (!MaterializedViewRowIdBoundaries.isDerivable(resolvedTable.olapTable())) {
                    PreSplitMetrics.recordEligibilitySkip(SkipReason.MATERIALIZED_VIEW_TARGET);
                    return;
                }
                // Part of identifying the candidate, not of serving it: turning the flag on would not make
                // a multi-partition refresh eligible, so charging it to the config gate would misreport it.
                if (partitionScope.catalogPartitionNames().size() != 1) {
                    PreSplitMetrics.recordEligibilitySkip(SkipReason.MULTIPLE_TEMPORARY_PARTITIONS);
                    return;
                }
                runStaticOverwriteMaterializedViewPreSplit(resolvedTable, context, partitionScope, estimates);
                return;
            }
            tryRunEligibleInsert(insertStmt, context, -1L, partitionScope, true);
        } catch (Throwable unexpected) {
            LOG.warn("Sample-Based Tablet Pre-Split (static INSERT OVERWRITE) hook failed; "
                    + "proceeding without pre-split", unexpected);
        }
    }

    /**
     * Derived-tier route for a static overwrite whose target is a materialized view. The sampled
     * sources cannot serve one (see {@link #resolveEligibleTable}), but an incremental view's hidden
     * row-id sort key needs no sample at all.
     *
     * <p>Deliberately does NOT run {@link #authorizeTargetSideEffects}. That re-check guards the
     * sampled paths because they read source data through an internal ROOT context, so it verifies the
     * invoking user could have read it and could have written the target. The derived tier reads
     * nothing whatsoever, which removes the first half; and the only way to reach this code is a
     * refresh of this view, which already required privileges on the view, which covers the second.
     * Adding the re-check anyway would be worse than redundant: an {@link AccessDeniedException} here
     * is swallowed by the caller's catch-all, so a context the authorizer happens to reject would turn
     * the feature into a silent no-op instead of an error anyone can see.
     */
    private static void runStaticOverwriteMaterializedViewPreSplit(
            ResolvedTable resolvedTable, ConnectContext context,
            PreSplitPartitionScope partitionScope, Estimates estimates) {
        // Both gates are checked here rather than left to TabletPreSplitCoordinator#maybeAct, which
        // checks them again: the flow below resolves its target and its boundary source first, and
        // each of those records its own skip reason, so reaching maybeAct with the flag off would
        // report a second, unrelated reason alongside DISABLED_BY_CONFIG. Returning here keeps it to
        // one recorded reason per statement, matching the sampled path.
        if (!Config.enable_tablet_pre_split_for_mv_refresh) {
            PreSplitMetrics.recordEligibilitySkip(SkipReason.DISABLED_BY_CONFIG);
            return;
        }
        if (PreSplitMetrics.shortCircuitOnSessionOptOut(context.getSessionVariable())) {
            return;
        }
        PreSplitFlow.runStaticOverwriteMaterializedViewFlow(
                resolvedTable.database(), resolvedTable.olapTable(), partitionScope, estimates,
                context.getCurrentComputeResource(), context::isStatementCancelled);
    }

    private static void tryRunPreSplit(StatementBase parsedStmt, ConnectContext context)
            throws AccessDeniedException {
        if (!(parsedStmt instanceof InsertStmt insertStmt)) {
            return;
        }
        if (!passesCommonPreFilters(insertStmt, context)) {
            return;
        }
        tryRunEligibleInsert(insertStmt, context, -1L,
                PreSplitPartitionScope.fromInsert(insertStmt), false);
    }

    private static void tryRunEligibleInsert(
            InsertStmt insertStmt, ConnectContext context, long overwriteTransactionId,
            PreSplitPartitionScope partitionScope, boolean staticOverwrite)
            throws AccessDeniedException {
        SelectRelation selectRelation = extractSelectRelation(insertStmt);
        if (selectRelation == null) {
            return;
        }
        InsertPreSplitSource source = selectSource(insertStmt, selectRelation);
        if (source == null) {
            return;
        }
        // Config gate AFTER candidate identification: only a real candidate whose path flag is off
        // records DISABLED_BY_CONFIG (no per-statement inflation, no double-count across sources).
        if (!source.configEnabled()) {
            PreSplitMetrics.recordEligibilitySkip(SkipReason.DISABLED_BY_CONFIG);
            return;
        }
        if (PreSplitMetrics.shortCircuitOnSessionOptOut(context.getSessionVariable())) {
            return;
        }
        ResolvedTable resolvedTable = resolveEligibleTable(insertStmt, context);
        if (resolvedTable == null) {
            return;
        }
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(resolvedTable.olapTable());
        if (!targetColumnListIsPreSplitSafe(insertStmt, resolvedTable.olapTable(), sortKeyColumns)) {
            return;
        }
        authorizeTargetSideEffects(resolvedTable, context);

        PreSplitFlow.Prepared prepared = source.prepare(
                insertStmt, selectRelation, resolvedTable.olapTable(), resolvedTable.database(), context);
        if (prepared == null) {
            return;
        }
        if (overwriteTransactionId > 0) {
            PreSplitFlow.runDynamicOverwriteFlow(resolvedTable.database(), resolvedTable.olapTable(),
                    prepared, source.loadKind(), context::isStatementCancelled, context, overwriteTransactionId);
        } else if (staticOverwrite) {
            PreSplitFlow.runStaticOverwriteFlow(resolvedTable.database(), resolvedTable.olapTable(),
                    prepared, source.loadKind(), context::isStatementCancelled, context, partitionScope);
        } else {
            PreSplitFlow.dispatch(resolvedTable.database(), resolvedTable.olapTable(),
                    prepared, source.loadKind(), context::isStatementCancelled, context, partitionScope);
        }
    }

    private static boolean passesCommonPreFilters(InsertStmt insertStmt, ConnectContext context) {
        // An explicit target column list is validated after target resolution:
        // the source-agnostic targetColumnListIsPreSplitSafe gate plus each
        // source's own column/source alignment in prepare(). The sort-key columns
        // and table schema are not available this early.
        if (insertStmt.isExplain() && !ExplainLevel.ANALYZE.equals(insertStmt.getExplainLevel())) {
            return false;
        }
        // Every overwrite variant is handled by InsertOverwriteJobRunner after its target temporary
        // partitions (and, for dynamic overwrite, transaction) exist. Splitting the normal partition
        // here would optimize the wrong write target.
        if (insertStmt.isOverwrite()) {
            return false;
        }
        if (context.getTxnId() != 0 || insertStmt.getTxnId() != DmlStmt.INVALID_TXN_ID) {
            return false;
        }
        if (insertStmt.isStaticKeyPartitionInsert()) {
            return false;
        }
        return !carriesLoadProperties(insertStmt);
    }

    /**
     * Whether the statement was written with a {@code PROPERTIES(...)} clause, which can change the
     * row set the load writes (max_filter_ratio, strict_mode, ...) relative to what the sampler saw.
     *
     * <p>Deliberately not {@code getProperties().isEmpty()}: {@code InsertAnalyzer#analyzeProperties}
     * fills that map with the session defaults for max_filter_ratio / strict_mode / timeout, so after
     * analysis it is never empty. {@link #passesCommonPreFilters} runs before analysis and would not
     * notice, but {@link #passesDynamicOverwritePreFilters} runs after it and would reject every
     * statement. Both ask the parse-time question so the two gates cannot drift apart.
     */
    private static boolean carriesLoadProperties(InsertStmt insertStmt) {
        return !insertStmt.getUserSpecifiedPropertyKeys().isEmpty();
    }

    private static boolean passesDynamicOverwritePreFilters(
            InsertStmt insertStmt, ConnectContext context, long overwriteTransactionId) {
        if (!insertStmt.isDynamicOverwrite() || !insertStmt.hasOverwriteJob() || overwriteTransactionId <= 0) {
            return false;
        }
        if (insertStmt.isExplain() && !ExplainLevel.ANALYZE.equals(insertStmt.getExplainLevel())) {
            return false;
        }
        if (context.getTxnId() != 0 || insertStmt.getTxnId() != DmlStmt.INVALID_TXN_ID) {
            return false;
        }
        if (insertStmt.isSpecifyPartitionNames() || insertStmt.isStaticKeyPartitionInsert()) {
            return false;
        }
        return !carriesLoadProperties(insertStmt);
    }

    private static boolean passesStaticOverwritePreFilters(InsertStmt insertStmt, ConnectContext context) {
        if (!insertStmt.isOverwrite() || insertStmt.isDynamicOverwrite() || !insertStmt.hasOverwriteJob()) {
            return false;
        }
        if (insertStmt.isExplain() && !ExplainLevel.ANALYZE.equals(insertStmt.getExplainLevel())) {
            return false;
        }
        if (context.getTxnId() != 0 || insertStmt.getTxnId() != DmlStmt.INVALID_TXN_ID
                || insertStmt.isStaticKeyPartitionInsert()) {
            return false;
        }
        return !carriesLoadProperties(insertStmt);
    }

    /**
     * Whether an explicit target column list is safe to pre-split on. Returns true
     * for a bare INSERT (null/empty list). For an explicit list every check below
     * must hold, otherwise pre-split is skipped so it never mutates tablet metadata
     * for a statement the analyzer would reject or that would split degenerately:
     *
     * <ul>
     *   <li>no duplicate names, and every listed name is a real base
     *       (non-generated) column — rejects the unknown/duplicate/generated lists
     *       that {@code InsertAnalyzer} fails later;</li>
     *   <li>every base column omitted from the list is fillable without an explicit
     *       value (has a default, is nullable, auto-increment, or generated) —
     *       mirrors InsertAnalyzer's "must be explicitly mentioned" rule, so a list
     *       missing a required column is skipped rather than resharded;</li>
     *   <li>every range-distribution (sort) key column of EVERY visible index (base
     *       plus any rollup) is present -- an omitted key is defaulted for every row,
     *       collapsing the data on that key and making split boundaries degenerate
     *       for whichever index the omitted column belongs to.</li>
     * </ul>
     *
     * <p>Source-specific column/source alignment is still checked later in each
     * {@link InsertPreSplitSource#prepare}.
     *
     * <p>Package-private (not private) so the unit test can drive it directly
     * without mocking the full eligibility chain that precedes it.
     */
    static boolean targetColumnListIsPreSplitSafe(
            InsertStmt insertStmt, OlapTable target, List<Column> sortKeyColumns) {
        List<String> targetColumnNames = insertStmt.getTargetColumnNames();
        if (targetColumnNames == null || targetColumnNames.isEmpty()) {
            return true;
        }
        Set<String> listed = new HashSet<>();
        for (String name : targetColumnNames) {
            if (!listed.add(name.toLowerCase())) {
                return false;   // duplicate target column
            }
        }
        Set<String> baseNonGenerated = new HashSet<>();
        for (Column column : target.getBaseSchemaWithoutGeneratedColumn()) {
            baseNonGenerated.add(column.getName().toLowerCase());
        }
        if (!baseNonGenerated.containsAll(listed)) {
            return false;   // unknown column, or a generated column, named in the list
        }
        // Mirror InsertAnalyzer: a column must be mentioned when it has no default,
        // is not nullable, and is neither auto-increment nor generated.
        for (Column column : target.getBaseSchema()) {
            if (column.getDefaultValueType() == Column.DefaultValueType.NULL
                    && !column.isAllowNull()
                    && !column.isAutoIncrement()
                    && !column.isGeneratedColumn()
                    && !listed.contains(column.getName().toLowerCase())) {
                return false;   // a required column is missing from the list
            }
        }
        for (Column sortKey : unionOfVisibleIndexSortKeyColumns(target, sortKeyColumns)) {
            if (!listed.contains(sortKey.getName().toLowerCase())) {
                return false;   // a sort-key column is missing -> degenerate split
            }
        }
        return true;
    }

    /**
     * Union of {@code baseSortKeyColumns} with every OTHER visible index's (rollup)
     * sort-key columns. A target column list must cover every visible index's sort
     * key, not just the base one -- an omitted rollup key still collapses that
     * index's data on split, even when the base split is fine.
     */
    private static List<Column> unionOfVisibleIndexSortKeyColumns(
            OlapTable target, List<Column> baseSortKeyColumns) {
        List<Column> union = new ArrayList<>(baseSortKeyColumns);
        for (MaterializedIndexMeta meta : target.getVisibleIndexMetas()) {
            if (meta.getIndexMetaId() == target.getBaseIndexMetaId()) {
                continue;
            }
            union.addAll(MetaUtils.getRangeDistributionColumns(target, meta.getIndexMetaId()));
        }
        return union;
    }

    /**
     * Whether the target column list names every base (non-generated) column
     * exactly once, in schema order — i.e. it is semantically identical to
     * omitting the list. Used by the INSERT-from-table source, whose column
     * mapping assumes the full base schema in order; partial / reordered lists
     * are not yet supported there.
     *
     * <p>Returns true when there is no target column list or when the list is a
     * full, in-order identity list.
     *
     * <p>Package-private (not private) so the unit test can drive it directly.
     */
    static boolean targetColumnListIsFullIdentity(InsertStmt insertStmt, OlapTable target) {
        List<String> targetColumnNames = insertStmt.getTargetColumnNames();
        if (targetColumnNames == null || targetColumnNames.isEmpty()) {
            return true;
        }
        List<Column> baseColumns = target.getBaseSchemaWithoutGeneratedColumn();
        if (targetColumnNames.size() != baseColumns.size()) {
            return false;
        }
        for (int i = 0; i < baseColumns.size(); i++) {
            if (!targetColumnNames.get(i).equalsIgnoreCase(baseColumns.get(i).getName())) {
                return false;
            }
        }
        return true;
    }

    private static SelectRelation extractSelectRelation(InsertStmt insertStmt) {
        if (insertStmt.getQueryStatement() == null) {
            return null;
        }
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        if (queryRelation == null || !queryRelation.getCteRelations().isEmpty()) {
            return null;
        }
        return queryRelation instanceof SelectRelation selectRelation ? selectRelation : null;
    }

    private static InsertPreSplitSource selectSource(InsertStmt insertStmt, SelectRelation selectRelation) {
        for (InsertPreSplitSource source : SOURCES) {
            if (source.matches(insertStmt, selectRelation)) {
                return source;
            }
        }
        return null;
    }

    /**
     * Resolves the statement's target through the session's catalog, applying no eligibility gate.
     * {@link #resolveEligibleTable} layers the gates on top; the static-overwrite entry needs the bare
     * target so it can route a materialized view to the derived tier, which those gates reject.
     */
    private static ResolvedTable resolveTarget(InsertStmt insertStmt, ConnectContext context) {
        TableRef normalizedTableRef = normalizeTableRefOrNull(insertStmt, context);
        if (normalizedTableRef == null) {
            return null;
        }
        Database database = resolveDatabase(normalizedTableRef, context);
        if (database == null) {
            return null;
        }
        OlapTable olapTable = resolveOlapTarget(normalizedTableRef, database, context);
        return olapTable == null ? null : new ResolvedTable(database, olapTable);
    }

    private static ResolvedTable resolveEligibleTable(InsertStmt insertStmt, ConnectContext context) {
        ResolvedTable resolvedTable = resolveTarget(insertStmt, context);
        if (resolvedTable == null) {
            return null;
        }
        OlapTable olapTable = resolvedTable.olapTable();
        // Materialized-view targets have always been declined here; this only records the reason
        // instead of returning silently, so an operator can tell it apart from the statement never
        // having been a pre-split candidate. An eligible incremental view is served by the derived
        // tier, which reaches its target without going through this resolver.
        //
        // The exclusion is not one rule: an INCREMENTAL view is keyed by its hidden row-id column,
        // which no sampled source can map back to a column of the refresh query, so sampling cannot
        // serve it at all. A PCT view is keyed by ordinary columns and could in principle be sampled
        // -- it is declined by this pre-existing conservative gate, not by a technical limit.
        if (olapTable instanceof MaterializedView) {
            PreSplitMetrics.recordEligibilitySkip(SkipReason.MATERIALIZED_VIEW_TARGET);
            return null;
        }
        SkipReason tableLevelSkip = PreSplitTargets.findEligibleTable(resolvedTable.database(), olapTable);
        if (tableLevelSkip != null) {
            PreSplitMetrics.recordEligibilitySkip(tableLevelSkip);
            return null;
        }
        return resolvedTable;
    }

    private static void authorizeTargetSideEffects(ResolvedTable resolvedTable, ConnectContext context)
            throws AccessDeniedException {
        if (context.isBypassAuthorizerCheck()) {
            return;
        }
        Authorizer.checkTableAction(context, resolvedTable.database().getFullName(),
                resolvedTable.olapTable().getName(), PrivilegeType.INSERT);
        Warehouse currentWarehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getWarehouse(context.getCurrentComputeResource().getWarehouseId());
        if (currentWarehouse.getId() != WarehouseManager.DEFAULT_WAREHOUSE_ID) {
            Authorizer.checkWarehouseAction(context, currentWarehouse.getName(), PrivilegeType.USAGE);
        }
    }

    private record ResolvedTable(Database database, OlapTable olapTable) {
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
            LOG.info("Sample-Based Tablet Pre-Split (INSERT): tableRef normalization failed for {}; skipping: {}",
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

    private static String targetNameForLog(InsertStmt insertStmt) {
        TableRef tableRef = insertStmt.getTableRef();
        return tableRef == null ? "<unknown>" : tableRef.getTableName();
    }
}
