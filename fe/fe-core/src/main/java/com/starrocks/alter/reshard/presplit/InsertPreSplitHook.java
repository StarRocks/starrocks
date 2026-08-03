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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Single {@code StmtExecutor} &rarr; {@link PreSplitFlow} bridge for Sample-Based
 * Tablet Pre-Split on the {@code INSERT ... SELECT} paths (FILES sources and OLAP
 * table sources). The hook runs pre-plan, outside the planner's
 * {@code PlannerMetaLocker}-scoped read lock, so it cannot deadlock with the
 * reshard daemon's write lock on the same target table.
 *
 * <p>The hook owns the parts of the flow that are common to both source kinds:
 * the statement-shape pre-filters, {@link SelectRelation} extraction, the
 * mutually-exclusive strategy selection, the per-path config gate, the
 * per-session opt-out, and target resolve + authorization. The conservative-skip
 * statement gates live alongside them: the target-partition and load-properties
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

    private static void tryRunPreSplit(StatementBase parsedStmt, ConnectContext context)
            throws AccessDeniedException {
        if (!(parsedStmt instanceof InsertStmt insertStmt)) {
            return;
        }
        if (!passesCommonPreFilters(insertStmt, context)) {
            return;
        }
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
        PreSplitFlow.dispatch(resolvedTable.database(), resolvedTable.olapTable(),
                prepared, source.loadKind(), context::isKilled, context);
    }

    private static boolean passesCommonPreFilters(InsertStmt insertStmt, ConnectContext context) {
        // An explicit target column list is validated after target resolution:
        // the source-agnostic targetColumnListIsPreSplitSafe gate plus each
        // source's own column/source alignment in prepare(). The sort-key columns
        // and table schema are not available this early.
        if (insertStmt.isExplain() && !ExplainLevel.ANALYZE.equals(insertStmt.getExplainLevel())) {
            return false;
        }
        if (insertStmt.isOverwrite() && !insertStmt.hasOverwriteJob()) {
            return false;
        }
        if (context.getTxnId() != 0 || insertStmt.getTxnId() != DmlStmt.INVALID_TXN_ID) {
            return false;
        }
        if (insertStmt.isSpecifyPartitionNames() || insertStmt.isStaticKeyPartitionInsert()) {
            return false;
        }
        if (insertStmt.getProperties() != null && !insertStmt.getProperties().isEmpty()) {
            return false;
        }
        return true;
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
     *   <li>every range-distribution (sort) key column is present — an omitted key
     *       is defaulted for every row, collapsing the data on that key and making
     *       split boundaries degenerate.</li>
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
        for (Column sortKey : sortKeyColumns) {
            if (!listed.contains(sortKey.getName().toLowerCase())) {
                return false;   // a sort-key column is missing -> degenerate split
            }
        }
        return true;
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
        if (olapTable instanceof MaterializedView) {
            return null;
        }
        SkipReason tableLevelSkip = PreSplitTargets.findEligibleTable(database, olapTable);
        if (tableLevelSkip != null) {
            PreSplitMetrics.recordEligibilitySkip(tableLevelSkip);
            return null;
        }
        return new ResolvedTable(database, olapTable);
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
