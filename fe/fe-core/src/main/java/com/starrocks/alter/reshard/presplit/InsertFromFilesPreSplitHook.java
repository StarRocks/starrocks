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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.planner.LoadScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StatementBase.ExplainLevel;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
 * <h2>Fire-and-forget semantics</h2>
 * <p>The hook calls
 * {@link TabletPreSplitCoordinator#submitAsynchronously} and returns. The
 * load planning that follows runs against whatever tablet layout is current
 * at plan time. The reshard daemon completes the split asynchronously after
 * the planner releases its meta-lock, so subsequent loads on this table
 * see the post-split layout.
 *
 * <h2>Detection</h2>
 * <p>The hook only matches the simple {@code INSERT INTO target SELECT ...
 * FROM FILES(...)} shape: a single {@code FileTableFunctionRelation} as the
 * {@code SelectRelation}'s FROM. Joins, unions, subqueries, CTEs, value
 * lists, and multi-source variants are rejected because the sampler-driven
 * boundaries would only represent a subset of the inserted rows.
 *
 * <p>The Tier 1 / Tier 2 sampler executors are currently placeholders
 * supplied by
 * {@link DefaultPreSplitPipeline#withPendingExecutors}. The per-path Config
 * flag defaults to {@code false}, so the hook never reaches them until
 * production wiring lands.
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

    private static void tryRunPreSplit(StatementBase parsedStmt, ConnectContext context) {
        InsertStmt insertStmt = qualifyingInsertStmt(parsedStmt, context);
        if (insertStmt == null) {
            return;
        }
        FileTableFunctionRelation filesRelation = extractSingleFilesSource(insertStmt);
        if (filesRelation == null) {
            return;
        }
        PreSplitTargets.EligibleTarget target = resolveEligibleTarget(insertStmt, context);
        if (target == null) {
            return;
        }
        TableFunctionTable sourceTable = resolveSourceTable(insertStmt, filesRelation, context);
        if (sourceTable == null) {
            return;
        }
        submitToCoordinator(target, sourceTable, context);
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
     * @return the FROM relation when it is exactly one {@link FileTableFunctionRelation}
     *         (no joins, unions, subqueries, CTEs), {@code null} otherwise.
     */
    private static FileTableFunctionRelation extractSingleFilesSource(InsertStmt insertStmt) {
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
        Relation from = selectRelation.getRelation();
        return from instanceof FileTableFunctionRelation filesRelation ? filesRelation : null;
    }

    /**
     * Walks the catalog to confirm the INSERT target is a single-partition,
     * single-tablet OlapTable. Returns {@code null} (no log) for any branch
     * that the eligibility gate inside {@link TabletPreSplitCoordinator} would
     * also reject — checking here avoids paying for the FILES() schema RPC.
     *
     * @return the resolved {@link PreSplitTargets.EligibleTarget}, or {@code null}
     *         when target resolution or any cheap eligibility check fails
     *         (caller no-ops).
     */
    private static PreSplitTargets.EligibleTarget resolveEligibleTarget(InsertStmt insertStmt, ConnectContext context) {
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
        return PreSplitTargets.findEligibleTarget(database, olapTable);
    }

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

        DefaultPreSplitPipeline pipeline = DefaultPreSplitPipeline.withPendingExecutors(
                target.database(), target.olapTable(), target.oldTabletId(), fileTotalBytes,
                LoadKind.INSERT_FROM_FILES);

        PreSplitOutcome outcome = TabletPreSplitCoordinator.submitAsynchronously(
                target.database(), target.olapTable(), target.partitionId(), scanContext,
                LoadKind.INSERT_FROM_FILES, pipeline, activeComputeNodeCount);
        LOG.info("Sample-Based Tablet Pre-Split outcome for table {}: {}",
                target.olapTable().getName(), outcome);
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
