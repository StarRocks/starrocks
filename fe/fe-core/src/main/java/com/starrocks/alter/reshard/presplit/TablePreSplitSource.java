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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * INSERT-from-table pre-split source. Matches a single plain internal OLAP or
 * external Iceberg {@link TableRelation} — rejecting FILES() sources and any source-slice
 * modifier (partition / tablet / replica / hint / sample / time-travel / GTID).
 * {@link #prepare} resolves the OLAP source, re-checks the user's SELECT
 * privilege and rejects row-access / column-masking policies, gates the WHERE
 * predicate, maps the projection onto the target, and builds an
 * {@link InsertFromTableScanContext}. The flow uses a data-tier sample for both source kinds;
 * Iceberg snapshot totals seed the sampling rate and the observed predicate hit ratio sizes the
 * target tablet count.
 */
final class TablePreSplitSource implements InsertPreSplitSource {

    private static final Logger LOG = LogManager.getLogger(TablePreSplitSource.class);

    @Override
    public boolean configEnabled() {
        return Config.enable_tablet_pre_split_for_insert_from_table;
    }

    @Override
    public LoadKind loadKind() {
        return LoadKind.INSERT_FROM_TABLE;
    }

    @Override
    public boolean matches(InsertStmt insertStmt, SelectRelation selectRelation) {
        Relation from = selectRelation.getRelation();
        return hasSupportedProjectionShape(selectRelation)
                && from instanceof TableRelation
                && !(from instanceof FileTableFunctionRelation)
                && isPlainTableReference((TableRelation) from);
    }

    @Override
    public PreSplitFlow.Prepared prepare(InsertStmt insertStmt, SelectRelation selectRelation,
                                         OlapTable target, Database database, ConnectContext context)
            throws AccessDeniedException {
        // The INSERT-from-table column mapping (InsertSelectSourceColumns) assumes
        // the load writes the full base schema in order, so a partial / reordered
        // target column list is not yet supported on this path (the common gate
        // only guarantees all sort keys are present, which is weaker).
        if (!InsertPreSplitHook.targetColumnListIsFullIdentity(insertStmt, target)) {
            return null;
        }
        TableRelation sourceRelation = (TableRelation) selectRelation.getRelation();
        ResolvedSource resolvedSource = resolveSourceTable(sourceRelation, context);
        if (resolvedSource == null) {
            return null;
        }
        if (!sourceAuthorizedAndPolicyFree(resolvedSource, context)) {
            return null;
        }
        Expr where = selectRelation.getWhereClause();
        if (!SamplingPredicateGate.isDeterministicAndSafe(
                where, resolvedSource.normalizedName(), resolvedSource.sourceAlias())) {
            return null;
        }
        String wherePredicateSql = where == null ? null : SamplingPredicateGate.toSql(where);

        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(target);
        List<Column> partitionColumns =
                target.getPartitionInfo().getPartitionColumns(target.getIdToColumn());
        Map<String, String> targetToSource = InsertSelectSourceColumns.resolve(
                insertStmt, selectRelation, target, resolvedSource.sourceTable(),
                resolvedSource.normalizedName(), resolvedSource.sourceAlias(),
                sortKeyColumns, partitionColumns);
        if (targetToSource == null) {
            return null;
        }
        InsertFromTableScanContext scanContext = new InsertFromTableScanContext(
                resolvedSource.sourceTable(), resolvedSource.sourceFromSql(),
                targetToSource,
                wherePredicateSql, context.getCurrentComputeResource(),
                resolvedSource.totalBytes(), resolvedSource.totalRows());
        long estimatedBytes = resolvedSource.totalBytes();
        List<SecondaryIndexSpec> secondaryIndexSpecs = SecondaryIndexSpec.forVisibleRollups(target);
        for (SecondaryIndexSpec spec : secondaryIndexSpecs) {
            // A rollup sort-key column with no source mapping (e.g. a range DUP rollup whose ORDER BY
            // promotes a generated column) cannot be projected by source name -> skip pre-split for the
            // whole load, using the same lookup gate as the base sort key above. The executor's
            // mapToSource throw stays as the fail-safe for a metadata race between here and sampling.
            if (InsertSelectSourceColumns.lookup(spec.sortKey(), targetToSource) == null) {
                return null;
            }
        }
        return new PreSplitFlow.Prepared(scanContext, sortKeyColumns, partitionColumns,
                estimatedBytes, context.getCurrentComputeResource(), secondaryIndexSpecs);
    }

    /**
     * Verifies the projection is a bare {@code SELECT *} (single star, no
     * qualifier / EXCLUDE / alias) or an explicit list without stars, with no
     * DISTINCT and no GROUP BY / HAVING / ORDER BY / LIMIT. A WHERE clause is
     * allowed (gated separately). Expressions in the explicit list are checked
     * later by {@link InsertSelectSourceColumns}: only target columns needed for
     * partitioning and range distribution must be direct source-column refs, and
     * an expression may reference nothing beyond the resolved source relation.
     */
    private static boolean hasSupportedProjectionShape(SelectRelation selectRelation) {
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
                if (item.isStar()) {
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
     * Resolves the OLAP source table referenced by the FROM clause and the
     * SQL bits the sampler needs to re-issue a scan against it.
     *
     * <p>The source {@link TableName} is cloned before normalization so the
     * AST's own {@code TableName} is never mutated in place.
     *
     * @return the resolved source bundle, or {@code null} when the source db /
     *         table cannot be resolved or the source is not an internal OLAP or Iceberg table.
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
        if (!isSupportedSourceTable(table)) {
            return null;
        }
        // The sampler runs as UserIdentity.ROOT in a fresh statistics ConnectContext that cannot see
        // the user's session temporary tables. A temp-table source would be re-resolved by ROOT to the
        // shadowed permanent table (or fail), so the sample would observe different rows than the load
        // writes — skip pre-split entirely when the resolved source is a temporary table.
        if (table instanceof OlapTable sourceTable && sourceTable.isTemporaryTable()) {
            return null;
        }
        String sourceAlias = sourceRelation.getAlias() == null ? null : sourceRelation.getAlias().getTbl();
        String sourceFromSql = (CatalogMgr.isInternalCatalog(normalized.getCatalog())
                ? "" : SqlUtils.getIdentSql(normalized.getCatalog()) + ".")
                + SqlUtils.getIdentSql(normalized.getDb()) + "." + SqlUtils.getIdentSql(normalized.getTbl())
                + (sourceAlias != null ? " " + SqlUtils.getIdentSql(sourceAlias) : "");
        Estimates estimates = sourceEstimates(table);
        return new ResolvedSource(table, normalized, sourceAlias, sourceFromSql,
                estimates.totalBytes(), estimates.totalRows());
    }

    static boolean isSupportedSourceTable(Table sourceTable) {
        return sourceTable instanceof OlapTable || sourceTable instanceof IcebergTable;
    }

    static Estimates sourceEstimates(Table sourceTable) {
        if (sourceTable instanceof OlapTable olapTable) {
            return new Estimates(Math.max(0L, olapTable.getDataSize()), Math.max(0L, olapTable.getRowCount()));
        }
        if (!(sourceTable instanceof IcebergTable icebergTable)) {
            return Estimates.ZERO;
        }
        org.apache.iceberg.Snapshot snapshot = icebergTable.getNativeTable().currentSnapshot();
        if (snapshot == null || snapshot.summary() == null) {
            LOG.info("Pre-split: Iceberg source {} exposes no current snapshot summary, so the load's "
                    + "input size is unknown", sourceTable.getName());
            return Estimates.ZERO;
        }
        Estimates estimates = new Estimates(parseNonNegativeLong(snapshot.summary().get("total-files-size")),
                parseNonNegativeLong(snapshot.summary().get("total-records")));
        if (estimates.totalBytes() == 0L || estimates.totalRows() == 0L) {
            // These summary keys are written by whichever engine produced the snapshot, so a writer
            // that omits them leaves the sampler with no size at all. That degrades silently and in
            // two directions at once: pickSamplingRate falls back to 1.0, so every predicate-matching
            // row reaches the ORDER BY rand() LIMIT instead of a small Bernoulli sample, and
            // selectPreSplitTabletCount sizes zero bytes down to the minimum two tablets. Log it so
            // an unsplit load is attributable rather than looking like a successful pre-split.
            LOG.warn("Pre-split: Iceberg source {} snapshot {} reports total-files-size={} and "
                            + "total-records={}, so the load cannot be sized from the snapshot; "
                            + "sampling will not be rate-limited and the split count falls to the minimum",
                    sourceTable.getName(), snapshot.snapshotId(),
                    snapshot.summary().get("total-files-size"), snapshot.summary().get("total-records"));
        }
        return estimates;
    }

    private static long parseNonNegativeLong(String value) {
        if (value == null) {
            return 0L;
        }
        try {
            return Math.max(0L, Long.parseLong(value));
        } catch (NumberFormatException ignored) {
            return 0L;
        }
    }

    /** Resolved source + the qualifier / SQL bits and snapshot estimates the sampler needs. */
    private record ResolvedSource(Table sourceTable, TableName normalizedName,
                                  String sourceAlias, String sourceFromSql,
                                  long totalBytes, long totalRows) { }

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
}
