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
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * INSERT-from-table pre-split source. Matches a single plain {@link TableRelation} — rejecting
 * FILES() sources and any source-slice modifier (partition / tablet / replica / hint / sample /
 * time-travel / GTID). The source may be an internal OLAP table or any base table an external
 * catalog serves (Hive, Iceberg, Paimon, Delta Lake, Hudi, JDBC, Elasticsearch, ...); views are
 * excluded. {@link #prepare} resolves the source, re-checks the user's SELECT privilege and rejects
 * row-access / column-masking policies, gates the WHERE predicate, maps the projection onto the
 * target, and builds an {@link InsertFromTableScanContext}. The flow uses a data-tier sample for
 * every source kind; the source size estimate seeds the sampling rate and the observed predicate hit
 * ratio sizes the target tablet count.
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
        return InsertPreSplitHook.hasSupportedProjectionShape(selectRelation)
                && from instanceof TableRelation
                && !(from instanceof FileTableFunctionRelation)
                && isPlainTableReference((TableRelation) from);
    }

    @Override
    public PreSplitFlow.Prepared prepare(InsertStmt insertStmt, SelectRelation selectRelation,
                                         OlapTable target, Database database, ConnectContext context)
            throws AccessDeniedException {
        // No column-list gate of its own: InsertPreSplitHook#targetColumnListIsPreSplitSafe has
        // already vetted the list for every path, and InsertSelectSourceColumns#resolve pairs the
        // SELECT outputs against the columns the list names, so a partial or reordered list maps
        // as written.
        TableRelation sourceRelation = (TableRelation) selectRelation.getRelation();
        ResolvedSource resolvedSource = resolveSourceTable(sourceRelation, context);
        if (resolvedSource == null) {
            return null;
        }
        if (!sourceAuthorizedAndPolicyFree(resolvedSource, context)) {
            return null;
        }
        // Fold plan-time constants in the user's context before the gate, so the ROOT sampler
        // never evaluates a function that reads session state (time zone, query start time).
        Expr where = SamplingPredicateGate.foldPlanTimeConstants(selectRelation.getWhereClause(), context);
        if (where == null && selectRelation.getWhereClause() != null) {
            return null;
        }
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
                sortKeyColumns, partitionColumns, InsertSelectSourceColumns.SchemaPairing.EXACT);
        if (targetToSource == null) {
            return null;
        }
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
        // Sized last: for an external source this reads connector metadata, so only a load that
        // passed every other gate -- the SELECT re-check included -- pays for it.
        Estimates estimates = sourceEstimates(resolvedSource.sourceTable(), context);
        if (!(resolvedSource.sourceTable() instanceof OlapTable)
                && (estimates.totalBytes() == 0L || estimates.totalRows() == 0L)) {
            // An unsized external source would be sampled at rate 1.0 -- a second full scan of the
            // source, sorted by rand() -- only for selectPreSplitTabletCount to size zero bytes down to
            // the minimum two tablets. That costs about as much as the load and buys almost nothing, so
            // decline and say why. An internal OLAP source keeps sampling unrated: its size comes from
            // the FE's own tablet statistics, and zero there means an empty or just-loaded table.
            LOG.info("Pre-split: source {} has no usable size estimate (bytes={}, rows={}); skipping",
                    resolvedSource.normalizedName(), estimates.totalBytes(), estimates.totalRows());
            PreSplitMetrics.recordEligibilitySkip(SkipReason.ESTIMATE_UNAVAILABLE);
            PreSplitProfile.recordOutcome("SKIPPED: " + SkipReason.ESTIMATE_UNAVAILABLE);
            return null;
        }
        InsertFromTableScanContext scanContext = new InsertFromTableScanContext(
                resolvedSource.sourceTable(), resolvedSource.sourceFromSql(),
                targetToSource,
                wherePredicateSql, context.getCurrentComputeResource(),
                estimates.totalBytes(), estimates.totalRows());
        long estimatedBytes = estimates.totalBytes();
        return new PreSplitFlow.Prepared(scanContext, sortKeyColumns, partitionColumns,
                estimatedBytes, context.getCurrentComputeResource(), secondaryIndexSpecs);
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
     *         table cannot be resolved or the source is not a supported kind.
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
        return new ResolvedSource(table, normalized, sourceAlias, sourceFromSql);
    }

    /**
     * An internal OLAP table (or materialized view), or a base table served by an external catalog.
     * The sampler reads the source with a plain catalog-qualified SELECT, so any table the optimizer
     * can scan is samplable. Views are excluded: their projection would have to be mapped through the
     * view definition. Iceberg metadata tables and information_schema tables are not load sources.
     */
    static boolean isSupportedSourceTable(Table sourceTable) {
        if (sourceTable instanceof OlapTable) {
            return true;
        }
        return sourceTable != null
                && CatalogMgr.isExternalCatalog(sourceTable.getCatalogName())
                && !sourceTable.isView()
                && !sourceTable.isMetadataTable()
                && sourceTable.getType() != Table.TableType.SCHEMA;
    }

    static Estimates sourceEstimates(Table sourceTable, ConnectContext context) {
        if (sourceTable instanceof OlapTable olapTable) {
            return new Estimates(Math.max(0L, olapTable.getDataSize()), Math.max(0L, olapTable.getRowCount()));
        }
        if (sourceTable instanceof IcebergTable icebergTable) {
            // The snapshot summary is exact and costs nothing, and it is the only source here that
            // reports stored file bytes -- the unit tablet_pre_split_target_size is measured in.
            Estimates snapshotTotals = icebergSnapshotEstimates(icebergTable);
            if (snapshotTotals.totalBytes() > 0L && snapshotTotals.totalRows() > 0L) {
                return snapshotTotals;
            }
        }
        return connectorStatisticsEstimates(sourceTable, context);
    }

    /**
     * Sizes an external source from the statistics the optimizer would use to cost a scan of it:
     * {@link MetadataMgr#getTableStatistics}, which prefers ANALYZE-collected statistics and falls
     * back to the connector's own metadata (HMS numRows, Paimon / Iceberg manifests, the JDBC
     * source's catalog, ...). A {@link Statistics.StatsSource#NONE} result is the optimizer's
     * placeholder, not a measurement, so it counts as no estimate.
     *
     * <p>The byte figure is the row count times the statistics' average row width. That describes
     * decoded rows, not stored files, and the width is itself an estimate where the connector has no
     * column statistics (a type-size guess for strings), so it can land on either side of the
     * source's on-disk size. tablet_reshard_max_split_count and tablet_reshard_min_split_size bound
     * the split either way, and the reshard daemon converges the tablets afterwards.
     */
    static Estimates connectorStatisticsEstimates(Table sourceTable, ConnectContext context) {
        try {
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            Map<ColumnRefOperator, Column> columns = new HashMap<>();
            for (Column column : sourceTable.getBaseSchema()) {
                columns.put(columnRefFactory.create(column.getName(), column.getType(), column.isAllowNull()),
                        column);
            }
            MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
            // The version and the partition keys are what the optimizer hands a scan's statistics
            // call once it has resolved the relation: Iceberg sizes nothing without a snapshot, and
            // Hive / Hudi size a partitioned table only over the partitions they are given.
            TvrVersionRange version = metadataMgr.getTableVersionRange(
                    sourceTable.getCatalogDBName(), sourceTable, Optional.empty(), Optional.empty());
            List<PartitionKey> partitionKeys = PartitionUtil.getPartitionKeys(sourceTable);
            Statistics statistics = metadataMgr.getTableStatistics(
                    OptimizerFactory.initContext(context, columnRefFactory), sourceTable.getCatalogName(),
                    sourceTable, columns, partitionKeys, null, -1, version);
            if (statistics == null || statistics.getStatsSource() == Statistics.StatsSource.NONE) {
                return Estimates.ZERO;
            }
            double rows = statistics.getOutputRowCount();
            if (!(rows >= 1.0) || Double.isInfinite(rows)) {
                return Estimates.ZERO;
            }
            double bytes = statistics.getComputeSize();
            return new Estimates(bytes >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) bytes,
                    rows >= Long.MAX_VALUE ? Long.MAX_VALUE : (long) rows);
        } catch (Exception e) {
            LOG.info("Pre-split: could not read statistics for source {}.{}.{}, so the load's input size "
                            + "is unknown: {}", sourceTable.getCatalogName(), sourceTable.getCatalogDBName(),
                    sourceTable.getName(), e.getMessage());
            return Estimates.ZERO;
        }
    }

    static Estimates icebergSnapshotEstimates(IcebergTable icebergTable) {
        org.apache.iceberg.Snapshot snapshot = icebergTable.getNativeTable().currentSnapshot();
        if (snapshot == null || snapshot.summary() == null) {
            LOG.info("Pre-split: Iceberg source {} exposes no current snapshot summary; "
                    + "falling back to the table statistics", icebergTable.getName());
            return Estimates.ZERO;
        }
        Estimates estimates = new Estimates(parseNonNegativeLong(snapshot.summary().get("total-files-size")),
                parseNonNegativeLong(snapshot.summary().get("total-records")));
        if (estimates.totalBytes() == 0L || estimates.totalRows() == 0L) {
            // These summary keys are written by whichever engine produced the snapshot, so a writer
            // may omit them. The caller then falls back to the connector statistics, and declines
            // the pre-split if those cannot size the source either. Log it so the fallback -- and a
            // declined load -- is attributable to the writer rather than to pre-split.
            LOG.warn("Pre-split: Iceberg source {} snapshot {} reports total-files-size={} and "
                            + "total-records={}, so the load cannot be sized from the snapshot; "
                            + "falling back to the table statistics",
                    icebergTable.getName(), snapshot.snapshotId(),
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

    /** Resolved source + the qualifier / SQL bits the sampler needs. */
    private record ResolvedSource(Table sourceTable, TableName normalizedName,
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
}
