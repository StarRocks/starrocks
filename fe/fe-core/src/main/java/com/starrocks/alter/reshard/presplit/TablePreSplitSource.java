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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
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
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.MetaUtils;

import java.util.List;
import java.util.Map;

/**
 * INSERT-from-OLAP-table pre-split source. Matches a single plain OLAP
 * {@link TableRelation} — rejecting FILES() sources and any source-slice
 * modifier (partition / tablet / replica / hint / sample / time-travel / GTID).
 * {@link #prepare} resolves the OLAP source, re-checks the user's SELECT
 * privilege and rejects row-access / column-masking policies, gates the WHERE
 * predicate, maps the projection onto the target, and builds an
 * {@link InsertFromTableScanContext}. An OLAP source has no Parquet/ORC footer,
 * so the data tier is forced downstream.
 */
final class TablePreSplitSource implements InsertPreSplitSource {

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
        return isMappableProjection(selectRelation)
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
        InsertSelectSourceColumns mapping = InsertSelectSourceColumns.resolve(
                insertStmt, selectRelation, target, resolvedSource.sourceTable(),
                resolvedSource.normalizedName(), resolvedSource.sourceAlias(),
                sortKeyColumns, partitionColumns);
        if (mapping == null) {
            return null;
        }
        InsertFromTableScanContext scanContext = new InsertFromTableScanContext(
                resolvedSource.sourceTable(), resolvedSource.sourceFromSql(),
                mapping.sortKeySourceColumnNames(), mapping.partitionSourceColumnNames(),
                wherePredicateSql, context.getCurrentComputeResource());
        long estimatedBytes = Math.max(0L, resolvedSource.sourceTable().getDataSize());
        return new PreSplitFlow.Prepared(scanContext, sortKeyColumns, partitionColumns,
                estimatedBytes, context.getCurrentComputeResource());
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
        // The sampler runs as UserIdentity.ROOT in a fresh statistics ConnectContext that cannot see
        // the user's session temporary tables. A temp-table source would be re-resolved by ROOT to the
        // shadowed permanent table (or fail), so the sample would observe different rows than the load
        // writes — skip pre-split entirely when the resolved source is a temporary table.
        if (sourceTable.isTemporaryTable()) {
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
}
