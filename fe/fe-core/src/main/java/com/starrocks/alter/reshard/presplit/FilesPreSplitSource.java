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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * INSERT-from-FILES pre-split source. Matches only the strict bare
 * {@code INSERT INTO target SELECT * FROM FILES(...)} shape. {@link #prepare}
 * triggers FILES() schema inference, enforces by-position name alignment
 * between the target and the inferred FILES schema, and builds an
 * {@link InsertFromFilesScanContext} for the shared flow.
 */
final class FilesPreSplitSource implements InsertPreSplitSource {

    private static final Logger LOG = LogManager.getLogger(FilesPreSplitSource.class);

    @Override
    public boolean configEnabled() {
        return Config.enable_tablet_pre_split_for_insert_from_files;
    }

    @Override
    public LoadKind loadKind() {
        return LoadKind.INSERT_FROM_FILES;
    }

    @Override
    public boolean matches(InsertStmt insertStmt, SelectRelation selectRelation) {
        return isStraightStarProjection(selectRelation)
                && selectRelation.getRelation() instanceof FileTableFunctionRelation;
    }

    @Override
    public PreSplitFlow.Prepared prepare(InsertStmt insertStmt, SelectRelation selectRelation,
                                         OlapTable target, Database database, ConnectContext context) {
        FileTableFunctionRelation filesRelation = (FileTableFunctionRelation) selectRelation.getRelation();
        TableFunctionTable sourceTable = resolveSourceTable(insertStmt, filesRelation, context);
        if (sourceTable == null) {
            return null;
        }
        if (!schemasAlignForByPositionInsert(insertStmt, target, sourceTable)) {
            return null;
        }
        InsertFromFilesScanContext scanContext =
                new InsertFromFilesScanContext(sourceTable, context.getCurrentComputeResource());
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(target);
        List<Column> partitionColumns =
                target.getPartitionInfo().getPartitionColumns(target.getIdToColumn());
        return new PreSplitFlow.Prepared(scanContext, sortKeyColumns, partitionColumns,
                sumFileBytes(sourceTable), context.getCurrentComputeResource());
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
        // The effective target columns are the explicit column list when present
        // (a partial list omits non-key columns, which are defaulted), otherwise
        // the full base schema. The load writes FILES column N into effective
        // target column N by position; requiring name equality at every ordinal
        // makes the sampler's by-name read of a sort-key column resolve to the
        // same FILES column the load writes into that key.
        List<String> targetColumnNames = effectiveTargetColumnNames(insertStmt, targetTable);
        List<Column> sourceColumns = sourceTable.getFullSchema();
        if (targetColumnNames.size() != sourceColumns.size()) {
            return false;
        }
        for (int ordinal = 0; ordinal < targetColumnNames.size(); ordinal++) {
            String targetName = targetColumnNames.get(ordinal);
            String sourceName = sourceColumns.get(ordinal).getName();
            if (!targetName.equalsIgnoreCase(sourceName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Target column names in INSERT (by-position) order: the explicit target
     * column list when present, otherwise the base (non-generated) schema names.
     */
    private static List<String> effectiveTargetColumnNames(InsertStmt insertStmt, OlapTable targetTable) {
        List<String> targetColumnNames = insertStmt.getTargetColumnNames();
        if (targetColumnNames != null && !targetColumnNames.isEmpty()) {
            return targetColumnNames;
        }
        return targetTable.getBaseSchemaWithoutGeneratedColumn().stream()
                .map(Column::getName)
                .collect(Collectors.toList());
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
