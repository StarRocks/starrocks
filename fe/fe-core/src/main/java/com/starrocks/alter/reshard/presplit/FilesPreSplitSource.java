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
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * INSERT-from-FILES pre-split source. Matches {@code INSERT INTO target SELECT ... FROM FILES(...)}
 * for every projection shape the sampler can reproduce in its own
 * {@code SELECT <key> FROM FILES(<verbatim properties>)} sub-query: a bare star, an explicit column
 * list, expressions on non-key columns, and an optional WHERE clause. {@link #prepare} triggers
 * FILES() schema inference, gates the WHERE predicate, maps the projection onto the target with
 * {@link InsertSelectSourceColumns}, and builds an {@link InsertFromFilesScanContext} for the
 * shared flow.
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
        return InsertPreSplitHook.hasSupportedProjectionShape(selectRelation)
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
        // The parser drops any alias written on FILES(...) (AstBuilder#visitFileTableFunction keeps
        // only the property list), so no alias is ever in scope and the sole qualifier a slot may
        // carry is the relation's own synthetic name -- which the sampler's own FILES() call carries
        // too, so such a predicate re-resolves inside the sub-query.
        Expr where = selectRelation.getWhereClause();
        if (!SamplingPredicateGate.isDeterministicAndSafe(where, filesRelation.getName(), /*sourceAlias*/ null)) {
            return null;
        }
        String wherePredicateSql = where == null ? null : SamplingPredicateGate.toSql(where);

        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(target);
        List<Column> partitionColumns =
                target.getPartitionInfo().getPartitionColumns(target.getIdToColumn());
        Map<String, String> targetToSource = InsertSelectSourceColumns.resolve(
                insertStmt, selectRelation, target, sourceTable,
                filesRelation.getName(), /*sourceAlias*/ null, sortKeyColumns, partitionColumns,
                InsertSelectSourceColumns.SchemaPairing.PER_COLUMN);
        if (targetToSource == null) {
            return null;
        }
        List<SecondaryIndexSpec> secondaryIndexSpecs = SecondaryIndexSpec.forVisibleRollups(target);
        for (SecondaryIndexSpec spec : secondaryIndexSpecs) {
            // A rollup sort-key column with no FILES column behind it cannot be projected by source
            // name -> skip pre-split for the whole load, using the same lookup gate the base sort key
            // already passed inside resolve(). Mirrors TablePreSplitSource.
            if (InsertSelectSourceColumns.lookup(spec.sortKey(), targetToSource) == null) {
                return null;
            }
        }
        InsertFromFilesScanContext scanContext =
                new InsertFromFilesScanContext(sourceTable, context.getCurrentComputeResource(),
                        context.getSessionVariable().getTimeZone(), targetToSource, wherePredicateSql);
        // Deliberately the WHOLE file byte total even when a predicate narrows the load: FILES()
        // exposes no row count, so the data tier has no denominator to turn its observed hit ratio
        // into a filtered size the way the table path does. Sizing from the full input can only
        // over-split, and pre-split is a sizing optimization -- under-sizing would be the harmful
        // direction.
        return new PreSplitFlow.Prepared(scanContext, sortKeyColumns, partitionColumns,
                sumFileBytes(sourceTable), context.getCurrentComputeResource(), secondaryIndexSpecs);
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
