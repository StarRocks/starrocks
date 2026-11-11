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
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
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
 * list, expressions on non-key columns, safe deterministic expressions on key columns
 * ({@code date_trunc('day', ts) AS dt}), generated sort-key and partition columns computed from the
 * FILES columns they read, and an optional WHERE clause. {@link #prepare} triggers
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
        // Fold plan-time constants in the user's context before the gate, so the ROOT sampler
        // never evaluates a function that reads session state (time zone, query start time).
        Expr where = SamplingPredicateGate.foldPlanTimeConstants(selectRelation.getWhereClause(), context);
        if (where == null && selectRelation.getWhereClause() != null) {
            return null;
        }
        if (!SamplingPredicateGate.isDeterministicAndSafe(where, filesRelation.getName(), /*sourceAlias*/ null)) {
            return null;
        }
        String wherePredicateSql = where == null ? null : SamplingPredicateGate.toSql(where);

        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(target);
        List<Column> partitionColumns =
                target.getPartitionInfo().getPartitionColumns(target.getIdToColumn());
        List<SecondaryIndexSpec> secondaryIndexSpecs = SecondaryIndexSpec.forVisibleRollups(target);
        sourceTable = withInferredColumnTypes(insertStmt, filesRelation, sourceTable, selectRelation, where,
                InsertSelectSourceColumns.sampledColumns(sortKeyColumns, partitionColumns, secondaryIndexSpecs));
        if (sourceTable == null) {
            return null;
        }
        // Checked only now, against the column types the sampler's own FILES() call infers: the schema has just been
        // read again where push-down rewrote it.
        if (where != null && !SamplingPredicateGate.whereEvaluatesAsTheLoad(where, sourceTable.getFullVisibleSchema(),
                filesRelation.getName(), /*sourceAlias*/ null, context, targetNameForLog(insertStmt))) {
            return null;
        }
        // The data tier evaluates its projections over the same FILES() rows, so a safe computed key is admitted.
        InsertSelectSourceColumns.Resolved resolved = InsertSelectSourceColumns.resolveUngated(
                insertStmt, selectRelation, target,
                new InsertSelectSourceColumns.ResolutionContext(sourceTable, filesRelation.getName(),
                        /*sourceAlias*/ null, InsertSelectSourceColumns.SchemaPairing.PER_COLUMN, context));
        if (resolved == null) {
            return null;
        }
        // INSERT binds each SELECT output to a generated column's definition as selected, after column-type
        // push-down may have read a FILES column at a target column's type.
        InsertSelectSourceColumns.InputReading reading = columnTypesMayBePushedDown(insertStmt, sourceTable)
                ? InsertSelectSourceColumns.InputReading.AS_SELECTED_AFTER_PUSH_DOWN
                : InsertSelectSourceColumns.InputReading.AS_SELECTED;
        resolved = InsertSelectSourceColumns.admitSampledColumns(resolved, target, sortKeyColumns, partitionColumns,
                secondaryIndexSpecs, reading, context, filesRelation.getName(), /*sourceAlias*/ null);
        if (resolved == null) {
            return null;
        }
        Map<String, String> targetToSource = resolved.targetToSource();
        InsertFromFilesScanContext scanContext =
                new InsertFromFilesScanContext(sourceTable, context.getCurrentComputeResource(),
                        context.getSessionVariable().getTimeZone(), targetToSource, wherePredicateSql,
                        resolved.targetToConstantSql(), resolved.targetToExpressionSql(),
                        SampleSessionSemantics.capture(context.getSessionVariable()));
        // Deliberately the WHOLE file byte total even when a predicate narrows the load: FILES()
        // exposes no row count, so the data tier has no denominator to turn its observed hit ratio
        // into a filtered size the way the table path does. Sizing from the full input can only
        // over-split, and pre-split is a sizing optimization -- under-sizing would be the harmful
        // direction.
        return new PreSplitFlow.Prepared(scanContext, sortKeyColumns, partitionColumns,
                sumFileBytes(sourceTable), context.getCurrentComputeResource(), secondaryIndexSpecs);
    }

    /**
     * Whether the load may read a directly projected FILES column at a target column's type
     * ({@code InsertAnalyzer#pushDownTargetTableSchemaToFiles}): never when FILES declares its own schema,
     * otherwise when the FE config or the statement's {@code enable_push_down_schema} turns it on. A plain
     * INSERT reaches this hook before analysis parses that property, so its raw value is read as well;
     * anything but "false" counts as on, which only makes the check stricter.
     */
    private static boolean columnTypesMayBePushedDown(InsertStmt insertStmt, TableFunctionTable sourceTable) {
        if (sourceTable.hasExplicitSchema()) {
            return false;
        }
        if (Config.files_enable_insert_push_down_column_type || insertStmt.isEnablePushDownSchema()) {
            return true;
        }
        Map<String, String> properties = insertStmt.getProperties();
        String statementValue = properties == null
                ? null : properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PUSH_DOWN_SCHEMA);
        return statementValue != null && !"false".equalsIgnoreCase(statementValue.trim());
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

    /**
     * {@code boundTable} with the column types inferred from the {@code FILES()} properties, as the sampler's own call
     * infers them. The INSERT OVERWRITE
     * paths run this hook on a statement already analyzed, and analysis applied column-type push-down to the bound
     * table in place, so its types are the target's rather than the file's. The sampler infers the schema afresh,
     * so the schema is read again here, without push-down -- but only when a source column's type can matter
     * ({@link #readsSourceColumnTypes}). {@code null} when that read fails.
     *
     * @param where the WHERE clause after plan-time folding, or {@code null}
     */
    private static TableFunctionTable withInferredColumnTypes(
            InsertStmt insertStmt, FileTableFunctionRelation filesRelation, TableFunctionTable boundTable,
            SelectRelation selectRelation, Expr where, List<Column> sampledColumns) {
        if (filesRelation.getPushDownSchemaFunc() == null || boundTable.hasExplicitSchema()
                || !readsSourceColumnTypes(selectRelation, where, sampledColumns)) {
            return boundTable;
        }
        try {
            return new TableFunctionTable(boundTable.getProperties());
        } catch (DdlException | RuntimeException failure) {
            LOG.info("Sample-Based Tablet Pre-Split: re-reading the FILES() schema failed for table {}; skipping: {}",
                    targetNameForLog(insertStmt), failure.getMessage());
            return null;
        }
    }

    /**
     * Whether a source column's type can matter to the sample: a computed or constant SELECT item, a generated sampled
     * column, or a folded WHERE clause that still holds a call. Only such a WHERE is analyzed against the column
     * types, to check that each call binds to a built-in function ({@link SamplingPredicateGate#evaluatesAsTheLoad});
     * a WHERE clause without one needs no such check. (The load may still compare a pushed-down column at another
     * type than the sampler; this re-read does not address that.)
     */
    private static boolean readsSourceColumnTypes(SelectRelation selectRelation, Expr where,
                                                  List<Column> sampledColumns) {
        return (where != null && where.containsSubclass(FunctionCallExpr.class))
                || sampledColumns.stream().anyMatch(Column::isGeneratedColumn)
                || selectRelation.getSelectList().getItems().stream()
                        .anyMatch(item -> !item.isStar() && !(item.getExpr() instanceof SlotRef));
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
