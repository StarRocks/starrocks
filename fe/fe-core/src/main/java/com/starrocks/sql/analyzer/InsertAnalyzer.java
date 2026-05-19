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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.iceberg.IcebergRowLineageUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.DefaultValueExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.NullType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.SnapshotRef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.catalog.OlapTable.OlapTableState.NORMAL;
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class InsertAnalyzer {
    private static final Logger LOG = LogManager.getLogger(InsertAnalyzer.class);
    private static final ImmutableSet<String> PUSH_DOWN_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(LoadStmt.STRICT_MODE)
            .build();

    /**
     * Normal path of analyzer
     */
    public static void analyze(InsertStmt insertStmt, ConnectContext session) {
        analyzeWithDeferredLock(insertStmt, session, () -> {
        });
    }

    /**
     * An optimistic path of analyzer for INSERT-SELECT, whose SELECT doesn't need a lock
     * So we can analyze the SELECT without lock, only take the lock when analyzing INSERT TARGET
     */
    public static void analyzeWithDeferredLock(InsertStmt insertStmt, ConnectContext session, Runnable takeLock) {
        try {
            // insert properties
            analyzeProperties(insertStmt, session);

            // push down schema to files
            pushDownTargetTableSchemaToFiles(insertStmt, session);

            new QueryAnalyzer(session).analyze(insertStmt.getQueryStatement());

            List<Table> tables = new ArrayList<>();
            AnalyzerUtils.collectSpecifyExternalTables(insertStmt.getQueryStatement(), tables, Table::isHiveTable);
            if (tables.stream().anyMatch(Table::isHiveTable) && session.getUseConnectorMetadataCache().isEmpty()) {
                session.setUseConnectorMetadataCache(Optional.of(false));
            }
        } finally {
            takeLock.run();
        }

        /*
         *  Target table
         */
        Table table;
        if (insertStmt.getTargetTable() != null) {
            // For the OLAP external table,
            // the target table is synchronized from another cluster and saved into InsertStmt during beginTransaction.
            table = insertStmt.getTargetTable();
        } else {
            table = getTargetTable(insertStmt, session);
        }

        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            List<Long> targetPartitionIds = Lists.newArrayList();
            PartitionRef targetPartitionNames = insertStmt.getTargetPartitionNames();

            if (insertStmt.isSpecifyPartitionNames()) {
                if (targetPartitionNames.getPartitionNames().isEmpty()) {
                    throw new SemanticException("No partition specified in partition lists",
                            targetPartitionNames.getPos());
                }

                List<String> deduplicatePartitionNames =
                        targetPartitionNames.getPartitionNames().stream().distinct().collect(Collectors.toList());
                if (deduplicatePartitionNames.size() != targetPartitionNames.getPartitionNames().size()) {
                    insertStmt.setTargetPartitionNames(new PartitionRef(deduplicatePartitionNames,
                            targetPartitionNames.isTemp(), targetPartitionNames.getPartitionColNames(),
                            targetPartitionNames.getPartitionColValues(), targetPartitionNames.getPos()));
                }
                for (String partitionName : deduplicatePartitionNames) {
                    if (Strings.isNullOrEmpty(partitionName)) {
                        throw new SemanticException("there are empty partition name", targetPartitionNames.getPos());
                    }

                    Partition partition = olapTable.getPartition(partitionName, targetPartitionNames.isTemp());
                    if (partition == null) {
                        throw new SemanticException("Unknown partition '%s' in table '%s'", partitionName,
                                olapTable.getName(), targetPartitionNames.getPos());
                    }
                    targetPartitionIds.add(partition.getId());
                }
            } else if (insertStmt.isStaticKeyPartitionInsert()) {
                checkStaticKeyPartitionInsert(insertStmt, table, targetPartitionNames);
            } else {
                if ((insertStmt.isOverwrite() && session.getSessionVariable().isDynamicOverwrite())
                        && olapTable.supportedAutomaticPartition()) {
                    insertStmt.setIsDynamicOverwrite(true);
                } else {
                    for (Partition partition : olapTable.getPartitions()) {
                        targetPartitionIds.add(partition.getId());
                    }
                    if (targetPartitionIds.isEmpty()) {
                        throw new SemanticException("data cannot be inserted into table with empty partition." +
                                "Use `SHOW PARTITIONS FROM %s` to see the currently partitions of this table. ",
                                olapTable.getName());
                    }
                }
            }
            insertStmt.setTargetPartitionIds(targetPartitionIds);
        }

        if (table.isIcebergTable() || table.isHiveTable()) {
            if (table.isHiveTable() && ((HiveTable) table).getHiveTableType() != HiveTable.HiveTableType.MANAGED_TABLE &&
                    !session.getSessionVariable().enableWriteHiveExternalTable()) {
                throw new SemanticException("Only support to write hive managed table, tableType: " +
                        ((HiveTable) table).getHiveTableType());
            }

            PartitionRef targetPartitionNames = insertStmt.getTargetPartitionNames();
            List<String> tablePartitionColumnNames = table.getPartitionColumnNames();
            if (insertStmt.getTargetColumnNames() != null) {
                for (String partitionColName : tablePartitionColumnNames) {
                    // case-insensitive match. refer to AstBuilder#getColumnNames
                    if (!insertStmt.getTargetColumnNames().contains(partitionColName.toLowerCase())) {
                        throw new SemanticException("Must include partition column %s", partitionColName);
                    }
                }
            } else if (insertStmt.isStaticKeyPartitionInsert()) {
                checkStaticKeyPartitionInsert(insertStmt, table, targetPartitionNames);
            }
            if (!table.isIcebergTable()) {
                List<Column> partitionColumns = tablePartitionColumnNames.stream()
                        .map(table::getColumn)
                        .collect(Collectors.toList());
                for (Column column : partitionColumns) {
                    if (isUnSupportedPartitionColumnType(column.getType())) {
                        throw new SemanticException("Unsupported partition column type [%s] for %s table sink",
                                column.getType().canonicalName(), table.getType());
                    }
                }
            }
        }

        // Set insert stmt target columns using select output columns if match column by name
        QueryRelation query = insertStmt.getQueryStatement().getQueryRelation();
        if (insertStmt.isColumnMatchByName()) {
            if (query instanceof ValuesRelation) {
                throw new SemanticException("Insert match column by name does not support values()");
            }

            Set<String> selectColumnNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            for (String colName : query.getColumnOutputNames()) {
                if (!selectColumnNames.add(colName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, colName);
                }
            }

            // column name is case insensitive
            insertStmt.setTargetColumnNames(
                    query.getColumnOutputNames().stream().map(String::toLowerCase).collect(Collectors.toList()));
        }

        // Build target columns
        List<Column> targetColumns;
        Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (insertStmt.getTargetColumnNames() == null) {
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                targetColumns = new ArrayList<>(olapTable.getBaseSchemaWithoutGeneratedColumn());
                mentionedColumns.addAll(olapTable.getBaseSchemaWithoutGeneratedColumn().stream().map(Column::getName)
                        .collect(Collectors.toSet()));
            } else if (table instanceof IcebergTable) {
                IcebergTable icebergTable = (IcebergTable) table;
                boolean writeRowLineage = IcebergRowLineageUtils.shouldWriteRowLineageColumns(insertStmt, icebergTable);
                targetColumns = new ArrayList<>();
                icebergTable.getFullSchema().forEach(column -> {
                    if (!column.getName().startsWith(FeConstants.GENERATED_PARTITION_COLUMN_PREFIX) &&
                            (!IcebergTable.ICEBERG_META_COLUMNS.contains(column.getName())
                                    || (writeRowLineage
                                    && (column.getName().equals(IcebergTable.ROW_ID)
                                    || column.getName().equals(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER))))) {
                        targetColumns.add(column);
                    }
                });
                mentionedColumns.addAll(targetColumns.stream().map(Column::getName).collect(Collectors.toSet()));
            } else {
                targetColumns = new ArrayList<>(table.getBaseSchema());
                mentionedColumns.addAll(table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toSet()));
            }
        } else {
            targetColumns = new ArrayList<>();
            Set<String> requiredKeyColumns = table.getBaseSchema().stream().filter(Column::isKey)
                    .filter(c -> c.getDefaultValueType() == Column.DefaultValueType.NULL)
                    .filter(c -> !c.isAutoIncrement()).map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
            for (String colName : insertStmt.getTargetColumnNames()) {
                Column column = table.getColumn(colName);
                if (column == null) {
                    throw new SemanticException("Unknown column '%s' in '%s'", colName, table.getName());
                }
                if (column.isGeneratedColumn()) {
                    throw new SemanticException("generated column '%s' can not be specified", colName);
                }
                if (!mentionedColumns.add(colName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, colName);
                }
                requiredKeyColumns.remove(colName.toLowerCase());
                targetColumns.add(column);
            }
            if (table.isNativeTable()) {
                OlapTable olapTable = (OlapTable) table;
                if (olapTable.getKeysType().equals(KeysType.PRIMARY_KEYS)) {
                    if (!requiredKeyColumns.isEmpty()) {
                        String missingKeyColumns = String.join(",", requiredKeyColumns);
                        ErrorReport.reportSemanticException(ErrorCode.ERR_MISSING_KEY_COLUMNS, missingKeyColumns);
                    }
                    if (targetColumns.size() < olapTable.getBaseSchemaWithoutGeneratedColumn().size() &&
                            session.getSessionVariable().isEnableInsertPartialUpdate()) {
                        insertStmt.setUsePartialUpdate();
                        // mark if partial update for auto increment column if and only if:
                        // 1. There is auto increment defined in base schema
                        // 2. targetColumns does not contain auto increment column
                        // 3. auto increment column is not key column
                        if (olapTable.hasAutoIncrementColumn() &&
                                !targetColumns.stream().anyMatch(col -> col.isAutoIncrement())) {
                            Column autoIncrementColumn =
                                    table.getBaseSchema().stream().filter(Column::isAutoIncrement).findFirst().get();
                            if (!autoIncrementColumn.isKey()) {
                                insertStmt.setAutoIncrementPartialUpdate();
                            }
                        }
                    }
                }
            }
        }

        if (!insertStmt.usePartialUpdate()) {
            for (Column column : table.getBaseSchema()) {
                Column.DefaultValueType defaultValueType = column.getDefaultValueType();
                if (defaultValueType == Column.DefaultValueType.NULL &&
                        !column.isAllowNull() &&
                        !column.isAutoIncrement() &&
                        !column.isGeneratedColumn() &&
                        !mentionedColumns.contains(column.getName())) {
                    StringBuilder msg = new StringBuilder();
                    for (String s : mentionedColumns) {
                        msg.append(" ").append(s).append(" ");
                    }
                    throw new SemanticException("'%s' must be explicitly mentioned in column permutation: %s",
                            column.getName(), msg.toString());
                }
            }
        }

        int mentionedColumnSize = mentionedColumns.size();
        if ((table.isIcebergTable() || table.isHiveTable()) && insertStmt.isStaticKeyPartitionInsert()) {
            // full column size = mentioned column size + partition column size for static partition insert
            mentionedColumnSize -= table.getPartitionColumnNames().size();
            mentionedColumns.removeAll(table.getPartitionColumnNames());
        }

        // check target and source columns match
        if (query.getRelationFields().size() != mentionedColumnSize) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INSERT_COLUMN_COUNT_MISMATCH, mentionedColumnSize,
                    query.getRelationFields().size());
        }

        // check default value expr
        if (query instanceof ValuesRelation) {
            ValuesRelation valuesRelation = (ValuesRelation) query;
            for (List<Expr> row : valuesRelation.getRows()) {
                for (int columnIdx = 0; columnIdx < row.size(); ++columnIdx) {
                    Column column = targetColumns.get(columnIdx);
                    Column.DefaultValueType defaultValueType = column.getDefaultValueType();
                    if (row.get(columnIdx) instanceof DefaultValueExpr &&
                            defaultValueType == Column.DefaultValueType.NULL &&
                            !column.isAutoIncrement()) {
                        throw new SemanticException("Column has no default value, column=%s", column.getName());
                    }

                    AnalyzerUtils.verifyNoAggregateFunctions(row.get(columnIdx), "Values");
                    AnalyzerUtils.verifyNoWindowFunctions(row.get(columnIdx), "Values");
                }
            }
        }

        insertStmt.setTargetTable(table);
        if (session.getDumpInfo() != null) {
            session.getDumpInfo().addTable(insertStmt.getDbName(), table);
        }

        // Set table function table used for load
        List<FileTableFunctionRelation> relations =
                AnalyzerUtils.collectFileTableFunctionRelation(insertStmt.getQueryStatement());
        for (FileTableFunctionRelation relation : relations) {
            ((TableFunctionTable) relation.getTable()).setFilesTableType(TableFunctionTable.FilesTableType.LOAD);
        }
    }

    private static void analyzeProperties(InsertStmt insertStmt, ConnectContext session) {
        Map<String, String> properties = insertStmt.getProperties();

        // check common properties
        // use session variable if not set max_filter_ratio, strict_mode, timeout property
        if (!properties.containsKey(LoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
            properties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY,
                    String.valueOf(session.getSessionVariable().getInsertMaxFilterRatio()));
        }
        if (!properties.containsKey(LoadStmt.STRICT_MODE)) {
            properties.put(LoadStmt.STRICT_MODE, String.valueOf(session.getSessionVariable().getEnableInsertStrict()));
        }
        if (!properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
            properties.put(LoadStmt.TIMEOUT_PROPERTY, String.valueOf(session.getSessionVariable().getInsertTimeoutS()));
        }

        // enable_push_down_schema is an INSERT-only property; validate and consume it here so that
        // malformed values are rejected regardless of the INSERT shape, and LoadStmt.checkProperties
        // does not see the key at all.
        insertStmt.setEnablePushDownSchema(PropertyAnalyzer.analyzeBooleanPropStrictly(
                properties, PropertyAnalyzer.PROPERTIES_ENABLE_PUSH_DOWN_SCHEMA, false));
        try {
            LoadStmt.checkProperties(properties);
        } catch (DdlException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
        }

        // push down some properties to file table function
        List<FileTableFunctionRelation> relations =
                AnalyzerUtils.collectFileTableFunctionRelation(insertStmt.getQueryStatement());
        for (FileTableFunctionRelation relation : relations) {
            Map<String, String> tableFunctionProperties = relation.getProperties();
            for (String property : PUSH_DOWN_PROPERTIES_SET) {
                if (properties.containsKey(property)) {
                    tableFunctionProperties.put(property, properties.get(property));
                }
            }
        }
    }

    /**
     * Push down target table column schema to files() table function.
     * <p>
     * Two modes controlled by different switches:
     * 1. insert property "enable_push_down_schema" = true:
     * Full push-down — reshapes files() schema to match the effective SELECT list:
     * - SlotRef columns: type is rewritten to the matching target column type; added if missing.
     * - Function-expression columns: inner SlotRef columns are only ensured to exist in the schema
     * (defaulting to VARCHAR if absent); no type push-down, because the function itself determines
     * the output type.
     * - Extra file columns not referenced in the SELECT list are excluded.
     * 2. FE config "files_enable_insert_push_down_column_type" = true (default):
     * Type-only push-down — only rewrites types of columns that already exist in the inferred files() schema.
     */
    private static void pushDownTargetTableSchemaToFiles(InsertStmt insertStmt, ConnectContext session) {
        if (!insertStmt.isEnablePushDownSchema() && !Config.files_enable_insert_push_down_column_type) {
            return;
        }

        if (insertStmt.useTableFunctionAsTargetTable() || insertStmt.useBlackHoleTableAsTargetTable()) {
            return;
        }

        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        if (!(queryRelation instanceof SelectRelation)) {
            return;
        }
        SelectRelation selectRelation = (SelectRelation) queryRelation;
        Relation fromRelation = selectRelation.getRelation();
        if (!(fromRelation instanceof FileTableFunctionRelation)) {
            return;
        }

        Table targetTable = getTargetTable(insertStmt, session);
        if (!targetTable.isNativeTable()) {
            return;
        }

        String dbName = insertStmt.getDbName();
        String catalogName = insertStmt.getCatalogName();

        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session, catalogName, dbName);
        if (database == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        Consumer<TableFunctionTable> pushDownSchemaFunc = (fileTable) -> {
            Locker locker = new Locker(session.getQueryId());
            locker.lockTableWithIntensiveDbLock(database.getId(), targetTable.getId(), LockType.READ);
            try {
                if (insertStmt.isEnablePushDownSchema()) {
                    rewriteFileTableSchema(fileTable, insertStmt, targetTable, selectRelation);
                } else {
                    rewriteFileTableColumnTypes(fileTable, insertStmt, targetTable, selectRelation);
                }
            } finally {
                locker.unLockTableWithIntensiveDbLock(database.getId(), targetTable.getId(), LockType.READ);
            }
        };

        FileTableFunctionRelation fileRelation = (FileTableFunctionRelation) fromRelation;
        fileRelation.setPushDownSchemaFunc(pushDownSchemaFunc);
    }

    // ======================== Type-only push-down (original behavior) ========================

    /**
     * Rewrite column types only for columns that already exist in the inferred files schema.
     * Does not add or remove columns.
     */
    private static void rewriteFileTableColumnTypes(TableFunctionTable fileTable, InsertStmt insertStmt,
                                                    Table targetTable, SelectRelation selectRelation) {
        List<String> selectColumnNames = Lists.newArrayList();
        List<String> selectOutputNames = Lists.newArrayList();
        for (SelectListItem item : selectRelation.getSelectList().getItems()) {
            if (item.isStar()) {
                List<String> fileColNames = fileTable.getFullSchema().stream().map(Column::getName)
                        .collect(Collectors.toList());
                selectColumnNames.addAll(fileColNames);
                selectOutputNames.addAll(fileColNames);
            } else if (item.getExpr() instanceof SlotRef) {
                SlotRef ref = (SlotRef) item.getExpr();
                selectColumnNames.add(ref.getColumnName());
                String alias = item.getAlias();
                selectOutputNames.add(alias != null ? alias : ref.getColumnName());
            } else {
                selectColumnNames.add(null);
                selectOutputNames.add(null);
            }
        }

        List<String> targetColumnNames;
        if (insertStmt.isColumnMatchByName()) {
            targetColumnNames = selectOutputNames;
        } else {
            targetColumnNames = insertStmt.getTargetColumnNames();
            if (targetColumnNames == null) {
                targetColumnNames = ((OlapTable) targetTable).getBaseSchemaWithoutGeneratedColumn().stream()
                        .map(Column::getName).collect(Collectors.toList());
            }
        }

        if (targetColumnNames.size() != selectColumnNames.size()) {
            return;
        }

        Map<String, Column> targetTableColumns = targetTable.getNameToColumn();
        Map<String, Column> newFileColumns = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        newFileColumns.putAll(fileTable.getNameToColumn());
        for (int i = 0; i < selectColumnNames.size(); ++i) {
            String selectCol = selectColumnNames.get(i);
            if (selectCol == null || !newFileColumns.containsKey(selectCol)) {
                continue;
            }
            String targetCol = targetColumnNames.get(i);
            if (!targetTableColumns.containsKey(targetCol)) {
                continue;
            }
            Column oldCol = newFileColumns.get(selectCol);
            Column newCol = targetTableColumns.get(targetCol).deepCopy();
            if (oldCol.getType().isComplexType() || newCol.getType().isComplexType()) {
                continue;
            }
            // Keep original column name (BE is case-sensitive) and nullable property
            newCol.setName(oldCol.getName());
            newCol.setIsAllowNull(oldCol.isAllowNull());
            newFileColumns.put(oldCol.getName(), newCol);
        }

        List<Column> newSchema = fileTable.getFullSchema().stream()
                .map(col -> newFileColumns.get(col.getName())).collect(Collectors.toList());
        fileTable.setNewFullSchema(newSchema);
    }

    // ======================== Full schema push-down ========================

    /**
     * Rewrite the files() table schema by pushing down target table column names and types.
     * <p>
     * Three kinds of SELECT items are handled:
     * - SELECT *: reshape files schema to exactly the target columns (by name or by position)
     * - SlotRef (e.g., col_a): push down the corresponding target column's type; add column if missing
     * - Function expr (e.g., CAST(col_b AS INT)): inner SlotRef columns are kept as VARCHAR if missing
     * <p>
     * The final files schema only contains columns that are actually used in the SELECT list.
     */
    private static void rewriteFileTableSchema(TableFunctionTable fileTable, InsertStmt insertStmt,
                                               Table targetTable, SelectRelation selectRelation) {
        List<String> targetColumnNames = insertStmt.getTargetColumnNames();
        if (targetColumnNames == null) {
            targetColumnNames = ((OlapTable) targetTable).getBaseSchemaWithoutGeneratedColumn().stream()
                    .map(Column::getName).collect(Collectors.toList());
        }

        // Step 1: parse SELECT list into column name mappings.
        // selectColumnNames: file-side column names (null for non-SlotRef expressions)
        // selectOutputNames: output names used to find the matching target column in BY NAME mode
        List<String> selectColumnNames = Lists.newArrayList();
        List<String> selectOutputNames = Lists.newArrayList();
        List<Expr> funcExprs = Lists.newArrayList();

        for (SelectListItem item : selectRelation.getSelectList().getItems()) {
            if (item.isStar()) {
                expandStarColumns(selectColumnNames, selectOutputNames,
                        insertStmt, targetColumnNames, fileTable);
            } else if (item.getExpr() instanceof SlotRef) {
                SlotRef ref = (SlotRef) item.getExpr();
                selectColumnNames.add(ref.getColumnName());
                String alias = item.getAlias();
                selectOutputNames.add(alias != null ? alias : ref.getColumnName());
            } else {
                selectColumnNames.add(null);
                selectOutputNames.add(null);
                funcExprs.add(item.getExpr());
            }
        }

        // For BY NAME, each SELECT item maps to its target column by output name independently.
        // targetColumnNames (full schema) is only needed above for expandStarColumns(*).
        // Override it with selectOutputNames so the size check works correctly for partial SELECTs.
        if (insertStmt.isColumnMatchByName()) {
            targetColumnNames = selectOutputNames;
        }

        if (targetColumnNames.size() != selectColumnNames.size()) {
            return;
        }

        // Step 2: for each SlotRef column, push down the target column's type.
        // If the column is absent from the inferred files schema, add it with the target type.
        Map<String, Column> targetTableColumns = targetTable.getNameToColumn();
        Map<String, Column> newFileColumns = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        newFileColumns.putAll(fileTable.getNameToColumn());

        for (int i = 0; i < selectColumnNames.size(); i++) {
            String selectCol = selectColumnNames.get(i);
            if (selectCol == null) {
                continue;
            }
            String targetCol = insertStmt.isColumnMatchByName()
                    ? selectOutputNames.get(i) : targetColumnNames.get(i);
            if (targetCol == null || !targetTableColumns.containsKey(targetCol)) {
                continue;
            }
            pushDownColumnType(selectCol, targetTableColumns.get(targetCol), newFileColumns);
        }

        // Step 3: for function expressions, ensure inner SlotRef columns exist in the schema.
        // Their output type is determined by the function, so we only need the column to be readable.
        // Columns that already exist in the file (added in Step 2) are untouched.
        // Columns absent from the physical file are added as nullable VARCHAR as a placeholder;
        // they will read NULL at runtime (requires fill_mismatch_column_with=null).
        // Note: if such a missing column is used in a type-sensitive expression (e.g. arithmetic),
        // the VARCHAR placeholder may cause a semantic type error. This is an obscure edge case.
        for (Expr funcExpr : funcExprs) {
            List<SlotRef> refs = Lists.newArrayList();
            funcExpr.collect(SlotRef.class, refs);
            for (SlotRef ref : refs) {
                String colName = ref.getColumnName();
                if (!newFileColumns.containsKey(colName)) {
                    newFileColumns.put(colName, new Column(colName, VarcharType.VARCHAR, true));
                }
            }
        }

        // Step 4: rebuild files schema with only the columns used in the SELECT list.
        // selectColumnNames (after star expansion) covers direct SlotRefs and star-expanded columns.
        // funcExprs inner SlotRefs are appended after.
        fileTable.setNewFullSchema(buildNewFileSchema(selectColumnNames, funcExprs, newFileColumns));
    }

    /**
     * Expand SELECT * to column names based on the insert matching mode.
     * BY NAME: use target column names so extra file columns are excluded.
     * BY POSITION: use file column names trimmed or extended to match target column count.
     */
    private static void expandStarColumns(List<String> selectColumnNames, List<String> selectOutputNames,
                                          InsertStmt insertStmt, List<String> targetColumnNames, TableFunctionTable fileTable) {
        if (insertStmt.isColumnMatchByName()) {
            selectColumnNames.addAll(targetColumnNames);
            selectOutputNames.addAll(targetColumnNames);
        } else {
            List<Column> fileSchema = fileTable.getFullSchema();
            for (int j = 0; j < targetColumnNames.size(); j++) {
                String name = j < fileSchema.size()
                        ? fileSchema.get(j).getName() : targetColumnNames.get(j);
                selectColumnNames.add(name);
                selectOutputNames.add(name);
            }
        }
    }

    /**
     * Push down a target column's type to the corresponding file column.
     * If the file column doesn't exist, add it with the target type.
     * Skips complex types which may fail to convert in the scanner.
     */
    private static void pushDownColumnType(String fileColName, Column targetCol,
                                           Map<String, Column> fileColumns) {
        Column newCol = targetCol.deepCopy();
        if (newCol.getType().isComplexType()) {
            return;
        }

        if (!fileColumns.containsKey(fileColName)) {
            // Column not inferred from files; add it so the scanner can look it up by name.
            // The scanner returns null if the column is absent from the physical file.
            newCol.setName(fileColName);
            newCol.setIsAllowNull(true);
            fileColumns.put(fileColName, newCol);
            return;
        }

        Column oldCol = fileColumns.get(fileColName);
        if (oldCol.getType().isComplexType()) {
            return;
        }

        // Keep original column name (BE is case-sensitive) and nullable property
        newCol.setName(oldCol.getName());
        newCol.setIsAllowNull(oldCol.isAllowNull());
        fileColumns.put(oldCol.getName(), newCol);
    }

    /**
     * Build the final file schema containing only columns used in the SELECT list:
     * 1. Non-null entries in selectColumnNames (SlotRefs and star-expanded columns), in order.
     * 2. Inner SlotRef columns from function expressions, appended after.
     * Duplicate column names are deduplicated (case-insensitive).
     */
    private static List<Column> buildNewFileSchema(List<String> selectColumnNames,
                                                   List<Expr> funcExprs, Map<String, Column> fileColumns) {
        Set<String> seen = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        List<Column> schema = Lists.newArrayList();

        for (String name : selectColumnNames) {
            if (name != null && seen.add(name)) {
                Column col = fileColumns.get(name);
                if (col != null) {
                    schema.add(col);
                }
            }
        }

        for (Expr funcExpr : funcExprs) {
            List<SlotRef> refs = Lists.newArrayList();
            funcExpr.collect(SlotRef.class, refs);
            for (SlotRef ref : refs) {
                String colName = ref.getColumnName();
                if (seen.add(colName)) {
                    Column col = fileColumns.get(colName);
                    if (col != null) {
                        schema.add(col);
                    }
                }
            }
        }

        return schema;
    }

    private static void checkStaticKeyPartitionInsert(InsertStmt insertStmt, Table table, PartitionRef targetPartitionNames) {
        List<String> partitionColNames = targetPartitionNames.getPartitionColNames();
        List<Expr> partitionColValues = targetPartitionNames.getPartitionColValues();
        List<String> tablePartitionColumnNames = table.getPartitionColumnNames();

        Preconditions.checkState(partitionColNames.size() == partitionColValues.size(),
                "Partition column names size must be equal to the partition column values size. %d vs %d",
                partitionColNames.size(), partitionColValues.size());

        if (tablePartitionColumnNames.size() > partitionColNames.size()) {
            throw new SemanticException("Must include all %d partition columns in the partition clause",
                    tablePartitionColumnNames.size());
        }

        if (tablePartitionColumnNames.size() < partitionColNames.size()) {
            throw new SemanticException("Only %d partition columns can be included in the partition clause",
                    tablePartitionColumnNames.size());
        }
        Map<String, Long> frequencies = partitionColNames.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        Optional<Map.Entry<String, Long>> duplicateKey = frequencies.entrySet().stream()
                .filter(entry -> entry.getValue() > 1).findFirst();
        if (duplicateKey.isPresent()) {
            throw new SemanticException("Found duplicate partition column name %s", duplicateKey.get().getKey());
        }

        for (int i = 0; i < partitionColNames.size(); i++) {
            String actualName = partitionColNames.get(i);
            if (!AnalyzerUtils.containsIgnoreCase(tablePartitionColumnNames, actualName)) {
                throw new SemanticException("Can't find partition column %s", actualName);
            }

            Expr partitionValue = partitionColValues.get(i);

            if (!ExprUtils.isLiteral(partitionValue)) {
                throw new SemanticException("partition value should be literal expression");
            }

            LiteralExpr literalExpr = (LiteralExpr) partitionValue;
            Column column = table.getColumn(actualName);
            try {
                Type type = literalExpr.isConstantNull() ? NullType.NULL : column.getType();
                Expr expr = LiteralExprFactory.create(literalExpr.getStringValue(), type);
                insertStmt.getTargetPartitionNames().getPartitionColValues().set(i, expr);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }
    }

    private static Table getTargetTable(InsertStmt insertStmt, ConnectContext session) {
        if (insertStmt.useTableFunctionAsTargetTable()) {
            return insertStmt.makeTableFunctionTable(session.getSessionVariable());
        } else if (insertStmt.useBlackHoleTableAsTargetTable()) {
            return insertStmt.makeBlackHoleTable();
        }

        TableRef tableRef = AnalyzerUtils.normalizedTableRef(insertStmt.getTableRef(), session);
        if (Strings.isNullOrEmpty(tableRef.getDbName()) || Strings.isNullOrEmpty(tableRef.getCatalogName())) {
            TableName tableName = TableName.fromTableRef(tableRef);
            tableName.normalization(session);
            QualifiedName normalizedName = QualifiedName.of(
                    Arrays.asList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl()),
                    tableRef.getPos());
            String alias = tableRef.hasExplicitAlias() ? tableRef.getExplicitAlias() : null;
            tableRef = new TableRef(normalizedName, tableRef.getPartitionRef(), alias, tableRef.getPos());
        }
        insertStmt.setTableRef(tableRef);
        String catalogName = tableRef.getCatalogName();
        String dbName = tableRef.getDbName();
        String tableName = tableRef.getTableName();

        MetaUtils.checkCatalogExistAndReport(catalogName);

        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session, catalogName, dbName);
        if (database == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        TableName tableNameObj = new TableName(catalogName, dbName, tableName, tableRef.getPos());
        Table table = MetaUtils.getSessionAwareTable(session, database, tableNameObj);
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName);
        }

        if (table instanceof MaterializedView && !insertStmt.isSystem()) {
            throw new SemanticException(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    tableName, tableName);
        }

        if (insertStmt.isOverwrite()) {
            if (!(table instanceof OlapTable) && !table.isIcebergTable() && !table.isHiveTable()) {
                throw unsupportedException("Only support insert overwrite olap/iceberg/hive table");
            }
            if (table instanceof OlapTable && ((OlapTable) table).getState() != NORMAL) {
                String msg =
                        String.format("table state is %s, please wait to insert overwrite until table state is normal",
                                ((OlapTable) table).getState());
                throw unsupportedException(msg);
            }
        }

        if (!table.supportInsert()) {
            if (table.isIcebergTable() || table.isHiveTable()) {
                throw unsupportedException(String.format("Only support insert into %s table with parquet file format",
                        table.getType()));
            }
            throw unsupportedException("Only support insert into olap/mysql/iceberg/hive table");
        }

        if ((table.isHiveTable() || table.isIcebergTable()) && CatalogMgr.isInternalCatalog(catalogName)) {
            throw unsupportedException(String.format("Doesn't support %s table sink in the internal catalog. " +
                    "You need to use %s catalog.", table.getType(), table.getType()));
        }

        if (insertStmt.getTargetBranch() != null) {
            if (!table.isIcebergTable()) {
                throw unsupportedException("Only support insert iceberg table with branch");
            }
            String targetBranch = insertStmt.getTargetBranch();
            SnapshotRef snapshotRef = ((IcebergTable) table).getNativeTable().refs().get(targetBranch);
            if (snapshotRef == null) {
                throw unsupportedException("Cannot find snapshot with reference name: " + targetBranch);
            }

            if (!snapshotRef.isBranch()) {
                throw unsupportedException(String.format("%s is a tag, not a branch", targetBranch));
            }
        }

        return table;
    }

    public static boolean isUnSupportedPartitionColumnType(Type type) {
        return type.isFloat() || type.isDecimalOfAnyVersion() || type.isDatetime();
    }
}
