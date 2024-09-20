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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.connector.hive.HiveWriteUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.MetaUtils;
import org.apache.iceberg.SnapshotRef;

import java.util.ArrayList;
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
        boolean isLockTaken = false;
        try {
            // insert properties
            analyzeProperties(insertStmt, session);

            // push down schema to files
            // should lock because this needs target table schema, only affacts insert from files()
            if (pushDownTargetTableSchemaToFiles(insertStmt, session)) {
                // Take the PlannerMetaLock
                takeLock.run();
                isLockTaken = true;
            }

            new QueryAnalyzer(session).analyze(insertStmt.getQueryStatement());

            List<Table> tables = new ArrayList<>();
            AnalyzerUtils.collectSpecifyExternalTables(insertStmt.getQueryStatement(), tables, Table::isHiveTable);
            tables.stream().map(table -> (HiveTable) table)
                    .forEach(table -> table.useMetadataCache(false));
        } finally {
            if (!isLockTaken) {
                // Take the PlannerMetaLock
                takeLock.run();
            }
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
            PartitionNames targetPartitionNames = insertStmt.getTargetPartitionNames();

            if (insertStmt.isSpecifyPartitionNames()) {
                if (targetPartitionNames.getPartitionNames().isEmpty()) {
                    throw new SemanticException("No partition specified in partition lists",
                            targetPartitionNames.getPos());
                }

                List<String> deduplicatePartitionNames =
                        targetPartitionNames.getPartitionNames().stream().distinct().collect(Collectors.toList());
                if (deduplicatePartitionNames.size() != targetPartitionNames.getPartitionNames().size()) {
                    insertStmt.setTargetPartitionNames(new PartitionNames(targetPartitionNames.isTemp(),
                            deduplicatePartitionNames, targetPartitionNames.getPartitionColNames(),
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
                for (Partition partition : olapTable.getPartitions()) {
                    targetPartitionIds.add(partition.getId());
                }
                if (targetPartitionIds.isEmpty()) {
                    throw new SemanticException("data cannot be inserted into table with empty partition." +
                            "Use `SHOW PARTITIONS FROM %s` to see the currently partitions of this table. ",
                            olapTable.getName());
                }
            }
            insertStmt.setTargetPartitionIds(targetPartitionIds);
        }

        if (table.isIcebergTable() || table.isHiveTable()) {
            if (table.isHiveTable() && table.isUnPartitioned() &&
                    HiveWriteUtils.isS3Url(table.getTableLocation()) && insertStmt.isOverwrite()) {
                throw new SemanticException("Unsupported insert overwrite hive unpartitioned table with s3 location");
            }

            if (table.isHiveTable() && ((HiveTable) table).getHiveTableType() != HiveTable.HiveTableType.MANAGED_TABLE &&
                    !session.getSessionVariable().enableWriteHiveExternalTable()) {
                throw new SemanticException("Only support to write hive managed table, tableType: " +
                        ((HiveTable) table).getHiveTableType());
            }

            PartitionNames targetPartitionNames = insertStmt.getTargetPartitionNames();
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

        // Build target columns
        List<Column> targetColumns;
        Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (insertStmt.getTargetColumnNames() == null) {
            if (table instanceof OlapTable) {
                targetColumns = new ArrayList<>(((OlapTable) table).getBaseSchemaWithoutGeneratedColumn());
                mentionedColumns =
                        ((OlapTable) table).getBaseSchemaWithoutGeneratedColumn().stream()
                                .map(Column::getName).collect(Collectors.toSet());
            } else {
                targetColumns = new ArrayList<>(table.getBaseSchema());
                mentionedColumns =
                        table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toSet());
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
                    throw new SemanticException("Column '%s' specified twice", colName);
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
                    if (targetColumns.size() < olapTable.getBaseSchemaWithoutGeneratedColumn().size()) {
                        insertStmt.setUsePartialUpdate();
                    }
                }
            }
        }

        if (!insertStmt.usePartialUpdate()) {
            for (Column column : table.getBaseSchema()) {
                Column.DefaultValueType defaultValueType = column.getDefaultValueType();
                if (defaultValueType == Column.DefaultValueType.NULL && !column.isAllowNull() &&
                        !column.isAutoIncrement() && !column.isGeneratedColumn() &&
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
        }

        QueryRelation query = insertStmt.getQueryStatement().getQueryRelation();
        if (query.getRelationFields().size() != mentionedColumnSize) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_INSERTED_COLUMN_MISMATCH, mentionedColumnSize,
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
            session.getDumpInfo().addTable(insertStmt.getTableName().getDb(), table);
        }
    }

    private static void analyzeProperties(InsertStmt insertStmt, ConnectContext session) {
        Map<String, String> properties = insertStmt.getProperties();
        // use session variable if not set max_filter_ratio property
        if (!properties.containsKey(LoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
            properties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY,
                    String.valueOf(session.getSessionVariable().getInsertMaxFilterRatio()));
        }
        // use session variable if not set strict_mode property
        if (!properties.containsKey(LoadStmt.STRICT_MODE) &&
                session.getSessionVariable().getEnableInsertStrict()) {
            properties.put(LoadStmt.STRICT_MODE, "true");
        }

        try {
            LoadStmt.checkProperties(properties);
        } catch (DdlException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
        }

        // push down some properties to file table function
        QueryStatement queryStatement = insertStmt.getQueryStatement();
        if (queryStatement != null) {
            List<FileTableFunctionRelation> relations = AnalyzerUtils.collectFileTableFunctionRelation(queryStatement);
            for (FileTableFunctionRelation relation : relations) {
                Map<String, String> tableFunctionProperties = relation.getProperties();
                for (String property : PUSH_DOWN_PROPERTIES_SET) {
                    if (properties.containsKey(property)) {
                        tableFunctionProperties.put(property, properties.get(property));
                    }
                }
            }
        }
    }

    /**
     * files() schema infer is not strict.
     * for example, integer in csv will be inferred to bigint type.
     * when the target table column is tinyint, the data may be filtered because it is bigger than tinyint.
     * but strict mode will not take effect in file scan if using bigint type.
     *
     * only push down slot ref select column to files.
     *
     * @return true if can push down schema, else false.
     */
    private static boolean pushDownTargetTableSchemaToFiles(InsertStmt insertStmt, ConnectContext session) {
        if (!Config.files_enable_insert_push_down_schema) {
            return false;
        }

        if (insertStmt.useTableFunctionAsTargetTable() || insertStmt.useBlackHoleTableAsTargetTable()) {
            return false;
        }

        // check insert native table from files()
        Table targetTable = getTargetTable(insertStmt, session);
        if (!targetTable.isNativeTable()) {
            return false;
        }

        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        if (!(queryRelation instanceof SelectRelation)) {
            return false;
        }
        SelectRelation selectRelation = (SelectRelation) queryRelation;
        Relation fromRelation = selectRelation.getRelation();
        if (!(fromRelation instanceof FileTableFunctionRelation)) {
            return false;
        }

        Consumer<TableFunctionTable> pushDownSchemaFunc = (fileTable) -> {
            // get target column names
            List<String> targetColumnNames = insertStmt.getTargetColumnNames();
            if (targetColumnNames == null) {
                targetColumnNames = ((OlapTable) targetTable).getBaseSchemaWithoutGeneratedColumn().stream()
                        .map(Column::getName).collect(Collectors.toList());
            }

            // get select column names, null if it is not slot ref column
            List<String> selectColumnNames = Lists.newArrayList();
            List<SelectListItem> listItems = selectRelation.getSelectList().getItems();
            for (SelectListItem item : listItems) {
                if (item.isStar()) {
                    selectColumnNames.addAll(fileTable.getFullSchema().stream().map(Column::getName)
                            .collect(Collectors.toList()));
                    continue;
                }

                Expr expr = item.getExpr();
                if (expr instanceof SlotRef) {
                    selectColumnNames.add(((SlotRef) expr).getColumnName());
                    continue;
                }

                selectColumnNames.add(null);
            }

            if (targetColumnNames.size() != selectColumnNames.size()) {
                return;
            }

            // update files table schema according to target table schema
            Map<String, Column> targetTableColumns = targetTable.getNameToColumn();
            Map<String, Column> newFileTableColumns = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            newFileTableColumns.putAll(fileTable.getNameToColumn());
            for (int i = 0; i < selectColumnNames.size(); ++i) {
                String selectColumnName = selectColumnNames.get(i);
                if (selectColumnName == null) {
                    continue;
                }

                String columnName = targetColumnNames.get(i);
                if (!targetTableColumns.containsKey(columnName)) {
                    continue;
                }

                Column oldCol = newFileTableColumns.get(selectColumnName);
                Column newCol = targetTableColumns.get(columnName).deepCopy();
                // bad case: complex types may fail to convert in scanner.
                // such as parquet json -> array<varchar>
                if (oldCol.getType().isComplexType() || newCol.getType().isComplexType()) {
                    continue;
                }

                newCol.setName(selectColumnName);
                newFileTableColumns.put(selectColumnName, newCol);
            }

            List<Column> newFileTableSchema = fileTable.getFullSchema().stream()
                    .map(col -> newFileTableColumns.get(col.getName())).collect(Collectors.toList());
            fileTable.setNewFullSchema(newFileTableSchema);
        };

        ((FileTableFunctionRelation) fromRelation).setPushDownSchemaFunc(pushDownSchemaFunc);
        return true;
    }

    private static void checkStaticKeyPartitionInsert(InsertStmt insertStmt, Table table, PartitionNames targetPartitionNames) {
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

            if (!partitionValue.isLiteral()) {
                throw new SemanticException("partition value should be literal expression");
            }

            LiteralExpr literalExpr = (LiteralExpr) partitionValue;
            Column column = table.getColumn(actualName);
            try {
                Type type = literalExpr.isConstantNull() ? Type.NULL : column.getType();
                Expr expr = LiteralExpr.create(literalExpr.getStringValue(), type);
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

        insertStmt.getTableName().normalization(session);
        String catalogName = insertStmt.getTableName().getCatalog();
        String dbName = insertStmt.getTableName().getDb();
        String tableName = insertStmt.getTableName().getTbl();

        MetaUtils.checkCatalogExistAndReport(catalogName);

        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
        if (database == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        Table table = MetaUtils.getSessionAwareTable(session, database, insertStmt.getTableName());
        if (table == null) {
            throw new SemanticException("Table %s is not found", tableName);
        }

        if (table instanceof MaterializedView && !insertStmt.isSystem()) {
            throw new SemanticException(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    insertStmt.getTableName().getTbl(), insertStmt.getTableName().getTbl());
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
