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
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.LoadPriority;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.external.starrocks.TableMetaSyncer;
import com.starrocks.load.Load;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TPartialUpdateMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.catalog.OlapTable.OlapTableState.NORMAL;
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class InsertAnalyzer {

    public static final String TIMEOUT_PROPERTY = "timeout";
    public static final String MAX_FILTER_RATIO_PROPERTY = "max_filter_ratio";
    public static final String LOAD_DELETE_FLAG_PROPERTY = "load_delete_flag";
    public static final String LOAD_MEM_LIMIT = "load_mem_limit";
    private static final String VERSION = "version";
    public static final String STRICT_MODE = "strict_mode";
    public static final String TIMEZONE = "timezone";
    public static final String PRIORITY = "priority";
    public static final String MERGE_CONDITION = "merge_condition";
    public static final String CASE_SENSITIVE = "case_sensitive";
    public static final String LOG_REJECTED_RECORD_NUM = "log_rejected_record_num";
    public static final String SPARK_LOAD_SUBMIT_TIMEOUT = "spark_load_submit_timeout";
    public static final String PARTIAL_UPDATE = "partial_update";
    public static final String PARTIAL_UPDATE_MODE = "partial_update_mode";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(TIMEOUT_PROPERTY)
            .add(MAX_FILTER_RATIO_PROPERTY)
            .add(LOAD_DELETE_FLAG_PROPERTY)
            .add(LOAD_MEM_LIMIT)
            .add(STRICT_MODE)
            .add(VERSION)
            .add(TIMEZONE)
            .add(PARTIAL_UPDATE)
            .add(PRIORITY)
            .add(CASE_SENSITIVE)
            .add(LOG_REJECTED_RECORD_NUM)
            .add(PARTIAL_UPDATE_MODE)
            .add(SPARK_LOAD_SUBMIT_TIMEOUT)
            .build();

    public static void analyze(InsertStmt insertStmt, ConnectContext session) {
        QueryRelation query = insertStmt.getQueryStatement().getQueryRelation();
        new QueryAnalyzer(session).analyze(insertStmt.getQueryStatement());

        List<Table> tables = new ArrayList<>();
        AnalyzerUtils.collectSpecifyExternalTables(insertStmt.getQueryStatement(), tables, Table::isHiveTable);
        tables.stream().map(table -> (HiveTable) table)
                .forEach(table -> table.useMetadataCache(false));

        /*
         *  Target table
         */
        MetaUtils.normalizationTableName(session, insertStmt.getTableName());
        String catalogName = insertStmt.getTableName().getCatalog();
        String dbName = insertStmt.getTableName().getDb();
        String tableName = insertStmt.getTableName().getTbl();

        try {
            MetaUtils.checkCatalogExistAndReport(catalogName);
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
        }

        // check properties
        try {
            checkProperties(insertStmt, insertStmt.getProperties());
        } catch (DdlException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_PROPERTY, e.getMessage());
        }

        Database database = MetaUtils.getDatabase(catalogName, dbName);
        Table table = MetaUtils.getTable(catalogName, dbName, tableName);

        if (table instanceof ExternalOlapTable) {
            table = getOLAPExternalTableMeta(database, (ExternalOlapTable) table);
        }

        if (table instanceof MaterializedView && !insertStmt.isSystem()) {
            throw new SemanticException(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    insertStmt.getTableName().getTbl(), insertStmt.getTableName().getTbl());
        }

        if (insertStmt.isOverwrite()) {
            if (!(table instanceof OlapTable) && !table.isIcebergTable()) {
                throw unsupportedException("Only support insert overwrite olap table and iceberg table");
            }
            if (table instanceof OlapTable && ((OlapTable) table).getState() != NORMAL) {
                String msg =
                        String.format("table state is %s, please wait to insert overwrite util table state is normal",
                                ((OlapTable) table).getState());
                throw unsupportedException(msg);
            }
        }

        if (!table.supportInsert()) {
            if (table.isIcebergTable()) {
                throw unsupportedException("Only support insert into iceberg table with parquet file format");
            }
            throw unsupportedException("Only support insert into olap table or mysql table or iceberg table");
        }

        if (table instanceof IcebergTable && CatalogMgr.isInternalCatalog(catalogName)) {
            throw unsupportedException("Doesn't support iceberg table sink in the internal catalog. " +
                    "You need to use iceberg catalog.");
        }

        List<Long> targetPartitionIds = Lists.newArrayList();
        PartitionNames targetPartitionNames = insertStmt.getTargetPartitionNames();
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            targetPartitionNames = insertStmt.getTargetPartitionNames();

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
        }

        if (table instanceof IcebergTable) {
            IcebergTable icebergTable = (IcebergTable) table;
            List<String> tablePartitionColumnNames = icebergTable.getPartitionColumnNames();
            if (insertStmt.getTargetColumnNames() != null) {
                for (String partitionColName : tablePartitionColumnNames) {
                    if (!insertStmt.getTargetColumnNames().contains(partitionColName)) {
                        throw new SemanticException("Must include partition column %s", partitionColName);
                    }
                }
            } else if (insertStmt.isStaticKeyPartitionInsert()) {
                checkStaticKeyPartitionInsert(insertStmt, icebergTable, targetPartitionNames);
            }

            for (Column column : icebergTable.getPartitionColumns()) {
                if (IcebergTableSink.isUnSupportedPartitionColumnType(column.getType())) {
                    throw new SemanticException("Unsupported partition column type [%s] for iceberg table sink",
                            column.getType().canonicalName());
                }
            }
        }

        // Build target columns
        List<Column> targetColumns;
        Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (insertStmt.getTargetColumnNames() == null) {
            if (table instanceof OlapTable) {
                targetColumns = new ArrayList<>(((OlapTable) table).getBaseSchemaWithoutMaterializedColumn());
                mentionedColumns =
                        ((OlapTable) table).getBaseSchemaWithoutMaterializedColumn().stream()
                            .map(Column::getName).collect(Collectors.toSet());
            } else {
                targetColumns = new ArrayList<>(table.getBaseSchema());
                mentionedColumns =
                        table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toSet());
            }
        } else {
            targetColumns = new ArrayList<>();
            for (String colName : insertStmt.getTargetColumnNames()) {
                Column column = table.getColumn(colName);
                if (column == null) {
                    throw new SemanticException("Unknown column '%s' in '%s'", colName, table.getName());
                }
                if (column.isMaterializedColumn()) {
                    throw new SemanticException("materialized column '%s' can not be specified", colName);
                }
                if (!mentionedColumns.add(colName)) {
                    throw new SemanticException("Column '%s' specified twice", colName);
                }
                targetColumns.add(column);
            }
        }

        for (Column column : table.getBaseSchema()) {
            Column.DefaultValueType defaultValueType = column.getDefaultValueType();
            if (defaultValueType == Column.DefaultValueType.NULL && !column.isAllowNull() &&
                    !column.isAutoIncrement() && !column.isMaterializedColumn() &&
                    !mentionedColumns.contains(column.getName())) {
                String msg = "";
                for (String s : mentionedColumns) {
                    msg = msg + " " + s + " ";
                }
                throw new SemanticException("'%s' must be explicitly mentioned in column permutation: %s",
                        column.getName(), msg);
            }
        }

        int mentionedColumnSize = mentionedColumns.size();
        if (table instanceof IcebergTable && insertStmt.isStaticKeyPartitionInsert()) {
            // full column size = mentioned column size + partition column size for static partition insert
            mentionedColumnSize -= table.getPartitionColumnNames().size();
        }

        if (query.getRelationFields().size() != mentionedColumnSize) {
            throw new SemanticException("Column count doesn't match value count");
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
        insertStmt.setTargetPartitionIds(targetPartitionIds);
        insertStmt.setTargetColumns(targetColumns);
        if (session.getDumpInfo() != null) {
            session.getDumpInfo().addTable(database.getFullName(), table);
        }
    }

    private static void checkProperties(InsertStmt insertStmt, Map<String, String> properties) throws DdlException {
        if (properties == null) {
            return;
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!PROPERTIES_SET.contains(entry.getKey())) {
                throw new DdlException(entry.getKey() + " is invalid property");
            }
        }

        // partial update
        String partialUpdate = properties.get(PARTIAL_UPDATE);
        if (partialUpdate != null) {
            boolean value = VariableMgr.parseBooleanVariable(partialUpdate);
            insertStmt.setPartialUpdate(value);
        }

        // partial update mode
        String partialUpdateMode = properties.get(PARTIAL_UPDATE_MODE);
        if (partialUpdateMode != null) {
            TPartialUpdateMode mode = TPartialUpdateMode.UNKNOWN_MODE;
            if (partialUpdateMode.equals("column")) {
                mode = TPartialUpdateMode.COLUMN_MODE;
            } else if (partialUpdateMode.equals("auto")) {
                mode = TPartialUpdateMode.AUTO_MODE;
            } else if (partialUpdateMode.equals("row")) {
                mode = TPartialUpdateMode.ROW_MODE;
            }
            insertStmt.setPartialUpdateMode(mode);
        }

        final String loadMemProperty = properties.get(LOAD_MEM_LIMIT);
        if (loadMemProperty != null) {
            try {
                final long loadMem = Long.parseLong(loadMemProperty);
                if (loadMem < 0) {
                    throw new DdlException(LOAD_MEM_LIMIT + " must be equal or greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(LOAD_MEM_LIMIT + " is not a number.");
            }
        }

        // timeout
        final String timeoutLimitProperty = properties.get(TIMEOUT_PROPERTY);
        if (timeoutLimitProperty != null) {
            try {
                final int timeoutLimit = Integer.parseInt(timeoutLimitProperty);
                if (timeoutLimit < 0) {
                    throw new DdlException(TIMEOUT_PROPERTY + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(TIMEOUT_PROPERTY + " is not a number.");
            }
        }

        // spark load wait yarn timeout
        final String sparkLoadSubmitTimeoutProperty = properties.get(SPARK_LOAD_SUBMIT_TIMEOUT);
        if (sparkLoadSubmitTimeoutProperty != null) {
            try {
                final long sparkLoadSubmitTimeout = Long.parseLong(sparkLoadSubmitTimeoutProperty);
                if (sparkLoadSubmitTimeout < 0) {
                    throw new DdlException(SPARK_LOAD_SUBMIT_TIMEOUT + " must be greater than 0");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(SPARK_LOAD_SUBMIT_TIMEOUT + " is not a number.");
            }
        }

        // max filter ratio
        final String maxFilterRadioProperty = properties.get(MAX_FILTER_RATIO_PROPERTY);
        if (maxFilterRadioProperty != null) {
            try {
                double maxFilterRatio = Double.valueOf(maxFilterRadioProperty);
                if (maxFilterRatio < 0.0 || maxFilterRatio > 1.0) {
                    throw new DdlException(MAX_FILTER_RATIO_PROPERTY + " must between 0.0 and 1.0.");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(MAX_FILTER_RATIO_PROPERTY + " is not a number.");
            }
        }

        // version
        final String versionProperty = properties.get(VERSION);
        if (versionProperty != null) {
            if (!versionProperty.equalsIgnoreCase(Load.VERSION)) {
                throw new DdlException(VERSION + " must be " + Load.VERSION);
            }
        }

        // strict mode
        final String strictModeProperty = properties.get(STRICT_MODE);
        if (strictModeProperty != null) {
            if (!strictModeProperty.equalsIgnoreCase("true")
                    && !strictModeProperty.equalsIgnoreCase("false")) {
                throw new DdlException(STRICT_MODE + " is not a boolean");
            }
        }

        // time zone
        final String timezone = properties.get(TIMEZONE);
        if (timezone != null) {
            properties.put(TIMEZONE, TimeUtils.checkTimeZoneValidAndStandardize(
                    properties.getOrDefault(LoadStmt.TIMEZONE, TimeUtils.DEFAULT_TIME_ZONE)));
        }

        // load priority
        final String priorityProperty = properties.get(PRIORITY);
        if (priorityProperty != null) {
            if (LoadPriority.priorityByName(priorityProperty) == null) {
                throw new DdlException(PRIORITY + " should in HIGHEST/HIGH/NORMAL/LOW/LOWEST");
            }
        }

        // log rejected record num
        final String logRejectedRecordNumProperty = properties.get(LOG_REJECTED_RECORD_NUM);
        if (logRejectedRecordNumProperty != null) {
            try {
                final long logRejectedRecordNum = Long.parseLong(logRejectedRecordNumProperty);
                if (logRejectedRecordNum < -1) {
                    throw new DdlException(LOG_REJECTED_RECORD_NUM + " must be equal or greater than -1");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(LOG_REJECTED_RECORD_NUM + " is not a number.");
            }
        }
    }

    private static void checkStaticKeyPartitionInsert(InsertStmt insertStmt, Table table,
                                                      PartitionNames targetPartitionNames) {
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

            if (partitionValue instanceof NullLiteral) {
                throw new SemanticException("partition value can't be null");
            }

            LiteralExpr literalExpr = (LiteralExpr) partitionValue;
            Column column = table.getColumn(actualName);
            try {
                Expr expr = LiteralExpr.create(literalExpr.getStringValue(), column.getType());
                insertStmt.getTargetPartitionNames().getPartitionColValues().set(i, expr);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }
    }
  
    private static ExternalOlapTable getOLAPExternalTableMeta(Database database, ExternalOlapTable externalOlapTable) {
        // copy the table, and release database lock when synchronize table meta
        ExternalOlapTable copiedTable = new ExternalOlapTable();
        externalOlapTable.copyOnlyForQuery(copiedTable);
        int lockTimes = 0;
        while (database.isReadLockHeldByCurrentThread()) {
            database.readUnlock();
            lockTimes++;
        }
        try {
            new TableMetaSyncer().syncTable(copiedTable);
        } finally {
            while (lockTimes-- > 0) {
                database.readLock();
            }
        }
        return copiedTable;
    }
}
