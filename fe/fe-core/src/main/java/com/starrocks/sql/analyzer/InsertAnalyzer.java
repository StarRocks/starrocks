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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.OlapTable.OlapTableState.NORMAL;
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class InsertAnalyzer {
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

        Database database = MetaUtils.getDatabase(catalogName, dbName);
        Table table = MetaUtils.getTable(catalogName, dbName, tableName);

        if (table instanceof MaterializedView && !insertStmt.isSystem()) {
            throw new SemanticException(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    insertStmt.getTableName().getTbl(), insertStmt.getTableName().getTbl());
        }

        if (insertStmt.isOverwrite()) {
            if (!(table instanceof OlapTable)) {
                throw unsupportedException("Only support insert overwrite olap table");
            }
            if (((OlapTable) table).getState() != NORMAL) {
                String msg =
                        String.format("table state is %s, please wait to insert overwrite util table state is normal",
                                ((OlapTable) table).getState());
                throw unsupportedException(msg);
            }
        } else if (!table.supportInsert()) {
            throw unsupportedException("Only support insert into olap table or mysql table or iceberg table");
        }

        List<Long> targetPartitionIds = Lists.newArrayList();
        PartitionNames targetPartitionNames = insertStmt.getTargetPartitionNames();
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            targetPartitionNames = insertStmt.getTargetPartitionNames();

            if (targetPartitionNames != null) {
                if (targetPartitionNames.getPartitionNames().isEmpty()) {
                    throw new SemanticException("No partition specified in partition lists",
                            targetPartitionNames.getPos());
                }

                for (String partitionName : targetPartitionNames.getPartitionNames()) {
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

        if (table instanceof IcebergTable && insertStmt.isStaticKeyPartitionInsert()) {
            IcebergTable icebergTable = (IcebergTable) table;
            List<String> partitionColNames = targetPartitionNames.getPartitionColNames();
            List<Expr> partitionColValues = targetPartitionNames.getPartitionColValues();
            List<String> tablePartitionColumnNames = icebergTable.getPartitionColumnNames();

            Preconditions.checkState(partitionColNames.size() == partitionColValues.size(),
                    "Partition column names size must be equal to the partition column values size. %d vs %d",
                    partitionColNames.size(), partitionColValues.size());

            if (tablePartitionColumnNames.size() != partitionColNames.size()) {
                throw new SemanticException("Must include all partition column names");
            }

            for (int i = 0; i < partitionColNames.size(); i++) {
                String actualName = partitionColNames.get(i);
                String expectedName = tablePartitionColumnNames.get(i);
                Expr partitionValue = partitionColValues.get(i);
                if (!actualName.equalsIgnoreCase(expectedName)) {
                    throw new SemanticException("Expected: %s, but actual: %s", expectedName, actualName);
                }

                if (!partitionValue.isLiteral()) {
                    throw new SemanticException("partition value should be literal expression");
                }

                LiteralExpr literalExpr = (LiteralExpr) partitionValue;
                Column column = icebergTable.getColumn(actualName);
                try {
                    literalExpr.castTo(column.getType());
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
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
        session.getDumpInfo().addTable(database.getFullName(), table);
    }
}
