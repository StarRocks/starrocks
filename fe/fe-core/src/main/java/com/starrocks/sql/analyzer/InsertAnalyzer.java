// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class InsertAnalyzer {
    public static void analyze(InsertStmt insertStmt, ConnectContext session) {
        QueryRelation query = insertStmt.getQueryStatement().getQueryRelation();
        new QueryAnalyzer(session).analyze(insertStmt.getQueryStatement());

        /*
         *  Target table
         */
        MetaUtils.normalizationTableName(session, insertStmt.getTableName());
        Database database = MetaUtils.getStarRocks(session, insertStmt.getTableName());
        Table table = MetaUtils.getStarRocksTable(session, insertStmt.getTableName());

        if (!(table instanceof OlapTable) && !(table instanceof MysqlTable)) {
            throw unsupportedException("Only support insert into olap table or mysql table");
        }

        List<Long> targetPartitionIds = Lists.newArrayList();
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            PartitionNames targetPartitionNames = insertStmt.getTargetPartitionNames();

            if (targetPartitionNames != null) {
                if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                    throw new SemanticException("PARTITION clause is not valid for INSERT into unpartitioned table");
                }

                if (targetPartitionNames.getPartitionNames().isEmpty()) {
                    throw new SemanticException("No partition specified in partition lists");
                }

                for (String partitionName : targetPartitionNames.getPartitionNames()) {
                    if (Strings.isNullOrEmpty(partitionName)) {
                        throw new SemanticException("there are empty partition name");
                    }

                    Partition partition = olapTable.getPartition(partitionName, targetPartitionNames.isTemp());
                    if (partition == null) {
                        throw new SemanticException("Unknown partition '%s' in table '%s'", partitionName,
                                olapTable.getName());
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

        // Build target columns
        List<Column> targetColumns;
        Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (insertStmt.getTargetColumnNames() == null) {
            targetColumns = new ArrayList<>(table.getBaseSchema());
            mentionedColumns =
                    table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toSet());
        } else {
            targetColumns = new ArrayList<>();
            for (String colName : insertStmt.getTargetColumnNames()) {
                Column column = table.getColumn(colName);
                if (column == null) {
                    throw new SemanticException("Unknown column '%s' in '%s'", colName, table.getName());
                }
                if (!mentionedColumns.add(colName)) {
                    throw new SemanticException("Column '%s' specified twice", colName);
                }
                targetColumns.add(column);
            }

            // object column must in mentionedColumns
            for (Column col : table.getBaseSchema()) {
                if (col.getType().isOnlyMetricType() && !mentionedColumns.contains(col.getName())) {
                    throw new SemanticException(
                            col.getType() + " type column " + col.getName() + " must in insert into columns");
                }
            }
        }

        for (Column column : table.getBaseSchema()) {
            Column.DefaultValueType defaultValueType = column.getDefaultValueType();
            if (defaultValueType == Column.DefaultValueType.NULL && !column.isAllowNull() &&
                    !mentionedColumns.contains(column.getName())) {
                throw new SemanticException("'%s' must be explicitly mentioned in column permutation",
                        column.getName());
            }
        }

        if (query.getRelationFields().size() != mentionedColumns.size()) {
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
                            defaultValueType == Column.DefaultValueType.NULL) {
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
        session.getDumpInfo().addTable(database.getFullName().split(":")[1], table);
    }
}
