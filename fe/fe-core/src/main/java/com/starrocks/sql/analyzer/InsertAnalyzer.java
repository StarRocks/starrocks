// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.relation.InsertRelation;
import com.starrocks.sql.analyzer.relation.QueryRelation;
import com.starrocks.sql.analyzer.relation.Relation;
import com.starrocks.sql.analyzer.relation.ValuesRelation;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class InsertAnalyzer {
    private final Catalog catalog;
    private final ConnectContext session;

    public InsertAnalyzer(Catalog catalog, ConnectContext session) {
        this.catalog = catalog;
        this.session = session;
    }

    public Relation transformInsertStmt(InsertStmt insertStmt) {
        QueryRelation query = new QueryAnalyzer(catalog, session)
                .transformQueryStmt(insertStmt.getQueryStmt(), new Scope(RelationId.anonymous(), new RelationFields()));

        /*
         *  Target table
         */
        MetaUtils.normalizationTableName(session, insertStmt.getTableName());
        Database database = MetaUtils.getStarRocks(session, insertStmt.getTableName());
        Table table = MetaUtils.getStarRocksTable(session, insertStmt.getTableName());

        if (table instanceof OlapTable) {
            OlapTable targetTable = (OlapTable) table;
            PartitionNames targetPartitionNames = insertStmt.getTargetPartitionNames();
            List<Long> targetPartitionIds = Lists.newArrayList();

            if (targetPartitionNames != null) {
                if (targetTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                    throw new SemanticException("PARTITION clause is not valid for INSERT into unpartitioned table");
                }

                if (targetPartitionNames.getPartitionNames().isEmpty()) {
                    throw new SemanticException("No partition specified in partition lists");
                }

                for (String partitionName : targetPartitionNames.getPartitionNames()) {
                    if (Strings.isNullOrEmpty(partitionName)) {
                        throw new SemanticException("there are empty partition name");
                    }

                    Partition partition = targetTable.getPartition(partitionName, targetPartitionNames.isTemp());
                    if (partition == null) {
                        throw new SemanticException("Unknown partition '%s' in table '%s'", partitionName,
                                targetTable.getName());
                    }
                    targetPartitionIds.add(partition.getId());
                }
            } else {
                for (Partition partition : targetTable.getPartitions()) {
                    targetPartitionIds.add(partition.getId());
                }
                if (targetPartitionIds.isEmpty()) {
                    throw new SemanticException("data cannot be inserted into table with empty partition." +
                            "Use `SHOW PARTITIONS FROM %s` to see the currently partitions of this table. ",
                            targetTable.getName());
                }
            }

            // Build target columns
            List<Column> targetColumns;
            Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            if (insertStmt.getTargetColumnNames() == null) {
                targetColumns = new ArrayList<>(targetTable.getBaseSchema());
                mentionedColumns =
                        targetTable.getBaseSchema().stream().map(Column::getName).collect(Collectors.toSet());
            } else {
                targetColumns = new ArrayList<>();
                for (String colName : insertStmt.getTargetColumnNames()) {
                    Column column = targetTable.getColumn(colName);
                    if (column == null) {
                        throw new SemanticException("Unknown column '%s' in '%s'", colName, targetTable.getName());
                    }
                    if (!mentionedColumns.add(colName)) {
                        throw new SemanticException("Column '%s' specified twice", colName);
                    }
                    targetColumns.add(column);
                }

                // object column must in mentionedColumns
                for (Column col : targetTable.getBaseSchema()) {
                    if (col.getType().isOnlyMetricType() && !mentionedColumns.contains(col.getName())) {
                        throw new SemanticException(
                                col.getType() + " type column " + col.getName() + " must in insert into columns");
                    }
                }
            }

            for (Column column : targetTable.getBaseSchema()) {
                if (!column.existBatchConstDefaultValue() && !column.isAllowNull() &&
                        !mentionedColumns.contains(column.getName())) {
                    throw new SemanticException("'%s' must be explicitly mentioned in column permutation",
                            column.getName());
                }
            }

            if (query.getOutputExpr().size() != mentionedColumns.size()) {
                throw new SemanticException("Column count doesn't match value count");
            }
            // check default value expr
            if (query instanceof ValuesRelation) {
                ValuesRelation valuesRelation = (ValuesRelation) query;
                for (List<Expr> row : valuesRelation.getRows()) {
                    for (int columnIdx = 0; columnIdx < row.size(); ++columnIdx) {
                        if (row.get(columnIdx) instanceof DefaultValueExpr &&
                                !targetColumns.get(columnIdx).existBatchConstDefaultValue()) {
                            throw new SemanticException(
                                    "Column has no default value, column=" + targetColumns.get(columnIdx).getName());
                        }
                    }
                }
            }

            return new InsertRelation(query, database, targetTable, targetPartitionIds, targetColumns,
                    insertStmt.getTargetColumnNames());
        } else {
            throw unsupportedException("New planner only support insert into olap table");
        }
    }
}
