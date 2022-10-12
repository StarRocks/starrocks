// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;
import java.util.Set;

public class LogicalHiveScanOperator extends LogicalScanOperator {
    private final Table.TableType tableType;
    private ScanOperatorPredicates predicates = new ScanOperatorPredicates();

    public LogicalHiveScanOperator(Table table,
                                   Table.TableType tableType,
                                   Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                   Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                   long limit,
                                   ScalarOperator predicate) {
        super(OperatorType.LOGICAL_HIVE_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate, null);

        Preconditions.checkState(table instanceof HiveTable);
        this.tableType = tableType;
        HiveTable hiveTable = (HiveTable) table;
        partitionColumns.addAll(hiveTable.getPartitionColumnNames());
    }

    private LogicalHiveScanOperator(LogicalHiveScanOperator.Builder builder) {
        super(OperatorType.LOGICAL_HIVE_SCAN,
                builder.table,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());

        this.tableType = builder.tableType;
        this.predicates = builder.predicates;
        this.partitionColumns = builder.partitionColumns;
    }

    public Table.TableType getTableType() {
        return tableType;
    }

    @Override
    public ScanOperatorPredicates getScanOperatorPredicates() {
        return this.predicates;
    }

    @Override
    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) {
        this.predicates = predicates;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalHiveScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalHiveScanOperator, LogicalHiveScanOperator.Builder> {
        private Table.TableType tableType;
        private ScanOperatorPredicates predicates = new ScanOperatorPredicates();
        private Set<String> partitionColumns = Sets.newHashSet();

        @Override
        public LogicalHiveScanOperator build() {
            return new LogicalHiveScanOperator(this);
        }

        @Override
        public LogicalHiveScanOperator.Builder withOperator(LogicalHiveScanOperator scanOperator) {
            super.withOperator(scanOperator);

            this.tableType = scanOperator.tableType;
            this.predicates = scanOperator.predicates;
            this.partitionColumns = scanOperator.partitionColumns;
            return this;
        }
    }
}
