// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalIcebergScanOperator extends LogicalScanOperator {
    private ScanOperatorPredicates predicates = new ScanOperatorPredicates();

    public LogicalIcebergScanOperator(Table table,
                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                      Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                      long limit,
                                      ScalarOperator predicate) {
        super(OperatorType.LOGICAL_ICEBERG_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate, null);

        Preconditions.checkState(table instanceof IcebergTable);
    }

    private LogicalIcebergScanOperator(LogicalIcebergScanOperator.Builder builder) {
        super(OperatorType.LOGICAL_ICEBERG_SCAN,
                builder.table,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());

        this.predicates = builder.predicates;
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
        return visitor.visitLogicalIcebergScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalIcebergScanOperator, LogicalIcebergScanOperator.Builder> {
        private ScanOperatorPredicates predicates = new ScanOperatorPredicates();

        @Override
        public LogicalIcebergScanOperator build() {
            return new LogicalIcebergScanOperator(this);
        }

        @Override
        public LogicalIcebergScanOperator.Builder withOperator(LogicalIcebergScanOperator scanOperator) {
            super.withOperator(scanOperator);

            this.predicates = scanOperator.predicates;
            return this;
        }
    }
}
