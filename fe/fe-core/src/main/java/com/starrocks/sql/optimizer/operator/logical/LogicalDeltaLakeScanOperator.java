// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalDeltaLakeScanOperator extends LogicalScanOperator {
    private ScanOperatorPredicates predicates = new ScanOperatorPredicates();

    public LogicalDeltaLakeScanOperator(Table table,
                                        Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                        Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                        long limit, ScalarOperator predicate) {
        super(OperatorType.LOGICAL_DELTALAKE_SCAN, table, colRefToColumnMetaMap, columnMetaToColRefMap, limit,
                predicate, null);
        Preconditions.checkState(table instanceof DeltaLakeTable);
    }

    private LogicalDeltaLakeScanOperator(LogicalDeltaLakeScanOperator.Builder builder) {
        super(OperatorType.LOGICAL_DELTALAKE_SCAN,
                builder.table,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());

        this.predicates = builder.predicates;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalDeltaLakeScan(this, context);
    }

    @Override
    public ScanOperatorPredicates getScanOperatorPredicates() {
        return this.predicates;
    }

    @Override
    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) {
        this.predicates = predicates;
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalDeltaLakeScanOperator, LogicalDeltaLakeScanOperator.Builder> {
        private ScanOperatorPredicates predicates = new ScanOperatorPredicates();

        @Override
        public LogicalDeltaLakeScanOperator build() {
            return new LogicalDeltaLakeScanOperator(this);
        }

        @Override
        public LogicalDeltaLakeScanOperator.Builder withOperator(LogicalDeltaLakeScanOperator scanOperator) {
            super.withOperator(scanOperator);

            this.predicates = scanOperator.predicates;
            return this;
        }
    }
}
