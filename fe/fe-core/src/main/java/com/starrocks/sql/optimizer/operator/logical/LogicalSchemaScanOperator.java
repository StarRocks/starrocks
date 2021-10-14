// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.SchemaTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class LogicalSchemaScanOperator extends LogicalScanOperator {
    public LogicalSchemaScanOperator(Table table,
                                     List<ColumnRefOperator> outputColumns,
                                     Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                     Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                     long limit,
                                     ScalarOperator predicate) {
        super(OperatorType.LOGICAL_SCHEMA_SCAN,
                table,
                outputColumns,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate);
        Preconditions.checkState(table instanceof SchemaTable);
    }

    private LogicalSchemaScanOperator(Builder builder) {
        super(OperatorType.LOGICAL_SCHEMA_SCAN,
                builder.table,
                builder.outputColumns,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate());
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalSchemaScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalSchemaScanOperator, LogicalSchemaScanOperator.Builder> {
        @Override
        public LogicalSchemaScanOperator build() {
            return new LogicalSchemaScanOperator(this);
        }

        @Override
        public LogicalSchemaScanOperator.Builder withOperator(LogicalSchemaScanOperator operator) {
            super.withOperator(operator);
            return this;
        }
    }
}
