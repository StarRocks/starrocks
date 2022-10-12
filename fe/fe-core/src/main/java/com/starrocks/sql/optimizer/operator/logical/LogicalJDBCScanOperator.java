// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalJDBCScanOperator extends LogicalScanOperator {

    public LogicalJDBCScanOperator(Table table,
                                   Map<ColumnRefOperator, Column> columnRefOperatorColumnMap,
                                   Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                   long limit,
                                   ScalarOperator predicate,
                                   Projection projection) {
        super(OperatorType.LOGICAL_JDBC_SCAN,
                table,
                columnRefOperatorColumnMap,
                columnMetaToColRefMap,
                limit, predicate, projection);
        Preconditions.checkState(table instanceof JDBCTable);
    }

    private LogicalJDBCScanOperator(LogicalJDBCScanOperator.Builder builder) {
        super(OperatorType.LOGICAL_JDBC_SCAN,
                builder.table,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJDBCScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalJDBCScanOperator, LogicalJDBCScanOperator.Builder> {
        @Override
        public LogicalJDBCScanOperator build() {
            return new LogicalJDBCScanOperator(this);
        }

        @Override
        public LogicalJDBCScanOperator.Builder withOperator(LogicalJDBCScanOperator operator) {
            super.withOperator(operator);
            return this;
        }
    }
}
