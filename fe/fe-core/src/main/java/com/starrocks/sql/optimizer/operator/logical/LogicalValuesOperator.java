// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;

public class LogicalValuesOperator extends LogicalOperator {
    private final List<ColumnRefOperator> columnRefSet;
    private final List<List<ScalarOperator>> rows;

    public LogicalValuesOperator(List<ColumnRefOperator> columnRefSet, List<List<ScalarOperator>> rows) {
        super(OperatorType.LOGICAL_VALUES);
        this.columnRefSet = columnRefSet;
        this.rows = rows;
    }

    private LogicalValuesOperator(Builder builder) {
        super(OperatorType.LOGICAL_VALUES, builder.getLimit(), builder.getPredicate(), builder.getProjection());
        this.columnRefSet = builder.columnRefSet;
        this.rows = builder.rows;
    }

    public List<ColumnRefOperator> getColumnRefSet() {
        return columnRefSet;
    }

    public List<List<ScalarOperator>> getRows() {
        return rows;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            return new ColumnRefSet(columnRefSet);
        }
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalValues(this, context);
    }

    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalValues(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public static class Builder extends LogicalOperator.Builder<LogicalValuesOperator, LogicalValuesOperator.Builder> {
        private List<ColumnRefOperator> columnRefSet;
        private List<List<ScalarOperator>> rows;

        @Override
        public LogicalValuesOperator build() {
            return new LogicalValuesOperator(this);
        }

        @Override
        public Builder withOperator(LogicalValuesOperator valuesOperator) {
            super.withOperator(valuesOperator);

            this.columnRefSet = valuesOperator.columnRefSet;
            this.rows = valuesOperator.rows;
            return this;
        }
    }
}
