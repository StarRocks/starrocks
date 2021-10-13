// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public class LogicalFilterOperator extends LogicalOperator {
    public LogicalFilterOperator(ScalarOperator predicate) {
        super(OperatorType.LOGICAL_FILTER);
        this.predicate = predicate;
    }

    private LogicalFilterOperator(Builder builder) {
        super(OperatorType.LOGICAL_FILTER);
        this.predicate = builder.getPredicate();
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    public ColumnRefSet getRequiredChildInputColumns() {
        return predicate.getUsedColumns();
    }

    @Override
    public int hashCode() {
        return predicate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LogicalFilterOperator)) {
            return false;
        }
        LogicalFilterOperator rhs = (LogicalFilterOperator) obj;
        if (this == rhs) {
            return true;
        }

        return predicate.equals(rhs.getPredicate());
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return expressionContext.getChildLogicalProperty(0).getOutputColumns();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFilter(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalFilter(optExpression, context);
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalFilterOperator, LogicalFilterOperator.Builder> {
        @Override
        public LogicalFilterOperator build() {
            return new LogicalFilterOperator(this);
        }

        @Override
        public LogicalFilterOperator.Builder withOperator(LogicalFilterOperator operator) {
            super.withOperator(operator);
            return this;
        }
    }
}
