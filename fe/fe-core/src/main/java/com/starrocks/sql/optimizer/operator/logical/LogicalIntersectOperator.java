// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

public class LogicalIntersectOperator extends LogicalSetOperator {
    private LogicalIntersectOperator(LogicalIntersectOperator.Builder builder) {
        super(OperatorType.LOGICAL_INTERSECT,
                builder.outputColumnRefOp,
                builder.childOutputColumns,
                builder.getLimit(),
                builder.getProjection());
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalIntersect(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalIntersect(optExpression, context);
    }

    public static class Builder
            extends LogicalSetOperator.Builder<LogicalIntersectOperator, LogicalIntersectOperator.Builder> {
        @Override
        public LogicalIntersectOperator build() {
            return new LogicalIntersectOperator(this);
        }

        @Override
        public LogicalIntersectOperator.Builder withOperator(LogicalIntersectOperator operator) {
            super.withOperator(operator);
            return this;
        }
    }
}

