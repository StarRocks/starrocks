// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

public class LogicalExceptOperator extends LogicalSetOperator {
    private LogicalExceptOperator(Builder builder) {
        super(OperatorType.LOGICAL_EXCEPT, builder.outputColumnRefOp, builder.childOutputColumns);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalExcept(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalExcept(optExpression, context);
    }

    public static class Builder
            extends LogicalSetOperator.Builder<LogicalExceptOperator, LogicalExceptOperator.Builder> {
        @Override
        public LogicalExceptOperator build() {
            return new LogicalExceptOperator(this);
        }

        @Override
        public Builder withOperator(LogicalExceptOperator operator) {
            super.withOperator(operator);
            return this;
        }
    }
}
