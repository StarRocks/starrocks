// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.Objects;

public class LogicalLimitOperator extends LogicalOperator {
    private final long offset;

    public LogicalLimitOperator(long limit) {
        super(OperatorType.LOGICAL_LIMIT);
        this.limit = limit;
        offset = -1;
    }

    public LogicalLimitOperator(long limit, long offset) {
        super(OperatorType.LOGICAL_LIMIT);
        this.limit = limit;
        this.offset = offset;
    }

    public LogicalLimitOperator(Builder builder) {
        super(OperatorType.LOGICAL_LIMIT);
        this.limit = builder.getLimit();
        this.offset = builder.offset;
    }

    public boolean hasOffset() {
        return offset > 0;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return expressionContext.getChildLogicalProperty(0).getOutputColumns();
    }

    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalLimit(optExpression, context);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalLimit(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalLimitOperator that = (LogicalLimitOperator) o;
        return offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), offset);
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalLimitOperator, LogicalLimitOperator.Builder> {
        private long offset = -1;

        @Override
        public LogicalLimitOperator build() {
            return new LogicalLimitOperator(this);
        }

        @Override
        public LogicalLimitOperator.Builder withOperator(LogicalLimitOperator operator) {
            super.withOperator(operator);
            this.offset = operator.offset;
            return this;
        }

        public Builder setOffset(long offset) {
            this.offset = offset;
            return this;
        }
    }
}
