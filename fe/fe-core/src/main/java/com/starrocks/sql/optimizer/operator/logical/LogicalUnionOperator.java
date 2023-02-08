// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

public class LogicalUnionOperator extends LogicalSetOperator {
    private final boolean isUnionAll;

    public LogicalUnionOperator(List<ColumnRefOperator> result, List<List<ColumnRefOperator>> childOutputColumns,
                                boolean isUnionAll) {
        super(OperatorType.LOGICAL_UNION, result, childOutputColumns, Operator.DEFAULT_LIMIT, null);
        this.isUnionAll = isUnionAll;
    }

    private LogicalUnionOperator(LogicalUnionOperator.Builder builder) {
        super(OperatorType.LOGICAL_UNION, builder.outputColumnRefOp, builder.childOutputColumns,
                builder.getLimit(),
                builder.getProjection());
        this.isUnionAll = builder.isUnionAll;
    }

    public boolean isUnionAll() {
        return isUnionAll;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        LogicalUnionOperator that = (LogicalUnionOperator) o;
        return isUnionAll == that.isUnionAll;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalUnion(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalUnion(optExpression, context);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends LogicalSetOperator.Builder<LogicalUnionOperator, LogicalUnionOperator.Builder> {
        private boolean isUnionAll;

        @Override
        public LogicalUnionOperator build() {
            return new LogicalUnionOperator(this);
        }

        @Override
        public Builder withOperator(LogicalUnionOperator unionOperator) {
            super.withOperator(unionOperator);
            isUnionAll = unionOperator.isUnionAll;
            return this;
        }

        public Builder isUnionAll(boolean isUnionAll) {
            this.isUnionAll = isUnionAll;
            return this;
        }
    }
}
