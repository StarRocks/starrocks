// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.ArrayList;
import java.util.Objects;

/*
 * This operator denotes where a particular CTE is defined in the query.
 * It defines the scope of that CTE. A CTE can be referenced only in the
 * subtree rooted by the corresponding CTEAnchor operator
 *
 * */
public class LogicalCTEAnchorOperator extends LogicalOperator {
    private final int cteId;

    public LogicalCTEAnchorOperator(int cteId) {
        super(OperatorType.LOGICAL_CTE_ANCHOR);
        this.cteId = cteId;
    }

    private LogicalCTEAnchorOperator(LogicalCTEAnchorOperator.Builder builder) {
        super(OperatorType.LOGICAL_CTE_ANCHOR, builder.getLimit(), builder.getPredicate(), builder.getProjection());
        this.cteId = builder.cteId;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            return expressionContext.getChildLogicalProperty(1).getOutputColumns();
        }
    }

    public int getCteId() {
        return cteId;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTEAnchor(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalCTEAnchor(optExpression, context);
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
        LogicalCTEAnchorOperator that = (LogicalCTEAnchorOperator) o;
        return Objects.equals(cteId, that.cteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId);
    }

    @Override
    public String toString() {
        return "LogicalCTEAnchorOperator{" +
                "cteId='" + cteId + '\'' +
                '}';
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalCTEAnchorOperator, LogicalCTEAnchorOperator.Builder> {
        private int cteId;

        @Override
        public LogicalCTEAnchorOperator build() {
            return new LogicalCTEAnchorOperator(this);
        }

        @Override
        public LogicalCTEAnchorOperator.Builder withOperator(LogicalCTEAnchorOperator operator) {
            super.withOperator(operator);
            this.cteId = operator.cteId;
            return this;
        }
    }
}
