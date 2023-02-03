// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;
import java.util.Objects;

public final class LogicalProjectOperator extends LogicalOperator {
    private final Map<ColumnRefOperator, ScalarOperator> columnRefMap;

    public LogicalProjectOperator(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        super(OperatorType.LOGICAL_PROJECT);
        this.columnRefMap = columnRefMap;
    }

    public LogicalProjectOperator(Map<ColumnRefOperator, ScalarOperator> columnRefMap, long limit) {
        super(OperatorType.LOGICAL_PROJECT);
        this.columnRefMap = columnRefMap;
        this.limit = limit;
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnRefMap;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        ColumnRefSet columns = new ColumnRefSet();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : columnRefMap.entrySet()) {
            columns.union(kv.getKey());
        }
        return columns;
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, columnRefMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LogicalProjectOperator)) {
            return false;
        }
        LogicalProjectOperator rhs = (LogicalProjectOperator) obj;
        if (this == rhs) {
            return true;
        }

        return columnRefMap.keySet().equals(rhs.columnRefMap.keySet());
    }

    @Override
    public String toString() {
        return "LogicalProjectOperator " + columnRefMap.keySet();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalProject(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalProject(optExpression, context);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends Operator.Builder<LogicalProjectOperator, LogicalProjectOperator.Builder> {
        private Map<ColumnRefOperator, ScalarOperator> columnRefMap;

        @Override
        public Builder withOperator(LogicalProjectOperator operator) {
            super.withOperator(operator);
            this.columnRefMap = operator.getColumnRefMap();
            return this;
        }

        public Builder setColumnRefMap(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
            this.columnRefMap = columnRefMap;
            return this;
        }

        @Override
        public Builder setProjection(Projection projection) {
            Preconditions.checkState(false, "Shouldn't set projection to Project Operator");
            return this;
        }

        @Override
        public LogicalProjectOperator build() {
            return new LogicalProjectOperator(columnRefMap, this.limit);
        }
    }
}