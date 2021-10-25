// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.SortPhase;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class LogicalTopNOperator extends LogicalOperator {
    private final List<Ordering> orderByElements;
    private final long offset;

    private final SortPhase sortPhase;
    private boolean isSplit = false;

    public LogicalTopNOperator(List<Ordering> orderByElements) {
        super(OperatorType.LOGICAL_TOPN);
        this.orderByElements = orderByElements;
        this.limit = -1;
        this.offset = 0;
        this.sortPhase = SortPhase.FINAL;
    }

    public LogicalTopNOperator(List<Ordering> orderByElements, long limit, long offset) {
        super(OperatorType.LOGICAL_TOPN);
        this.orderByElements = orderByElements;
        this.limit = limit;
        this.offset = offset;
        this.sortPhase = SortPhase.FINAL;
    }

    public LogicalTopNOperator(List<Ordering> orderByElements, long limit, long offset,
                               SortPhase sortPhase) {
        super(OperatorType.LOGICAL_TOPN);
        this.orderByElements = orderByElements;
        this.limit = limit;
        this.offset = offset;
        this.sortPhase = sortPhase;
    }

    private LogicalTopNOperator(Builder builder) {
        super(OperatorType.LOGICAL_TOPN, builder.getLimit(), builder.getPredicate(), builder.getProjection());
        this.orderByElements = builder.orderByElements;
        this.offset = builder.offset;
        this.sortPhase = builder.sortPhase;
        this.isSplit = builder.isSplit;
    }

    public SortPhase getSortPhase() {
        return sortPhase;
    }

    public boolean isSplit() {
        return isSplit;
    }

    public void setSplit() {
        isSplit = true;
    }

    public ColumnRefSet getRequiredChildInputColumns() {
        ColumnRefSet columns = new ColumnRefSet();
        for (Ordering ordering : orderByElements) {
            columns.union(ordering.getColumnRef());
        }
        return columns;
    }

    public long getOffset() {
        return offset;
    }

    public List<Ordering> getOrderByElements() {
        return orderByElements;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            ColumnRefSet columns = new ColumnRefSet();

            columns.union(expressionContext.getChildLogicalProperty(0).getOutputColumns());
            for (Ordering ordering : orderByElements) {
                columns.union(ordering.getColumnRef());
            }
            return columns;
        }
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalTopN(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalTopN(optExpression, context);
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
        LogicalTopNOperator that = (LogicalTopNOperator) o;
        return offset == that.offset && Objects.equals(orderByElements, that.orderByElements) &&
                sortPhase == that.sortPhase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortPhase, orderByElements, offset);
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalTopNOperator, LogicalTopNOperator.Builder> {
        private List<Ordering> orderByElements;
        private long offset;
        private SortPhase sortPhase;
        private boolean isSplit = false;

        @Override
        public LogicalTopNOperator build() {
            return new LogicalTopNOperator(this);
        }

        @Override
        public LogicalTopNOperator.Builder withOperator(LogicalTopNOperator topNOperator) {
            super.withOperator(topNOperator);
            this.orderByElements = topNOperator.orderByElements;
            this.offset = topNOperator.offset;
            this.sortPhase = topNOperator.sortPhase;
            this.isSplit = topNOperator.isSplit;
            return this;
        }
    }
}
