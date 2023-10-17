// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.logical;

<<<<<<< HEAD
=======
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
>>>>>>> d868096d84 ([BugFix] optimize topn 0,0 (#32818))
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class LogicalTopNOperator extends LogicalOperator {
    private final List<ColumnRefOperator> partitionByColumns;
    private final long partitionLimit;
    private final List<Ordering> orderByElements;
    private final long offset;
    private final SortPhase sortPhase;
    private final TopNType topNType;
    private final boolean isSplit;

    public LogicalTopNOperator(List<Ordering> orderByElements) {
        this(DEFAULT_LIMIT, null, null, null, DEFAULT_LIMIT, orderByElements, DEFAULT_OFFSET, SortPhase.FINAL,
                TopNType.ROW_NUMBER,
                false);
    }

    public LogicalTopNOperator(List<Ordering> orderByElements, long limit, long offset) {
        this(limit, null, null, null, DEFAULT_LIMIT, orderByElements, offset, SortPhase.FINAL, TopNType.ROW_NUMBER,
                false);
    }

    public LogicalTopNOperator(List<Ordering> orderByElements, long limit, long offset,
                               SortPhase sortPhase) {
        this(limit, null, null, null, DEFAULT_LIMIT, orderByElements, offset, sortPhase, TopNType.ROW_NUMBER, false);
    }

    private LogicalTopNOperator(Builder builder) {
        this(builder.getLimit(), builder.getPredicate(), builder.getProjection(), builder.partitionByColumns,
                builder.partitionLimit, builder.orderByElements, builder.offset, builder.sortPhase, builder.topNType,
                builder.isSplit);
    }

    private LogicalTopNOperator(long limit,
                                ScalarOperator predicate, Projection projection,
                                List<ColumnRefOperator> partitionByColumns,
                                long partitionLimit,
                                List<Ordering> orderByElements, long offset,
                                SortPhase sortPhase, TopNType topNType, boolean isSplit) {
        super(OperatorType.LOGICAL_TOPN, limit, predicate, projection);
        this.partitionByColumns = partitionByColumns;
        this.partitionLimit = partitionLimit;
        this.orderByElements = orderByElements;
        this.offset = offset;
        this.sortPhase = sortPhase;
        this.topNType = topNType;
        this.isSplit = isSplit;
        Preconditions.checkState(limit != 0);
    }

    public SortPhase getSortPhase() {
        return sortPhase;
    }

    public TopNType getTopNType() {
        return topNType;
    }

    public boolean isSplit() {
        return isSplit;
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

    public List<ColumnRefOperator> getPartitionByColumns() {
        return partitionByColumns;
    }

    public long getPartitionLimit() {
        return partitionLimit;
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

        if (!super.equals(o)) {
            return false;
        }

        LogicalTopNOperator that = (LogicalTopNOperator) o;
        return partitionLimit == that.partitionLimit && offset == that.offset && isSplit == that.isSplit &&
                Objects.equals(partitionByColumns, that.partitionByColumns) &&
                Objects.equals(orderByElements, that.orderByElements) &&
                sortPhase == that.sortPhase && topNType == that.topNType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), orderByElements, offset, sortPhase, topNType, isSplit);
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalTopNOperator, LogicalTopNOperator.Builder> {
        private List<ColumnRefOperator> partitionByColumns;
        private long partitionLimit;
        private List<Ordering> orderByElements;
        private long offset;
        private SortPhase sortPhase;
        private TopNType topNType = TopNType.ROW_NUMBER;
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
            this.topNType = topNOperator.topNType;
            this.isSplit = topNOperator.isSplit;
            this.partitionLimit = topNOperator.partitionLimit;
            this.partitionByColumns = topNOperator.partitionByColumns;
            return this;
        }

        public LogicalTopNOperator.Builder setPartitionByColumns(List<ColumnRefOperator> partitionByColumns) {
            this.partitionByColumns = partitionByColumns;
            return this;
        }

        public LogicalTopNOperator.Builder setPartitionLimit(long partitionLimit) {
            this.partitionLimit = partitionLimit;
            return this;
        }

        public LogicalTopNOperator.Builder setOrderByElements(List<Ordering> orderByElements) {
            this.orderByElements = orderByElements;
            return this;
        }

        public LogicalTopNOperator.Builder setOffset(int offset) {
            this.offset = offset;
            return this;
        }

        public LogicalTopNOperator.Builder setTopNType(TopNType topNType) {
            this.topNType = topNType;
            return this;
        }

        public LogicalTopNOperator.Builder setSortPhase(SortPhase sortPhase) {
            this.sortPhase = sortPhase;
            return this;
        }

        public LogicalTopNOperator.Builder setIsSplit(boolean isSplit) {
            this.isSplit = isSplit;
            return this;
        }
    }
}
