// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.Operator;
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
    private List<ColumnRefOperator> partitionByColumns;
    private long partitionLimit;
    private List<Ordering> orderByElements;
    private long offset;
    private SortPhase sortPhase;
    private TopNType topNType;
    private boolean isSplit;

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

    private LogicalTopNOperator() {
        super(OperatorType.LOGICAL_TOPN);
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

    public boolean hasOffset() {
        return offset != Operator.DEFAULT_OFFSET;
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
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> entryList = Lists.newArrayList();
        for (ColumnOutputInfo entry : inputs.get(0).getRowOutputInfo().getColumnOutputInfo()) {
            entryList.add(new ColumnOutputInfo(entry.getColumnRef(), entry.getColumnRef()));
        }
        for (Ordering ordering : orderByElements) {
            entryList.add(new ColumnOutputInfo(ordering.getColumnRef(), ordering.getColumnRef()));
        }
        return new RowOutputInfo(entryList);
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

        @Override
        protected LogicalTopNOperator newInstance() {
            return new LogicalTopNOperator();
        }

        @Override
        public LogicalTopNOperator.Builder withOperator(LogicalTopNOperator topNOperator) {
            super.withOperator(topNOperator);
            builder.orderByElements = topNOperator.orderByElements;
            builder.offset = topNOperator.offset;
            builder.sortPhase = topNOperator.sortPhase;
            builder.topNType = topNOperator.topNType;
            builder.isSplit = topNOperator.isSplit;
            builder.partitionLimit = topNOperator.partitionLimit;
            builder.partitionByColumns = topNOperator.partitionByColumns;
            return this;
        }

        public LogicalTopNOperator.Builder setPartitionByColumns(List<ColumnRefOperator> partitionByColumns) {
            builder.partitionByColumns = partitionByColumns;
            return this;
        }

        public LogicalTopNOperator.Builder setPartitionLimit(long partitionLimit) {
            builder.partitionLimit = partitionLimit;
            return this;
        }

        public LogicalTopNOperator.Builder setOrderByElements(List<Ordering> orderByElements) {
            builder.orderByElements = orderByElements;
            return this;
        }

        public LogicalTopNOperator.Builder setOffset(int offset) {
            builder.offset = offset;
            return this;
        }

        public LogicalTopNOperator.Builder setTopNType(TopNType topNType) {
            builder.topNType = topNType;
            return this;
        }

        public LogicalTopNOperator.Builder setSortPhase(SortPhase sortPhase) {
            builder.sortPhase = sortPhase;
            return this;
        }

        public LogicalTopNOperator.Builder setIsSplit(boolean isSplit) {
            builder.isSplit = isSplit;
            return this;
        }
    }
}
