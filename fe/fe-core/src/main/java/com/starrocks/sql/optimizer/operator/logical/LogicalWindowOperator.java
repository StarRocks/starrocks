// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LogicalWindowOperator extends LogicalOperator {
    private final ImmutableMap<ColumnRefOperator, CallOperator> windowCall;
    private final ImmutableList<ScalarOperator> partitionExpressions;
    private final ImmutableList<Ordering> orderByElements;
    private final AnalyticWindow analyticWindow;
    /**
     * Each LogicalWindowOperator will belong to a SortGroup,
     * so we need to record sortProperty to ensure that only one SortNode is enforced
     */
    private final ImmutableList<Ordering> enforceSortColumns;

    /**
     * For window functions with only partition by column but without order by column,
     * we can perform hash-based partition according to hint.
     */
    private final boolean useHashBasedPartition;

    private LogicalWindowOperator(Builder builder) {
        super(OperatorType.LOGICAL_WINDOW, builder.getLimit(), builder.getPredicate(), builder.getProjection());
        this.windowCall = ImmutableMap.copyOf(builder.windowCall);
        this.partitionExpressions = ImmutableList.copyOf(builder.partitionExpressions);
        this.orderByElements = ImmutableList.copyOf(builder.orderByElements);
        this.analyticWindow = builder.analyticWindow;
        this.enforceSortColumns = ImmutableList.copyOf(builder.enforceSortColumns);
        this.useHashBasedPartition = builder.useHashBasedPartition;
    }

    public Map<ColumnRefOperator, CallOperator> getWindowCall() {
        return windowCall;
    }

    public List<ScalarOperator> getPartitionExpressions() {
        return partitionExpressions;
    }

    public List<Ordering> getOrderByElements() {
        return orderByElements;
    }

    public AnalyticWindow getAnalyticWindow() {
        return analyticWindow;
    }

    public List<Ordering> getEnforceSortColumns() {
        return enforceSortColumns;
    }

    public boolean isUseHashBasedPartition() {
        return useHashBasedPartition;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            ColumnRefSet columns = new ColumnRefSet();
            columns.union(new ArrayList<>(windowCall.keySet()));
            columns.union(expressionContext.getChildLogicalProperty(0).getOutputColumns());
            return columns;
        }
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalAnalytic(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalWindow(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        LogicalWindowOperator that = (LogicalWindowOperator) o;
        return Objects.equals(windowCall, that.windowCall)
                && Objects.equals(partitionExpressions, that.partitionExpressions)
                && Objects.equals(orderByElements, that.orderByElements)
                && Objects.equals(analyticWindow, that.analyticWindow)
                && Objects.equals(useHashBasedPartition, that.useHashBasedPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), windowCall, partitionExpressions, orderByElements, analyticWindow,
                useHashBasedPartition);
    }

    public static class Builder extends LogicalOperator.Builder<LogicalWindowOperator, LogicalWindowOperator.Builder> {
        private Map<ColumnRefOperator, CallOperator> windowCall;
        private List<ScalarOperator> partitionExpressions = Lists.newArrayList();
        private List<Ordering> orderByElements = Lists.newArrayList();
        private AnalyticWindow analyticWindow;
        private List<Ordering> enforceSortColumns = Lists.newArrayList();
        private boolean useHashBasedPartition = false;

        @Override
        public LogicalWindowOperator build() {
            return new LogicalWindowOperator(this);
        }

        @Override
        public LogicalWindowOperator.Builder withOperator(LogicalWindowOperator windowOperator) {
            super.withOperator(windowOperator);

            this.windowCall = windowOperator.windowCall;
            this.partitionExpressions = windowOperator.partitionExpressions;
            this.orderByElements = windowOperator.orderByElements;
            this.analyticWindow = windowOperator.analyticWindow;
            this.enforceSortColumns = windowOperator.enforceSortColumns;
            this.useHashBasedPartition = windowOperator.useHashBasedPartition;
            return this;
        }

        public Builder setWindowCall(Map<ColumnRefOperator, CallOperator> windowCall) {
            this.windowCall = windowCall;
            return this;
        }

        public Builder setPartitionExpressions(
                List<ScalarOperator> partitionExpressions) {
            this.partitionExpressions = partitionExpressions;
            return this;
        }

        public Builder setOrderByElements(List<Ordering> orderByElements) {
            this.orderByElements = orderByElements;
            return this;
        }

        public Builder setAnalyticWindow(AnalyticWindow analyticWindow) {
            this.analyticWindow = analyticWindow;
            return this;
        }

        public Builder setEnforceSortColumns(List<Ordering> enforceSortColumns) {
            this.enforceSortColumns = enforceSortColumns;
            return this;
        }

        public Builder setUseHashBasedPartition(boolean useHashBasedPartition) {
            this.useHashBasedPartition = useHashBasedPartition;
            return this;
        }
    }
}
