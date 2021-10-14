// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

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
    private final Map<ColumnRefOperator, CallOperator> windowCall;
    private final List<ScalarOperator> partitionExpressions;
    private final List<Ordering> orderByElements;
    private final AnalyticWindow analyticWindow;

    public LogicalWindowOperator(Map<ColumnRefOperator, CallOperator> windowCall,
                                 List<ScalarOperator> partitionExpressions,
                                 List<Ordering> orderByElements, AnalyticWindow analyticWindow) {
        super(OperatorType.LOGICAL_WINDOW);
        this.windowCall = windowCall;
        this.partitionExpressions = partitionExpressions;
        this.orderByElements = orderByElements;
        this.analyticWindow = analyticWindow;
    }

    private LogicalWindowOperator(Builder builder) {
        super(OperatorType.LOGICAL_WINDOW);
        this.windowCall = builder.windowCall;
        this.partitionExpressions = builder.partitionExpressions;
        this.orderByElements = builder.orderByElements;
        this.analyticWindow = builder.analyticWindow;
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

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        ColumnRefSet columns = new ColumnRefSet();
        columns.union(new ArrayList<>(windowCall.keySet()));
        columns.union(expressionContext.getChildLogicalProperty(0).getOutputColumns());
        return columns;
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalWindowOperator that = (LogicalWindowOperator) o;
        return Objects.equals(windowCall, that.windowCall)
                && Objects.equals(partitionExpressions, that.partitionExpressions)
                && Objects.equals(orderByElements, that.orderByElements)
                && Objects.equals(analyticWindow, that.analyticWindow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), windowCall, partitionExpressions, orderByElements, analyticWindow);
    }

    public static class Builder extends LogicalOperator.Builder<LogicalWindowOperator, LogicalWindowOperator.Builder> {
        private Map<ColumnRefOperator, CallOperator> windowCall;
        private List<ScalarOperator> partitionExpressions;
        private List<Ordering> orderByElements;
        private AnalyticWindow analyticWindow;

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
            return this;
        }
    }
}
