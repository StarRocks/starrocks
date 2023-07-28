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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
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
    private ImmutableMap<ColumnRefOperator, CallOperator> windowCall;
    private ImmutableList<ScalarOperator> partitionExpressions;
    private ImmutableList<Ordering> orderByElements;
    private AnalyticWindow analyticWindow;
    /**
     * Each LogicalWindowOperator will belong to a SortGroup,
     * so we need to record sortProperty to ensure that only one SortNode is enforced
     */
    private ImmutableList<Ordering> enforceSortColumns;

    /**
     * For window functions with only partition by column but without order by column,
     * we can perform hash-based partition according to hint.
     */
    private boolean useHashBasedPartition;

    private LogicalWindowOperator() {
        super(OperatorType.LOGICAL_WINDOW);
        this.partitionExpressions = ImmutableList.of();
        this.orderByElements = ImmutableList.of();
        this.enforceSortColumns = ImmutableList.of();
        this.useHashBasedPartition = false;
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
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> columnOutputInfoList = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : windowCall.entrySet()) {
            columnOutputInfoList.add(new ColumnOutputInfo(entry.getKey(), entry.getValue()));
        }
        for (ColumnOutputInfo entry : inputs.get(0).getRowOutputInfo().getColumnOutputInfo()) {
            columnOutputInfoList.add(new ColumnOutputInfo(entry.getColumnRef(), entry.getColumnRef()));
        }
        return new RowOutputInfo(columnOutputInfoList);
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends LogicalOperator.Builder<LogicalWindowOperator, LogicalWindowOperator.Builder> {
        @Override
        protected LogicalWindowOperator newInstance() {
            return new LogicalWindowOperator();
        }

        @Override
        public LogicalWindowOperator.Builder withOperator(LogicalWindowOperator windowOperator) {
            super.withOperator(windowOperator);

            builder.windowCall = windowOperator.windowCall;
            builder.partitionExpressions = windowOperator.partitionExpressions;
            builder.orderByElements = windowOperator.orderByElements;
            builder.analyticWindow = windowOperator.analyticWindow;
            builder.enforceSortColumns = windowOperator.enforceSortColumns;
            builder.useHashBasedPartition = windowOperator.useHashBasedPartition;
            return this;
        }

        public Builder setWindowCall(Map<ColumnRefOperator, CallOperator> windowCall) {
            builder.windowCall = ImmutableMap.copyOf(windowCall);
            return this;
        }

        public Builder setPartitionExpressions(List<ScalarOperator> partitionExpressions) {
            builder.partitionExpressions = ImmutableList.copyOf(partitionExpressions);
            return this;
        }

        public Builder setOrderByElements(List<Ordering> orderByElements) {
            builder.orderByElements = ImmutableList.copyOf(orderByElements);
            return this;
        }

        public Builder setAnalyticWindow(AnalyticWindow analyticWindow) {
            builder.analyticWindow = analyticWindow;
            return this;
        }

        public Builder setEnforceSortColumns(List<Ordering> enforceSortColumns) {
            builder.enforceSortColumns = ImmutableList.copyOf(enforceSortColumns);
            return this;
        }

        public Builder setUseHashBasedPartition(boolean useHashBasedPartition) {
            builder.useHashBasedPartition = useHashBasedPartition;
            return this;
        }
    }
}
