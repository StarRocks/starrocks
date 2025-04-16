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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LogicalRepeatOperator extends LogicalOperator {
    private List<ColumnRefOperator> outputGrouping;
    private List<List<ColumnRefOperator>> repeatColumnRefList;
    private List<List<Long>> groupingIds;
    private Map<ColumnRefOperator, List<ColumnRefOperator>> groupingsFnArgs; // SPM use
    private boolean hasPushDown;

    public LogicalRepeatOperator(List<ColumnRefOperator> outputGrouping,
                                 List<List<ColumnRefOperator>> repeatColumnRefList, List<List<Long>> groupingIds,
                                 Map<ColumnRefOperator, List<ColumnRefOperator>> groupingsFnArgs) {
        super(OperatorType.LOGICAL_REPEAT);
        this.outputGrouping = outputGrouping;
        this.repeatColumnRefList = repeatColumnRefList;
        this.groupingIds = groupingIds;
        this.hasPushDown = false;
        this.groupingsFnArgs = groupingsFnArgs;
    }

    private LogicalRepeatOperator() {
        super(OperatorType.LOGICAL_REPEAT);
    }

    public List<ColumnRefOperator> getOutputGrouping() {
        return outputGrouping;
    }

    public List<List<ColumnRefOperator>> getRepeatColumnRef() {
        return repeatColumnRefList;
    }

    public List<List<Long>> getGroupingIds() {
        return groupingIds;
    }

    public boolean hasPushDown() {
        return hasPushDown;
    }

    public Map<ColumnRefOperator, List<ColumnRefOperator>> getGroupingsFnArgs() {
        return groupingsFnArgs;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        ColumnRefSet outputColumns = new ColumnRefSet(outputGrouping);
        for (List<ColumnRefOperator> refSets : repeatColumnRefList) {
            outputColumns.union(new ArrayList<>(refSets));
        }

        return outputColumns;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> columnOutputInfoList = Lists.newArrayList();
        outputGrouping.forEach(e -> columnOutputInfoList.add(new ColumnOutputInfo(e, e)));
        for (ColumnOutputInfo columnOutputInfo : inputs.get(0).getRowOutputInfo().getColumnOutputInfo()) {
            columnOutputInfoList.add(new ColumnOutputInfo(columnOutputInfo.getColumnRef(), columnOutputInfo.getColumnRef()));
        }
        return new RowOutputInfo(columnOutputInfoList, outputGrouping);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalRepeat(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalRepeat(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        LogicalRepeatOperator that = (LogicalRepeatOperator) o;
        return Objects.equals(outputGrouping, that.outputGrouping) &&
                Objects.equals(repeatColumnRefList, that.repeatColumnRefList) &&
                Objects.equals(groupingIds, that.groupingIds) &&
                Objects.equals(hasPushDown, that.hasPushDown);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), outputGrouping, repeatColumnRefList, hasPushDown);
    }

    public static Builder builder() {
        return new Builder();
    }
    public static class Builder
            extends LogicalOperator.Builder<LogicalRepeatOperator, LogicalRepeatOperator.Builder> {

        @Override
        protected LogicalRepeatOperator newInstance() {
            return new LogicalRepeatOperator();
        }

        public LogicalRepeatOperator.Builder setHasPushDown(boolean hasPushDown) {
            builder.hasPushDown = hasPushDown;
            return this;
        }

        public LogicalRepeatOperator.Builder setOutputGrouping(List<ColumnRefOperator> outputGrouping) {
            builder.outputGrouping = outputGrouping;
            return this;
        }

        public LogicalRepeatOperator.Builder setGroupingIds(List<List<Long>> groupingIds) {
            builder.groupingIds = groupingIds;
            return this;
        }

        public LogicalRepeatOperator.Builder setRepeatColumnRefList(List<List<ColumnRefOperator>> repeatColumnRefList) {
            builder.repeatColumnRefList = repeatColumnRefList;
            return this;
        }

        @Override
        public LogicalRepeatOperator.Builder withOperator(LogicalRepeatOperator operator) {
            super.withOperator(operator);
            builder.outputGrouping = operator.outputGrouping;
            builder.repeatColumnRefList = operator.repeatColumnRefList;
            builder.groupingIds = operator.groupingIds;
            return this;
        }
    }
}
