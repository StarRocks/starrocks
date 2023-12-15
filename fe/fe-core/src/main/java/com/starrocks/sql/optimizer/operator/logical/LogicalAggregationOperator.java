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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.DataSkewInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

public class LogicalAggregationOperator extends LogicalOperator {
    private AggType type;
    // The flag for this aggregate operator has split to
    // two stage aggregate or three stage aggregate
    private boolean isSplit;
    /**
     * aggregation key is output variable of aggregate function
     */
    private ImmutableMap<ColumnRefOperator, CallOperator> aggregations;
    private ImmutableList<ColumnRefOperator> groupingKeys;

    // For normal aggregate function, partitionByColumns are same with groupingKeys
    // but for single distinct function, partitionByColumns are not same with groupingKeys
    private List<ColumnRefOperator> partitionByColumns;

    // @todo: refactor it, SingleDistinctFunctionPos depend on the map's order, but the order is not fixed
    // When generate plan fragment, we need this info.
    // For SQL: select count(distinct id_bigint), sum(id_int) from test_basic;
    // In the distinct local (update serialize) agg stage:
    //|   5:AGGREGATE (update serialize)                                                      |
    //|   |  output: count(<slot 13>), sum(<slot 16>)                                         |
    //|   |  group by:                                                                        |
    // count function is update function, but sum is merge function
    // if singleDistinctFunctionPos is -1, means no single distinct function
    private int singleDistinctFunctionPos = -1;

    private DataSkewInfo distinctColumnDataSkew = null;

    // If the AggType is not GLOBAL, it means we have split the agg hence the isSplit should be true.
    // `this.isSplit = !type.isGlobal() || isSplit;` helps us do the work.
    // If you want to manually set this value, you could invoke setOnlyLocalAggregate().
    public LogicalAggregationOperator(AggType type,
                                      List<ColumnRefOperator> groupingKeys,
                                      Map<ColumnRefOperator, CallOperator> aggregations) {
        this(type, groupingKeys, groupingKeys, aggregations, false, -1, -1, null);
    }

    public LogicalAggregationOperator(
            AggType type,
            List<ColumnRefOperator> groupingKeys,
            List<ColumnRefOperator> partitionByColumns,
            Map<ColumnRefOperator, CallOperator> aggregations,
            boolean isSplit,
            int singleDistinctFunctionPos,
            long limit,
            ScalarOperator predicate) {
        super(OperatorType.LOGICAL_AGGR, limit, predicate, null);
        this.type = type;
        this.groupingKeys = ImmutableList.copyOf(groupingKeys);
        this.partitionByColumns = partitionByColumns;
        this.aggregations = ImmutableMap.copyOf(aggregations);
        this.isSplit = !type.isGlobal() || isSplit;
        this.singleDistinctFunctionPos = singleDistinctFunctionPos;
    }

    private LogicalAggregationOperator() {
        super(OperatorType.LOGICAL_AGGR);
        this.isSplit = false;
        this.singleDistinctFunctionPos = -1;
    }

    public AggType getType() {
        return type;
    }

    public Map<ColumnRefOperator, CallOperator> getAggregations() {
        return aggregations;
    }

    public List<ColumnRefOperator> getGroupingKeys() {
        return groupingKeys;
    }

    public boolean isSplit() {
        return isSplit;
    }

    public void setOnlyLocalAggregate() {
        isSplit = false;
    }

    public int getSingleDistinctFunctionPos() {
        return singleDistinctFunctionPos;
    }

    public List<ColumnRefOperator> getPartitionByColumns() {
        return partitionByColumns;
    }

    public void setPartitionByColumns(List<ColumnRefOperator> partitionByColumns) {
        this.partitionByColumns = partitionByColumns;
    }

    public void setDistinctColumnDataSkew(DataSkewInfo distinctColumnDataSkew) {
        this.distinctColumnDataSkew = distinctColumnDataSkew;
    }

    public DataSkewInfo getDistinctColumnDataSkew() {
        return distinctColumnDataSkew;
    }

    public boolean checkGroupByCountDistinct() {
        if (groupingKeys.isEmpty() || aggregations.size() != 1) {
            return false;
        }

        CallOperator call = aggregations.values().stream().iterator().next();
        if (call.isDistinct() && call.getFnName().equalsIgnoreCase(FunctionSet.COUNT) &&
                call.getChildren().size() == 1 && call.getChild(0).isColumnRef() &&
                groupingKeys.stream().noneMatch(groupCol -> call.getChild(0).equals(groupCol))) {
            // GroupByCountDistinctDataSkewEliminateRule will return with empty logical plan
            // in case below, so that we should not skip SplitAggregateRule in this case
            return ScalarOperatorUtil.buildMultiCountDistinct(call) != null;
        }
        return false;
    }

    public boolean hasSkew() {
        return this.getAggregations().values().stream().anyMatch(call ->
                call.isDistinct() && call.getFnName().equals(FunctionSet.COUNT) && call.getHints().contains("skew"));
    }

    public boolean checkGroupByCountDistinctWithSkewHint() {
        return checkGroupByCountDistinct() && hasSkew();
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            ColumnRefSet columns = new ColumnRefSet();
            columns.union(groupingKeys);
            columns.union(new ArrayList<>(aggregations.keySet()));
            return columns;
        }
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> columnOutputInfoList = Lists.newArrayList();
        groupingKeys.stream().forEach(e -> columnOutputInfoList.add(new ColumnOutputInfo(e, e)));
        aggregations.entrySet().forEach(entry -> columnOutputInfoList.add(new ColumnOutputInfo(entry.getKey(),
                entry.getValue())));
        return new RowOutputInfo(columnOutputInfoList);
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> keyMap =
                groupingKeys.stream().collect(Collectors.toMap(identity(), identity()));
        columnRefMap.putAll(keyMap);
        columnRefMap.putAll(aggregations);
        return columnRefMap;
    }

    @Override
    public Map<ColumnRefOperator, ScalarOperator> getLineage(
            ColumnRefFactory refFactory, ExpressionContext expressionContext) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        if (projection != null) {
            columnRefMap.putAll(projection.getColumnRefMap());
        }
        columnRefMap.putAll(getColumnRefMap());
        return columnRefMap;
    }

    @Override
    public String toString() {
        return "LogicalAggregation" + " type " + type.toString();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalAggregation(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalAggregate(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }
        LogicalAggregationOperator that = (LogicalAggregationOperator) o;
        return isSplit == that.isSplit && singleDistinctFunctionPos == that.singleDistinctFunctionPos &&
                type == that.type && Objects.equals(aggregations, that.aggregations) &&
                Objects.equals(groupingKeys, that.groupingKeys) &&
                Objects.equals(partitionByColumns, that.partitionByColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), type, isSplit, aggregations, groupingKeys, partitionByColumns,
                singleDistinctFunctionPos);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalAggregationOperator, LogicalAggregationOperator.Builder> {

        @Override
        protected LogicalAggregationOperator newInstance() {
            return new LogicalAggregationOperator();
        }

        @Override
        public LogicalAggregationOperator build() {
            Preconditions.checkNotNull(builder.type);
            Preconditions.checkNotNull(builder.aggregations);
            Preconditions.checkNotNull(builder.groupingKeys);
            Preconditions.checkNotNull(builder.partitionByColumns);
            return super.build();
        }

        @Override
        public LogicalAggregationOperator.Builder withOperator(LogicalAggregationOperator aggregationOperator) {
            super.withOperator(aggregationOperator);
            builder.type = aggregationOperator.type;
            builder.groupingKeys = aggregationOperator.groupingKeys;
            builder.partitionByColumns = aggregationOperator.partitionByColumns;
            builder.aggregations = aggregationOperator.aggregations;
            builder.isSplit = aggregationOperator.isSplit;
            builder.singleDistinctFunctionPos = aggregationOperator.singleDistinctFunctionPos;
            builder.distinctColumnDataSkew = aggregationOperator.distinctColumnDataSkew;
            return this;
        }

        public Builder setType(AggType type) {
            builder.type = type;
            return this;
        }

        public Builder setGroupingKeys(
                List<ColumnRefOperator> groupingKeys) {
            builder.groupingKeys = ImmutableList.copyOf(groupingKeys);
            return this;
        }

        public Builder setAggregations(Map<ColumnRefOperator, CallOperator> aggregations) {
            builder.aggregations = ImmutableMap.copyOf(aggregations);
            return this;
        }

        public Builder setSplit() {
            builder.isSplit = true;
            return this;
        }

        public Builder setPartitionByColumns(
                List<ColumnRefOperator> partitionByColumns) {
            builder.partitionByColumns = partitionByColumns;
            return this;
        }

        public Builder setSingleDistinctFunctionPos(int singleDistinctFunctionPos) {
            builder.singleDistinctFunctionPos = singleDistinctFunctionPos;
            return this;
        }

        public void setDistinctColumnDataSkew(DataSkewInfo distinctColumnDataSkew) {
            builder.distinctColumnDataSkew = distinctColumnDataSkew;
        }

        public DataSkewInfo getDistinctColumnDataSkew() {
            return builder.distinctColumnDataSkew;
        }
    }
}
