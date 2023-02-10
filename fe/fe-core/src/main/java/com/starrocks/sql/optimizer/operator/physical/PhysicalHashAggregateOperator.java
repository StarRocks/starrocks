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


package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PhysicalHashAggregateOperator extends PhysicalOperator {
    public static final Set<String> COULD_APPLY_LOW_CARD_AGGREGATE_FUNCTION = Sets.newHashSet(
            FunctionSet.COUNT, FunctionSet.MULTI_DISTINCT_COUNT, FunctionSet.MAX, FunctionSet.MIN
    );

    private final AggType type;
    private final List<ColumnRefOperator> groupBys;
    // For normal aggregate function, partitionByColumns are same with groupingKeys
    // but for single distinct function, partitionByColumns are not same with groupingKeys
    private final List<ColumnRefOperator> partitionByColumns;
    private final Map<ColumnRefOperator, CallOperator> aggregations;

    // When generate plan fragment, we need this info.
    // For select count(distinct id_bigint), sum(id_int) from test_basic;
    // In the distinct local (update serialize) agg stage:
    // |   5:AGGREGATE (update serialize)                                                      |
    //|   |  output: count(<slot 13>), sum(<slot 16>)                                         |
    //|   |  group by:                                                                        |
    // count function is update function, but sum is merge function
    // if singleDistinctFunctionPos is -1, means no single distinct function
    private final int singleDistinctFunctionPos;

    // The flag for this aggregate operator has split to
    // two stage aggregate or three stage aggregate
    private final boolean isSplit;
    // flg for this aggregate operator could use streaming pre-aggregation
    private boolean useStreamingPreAgg = true;

    private boolean useSortAgg = false;

    public PhysicalHashAggregateOperator(AggType type,
                                         List<ColumnRefOperator> groupBys,
                                         List<ColumnRefOperator> partitionByColumns,
                                         Map<ColumnRefOperator, CallOperator> aggregations,
                                         int singleDistinctFunctionPos,
                                         boolean isSplit,
                                         long limit,
                                         ScalarOperator predicate,
                                         Projection projection) {
        super(OperatorType.PHYSICAL_HASH_AGG);
        this.type = type;
        this.groupBys = groupBys;
        this.partitionByColumns = partitionByColumns;
        this.aggregations = aggregations;
        this.singleDistinctFunctionPos = singleDistinctFunctionPos;
        this.isSplit = isSplit;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public List<ColumnRefOperator> getGroupBys() {
        return groupBys;
    }

    public Map<ColumnRefOperator, CallOperator> getAggregations() {
        return aggregations;
    }

    public AggType getType() {
        return type;
    }

    public boolean isOnePhaseAgg() {
        return type.isGlobal() && !isSplit;
    }

    /**
     * Whether it is the first phase in three/four-phase agg whose second phase is pruned.
     */
    public boolean isMergedLocalAgg() {
        return type.isLocal() && !useStreamingPreAgg;
    }

    public List<ColumnRefOperator> getPartitionByColumns() {
        return partitionByColumns;
    }

    public boolean hasSingleDistinct() {
        return singleDistinctFunctionPos > -1;
    }

    public int getSingleDistinctFunctionPos() {
        return singleDistinctFunctionPos;
    }

    public boolean isSplit() {
        return isSplit;
    }

    public void setUseStreamingPreAgg(boolean useStreamingPreAgg) {
        this.useStreamingPreAgg = useStreamingPreAgg;
    }

    public boolean isUseStreamingPreAgg() {
        return this.useStreamingPreAgg;
    }

    public boolean isUseSortAgg() {
        return useSortAgg;
    }

    public void setUseSortAgg(boolean useSortAgg) {
        this.useSortAgg = useSortAgg;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), type, groupBys, aggregations.keySet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalHashAggregateOperator that = (PhysicalHashAggregateOperator) o;
        return type == that.type && Objects.equals(aggregations, that.aggregations) &&
                Objects.equals(groupBys, that.groupBys);
    }

    @Override
    public String toString() {
        return "PhysicalHashAggregate" + " type " + type.toString();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHashAggregate(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalHashAggregate(optExpression, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet set = super.getUsedColumns();
        groupBys.forEach(set::union);
        partitionByColumns.forEach(set::union);
        aggregations.values().forEach(d -> set.union(d.getUsedColumns()));
        return set;
    }

    @Override
    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = ColumnRefSet.createByIds(childDictColumns);

        for (CallOperator operator : aggregations.values()) {
            if (couldApplyStringDict(operator, dictSet)) {
                return true;
            }
        }

        for (ColumnRefOperator groupBy : groupBys) {
            if (childDictColumns.contains(groupBy.getId())) {
                return true;
            }
        }

        return false;
    }

    private boolean couldApplyStringDict(CallOperator operator, ColumnRefSet dictSet) {
        for (ScalarOperator child : operator.getChildren()) {
            if (!(child instanceof ColumnRefOperator)) {
                return false;
            }
        }
        ColumnRefSet usedColumns = operator.getUsedColumns();
        if (usedColumns.isIntersect(dictSet)) {
            return COULD_APPLY_LOW_CARD_AGGREGATE_FUNCTION.contains(operator.getFnName());
        }
        return true;
    }

    public void fillDisableDictOptimizeColumns(ColumnRefSet resultSet, Set<Integer> dictColIds) {
        ColumnRefSet dictSet = new ColumnRefSet();
        dictColIds.forEach(dictSet::union);
        final ScalarOperator predicate = getPredicate();
        getAggregations().forEach((k, v) -> {
            if (resultSet.contains(k.getId())) {
                resultSet.union(v.getUsedColumns());
            } else {
                if (!couldApplyStringDict(v, dictSet)) {
                    resultSet.union(v.getUsedColumns());
                }

                // disable DictOptimize when having predicate couldn't push down
                if (predicate != null && predicate.getUsedColumns().isIntersect(k.getUsedColumns())) {
                    resultSet.union(v.getUsedColumns());
                }
            }
        });
        // Now we disable DictOptimize when group by predicate couldn't push down
        if (predicate != null) {
            final ColumnRefSet predicateUsedColumns = predicate.getUsedColumns();
            for (Integer dictColId : dictColIds) {
                if (predicateUsedColumns.contains(dictColId)) {
                    resultSet.union(dictColId);
                }
            }
        }
    }

}
