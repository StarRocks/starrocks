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

package com.starrocks.sql.optimizer.rule.tree.pdagg;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregatePushDownUtils.createNewCallOperator;

public class AggregatePushDownContext {
    public static final AggregatePushDownContext EMPTY = new AggregatePushDownContext();

    public LogicalAggregationOperator origAggregator;
    public final Map<ColumnRefOperator, CallOperator> aggregations;
    public final Map<ColumnRefOperator, ScalarOperator> groupBys;

    // Push-down aggregate function can be split into partial and final stage, partial stage is pushed down to
    // scan operator and final stage is pushed down to the parent operator of scan operator.
    // For equivalent aggregate function, we need to record the final stage aggregate function to replace the original
    // aggregate function.
    public final Map<CallOperator, CallOperator> aggToPartialAggMap = Maps.newHashMap();
    public final Map<CallOperator, CallOperator> aggToFinalAggMap = Maps.newHashMap();
    public final Map<CallOperator, CallOperator> aggToOrigAggMap = Maps.newHashMap();
    // Query's aggregate call operator to push down aggregate call operator mapping,
    // those two operators are not the same so record it to be used later.
    public final Map<ColumnRefOperator, CallOperator> aggColRefToPushDownAggMap = Maps.newHashMap();
    // For avg function, split it into sum and count function, this map records the mapping from avg function to sum and
    // count's column ref operator.
    public final Map<CallOperator, Pair<ColumnRefOperator, ColumnRefOperator>> avgToSumCountMapping = Maps.newHashMap();

    // Aggregator will be pushed down to the position above targetPosition.
    public OptExpression targetPosition = null;
    // Whether targetPosition is an immediate left child of a small broadcast join.
    public boolean immediateChildOfSmallBroadcastJoin = false;
    public int rootToLeafPathIndex = 0;

    public boolean hasWindow = false;

    // record push down path
    // the index of children which should push down
    public final List<Integer> pushPaths;

    public AggregatePushDownContext() {
        origAggregator = null;
        aggregations = Maps.newHashMap();
        groupBys = Maps.newHashMap();
        pushPaths = Lists.newArrayList();
    }

    public AggregatePushDownContext(int rootToLeafPathIndex) {
        this();
        this.rootToLeafPathIndex = rootToLeafPathIndex;
    }

    public void setAggregator(LogicalAggregationOperator aggregator) {
        this.origAggregator = aggregator;
        this.aggregations.putAll(aggregator.getAggregations());
        aggregator.getGroupingKeys().forEach(c -> groupBys.put(c, c));
        this.pushPaths.clear();
    }

    /**
     * Set the aggregator and create new column ref operator for avg function.
     */
    public void setAggregator(ColumnRefFactory columnRefFactory,
                              LogicalAggregationOperator aggregator) {
        this.origAggregator = aggregator;
        final Map<ColumnRefOperator, CallOperator> aggregations = aggregator.getAggregations();
        for (Map.Entry<ColumnRefOperator, CallOperator> e : aggregations.entrySet()) {
            if (e.getValue().getFunction().functionName().equalsIgnoreCase(FunctionSet.AVG)) {
                CallOperator agg = e.getValue();
                // for avg function, split it into sum and count function and push them down below join.
                Function sumFn = ScalarOperatorUtil.findSumFn(agg.getFunction().getArgs());
                Pair<ColumnRefOperator, CallOperator> sumCallOp =
                        createNewCallOperator(columnRefFactory, aggregations, sumFn, agg.getChildren());
                this.aggregations.put(sumCallOp.first, sumCallOp.second);
                Function countFn = ScalarOperatorUtil.findArithmeticFunction(agg.getFunction().getArgs(), FunctionSet.COUNT);
                Pair<ColumnRefOperator, CallOperator> countCallOp = createNewCallOperator(columnRefFactory, aggregations,
                        countFn, agg.getChildren());
                this.aggregations.put(countCallOp.first, countCallOp.second);
                this.avgToSumCountMapping.put(e.getValue(), Pair.create(sumCallOp.first, countCallOp.first));
            } else {
                this.aggregations.put(e.getKey(), e.getValue());
            }
        }
        aggregator.getGroupingKeys().forEach(c -> groupBys.put(c, c));
        this.pushPaths.clear();
    }

    public boolean isEmpty() {
        return origAggregator == null;
    }

    /**
     * If the push-down agg has been rewritten, record its partial and final stage aggregate function.
     */
    public void registerAggRewriteInfo(CallOperator aggFunc,
                                       CallOperator partialStageAgg,
                                       CallOperator finalStageAgg) {
        aggToPartialAggMap.put(aggFunc, partialStageAgg);
        aggToFinalAggMap.put(aggFunc, finalStageAgg);
    }

    public void registerOrigAggRewriteInfo(CallOperator aggFunc,
                                           CallOperator origAgg) {
        aggToOrigAggMap.put(aggFunc, origAgg);
    }

    /**
     * Combine input ctx into current context.
     */
    public void combine(AggregatePushDownContext ctx) {
        aggToFinalAggMap.putAll(ctx.aggToFinalAggMap);
        aggColRefToPushDownAggMap.putAll(ctx.aggColRefToPushDownAggMap);
        aggToPartialAggMap.putAll(ctx.aggToPartialAggMap);
        avgToSumCountMapping.putAll(ctx.avgToSumCountMapping);
    }

    public boolean isRewrittenByEquivalent(CallOperator aggCall) {
        return aggToFinalAggMap.containsKey(aggCall);
    }

    public OptExpression getTargetPosition() {
        return targetPosition;
    }
}
