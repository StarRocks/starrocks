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
package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class RankingWindowUtils {
    public static boolean isSingleRankRelatedAnalyticOperator(LogicalWindowOperator operator) {
        if (operator.getWindowCall().size() != 1) {
            return false;
        }

        ColumnRefOperator windowCol = Lists.newArrayList(operator.getWindowCall().keySet()).get(0);
        CallOperator callOperator = operator.getWindowCall().get(windowCol);

        return FunctionSet.ROW_NUMBER.equals(callOperator.getFnName()) ||
                FunctionSet.RANK.equals(callOperator.getFnName());
    }

    // actually if operator's window partition columns is subset of rankRelatedOperator's window partition columns and operator's window
    // functions are all rollup agg function, we also can support this optimization. But this means rankRelatedOperator's partition columns
    // maybe has high cardinality which is bad case, so we only support same partition columns case right now.
    public static boolean satisfyRankingPreAggOptimization(LogicalWindowOperator operator,
                                                           LogicalWindowOperator rankRelatedOperator) {
        // if partition expr is empty, we don't support pre-Agg optimization.
        // TODO(JYJ): support this case in the future. which need to modify PartitionSort Sink/Source Operator
        if (rankRelatedOperator.getPartitionExpressions().isEmpty() || operator.getPartitionExpressions().isEmpty()) {
            return false;
        }

        // same partition exprs
        if (!operator.getPartitionExpressions().containsAll(rankRelatedOperator.getPartitionExpressions()) ||
                !rankRelatedOperator.getPartitionExpressions().containsAll(operator.getPartitionExpressions())) {
            return false;
        }

        // if operator has window clause, we can't support this optimization.
        if (operator.getAnalyticWindow() != null) {
            return false;
        }

        // if operator has order by elements, we can't support this optimization.
        // because order by without window clause will have "Range between UNBOUNDED_PRECEDING and CURRENT_ROW"
        if (operator.getOrderByElements() != null && !operator.getOrderByElements().isEmpty()) {
            return false;
        }

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : operator.getWindowCall().entrySet()) {
            CallOperator callOperator = entry.getValue();
            AggregateFunction function = (AggregateFunction) callOperator.getFunction();
            // if any function is only window function, we can't support this optimization.
            if (!function.isAggregateFn()) {
                return false;
            }
            // if any function has at least two columns, we don't support this optimization.
            if (callOperator.getArguments().size() > 1) {
                return false;
            }
        }

        // same partition and without order by, so they are in same sort group and partition group, which means share the same sort Node and exchange Node
        Preconditions.checkState(operator.getEnforceSortColumns().equals(rankRelatedOperator.getEnforceSortColumns()));
        return true;
    }

    // This is similar to Splitting Aggregation into two phase Aggregation
    public static Pair<Map<ColumnRefOperator, CallOperator>, Map<ColumnRefOperator, CallOperator>> splitWindowCall(
            Map<ColumnRefOperator, CallOperator> windowCall, ColumnRefFactory columnFactory) {
        Map<ColumnRefOperator, CallOperator> globalCall = Maps.newHashMap();
        Map<ColumnRefOperator, CallOperator> localCall = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : windowCall.entrySet()) {
            ColumnRefOperator originalOutputColumnRefOp = entry.getKey();
            CallOperator aggregation = entry.getValue();

            Type intermediateType = getIntermediateType(aggregation);

            CallOperator localCallOperator =
                    new CallOperator(aggregation.getFnName(), intermediateType, aggregation.getChildren(),
                            aggregation.getFunction(), aggregation.isDistinct(), aggregation.isRemovedDistinct());

            // local output column always nullable
            // localOutputColumnRefOp must have new ColumnId, which is different from two phase agg
            ColumnRefOperator localOutputColumnRefOp =
                    columnFactory.create(originalOutputColumnRefOp, intermediateType,
                            originalOutputColumnRefOp.isNullable());
            localCall.put(localOutputColumnRefOp, localCallOperator);

            // because we only support one argument function, so code below is safe
            List<ScalarOperator> arguments = Lists.newArrayList(localOutputColumnRefOp);

            Function newFn = aggregation.getFunction().copy();
            CallOperator glocalCallOperator =
                    new CallOperator(aggregation.getFnName(), aggregation.getType(), arguments,
                            newFn);
            ColumnRefOperator globalColumnRefOp =
                    new ColumnRefOperator(originalOutputColumnRefOp.getId(), originalOutputColumnRefOp.getType(),
                            originalOutputColumnRefOp.getName(), originalOutputColumnRefOp.isNullable());
            globalCall.put(globalColumnRefOp, glocalCallOperator);
        }

        return Pair.create(globalCall, localCall);
    }

    private static Type getIntermediateType(CallOperator aggregation) {
        AggregateFunction af = (AggregateFunction) aggregation.getFunction();
        Preconditions.checkState(af != null);
        return af.getIntermediateType() == null ? af.getReturnType() : af.getIntermediateType();
    }
}
