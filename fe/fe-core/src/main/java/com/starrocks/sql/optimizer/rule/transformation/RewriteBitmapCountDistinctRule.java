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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;

public class RewriteBitmapCountDistinctRule extends TransformationRule {
    public RewriteBitmapCountDistinctRule() {
        super(RuleType.TF_REWRITE_BITMAP_COUNT_DISTINCT,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                        OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();

        return aggregationOperator.getAggregations().values().stream().anyMatch(
                agg -> agg.isDistinct() &&
                        agg.getFunction().getFunctionName().getFunction().equals(FunctionSet.COUNT) &&
                        agg.getChildren().size() == 1 &&
                        agg.getChildren().get(0).getType().isBitmapType());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        Map<ColumnRefOperator, CallOperator> newAggMap = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, CallOperator> aggEntry : aggregationOperator.getAggregations().entrySet()) {
            CallOperator oldFunctionCall = aggEntry.getValue();
            if (oldFunctionCall.isDistinct() &&
                    oldFunctionCall.getFunction().getFunctionName().getFunction().equals(FunctionSet.COUNT) &&
                    oldFunctionCall.getChildren().size() == 1 &&
                    oldFunctionCall.getChildren().get(0).getType().isBitmapType()) {

                Function searchDesc = new Function(new FunctionName(FunctionSet.BITMAP_UNION_COUNT),
                        oldFunctionCall.getFunction().getArgs(), Type.INVALID, false);
                Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);

                CallOperator c = new CallOperator(FunctionSet.BITMAP_UNION_COUNT,
                        oldFunctionCall.getType(), oldFunctionCall.getChildren(), fn);
                newAggMap.put(aggEntry.getKey(), c);
            } else {
                newAggMap.put(aggEntry.getKey(), aggEntry.getValue());
            }
        }
        return Lists.newArrayList(OptExpression.create(
                new LogicalAggregationOperator(AggType.GLOBAL, aggregationOperator.getGroupingKeys(), newAggMap),
                input.getInputs()));
    }
}
