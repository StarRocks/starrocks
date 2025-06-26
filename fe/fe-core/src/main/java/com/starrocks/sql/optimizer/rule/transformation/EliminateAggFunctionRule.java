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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// select max(a), min(a), any_value(a) from demo group by a;
// -> select a from demo group by a;
//
// select max(a), min(a), any_value(a) from demo group by a, b;
// -> select a from demo group by a, b;

public class EliminateAggFunctionRule extends TransformationRule {
    public EliminateAggFunctionRule() {
        super(RuleType.TF_ELIMINATE_AGG_FUNCTION, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private static final EliminateAggFunctionRule INSTANCE = new EliminateAggFunctionRule();

    public static EliminateAggFunctionRule getInstance() {
        return INSTANCE;
    }

    private static final Set<String> SUPPORTED_AGG_FUNCTIONS = ImmutableSet.of(
            FunctionSet.FIRST_VALUE, FunctionSet.LAST_VALUE, FunctionSet.ANY_VALUE,
            FunctionSet.MAX, FunctionSet.MIN
    );

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableEliminateAgg()) {
            return false;
        }

        LogicalAggregationOperator aggOp = input.getOp().cast();
        ColumnRefSet groupByColumns = new ColumnRefSet(aggOp.getGroupingKeys());
        for (CallOperator call : aggOp.getAggregations().values()) {
            if (!call.isDistinct() && SUPPORTED_AGG_FUNCTIONS.contains(call.getFnName()) &&
                    call.getChild(0).isColumnRef()) {
                if (groupByColumns.isIntersect(call.getUsedColumns())) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();

        ColumnRefSet groupByColumns = new ColumnRefSet(aggOp.getGroupingKeys());
        Map<ColumnRefOperator, ScalarOperator> newProjectMap = new HashMap<>();
        Map<ColumnRefOperator, CallOperator> newAggCallMap = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            ColumnRefOperator aggColumnRef = entry.getKey();
            CallOperator call = entry.getValue();
            if (!call.isDistinct() && SUPPORTED_AGG_FUNCTIONS.contains(call.getFnName()) &&
                    call.getChild(0).isColumnRef() && groupByColumns.isIntersect(call.getUsedColumns())) {
                newProjectMap.put(aggColumnRef, call.getChild(0).getColumnRefs().get(0));
            } else {
                newAggCallMap.put(aggColumnRef, call);
            }
        }

        for (ColumnRefOperator groupingKey : aggOp.getGroupingKeys()) {
            newProjectMap.put(groupingKey, groupingKey);
        }

        LogicalProjectOperator newProjectOp = LogicalProjectOperator.builder().setColumnRefMap(newProjectMap).build();

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(newProjectMap);
        final ScalarOperator newPredicate = rewriter.rewrite(aggOp.getPredicate());

        LogicalAggregationOperator newAggOp = LogicalAggregationOperator.builder()
                .withOperator(aggOp)
                .setAggregations(newAggCallMap)
                .setGroupingKeys(aggOp.getGroupingKeys())
                .setPartitionByColumns(aggOp.getPartitionByColumns())
                .setPredicate(newPredicate)
                .build();

        final OptExpression opt = OptExpression.create(newAggOp, input.inputAt(0));
        return List.of(OptExpression.create(newProjectOp, opt));
    }
}
