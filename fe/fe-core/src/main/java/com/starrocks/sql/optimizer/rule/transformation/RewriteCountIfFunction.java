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
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

/**
 * For count_if(x) we will rewrite it to count(if(x, 1, null))
 */
public class RewriteCountIfFunction extends TransformationRule {

    public RewriteCountIfFunction() {
        super(RuleType.TF_REWRITE_COUNT_IF_RULE,
                Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();

        for (Map.Entry<ColumnRefOperator, CallOperator> aggregation :
                aggregationOperator.getAggregations().entrySet()) {
            CallOperator aggFunction = aggregation.getValue();

            if (aggFunction.isAggregate() && aggFunction.getFnName().equals(FunctionSet.COUNT_IF)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        Map<ColumnRefOperator, CallOperator> newAggMap = Maps.newHashMap();
        boolean changed = false;

        for (Map.Entry<ColumnRefOperator, CallOperator> entry :
                aggregationOperator.getAggregations().entrySet()) {
            CallOperator aggFunction = entry.getValue();

            if (aggFunction.isAggregate() && aggFunction.getFnName().equals(FunctionSet.COUNT_IF)) {
                ScalarOperator aggExpr = aggFunction.getArguments().get(0);

                // Args for the IF function
                List<ScalarOperator> args = Lists.newArrayList();
                args.add(aggExpr);
                args.add(ConstantOperator.createBigint(1));
                args.add(ConstantOperator.createNull(Type.BIGINT));

                Function ifFn = Expr.getBuiltinFunction(FunctionSet.IF,
                        new Type[] {aggExpr.getType(), Type.BIGINT, Type.BIGINT}, Function.CompareMode.IS_IDENTICAL);
                if (ifFn == null) {
                    newAggMap.put(entry.getKey(), entry.getValue());
                } else {
                    CallOperator ifCall = new CallOperator(FunctionSet.IF, aggExpr.getType(), args, ifFn);

                    Function countFn = Expr.getBuiltinFunction(FunctionSet.COUNT, new Type[] {Type.BIGINT},
                            Function.CompareMode.IS_IDENTICAL);
                    CallOperator countCall = new CallOperator(FunctionSet.COUNT, Type.BIGINT,
                            Lists.newArrayList(ifCall), countFn);

                    newAggMap.put(entry.getKey(), countCall);
                    changed = true;
                }
            } else {
                newAggMap.put(entry.getKey(), entry.getValue());
            }
        }

        if (!changed) {
            return Lists.newArrayList();
        }
        LogicalAggregationOperator newAggregation =
                new LogicalAggregationOperator(aggregationOperator.getType(), aggregationOperator.getGroupingKeys(), newAggMap);

        return Lists.newArrayList(OptExpression.create(newAggregation, input.getInputs()));
    }
}
