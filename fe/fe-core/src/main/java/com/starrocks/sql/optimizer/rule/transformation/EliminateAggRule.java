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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.UKFKConstraintsCollector;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// When a column used in a SQL query's Group By statement has a unique attribute, aggregation can be eliminated,
// and the LogicalAggregationOperator can be replaced with a LogicalProjectOperator.
//
// Pattern:
//
//      Agg
//       |
//     Child
//
// Transform:
//
//     Project
//       |
//     Child
//
//
// example:
// 1. select count(row1) from demo group by pk -> SELECT IF(row1 IS NULL, 0, 1) from demo;
// 2. select avg(row1) from demo group by pk -> SELECT row1 FROM demo;
//

public class EliminateAggRule extends TransformationRule {

    private EliminateAggRule() {
        super(RuleType.TF_ELIMINATE_AGG, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    public static EliminateAggRule getInstance() {
        return INSTANCE;
    }

    private static final EliminateAggRule INSTANCE = new EliminateAggRule();

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            String fnName = entry.getValue().getFnName();
            if (!(fnName.equals(FunctionSet.SUM) || fnName.equals(FunctionSet.COUNT) ||
                    fnName.equals(FunctionSet.AVG) ||
                    fnName.equals(FunctionSet.FIRST_VALUE) ||
                    fnName.equals(FunctionSet.MAX) || fnName.equals(FunctionSet.MIN) ||
                    fnName.equals(FunctionSet.GROUP_CONCAT))) {
                return false;
            }
        }

        // collect uk pk key
        UKFKConstraintsCollector collector = new UKFKConstraintsCollector();
        input.getOp().accept(collector, input, null);
        OptExpression childOptExpression = input.inputAt(0);

        if (aggOp.getGroupingKeys().isEmpty()) {
            return false;
        }

        for (ColumnRefOperator columnRefOperator : aggOp.getGroupingKeys()) {
            if (childOptExpression.getConstraints().getUniqueConstraint(columnRefOperator.getId()) == null) {
                return false;
            }
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap(aggOp.getAggregations());

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            String fnName = entry.getValue().getFnName();
            if (fnName.equals(FunctionSet.COUNT)) {
                for (Map.Entry<ColumnRefOperator, ScalarOperator> rewriteEntry : rewriteCountFunction(input)
                        .entrySet()) {
                    newProjectMap.put(rewriteEntry.getKey(), rewriteEntry.getValue());
                }
            } else if (fnName.equals(FunctionSet.SUM) || fnName.equals(FunctionSet.AVG) ||
                    fnName.equals(FunctionSet.FIRST_VALUE) ||
                    fnName.equals(FunctionSet.MAX) || fnName.equals(FunctionSet.MIN) ||
                    fnName.equals(FunctionSet.GROUP_CONCAT)) {
                for (Map.Entry<ColumnRefOperator, ScalarOperator> rewriteEntry : rewriteCastFunction(input)
                        .entrySet()) {
                    newProjectMap.put(rewriteEntry.getKey(), rewriteEntry.getValue());
                }
            }
        }

        LogicalProjectOperator newProjectOp = LogicalProjectOperator.builder().setColumnRefMap(newProjectMap).build();
        return List.of(OptExpression.create(newProjectOp, input.inputAt(0).getInputs()));
    }

    private Map<ColumnRefOperator, ScalarOperator> rewriteCountFunction(OptExpression input) {
        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        LogicalAggregationOperator aggOp = input.getOp().cast();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            IsNullPredicateOperator isNullPredicateOperator =
                    new IsNullPredicateOperator(entry.getValue().getArguments().get(0));
            ArrayList<ScalarOperator> ifArgs = Lists.newArrayList();
            ScalarOperator thenExpr = ConstantOperator.createInt(0);
            ScalarOperator elseExpr = ConstantOperator.createInt(1);
            ifArgs.add(isNullPredicateOperator);
            ifArgs.add(thenExpr);
            ifArgs.add(elseExpr);

            Type[] argumentTypes = ifArgs.stream().map(arg -> arg.getType()).toArray(Type[]::new);
            Function fn = Expr.getBuiltinFunction(FunctionSet.IF, argumentTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            CallOperator callOperator =
                    new CallOperator(FunctionSet.IF, ScalarType.createType(PrimitiveType.TINYINT), ifArgs, fn);
            newProjectMap.put(entry.getKey(), callOperator);
        }

        return newProjectMap;
    }

    private Map<ColumnRefOperator, ScalarOperator> rewriteCastFunction(OptExpression input) {
        Map<ColumnRefOperator, ScalarOperator> newProjectMap = Maps.newHashMap();
        LogicalAggregationOperator aggOp = input.getOp().cast();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            newProjectMap.put(entry.getKey(), entry.getValue().getArguments().get(0));
        }

        return newProjectMap;
    }

}
