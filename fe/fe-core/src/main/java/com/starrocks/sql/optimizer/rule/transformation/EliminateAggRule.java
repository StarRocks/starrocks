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
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.UKFKConstraintsCollector;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private static final Set<String> SUPPORTED_AGG_FUNCTIONS = ImmutableSet.of(
            FunctionSet.SUM, FunctionSet.COUNT, FunctionSet.AVG, FunctionSet.FIRST_VALUE,
            FunctionSet.MAX, FunctionSet.MIN, FunctionSet.GROUP_CONCAT
    );

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableEliminateAgg()) {
            return false;
        }

        LogicalAggregationOperator aggOp = input.getOp().cast();
        OptExpression childOpt = input.inputAt(0);

        List<ColumnRefOperator> groupBys = aggOp.getGroupingKeys();
        if (groupBys.isEmpty()) {
            return false;
        }

        boolean supportedAllAggFunctions = aggOp.getAggregations().values().stream()
                .allMatch(call -> !call.isDistinct() && SUPPORTED_AGG_FUNCTIONS.contains(call.getFnName()));
        if (!supportedAllAggFunctions) {
            return false;
        }

        UKFKConstraintsCollector.collectColumnConstraintsForce(input);

        List<UKFKConstraints.UniqueConstraintWrapper> uniqueKeys = childOpt.getConstraints().getAggUniqueKeys();
        if (uniqueKeys.isEmpty()) {
            return false;
        }

        ColumnRefSet groupByIds = new ColumnRefSet();
        groupBys.stream().map(ColumnRefOperator::getId).forEach(groupByIds::union);
        return uniqueKeys.stream().anyMatch(constraint -> groupByIds.containsAll(constraint.ukColumnRefs));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();

        Map<ColumnRefOperator, ScalarOperator> newProjectMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            ColumnRefOperator aggColumnRef = entry.getKey();
            CallOperator callOperator = entry.getValue();
            ScalarOperator newOperator = handleAggregationFunction(callOperator.getFnName(), callOperator);
            newProjectMap.put(aggColumnRef, newOperator);
        }
        aggOp.getGroupingKeys().forEach(ref -> newProjectMap.put(ref, ref));
        LogicalProjectOperator newProjectOp = LogicalProjectOperator.builder().setColumnRefMap(newProjectMap).build();

        if (aggOp.getPredicate() != null) {
            return List.of(OptExpression.create(new LogicalFilterOperator(aggOp.getPredicate()),
                    OptExpression.create(newProjectOp, input.getInputs())));
        }

        return List.of(OptExpression.create(newProjectOp, input.inputAt(0)));
    }

    private ScalarOperator handleAggregationFunction(String fnName, CallOperator callOperator) {
        if (fnName.equals(FunctionSet.COUNT)) {
            return rewriteCountFunction(callOperator);
        } else if (fnName.equals(FunctionSet.SUM) || fnName.equals(FunctionSet.AVG) ||
                fnName.equals(FunctionSet.FIRST_VALUE) || fnName.equals(FunctionSet.MAX) ||
                fnName.equals(FunctionSet.MIN) || fnName.equals(FunctionSet.GROUP_CONCAT)) {
            return rewriteNormalFunction(callOperator);
        }
        return callOperator;
    }

    private ScalarOperator rewriteCountFunction(CallOperator callOperator) {
        Type outType = callOperator.getType();

        if (callOperator.getArguments().isEmpty()) {
            return rewriteCastFunction(outType, ConstantOperator.createInt(1));
        }

        IsNullPredicateOperator isNullPredicateOperator =
                new IsNullPredicateOperator(callOperator.getArguments().get(0));
        ArrayList<ScalarOperator> ifArgs = Lists.newArrayList();
        ScalarOperator thenExpr = rewriteCastFunction(outType, ConstantOperator.createInt(0));
        ScalarOperator elseExpr = rewriteCastFunction(outType, ConstantOperator.createInt(1));
        ifArgs.add(isNullPredicateOperator);
        ifArgs.add(thenExpr);
        ifArgs.add(elseExpr);

        Type[] argumentTypes = ifArgs.stream().map(ScalarOperator::getType).toArray(Type[]::new);
        Function fn =
                Expr.getBuiltinFunction(FunctionSet.IF, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(FunctionSet.IF, outType, ifArgs, fn);
    }

    private ScalarOperator rewriteNormalFunction(CallOperator callOperator) {
        ScalarOperator argument = callOperator.getArguments().get(0);
        return rewriteCastFunction(callOperator.getType(), argument);
    }

    private ScalarOperator rewriteCastFunction(Type outType, ScalarOperator func) {
        if (outType.equals(func.getType())) {
            return func;
        }
        return new CastOperator(outType, func);
    }

}
