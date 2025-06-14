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
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;

public class SplitTwoPhaseAggRule extends SplitAggregateRule {

    private SplitTwoPhaseAggRule() {
        super(RuleType.TF_SPLIT_TWO_PHASE_AGGREGATE);
    }

    private static final SplitTwoPhaseAggRule INSTANCE = new SplitTwoPhaseAggRule();

    public static SplitTwoPhaseAggRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (context.getSessionVariable().isMVPlanner()) {
            return false;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        if (agg.checkGroupByCountDistinctWithSkewHint()) {
            return false;
        }

        if (!Utils.couldGenerateMultiStageAggregate(input.getLogicalProperty(), input.getOp(), input.inputAt(0).getOp())) {
            return false;
        }

        return agg.getType().isGlobal() && !agg.isSplit() && agg.getDistinctColumnDataSkew() == null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {

        LogicalAggregationOperator aggOp = input.getOp().cast();
        Optional<List<ColumnRefOperator>> distinctCols = Utils.extractCommonDistinctCols(aggOp.getAggregations().values());
        if (distinctCols.isEmpty()) {
            throw new StarRocksPlannerException("The query contains multiple distinct agg functions, " +
                    "each can't have multi columns.", ErrorType.USER_ERROR);
        }

        if (!isSuitableForTwoStageDistinct(input, aggOp, distinctCols.get())) {
            return List.of();
        }

        Map<ColumnRefOperator, CallOperator> newAggMap = Maps.newHashMap(aggOp.getAggregations());

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            CallOperator aggregation = entry.getValue();
            if (aggregation.isDistinct()) {
                CallOperator call = rewriteDistinctAggFn(aggregation);
                newAggMap.put(entry.getKey(), call);
            }
        }

        long localAggLimit = Operator.DEFAULT_LIMIT;
        boolean isOnlyGroupBy = aggOp.getAggregations().isEmpty();
        if (isOnlyGroupBy && aggOp.getLimit() < context.getSessionVariable().cboPushDownDistinctLimit()) {
            localAggLimit = aggOp.getLimit();
        }

        LogicalAggregationOperator local = new LogicalAggregationOperator.Builder().withOperator(aggOp)
                .setType(AggType.LOCAL)
                .setAggregations(createNormalAgg(AggType.LOCAL, newAggMap))
                .setSplit()
                .setPredicate(null)
                .setLimit(localAggLimit)
                .setProjection(null)
                .build();
        OptExpression localOptExpression = OptExpression.create(local, input.getInputs());

        LogicalAggregationOperator global = new LogicalAggregationOperator.Builder().withOperator(aggOp)
                .setType(AggType.GLOBAL)
                .setAggregations(createNormalAgg(AggType.GLOBAL, newAggMap))
                .setSplit()
                .build();
        OptExpression globalOptExpression = OptExpression.create(global, localOptExpression);

        return Lists.newArrayList(globalOptExpression);
    }

    private CallOperator rewriteDistinctAggFn(CallOperator fnCall) {
        final String functionName = fnCall.getFnName();
        if (functionName.equalsIgnoreCase(FunctionSet.COUNT)) {
            return new CallOperator(FunctionSet.MULTI_DISTINCT_COUNT, fnCall.getType(), fnCall.getChildren(),
                    Expr.getBuiltinFunction(FunctionSet.MULTI_DISTINCT_COUNT, new Type[] {fnCall.getChild(0).getType()},
                            IS_NONSTRICT_SUPERTYPE_OF), false);
        } else if (functionName.equalsIgnoreCase(FunctionSet.SUM)) {
            Function multiDistinctSumFn = DecimalV3FunctionAnalyzer.convertSumToMultiDistinctSum(
                    fnCall.getFunction(), fnCall.getChild(0).getType());
            return new CallOperator(
                    FunctionSet.MULTI_DISTINCT_SUM, fnCall.getType(), fnCall.getChildren(), multiDistinctSumFn, false);
        } else if (functionName.equalsIgnoreCase(FunctionSet.ARRAY_AGG)) {
            if (fnCall.getUsedColumns().isEmpty() && fnCall.getChild(0).getType().isDecimalOfAnyVersion()) {
                return fnCall;
            } else {
                return new CallOperator(FunctionSet.ARRAY_AGG_DISTINCT, fnCall.getType(), fnCall.getChildren(),
                        Expr.getBuiltinFunction(FunctionSet.ARRAY_AGG_DISTINCT, new Type[] {fnCall.getChild(0).getType()},
                                IS_NONSTRICT_SUPERTYPE_OF), false);
            }

        } else if (functionName.equals(FunctionSet.GROUP_CONCAT)) {
            // all children of group_concat are constant
            return fnCall;
        } else if (functionName.equals(FunctionSet.AVG)) {
            // all children of avg are constant
            return new CallOperator(FunctionSet.AVG, fnCall.getType(), fnCall.getChildren(), fnCall.getFunction(), false);
        }
        throw new StarRocksPlannerException(ErrorType.INTERNAL_ERROR, "unsupported distinct agg functions: %s in two phase agg",
                fnCall);
    }
}
