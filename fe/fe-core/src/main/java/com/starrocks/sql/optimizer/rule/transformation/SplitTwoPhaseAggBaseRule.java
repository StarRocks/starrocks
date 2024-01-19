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
import com.starrocks.qe.ConnectContext;
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
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;
import static com.starrocks.qe.SessionVariableConstants.AggregationStage.AUTO;
import static com.starrocks.qe.SessionVariableConstants.AggregationStage.TWO_STAGE;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.LOW_AGGREGATE_EFFECT_COEFFICIENT;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT;

public class SplitTwoPhaseAggBaseRule extends SplitAggregateBaseRule {

    private SplitTwoPhaseAggBaseRule() {
        super(RuleType.TF_SPLIT_WITHOUT_DISTINCT_AGGREGATE);
    }

    private static final SplitTwoPhaseAggBaseRule INSTANCE = new SplitTwoPhaseAggBaseRule();

    public static SplitTwoPhaseAggBaseRule getInstance() {
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
        Optional<List<ColumnRefOperator>> distinctCols = extractCommonDistinctCols(aggOp.getAggregations().values());
        if (distinctCols.isEmpty()) {
            throw new StarRocksPlannerException("The query contains multiple distinct agg functions, " +
                    "each can't have multi columns.", ErrorType.USER_ERROR);
        }

        if (CollectionUtils.isNotEmpty(distinctCols.get()) && !isSuitableForTwoStageDistinct(input, aggOp, distinctCols.get())) {
            return Lists.newArrayList();
        }

        Map<ColumnRefOperator, CallOperator> newAggMap = Maps.newHashMap(aggOp.getAggregations());

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            CallOperator aggregation = entry.getValue();
            if (aggregation.isDistinct()) {
                CallOperator call = rewriteDistinctAggFn(aggregation);
                newAggMap.put(entry.getKey(), call);
            }
        }

        LogicalAggregationOperator local = new LogicalAggregationOperator.Builder().withOperator(aggOp)
                .setType(AggType.LOCAL)
                .setAggregations(createNormalAgg(AggType.LOCAL, newAggMap))
                .setSplit()
                .setPredicate(null)
                .setLimit(Operator.DEFAULT_LIMIT)
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

    private boolean isSuitableForTwoStageDistinct(OptExpression input, LogicalAggregationOperator operator,
                                                  List<ColumnRefOperator> distinctColumns) {
        int aggMode = ConnectContext.get().getSessionVariable().getNewPlannerAggStage();
        boolean canTwoStage = canGenerateTwoStageAggregate(operator, distinctColumns);

        if (!canTwoStage) {
            return false;
        }

        if (aggMode == TWO_STAGE.ordinal()) {
            return true;
        }

        return CollectionUtils.isNotEmpty(operator.getGroupingKeys())
                && aggMode == AUTO.ordinal()
                && isTwoStageMoreEfficient(input, distinctColumns);
    }

    private boolean canGenerateTwoStageAggregate(LogicalAggregationOperator operator,
                                                 List<ColumnRefOperator> distinctColumns) {

        // 1. multiple cols distinct is not support two stage aggregate
        // 2. array type col is not support two stage aggregate
        // 3. group_concat distinct with columnRef is not support two stage aggregate
        if (distinctColumns.size() > 1 || distinctColumns.get(0).getType().isArrayType() ||
                operator.getAggregations().values().stream()
                        .anyMatch(e -> e.getFnName().equals(FunctionSet.GROUP_CONCAT))) {
            return false;
        }
        return true;
    }

    private boolean isTwoStageMoreEfficient(OptExpression input, List<ColumnRefOperator> distinctColumns) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        Statistics inputStatistics = input.getGroupExpression().inputAt(0).getStatistics();
        Collection<ColumnStatistic> inputsColumnStatistics = inputStatistics.getColumnStatistics().values();
        if (inputsColumnStatistics.stream().anyMatch(ColumnStatistic::isUnknown) || !aggOp.hasLimit()) {
            return false;
        }

        double inputRowCount = inputStatistics.getOutputRowCount();
        double aggOutputRow = StatisticsCalculator.computeGroupByStatistics(aggOp.getGroupingKeys(), inputStatistics,
                Maps.newHashMap());

        double distinctOutputRow = StatisticsCalculator.computeGroupByStatistics(distinctColumns, inputStatistics,
                Maps.newHashMap());

        // both group by key and distinct key cannot with high cardinality
        return aggOutputRow * MEDIUM_AGGREGATE_EFFECT_COEFFICIENT < inputRowCount
                && distinctOutputRow * LOW_AGGREGATE_EFFECT_COEFFICIENT < inputRowCount
                && aggOutputRow > aggOp.getLimit();
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
            return new CallOperator(FunctionSet.ARRAY_AGG_DISTINCT, fnCall.getType(), fnCall.getChildren(),
                    Expr.getBuiltinFunction(FunctionSet.ARRAY_AGG_DISTINCT, new Type[] {fnCall.getChild(0).getType()},
                            IS_NONSTRICT_SUPERTYPE_OF), false);
        } else if (functionName.equals(FunctionSet.GROUP_CONCAT)) {
            // all children of group_concat is constant
            return fnCall;
        }
        throw new StarRocksPlannerException(ErrorType.INTERNAL_ERROR, "unsupported distinct agg functions: %s in two phase agg",
                fnCall);
    }
}
