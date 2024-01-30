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

import com.google.common.collect.Maps;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.LOW_AGGREGATE_EFFECT_COEFFICIENT;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT;

public class RewriteMultiDistinctRule extends TransformationRule {
    public RewriteMultiDistinctRule() {
        super(RuleType.TF_REWRITE_MULTI_DISTINCT,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                        OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();

        Optional<List<ColumnRefOperator>> distinctCols = Utils.extractCommonDistinctCols(agg.getAggregations().values());

        // all distinct function use the same distinct columns, we use the split rule to rewrite
        if (distinctCols.isPresent()) {
            return false;
        }

        return true;
    }

    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        if (useCteToRewrite(input, context)) {
            MultiDistinctByCTERewriter rewriter = new MultiDistinctByCTERewriter();
            return rewriter.transformImpl(input, context);
        } else {
            MultiDistinctByMultiFuncRewriter rewriter = new MultiDistinctByMultiFuncRewriter();
            return rewriter.transformImpl(input, context);
        }
    }

    private boolean useCteToRewrite(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        List<CallOperator> distinctAggOperatorList = agg.getAggregations().values().stream()
                .filter(CallOperator::isDistinct).collect(Collectors.toList());
        boolean hasMultiColumns = distinctAggOperatorList.stream().anyMatch(f -> f.getColumnRefs().size() > 1);
        // exist multiple distinct columns should use cte to rewrite
        if (hasMultiColumns) {
            if (!context.getSessionVariable().isCboCteReuse()) {
                throw new StarRocksPlannerException(ErrorType.USER_ERROR,
                        "%s is unsupported when cbo_cte_reuse is disabled", distinctAggOperatorList);
            }
            return true;
        }

        // exist unsupported two stage aggregate func should use cte to rewrite
        for (CallOperator distinctCall : distinctAggOperatorList) {
            List<ColumnRefOperator> distinctCols = distinctCall.getColumnRefs();
            if (!Utils.canGenerateTwoStageAggregate(distinctCall, distinctCols)) {
                return true;
            }
        }

        // respect prefer cte rewrite hint
        if (context.getSessionVariable().isCboCteReuse() && context.getSessionVariable().isPreferCTERewrite()) {
            return true;
        }

        // respect skew int
        if (agg.hasSkew() && !agg.getGroupingKeys().isEmpty()) {
            return true;
        }

        if (isCTEMoreEfficient(input, context, distinctAggOperatorList)) {
            return true;
        }

        return false;

    }

    private boolean isCTEMoreEfficient(OptExpression input, OptimizerContext context,
                                       List<CallOperator> distinctAggOperatorList) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        if (aggOp.hasLimit()) {
            return true;
        }

        calculateStatistics(input, context);

        Statistics inputStatistics = input.inputAt(0).getStatistics();
        Collection<ColumnStatistic> inputsColumnStatistics = inputStatistics.getColumnStatistics().values();
        if (inputsColumnStatistics.stream().anyMatch(ColumnStatistic::isUnknown) || !aggOp.hasLimit()) {
            return false;
        }

        double inputRowCount = inputStatistics.getOutputRowCount();
        double aggOutputRow = StatisticsCalculator.computeGroupByStatistics(aggOp.getGroupingKeys(), inputStatistics,
                Maps.newHashMap());


        if (aggOutputRow > aggOp.getLimit()) {
            return false;
        }

        boolean existHighCardinality = false;
        for (CallOperator callOperator : distinctAggOperatorList) {
            List<ColumnRefOperator> distinctColumns = callOperator.getColumnRefs();
            double distinctOutputRow = StatisticsCalculator.computeGroupByStatistics(distinctColumns, inputStatistics,
                    Maps.newHashMap());
            if (distinctOutputRow * MEDIUM_AGGREGATE_EFFECT_COEFFICIENT > inputRowCount) {
                existHighCardinality = true;
            }
        }

        // group by key with a low cardinality but distinct key with high cardinality use cte is more efficient
        if (aggOutputRow * LOW_AGGREGATE_EFFECT_COEFFICIENT < inputRowCount && existHighCardinality) {
            return true;
        }
        return false;
    }

    private void calculateStatistics(OptExpression expr, OptimizerContext context) {
        // Avoid repeated calculate
        if (expr.getStatistics() != null) {
            return;
        }

        for (OptExpression child : expr.getInputs()) {
            calculateStatistics(child, context);
        }

        ExpressionContext expressionContext = new ExpressionContext(expr);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                expressionContext, context.getColumnRefFactory(), context);
        statisticsCalculator.estimatorStats();
        expr.setStatistics(expressionContext.getStatistics());
    }
}
