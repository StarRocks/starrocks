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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

        // any aggregate function is distinct and constant, we replace it to any_value
        if (isComplexConstantCountDistinct(input)) {
            return true;
        }

        Optional<List<ColumnRefOperator>> distinctCols = Utils.extractCommonDistinctCols(agg.getAggregations().values());

        // all distinct function use the same distinct columns, we use the split rule to rewrite
        return distinctCols.isEmpty();
    }

    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        if (isComplexConstantCountDistinct(input)) {
            return rewriteComplexConstantDistinct(input);
        }
        if (useCteToRewrite(input, context)) {
            MultiDistinctByCTERewriter rewriter = new MultiDistinctByCTERewriter();
            return rewriter.transformImpl(input, context);
        } else {
            MultiDistinctByMultiFuncRewriter rewriter = new MultiDistinctByMultiFuncRewriter();
            return rewriter.transformImpl(input, context);
        }
    }

    private boolean isComplexConstantCountDistinct(OptExpression input) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        // sr support count(distinct constant array/struct/map) in four-phase aggregate,
        // but doesn't support in two-phase aggregate, we replace it to any_value
        return agg.getAggregations().values().stream()
                .anyMatch(c -> c.isDistinct() && c.isConstant() && FunctionSet.COUNT.equalsIgnoreCase(c.getFnName()) &&
                        (c.getChildren().size() == 1) && c.getChild(0).getType().isComplexType());
    }

    private List<OptExpression> rewriteComplexConstantDistinct(OptExpression input) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();

        agg.getAggregations().forEach((k, v) -> {
            if (v.isDistinct() && v.isConstant() && FunctionSet.COUNT.equalsIgnoreCase(v.getFnName()) &&
                    (v.getChildren().size() == 1) && v.getChild(0).getType().isComplexType()) {
                Preconditions.checkState(v.getType() == Type.BIGINT);
                IsNullPredicateOperator isNull = new IsNullPredicateOperator(true, v.getChild(0));
                CastOperator cast = new CastOperator(Type.BIGINT, isNull);
                Function fn = Expr.getBuiltinFunction(FunctionSet.ANY_VALUE, new Type[] {Type.BIGINT},
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                CallOperator anyValue =
                        new CallOperator(FunctionSet.ANY_VALUE, v.getType(), Lists.newArrayList(cast), fn, false);
                newAggregations.put(k, anyValue);
            } else {
                newAggregations.put(k, v);
            }
        });

        LogicalAggregationOperator newAgg =
                LogicalAggregationOperator.builder().withOperator(agg).setAggregations(newAggregations).build();
        return Lists.newArrayList(OptExpression.create(newAgg, input.getInputs()));
    }

    private boolean useCteToRewrite(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        List<CallOperator> distinctAggOperatorList = agg.getAggregations().values().stream()
                .filter(CallOperator::isDistinct).collect(Collectors.toList());
        boolean hasMultiColumns = distinctAggOperatorList.stream().anyMatch(f -> f.getColumnRefs().size() > 1);
        // exist multiple distinct columns should enable cte use
        if (hasMultiColumns) {
            if (!context.getSessionVariable().isCboCteReuse()) {
                throw new StarRocksPlannerException(ErrorType.USER_ERROR,
                        "%s is unsupported when cbo_cte_reuse is disabled", distinctAggOperatorList);
            } else {
                return true;
            }
        }

        // respect prefer cte rewrite hint
        if (context.getSessionVariable().isCboCteReuse() && context.getSessionVariable().isPreferCTERewrite()) {
            return true;
        }

        // respect skew int
        if (context.getSessionVariable().isCboCteReuse() && agg.hasSkew() && !agg.getGroupingKeys().isEmpty()) {
            return true;
        }

        if (context.getSessionVariable().isCboCteReuse() &&
                isCTEMoreEfficient(input, context, distinctAggOperatorList)) {
            return true;
        }

        // all distinct one column function can be rewritten by multi distinct function
        boolean canRewriteByMultiFunc = true;
        for (CallOperator distinctCall : distinctAggOperatorList) {
            String fnName = distinctCall.getFnName();
            List<ScalarOperator> children = distinctCall.getChildren();
            Type type = children.get(0).getType();
            if (type.isComplexType()
                    || type.isJsonType()
                    || FunctionSet.GROUP_CONCAT.equalsIgnoreCase(fnName)
                    || (FunctionSet.ARRAY_AGG.equalsIgnoreCase(fnName) && type.isDecimalOfAnyVersion())) {
                canRewriteByMultiFunc = false;
                break;
            }
        }

        if (!context.getSessionVariable().isCboCteReuse() && !canRewriteByMultiFunc) {
            throw new StarRocksPlannerException(ErrorType.USER_ERROR,
                    "%s is unsupported when cbo_cte_reuse is disabled", distinctAggOperatorList);
        }

        return !canRewriteByMultiFunc;
    }

    private boolean isCTEMoreEfficient(OptExpression input, OptimizerContext context,
                                       List<CallOperator> distinctAggOperatorList) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        if (aggOp.hasLimit()) {
            return false;
        }
        Utils.calculateStatistics(input, context);

        Statistics inputStatistics = input.inputAt(0).getStatistics();
        // inputStatistics may be null if it's a cte consumer operator
        if (inputStatistics == null) {
            return false;
        }
        List<ColumnRefOperator> neededCols = Lists.newArrayList(aggOp.getGroupingKeys());
        distinctAggOperatorList.stream().forEach(e -> neededCols.addAll(e.getColumnRefs()));

        // no statistics available, use cte for no group by or group by only one col scenes to avoid bad case of multiple_func
        if (neededCols.stream().anyMatch(e -> inputStatistics.getColumnStatistics().get(e).isUnknown())) {
            return aggOp.getGroupingKeys().size() < 2;
        }

        double inputRowCount = inputStatistics.getOutputRowCount();
        List<Double> deduplicateOutputRows = Lists.newArrayList();
        List<Double> distinctValueCounts = Lists.newArrayList();
        for (CallOperator callOperator : distinctAggOperatorList) {
            List<ColumnRefOperator> distinctColumns = callOperator.getColumnRefs();
            if (distinctColumns.isEmpty()) {
                continue;
            }
            Set<ColumnRefOperator> deduplicateKeys = Sets.newHashSet();
            deduplicateKeys.addAll(aggOp.getGroupingKeys());
            deduplicateKeys.addAll(distinctColumns);
            deduplicateOutputRows.add(StatisticsCalculator.computeGroupByStatistics(Lists.newArrayList(deduplicateKeys),
                    inputStatistics, Maps.newHashMap()));
            distinctValueCounts.add(inputStatistics.getColumnStatistics().get(distinctColumns.get(0)).getDistinctValuesCount());
        }

        if (distinctValueCounts.stream().allMatch(d -> d < MEDIUM_AGGREGATE_EFFECT_COEFFICIENT)) {
            // distinct key with an extreme low cardinality use multi_distinct_func maybe more efficient
            return false;
        } else if (deduplicateOutputRows.stream().allMatch(row -> row * LOW_AGGREGATE_EFFECT_COEFFICIENT < inputRowCount)) {
            return false;
        }
        return true;
    }
}
