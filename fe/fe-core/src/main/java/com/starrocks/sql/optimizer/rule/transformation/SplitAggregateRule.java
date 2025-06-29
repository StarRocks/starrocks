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
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.starrocks.qe.SessionVariableConstants.AggregationStage.AUTO;
import static com.starrocks.qe.SessionVariableConstants.AggregationStage.TWO_STAGE;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.LOW_AGGREGATE_EFFECT_COEFFICIENT;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT;

public abstract class SplitAggregateRule extends TransformationRule {
    protected SplitAggregateRule(RuleType ruleType) {
        super(ruleType, Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF));
    }

    public Map<ColumnRefOperator, CallOperator> createNormalAgg(AggType aggType,
                                                                   Map<ColumnRefOperator, CallOperator> aggregationMap) {
        Map<ColumnRefOperator, CallOperator> newAggregationMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationMap.entrySet()) {
            ColumnRefOperator column = entry.getKey();
            CallOperator aggregation = entry.getValue();

            CallOperator callOperator;
            Type intermediateType = getIntermediateType(aggregation);
            // For merge agg function, we need to replace the agg input args to the update agg function result
            if (aggType.isGlobal()) {
                List<ScalarOperator> arguments =
                        Lists.newArrayList(new ColumnRefOperator(column.getId(), intermediateType, column.getName(),
                                column.isNullable()));
                appendConstantColumns(arguments, aggregation);
                callOperator = new CallOperator(
                        aggregation.getFnName(),
                        aggregation.getType(),
                        arguments,
                        aggregation.getFunction());
            } else {
                callOperator = new CallOperator(aggregation.getFnName(), intermediateType,
                        aggregation.getChildren(), aggregation.getFunction(),
                        aggregation.isDistinct(), aggregation.isRemovedDistinct());
            }

            newAggregationMap.put(
                    new ColumnRefOperator(column.getId(), column.getType(), column.getName(), column.isNullable()),
                    callOperator);
        }

        return newAggregationMap;
    }

    // For Multi args aggregation functions, if they have const args,
    // We should also pass the const args to merge phase aggregator for performance.
    // For example, for intersect_count(user_id, dt, '20191111'),
    // We should pass '20191111' to update and merge phase aggregator in BE both.
    protected static void appendConstantColumns(List<ScalarOperator> arguments, CallOperator aggregation) {
        if (aggregation.getChildren().size() > 1) {
            aggregation.getChildren().stream().filter(ScalarOperator::isConstant).forEach(arguments::add);
        }
    }

    protected Type getIntermediateType(CallOperator aggregation) {
        AggregateFunction af = (AggregateFunction) aggregation.getFunction();
        Preconditions.checkState(af != null);
        return af.getIntermediateType() == null ? af.getReturnType() : af.getIntermediateType();
    }

    protected boolean isSuitableForTwoStageDistinct(OptExpression input, LogicalAggregationOperator operator,
                                                  List<ColumnRefOperator> distinctColumns) {
        if (distinctColumns.isEmpty()) {
            return true;
        }
        int aggMode = ConnectContext.get().getSessionVariable().getNewPlannerAggStage();
        for (CallOperator callOperator : operator.getAggregations().values()) {
            if (callOperator.isDistinct() && !canGenerateTwoStageAggregate(callOperator)) {
                return false;
            }
        }

        if (aggMode == TWO_STAGE.ordinal()) {
            return true;
        }

        return CollectionUtils.isNotEmpty(operator.getGroupingKeys())
                && aggMode == AUTO.ordinal()
                && isTwoStageMoreEfficient(input, distinctColumns);
    }

    private boolean canGenerateTwoStageAggregate(CallOperator distinctCall) {
        List<ColumnRefOperator> distinctCols = distinctCall.getColumnRefs();
        List<ScalarOperator> children = distinctCall.getChildren();
        // 1. multiple cols distinct is not support two stage aggregate
        // 2. array type col is not support two stage aggregate
        if (distinctCols.size() > 1 || children.get(0).getType().isComplexType()) {
            return false;
        }

        // 3. group_concat distinct or avg distinct is not support two stage aggregate
        // 4. array_agg with order by clause or decimal distinct col is not support two stage aggregate
        String fnName = distinctCall.getFnName();
        if (FunctionSet.GROUP_CONCAT.equalsIgnoreCase(fnName) || FunctionSet.AVG.equalsIgnoreCase(fnName)) {
            return false;
        } else if (FunctionSet.ARRAY_AGG.equalsIgnoreCase(fnName)) {
            AggregateFunction aggregateFunction = (AggregateFunction) distinctCall.getFunction();
            if (CollectionUtils.isNotEmpty(aggregateFunction.getIsAscOrder()) ||
                    children.get(0).getType().isDecimalOfAnyVersion()) {
                return false;
            }
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
}
