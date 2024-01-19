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
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.qe.SessionVariableConstants.AggregationStage.FOUR_STAGE;
import static com.starrocks.qe.SessionVariableConstants.AggregationStage.THREE_STAGE;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.LOW_AGGREGATE_EFFECT_COEFFICIENT;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.SMALL_SCALE_ROWS_LIMIT;

public class SplitMultiPhaseAggBaseRule extends SplitAggregateBaseRule {

    private SplitMultiPhaseAggBaseRule() {
        super(RuleType.TF_SPLIT_DISTINCT_AGGREGATE);
    }

    private static final SplitMultiPhaseAggBaseRule INSTANCE = new SplitMultiPhaseAggBaseRule();

    public static SplitMultiPhaseAggBaseRule getInstance() {
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

        if (agg.getAggregations().values().stream().noneMatch(CallOperator::isDistinct)) {
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

        if (aggOp.getGroupingKeys().isEmpty()) {
            return implementDistinctWithoutGroupByAgg(context.getColumnRefFactory(),
                    input, aggOp, distinctCols.get());
        } else if (isGroupByAllConstant(input, aggOp)) {
            return implementDistinctWithConstantGroupByAgg(context.getColumnRefFactory(),
                    input, aggOp, distinctCols.get(), aggOp.getGroupingKeys());
        } else {
            return implementDistinctWithGroupByAgg(context.getColumnRefFactory(), input, aggOp);
        }

    }

    private List<OptExpression> implementDistinctWithoutGroupByAgg(
            ColumnRefFactory columnRefFactory,
            OptExpression input,
            LogicalAggregationOperator oldAgg,
            List<ColumnRefOperator> distinctAggColumns) {
        List<ColumnRefOperator> partitionColumns = distinctAggColumns;

        LogicalAggregationOperator local = createDistinctAggForFirstPhase(
                columnRefFactory,
                Lists.newArrayList(), oldAgg.getAggregations(), AggType.LOCAL);
        local.setPartitionByColumns(partitionColumns);
        OptExpression localOptExpression = OptExpression.create(local, input.getInputs());

        LogicalAggregationOperator distinctGlobal = createDistinctAggForFirstPhase(
                columnRefFactory,
                Lists.newArrayList(), oldAgg.getAggregations(), AggType.DISTINCT_GLOBAL);
        OptExpression distinctGlobalExpression = OptExpression.create(distinctGlobal, localOptExpression);

        LogicalAggregationOperator distinctLocal = new LogicalAggregationOperator.Builder()
                .withOperator(oldAgg)
                .setType(AggType.DISTINCT_LOCAL)
                .setAggregations(createDistinctAggForSecondPhase(AggType.DISTINCT_LOCAL, oldAgg.getAggregations()))
                .setGroupingKeys(Lists.newArrayList())
                .setPartitionByColumns(partitionColumns)
                .setPredicate(null)
                .setLimit(Operator.DEFAULT_LIMIT)
                .setProjection(null)
                .build();
        OptExpression distinctLocalExpression = OptExpression.create(distinctLocal, distinctGlobalExpression);

        // in DISTINCT_LOCAL phase, may rewrite the aggregate function, we use the rewrite function in GLOBAL phase
        Map<ColumnRefOperator, CallOperator> reRewriteAggregate = Maps.newHashMap(oldAgg.getAggregations());
        Map<ColumnRefOperator, CallOperator> countDistinctMap = oldAgg.getAggregations().entrySet().stream().
                filter(entry -> entry.getValue().isDistinct() &&
                        entry.getValue().getFnName().equalsIgnoreCase(FunctionSet.COUNT)).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!countDistinctMap.isEmpty()) {
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : countDistinctMap.entrySet()) {
                reRewriteAggregate.put(entry.getKey(), distinctLocal.getAggregations().get(entry.getKey()));
            }
        }

        LogicalAggregationOperator.Builder builder = new LogicalAggregationOperator.Builder();
        LogicalAggregationOperator global = builder.withOperator(oldAgg)
                .setType(AggType.GLOBAL)
                .setGroupingKeys(Lists.newArrayList())
                .setAggregations(createNormalAgg(AggType.GLOBAL, reRewriteAggregate))
                .setSplit()
                .build();

        OptExpression globalOptExpression = OptExpression.create(global, distinctLocalExpression);
        return Lists.newArrayList(globalOptExpression);
    }

    private List<OptExpression> implementDistinctWithConstantGroupByAgg(
            ColumnRefFactory columnRefFactory,
            OptExpression input,
            LogicalAggregationOperator oldAgg,
            List<ColumnRefOperator> distinctAggColumns,
            List<ColumnRefOperator> groupByColumns) {
        List<ColumnRefOperator> partitionColumns = distinctAggColumns;

        LogicalAggregationOperator local = createDistinctAggForFirstPhase(
                columnRefFactory,
                Lists.newArrayList(), oldAgg.getAggregations(), AggType.LOCAL);
        local.setPartitionByColumns(partitionColumns);
        OptExpression localOptExpression = OptExpression.create(local, input.getInputs());

        LogicalAggregationOperator distinctGlobal = createDistinctAggForFirstPhase(
                columnRefFactory, Lists.newArrayList(), oldAgg.getAggregations(), AggType.DISTINCT_GLOBAL);
        distinctGlobal.setPartitionByColumns(partitionColumns);

        // build projection for DISTINCT_GLOBAL aggregate node
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        distinctGlobal.getGroupingKeys().forEach(k -> columnRefMap.put(k, k));
        distinctGlobal.getAggregations().keySet().forEach(key -> columnRefMap.put(key, key));

        Projection childProjection = input.getInputs().get(0).getOp().getProjection();
        if (childProjection != null) {
            childProjection.getColumnRefMap().entrySet().stream()
                    .filter(entry -> groupByColumns.contains(entry.getKey())).forEach(entry -> columnRefMap.put(
                            entry.getKey(), entry.getValue()));
        }

        LogicalAggregationOperator distinctGlobalWithProjection = new LogicalAggregationOperator.Builder()
                .withOperator(distinctGlobal)
                .setProjection(new Projection(columnRefMap))
                .build();

        OptExpression distinctGlobalExpression = OptExpression.create(distinctGlobalWithProjection, localOptExpression);

        LogicalAggregationOperator distinctLocal = new LogicalAggregationOperator.Builder()
                .withOperator(oldAgg)
                .setType(AggType.DISTINCT_LOCAL)
                .setAggregations(createDistinctAggForSecondPhase(AggType.DISTINCT_LOCAL, oldAgg.getAggregations()))
                .setGroupingKeys(groupByColumns)
                .setPartitionByColumns(partitionColumns)
                .setPredicate(null)
                .setLimit(Operator.DEFAULT_LIMIT)
                .setProjection(null)
                .build();
        OptExpression distinctLocalExpression = OptExpression.create(distinctLocal, distinctGlobalExpression);

        // in DISTINCT_LOCAL phase, may rewrite the aggregate function, we use the rewrite function in GLOBAL phase
        Map<ColumnRefOperator, CallOperator> reRewriteAggregate = Maps.newHashMap(oldAgg.getAggregations());
        Map<ColumnRefOperator, CallOperator> countDistinctMap = oldAgg.getAggregations().entrySet().stream().
                filter(entry -> entry.getValue().isDistinct() &&
                        entry.getValue().getFnName().equalsIgnoreCase(FunctionSet.COUNT)).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!countDistinctMap.isEmpty()) {
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : countDistinctMap.entrySet()) {
                reRewriteAggregate.put(entry.getKey(), distinctLocal.getAggregations().get(entry.getKey()));
            }
        }

        LogicalAggregationOperator.Builder builder = new LogicalAggregationOperator.Builder();
        LogicalAggregationOperator global = builder.withOperator(oldAgg)
                .setType(AggType.GLOBAL)
                .setGroupingKeys(groupByColumns)
                .setPartitionByColumns(Lists.newArrayList())
                .setAggregations(createNormalAgg(AggType.GLOBAL, reRewriteAggregate))
                .setSplit()
                .build();

        OptExpression globalOptExpression = OptExpression.create(global, distinctLocalExpression);
        return Lists.newArrayList(globalOptExpression);
    }

    // For SQL: select count(distinct id_bigint) from test_basic group by id_int;
    // Local Agg -> Distinct global Agg -> Global Agg
    private List<OptExpression> implementDistinctWithGroupByAgg(
            ColumnRefFactory columnRefFactory,
            OptExpression input,
            LogicalAggregationOperator oldAgg) {

        LogicalAggregationOperator local = createDistinctAggForFirstPhase(
                columnRefFactory,
                oldAgg.getGroupingKeys(), oldAgg.getAggregations(), AggType.LOCAL);
        local.setPartitionByColumns(oldAgg.getGroupingKeys());
        OptExpression localOptExpression = OptExpression.create(local, input.getInputs());

        LogicalAggregationOperator distinctGlobal = createDistinctAggForFirstPhase(
                columnRefFactory,
                oldAgg.getGroupingKeys(), oldAgg.getAggregations(), AggType.DISTINCT_GLOBAL);
        List<ColumnRefOperator> partitionByCols;

        boolean shouldFurtherSplit = false;
        if (isThreeStageMoreEfficient(input, distinctGlobal.getGroupingKeys(), local.getPartitionByColumns())
                || oldAgg.getGroupingKeys().containsAll(distinctGlobal.getGroupingKeys())) {
            partitionByCols = oldAgg.getGroupingKeys();
        } else {
            partitionByCols = distinctGlobal.getGroupingKeys();
            // use grouping keys and distinct cols to distribute data, we need to continue split the global agg.
            shouldFurtherSplit = true;
        }

        distinctGlobal.setPartitionByColumns(partitionByCols);
        OptExpression distinctGlobalOptExpression = OptExpression.create(distinctGlobal, localOptExpression);

        LogicalAggregationOperator.Builder aggBuilder = new LogicalAggregationOperator.Builder().withOperator(oldAgg)
                .setType(AggType.GLOBAL)
                .setAggregations(createDistinctAggForSecondPhase(AggType.GLOBAL, oldAgg.getAggregations()));
        if (!shouldFurtherSplit) {
            // set isSplit = true to avoid split the global agg
            aggBuilder.setSplit();
        }

        LogicalAggregationOperator global = aggBuilder.build();
        OptExpression globalOptExpression = OptExpression.create(global, distinctGlobalOptExpression);

        return Lists.newArrayList(globalOptExpression);
    }

    private LogicalAggregationOperator createDistinctAggForFirstPhase(
            ColumnRefFactory columnRefFactory,
            List<ColumnRefOperator> groupKeys,
            Map<ColumnRefOperator, CallOperator> aggregationMap,
            AggType type) {
        Set<ColumnRefOperator> newGroupKeys = Sets.newHashSet(groupKeys);

        Map<ColumnRefOperator, CallOperator> localAggMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationMap.entrySet()) {
            ColumnRefOperator column = entry.getKey();
            CallOperator aggregation = entry.getValue();

            // Add distinct column to group by column
            if (aggregation.isDistinct()) {
                for (int i = 0; i < aggregation.getUsedColumns().cardinality(); ++i) {
                    newGroupKeys.add(columnRefFactory.getColumnRef(aggregation.getUsedColumns().getColumnIds()[i]));
                }
                continue;
            }

            Type intermediateType = getIntermediateType(aggregation);
            CallOperator callOperator;
            if (!type.isLocal()) {
                List<ScalarOperator> arguments =
                        Lists.newArrayList(
                                new ColumnRefOperator(column.getId(), aggregation.getType(), column.getName(),
                                        aggregation.isNullable()));
                appendConstantColumns(arguments, aggregation);

                callOperator = new CallOperator(
                        aggregation.getFnName(),
                        aggregation.getType(),
                        arguments,
                        aggregation.getFunction());
            } else {
                callOperator = new CallOperator(aggregation.getFnName(), intermediateType,
                        aggregation.getChildren(), aggregation.getFunction());
            }

            localAggMap
                    .put(new ColumnRefOperator(column.getId(), intermediateType, column.getName(), column.isNullable()),
                            callOperator);
        }

        return new LogicalAggregationOperator(type, Lists.newArrayList(newGroupKeys), localAggMap);
    }

    // The phase concept please refer to AggregateInfo::AggPhase
    private Map<ColumnRefOperator, CallOperator> createDistinctAggForSecondPhase(
            AggType aggType, Map<ColumnRefOperator, CallOperator> aggregationMap) {
        Map<ColumnRefOperator, CallOperator> newAggregationMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationMap.entrySet()) {
            ColumnRefOperator column = entry.getKey();
            CallOperator aggregation = entry.getValue();
            CallOperator callOperator;
            Type intermediateType = getIntermediateType(aggregation);
            if (!aggregation.isDistinct()) {
                List<ScalarOperator> arguments =
                        Lists.newArrayList(new ColumnRefOperator(column.getId(), intermediateType, column.getName(),
                                aggregation.isNullable()));
                appendConstantColumns(arguments, aggregation);

                if (aggType.isGlobal()) {
                    callOperator = new CallOperator(
                            aggregation.getFnName(),
                            aggregation.getType(),
                            arguments,
                            aggregation.getFunction());
                } else {
                    callOperator = new CallOperator(
                            aggregation.getFnName(),
                            intermediateType,
                            arguments,
                            aggregation.getFunction());
                }
            } else {
                if (aggregation.getFnName().equalsIgnoreCase(FunctionSet.COUNT)) {
                    // COUNT(DISTINCT ...) -> COUNT(IF(IsNull(<agg slot 1>), NULL, IF(IsNull(<agg slot 2>), NULL, ...)))
                    // We need the nested IF to make sure that we do not count
                    // column-value combinations if any of the distinct columns are NULL.
                    // This behavior is consistent with MySQL.
                    ScalarOperator newChildren = createCountDistinctAggParam(aggregation.getChildren());
                    // because of the change of children, we need to get function again.
                    Function fn = Expr.getBuiltinFunction(FunctionSet.COUNT, new Type[] {newChildren.getType()},
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                    aggregation = new CallOperator(aggregation.getFnName(), aggregation.getType(),
                            Lists.newArrayList(newChildren), fn);
                }
                // Remove distinct
                callOperator = new CallOperator(aggregation.getFnName(), aggType.isAnyGlobal() ?
                        aggregation.getType() : intermediateType,
                        aggregation.getChildren(), aggregation.getFunction(), false, true);
            }
            newAggregationMap.put(column, callOperator);
        }
        return newAggregationMap;
    }

    private Type getIntermediateType(CallOperator aggregation) {
        AggregateFunction af = (AggregateFunction) aggregation.getFunction();
        Preconditions.checkState(af != null);
        return af.getIntermediateType() == null ? af.getReturnType() : af.getIntermediateType();
    }

    /**
     * Creates an IF function call that returns NULL if any of the children
     * at indexes [firstIdx, lastIdx] return NULL.
     * For example, the resulting IF function would like this for 3 scala operator:
     * IF(IsNull(child1), NULL, IF(IsNull(child2), NULL, child3))
     */
    private ScalarOperator createCountDistinctAggParam(List<ScalarOperator> children) {
        Preconditions.checkState(children.size() >= 1);
        if (children.size() == 1) {
            return children.get(0);
        }
        int firstIdx = 0;
        int lastIdx = children.size() - 1;

        ScalarOperator elseOperator = children.get(lastIdx);
        for (int i = lastIdx - 1; i >= firstIdx; --i) {
            ArrayList<ScalarOperator> ifArgs = Lists.newArrayList();
            ScalarOperator isNullColumn = children.get(i);
            // Build expr: IF(IsNull(slotRef), NULL, elseExpr)
            IsNullPredicateOperator isNullPredicateOperator = new IsNullPredicateOperator(isNullColumn);
            ifArgs.add(isNullPredicateOperator);
            ifArgs.add(ConstantOperator.createNull(elseOperator.getType()));
            ifArgs.add(elseOperator);
            Type[] argumentTypes = ifArgs.stream().map(arg -> arg.getType()).toArray(Type[]::new);

            Function fn = Expr.getBuiltinFunction(FunctionSet.IF, argumentTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            elseOperator = new CallOperator(FunctionSet.IF, fn.getReturnType(), ifArgs, fn);

        }
        return elseOperator;
    }

    private boolean isGroupByAllConstant(OptExpression input, LogicalAggregationOperator operator) {
        Map<ColumnRefOperator, ScalarOperator> rewriteProjectMap = Maps.newHashMap();
        Projection childProjection = input.getInputs().get(0).getOp().getProjection();
        if (childProjection != null) {
            rewriteProjectMap.putAll(childProjection.getColumnRefMap());
        }

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteProjectMap);
        List<ScalarOperator> groupingKeys = operator.getGroupingKeys().stream().map(rewriter::rewrite).
                collect(Collectors.toList());
        return groupingKeys.stream().allMatch(ScalarOperator::isConstant);
    }

    private boolean isThreeStageMoreEfficient(OptExpression input, List<ColumnRefOperator> groupKeys,
                                              List<ColumnRefOperator> partitionByColumns) {
        if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() == FOUR_STAGE.ordinal()) {
            return false;
        }
        if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() == THREE_STAGE.ordinal()) {
            return true;
        }

        Statistics inputStatistics = input.getGroupExpression().inputAt(0).getStatistics();
        Collection<ColumnStatistic> inputsColumnStatistics = inputStatistics.getColumnStatistics().values();

        // Estimate the NDV when use partitionBy columns to shuffle, if the NDV is small,
        // this may result in only a few nodes participating in subsequent calculations.
        // To take full advantage of all compute nodes, should select the four stages
        List<ColumnStatistic> partitionByColumnStatistics = partitionByColumns.stream().
                map(inputStatistics::getColumnStatistic).collect(Collectors.toList());
        if (partitionByColumnStatistics.stream().noneMatch(ColumnStatistic::isUnknown)) {
            Statistics statistics = inputStatistics;
            if (inputStatistics.getOutputRowCount() <= 1) {
                double rowCount = 1.0;
                for (ColumnStatistic columnStatistic : partitionByColumnStatistics) {
                    rowCount *= columnStatistic.getDistinctValuesCount();
                }
                statistics = Statistics.buildFrom(inputStatistics).setOutputRowCount(rowCount).build();
            }
            double aggOutputRow = StatisticsCalculator.computeGroupByStatistics(partitionByColumns, statistics,
                    Maps.newHashMap());
            if (aggOutputRow <= LOW_AGGREGATE_EFFECT_COEFFICIENT) {
                return false;
            }
        }

        if (inputsColumnStatistics.stream().anyMatch(ColumnStatistic::isUnknown)) {
            return true;
        }

        LogicalAggregationOperator aggOp = input.getOp().cast();

        double inputRowCount = inputStatistics.getOutputRowCount();
        double aggOutputRow = StatisticsCalculator.computeGroupByStatistics(aggOp.getGroupingKeys(), inputStatistics,
                Maps.newHashMap());

        double distinctOutputRow = StatisticsCalculator.computeGroupByStatistics(groupKeys, inputStatistics,
                Maps.newHashMap());

        return inputRowCount < SMALL_SCALE_ROWS_LIMIT
                || aggOutputRow > LOW_AGGREGATE_EFFECT_COEFFICIENT
                || distinctOutputRow * MEDIUM_AGGREGATE_EFFECT_COEFFICIENT < inputRowCount;
    }

}
