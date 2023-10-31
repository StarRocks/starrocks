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
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
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
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;
import static com.starrocks.qe.SessionVariableConstants.AUTO_AGG;
import static com.starrocks.qe.SessionVariableConstants.FOUR_STAGE_AGG;
import static com.starrocks.qe.SessionVariableConstants.TWO_STAGE_AGG;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.LOW_AGGREGATE_EFFECT_COEFFICIENT;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.SMALL_SCALE_ROWS_LIMIT;

public class SplitAggregateRule extends TransformationRule {
    private SplitAggregateRule() {
        super(RuleType.TF_SPLIT_AGGREGATE, Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF));
    }

    private static final SplitAggregateRule INSTANCE = new SplitAggregateRule();

    public static SplitAggregateRule getInstance() {
        return INSTANCE;
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        // TODO(murphy) support multi-stage StreamAgg
        if (context.getSessionVariable().isMVPlanner()) {
            return false;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        if (agg.checkGroupByCountDistinctWithSkewHint()) {
            return false;
        }
        // Only apply this rule if the aggregate type is global and not split
        return agg.getType().isGlobal() && !agg.isSplit() && agg.getDistinctColumnDataSkew() == null;
    }

    // check if multi distinct functions used the same columns.
    private void checkDistinctAgg(List<CallOperator> distinctAggCallOperator) {
        List<ScalarOperator> distinctChild0 = distinctAggCallOperator.get(0).getChildren();
        for (int i = 1; i < distinctAggCallOperator.size(); ++i) {
            List<ScalarOperator> distinctChildI = distinctAggCallOperator.get(i).getChildren();
            if (!distinctChild0.equals(distinctChildI)) {
                throw new StarRocksPlannerException("The query contains multi count distinct or " +
                        "sum distinct, each can't have multi columns.", ErrorType.USER_ERROR);
            }
        }
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


    private boolean isThreeStageMoreEfficient(OptExpression input, List<ColumnRefOperator> groupKeys) {
        if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() == FOUR_STAGE_AGG) {
            return false;
        }

        Statistics inputStatistics = input.getGroupExpression().inputAt(0).getStatistics();
        Collection<ColumnStatistic> inputsColumnStatistics = inputStatistics.getColumnStatistics().values();
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

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator operator = (LogicalAggregationOperator) input.getOp();

        List<CallOperator> distinctAggCallOperator = operator.getAggregations().values().stream()
                .filter(CallOperator::isDistinct)
                // Don't need to do three stage or four stage aggregate for const column
                .filter(c -> !c.getUsedColumns().isEmpty())
                .collect(Collectors.toList());

        // no need to do multiple stage agg
        if (!Utils.needGenerateMultiStageAggregate(input.getLogicalProperty(), input.getOp(), input.inputAt(0).getOp())) {
            return Lists.newArrayList();
        }

        // do two stage agg if without distinct agg
        if (CollectionUtils.isEmpty(distinctAggCallOperator)) {
            return implementTwoStageAgg(input, operator);
        }

        // now the distinctAggCallOperator must contain at least one multiple column agg
        // we need ensure all the distinctAggCall contains same columns
        checkDistinctAgg(distinctAggCallOperator);

        // Get distinct columns and position in all aggregate functions
        List<ColumnRefOperator> distinctColumns = Lists.newArrayList();
        int singleDistinctFunctionPos = -1;
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : operator.getAggregations().entrySet()) {
            singleDistinctFunctionPos++;
            if (kv.getValue().isDistinct()) {
                distinctColumns = kv.getValue().getUsedColumns().getStream().
                        map(id -> context.getColumnRefFactory().getColumnRef(id)).collect(Collectors.toList());
                break;
            }
        }

        // if two stage agg is suitable, transform it to two stage agg
        if (isSuitableForTwoStage(input, operator, distinctColumns)) {
            return implementTwoStageAgg(input, operator);
        }

        if (!operator.getGroupingKeys().isEmpty()) {
            if (isGroupByAllConstant(input, operator)) {
                return implementOneDistinctWithConstantGroupByAgg(context.getColumnRefFactory(),
                        input, operator, distinctColumns, singleDistinctFunctionPos,
                        operator.getGroupingKeys());
            } else {
                return implementOneDistinctWithGroupByAgg(context.getColumnRefFactory(), input, operator,
                        singleDistinctFunctionPos);
            }
        } else {
            return implementOneDistinctWithoutGroupByAgg(context.getColumnRefFactory(),
                    input, operator, distinctColumns, singleDistinctFunctionPos);
        }
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

    private boolean isSuitableForTwoStage(OptExpression input, LogicalAggregationOperator operator,
                                          List<ColumnRefOperator> distinctColumns) {
        int aggMode = ConnectContext.get().getSessionVariable().getNewPlannerAggStage();
        boolean canTwoStage = canGenerateTwoStageAggregate(operator, distinctColumns);

        if (!canTwoStage) {
            return false;
        }

        if (aggMode == TWO_STAGE_AGG) {
            return true;
        }

        return CollectionUtils.isNotEmpty(operator.getGroupingKeys())
                && aggMode == AUTO_AGG
                && isTwoStageMoreEfficient(input, distinctColumns);
    }

    private boolean canGenerateTwoStageAggregate(LogicalAggregationOperator operator,
                                                 List<ColumnRefOperator> distinctColumns) {
        // Array type not support two stage distinct
        // Count Distinct with multi columns not support two stage aggregate
        if (distinctColumns.stream().anyMatch(column -> column.getType().isArrayType()) ||
                operator.getAggregations().values().stream().
                        anyMatch(callOperator -> callOperator.isDistinct() &&
                                callOperator.getChildren().size() > 1)) {
            return false;
        }
        return true;
    }

    private CallOperator rewriteDistinctAggFn(CallOperator fnCall) {
        final String functionName = fnCall.getFnName();
        if (functionName.equalsIgnoreCase(FunctionSet.COUNT)) {
            return new CallOperator(FunctionSet.MULTI_DISTINCT_COUNT, fnCall.getType(), fnCall.getChildren(),
                    Expr.getBuiltinFunction(FunctionSet.MULTI_DISTINCT_COUNT, new Type[] {fnCall.getChild(0).getType()},
                            IS_NONSTRICT_SUPERTYPE_OF), false);
        } else if (functionName.equals(FunctionSet.SUM)) {
            Function multiDistinctSumFn = DecimalV3FunctionAnalyzer.convertSumToMultiDistinctSum(
                    fnCall.getFunction(), fnCall.getChild(0).getType());
            return new CallOperator(
                    FunctionSet.MULTI_DISTINCT_SUM, fnCall.getType(), fnCall.getChildren(), multiDistinctSumFn, false);
        } else if (functionName.equals(FunctionSet.ARRAY_AGG)) {
            return new CallOperator(FunctionSet.ARRAY_AGG_DISTINCT, fnCall.getType(), fnCall.getChildren(),
                    Expr.getBuiltinFunction(FunctionSet.ARRAY_AGG_DISTINCT, new Type[] {fnCall.getChild(0).getType()},
                            IS_NONSTRICT_SUPERTYPE_OF), false);
        } else if (functionName.equals(FunctionSet.GROUP_CONCAT)) {
            // only support const inputs
            boolean allConst = fnCall.getChildren().stream().allMatch(ScalarOperator::isConstant);
            Preconditions.checkState(allConst, "can't rewrite to group_concat_distinct for non-const inputs");
            return fnCall;
        }
        return null;
    }

    // Local Agg -> Global Agg
    private List<OptExpression> implementTwoStageAgg(OptExpression input, LogicalAggregationOperator oldAgg) {
        Preconditions.checkState(oldAgg.getAggregations().values().stream()
                        .map(call -> call.isDistinct() ? 1 : 0).reduce(Integer::sum).orElse(0) <= 1,
                "There are multi count(distinct) function call, multi distinct rewrite error");
        Map<ColumnRefOperator, CallOperator> newAggMap = new HashMap<>(oldAgg.getAggregations());
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : oldAgg.getAggregations().entrySet()) {
            CallOperator aggregation = entry.getValue();
            // rewrite single distinct agg function
            if (aggregation.isDistinct()) {
                CallOperator call = rewriteDistinctAggFn(aggregation);
                Preconditions.checkState(call != null);
                newAggMap.put(entry.getKey(), call);
                break;
            }
        }

        LogicalAggregationOperator local = new LogicalAggregationOperator.Builder().withOperator(oldAgg)
                .setType(AggType.LOCAL)
                .setAggregations(createNormalAgg(AggType.LOCAL, newAggMap))
                .setSplit()
                .setPredicate(null)
                .setLimit(Operator.DEFAULT_LIMIT)
                .setProjection(null)
                .build();
        OptExpression localOptExpression = OptExpression.create(local, input.getInputs());

        LogicalAggregationOperator global = new LogicalAggregationOperator.Builder().withOperator(oldAgg)
                .setType(AggType.GLOBAL)
                .setAggregations(createNormalAgg(AggType.GLOBAL, newAggMap))
                .setSplit()
                // we have split the agg to two phase, hence clear the distinct pos
                .setSingleDistinctFunctionPos(-1)
                .build();
        OptExpression globalOptExpression = OptExpression.create(global, localOptExpression);

        return Lists.newArrayList(globalOptExpression);
    }

    // For SQL: select count(distinct id_bigint) from test_basic group by id_int;
    // Local Agg -> Distinct global Agg -> Global Agg
    private List<OptExpression> implementOneDistinctWithGroupByAgg(
            ColumnRefFactory columnRefFactory,
            OptExpression input,
            LogicalAggregationOperator oldAgg,
            int singleDistinctFunctionPos) {

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
        if (isThreeStageMoreEfficient(input, distinctGlobal.getGroupingKeys())
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
                .setAggregations(createDistinctAggForSecondPhase(AggType.GLOBAL, oldAgg.getAggregations()))
                .setSingleDistinctFunctionPos(singleDistinctFunctionPos);
        if (!shouldFurtherSplit) {
            // set isSplit = true to avoid split the global agg
            aggBuilder.setSplit();
        }

        LogicalAggregationOperator global = aggBuilder.build();
        OptExpression globalOptExpression = OptExpression.create(global, distinctGlobalOptExpression);

        return Lists.newArrayList(globalOptExpression);
    }

    private List<OptExpression> implementOneDistinctWithConstantGroupByAgg(
            ColumnRefFactory columnRefFactory,
            OptExpression input,
            LogicalAggregationOperator oldAgg,
            List<ColumnRefOperator> distinctAggColumns,
            int singleDistinctFunctionPos,
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
                .setSingleDistinctFunctionPos(singleDistinctFunctionPos)
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

    // For SQL: select count(distinct id_bigint), sum(id_int)from test_basic ;
    // Local Agg -> Distinct global Agg -> Distinct local Agg -> Global Agg
    private List<OptExpression> implementOneDistinctWithoutGroupByAgg(
            ColumnRefFactory columnRefFactory,
            OptExpression input,
            LogicalAggregationOperator oldAgg,
            List<ColumnRefOperator> distinctAggColumns,
            int singleDistinctFunctionPos) {
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
                .setSingleDistinctFunctionPos(singleDistinctFunctionPos)
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

    // For Multi args aggregation functions, if they have const args,
    // We should also pass the const args to merge phase aggregator for performance.
    // For example, for intersect_count(user_id, dt, '20191111'),
    // We should pass '20191111' to update and merge phase aggregator in BE both.
    private static void appendConstantColumns(List<ScalarOperator> arguments, CallOperator aggregation) {
        if (aggregation.getChildren().size() > 1) {
            aggregation.getChildren().stream().filter(ScalarOperator::isConstantRef).forEach(arguments::add);
        }
    }

    private Map<ColumnRefOperator, CallOperator> createNormalAgg(AggType aggType,
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
                        aggregation.isDistinct());
            }

            newAggregationMap.put(
                    new ColumnRefOperator(column.getId(), column.getType(), column.getName(), column.isNullable()),
                    callOperator);
        }

        return newAggregationMap;
    }

    private Type getIntermediateType(CallOperator aggregation) {
        AggregateFunction af = (AggregateFunction) aggregation.getFunction();
        Preconditions.checkState(af != null);
        return af.getIntermediateType() == null ? af.getReturnType() : af.getIntermediateType();
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
                        aggregation.getChildren(), aggregation.getFunction());
            }
            newAggregationMap.put(column, callOperator);
        }
        return newAggregationMap;
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

    // The phase concept please refer to AggregateInfo::AggPhase
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
}
