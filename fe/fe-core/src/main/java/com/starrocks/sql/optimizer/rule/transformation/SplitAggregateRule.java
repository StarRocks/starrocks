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
import com.starrocks.catalog.Type;
<<<<<<< HEAD
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
=======
>>>>>>> 16d958075c ([Refactor] refactor split agg rule  (#39556))
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
<<<<<<< HEAD
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
=======
>>>>>>> 16d958075c ([Refactor] refactor split agg rule  (#39556))
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

<<<<<<< HEAD
import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.LOW_AGGREGATE_EFFECT_COEFFICIENT;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.SMALL_SCALE_ROWS_LIMIT;

public class SplitAggregateRule extends TransformationRule {
    private SplitAggregateRule() {
        super(RuleType.TF_SPLIT_AGGREGATE, Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF));
    }

    private static final SplitAggregateRule INSTANCE = new SplitAggregateRule();

    private static final int AUTO_MODE = 0;

    private static final int ONE_STAGE = 1;

    private static final int TWO_STAGE = 2;

    private static final int THREE_STAGE = 3;

    private static final int FOUR_STAGE = 4;

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

    private boolean mustGenerateMultiStageAggregate(OptExpression input, List<CallOperator> distinctAggCallOperator) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        // 1 Must do two stage aggregate if child operator is LogicalRepeatOperator
        //   If the repeat node is used as the input node of the Exchange node.
        //   Will cause the node to be unable to confirm whether it is const during serialization
        //   (BE does this for efficiency reasons).
        //   Therefore, it is forcibly ensured that no one-stage aggregation nodes are generated
        //   on top of the repeat node.
        if (input.inputAt(0).getOp() instanceof LogicalRepeatOperator) {
            return true;
        }
        // 2 Must do multi stage aggregate when aggregate distinct function has array type
        if (aggregationOperator.getAggregations().values().stream().anyMatch(callOperator
                -> callOperator.getChildren().stream().anyMatch(c -> c.getType().isComplexType()) &&
                callOperator.isDistinct())) {
            return true;
        }
        // 3. Must generate three, four phase aggregate for distinct aggregate with multi columns
        boolean hasMultiColumns =
                distinctAggCallOperator.stream().anyMatch(callOperator -> callOperator.getChildren().size() > 1);
        if (distinctAggCallOperator.size() > 0 && hasMultiColumns) {
            return true;
        }
        return false;
    }

    // Note: This method logic must consistent with CostEstimator::needGenerateOneStageAggNode
    private boolean needGenerateMultiStageAggregate(OptExpression input, List<CallOperator> distinctAggCallOperator) {
        // 1. Must do one stage aggregate If the child contains limit,
        //    the aggregation must be a single node to ensure correctness.
        //    eg. select count(*) from (select * table limit 2) t
        if (input.inputAt(0).getOp().hasLimit()) {
            return false;
        }
        // 2. check if must generate multi stage aggregate.
        if (mustGenerateMultiStageAggregate(input, distinctAggCallOperator)) {
            return true;
        }
        // 3. Respect user hint
        int aggStage = ConnectContext.get().getSessionVariable().getNewPlannerAggStage();
        if (aggStage == ONE_STAGE) {
            return false;
        }
        // 4. If scan tablet sum leas than 1, do one phase aggregate is enough
        if (aggStage == AUTO_MODE && input.getLogicalProperty().oneTabletProperty().supportOneTabletOpt) {
            return false;
        }
        // Default, we could generate two stage aggregate
        return true;
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
        if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() == FOUR_STAGE) {
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
        if (!needGenerateMultiStageAggregate(input, distinctAggCallOperator)) {
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

        if (aggMode == TWO_STAGE) {
            return true;
        }

        return CollectionUtils.isNotEmpty(operator.getGroupingKeys())
                && aggMode == AUTO_MODE
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
=======
public abstract class SplitAggregateRule extends TransformationRule {
    protected SplitAggregateRule(RuleType ruleType) {
        super(ruleType, Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF));
    }

    protected Map<ColumnRefOperator, CallOperator> createNormalAgg(AggType aggType,
                                                                   Map<ColumnRefOperator, CallOperator> aggregationMap) {
>>>>>>> 16d958075c ([Refactor] refactor split agg rule  (#39556))
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
            aggregation.getChildren().stream().filter(ScalarOperator::isConstantRef).forEach(arguments::add);
        }
    }

    protected Type getIntermediateType(CallOperator aggregation) {
        AggregateFunction af = (AggregateFunction) aggregation.getFunction();
        Preconditions.checkState(af != null);
        return af.getIntermediateType() == null ? af.getReturnType() : af.getIntermediateType();
    }
}
