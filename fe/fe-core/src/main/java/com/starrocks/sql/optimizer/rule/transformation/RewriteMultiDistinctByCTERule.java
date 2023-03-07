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
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ImplicitCastRule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;

/*
 *
 * Optimize multi count distinct aggregate node without group by.
 * multi_count_distinct will gather to one instance work, doesn't take advantage of the MPP architecture.
 *
 * e.g:
 * Mark count(distinct v1) as cd(v1), avg(distinct v1) as ad(v1), sum(distinct v1) as sd(v1).
 *
 * Before:
 *          Agg[cd(v1), cd(v2), sum(v4), max(v5)]
 *                      |
 *                  Child Plan
 *
 * After:
 *                     CTEAnchor
 *                   /           \
 *           CTEProduce           Cross join
 *             /                 /          \
 *      Child Plan        Cross join       Agg[sum(v4), max(v5)]
 *                        /          \            \
 *                 Agg[cd(v1)]     Agg[cd(v2)]   CTEConsume
 *                     /               \
 *                CTEConsume        CTEConsume
 *
 *
 * Before:
 *          Agg[ad(v1), sum(v4), max(v5)]
 *                      |
 *                  Child Plan
 *
 * After:
 *                     CTEAnchor
 *                   /           \
 *           CTEProduce          project[ad(v1) = sd(v1) / cd(v1)]
 *                /                  \
 *           Child Plan           Cross join
 *                               /          \
 *                       Cross join       Agg[sum(v4), max(v5)]
 *                        /          \            \
 *                 Agg[cd(v1)]     Agg[sd(v1)]   CTEConsume
 *                     /               \
 *                CTEConsume        CTEConsume
 *
 * Before:
 *          Agg[cd(v1), cd(v2), sum(v4), max(v5)] group by v3
 *                      |
 *                  Child Plan
 *
 * After:
 *                     CTEAnchor
 *                   /            \
 *           CTEProduce         inner join (v3 <=> v3)
 *             /                 /                  \
 *      Child Plan       inner join(v3 <=> v3)     Agg[sum(v4), max(v5)]
 *                        /          \               group by v3
 *                       /            \                   \
 *                 Agg[cd(v1)]     Agg[cd(v2)]        CTEConsume
 *                 group by v3     group by v3
 *                     /               \
 *                CTEConsume        CTEConsume
 *
 *
 */
public class RewriteMultiDistinctByCTERule extends TransformationRule {
    private final ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();

    public RewriteMultiDistinctByCTERule() {
        super(RuleType.TF_REWRITE_MULTI_DISTINCT_WITHOUT_GROUP_BY,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                        OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // check cte is disabled or hasNoGroup false
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        if (!context.getSessionVariable().isCboCteReuse()) {
            return false;
        }

        if (agg.getLimit() >= 0) {
            return false;
        }

        List<CallOperator> distinctAggOperatorList = agg.getAggregations().values().stream()
                .filter(CallOperator::isDistinct).collect(Collectors.toList());
        boolean hasMultiColumns = distinctAggOperatorList.stream().anyMatch(f -> f.getChildren().size() > 1);

        if (!hasMultiColumns && agg.getGroupingKeys().size() > 1) {
            return false;
        }
        
        return distinctAggOperatorList.size() > 1 || agg.getAggregations().values().stream()
                .anyMatch(call -> call.isDistinct() && call.getFnName().equals(FunctionSet.AVG));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        // define cteId
        int cteId = columnRefFactory.getNextRelationId();

        // build logic cte produce operator
        OptExpression cteProduce = OptExpression.create(new LogicalCTEProduceOperator(cteId), input.getInputs());

        // generate all aggregation operator, split count distinct function and other function
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();

        List<ColumnRefOperator> distinctAggList = aggregate.getAggregations().entrySet().stream()
                .filter(kv -> kv.getValue().isDistinct()).map(Map.Entry::getKey).collect(Collectors.toList());
        List<ColumnRefOperator> otherAggregate = aggregate.getAggregations().entrySet().stream()
                .filter(kv -> !kv.getValue().isDistinct()).map(Map.Entry::getKey).collect(Collectors.toList());
        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        boolean hasGroupBy = !groupingKeys.isEmpty();

        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        LinkedList<OptExpression> allCteConsumes = buildDistinctAggCTEConsume(aggregate, distinctAggList, cteProduce,
                columnRefFactory, columnRefMap);
        // For inner join(with group by), distinct aggregate cannot have limitation
        if (hasGroupBy) {
            allCteConsumes.forEach(opt -> opt.getOp().setLimit(Operator.DEFAULT_LIMIT));
        }

        if (otherAggregate.size() > 0) {
            allCteConsumes.offer(
                    buildOtherAggregateCTEConsume(otherAggregate, aggregate, cteProduce, columnRefFactory,
                            columnRefMap));
        }

        // left deep join tree
        while (allCteConsumes.size() > 1) {
            OptExpression left = allCteConsumes.poll();
            OptExpression right = allCteConsumes.poll();
            OptExpression join;
            if (!hasGroupBy) {
                join = OptExpression.create(
                        new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null, JoinOperator.HINT_UNREORDER),
                        left, right);
            } else {
                // create inner join when aggregate has group by keys
                join = buildInnerJoin(left, right);
                // Add project map for group keys.
                LogicalJoinOperator joinOperator = (LogicalJoinOperator) join.getOp();
                List<ColumnRefOperator> joinOnPredicateColumns = getJoinOnPredicateColumn(joinOperator);
                Preconditions.checkState(groupingKeys.size() == joinOnPredicateColumns.size());

                for (int index = 0; index < groupingKeys.size(); ++index) {
                    columnRefMap.put(groupingKeys.get(index), joinOnPredicateColumns.get(index));
                }
            }
            allCteConsumes.offerFirst(join);
        }
        // Add project node
        LogicalProjectOperator.Builder builder = new LogicalProjectOperator.Builder();
        builder.setColumnRefMap(columnRefMap);

        OptExpression rightTree = OptExpression.create(builder.build(), allCteConsumes.get(0));

        // Add filter node
        if (aggregate.getPredicate() != null) {
            rightTree = OptExpression.create(new LogicalFilterOperator(aggregate.getPredicate()), rightTree);
        }

        context.getCteContext().addForceCTE(cteId);

        LogicalCTEAnchorOperator cteAnchor = new LogicalCTEAnchorOperator(cteId);
        return Lists.newArrayList(OptExpression.create(cteAnchor, cteProduce, rightTree));
    }

    private OptExpression buildInnerJoin(OptExpression left, OptExpression right) {
        // Get on predicate columns from left and right children.
        List<ColumnRefOperator> onPredicateLeftColumns = getJoinOnPredicateColumn(left.getOp());
        List<ColumnRefOperator> onPredicateRightColumns = getJoinOnPredicateColumn(right.getOp());
        Preconditions.checkState(onPredicateLeftColumns.size() == onPredicateRightColumns.size());

        List<ScalarOperator> onPredicateList = Lists.newArrayList();
        for (int index = 0; index < onPredicateLeftColumns.size(); ++index) {
            // Build on predicate for new inner join.
            ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                    onPredicateLeftColumns.get(index), onPredicateRightColumns.get(index));
            onPredicateList.add(onPredicate);
        }
        return OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundAnd(onPredicateList), JoinOperator.HINT_UNREORDER), left, right);
    }

    private List<ColumnRefOperator> getJoinOnPredicateColumn(Operator operator) {
        List<ColumnRefOperator> onPredicateColumns;
        if (operator instanceof LogicalAggregationOperator) {
            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) operator;
            onPredicateColumns = aggregationOperator.getGroupingKeys();
        } else {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            onPredicateColumns = Utils.extractConjuncts(joinOperator.getOnPredicate()).stream().
                    map(predicate -> (ColumnRefOperator) predicate.getChild(0)).collect(Collectors.toList());
        }
        return onPredicateColumns;
    }

    private LinkedList<OptExpression> buildDistinctAggCTEConsume(LogicalAggregationOperator aggregate,
                                                                 List<ColumnRefOperator> distinctAggList,
                                                                 OptExpression cteProduce,
                                                                 ColumnRefFactory factory,
                                                                 Map<ColumnRefOperator, ScalarOperator> projectionMap) {
        LinkedList<OptExpression> allCteConsumes = Lists.newLinkedList();
        Map<CallOperator, ColumnRefOperator> consumeAggCallMap = Maps.newHashMap();
        for (ColumnRefOperator distinctAggRef : distinctAggList) {
            CallOperator aggCallOperator = aggregate.getAggregations().get(distinctAggRef);
            if (aggCallOperator.getFnName().equalsIgnoreCase(FunctionSet.COUNT) ||
                    aggCallOperator.getFnName().equalsIgnoreCase(FunctionSet.SUM)) {
                allCteConsumes.offer(buildCountSumDistinctCTEConsume(distinctAggRef, aggCallOperator,
                        aggregate, cteProduce, factory));
                consumeAggCallMap.put(aggCallOperator, distinctAggRef);
                projectionMap.put(distinctAggRef, distinctAggRef);
            }
        }

        // Deal with avg(distinct xx) function, because avg needs to compute count and sum, there can use the aggregate
        // node with sum/count function directly if these aggregate node has generated before.
        Map<ColumnRefOperator, CallOperator> distinctAvgAggregate = aggregate.getAggregations().entrySet().stream().
                filter(kv -> kv.getValue().getFnName().equalsIgnoreCase(FunctionSet.AVG)).collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue));
        if (!distinctAvgAggregate.isEmpty()) {
            for (Map.Entry<ColumnRefOperator, CallOperator> aggregation : distinctAvgAggregate.entrySet()) {
                CallOperator avgCallOperator = aggregation.getValue();
                // compute the sum and count operator needed by avg operator
                CallOperator sumOperator = buildDistinctAggCall(avgCallOperator, FunctionSet.SUM);
                CallOperator countOperator = buildDistinctAggCall(avgCallOperator, FunctionSet.COUNT);

                ColumnRefOperator sumColumnRef = consumeAggCallMap.get(sumOperator);
                ColumnRefOperator countColumnRef = consumeAggCallMap.get(countOperator);
                // If distinct sum and count not computed, there need to build one
                if (sumColumnRef == null) {
                    sumColumnRef = factory.create(sumOperator, sumOperator.getType(), sumOperator.isNullable());
                    allCteConsumes.offer(buildCountSumDistinctCTEConsume(sumColumnRef, sumOperator, aggregate,
                            cteProduce, factory));
                }
                if (countColumnRef == null) {
                    countColumnRef = factory.create(countOperator, countOperator.getType(), countOperator.isNullable());
                    allCteConsumes.offer(buildCountSumDistinctCTEConsume(countColumnRef, countOperator, aggregate,
                            cteProduce, factory));
                }

                CallOperator distinctAvgCallOperator = new CallOperator(FunctionSet.DIVIDE, avgCallOperator.getType(),
                        Lists.newArrayList(sumColumnRef, countColumnRef));
                if (avgCallOperator.getType().isDecimalV3()) {
                    // There is not need to apply ImplicitCastRule to divide operator of decimal types.
                    // but we should cast BIGINT-typed countColRef into DECIMAL(38,0).
                    ScalarType decimal128p38s0 = ScalarType.createDecimalV3NarrowestType(38, 0);
                    distinctAvgCallOperator.getChildren().set(
                            1, new CastOperator(decimal128p38s0, distinctAvgCallOperator.getChild(1), true));
                } else {
                    distinctAvgCallOperator = (CallOperator) scalarRewriter.rewrite(distinctAvgCallOperator,
                            Lists.newArrayList(new ImplicitCastRule()));
                }
                projectionMap.put(aggregation.getKey(), distinctAvgCallOperator);
            }
        }
        return allCteConsumes;
    }

    private CallOperator buildDistinctAggCall(CallOperator avgCallOperator, String functionName) {
        Function searchDesc = new Function(new FunctionName(functionName),
                avgCallOperator.getFunction().getArgs(), avgCallOperator.getType(), false);
        Function fn = GlobalStateMgr.getCurrentState().getFunction(searchDesc, IS_NONSTRICT_SUPERTYPE_OF);

        if (fn.getFunctionName().getFunction().equals(FunctionSet.SUM)) {
            fn = DecimalV3FunctionAnalyzer.rectifySumDistinct(fn, avgCallOperator.getChild(0).getType());
        }
        return new CallOperator(functionName, fn.getReturnType(), avgCallOperator.getChildren(), fn,
                avgCallOperator.isDistinct());
    }

    /*
     * create aggregate node with cte consume:
     *
     * Output:
     *      Agg[sum(v1), max(v1)]
     *          |
     *      CTEConsume
     */
    private OptExpression buildOtherAggregateCTEConsume(List<ColumnRefOperator> otherAggregateRef,
                                                        LogicalAggregationOperator aggregate,
                                                        OptExpression cteProduce,
                                                        ColumnRefFactory factory,
                                                        Map<ColumnRefOperator, ScalarOperator> projectionMap) {
        ColumnRefSet allCteConsumeRequiredColumns = new ColumnRefSet();
        otherAggregateRef.stream().map(k -> aggregate.getAggregations().get(k).getUsedColumns())
                .forEach(allCteConsumeRequiredColumns::union);
        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        allCteConsumeRequiredColumns.union(groupingKeys);

        LogicalCTEConsumeOperator cteConsume = buildCteConsume(cteProduce, allCteConsumeRequiredColumns, factory);
        // rewrite aggregate
        Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
        cteConsume.getCteOutputColumnRefMap().forEach((k, v) -> rewriteMap.put(v, k));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

        Map<ColumnRefOperator, CallOperator> aggregateFn = Maps.newHashMap();
        for (ColumnRefOperator otherRef : otherAggregateRef) {
            CallOperator otherAggFn = (CallOperator) rewriter.rewrite(aggregate.getAggregations().get(otherRef));
            aggregateFn.put(otherRef, otherAggFn);
            projectionMap.put(otherRef, otherRef);
        }
        List<ColumnRefOperator> rewriteGroupingKeys = groupingKeys.stream().
                map(column -> (ColumnRefOperator) rewriter.rewrite(column)).collect(
                        Collectors.toList());

        LogicalAggregationOperator newAggregate = new LogicalAggregationOperator.Builder().withOperator(aggregate).
                setAggregations(aggregateFn).setGroupingKeys(rewriteGroupingKeys).
                setPartitionByColumns(rewriteGroupingKeys).setPredicate(null).build();
        return OptExpression.create(newAggregate, OptExpression.create(cteConsume));
    }

    /*
     * create single count/sum distinct aggregate node with cte consume:
     *
     * Output:
     *      Agg[cd(v1)]
     *          |
     *      CTEConsume
     */
    private OptExpression buildCountSumDistinctCTEConsume(ColumnRefOperator aggDistinctRef,
                                                          CallOperator aggDistinctCall,
                                                          LogicalAggregationOperator aggregate,
                                                          OptExpression cteProduce, ColumnRefFactory factory) {
        ColumnRefSet cteConsumeRequiredColumns = aggDistinctCall.getUsedColumns();
        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        cteConsumeRequiredColumns.union(groupingKeys);

        LogicalCTEConsumeOperator cteConsume = buildCteConsume(cteProduce, cteConsumeRequiredColumns, factory);

        // rewrite aggregate count distinct
        Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
        cteConsume.getCteOutputColumnRefMap().forEach((k, v) -> rewriteMap.put(v, k));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
        CallOperator aggDistinctFn = (CallOperator) rewriter.rewrite(aggDistinctCall);
        List<ColumnRefOperator> rewriteGroupingKeys = groupingKeys.stream().
                map(column -> (ColumnRefOperator) rewriter.rewrite(column)).collect(
                        Collectors.toList());

        Map<ColumnRefOperator, CallOperator> aggregateFn = Maps.newHashMap();
        aggregateFn.put(aggDistinctRef, aggDistinctFn);

        LogicalAggregationOperator newAggregate = new LogicalAggregationOperator.Builder().withOperator(aggregate).
                setAggregations(aggregateFn).setGroupingKeys(rewriteGroupingKeys).
                setPartitionByColumns(rewriteGroupingKeys).setPredicate(null).build();

        return OptExpression.create(newAggregate, OptExpression.create(cteConsume));
    }

    private LogicalCTEConsumeOperator buildCteConsume(OptExpression cteProduce, ColumnRefSet requiredColumns,
                                                      ColumnRefFactory factory) {
        LogicalCTEProduceOperator produceOperator = (LogicalCTEProduceOperator) cteProduce.getOp();
        int cteId = produceOperator.getCteId();

        // create cte consume, cte output columns
        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputMap = Maps.newHashMap();
        for (int columnId : requiredColumns.getColumnIds()) {
            ColumnRefOperator produceOutput = factory.getColumnRef(columnId);
            ColumnRefOperator consumeOutput =
                    factory.create(produceOutput, produceOutput.getType(), produceOutput.isNullable());
            consumeOutputMap.put(consumeOutput, produceOutput);
        }
        // If there is no requiredColumns, we need to add least one column which is smallest
        if (consumeOutputMap.isEmpty()) {
            List<ColumnRefOperator> outputColumns =
                    produceOperator.getOutputColumns(new ExpressionContext(cteProduce)).getStream().
                            map(factory::getColumnRef).collect(Collectors.toList());
            ColumnRefOperator smallestColumn = Utils.findSmallestColumnRef(outputColumns);
            ColumnRefOperator consumeOutput =
                    factory.create(smallestColumn, smallestColumn.getType(), smallestColumn.isNullable());
            consumeOutputMap.put(consumeOutput, smallestColumn);
        }

        return new LogicalCTEConsumeOperator(cteId, consumeOutputMap);
    }
}
