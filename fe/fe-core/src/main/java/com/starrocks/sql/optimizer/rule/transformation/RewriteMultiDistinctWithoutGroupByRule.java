// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 *
 * Optimize multi count distinct aggregate node without group by.
 * multi_count_distinct will gather to one instance work, doesn't take advantage of the MPP architecture.
 *
 * e.g:
 * Mark multi_count_distinct(v1) as mcd(v1)
 * Mark count(distinct v1) as cd(v1)
 *
 * Before:
 *          Agg[mcd(v1), mcd(v2), sum(v4), max(v5)]
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
 */
public class RewriteMultiDistinctWithoutGroupByRule extends TransformationRule {
    public RewriteMultiDistinctWithoutGroupByRule() {
        super(RuleType.TF_REWRITE_MULTI_DISTINCT_WITHOUT_GROUP_BY,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                        OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // check cte is disabled or hasNoGroup false
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        boolean hasNoGroup = agg.getGroupingKeys().isEmpty();
        if (!context.getSessionVariable().isCboCteReuse() || !hasNoGroup) {
            return false;
        }

        List<CallOperator> distinctAggOperatorList = agg.getAggregations().values().stream()
                .filter(CallOperator::isDistinct).collect(Collectors.toList());
        boolean hasMultiColumns = distinctAggOperatorList.stream().anyMatch(c -> c.getChildren().size() > 1);

        return (distinctAggOperatorList.size() > 1
                && !hasMultiColumns) || agg.getAggregations().values().stream()
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

        List<ColumnRefOperator> countDistinctList = aggregate.getAggregations().entrySet().stream()
                .filter(kv -> kv.getValue().isDistinct()).map(Map.Entry::getKey).collect(Collectors.toList());
        List<ColumnRefOperator> otherAggregate = aggregate.getAggregations().entrySet().stream()
                .filter(kv -> !kv.getValue().isDistinct()).map(Map.Entry::getKey).collect(Collectors.toList());

        LinkedList<OptExpression> allCteConsumes = Lists.newLinkedList();

        for (ColumnRefOperator countDistinct : countDistinctList) {
            allCteConsumes.offer(buildCountDistinctCTEConsume(countDistinct, aggregate, cteProduce, columnRefFactory));
        }

        if (otherAggregate.size() > 0) {
            allCteConsumes.offer(
                    buildOtherAggregateCTEConsume(otherAggregate, aggregate, cteProduce, columnRefFactory));
        }

        // left deep join tree
        while (allCteConsumes.size() > 1) {
            OptExpression left = allCteConsumes.poll();
            OptExpression right = allCteConsumes.poll();

            OptExpression join =
                    OptExpression.create(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null), left, right);
            allCteConsumes.offerFirst(join);
        }

        context.getCteContext().addForceCTE(cteId);

        LogicalCTEAnchorOperator cteAnchor = new LogicalCTEAnchorOperator(cteId);
        return Lists.newArrayList(OptExpression.create(cteAnchor, cteProduce, allCteConsumes.get(0)));
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
                                                        LogicalAggregationOperator aggregate, OptExpression cteProduce,
                                                        ColumnRefFactory factory) {
        ColumnRefSet allCteConsumeRequiredColumns = new ColumnRefSet();
        otherAggregateRef.stream().map(k -> aggregate.getAggregations().get(k).getUsedColumns())
                .forEach(allCteConsumeRequiredColumns::union);
        LogicalCTEConsumeOperator cteConsume = buildCteConsume(cteProduce, allCteConsumeRequiredColumns, factory);

        // rewrite aggregate
        Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
        cteConsume.getCteOutputColumnRefMap().forEach((k, v) -> rewriteMap.put(v, k));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

        Map<ColumnRefOperator, CallOperator> aggregateFn = Maps.newHashMap();
        for (ColumnRefOperator otherRef : otherAggregateRef) {
            CallOperator otherAggFn =
                    (CallOperator) aggregate.getAggregations().get(otherRef).accept(rewriter, null);
            aggregateFn.put(otherRef, otherAggFn);
        }
        LogicalAggregationOperator newAggregate =
                new LogicalAggregationOperator.Builder().withOperator(aggregate).setAggregations(aggregateFn).build();

        return OptExpression.create(newAggregate, OptExpression.create(cteConsume));
    }

    /*
     * create single count distinct aggregate node with cte consume:
     *
     * Output:
     *      Agg[cd(v1)]
     *          |
     *      CTEConsume
     */
    private OptExpression buildCountDistinctCTEConsume(ColumnRefOperator countDistinctRef,
                                                       LogicalAggregationOperator aggregate,
                                                       OptExpression cteProduce, ColumnRefFactory factory) {
        ColumnRefSet cteConsumeRequiredColumns = aggregate.getAggregations().get(countDistinctRef).getUsedColumns();
        LogicalCTEConsumeOperator cteConsume = buildCteConsume(cteProduce, cteConsumeRequiredColumns, factory);

        // rewrite aggregate count distinct
        Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
        cteConsume.getCteOutputColumnRefMap().forEach((k, v) -> rewriteMap.put(v, k));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);
        CallOperator countDistinctFn =
                (CallOperator) aggregate.getAggregations().get(countDistinctRef).accept(rewriter, null);

        Map<ColumnRefOperator, CallOperator> aggregateFn = Maps.newHashMap();
        aggregateFn.put(countDistinctRef, countDistinctFn);

        LogicalAggregationOperator newAggregate =
                new LogicalAggregationOperator.Builder().withOperator(aggregate).setAggregations(aggregateFn).build();

        return OptExpression.create(newAggregate, OptExpression.create(cteConsume));
    }

    private LogicalCTEConsumeOperator buildCteConsume(OptExpression cteProduce, ColumnRefSet requiredColumns,
                                                      ColumnRefFactory factory) {
        int cteId = ((LogicalCTEProduceOperator) cteProduce.getOp()).getCteId();

        // create cte consume, cte output columns
        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputMap = Maps.newHashMap();
        for (int columnId : requiredColumns.getColumnIds()) {
            ColumnRefOperator produceOutput = factory.getColumnRef(columnId);
            ColumnRefOperator consumeOutput =
                    factory.create(produceOutput, produceOutput.getType(), produceOutput.isNullable());
            consumeOutputMap.put(consumeOutput, produceOutput);
        }

        return new LogicalCTEConsumeOperator(cteId, consumeOutputMap);
    }
}
