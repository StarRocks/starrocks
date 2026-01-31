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
import com.starrocks.catalog.combinator.AggStateIf;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * Optimize aggregate functions with _if suffix (e.g., min_by_if, max_by_if, array_agg_if, array_agg_distinct_if) by using CTE.
 *
 * The optimization splits agg_if functions into two steps:
 * 1. Filter rows where condition is true, then apply the base aggregate function (without _if)
 * 2. Join the result with main query using FULL JOIN or LEFT JOIN
 *
 * For agg_if functions without group by:
 * e.g:
 * Before:
 *          Agg[array_agg_if(x, cd1), min_by_if(x, cd1), sum(v4), max(v5)]
 *                      |
 *                  Child Plan
 *
 * After:
 *                     CTEAnchor
 *                   /           \
 *           CTEProduce           full join
 *             /                 /           \
 *      Child Plan        full join       Agg[sum(v4), max(v5)]
 *                        /      \               \
 *               Agg[agg(x)]  Agg[agg(x)]   CTEConsume
 *                    |            |
 *               Filter[cd1]   Filter[cd2]
 *                    |            |
 *                CTEConsume   CTEConsume
 *
 *
 * For agg_if functions with group by:
 * Before:
 *          Agg[array_agg_if(x, condition), sum(v4)] group by v3
 *                      |
 *                  Child Plan
 *
 * After:
 *                     CTEAnchor
 *                   /            \
 *           CTEProduce         Left join (on group keys)
 *             /                 /                  \
 *      Child Plan       Agg[sum(v4)]             Agg[array_agg(v4)]
 *                           |                            |
 *                       CTEConsume                   Filter[cd1]
 *                                                        |
 *                                                    CTEConsume
 */
public class AggIfCTERewriter {

    public List<OptExpression> transformImpl(OptExpression input, OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        // define cteId
        int cteId = context.getCteContext().getNextCteId();

        // build logic cte produce operator
        OptExpression cteProduce = OptExpression.create(new LogicalCTEProduceOperator(cteId), input.getInputs());

        // generate all aggregation operator, split agg_if function and other function
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getOp();

        List<ColumnRefOperator> aggIfAggList = aggregate.getAggregations().entrySet().stream()
                .filter(kv -> isAggIf(kv.getValue())).map(Map.Entry::getKey).collect(Collectors.toList());
        List<ColumnRefOperator> otherAggregate = aggregate.getAggregations().entrySet().stream()
                .filter(kv -> !isAggIf(kv.getValue())).map(Map.Entry::getKey).collect(Collectors.toList());
        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        boolean hasGroupBy = !groupingKeys.isEmpty();

        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        LinkedList<OptExpression> allCteConsumes = Lists.newLinkedList();

        // First add normal aggregations (main CTE) to the left side
        if (otherAggregate.size() > 0) {
            allCteConsumes.offer(
                    CTERewriterUtils.buildOtherAggregateCTEConsume(otherAggregate, aggregate, cteProduce, columnRefFactory,
                            columnRefMap));
        } else if (hasGroupBy) {
            // When there are no normal aggregations but we have GROUP BY,
            // create a dimension table CTE that only contains distinct grouping keys
            allCteConsumes.offer(
                    buildDimensionTableCTEConsume(aggregate, groupingKeys, cteProduce, columnRefFactory, columnRefMap));
        }

        // Then add agg_if aggregations (sub CTEs) to the right side
        allCteConsumes.addAll(buildAggIfCTEConsume(aggregate, aggIfAggList, cteProduce,
                columnRefFactory, columnRefMap));

        // left deep join tree with FULL JOIN
        // Normal aggregations (main CTE) should be on the LEFT, agg_if (sub CTEs) on the RIGHT
        while (allCteConsumes.size() > 1) {
            OptExpression left = allCteConsumes.poll();
            OptExpression right = allCteConsumes.poll();
            OptExpression join;
            if (!hasGroupBy) {
                // For non-group-by case, use FULL JOIN
                // LEFT = main CTE (normal aggregations), RIGHT = sub CTE (agg_if)
                join = OptExpression.create(
                        new LogicalJoinOperator(JoinOperator.FULL_OUTER_JOIN, null, HintNode.HINT_JOIN_UNREORDER),
                                left, right);
            } else {
                // create left join when aggregate has group by keys
                // LEFT = main CTE, RIGHT = sub CTE, join on group keys
                join = CTERewriterUtils.buildJoin(left, right, JoinOperator.LEFT_OUTER_JOIN);
                // Add project map for group keys.
                LogicalJoinOperator joinOperator = (LogicalJoinOperator) join.getOp();
                List<ColumnRefOperator> joinOnPredicateColumns = CTERewriterUtils.getJoinOnPredicateColumn(joinOperator);
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

        if (aggregate.hasLimit()) {
            Operator.Builder opBuilder = OperatorBuilderFactory.build(rightTree.getOp());
            opBuilder.withOperator(rightTree.getOp()).setLimit(aggregate.getLimit());
            rightTree = OptExpression.create(opBuilder.build(), rightTree.getInputs());
        }

        context.getCteContext().addForceCTE(cteId);

        LogicalCTEAnchorOperator cteAnchor = new LogicalCTEAnchorOperator(cteId);
        return Lists.newArrayList(OptExpression.create(cteAnchor, cteProduce, rightTree));
    }

    private boolean isAggIf(CallOperator callOperator) {
        if (!callOperator.isAggregate()) {
            return false;
        }
        String fnName = callOperator.getFnName();
        return fnName.equalsIgnoreCase(FunctionSet.MAX_BY + FunctionSet.AGG_STATE_IF_SUFFIX) ||
                fnName.equalsIgnoreCase(FunctionSet.MIN_BY + FunctionSet.AGG_STATE_IF_SUFFIX) ||
                fnName.equalsIgnoreCase(FunctionSet.ARRAY_AGG + FunctionSet.AGG_STATE_IF_SUFFIX) ||
                fnName.equalsIgnoreCase(FunctionSet.ARRAY_AGG_DISTINCT + FunctionSet.AGG_STATE_IF_SUFFIX);
    }

    private LinkedList<OptExpression> buildAggIfCTEConsume(LogicalAggregationOperator aggregate,
                                                           List<ColumnRefOperator> aggIfAggList,
                                                           OptExpression cteProduce,
                                                           ColumnRefFactory factory,
                                                           Map<ColumnRefOperator, ScalarOperator> projectionMap) {
        LinkedList<OptExpression> allCteConsumes = Lists.newLinkedList();

        for (ColumnRefOperator aggIfAggRef : aggIfAggList) {
            CallOperator aggIfCall = aggregate.getAggregations().get(aggIfAggRef);
            Preconditions.checkState(aggIfCall.getArguments().size() >= 2,
                    "agg_if functions should have at least 2 arguments: at least one arg and condition");

            // Extract arguments: all args except the last one (condition)
            List<ScalarOperator> args = aggIfCall.getArguments();
            ScalarOperator conditionArg = args.get(args.size() - 1); // last argument is condition
            List<ScalarOperator> baseAggArgs = args.subList(0, args.size() - 1); // all args except condition

            // Build base aggregate function call operator (without _if suffix)
            // Get cached base function from AggStateIf (preserves order by info)
            Preconditions.checkState(aggIfCall.getFunction() instanceof AggStateIf,
                    "Expected AggStateIf function, but got: " + aggIfCall.getFnName());
            
            AggStateIf aggStateIf = (AggStateIf) aggIfCall.getFunction();
            AggregateFunction baseFn = aggStateIf.getBaseFunction();
            Preconditions.checkState(baseFn != null,
                    "baseFunction should not be null in AggStateIf: " + aggIfCall.getFnName());

            // Use cached base function which already has order by information preserved
            CallOperator baseAggCall = new CallOperator(baseFn.functionName(), aggIfCall.getType(),
                    baseAggArgs, baseFn, false);

            // Build CTE consume with filter
            OptExpression cteConsumeWithFilter = buildAggIfCTEConsumeWithFilter(
                    aggIfAggRef, baseAggCall, conditionArg, aggregate, cteProduce, factory);

            allCteConsumes.offer(cteConsumeWithFilter);
            projectionMap.put(aggIfAggRef, aggIfAggRef);
        }

        return allCteConsumes;
    }

    /*
     * create single agg_if aggregate node with cte consume and filter:
     *
     * Output:
     *      Agg[base_agg(x,y)]
     *          |
     *      Filter[condition]
     *          |
     *      CTEConsume
     */
    private OptExpression buildAggIfCTEConsumeWithFilter(ColumnRefOperator aggRef,
                                                         CallOperator baseAggCall,
                                                         ScalarOperator conditionArg,
                                                         LogicalAggregationOperator aggregate,
                                                         OptExpression cteProduce,
                                                         ColumnRefFactory factory) {
        // Collect required columns: x, y, condition, and grouping keys
        ColumnRefSet cteConsumeRequiredColumns = new ColumnRefSet();
        cteConsumeRequiredColumns.union(baseAggCall.getUsedColumns());
        cteConsumeRequiredColumns.union(conditionArg.getUsedColumns());
        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        cteConsumeRequiredColumns.union(groupingKeys);

        LogicalCTEConsumeOperator cteConsume = CTERewriterUtils.buildCteConsume(cteProduce, cteConsumeRequiredColumns, factory);

        // rewrite aggregate and condition
        Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
        cteConsume.getCteOutputColumnRefMap().forEach((k, v) -> rewriteMap.put(v, k));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

        // Rewrite base aggregate call
        CallOperator rewrittenBaseAggCall = (CallOperator) rewriter.rewrite(baseAggCall);

        // Rewrite condition
        ScalarOperator rewrittenCondition = rewriter.rewrite(conditionArg);

        // Rewrite grouping keys
        List<ColumnRefOperator> rewriteGroupingKeys = groupingKeys.stream().
                map(column -> (ColumnRefOperator) rewriter.rewrite(column)).collect(
                        Collectors.toList());

        // Build aggregation with base aggregate function
        Map<ColumnRefOperator, CallOperator> aggregateFn = Maps.newHashMap();
        aggregateFn.put(aggRef, rewrittenBaseAggCall);

        LogicalAggregationOperator newAggregate = new LogicalAggregationOperator.Builder()
                .withOperator(aggregate)
                .setAggregations(aggregateFn)
                .setGroupingKeys(rewriteGroupingKeys)
                .setPartitionByColumns(rewriteGroupingKeys)
                .setLimit(Operator.DEFAULT_LIMIT)
                .setPredicate(null).build();

        // Add filter node before aggregation
        OptExpression filterNode = OptExpression.create(
                new LogicalFilterOperator(rewrittenCondition),
                        OptExpression.create(cteConsume));

        return OptExpression.create(newAggregate, filterNode);
    }

    /*
     * Create a dimension table CTE that only contains distinct grouping keys (no aggregations).
     * This is used when there are no normal aggregations, but we have GROUP BY.
     *
     * Output:
     *      Agg[group by keys only, no aggregations]
     *          |
     *      CTEConsume
     */
    private OptExpression buildDimensionTableCTEConsume(LogicalAggregationOperator aggregate,
                                                        List<ColumnRefOperator> groupingKeys,
                                                        OptExpression cteProduce,
                                                        ColumnRefFactory factory,
                                                        Map<ColumnRefOperator, ScalarOperator> projectionMap) {
        // Collect required columns: only grouping keys
        ColumnRefSet cteConsumeRequiredColumns = new ColumnRefSet();
        cteConsumeRequiredColumns.union(groupingKeys);

        LogicalCTEConsumeOperator cteConsume = CTERewriterUtils.buildCteConsume(cteProduce, cteConsumeRequiredColumns, factory);

        // Rewrite grouping keys
        Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
        cteConsume.getCteOutputColumnRefMap().forEach((k, v) -> rewriteMap.put(v, k));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

        List<ColumnRefOperator> rewriteGroupingKeys = groupingKeys.stream().
                map(column -> (ColumnRefOperator) rewriter.rewrite(column)).collect(
                        Collectors.toList());

        // Build aggregation with no aggregations, only grouping keys (acts as DISTINCT)
        LogicalAggregationOperator newAggregate = new LogicalAggregationOperator.Builder()
                .withOperator(aggregate)
                .setAggregations(Maps.newHashMap())
                .setGroupingKeys(rewriteGroupingKeys)
                .setPartitionByColumns(rewriteGroupingKeys)
                .setLimit(Operator.DEFAULT_LIMIT)
                .setPredicate(null)
                .build();

        // Update projection map for grouping keys
        for (int index = 0; index < groupingKeys.size(); ++index) {
            projectionMap.put(groupingKeys.get(index), rewriteGroupingKeys.get(index));
        }

        return OptExpression.create(newAggregate, OptExpression.create(cteConsume));
    }

}
