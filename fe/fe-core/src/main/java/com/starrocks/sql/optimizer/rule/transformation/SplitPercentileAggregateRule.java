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
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * Keep percentile aggregates away from other aggregate states.
 *
 * percentile_approx/percentile_approx_weighted have expensive serialized intermediate states.
 * If they share a streaming aggregate node with other aggregates, those aggregates can make
 * pre-aggregation switch to PASS_THROUGH, forcing percentile values to be serialized row by row.
 *
 * This is a planner-side workaround. The root fix should optimize percentile aggregate execution
 * when streaming pre-aggregation switches to PASS_THROUGH.
 */
public class SplitPercentileAggregateRule extends TransformationRule {
    public SplitPercentileAggregateRule() {
        super(RuleType.TF_SPLIT_PERCENTILE_AGGREGATE,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate = input.getOp().cast();
        boolean hasPercentile = aggregate.getAggregations().values().stream()
                .anyMatch(SplitPercentileAggregateRule::isPercentileAggregate);
        boolean hasOtherAggregate = aggregate.getAggregations().values().stream()
                .anyMatch(call -> !isPercentileAggregate(call));
        return hasPercentile && hasOtherAggregate;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate = input.getOp().cast();
        ColumnRefFactory factory = context.getColumnRefFactory();
        int cteId = context.getCteContext().getNextCteId();
        context.getCteContext().addForceCTE(cteId);

        OptExpression cteProduce = OptExpression.create(new LogicalCTEProduceOperator(cteId), input.getInputs());

        List<ColumnRefOperator> percentileAggregates = aggregate.getAggregations().entrySet().stream()
                .filter(entry -> isPercentileAggregate(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        List<ColumnRefOperator> otherAggregates = aggregate.getAggregations().entrySet().stream()
                .filter(entry -> !isPercentileAggregate(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        Map<ColumnRefOperator, ScalarOperator> projectionMap = Maps.newHashMap();
        OptExpression percentileConsume = buildAggregateConsume(percentileAggregates, aggregate, cteProduce,
                factory, projectionMap);
        OptExpression otherConsume = buildAggregateConsume(otherAggregates, aggregate, cteProduce, factory,
                projectionMap);

        OptExpression joined = buildJoin(aggregate, percentileConsume, otherConsume, projectionMap);
        OptExpression rewritten = OptExpression.create(new LogicalProjectOperator(projectionMap), joined);

        if (aggregate.getPredicate() != null) {
            rewritten = OptExpression.create(new LogicalFilterOperator(aggregate.getPredicate()), rewritten);
        }

        if (aggregate.hasLimit()) {
            Operator.Builder builder = OperatorBuilderFactory.build(rewritten.getOp());
            builder.withOperator(rewritten.getOp()).setLimit(aggregate.getLimit());
            rewritten = OptExpression.create(builder.build(), rewritten.getInputs());
        }

        return Lists.newArrayList(OptExpression.create(new LogicalCTEAnchorOperator(cteId), cteProduce, rewritten));
    }

    private OptExpression buildAggregateConsume(List<ColumnRefOperator> aggregateRefs,
                                                LogicalAggregationOperator aggregate,
                                                OptExpression cteProduce,
                                                ColumnRefFactory factory,
                                                Map<ColumnRefOperator, ScalarOperator> projectionMap) {
        ColumnRefSet requiredColumns = new ColumnRefSet();
        aggregateRefs.stream()
                .map(ref -> aggregate.getAggregations().get(ref).getUsedColumns())
                .forEach(requiredColumns::union);
        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        requiredColumns.union(groupingKeys);

        LogicalCTEConsumeOperator cteConsume = buildCteConsume(cteProduce, requiredColumns, factory);
        Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
        cteConsume.getCteOutputColumnRefMap().forEach((consume, produce) -> rewriteMap.put(produce, consume));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

        Map<ColumnRefOperator, CallOperator> aggregateFns = Maps.newHashMap();
        for (ColumnRefOperator aggregateRef : aggregateRefs) {
            CallOperator rewrittenCall = (CallOperator) rewriter.rewrite(aggregate.getAggregations().get(aggregateRef));
            aggregateFns.put(aggregateRef, rewrittenCall);
            projectionMap.put(aggregateRef, aggregateRef);
        }
        List<ColumnRefOperator> rewrittenGroupingKeys = groupingKeys.stream()
                .map(column -> (ColumnRefOperator) rewriter.rewrite(column))
                .collect(Collectors.toList());

        LogicalAggregationOperator newAggregate = new LogicalAggregationOperator.Builder()
                .withOperator(aggregate)
                .setAggregations(aggregateFns)
                .setGroupingKeys(rewrittenGroupingKeys)
                .setPartitionByColumns(rewrittenGroupingKeys)
                .setPredicate(null)
                .setLimit(Operator.DEFAULT_LIMIT)
                .build();
        return OptExpression.create(newAggregate, OptExpression.create(cteConsume));
    }

    private OptExpression buildJoin(LogicalAggregationOperator aggregate,
                                    OptExpression left,
                                    OptExpression right,
                                    Map<ColumnRefOperator, ScalarOperator> projectionMap) {
        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        if (groupingKeys.isEmpty()) {
            return OptExpression.create(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null,
                    JoinOperator.HINT_UNREORDER), left, right);
        }

        List<ColumnRefOperator> leftColumns = getJoinOnPredicateColumn(left.getOp());
        List<ColumnRefOperator> rightColumns = getJoinOnPredicateColumn(right.getOp());
        Preconditions.checkState(leftColumns.size() == rightColumns.size());
        Preconditions.checkState(groupingKeys.size() == leftColumns.size());

        List<ScalarOperator> onPredicates = Lists.newArrayList();
        for (int index = 0; index < leftColumns.size(); ++index) {
            onPredicates.add(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                    leftColumns.get(index), rightColumns.get(index)));
            projectionMap.put(groupingKeys.get(index), leftColumns.get(index));
        }
        return OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundAnd(onPredicates), JoinOperator.HINT_UNREORDER), left, right);
    }

    private List<ColumnRefOperator> getJoinOnPredicateColumn(Operator operator) {
        LogicalAggregationOperator aggregationOperator = operator.cast();
        return aggregationOperator.getGroupingKeys();
    }

    private LogicalCTEConsumeOperator buildCteConsume(OptExpression cteProduce, ColumnRefSet requiredColumns,
                                                      ColumnRefFactory factory) {
        LogicalCTEProduceOperator produceOperator = cteProduce.getOp().cast();
        int cteId = produceOperator.getCteId();

        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputMap = Maps.newHashMap();
        for (int columnId : requiredColumns.getColumnIds()) {
            ColumnRefOperator produceOutput = factory.getColumnRef(columnId);
            ColumnRefOperator consumeOutput = factory.create(produceOutput, produceOutput.getType(),
                    produceOutput.isNullable());
            consumeOutputMap.put(consumeOutput, produceOutput);
        }

        if (consumeOutputMap.isEmpty()) {
            List<ColumnRefOperator> outputColumns = produceOperator.getOutputColumns(new ExpressionContext(cteProduce))
                    .getStream()
                    .map(factory::getColumnRef)
                    .collect(Collectors.toList());
            ColumnRefOperator smallestColumn = Utils.findSmallestColumnRef(outputColumns);
            ColumnRefOperator consumeOutput = factory.create(smallestColumn, smallestColumn.getType(),
                    smallestColumn.isNullable());
            consumeOutputMap.put(consumeOutput, smallestColumn);
        }

        return new LogicalCTEConsumeOperator(cteId, consumeOutputMap);
    }

    private static boolean isPercentileAggregate(CallOperator call) {
        return FunctionSet.PERCENTILE_APPROX.equalsIgnoreCase(call.getFnName()) ||
                FunctionSet.PERCENTILE_APPROX_WEIGHTED.equalsIgnoreCase(call.getFnName());
    }
}
