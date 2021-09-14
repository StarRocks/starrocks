// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

// For optimize sql like: select sum(x) from (select k1, sum(v1) as x from test group by k1);
//
// Before:
//      Aggregate
//          |
//       Project
//          |
//      Aggregate
//          |
//         Node
//
// After:
//      Aggregate
//          |
//         Node
public class MergeTwoAggRule extends TransformationRule {
    private static final List<String> ALLOW_MERGE_AGGREGATE_FUNCTIONS = Lists.newArrayList(FunctionSet.SUM,
            FunctionSet.MAX, FunctionSet.MIN, FunctionSet.BITMAP_UNION, FunctionSet.HLL_UNION,
            FunctionSet.PERCENTILE_UNION);

    public MergeTwoAggRule() {
        super(RuleType.TF_MERGE_TWO_AGG_RULE, Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(
                Pattern.create(OperatorType.LOGICAL_PROJECT)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate1 = (LogicalAggregationOperator) input.getOp();
        LogicalProjectOperator project = (LogicalProjectOperator) input.getInputs().get(0).getOp();
        LogicalAggregationOperator aggregate2 =
                (LogicalAggregationOperator) input.getInputs().get(0).getInputs().get(0).getOp();

        // Only handle column prune, forbidden expression
        if (!project.getColumnRefMap().entrySet().stream().allMatch(e -> e.getKey().equals(e.getValue()))) {
            return false;
        }

        if (aggregate2.getPredicate() != null || aggregate2.hasLimit()) {
            return false;
        }

        // The data distribution of the aggregate nodes keeps same
        if (!aggregate2.getGroupingKeys().containsAll(aggregate1.getGroupingKeys())) {
            return false;
        }

        Map<ColumnRefOperator, CallOperator> aggCallMap1 = aggregate1.getAggregations();
        Map<ColumnRefOperator, CallOperator> aggCallMap2 = aggregate2.getAggregations();

        if (!aggCallMap1.values().stream().allMatch(
                c -> c.isAggregate() && c.getUsedColumns().cardinality() == 1 && c.getChild(0).isColumnRef())) {
            return false;
        }

        for (CallOperator call : aggCallMap1.values()) {
            ColumnRefOperator ref = (ColumnRefOperator) call.getChild(0);
            if (!ALLOW_MERGE_AGGREGATE_FUNCTIONS.contains(call.getFnName().toLowerCase())) {
                return false;
            }

            if (!aggCallMap2.containsKey(ref) && !FunctionSet.MAX.equalsIgnoreCase(call.getFnName()) &&
                    !FunctionSet.MIN.equalsIgnoreCase(call.getFnName())) {
                return false;
            }

            if (!aggCallMap2.get(ref).getFnName().equalsIgnoreCase(call.getFnName())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregate1 = (LogicalAggregationOperator) input.getOp();
        LogicalAggregationOperator aggregate2 =
                (LogicalAggregationOperator) input.getInputs().get(0).getInputs().get(0).getOp();

        Map<ColumnRefOperator, CallOperator> aggCallMap1 = aggregate1.getAggregations();
        Map<ColumnRefOperator, CallOperator> aggCallMap2 = aggregate2.getAggregations();

        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggCallMap1.entrySet()) {
            CallOperator fn = entry.getValue();
            ColumnRefOperator ref = (ColumnRefOperator) fn.getChild(0);

            CallOperator newFn;
            if (aggCallMap2.containsKey(ref)) {
                newFn = new CallOperator(fn.getFnName(), fn.getType(), aggCallMap2.get(ref).getChildren(),
                        fn.getFunction(), fn.isDistinct());
            } else {
                newFn = new CallOperator(fn.getFnName(), fn.getType(), Lists.newArrayList(ref), fn.getFunction(),
                        fn.isDistinct());
            }

            ColumnRefOperator newName = new ColumnRefOperator(entry.getKey().getId(), entry.getKey().getType(),
                    ref.getName(), entry.getKey().isNullable());
            newAggregations.put(newName, newFn);
        }

        LogicalAggregationOperator result =
                new LogicalAggregationOperator(aggregate1.getGroupingKeys(), newAggregations);
        result.setLimit(aggregate1.getLimit());
        result.setPredicate(aggregate1.getPredicate());

        return Lists
                .newArrayList(OptExpression.create(result, input.getInputs().get(0).getInputs().get(0).getInputs()));
    }
}
