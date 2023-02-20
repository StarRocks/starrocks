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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.AggType;
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
            FunctionSet.PERCENTILE_UNION, FunctionSet.ANY_VALUE);

    public MergeTwoAggRule() {
        super(RuleType.TF_MERGE_TWO_AGG_RULE, Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(
                Pattern.create(OperatorType.LOGICAL_PROJECT)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregateAbove = (LogicalAggregationOperator) input.getOp();
        LogicalProjectOperator project = (LogicalProjectOperator) input.getInputs().get(0).getOp();
        LogicalAggregationOperator aggregateBelow =
                (LogicalAggregationOperator) input.getInputs().get(0).getInputs().get(0).getOp();

        // Only handle column prune, forbidden expression
        if (!project.getColumnRefMap().entrySet().stream().allMatch(e -> e.getKey().equals(e.getValue()))) {
            return false;
        }

        if (aggregateBelow.getPredicate() != null || aggregateBelow.hasLimit()) {
            return false;
        }

        // The data distribution of the aggregate nodes keeps same
        if (!aggregateBelow.getGroupingKeys().containsAll(aggregateAbove.getGroupingKeys())) {
            return false;
        }

        Map<ColumnRefOperator, CallOperator> aggCallMapAbove = aggregateAbove.getAggregations();
        Map<ColumnRefOperator, CallOperator> aggCallMapBelow = aggregateBelow.getAggregations();

        if (!aggCallMapAbove.values().stream().allMatch(
                c -> c.isAggregate() && c.getUsedColumns().cardinality() == 1 && c.getChild(0).isColumnRef())) {
            return false;
        }

        for (CallOperator call : aggCallMapAbove.values()) {
            ColumnRefOperator ref = (ColumnRefOperator) call.getChild(0);
            if (!ALLOW_MERGE_AGGREGATE_FUNCTIONS.contains(call.getFnName().toLowerCase()) || call.isDistinct()) {
                return false;
            }

            if (!aggCallMapBelow.containsKey(ref)) {
                // max/min on grouping column is equals with direct aggregate on column
                if (!FunctionSet.MAX.equalsIgnoreCase(call.getFnName()) &&
                        !FunctionSet.MIN.equalsIgnoreCase(call.getFnName()) &&
                        !FunctionSet.ANY_VALUE.equalsIgnoreCase(call.getFnName())) {
                    return false;
                }
            } else {
                if (!aggCallMapBelow.get(ref).getFnName().equalsIgnoreCase(call.getFnName()) ||
                        aggCallMapBelow.get(ref).isDistinct()) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregateAbove = (LogicalAggregationOperator) input.getOp();
        LogicalAggregationOperator aggregateBelow =
                (LogicalAggregationOperator) input.getInputs().get(0).getInputs().get(0).getOp();

        Map<ColumnRefOperator, CallOperator> aggCallMapAbove = aggregateAbove.getAggregations();
        Map<ColumnRefOperator, CallOperator> aggCallMapBelow = aggregateBelow.getAggregations();

        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggCallMapAbove.entrySet()) {
            CallOperator fn = entry.getValue();
            ColumnRefOperator ref = (ColumnRefOperator) fn.getChild(0);

            CallOperator newFn;
            if (aggCallMapBelow.containsKey(ref)) {
                CallOperator belowOp = aggCallMapBelow.get(ref);
                newFn = new CallOperator(fn.getFnName(), fn.getType(), belowOp.getChildren(),
                        belowOp.getFunction(), fn.isDistinct());
            } else {
                newFn = new CallOperator(fn.getFnName(), fn.getType(), Lists.newArrayList(ref), fn.getFunction(),
                        fn.isDistinct());
            }

            ColumnRefOperator newName = new ColumnRefOperator(entry.getKey().getId(), entry.getKey().getType(),
                    ref.getName(), entry.getKey().isNullable());
            newAggregations.put(newName, newFn);
        }

        LogicalAggregationOperator result = new LogicalAggregationOperator(AggType.GLOBAL,
                aggregateAbove.getGroupingKeys(),
                aggregateAbove.getPartitionByColumns(),
                newAggregations,
                false,
                -1,
                aggregateAbove.getLimit(),
                aggregateAbove.getPredicate());

        return Lists.newArrayList(
                OptExpression.create(result, input.getInputs().get(0).getInputs().get(0).getInputs()));
    }
}
