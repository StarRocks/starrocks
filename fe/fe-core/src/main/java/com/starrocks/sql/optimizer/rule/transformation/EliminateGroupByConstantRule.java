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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class EliminateGroupByConstantRule extends TransformationRule {

    public static final EliminateGroupByConstantRule INSTANCE = new EliminateGroupByConstantRule();

    private EliminateGroupByConstantRule() {
        super(RuleType.TF_ELIMINATE_GROUP_BY_CONSTANT,
                Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.LOGICAL_PROJECT));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        LogicalProjectOperator projectOp = input.inputAt(0).getOp().cast();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projectOp.getColumnRefMap();
        if (aggOp.getGroupingKeys().size() <= 1) {
            return false;
        }

        for (ColumnRefOperator groupByCol : aggOp.getGroupingKeys()) {
            if (columnRefMap.get(groupByCol).isConstant()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = input.getOp().cast();
        LogicalProjectOperator projectOp = input.inputAt(0).getOp().cast();
        Map<ColumnRefOperator, ScalarOperator> bottomMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> topMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projectOp.getColumnRefMap().entrySet()) {
            if (entry.getValue().isConstant() && aggOp.getGroupingKeys().contains(entry.getKey())) {
                topMap.put(entry.getKey(), entry.getValue());
            } else {
                bottomMap.put(entry.getKey(), entry.getValue());
            }
        }

        List<ColumnRefOperator> newGroupByKeys = Lists.newArrayList();
        if (topMap.keySet().containsAll(aggOp.getGroupingKeys())) {
            // all group by keys are constant, we need keep one group by key
            newGroupByKeys.add(aggOp.getGroupingKeys().get(0));
            ScalarOperator scalarOperator = topMap.remove(aggOp.getGroupingKeys().get(0));
            bottomMap.put(aggOp.getGroupingKeys().get(0), scalarOperator);
        } else {
            for (ColumnRefOperator groupByKey : aggOp.getGroupingKeys()) {
                if (!topMap.containsKey(groupByKey)) {
                    newGroupByKeys.add(groupByKey);
                }
            }
        }

        ScalarOperator newPredicate = aggOp.getPredicate();
        if (newPredicate != null) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(topMap);
            newPredicate = rewriter.rewrite(newPredicate);
        }

        Map<ColumnRefOperator, CallOperator> newAggCallMap = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            CallOperator callOperator = entry.getValue();
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(topMap);
            CallOperator newCallOperator = (CallOperator) rewriter.rewrite(callOperator);
            newAggCallMap.put(entry.getKey(), newCallOperator);
        }

        newAggCallMap.keySet().forEach(e -> topMap.put(e, e));
        newGroupByKeys.forEach(e -> topMap.put(e, e));
        LogicalProjectOperator newProjectOp = LogicalProjectOperator.builder().setColumnRefMap(topMap).build();

        LogicalAggregationOperator newAggOp = LogicalAggregationOperator.builder()
                .withOperator(aggOp)
                .setAggregations(newAggCallMap)
                .setGroupingKeys(newGroupByKeys)
                .setPartitionByColumns(newGroupByKeys)
                .setPredicate(newPredicate)
                .build();

        OptExpression result;

        if (bottomMap.isEmpty()) {
            result = OptExpression.create(newProjectOp,
                    OptExpression.create(newAggOp, input.inputAt(0).getInputs()));
        } else {
            LogicalProjectOperator bottomProject = new LogicalProjectOperator(bottomMap);
            result = OptExpression.create(newProjectOp,
                    OptExpression.create(newAggOp,
                            OptExpression.create(bottomProject,
                                    input.inputAt(0).getInputs())));
        }

        return Lists.newArrayList(result);
    }
}
