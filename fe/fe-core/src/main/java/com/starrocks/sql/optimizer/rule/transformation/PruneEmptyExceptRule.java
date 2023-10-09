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
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/*
case1:
         Except
      /    |     \       ->    Empty
   Empty  Child1  Child2

case2:
       Except
      /      \     ->  Child1
   Child1    Empty
 */
public class PruneEmptyExceptRule extends TransformationRule {
    public PruneEmptyExceptRule() {
        super(RuleType.TF_PRUNE_EMPTY_EXCEPT,
                Pattern.create(OperatorType.LOGICAL_EXCEPT, OperatorType.PATTERN_MULTI_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return input.getInputs().stream().map(OptExpression::getOp).filter(op -> op instanceof LogicalValuesOperator)
                .anyMatch(op -> ((LogicalValuesOperator) op).getRows().isEmpty());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalExceptOperator exceptOperator = (LogicalExceptOperator) input.getOp();
        List<OptExpression> newInputs = Lists.newArrayList();
        List<List<ColumnRefOperator>> childOutputs = Lists.newArrayList();

        boolean firstIsEmpty = false;
        for (int i = 0; i < input.getInputs().size(); i++) {
            OptExpression child = input.getInputs().get(i);
            if ((child.getOp() instanceof LogicalValuesOperator) &&
                    ((LogicalValuesOperator) child.getOp()).getRows().isEmpty()) {
                firstIsEmpty = i == 0 || firstIsEmpty;
                continue;
            }

            newInputs.add(child);
            childOutputs.add(exceptOperator.getChildOutputColumns().get(i));
        }

        if (firstIsEmpty) {
            return Lists.newArrayList(OptExpression
                    .create(new LogicalValuesOperator(exceptOperator.getOutputColumnRefOp(), Collections.emptyList())));
        } else if (newInputs.size() > 1) {
            return Lists.newArrayList(OptExpression
                    .create(new LogicalExceptOperator.Builder().withOperator(exceptOperator)
                            .setChildOutputColumns(childOutputs).build(), newInputs));
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (List<ColumnRefOperator> childOutputColumn : exceptOperator.getChildOutputColumns()) {
            if (newInputs.get(0).getOutputColumns().isIntersect(new ColumnRefSet(childOutputColumn))) {
                for (int i = 0; i < exceptOperator.getOutputColumnRefOp().size(); i++) {
                    projectMap.put(exceptOperator.getOutputColumnRefOp().get(i), childOutputColumn.get(i));
                }
                break;
            }
        }

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);
        return Lists.newArrayList(OptExpression.create(projectOperator, newInputs));

    }
}
