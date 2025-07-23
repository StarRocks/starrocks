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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/*
 *          Filter          CTEConsume(Predicate)
 *            |                 |
 *        CTEConsume   =>     Filter
 *
 * */
public class PushDownPredicateCTEConsumeRule extends TransformationRule {
    public PushDownPredicateCTEConsumeRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_CTE_CONSUME, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.LOGICAL_CTE_CONSUME)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {

        LogicalFilterOperator filter = (LogicalFilterOperator) input.getOp();
        OptExpression child = input.getInputs().get(0);
        LogicalCTEConsumeOperator consume = child.getOp().cast();

        ScalarOperator mergedPredicate = Utils.compoundAnd(filter.getPredicate(), consume.getPredicate());

        LogicalCTEConsumeOperator newConsume = new LogicalCTEConsumeOperator.Builder()
                .withOperator(consume).setPredicate(mergedPredicate).build();

        if (child.getInputs().isEmpty()) {
            return Lists.newArrayList(OptExpression.create(newConsume));
        } else {
            OptExpression output = OptExpression.create(newConsume, OptExpression.create(filter, child.getInputs()));
            return Lists.newArrayList(output);
        }
    }
}
