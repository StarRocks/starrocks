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
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PushDownTopNWithLeftJoinRule extends TransformationRule {
    public PushDownTopNWithLeftJoinRule() {
        super(RuleType.TF_PUSH_DOWN_TOPN_LEFT_JOIN,
                Pattern.create(OperatorType.LOGICAL_TOPN).addChildren(Pattern.create(OperatorType.LOGICAL_JOIN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // TODO check orderings
        if (!context.getSessionVariable().isEnablePipelineEngine()) {
            return false;
        }

        OptExpression childExpr = input.inputAt(0);
        LogicalJoinOperator joinOperator = childExpr.getOp().cast();

        if (!joinOperator.getJoinType().isLeftTransform()) {
            return false;
        }

        OptExpression joinLeftChild = childExpr.inputAt(0);
        if (joinLeftChild.getOp() instanceof LogicalTopNOperator) {
            return false;
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // TODO process project
        LogicalTopNOperator topNOp = input.getOp().cast();
        OptExpression childExpr = input.inputAt(0);
        LogicalJoinOperator joinOperator = childExpr.getOp().cast();

        OptExpression joinLeftChild = childExpr.inputAt(0);

        List<Ordering> orderings = topNOp.getOrderByElements();
        List<Ordering> newOrderings = new ArrayList<>();
        for (Ordering ordering : orderings) {
            Ordering newOrdering = new Ordering(ordering.getColumnRef(), ordering.isAscending(), ordering.isNullsFirst());
            newOrderings.add(newOrdering);
        }

        OptExpression newTopNOp = OptExpression.create(new LogicalTopNOperator.Builder()
                .setOrderByElements(newOrderings)
                .setLimit(topNOp.getLimit())
                .setTopNType(topNOp.getTopNType())
                .setSortPhase(topNOp.getSortPhase())
                .build(), childExpr.inputAt(0));

        OptExpression newJoinOperator = OptExpression.create(joinOperator, Lists.newArrayList(newTopNOp, childExpr.inputAt(1)));
        return Collections.singletonList(OptExpression.create(topNOp, newJoinOperator));
    }
}
