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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PushDownTopNBelowOuterJoinRule extends TransformationRule {
    public PushDownTopNBelowOuterJoinRule() {
        super(RuleType.TF_PUSH_DOWN_TOPN_OUTER_JOIN,
                Pattern.create(OperatorType.LOGICAL_TOPN).addChildren(
                        Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topn = (LogicalTopNOperator) input.getOp();

        if (!topn.hasLimit() || topn.getLimit() > context.getSessionVariable().getCboPushDownTopNLimit()) {
            return false;
        }

        if (topn.hasOffset()) {
            return false;
        }

        OptExpression childExpr = input.inputAt(0);
        LogicalJoinOperator joinOperator = childExpr.getOp().cast();
        JoinOperator joinType = joinOperator.getJoinType();

        if (!joinType.isLeftOuterJoin() && !joinType.isRightOuterJoin()) {
            return false;
        }

        if (!Strings.isNullOrEmpty(joinOperator.getJoinHint())) {
            return false;
        }

        if (joinOperator.getPredicate() != null) {
            return false;
        }

        OptExpression joinChild = null;
        if (joinType.isLeftOuterJoin()) {
            joinChild = childExpr.inputAt(0);
        } else if (joinType.isRightJoin()) {
            joinChild = childExpr.inputAt(1);
        }
        Preconditions.checkState(joinChild != null);

        if (topn.hasLimit() && joinChild.getOp().hasLimit() && topn.getLimit() >= joinChild.getOp().getLimit()) {
            return false;
        }

        List<Integer> colIds = topn.getOrderByElements().stream()
                .map(Ordering::getColumnRef)
                .map(ColumnRefOperator::getId)
                .collect(Collectors.toList());

        return joinChild.getOutputColumns().containsAll(colIds);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topn = input.getOp().cast();
        OptExpression childExpr = input.inputAt(0);
        LogicalJoinOperator joinOperator = childExpr.getOp().cast();

        OptExpression joinChildWithSort;
        if (joinOperator.getJoinType().isLeftOuterJoin()) {
            joinChildWithSort = childExpr.inputAt(0);
        } else {
            joinChildWithSort = childExpr.inputAt(1);
        }

        OptExpression newTopNOperator = OptExpression.create(new LogicalTopNOperator.Builder()
                .setOrderByElements(topn.getOrderByElements())
                .setLimit(topn.getLimit())
                .setTopNType(topn.getTopNType())
                .setSortPhase(topn.getSortPhase())
                .setIsSplit(false)
                .build(), joinChildWithSort);

        OptExpression newJoinOperator;
        if (joinOperator.getJoinType().isLeftOuterJoin()) {
            newJoinOperator = OptExpression.create(joinOperator,
                    Lists.newArrayList(newTopNOperator, childExpr.inputAt(1)));
        } else {
            newJoinOperator = OptExpression.create(joinOperator,
                    Lists.newArrayList(childExpr.inputAt(0), newTopNOperator));
        }

        return Collections.singletonList(OptExpression.create(topn, newJoinOperator));
    }
}
