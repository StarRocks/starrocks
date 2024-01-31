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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.JoinPredicatePushdown;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class PushDownJoinOnClauseRule extends TransformationRule {
    public PushDownJoinOnClauseRule() {
        super(RuleType.TF_PUSH_DOWN_JOIN_CLAUSE, Pattern.create(OperatorType.LOGICAL_JOIN).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();

        if (joinOperator.hasPushDownJoinOnClause()) {
            return false;
        }
        return joinOperator.getOnPredicate() != null;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        List<OptExpression> children = Lists.newArrayList(input.getInputs());
        LogicalJoinOperator join = (LogicalJoinOperator) input.getOp();
        ScalarOperator on = join.getOnPredicate();
        JoinPredicatePushdown joinPredicatePushdown = new JoinPredicatePushdown(
                input, true, false, context.getColumnRefFactory(),
                context.isEnableLeftRightJoinEquivalenceDerive(), context);
        OptExpression root = joinPredicatePushdown.pushdown(join.getOnPredicate());
        ((LogicalJoinOperator) root.getOp()).setHasPushDownJoinOnClause(true);
        if (root.getOp().equals(input.getOp()) && on.equals(join.getOnPredicate()) &&
                children.equals(root.getInputs())) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(root);
    }
}
