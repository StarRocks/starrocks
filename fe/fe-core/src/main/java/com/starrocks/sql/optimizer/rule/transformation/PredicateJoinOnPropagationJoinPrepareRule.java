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
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.JoinPredicatePushdown;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PredicateJoinOnPropagationJoinPrepareRule extends TransformationRule {
    public PredicateJoinOnPropagationJoinPrepareRule() {
        super(RuleType.TF_PREDICATE_JOIN_ON_PROPAGATION_JOIN_PREPARE, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        OptExpression joinOptExpression = input.getInputs().get(0);
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) joinOptExpression.getOp();
        if (!joinOperator.getJoinType().isInnerJoin()) {
            return false;
        } else {
            return shouldPushDownPredicate(input);
        }
    }

    private boolean shouldPushDownPredicate(OptExpression input) {
        List<ColumnRefOperator> joinOnColumnRefs = getJoinOnColumn(input.getInputs().get(0));
        ScalarOperator filterScalarOperator = input.getOp().getPredicate();
        List<ScalarOperator> filterPredicateList = Utils.extractConjuncts(filterScalarOperator);
        for (ScalarOperator filterPredicate : filterPredicateList) {
            List<ColumnRefOperator> filterPredicateColumnRefs = Utils.extractColumnRef(filterPredicate);
            for (ColumnRefOperator filterPredicateColumnRef : filterPredicateColumnRefs) {
                if (!joinOnColumnRefs.contains(filterPredicateColumnRef) || filterScalarOperator.isPushdown()) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<ColumnRefOperator> getJoinOnColumn(OptExpression joinOptExpression) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) joinOptExpression.getOp();
        ScalarOperator joinOnPredicate = joinOperator.getOnPredicate();
        List<ScalarOperator> joinOnPredicateList = Utils.extractConjuncts(joinOnPredicate);
        List<BinaryPredicateOperator> joinOnConjunctsList = JoinHelper.getEqualsPredicate(
                joinOptExpression.inputAt(0).getOutputColumns(),
                joinOptExpression.inputAt(1).getOutputColumns(), joinOnPredicateList);
        List<ColumnRefOperator> joinOnColumnRefs = Lists.newLinkedList();
        for (BinaryPredicateOperator eqPredicate : joinOnConjunctsList) {
            joinOnColumnRefs.addAll(Utils.extractColumnRef(eqPredicate));
        }
        return joinOnColumnRefs;
    }


    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOperator = (LogicalFilterOperator) input.getOp();
        OptExpression joinOpt = input.getInputs().get(0);
        JoinPredicatePushdown joinPredicatePushdown = new JoinPredicatePushdown(
                joinOpt, false, false, context.getColumnRefFactory(),
                context.isEnableLeftRightJoinEquivalenceDerive(), context);
        OptExpression newJoinOpt = joinPredicatePushdown.pushdown(filterOperator.getPredicate());
        setPushDownTag(newJoinOpt.getInputs().get(0));
        setPushDownTag(newJoinOpt.getInputs().get(1));
        return Lists.newArrayList(newJoinOpt);
    }

    private void setPushDownTag(OptExpression joinChildOpt) {
        if (joinChildOpt.getOp() instanceof LogicalFilterOperator) {
            joinChildOpt.getOp().getPredicate().setIsPushdown(true);
        }
    }
}
