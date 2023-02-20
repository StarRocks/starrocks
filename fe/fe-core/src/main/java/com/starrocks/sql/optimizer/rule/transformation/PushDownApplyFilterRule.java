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
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownApplyFilterRule extends TransformationRule {
    public PushDownApplyFilterRule() {
        super(RuleType.TF_PUSH_DOWN_APPLY_FILTER, Pattern.create(OperatorType.LOGICAL_APPLY).addChildren(
                Pattern.create(OperatorType.PATTERN_LEAF),
                Pattern.create(OperatorType.LOGICAL_FILTER, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return SubqueryUtils.containsCorrelationSubquery(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        OptExpression child = input.getInputs().get(1);
        LogicalFilterOperator filter = (LogicalFilterOperator) child.getOp();

        List<ScalarOperator> predicates = Utils.extractConjuncts(filter.getPredicate());

        // if filter is correlation, set in ApplyNode's correlation predicate(will use in transform ApplyNode to Join)
        // else set in ApplyNode's un-correlation predicate(will use in transform Exists/In Apply to Join)
        List<ScalarOperator> correlationPredicates = Lists.newArrayList();
        List<ScalarOperator> unCorrelationPredicates = Lists.newArrayList();

        for (ScalarOperator scalarOperator : predicates) {
            if (Utils.containAnyColumnRefs(apply.getCorrelationColumnRefs(), scalarOperator)) {
                correlationPredicates.add(scalarOperator);
            } else {
                unCorrelationPredicates.add(scalarOperator);
            }
        }

        ScalarOperator correlationPredicate = apply.getCorrelationConjuncts();
        ScalarOperator unCorrelationPredicate = apply.getPredicate();
        if (!correlationPredicates.isEmpty()) {
            correlationPredicates.add(apply.getCorrelationConjuncts());
            correlationPredicate = Utils.compoundAnd(correlationPredicates);
        }

        // save un-correlation predicate on apply node
        if (!unCorrelationPredicates.isEmpty()) {
            unCorrelationPredicates.add(apply.getPredicate());
            unCorrelationPredicate = Utils.compoundAnd(unCorrelationPredicates);
        }

        LogicalApplyOperator newApply = LogicalApplyOperator.builder().withOperator(apply)
                .setCorrelationConjuncts(correlationPredicate)
                .setPredicate(unCorrelationPredicate).build();

        OptExpression newApplyOptExpression = new OptExpression(newApply);
        newApplyOptExpression.getInputs().add(input.getInputs().get(0));
        newApplyOptExpression.getInputs().addAll(child.getInputs());

        return Lists.newArrayList(newApplyOptExpression);
    }
}
