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
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class PushDownApplyLimitFilterRule extends TransformationRule {
    public PushDownApplyLimitFilterRule() {
        super(RuleType.TF_PUSH_DOWN_APPLY_FILTER,
                Pattern.create(OperatorType.LOGICAL_APPLY).addChildren(
                        Pattern.create(OperatorType.PATTERN_LEAF),
                        Pattern.create(OperatorType.LOGICAL_LIMIT).addChildren(
                                Pattern.create(OperatorType.LOGICAL_FILTER).addChildren(
                                        Pattern.create(OperatorType.PATTERN_LEAF)))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!SubqueryUtils.containsCorrelationSubquery(input)) {
            return false;
        }

        LogicalLimitOperator limitOperator = (LogicalLimitOperator) input.getInputs().get(1).getOp();
        return limitOperator.getLimit() == 1 && !limitOperator.hasOffset();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        OptExpression limitExpression = input.getInputs().get(1);
        LogicalLimitOperator limitOperator = (LogicalLimitOperator) limitExpression.getOp();

        OptExpression filterExpression = limitExpression.getInputs().get(0);
        LogicalFilterOperator filter = (LogicalFilterOperator) filterExpression.getOp();

        List<ScalarOperator> predicates = Utils.extractConjuncts(filter.getPredicate());

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
        if (!correlationPredicates.isEmpty()) {
            if (correlationPredicate != null) {
                correlationPredicates.add(correlationPredicate);
            }
            correlationPredicate = Utils.compoundAnd(correlationPredicates);
        }

        LogicalApplyOperator newApply = LogicalApplyOperator.builder().withOperator(apply)
                .setCorrelationConjuncts(correlationPredicate)
                .setPredicate(apply.getPredicate())
                .setNeedCheckMaxRows(false)
                .build();

        OptExpression newApplyOptExpression = new OptExpression(newApply);
        newApplyOptExpression.getInputs().add(input.getInputs().get(0));

        // Keep full filter predicate below LIMIT to preserve semantics
        OptExpression newFilterExpression = OptExpression.create(new LogicalFilterOperator(filter.getPredicate()),
                filterExpression.getInputs().get(0));

        OptExpression newLimitExpression = new OptExpression(limitOperator);
        newLimitExpression.getInputs().add(newFilterExpression);
        newApplyOptExpression.getInputs().add(newLimitExpression);

        return Lists.newArrayList(newApplyOptExpression);
    }
}
