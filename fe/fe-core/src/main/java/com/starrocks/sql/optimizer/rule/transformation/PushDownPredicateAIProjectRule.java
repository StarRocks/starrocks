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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;

/**
 * Pushes deterministic predicates that depend only on pass-through child columns below an
 * AIProject. An AI call is never duplicated, and a predicate never moves below the operator
 * that produces one of its inputs.
 */
public final class PushDownPredicateAIProjectRule extends TransformationRule {
    public PushDownPredicateAIProjectRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_AI_PROJECT,
                Pattern.create(OperatorType.LOGICAL_FILTER)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AI_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        OptExpression aiProjectExpression = input.inputAt(0);
        LogicalAIProjectOperator aiProject = aiProjectExpression.getOp().cast();
        // A limit is not transparent to filtering. Generic predicate/projection state would also
        // require expression rewriting that is outside this pass-through-only rule.
        if (aiProject.hasLimit() || aiProject.getPredicate() != null || aiProject.getProjection() != null) {
            return false;
        }

        ColumnRefSet childOutputs = aiProjectExpression.inputAt(0).getOutputColumns();
        return Utils.extractConjuncts(input.getOp().getPredicate()).stream()
                .anyMatch(predicate -> canPushDown(predicate, childOutputs));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filter = input.getOp().cast();
        OptExpression aiProjectExpression = input.inputAt(0);
        LogicalAIProjectOperator aiProject = aiProjectExpression.getOp().cast();
        ColumnRefSet childOutputs = aiProjectExpression.inputAt(0).getOutputColumns();

        List<ScalarOperator> pushDownPredicates = new ArrayList<>();
        List<ScalarOperator> residualPredicates = new ArrayList<>();
        for (ScalarOperator predicate : Utils.extractConjuncts(filter.getPredicate())) {
            if (canPushDown(predicate, childOutputs)) {
                pushDownPredicates.add(predicate);
            } else {
                residualPredicates.add(predicate);
            }
        }
        if (pushDownPredicates.isEmpty()) {
            return List.of();
        }
        if (residualPredicates.isEmpty() && hasFilterDecoration(filter)) {
            return List.of();
        }

        OptExpression pushedFilter = OptExpression.create(
                new LogicalFilterOperator(Utils.compoundAnd(pushDownPredicates)),
                aiProjectExpression.inputAt(0));
        LogicalAIProjectOperator rewrittenAIProject = LogicalAIProjectOperator.builder()
                .withOperator(aiProject)
                .build();
        OptExpression rewrittenRoot = OptExpression.create(rewrittenAIProject, pushedFilter);

        if (!residualPredicates.isEmpty()) {
            LogicalFilterOperator residualFilter = new LogicalFilterOperator.Builder()
                    .withOperator(filter)
                    .setPredicate(Utils.compoundAnd(residualPredicates))
                    .build();
            rewrittenRoot = OptExpression.create(residualFilter, rewrittenRoot);
        }
        return List.of(rewrittenRoot);
    }

    private static boolean canPushDown(ScalarOperator predicate, ColumnRefSet childOutputs) {
        return Utils.canPushDownPredicate(predicate)
                && childOutputs.containsAll(predicate.getUsedColumns());
    }

    private static boolean hasFilterDecoration(LogicalFilterOperator filter) {
        boolean hasPredicateCommonOperators = filter.getPredicateCommonOperators() != null
                && !filter.getPredicateCommonOperators().isEmpty();
        return filter.hasLimit() || filter.getProjection() != null
                || hasPredicateCommonOperators;
    }
}
