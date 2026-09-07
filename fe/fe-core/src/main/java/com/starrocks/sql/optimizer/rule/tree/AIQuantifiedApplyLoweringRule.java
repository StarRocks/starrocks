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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.AiFunctionExtractor;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Lowers AI calls in the left operands of quantified Apply operators before
 * subquery decorrelation turns them into semi/anti joins.
 */
public final class AIQuantifiedApplyLoweringRule implements TreeRewriteRule {
    private boolean hasRewrite;

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        hasRewrite = false;
        OptExpression result = rewriteBottomUp(root, taskContext.getOptimizerContext());
        if (hasRewrite) {
            result.clearStatsAndInitOutputInfo();
            deriveLogicalProperties(result);
        }
        return result;
    }

    public boolean hasRewrite() {
        return hasRewrite;
    }

    private OptExpression rewriteBottomUp(OptExpression root, OptimizerContext context) {
        boolean childChanged = false;
        for (int childIndex = 0; childIndex < root.arity(); childIndex++) {
            OptExpression originalChild = root.inputAt(childIndex);
            OptExpression rewrittenChild = rewriteBottomUp(originalChild, context);
            if (rewrittenChild != originalChild) {
                root.setChild(childIndex, rewrittenChild);
                childChanged = true;
            }
        }
        if (childChanged || root.getLogicalProperty() == null) {
            root.deriveLogicalPropertyItself();
        }

        if (!(root.getOp() instanceof LogicalApplyOperator apply) || !apply.isQuantified()) {
            return root;
        }
        if (apply.getSubqueryOperator() instanceof InPredicateOperator inPredicate) {
            return lowerInPredicate(root, apply, inPredicate, context);
        }
        if (apply.getSubqueryOperator() instanceof MultiInPredicateOperator multiInPredicate) {
            return lowerMultiInPredicate(root, apply, multiInPredicate, context);
        }
        return root;
    }

    private OptExpression lowerInPredicate(OptExpression input, LogicalApplyOperator apply,
                                           InPredicateOperator predicate, OptimizerContext context) {
        ScalarOperator leftOperand = predicate.getChild(0);
        if (!AiFunctionExtractor.containsAI(leftOperand)) {
            return input;
        }
        validateAIInputs(leftOperand, input.inputAt(0).getOutputColumns());

        AiFunctionExtractor.Result result = AiFunctionExtractor.extract(
                input.inputAt(0), List.of(leftOperand), context);
        List<ScalarOperator> arguments = new ArrayList<>(predicate.getChildren());
        arguments.set(0, result.expressions().get(0));
        InPredicateOperator rewritten = new InPredicateOperator(
                predicate.isNotIn(), predicate.isSubquery(), arguments.toArray(ScalarOperator[]::new));
        return rebuildApply(input, apply, result.root(), rewritten, context);
    }

    private OptExpression lowerMultiInPredicate(OptExpression input, LogicalApplyOperator apply,
                                                MultiInPredicateOperator predicate, OptimizerContext context) {
        List<ScalarOperator> leftOperands = predicate.getChildren().subList(0, predicate.getTupleSize());
        if (leftOperands.stream().noneMatch(AiFunctionExtractor::containsAI)) {
            return input;
        }
        ColumnRefSet leftOutputs = input.inputAt(0).getOutputColumns();
        leftOperands.forEach(operand -> validateAIInputs(operand, leftOutputs));

        AiFunctionExtractor.Result result = AiFunctionExtractor.extract(
                input.inputAt(0), leftOperands, context);
        List<ScalarOperator> arguments = new ArrayList<>(predicate.getChildren());
        for (int index = 0; index < predicate.getTupleSize(); index++) {
            arguments.set(index, result.expressions().get(index));
        }
        MultiInPredicateOperator rewritten = new MultiInPredicateOperator(
                predicate.isNotIn(), arguments, predicate.getTupleSize());
        return rebuildApply(input, apply, result.root(), rewritten, context);
    }

    private OptExpression rebuildApply(OptExpression input, LogicalApplyOperator apply,
                                       OptExpression leftChild, ScalarOperator subqueryOperator,
                                       OptimizerContext context) {
        ColumnRefSet originalOutputs = input.getOutputColumns();
        LogicalApplyOperator rewrittenApply = LogicalApplyOperator.builder()
                .withOperator(apply)
                .setSubqueryOperator(subqueryOperator)
                .build();
        OptExpression root = derived(OptExpression.create(
                rewrittenApply, leftChild, input.inputAt(1)));
        if (!originalOutputs.equals(root.getOutputColumns())) {
            root = AiFunctionExtractor.retainOutputs(root, originalOutputs, context);
        }
        hasRewrite = true;
        return root;
    }

    private static void validateAIInputs(ScalarOperator expression, ColumnRefSet leftOutputs) {
        if (AiFunctionExtractor.isAICall(expression)) {
            CallOperator call = expression.cast();
            ColumnRefSet semanticInputs = new ColumnRefSet();
            int semanticArity = Math.min(call.getFunction().getNumArgs(), call.getChildren().size());
            for (int index = 0; index < semanticArity; index++) {
                ScalarOperator argument = call.getChild(index);
                semanticInputs.union(argument.getUsedColumns());
                validateAIInputs(argument, leftOutputs);
            }
            if (!leftOutputs.containsAll(semanticInputs)) {
                throw new IllegalStateException(
                        "AI function in quantified predicate must reference only the left Apply input");
            }
            return;
        }
        expression.getChildren().forEach(child -> validateAIInputs(child, leftOutputs));
    }

    private static OptExpression derived(OptExpression expression) {
        expression.deriveLogicalPropertyItself();
        return expression;
    }

    private static void deriveLogicalProperties(OptExpression expression) {
        expression.getInputs().forEach(AIQuantifiedApplyLoweringRule::deriveLogicalProperties);
        expression.deriveLogicalPropertyItself();
    }
}
