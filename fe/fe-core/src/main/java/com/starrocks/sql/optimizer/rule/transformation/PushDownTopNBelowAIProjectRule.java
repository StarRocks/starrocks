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
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/** Reduces AI input rows when ordering depends only on identity pass-through columns. */
public final class PushDownTopNBelowAIProjectRule extends TransformationRule {
    public PushDownTopNBelowAIProjectRule() {
        // RBO-only: inspect the full child tree, including an optional ordinary Project above AIProject.
        super(RuleType.TF_PUSH_DOWN_TOPN_AI_PROJECT,
                Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableAiTopnPushdown()) {
            return false;
        }
        LogicalTopNOperator topN = input.getOp().cast();
        if (!topN.hasLimit() || topN.getLimit() <= 0 || topN.hasOffset()
                || topN.getSortPhase() != SortPhase.FINAL || topN.isSplit() || topN.isPerPipeline()
                || topN.getTopNType() != TopNType.ROW_NUMBER
                || CollectionUtils.isEmpty(topN.getOrderByElements())
                || CollectionUtils.isNotEmpty(topN.getPartitionByColumns())
                || topN.getPartitionLimit() != Operator.DEFAULT_LIMIT
                || (topN.getPartitionPreAggCall() != null && !topN.getPartitionPreAggCall().isEmpty())
                || hasPredicate(topN)) {
            return false;
        }

        OptExpression child = input.inputAt(0);
        LogicalProjectOperator project = null;
        if (child.getOp() instanceof LogicalProjectOperator) {
            project = child.getOp().cast();
            if (hasBarrier(project) || child.arity() != 1
                    || project.getColumnRefMap().values().stream()
                            .anyMatch(AiFunctionExtractor::containsNonReusableExpression)) {
                return false;
            }
            child = child.inputAt(0);
        }
        if (!(child.getOp() instanceof LogicalAIProjectOperator aiProject)
                || child.arity() != 1 || hasBarrier(aiProject)) {
            return false;
        }

        Operator aiChild = child.inputAt(0).getOp();
        // A pre-existing bound can be local and prevents SplitTopNRule from adding a global stage.
        // Leave bounded inputs unchanged. This also makes repeated rewrites idempotent.
        if (aiChild.hasLimit()) {
            return false;
        }
        ColumnRefSet childOutputs = child.inputAt(0).getOutputColumns();
        for (Ordering ordering : topN.getOrderByElements()) {
            ColumnRefOperator column = ordering.getColumnRef();
            if (!childOutputs.contains(column) || !column.equals(aiProject.getColumnRefMap().get(column))
                    || (project != null && !column.equals(project.getColumnRefMap().get(column)))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        if (!check(input, context)) {
            return List.of();
        }
        LogicalTopNOperator topN = input.getOp().cast();
        OptExpression child = input.inputAt(0);
        boolean hasProject = child.getOp() instanceof LogicalProjectOperator;
        OptExpression aiExpression = hasProject ? child.inputAt(0) : child;
        LogicalAIProjectOperator aiProject = aiExpression.getOp().cast();

        // Small limits use the standard global TopN path. For larger limits, keep candidates in
        // each input fragment instead of gathering all AI work into one instance.
        SortPhase candidatePhase = topN.getLimit() <= context.getSessionVariable().getAiTopnPushdownMaxGlobalLimit()
                ? SortPhase.FINAL : SortPhase.PARTIAL;
        // Keep all prompt/common-expression inputs and ordinary per-instance (not per-pipeline) semantics.
        LogicalTopNOperator candidates = new LogicalTopNOperator(
                topN.getOrderByElements(), topN.getLimit(), Operator.DEFAULT_OFFSET, candidatePhase);
        OptExpression rewritten = OptExpression.create(LogicalAIProjectOperator.builder().withOperator(aiProject).build(),
                OptExpression.create(candidates, aiExpression.inputAt(0)));
        if (hasProject) {
            rewritten = OptExpression.create(
                    LogicalProjectOperator.builder().withOperator(child.getOp().cast()).build(), rewritten);
        }
        // Keep the original TopN to enforce the final ordering, limit, and projection.
        // Candidate reduction alone, especially per-fragment reduction, does not replace it.
        return List.of(OptExpression.create(LogicalTopNOperator.builder().withOperator(topN).build(), rewritten));
    }

    private static boolean hasBarrier(Operator operator) {
        return operator.hasLimit() || operator.getProjection() != null || hasPredicate(operator);
    }

    private static boolean hasPredicate(Operator operator) {
        return operator.getPredicate() != null
                || (operator.getPredicateCommonOperators() != null && !operator.getPredicateCommonOperators().isEmpty());
    }
}
