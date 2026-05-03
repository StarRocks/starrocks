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
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SplitTopNOnGroupKeyAggRule extends TransformationRule {
    private static final SplitTopNOnGroupKeyAggRule INSTANCE = new SplitTopNOnGroupKeyAggRule();

    private SplitTopNOnGroupKeyAggRule() {
        super(RuleType.TF_SPLIT_TOPN_ON_GROUP_KEY_AGG_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.PATTERN_LEAF));
    }

    public static SplitTopNOnGroupKeyAggRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return match(input, context.getSessionVariable().getCboPushDownTopNLimit()).isPresent();
    }

    public boolean canRewrite(OptExpression input, OptimizerContext context) {
        return check(input, context);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        Optional<MatchContext> match = match(input, context.getSessionVariable().getCboPushDownTopNLimit());
        if (match.isEmpty()) {
            return List.of();
        }

        LogicalTopNOperator topN = input.getOp().cast();
        Preconditions.checkState(topN.getLimit() < 0 || topN.getLimit() + topN.getOffset() >= 0,
                String.format("limit(%d) + offset(%d) is too large and yields an overflow result(%d)",
                        topN.getLimit(), topN.getOffset(), topN.getLimit() + topN.getOffset()));
        long limit = topN.getLimit() + topN.getOffset();

        LogicalTopNOperator finalSort = LogicalTopNOperator.builder()
                .withOperator(topN)
                .setIsSplit(true)
                .setOrderByElements(match.get().rewrittenOrderings)
                .build();
        LogicalTopNOperator partialSort = new LogicalTopNOperator(
                match.get().rewrittenOrderings, limit, Operator.DEFAULT_OFFSET, SortPhase.PARTIAL);

        OptExpression rewritten = OptExpression.create(finalSort,
                OptExpression.create(partialSort, match.get().aggExpression));
        List<OptExpression> projectChain = match.get().projectChain;
        for (int i = projectChain.size() - 1; i >= 0; i--) {
            rewritten = OptExpression.create(projectChain.get(i).getOp(), rewritten);
        }
        return Lists.newArrayList(rewritten);
    }

    private Optional<MatchContext> match(OptExpression input, long maxLimit) {
        if (!(input.getOp() instanceof LogicalTopNOperator) || input.getInputs().size() != 1) {
            return Optional.empty();
        }

        LogicalTopNOperator topN = input.getOp().cast();
        if (!topN.hasLimit() || topN.getLimit() > maxLimit || topN.hasOffset() || topN.getPredicate() != null ||
                !topN.getSortPhase().isFinal() || topN.isSplit()) {
            return Optional.empty();
        }

        List<OptExpression> projectChain = new ArrayList<>();
        OptExpression cur = input.inputAt(0);
        while (cur.getInputs().size() == 1 && cur.getOp() instanceof LogicalProjectOperator) {
            LogicalProjectOperator project = cur.getOp().cast();
            if (project.hasLimit() || !isColumnRefProject(project)) {
                return Optional.empty();
            }
            projectChain.add(cur);
            cur = cur.inputAt(0);
        }

        if (!(cur.getOp() instanceof LogicalAggregationOperator)) {
            return Optional.empty();
        }
        LogicalAggregationOperator agg = cur.getOp().cast();
        if (agg.getPredicate() != null) {
            return Optional.empty();
        }

        List<Ordering> rewrittenOrderings = new ArrayList<>();
        List<ColumnRefOperator> mappedOrderByColumns = new ArrayList<>();
        for (Ordering ordering : topN.getOrderByElements()) {
            ColumnRefOperator colRef = ordering.getColumnRef();
            for (OptExpression projectExpr : projectChain) {
                LogicalProjectOperator project = projectExpr.getOp().cast();
                ScalarOperator mapped = project.getColumnRefMap().get(colRef);
                if (!(mapped instanceof ColumnRefOperator)) {
                    return Optional.empty();
                }
                colRef = (ColumnRefOperator) mapped;
            }
            mappedOrderByColumns.add(colRef);
            rewrittenOrderings.add(new Ordering(colRef, ordering.isAscending(), ordering.isNullsFirst()));
        }

        List<ColumnRefOperator> groupingKeys = agg.getGroupingKeys();
        if (!groupingKeys.containsAll(mappedOrderByColumns) || !mappedOrderByColumns.containsAll(groupingKeys)) {
            return Optional.empty();
        }

        return Optional.of(new MatchContext(projectChain, cur, rewrittenOrderings));
    }

    private boolean isColumnRefProject(LogicalProjectOperator project) {
        return project.getColumnRefMap().values().stream().allMatch(ScalarOperator::isColumnRef);
    }

    private static class MatchContext {
        private final List<OptExpression> projectChain;
        private final OptExpression aggExpression;
        private final List<Ordering> rewrittenOrderings;

        private MatchContext(List<OptExpression> projectChain, OptExpression aggExpression,
                             List<Ordering> rewrittenOrderings) {
            this.projectChain = projectChain;
            this.aggExpression = aggExpression;
            this.rewrittenOrderings = rewrittenOrderings;
        }
    }
}
