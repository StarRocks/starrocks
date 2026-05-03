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
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SplitTopNRule extends TransformationRule {
    private SplitTopNRule() {
        super(RuleType.TF_SPLIT_TOPN, Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.PATTERN_LEAF));
    }

    private static final SplitTopNRule INSTANCE = new SplitTopNRule();

    public static SplitTopNRule getInstance() {
        return INSTANCE;
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topN = (LogicalTopNOperator) input.getOp();
        LogicalOperator child = (LogicalOperator) input.getInputs().get(0).getOp();
        // Only apply this rule if the sort phase is final and not split
        return topN.getSortPhase().isFinal() && !topN.isSplit() && !child.hasLimit();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator src = (LogicalTopNOperator) input.getOp();

        Preconditions.checkState(src.getLimit() < 0 || src.getLimit() + src.getOffset() >= 0,
                String.format("limit(%d) + offset(%d) is too large and yields an overflow result(%d)", src.getLimit(),
                        src.getOffset(), src.getLimit() + src.getOffset()));
        long limit = src.getLimit() + src.getOffset();
        LogicalTopNOperator finalSort = LogicalTopNOperator.builder().withOperator(src).setIsSplit(true).build();

        Optional<OptExpression> splitThroughProject = splitThroughProject(input, src, finalSort, limit);
        if (splitThroughProject.isPresent()) {
            return Lists.newArrayList(splitThroughProject.get());
        }

        LogicalTopNOperator partialSort = new LogicalTopNOperator(
                src.getOrderByElements(), limit, Operator.DEFAULT_OFFSET, SortPhase.PARTIAL);

        OptExpression partialSortExpression = OptExpression.create(partialSort, input.getInputs());
        OptExpression finalSortExpression = OptExpression.create(finalSort, partialSortExpression);
        return Lists.newArrayList(finalSortExpression);
    }

    // Pushes a PARTIAL TopN through one or more stacked identity-like Project nodes so that
    // PushDownTopNToPreAggRule can later see TopN(PARTIAL) → Agg(GLOBAL) → Agg(LOCAL) directly.
    private Optional<OptExpression> splitThroughProject(OptExpression input, LogicalTopNOperator src,
                                                        LogicalTopNOperator finalSort, long limit) {
        if (input.getInputs().size() != 1 || !(input.inputAt(0).getOp() instanceof LogicalProjectOperator)) {
            return Optional.empty();
        }

        // Collect the chain of consecutive, non-limiting Project nodes above the agg.
        List<OptExpression> projectChain = new ArrayList<>();
        OptExpression cur = input.inputAt(0);
        while (cur.getInputs().size() == 1 && cur.getOp() instanceof LogicalProjectOperator) {
            LogicalProjectOperator proj = cur.getOp().cast();
            if (proj.hasLimit()) {
                return Optional.empty();
            }
            projectChain.add(cur);
            cur = cur.inputAt(0);
        }

        // Map each ordering column ref through the entire project chain (outermost → innermost).
        List<Ordering> rewrittenOrderings = new ArrayList<>();
        for (Ordering ordering : src.getOrderByElements()) {
            ColumnRefOperator colRef = ordering.getColumnRef();
            for (OptExpression projectExpr : projectChain) {
                LogicalProjectOperator proj = projectExpr.getOp().cast();
                ScalarOperator mapped = proj.getColumnRefMap().get(colRef);
                if (!(mapped instanceof ColumnRefOperator)) {
                    return Optional.empty();
                }
                colRef = (ColumnRefOperator) mapped;
            }
            rewrittenOrderings.add(new Ordering(colRef, ordering.isAscending(), ordering.isNullsFirst()));
        }

        // Place PARTIAL TopN below all projects, then rebuild the chain bottom-up.
        // Result: TopN(FINAL,split) → P1 → ... → Pn → TopN(PARTIAL) → <rest>
        LogicalTopNOperator partialSort = new LogicalTopNOperator(
                rewrittenOrderings, limit, Operator.DEFAULT_OFFSET, SortPhase.PARTIAL);
        OptExpression rebuilt = OptExpression.create(partialSort, cur);
        for (int i = projectChain.size() - 1; i >= 0; i--) {
            rebuilt = OptExpression.create(projectChain.get(i).getOp(), rebuilt);
        }
        return Optional.of(OptExpression.create(finalSort, rebuilt));
    }
}
