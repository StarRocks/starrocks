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
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class PruneEmptyJoinRule extends TransformationRule {
    public static PruneEmptyJoinRule JOIN_LEFT_EMPTY = new PruneEmptyJoinRule(0);
    public static PruneEmptyJoinRule JOIN_RIGHT_EMPTY = new PruneEmptyJoinRule(1);

    private final int emptyIndex;

    private PruneEmptyJoinRule(int index) {
        super(RuleType.TF_PRUNE_EMPTY_JOIN, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
        this.emptyIndex = index;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!OperatorType.LOGICAL_VALUES.equals(input.inputAt(emptyIndex).getOp().getOpType())) {
            return false;
        }
        LogicalValuesOperator v = input.inputAt(emptyIndex).getOp().cast();
        return v.getRows().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = input.getOp().cast();
        JoinOperator type = join.getJoinType();

        int joinIndex; // 0 left, 1 right
        if (type.isInnerJoin() || type.isCrossJoin() || type.isSemiJoin()) {
            /* inner join, cross join, semi join
             *      join
             *     /    \     ->  Empty
             *   Empty   B
             */
            return transToEmpty(input, context);
        } else if (type.isRightOuterJoin() || type.isLeftOuterJoin()) {
            /* left outer join        Project (remain B columns(null))
             *     /     \        ->     |
             *    A      Empty           A
             *
             * right outer join
             *     /     \        ->   Empty
             *    A      Empty
             */
            joinIndex = type.isRightOuterJoin() ? 1 : 0;
            if (joinIndex == emptyIndex) {
                return transToEmpty(input, context);
            } else {
                return remainNullOutput(input, context);
            }
        } else if (type.isFullOuterJoin()) {
            return remainNullOutput(input, context);
        } else if (type.isAntiJoin()) {
            /*
             * A left  anti join Empty -> A
             * A right anti join Empty -> Empty
             *
             * Empty left  anti join A  -> Empty
             * Empty right anti join A  -> A
             */
            joinIndex = type.isRightAntiJoin() ? 1 : 0;
            return Lists.newArrayList(input.inputAt(joinIndex));
        }

        return Lists.newArrayList(input);
    }

    public List<OptExpression> remainNullOutput(OptExpression input, OptimizerContext context) {
        OptExpression empty = input.inputAt(emptyIndex);
        Map<ColumnRefOperator, ScalarOperator> outputs = Maps.newHashMap();

        input.getOutputColumns().getStream().map(context.getColumnRefFactory()::getColumnRef)
                .forEach(ref -> outputs.put(ref, ref));
        empty.getOutputColumns().getStream().map(context.getColumnRefFactory()::getColumnRef)
                .forEach(ref -> outputs.put(ref, ConstantOperator.createNull(ref.getType())));

        LogicalProjectOperator project = new LogicalProjectOperator(outputs);
        OptExpression result = OptExpression.create(project, input.inputAt(1 - emptyIndex));

        // save predicate
        if (input.getOp().getPredicate() != null) {
            // don't set predicate to child direct, because some operator doesn't support predicate
            result = OptExpression.create(new LogicalFilterOperator(input.getOp().getPredicate()), result);
        }

        return Lists.newArrayList(result);
    }

    public List<OptExpression> transToEmpty(OptExpression input, OptimizerContext context) {
        List<ColumnRefOperator> refs =
                input.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
        return Lists.newArrayList(OptExpression.create(new LogicalValuesOperator(refs)));
    }
}
