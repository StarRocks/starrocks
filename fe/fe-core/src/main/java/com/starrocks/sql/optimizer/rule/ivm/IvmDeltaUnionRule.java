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

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;

/**
 * Pushes {@link LogicalDeltaOperator} below {@link LogicalUnionOperator} by wrapping each
 * union child with its own Delta marker.
 *
 * <p>Pattern: {@code LogicalDeltaOperator -> LogicalUnionOperator(MULTI_LEAF)}
 *
 * <p>Only supports UNION ALL. Each child gets its own {@code branchActionColumn} (new ColumnRef)
 * and {@code isRootDelta=false}. The child outputs are extended with the branch action column
 * and mapped back to the final output columns (including the original action column) by the
 * rewritten union operator.
 */
public class IvmDeltaUnionRule extends TransformationRule {
    public IvmDeltaUnionRule() {
        super(RuleType.TF_IVM_DELTA_UNION,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_UNION, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalUnionOperator union = (LogicalUnionOperator) input.inputAt(0).getOp();
        return union.isUnionAll();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        OptExpression unionExpr = input.inputAt(0);
        LogicalUnionOperator union = (LogicalUnionOperator) unionExpr.getOp();

        ColumnRefFactory factory = context.getColumnRefFactory();
        ColumnRefOperator finalActionColumn = delta.getActionColumn();
        List<ColumnRefOperator> finalOutputs = input.getOutputColumns().getColumnRefOperators(factory);

        List<OptExpression> unionChildren = Lists.newArrayListWithCapacity(unionExpr.arity());
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayListWithCapacity(unionExpr.arity());
        for (int i = 0; i < unionExpr.arity(); i++) {
            ColumnRefOperator childActionColumn = finalActionColumn == null ? null :
                    factory.create(finalActionColumn.getName(), finalActionColumn.getType(),
                            finalActionColumn.isNullable());
            LogicalDeltaOperator childDelta = new LogicalDeltaOperator(false, childActionColumn);
            unionChildren.add(OptExpression.create(childDelta, unionExpr.inputAt(i)));
            List<ColumnRefOperator> childOutputs = Lists.newArrayList(union.getChildOutputColumns().get(i));
            if (childActionColumn != null) {
                childOutputs.add(childActionColumn);
            }
            unionChildOutputs.add(childOutputs);
        }

        LogicalUnionOperator rewrittenUnion = LogicalUnionOperator.builder()
                .withOperator(union)
                .setOutputColumnRefOp(finalOutputs)
                .setChildOutputColumns(unionChildOutputs)
                .build();
        return List.of(OptExpression.create(rewrittenUnion, unionChildren));
    }
}
