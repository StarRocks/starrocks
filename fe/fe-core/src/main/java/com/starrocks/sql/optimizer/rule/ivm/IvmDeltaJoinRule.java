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
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;

import java.util.List;
import java.util.Map;

/**
 * Resolves a {@link LogicalDeltaOperator} wrapping a {@link LogicalJoinOperator} by applying
 * the incremental join delta formula.
 *
 * <p>Pattern: {@code LogicalDeltaOperator -> LogicalJoinOperator(LEAF, LEAF)}
 *
 * <p>Only supports inner join and cross join. For inner join:
 * <pre>
 *   Delta(A ⋈ B) = (Delta_A ⋈ Version(FROM, B)) ∪ (Version(TO, A) ⋈ Delta_B)
 * </pre>
 *
 * <p>Each branch gets its own cloned join subtree (via {@link OptExpressionDuplicator})
 * with fresh ColumnRefs. The Delta marker is set to {@code isRootDelta=false} on each branch
 * since these are child deltas, not the root delta created by IvmRewriter.
 */
public class IvmDeltaJoinRule extends TransformationRule {
    public IvmDeltaJoinRule() {
        super(RuleType.TF_IVM_DELTA_JOIN,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN,
                                OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator join = (LogicalJoinOperator) input.inputAt(0).getOp();
        return join.getJoinType().isInnerJoin() || join.getJoinType().isCrossJoin();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        OptExpression joinExpr = input.inputAt(0);
        ColumnRefFactory factory = context.getColumnRefFactory();
        ColumnRefOperator actionColumn = delta.getActionColumn();

        List<ColumnRefOperator> finalOutputColumns = input.getOutputColumns().getColumnRefOperators(factory);
        List<ColumnRefOperator> joinOutputColumns = getJoinOutputsWithoutAction(finalOutputColumns, actionColumn);

        List<OptExpression> unionChildren = Lists.newArrayList();
        List<List<ColumnRefOperator>> unionChildOutputs = Lists.newArrayList();

        // Branch 1: Delta_left ⋈ Version(FROM, right)
        BranchResult branch1 = buildBranch(factory, context, joinExpr, joinOutputColumns, actionColumn, true);
        // Branch 2: Version(TO, left) ⋈ Delta_right
        BranchResult branch2 = buildBranch(factory, context, joinExpr, joinOutputColumns, actionColumn, false);
        if (!appendBranch(unionChildren, unionChildOutputs, branch1)
                || !appendBranch(unionChildren, unionChildOutputs, branch2)) {
            return List.of();
        }

        LogicalUnionOperator unionOp = new LogicalUnionOperator(finalOutputColumns, unionChildOutputs, true);
        return List.of(OptExpression.create(unionOp, unionChildren));
    }

    /**
     * Build one branch of the join delta formula.
     *
     * @param isLeftDelta if true: Delta(left) ⋈ Version(FROM, right);
     *                    if false: Version(TO, left) ⋈ Delta(right)
     */
    private BranchResult buildBranch(ColumnRefFactory factory,
                                     OptimizerContext context,
                                     OptExpression joinExpr,
                                     List<ColumnRefOperator> joinOutputColumns,
                                     ColumnRefOperator actionColumn,
                                     boolean isLeftDelta) {
        DuplicatedJoin dup = duplicateJoin(factory, context, joinExpr);
        LogicalJoinOperator dupJoinOp = (LogicalJoinOperator) dup.joinExpr().getOp();
        ColumnRefOperator branchAction = duplicateActionColumn(factory, actionColumn);

        OptExpression left;
        OptExpression right;
        if (isLeftDelta) {
            left = OptExpression.create(new LogicalDeltaOperator(false, branchAction),
                    dup.joinExpr().inputAt(0));
            right = OptExpression.create(LogicalVersionOperator.fromVersion(),
                    dup.joinExpr().inputAt(1));
        } else {
            left = OptExpression.create(LogicalVersionOperator.toVersion(),
                    dup.joinExpr().inputAt(0));
            right = OptExpression.create(new LogicalDeltaOperator(false, branchAction),
                    dup.joinExpr().inputAt(1));
        }

        List<ColumnRefOperator> outputs = deriveBranchOutputs(joinOutputColumns, branchAction, dup.columnMapping());
        if (outputs == null) {
            return null;
        }

        LogicalJoinOperator newJoin = LogicalJoinOperator.builder()
                .withOperator(dupJoinOp)
                .build();
        return new BranchResult(OptExpression.create(newJoin, left, right), outputs);
    }

    private DuplicatedJoin duplicateJoin(ColumnRefFactory factory, OptimizerContext context,
                                         OptExpression joinExpr) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(factory, context);
        return new DuplicatedJoin(duplicator.duplicate(joinExpr), duplicator.getColumnMapping());
    }

    private ColumnRefOperator duplicateActionColumn(ColumnRefFactory factory, ColumnRefOperator actionColumn) {
        if (actionColumn == null) {
            return null;
        }
        return factory.create(actionColumn.getName(), actionColumn.getType(), actionColumn.isNullable());
    }

    private List<ColumnRefOperator> getJoinOutputsWithoutAction(List<ColumnRefOperator> finalOutputColumns,
                                                                 ColumnRefOperator actionColumn) {
        List<ColumnRefOperator> result = Lists.newArrayList();
        for (ColumnRefOperator col : finalOutputColumns) {
            if (actionColumn == null || col.getId() != actionColumn.getId()) {
                result.add(col);
            }
        }
        return result;
    }

    private List<ColumnRefOperator> deriveBranchOutputs(List<ColumnRefOperator> joinOutputColumns,
                                                         ColumnRefOperator actionColumn,
                                                         Map<ColumnRefOperator, ColumnRefOperator> oldToNew) {
        List<ColumnRefOperator> outputs =
                Lists.newArrayListWithCapacity(joinOutputColumns.size() + (actionColumn == null ? 0 : 1));
        for (ColumnRefOperator col : joinOutputColumns) {
            ColumnRefOperator mapped = oldToNew.get(col);
            if (mapped == null) {
                return null;
            }
            outputs.add(mapped);
        }
        if (actionColumn != null) {
            outputs.add(actionColumn);
        }
        return outputs;
    }

    private boolean appendBranch(List<OptExpression> unionChildren,
                                 List<List<ColumnRefOperator>> unionChildOutputs,
                                 BranchResult branch) {
        if (branch == null || branch.outputs() == null) {
            return false;
        }
        unionChildren.add(branch.branchExpr());
        unionChildOutputs.add(branch.outputs());
        return true;
    }

    private record DuplicatedJoin(OptExpression joinExpr,
                                  Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
    }

    private record BranchResult(OptExpression branchExpr, List<ColumnRefOperator> outputs) {
    }
}
