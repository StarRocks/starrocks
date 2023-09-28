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
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// ProjectOperator or Projection of Operator may have several ColumnRefs remapped to the same ColumnRef, for an example:
// 1.ColumnRef(1)->ColumnRef(1);
// 2.ColumnRef(2)->ColumnRef(1);
// This would lead to that column shared by multiple SlotRefs in a Chunk during the plan executed in BE,
// when some conjuncts apply to such chunks, the shared column may be written twice unexpectedly; at present,
// BE does not support COW; so we substitute duplicate ColumnRef with CloneOperator to avoid this.
// After this Rule applied, the ColumnRef remapping will convert to:
// 1.ColumnRef(1)->ColumnRef(1);
// 2.ColumnRef(2)->CloneOperator(ColumnRef(1)).
public class CloneDuplicateColRefRule implements TreeRewriteRule {
    private static final Visitor VISITOR = new Visitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        VISITOR.visit(root, null);
        return root;
    }

    private static class Visitor extends OptExpressionVisitor<Void, Void> {
        void substColumnRefOperatorWithCloneOperator(Map<ColumnRefOperator, ScalarOperator> colRefMap) {
            if (colRefMap == null || colRefMap.isEmpty()) {
                return;
            }

            List<Map.Entry<ColumnRefOperator, ScalarOperator>> entries =
                    colRefMap.entrySet().stream()
                            .sorted(Comparator.comparing(entry -> entry.getKey().getId()))
                            .collect(Collectors.toList());

            ColumnRefSet duplicateColRefs = new ColumnRefSet();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : entries) {
                if (!entry.getValue().isColumnRef()) {
                    continue;
                }
                ColumnRefOperator colRef = entry.getValue().cast();
                if (duplicateColRefs.contains(colRef)) {
                    entry.setValue(new CloneOperator(colRef));
                } else {
                    duplicateColRefs.union(colRef);
                }
            }
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            if (optExpression.getOp() instanceof PhysicalProjectOperator) {
                PhysicalProjectOperator projectOp = optExpression.getOp().cast();
                substColumnRefOperatorWithCloneOperator(projectOp.getColumnRefMap());
                substColumnRefOperatorWithCloneOperator(projectOp.getCommonSubOperatorMap());
            } else if (optExpression.getOp().getProjection() != null) {
                Projection projection = optExpression.getOp().getProjection();
                substColumnRefOperatorWithCloneOperator(projection.getColumnRefMap());
                substColumnRefOperatorWithCloneOperator(projection.getCommonSubOperatorMap());
            }
            optExpression.getInputs().forEach(input -> this.visit(input, context));
            return null;
        }
    }
}
