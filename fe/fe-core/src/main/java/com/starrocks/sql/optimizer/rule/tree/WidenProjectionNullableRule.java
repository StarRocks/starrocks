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
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Map;

// Enforce the projection invariant that an output ColumnRef is at least as nullable as the
// input expression it maps to.
//
// MV rewrite can leave a Projection where the target ColumnRef is the original (pre-rewrite)
// query ColumnRef -- carrying its base-table nullability (e.g. NOT NULL for a PK column) --
// while the source has been rewritten to read from an MV column whose nullability encodes
// semantics like outer-join (nullable=true). Normal plans mask this because the runtime
// Project propagates the source's nullable into the output. Global late materialization
// splits the embedded projection out and FETCH writes the target slot directly from storage,
// exposing the stale nullable=false on the target ColumnRef.
//
// Run this rule after MV rewrite and any equivalence-based reasoning (so mutating a shared
// ColumnRef's nullable is safe), and before GlobalLateMaterializationRewriter (so FETCH sees
// the corrected nullable).
public class WidenProjectionNullableRule implements TreeRewriteRule {
    private static final Visitor VISITOR = new Visitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        VISITOR.visit(root, null);
        return root;
    }

    private static class Visitor extends OptExpressionVisitor<Void, Void> {
        private static void widenOutputNullable(Map<ColumnRefOperator, ScalarOperator> map) {
            if (map == null) {
                return;
            }
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : map.entrySet()) {
                ColumnRefOperator target = entry.getKey();
                ScalarOperator source = entry.getValue();
                // Conservative: only widen when source is a direct ColumnRef. For more complex
                // expressions (function calls, casts, etc.) the nullable may be derived from
                // other inputs and widening the target unconditionally is unsafe.
                // TODO: extend to non-ColumnRef sources once expression-level nullable
                //  derivation is reliable enough to drive target widening.
                if (!(source instanceof ColumnRefOperator)) {
                    continue;
                }
                if (source.isNullable() && !target.isNullable()) {
                    target.setNullable(true);
                }
            }
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            if (optExpression.getOp() instanceof PhysicalProjectOperator) {
                PhysicalProjectOperator projectOp = optExpression.getOp().cast();
                widenOutputNullable(projectOp.getColumnRefMap());
            } else if (optExpression.getOp().getProjection() != null) {
                Projection projection = optExpression.getOp().getProjection();
                widenOutputNullable(projection.getColumnRefMap());
            }
            optExpression.getInputs().forEach(input -> this.visit(input, context));
            return null;
        }
    }
}
