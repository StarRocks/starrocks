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
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

public class InlineCteProjectPruneRule implements TreeRewriteRule {
    private static final InlineCteProjectPruneRuleVisitor HANDLER = new InlineCteProjectPruneRuleVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(HANDLER, root, taskContext);
        return root;
    }

    private static class InlineCteProjectPruneRuleVisitor extends OptExpressionVisitor<Void, TaskContext> {
        @Override
        public Void visit(OptExpression opt, TaskContext context) {
            for (OptExpression input : opt.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalNoCTE(OptExpression opt, TaskContext context) {
            PhysicalNoCTEOperator op = (PhysicalNoCTEOperator) opt.getOp();
            if (op.getProjection() == null) {
                visit(opt, context);
                return null;
            }
            Projection parentProjection = op.getProjection();

            PhysicalOperator child = (PhysicalOperator) opt.getInputs().get(0).getOp();
            if (child.getProjection() == null) {
                visit(opt, context);
                return null;
            }

            Projection childProjection = child.getProjection();
            if (!parentProjection.getUsedColumns().containsAny(childProjection.getOutputColumns())) {
                child.setProjection(null);
            }

            visit(opt, context);
            return null;
        }
    }
}
