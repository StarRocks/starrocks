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
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Map;


public class ScalarOperatorsReuseRule implements TreeRewriteRule {
    private static final ReuseVisitor HANDLER = new ReuseVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(HANDLER, root, taskContext);
        return root;
    }

    private static class ReuseVisitor extends OptExpressionVisitor<Void, TaskContext> {
        @Override
        public Void visit(OptExpression opt, TaskContext context) {
            if (opt.getOp().getProjection() != null) {
                opt.getOp().setProjection(rewriteProject(opt, context));
            }

            for (OptExpression input : opt.getInputs()) {
                input.getOp().accept(this, input, context);
            }
            return null;
        }

        Projection rewriteProject(OptExpression input, TaskContext context) {
            Projection projection = input.getOp().getProjection();

            projection = ScalarOperatorsReuse.getNewProjection(projection,
                    context.getOptimizerContext().getColumnRefFactory(), false);

            // rewrite lambda functions
            rewriteLambdaFunction(projection.getCommonSubOperatorMap(),
                    context.getOptimizerContext().getColumnRefFactory());
            rewriteLambdaFunction(projection.getColumnRefMap(), context.getOptimizerContext().getColumnRefFactory());
            return projection;
        }

        void rewriteLambdaFunction(Map<ColumnRefOperator, ScalarOperator> operatorMap, ColumnRefFactory factory) {
            if (operatorMap.isEmpty()) {
                return;
            }
            ScalarOperatorsReuse.LambdaOperatorRewriter rewriter =
                    new ScalarOperatorsReuse.LambdaOperatorRewriter(factory);

            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : operatorMap.entrySet()) {
                kv.getValue().accept(rewriter, null);
            }

            return;
        }
    }
}
