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

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

public class DisableRuntimeAdaptiveDopRule implements TreeRewriteRule {
    public static final DisableRuntimeAdaptiveDopRule INSTANCE = new DisableRuntimeAdaptiveDopRule();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable sessionVariable = taskContext.getOptimizerContext().getSessionVariable();

        if (!sessionVariable.isEnableRuntimeAdaptiveDop()) {
            return root;
        }

        if (isSimpleAggregateQuery(root)) {
            sessionVariable.setEnableRuntimeAdaptiveDop(false);
        }

        return root;
    }

    private boolean isSimpleAggregateQuery(OptExpression root) {
        return SimpleAggregateQueryChecker.CHECKER.check(root);
    }

    // plan only has agg/scan/project and agg doesn't have group by
    private static class SimpleAggregateQueryChecker extends OptExpressionVisitor<Boolean, Void> {
        private static final SimpleAggregateQueryChecker CHECKER = new SimpleAggregateQueryChecker();

        public boolean check(OptExpression root) {
            return root.getOp().accept(this, root, null);
        }

        @Override
        public Boolean visit(OptExpression optExpression, Void context) {
            // default behavior
            return false;
        }

        @Override
        public Boolean visitLogicalTreeAnchor(OptExpression optExpression, Void context) {
            return visitChildren(optExpression, context);
        }

        private Boolean visitChildren(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                if (!input.getOp().accept(this, input, context)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public Boolean visitLogicalAggregate(OptExpression optExpression, Void context) {
            LogicalAggregationOperator aggOp = (LogicalAggregationOperator) optExpression.getOp();

            if (aggOp.getGroupingKeys() != null && !aggOp.getGroupingKeys().isEmpty()) {
                return false;
            }

            return visitChildren(optExpression, context);
        }

        @Override
        public Boolean visitLogicalProject(OptExpression optExpression, Void context) {
            return visitChildren(optExpression, context);
        }

        @Override
        public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
            return true;
        }
    }
}
