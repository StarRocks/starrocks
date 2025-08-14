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
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.task.TaskContext;

public class RemoveUselessScanOutputPropertyRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        RemoveUselessOutputPropertyVisitor visitor = new RemoveUselessOutputPropertyVisitor();
        root.getOp().accept(visitor, root, false);
        return root;
    }

    public static class RemoveUselessOutputPropertyVisitor extends OptExpressionVisitor<Void, Boolean> {
        @Override
        public Void visit(OptExpression optExpression, Boolean context) {
            // set operation except/intersect/union
            // join operation
            if (optExpression.getInputs().size() > 1) {
                for (OptExpression opt : optExpression.getInputs()) {
                    opt.getOp().accept(this, opt, false);
                }
            } else {
                for (OptExpression opt : optExpression.getInputs()) {
                    opt.getOp().accept(this, opt, context);
                }
            }
            return null;
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, Boolean context) {
            OptExpression opt = optExpression.getInputs().get(0);
            opt.getOp().accept(this, opt, false);
            return null;
        }

        @Override
        public Void visitPhysicalAnalytic(OptExpression optExpression, Boolean context) {
            OptExpression opt = optExpression.getInputs().get(0);
            opt.getOp().accept(this, opt, false);
            return null;
        }

        @Override
        public Void visitPhysicalDistribution(OptExpression optExpression, Boolean context) {
            OptExpression opt = optExpression.getInputs().get(0);
            opt.getOp().accept(this, opt, true);
            return null;
        }

        @Override
        public Void visitPhysicalIcebergScan(OptExpression optExpression, Boolean context) {
            if (context && optExpression.getOutputProperty().getDistributionProperty().isShuffle()) {
                optExpression.setOutputProperty(PhysicalPropertySet.EMPTY);
            }
            return null;
        }
    }
}
