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

package com.starrocks.sql.optimizer;

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.implementation.OlapScanImplementationRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;
import com.starrocks.sql.optimizer.validate.OptExpressionValidator;

public class ShortCircuitOptimizer extends Optimizer {

    ShortCircuitOptimizer(OptimizerContext context) {
        super(context);
    }

    @Override
    public OptExpression optimize(OptExpression tree, PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns) {
        TaskScheduler scheduler = new TaskScheduler();
        context.setTaskScheduler(scheduler);

        // Phase 1: none
        OptimizerTraceUtil.logOptExpression("origin logicOperatorTree:\n%s", tree);
        // Phase 2: rewrite based on memo and group
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);

        try (Timer ignored = Tracers.watchScope("ShortCircuitOptimize")) {
            tree = OptExpression.create(new LogicalTreeAnchorOperator(), tree);
            // for short circuit
            deriveLogicalProperty(tree);
            scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.SHORT_CIRCUIT_SET_RULES);
            scheduler.rewriteOnce(tree, rootTaskContext, new MergeProjectWithChildRule());
            tree = tree.getInputs().get(0);

            if (OperatorType.LOGICAL_LIMIT.equals(tree.getOp().getOpType())) {
                tree = tree.getInputs().get(0);
            }

            OptExpressionValidator validator = new OptExpressionValidator();
            validator.validate(tree);

            // skip memo
            tree = new OlapScanImplementationRule().transform(tree, null).get(0);
            return tree;
        }
    }
}
