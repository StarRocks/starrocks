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


package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.Group;

/**
 * Optimize a group within a given context.
 * <p>
 * OptimizeGroupTask will generate {@link OptimizeExpressionTask} to
 * optimize all logically equivalent operator trees.
 * <p>
 * OptimizeGroupTask will then generate {@link EnforceAndCostTask} to
 * enforce and cost all physical operator trees given the current OptimizerContext.
 */
public class OptimizeGroupTask extends OptimizerTask {
    private final Group group;

    public OptimizeGroupTask(TaskContext context, Group group) {
        super(context);
        this.group = group;
    }

    @Override
    public String toString() {
        return "OptimizeGroupTask for group " + group;
    }

    @Override
    public void execute() {
        // 1 Group Cost LB > Context Cost UB
        // 2 Group has optimized given the context
        if (group.getCostLowerBound() >= context.getUpperBoundCost() ||
                group.hasBestExpression(context.getRequiredProperty())) {
            return;
        }

        for (int i = group.getLogicalExpressions().size() - 1; i >= 0; i--) {
            pushTask(new OptimizeExpressionTask(context, group.getLogicalExpressions().get(i)));
        }

        for (int i = group.getPhysicalExpressions().size() - 1; i >= 0; i--) {
            pushTask((new EnforceAndCostTask(context, group.getPhysicalExpressions().get(i))));
        }
    }
}
