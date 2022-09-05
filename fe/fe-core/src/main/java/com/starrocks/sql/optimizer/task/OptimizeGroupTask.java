// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
