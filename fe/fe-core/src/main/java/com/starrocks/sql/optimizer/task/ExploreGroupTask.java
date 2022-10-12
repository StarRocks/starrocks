// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;

/*
 * Explore group logical transform
 */
public class ExploreGroupTask extends OptimizerTask {
    private final Group group;

    ExploreGroupTask(TaskContext context, Group group) {
        super(context);
        this.group = group;
    }

    @Override
    public void execute() {
        if (group.isExplored()) {
            return;
        }

        for (GroupExpression logical : group.getLogicalExpressions()) {
            pushTask(new OptimizeExpressionTask(context, logical, true));
        }

        group.setExplored();
    }

    @Override
    public String toString() {
        return "ExploreGroupTask for group " + group.getId();
    }
}
