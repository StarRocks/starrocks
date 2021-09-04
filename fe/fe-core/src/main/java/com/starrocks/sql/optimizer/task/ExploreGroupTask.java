// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;

/**
 * Generate all logical transformation expressions
 * by applying logical transformation rules to logical operators.
 */
public class ExploreGroupTask extends OptimizerTask {
    private final Group group;

    ExploreGroupTask(TaskContext context, Group group) {
        super(context);
        this.group = group;
    }

    @Override
    public String toString() {
        return "ExploreGroupTask for group " + group;
    }

    @Override
    public void execute() {
        if (!group.hasExplored()) {
            for (GroupExpression expression : group.getLogicalExpressions()) {
                pushTask(new OptimizeExpressionTask(context, expression, true));
            }
            group.setHasExplored();
        }
    }
}
