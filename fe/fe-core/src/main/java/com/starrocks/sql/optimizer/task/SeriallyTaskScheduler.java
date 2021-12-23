// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Group;

import java.util.Stack;

public class SeriallyTaskScheduler implements TaskScheduler {
    private final Stack<OptimizerTask> tasks;

    private SeriallyTaskScheduler() {
        tasks = new Stack<>();
    }

    public static TaskScheduler create() {
        return new SeriallyTaskScheduler();
    }

    @Override
    public void executeTasks(TaskContext context, Group group) {
        long startTime = System.currentTimeMillis();
        long timeout = context.getOptimizerContext().getSessionVariable().getOptimizerExecuteTimeout();
        long endTime = startTime + timeout;
        while (!tasks.empty()) {
            if (System.currentTimeMillis() >= endTime) {
                // Should have at least one valid plan
                if (!group.hasBestExpression(context.getRequiredProperty())) {
                    throw new StarRocksPlannerException("StarRocks planner use long time " + timeout +
                            " ms, This probably because 1. FE Full GC, 2. Hive external table fetch metadata took a long time, " +
                            "3. The SQL is very complex. " +
                            "You could 1. adjust FE JVM config, 2. try query again, " +
                            "3. enlarge new_planner_optimize_timeout session variable",
                            ErrorType.INTERNAL_ERROR);
                }
                break;
            }
            tasks.pop().execute();
        }
    }

    @Override
    public void pushTask(OptimizerTask task) {
        tasks.push(task);
    }
}
