// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.google.common.base.Stopwatch;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Group;

import java.util.Stack;
import java.util.concurrent.TimeUnit;

public class SeriallyTaskScheduler implements TaskScheduler {
    private final Stack<OptimizerTask> tasks;

    private SeriallyTaskScheduler() {
        tasks = new Stack<>();
    }

    public static TaskScheduler create() {
        return new SeriallyTaskScheduler();
    }

    @Override
    public void executeTasks(TaskContext context) {
        long timeout = context.getOptimizerContext().getSessionVariable().getOptimizerExecuteTimeout();
        Stopwatch watch = context.getOptimizerContext().getTraceInfo().getStopwatch();
        while (!tasks.empty()) {
            if (watch.elapsed(TimeUnit.MILLISECONDS) > timeout) {
                // Should have at least one valid plan
                // group will be null when in rewrite phase
                Group group = context.getOptimizerContext().getMemo().getRootGroup();
                if (group == null || !group.hasBestExpression(context.getRequiredProperty())) {
                    throw new StarRocksPlannerException("StarRocks planner use long time " + timeout +
                            " ms in " + (group == null ? "logical" : "memo") + " phase, This probably because " +
                            "1. FE Full GC, " +
                            "2. Hive external table fetch metadata took a long time, " +
                            "3. The SQL is very complex. " +
                            "You could " +
                            "1. adjust FE JVM config, " +
                            "2. try query again, " +
                            "3. enlarge new_planner_optimize_timeout session variable",
                            ErrorType.INTERNAL_ERROR);
                }
                break;
            }
            OptimizerTask task = tasks.pop();
            context.getOptimizerContext().setTaskContext(context);
            task.execute();
        }
    }

    @Override
    public void pushTask(OptimizerTask task) {
        tasks.push(task);
    }
}
