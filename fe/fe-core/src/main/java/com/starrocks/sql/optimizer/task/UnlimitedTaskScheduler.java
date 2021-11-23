// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.Group;

import java.util.Stack;

public class UnlimitedTaskScheduler implements TaskScheduler {
    private final Stack<OptimizerTask> tasks;

    private UnlimitedTaskScheduler() {
        tasks = new Stack<>();
    }

    public static TaskScheduler create() {
        return new UnlimitedTaskScheduler();
    }

    @Override
    public void executeTasks(TaskContext context, Group group) {
        while (!tasks.empty()) {
            tasks.pop().execute();
        }
    }

    @Override
    public void pushTask(OptimizerTask task) {
        tasks.push(task);
    }
}
