// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

// The scheduler for optimizer task
// Store tasks in a stack
public interface TaskScheduler {
    void executeTasks(TaskContext context);

    void pushTask(OptimizerTask task);
}
