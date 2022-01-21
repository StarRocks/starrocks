// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.Group;

// The scheduler for optimizer task
// Store tasks in a stack
public interface TaskScheduler {
    void executeTasks(TaskContext context, Group group);

    void pushTask(OptimizerTask task);
}
