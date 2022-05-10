// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.scheduler;

import com.starrocks.qe.ConnectContext;

import java.time.LocalDateTime;

public class TaskRunBuilder {
    private Task task;

    public static TaskRunBuilder newBuilder(Task task) {
        return new TaskRunBuilder(task);
    }

    public TaskRunBuilder(Task task) {
        this.task = task;
    }

    // TaskRun is the smallest unit of execution.
    public TaskRun build() {
        TaskRun taskRun = new TaskRun();
        taskRun.setTaskId(task.getId());
        taskRun.setDbName(task.getDbName());
        taskRun.setCreateTime(LocalDateTime.now());
        taskRun.setDefinition(task.getDefinition());
        taskRun.setProperties(task.getProperties());
        taskRun.setCtx(ConnectContext.get());
        taskRun.setTask(task);
        taskRun.setProcessor(task.getProcessor());
        return taskRun;
    }

    public Long getTaskId() {
        return task.getId();
    }
}
