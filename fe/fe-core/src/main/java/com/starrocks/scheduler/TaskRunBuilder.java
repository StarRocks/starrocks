// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

public class TaskRunBuilder {
    private final Task task;

    public static TaskRunBuilder newBuilder(Task task) {
        return new TaskRunBuilder(task);
    }

    private TaskRunBuilder(Task task) {
        this.task = task;
    }

    // TaskRun is the smallest unit of execution.
    public TaskRun build() {
        TaskRun taskRun = new TaskRun();
        taskRun.setTaskId(task.getId());
        taskRun.setProperties(task.getProperties());
        taskRun.setTask(task);
        if (task.getSource().equals(Constants.TaskSource.MV)) {
            taskRun.setProcessor(new PartitionBasedMaterializedViewRefreshProcessor());
        } else {
            taskRun.setProcessor(new SqlTaskRunProcessor());
        }
        return taskRun;
    }

    public Long getTaskId() {
        return task.getId();
    }
}
