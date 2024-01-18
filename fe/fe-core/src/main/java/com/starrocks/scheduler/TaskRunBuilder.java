// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.starrocks.qe.ConnectContext;

import java.util.HashMap;
import java.util.Map;

public class TaskRunBuilder {
    private final Task task;
    private Map<String, String> properties;
    private Constants.TaskType type;
<<<<<<< HEAD
=======
    private ConnectContext connectContext;
>>>>>>> 2.5.18

    public static TaskRunBuilder newBuilder(Task task) {
        return new TaskRunBuilder(task);
    }

    private TaskRunBuilder(Task task) {
        this.task = task;
    }

    public TaskRunBuilder setConnectContext(ConnectContext connectContext) {
        this.connectContext = connectContext;
        return this;
    }

    // TaskRun is the smallest unit of execution.
    public TaskRun build() {
        TaskRun taskRun = new TaskRun();
        taskRun.setConnectContext(connectContext);
        taskRun.setTaskId(task.getId());
        taskRun.setProperties(mergeProperties());
        taskRun.setTask(task);
        taskRun.setType(getTaskType());
        if (task.getSource().equals(Constants.TaskSource.MV)) {
            taskRun.setProcessor(new PartitionBasedMvRefreshProcessor());
        } else {
            taskRun.setProcessor(new SqlTaskRunProcessor());
        }
        return taskRun;
    }

    private Constants.TaskType getTaskType() {
        return type != null ? type : task.getType();
    }

    private Map<String, String> mergeProperties() {
        if (task.getProperties() == null) {
            return properties;
        }
        if (properties == null) {
            return task.getProperties();
        }
        Map<String, String> result = new HashMap<>();
        result.putAll(task.getProperties());
        result.putAll(properties);
        return result;
    }

    public TaskRunBuilder properties(Map<String, String> properties) {
        this.properties = properties;
        return this;
    }

    public TaskRunBuilder type(ExecuteOption option) {
        if (option.isManual()) {
            this.type = Constants.TaskType.MANUAL;
        }
        return this;
    }

    public Long getTaskId() {
        return task.getId();
    }
}
