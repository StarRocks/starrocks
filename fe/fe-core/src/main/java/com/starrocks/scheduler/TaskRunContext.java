// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.scheduler;

import com.starrocks.qe.ConnectContext;

import java.util.Map;

public class TaskRunContext {
    ConnectContext ctx;
    String definition;
    String remoteIp;
    int priority;
    Map<String, String> properties;
    Constants.TaskType type;
<<<<<<< HEAD
=======
    TaskRunStatus status;
    ExecuteOption executeOption;
    String taskRunId;
    TaskRun taskRun;

    public TaskRunContext() {
    }

    public TaskRunContext(TaskRunContext context) {
        this.ctx = context.ctx;
        this.definition = context.definition;
        this.postRun = context.postRun;
        this.remoteIp = context.remoteIp;
        this.priority = context.priority;
        this.properties = context.properties;
        this.type = context.type;
        this.status = context.status;
        this.executeOption = context.executeOption;
        this.taskRunId = context.taskRunId;
        this.taskRun = context.taskRun;
    }
>>>>>>> 54e73cd039 ([BugFix] fix cancel refresh mv command cannot stop task (#40649))

    public ConnectContext getCtx() {
        return ctx;
    }

    public void setCtx(ConnectContext ctx) {
        this.ctx = ctx;
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    public String getRemoteIp() {
        return remoteIp;
    }

    public void setRemoteIp(String remoteIp) {
        this.remoteIp = remoteIp;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Constants.TaskType getTaskType() {
        return this.type;
    }

    public void setTaskType(Constants.TaskType type) {
        this.type = type;
    }
<<<<<<< HEAD
=======

    public TaskRunStatus getStatus() {
        return status;
    }

    public void setStatus(TaskRunStatus status) {
        this.status = status;
    }

    public ExecuteOption getExecuteOption() {
        return executeOption;
    }

    public void setExecuteOption(ExecuteOption executeOption) {
        this.executeOption = executeOption;
    }

    public String getTaskRunId() {
        return taskRunId;
    }

    public void setTaskRunId(String uuid) {
        this.taskRunId = uuid;
    }

    public TaskRun getTaskRun() {
        return taskRun;
    }

    public void setTaskRun(TaskRun taskRun) {
        this.taskRun = taskRun;
    }
>>>>>>> 54e73cd039 ([BugFix] fix cancel refresh mv command cannot stop task (#40649))
}
