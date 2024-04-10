// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.scheduler;

import com.starrocks.qe.ConnectContext;

import java.util.HashMap;
import java.util.Map;

public class TaskRunBuilder {
    private final Task task;
    private Map<String, String> properties;
    private ConnectContext connectContext;
    private ExecuteOption executeOption = new ExecuteOption();

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
        taskRun.setExecuteOption(executeOption);
        taskRun.setType(getTaskType());
        if (task.getSource().equals(Constants.TaskSource.MV)) {
            taskRun.setProcessor(new PartitionBasedMvRefreshProcessor());
        } else {
            taskRun.setProcessor(new SqlTaskRunProcessor());
        }
        return taskRun;
    }

    private Constants.TaskType getTaskType() {
        if (executeOption.isManual()) {
            return Constants.TaskType.MANUAL;
        } else {
            return task.getType();
        }
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

    public Long getTaskId() {
        return task.getId();
    }

    public ExecuteOption getExecuteOption() {
        return executeOption;
    }

    public TaskRunBuilder setExecuteOption(ExecuteOption executeOption) {
        this.executeOption = executeOption;
        return this;
    }
}
