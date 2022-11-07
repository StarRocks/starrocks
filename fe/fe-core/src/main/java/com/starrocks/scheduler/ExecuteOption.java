// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import java.util.HashMap;
import java.util.Map;

public class ExecuteOption {

    private int priority = Constants.TaskRunPriority.LOWEST.value();
    private boolean mergeRedundant = false;
    private HashMap<String, String> taskRunProperties;

    public ExecuteOption() {
    }

    public ExecuteOption(int priority) {
        this.priority = priority;
    }

    public ExecuteOption(int priority, boolean mergeRedundant, HashMap<String, String> taskRunProperties) {
        this.priority = priority;
        this.mergeRedundant = mergeRedundant;
        this.taskRunProperties = taskRunProperties;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public boolean isMergeRedundant() {
        return mergeRedundant;
    }

    public void setMergeRedundant(boolean mergeRedundant) {
        this.mergeRedundant = mergeRedundant;
    }

    public Map<String, String> getTaskRunProperties() {
        return this.taskRunProperties;
    }
}
