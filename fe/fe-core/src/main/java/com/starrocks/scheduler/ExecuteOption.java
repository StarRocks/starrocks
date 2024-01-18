// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import java.util.Map;

public class ExecuteOption {

    private int priority = Constants.TaskRunPriority.LOWEST.value();
    private boolean mergeRedundant = false;
    private Map<String, String> taskRunProperties;
    // indicates whether the current execution is manual
    private boolean isManual = false;
<<<<<<< HEAD
=======
    private boolean isSync = false;
>>>>>>> 2.5.18

    public ExecuteOption() {
    }

    public ExecuteOption(int priority) {
        this.priority = priority;
    }

    public ExecuteOption(int priority, boolean mergeRedundant, Map<String, String> taskRunProperties) {
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

    public boolean isManual() {
        return isManual;
    }

    public void setManual() {
        this.isManual = true;
    }
<<<<<<< HEAD
=======

    public boolean getIsSync() {
        return isSync;
    }

    public void setSync(boolean isSync) {
        this.isSync = isSync;
    }
>>>>>>> 2.5.18
}
