// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

public class ExecuteOption {

    private int priority = Constants.TaskRunPriority.LOWEST.value();
    private boolean mergeRedundant = false;

    public ExecuteOption() {
    }

    public ExecuteOption(int priority) {
        this.priority = priority;
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
}
