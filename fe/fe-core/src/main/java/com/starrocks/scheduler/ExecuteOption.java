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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class ExecuteOption {
    @SerializedName("priority")
    private int priority = Constants.TaskRunPriority.LOWEST.value();
    @SerializedName("mergeRedundant")
    private boolean mergeRedundant = false;

    @SerializedName("taskRunProperties")
    private Map<String, String> taskRunProperties;

    // indicates whether the current execution is manual
    @SerializedName("isManual")
    private boolean isManual = false;

    @SerializedName("isSync")
    private boolean isSync = false;

    @SerializedName("isReplay")
    private boolean isReplay = false;

    public ExecuteOption(boolean isMergeRedundant) {
        this.mergeRedundant = isMergeRedundant;
    }

    public ExecuteOption(int priority, boolean mergeRedundant, Map<String, String> taskRunProperties) {
        this.priority = priority;
        this.mergeRedundant = mergeRedundant;
        this.taskRunProperties = taskRunProperties;
    }

    public static ExecuteOption makeMergeRedundantOption() {
        return new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(), true, Maps.newHashMap());
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public boolean isMergeRedundant() {
        // If old task run is a sync-mode task, skip to merge it to avoid sync-mode task
        // hanging after removing it.
        return !isSync && mergeRedundant;
    }

    public Map<String, String> getTaskRunProperties() {
        return this.taskRunProperties;
    }

    public boolean isManual() {
        return isManual;
    }

    public void setManual(boolean isManual) {
        this.isManual = isManual;
    }

    public boolean getIsSync() {
        return isSync;
    }

    public void setSync(boolean isSync) {
        this.isSync = isSync;
    }

    public void setReplay(boolean isReplay) {
        this.isReplay = isReplay;
    }

    @Override
    public String toString() {
        return String.format("ExecuteOption{priority=%s, mergeRedundant=%s, isManual=%s, " +
                        "isSync=%s, taskRunProperties={%s}}",
                priority, mergeRedundant, isManual, isSync, taskRunProperties
        );
    }
}
