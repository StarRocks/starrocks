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

import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonUtils;

import java.util.Map;

public class ExecuteOption {

    @SerializedName("priority")
    private int priority = Constants.TaskRunPriority.LOWEST.value();

    @SerializedName("taskRunProperties")
    private Map<String, String> taskRunProperties;

    @SerializedName("isMergeRedundant")
    private boolean isMergeRedundant = false;
    // indicates whether the current execution is manual

    @SerializedName("isManual")
    private boolean isManual = false;
    @SerializedName("isSync")
    private boolean isSync = false;

    @SerializedName("isReplay")
    private boolean isReplay = false;

    public ExecuteOption() {
    }

    public ExecuteOption(int priority) {
        this.priority = priority;
    }

    public ExecuteOption(int priority, boolean isMergeRedundant, Map<String, String> taskRunProperties) {
        this.priority = priority;
        this.isMergeRedundant = isMergeRedundant;
        this.taskRunProperties = taskRunProperties;
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
        return !isSync && isMergeRedundant;
    }

    public void setMergeRedundant(boolean mergeRedundant) {
        this.isMergeRedundant = mergeRedundant;
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

    public boolean isReplay() {
        return isReplay;
    }

    public void setReplay(boolean replay) {
        isReplay = replay;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
