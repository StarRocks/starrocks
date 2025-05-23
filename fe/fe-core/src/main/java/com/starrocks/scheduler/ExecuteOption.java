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
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Map;
import java.util.Objects;

public class ExecuteOption {

    @SerializedName("priority")
    private int priority = Constants.TaskRunPriority.LOWEST.value();

    @SerializedName("taskRunProperties")
    private Map<String, String> taskRunProperties;

    @SerializedName("isMergeRedundant")
    private final boolean isMergeRedundant;

    // indicates whether the current execution is manual
    @SerializedName("isManual")
    private boolean isManual = false;

    @SerializedName("isSync")
    private boolean isSync = false;

    @SerializedName("isReplay")
    private boolean isReplay = false;

    public ExecuteOption(Task task) {
        this(Constants.TaskRunPriority.LOWEST.value(), task.getSource().isMergeable(), task.getProperties());
    }

    public ExecuteOption(int priority, boolean isMergeRedundant, Map<String, String> taskRunProperties) {
        this.priority = priority;
        this.isMergeRedundant = isMergeRedundant;
        // clone the taskRunProperties to avoid modifying the original map because `mergeProperties` may change it.
        if (taskRunProperties != null) {
            this.taskRunProperties = Maps.newHashMap(taskRunProperties);
        }
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
        if (Config.enable_mv_refresh_sync_refresh_mergeable) {
            return isMergeRedundant;
        } else {
            return !isSync && isMergeRedundant;
        }
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

    /**
     * Check if the current ExecuteOption can be merged with the given option:
     * - Only all taskRunProperties are empty or the same with specific keys, we can merge two pending task runs.
     */
    public boolean isMergeableWith(ExecuteOption other) {
        if (!isMergeRedundant || !other.isMergeRedundant) {
            return false;
        }
        if (CollectionUtils.sizeIsEmpty(this.taskRunProperties)
                && CollectionUtils.sizeIsEmpty(other.taskRunProperties)) {
            return true;
        }
        return Objects.equals(this.getTaskRunComparableProperties(), other.getTaskRunComparableProperties());
    }

    /**
     * See @{code TaskRun#MV_COMPARABLE_PROPERTIES} for more details.
     * @return a map of task run properties that are comparable for merging.
     */
    public Map<String, String> getTaskRunComparableProperties() {
        // For non-mv task, we don't need to check the task run properties.
        if (taskRunProperties == null || !taskRunProperties.containsKey(TaskRun.MV_ID)) {
            return taskRunProperties;
        }
        Map<String, String> result = Maps.newHashMap();
        for (Map.Entry<String, String> e : taskRunProperties.entrySet()) {
            if (TaskRun.MV_COMPARABLE_PROPERTIES.contains(e.getKey())) {
                result.put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }
}
