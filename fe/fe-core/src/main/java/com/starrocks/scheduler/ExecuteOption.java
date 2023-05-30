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

import java.util.Map;

public class ExecuteOption {

    private int priority = Constants.TaskRunPriority.LOWEST.value();
    private boolean mergeRedundant = false;
    private Map<String, String> taskRunProperties;
    // indicates whether the current execution is manual
    private boolean isManual = false;
    private boolean isSync = false;

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

    public boolean getIsSync() {
        return isSync;
    }

    public void setSync(boolean isSync) {
        this.isSync = isSync;
    }
}
