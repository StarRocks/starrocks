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

import com.google.common.collect.Range;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.TableProperty;
import com.starrocks.sql.plan.ExecPlan;

import java.util.Map;
import java.util.Set;

public class MvTaskRunContext extends TaskRunContext {

    Map<String, Set<String>> baseToMvNameRef;
    Map<String, Set<String>> mvToBaseNameRef;
    Map<String, Range<PartitionKey>> basePartitionMap;

    String nextPartitionStart = null;
    String nextPartitionEnd = null;
    ExecPlan execPlan = null;

    int partitionTTLNumber = TableProperty.INVALID;

    public MvTaskRunContext(TaskRunContext context) {
        this.ctx = context.ctx;
        this.definition = context.definition;
        this.remoteIp = context.remoteIp;
        this.properties = context.properties;
        this.type = context.type;
        this.status = context.status;
        this.taskRun = context.taskRun;
    }

    public Map<String, Set<String>> getBaseToMvNameRef() {
        return baseToMvNameRef;
    }

    public void setBaseToMvNameRef(Map<String, Set<String>> baseToMvNameRef) {
        this.baseToMvNameRef = baseToMvNameRef;
    }

    public Map<String, Set<String>> getMvToBaseNameRef() {
        return mvToBaseNameRef;
    }

    public void setMvToBaseNameRef(Map<String, Set<String>> mvToBaseNameRef) {
        this.mvToBaseNameRef = mvToBaseNameRef;
    }

    public boolean hasNextBatchPartition() {
        return nextPartitionStart != null && nextPartitionEnd != null;
    }

    public String getNextPartitionStart() {
        return nextPartitionStart;
    }

    public void setNextPartitionStart(String nextPartitionStart) {
        this.nextPartitionStart = nextPartitionStart;
    }

    public String getNextPartitionEnd() {
        return nextPartitionEnd;
    }

    public void setNextPartitionEnd(String nextPartitionEnd) {
        this.nextPartitionEnd = nextPartitionEnd;
    }

    public void setBasePartitionMap(Map<String, Range<PartitionKey>> basePartitionMap) {
        this.basePartitionMap = basePartitionMap;
    }

    public Map<String, Range<PartitionKey>> getBasePartitionMap() {
        return this.basePartitionMap;
    }

    public ExecPlan getExecPlan() {
        return this.execPlan;
    }

    public void setExecPlan(ExecPlan execPlan) {
        this.execPlan = execPlan;
    }

    public Constants.TaskType getTaskType() {
        return this.type;
    }

    public int getPartitionTTLNumber() {
        return partitionTTLNumber;
    }

    public void setPartitionTTLNumber(int partitionTTLNumber) {
        this.partitionTTLNumber = partitionTTLNumber;
    }
}
