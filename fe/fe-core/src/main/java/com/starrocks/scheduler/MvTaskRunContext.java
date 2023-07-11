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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.sql.common.PartitionRange;
import com.starrocks.sql.plan.ExecPlan;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MvTaskRunContext extends TaskRunContext {

    Map<String, Set<String>> baseToMvNameRef;
    Map<String, Set<String>> mvToBaseNameRef;
    List<PartitionRange> basePartitionRanges;

    Map<String, List<List<String>>> baseListPartitionMap;

    Table partitionBaseTable;
    Column partitionColumn;

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

    public List<PartitionRange> getBasePartitionRanges() {
        return basePartitionRanges;
    }

    public void setBasePartitionRanges(List<PartitionRange> basePartitionRanges) {
        this.basePartitionRanges = basePartitionRanges;
    }

    public Map<String, List<List<String>>> getBaseListPartitionMap() {
        return baseListPartitionMap;
    }

    public void setBaseListPartitionMap(Map<String, List<List<String>>> baseListPartitionMap) {
        this.baseListPartitionMap = baseListPartitionMap;
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

    public Table getPartitionBaseTable() {
        return partitionBaseTable;
    }

    public void setPartitionBaseTable(Table partitionBaseTable) {
        Preconditions.checkNotNull(partitionBaseTable);
        this.partitionBaseTable = partitionBaseTable;
    }

    public Column getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(Column partitionColumn) {
        Preconditions.checkNotNull(partitionColumn);
        this.partitionColumn = partitionColumn;
    }
}
