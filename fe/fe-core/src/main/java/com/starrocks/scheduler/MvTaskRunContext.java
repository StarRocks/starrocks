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

    // all the RefBaseTable's partition name to its intersected materialized view names.
    Map<String, Set<String>> refBaseTableMVIntersectedPartitions;
    // all the materialized view's partition name to its intersected RefBaseTable's partition names.
    Map<String, Set<String>> mvRefBaseTableIntersectedPartitions;
    // all the RefBaseTable's partition name to its partition key range.
    Map<String, PartitionRange> refBaseTableRangePartitionMap;
    // all the RefBaseTable's partition name to its list partition keys.
    Map<String, List<List<String>>> refBaseTableListPartitionMap;

    // The Table which materialized view' partition column comes from is called `RefBaseTable`:
    // - Materialized View's to-refresh partitions is synced from its `refBaseTable`.
    Table refBaseTable;
    // The `RefBaseTable`'s partition column which materialized view's partition column derives from
    // is called `refBaseTablePartitionColumn`.
    Column refBaseTablePartitionColumn;

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

    public Map<String, Set<String>> getRefBaseTableMVIntersectedPartitions() {
        return refBaseTableMVIntersectedPartitions;
    }

    public void setRefBaseTableMVIntersectedPartitions(Map<String, Set<String>> refBaseTableMVIntersectedPartitions) {
        this.refBaseTableMVIntersectedPartitions = refBaseTableMVIntersectedPartitions;
    }

    public Map<String, Set<String>> getMvRefBaseTableIntersectedPartitions() {
        return mvRefBaseTableIntersectedPartitions;
    }

    public void setMvRefBaseTableIntersectedPartitions(Map<String, Set<String>> mvRefBaseTableIntersectedPartitions) {
        this.mvRefBaseTableIntersectedPartitions = mvRefBaseTableIntersectedPartitions;
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

    public Map<String, PartitionRange> getRefBaseTableRangePartitionMap() {
        return refBaseTableRangePartitionMap;
    }

    public void setRefBaseTableRangePartitionMap(Map<String, PartitionRange> refBaseTableRangePartitionMap) {
        this.refBaseTableRangePartitionMap = refBaseTableRangePartitionMap;
    }

    public Map<String, List<List<String>>> getRefBaseTableListPartitionMap() {
        return refBaseTableListPartitionMap;
    }

    public void setRefBaseTableListPartitionMap(Map<String, List<List<String>>> refBaseTableListPartitionMap) {
        this.refBaseTableListPartitionMap = refBaseTableListPartitionMap;
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

    public Table getRefBaseTable() {
        return refBaseTable;
    }

    public void setRefBaseTable(Table refBaseTable) {
        Preconditions.checkNotNull(refBaseTable);
        this.refBaseTable = refBaseTable;
    }

    public Column getRefBaseTablePartitionColumn() {
        return refBaseTablePartitionColumn;
    }

    public void setRefBaseTablePartitionColumn(Column refBaseTablePartitionColumn) {
        Preconditions.checkNotNull(refBaseTablePartitionColumn);
        this.refBaseTablePartitionColumn = refBaseTablePartitionColumn;
    }
}
