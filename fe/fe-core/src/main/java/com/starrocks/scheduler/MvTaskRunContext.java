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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.plan.ExecPlan;

import java.util.Map;
import java.util.Set;

public class MvTaskRunContext extends TaskRunContext {

    // all the RefBaseTable's partition name to its intersected materialized view names.
    //baseTable -> basePartition -> mvPartitions
    private Map<Table, Map<String, Set<String>>> refBaseTableMVIntersectedPartitions;
    // all the materialized view's partition name to its intersected RefBaseTable's partition names.
    //mvPartition -> baseTable -> basePartitions
    private Map<String, Map<Table, Set<String>>> mvRefBaseTableIntersectedPartitions;
    // all the RefBaseTable's partition name to its partition key range.
    private Map<Table, Map<String, Range<PartitionKey>>> refBaseTableRangePartitionMap;
    private Map<String, Range<PartitionKey>> mvRangePartitionMap;

    // all the RefBaseTable's partition name to its list partition keys.
    private Map<Table, Map<String, PListCell>> refBaseTableListPartitionMap;
    // the external ref base table's mv partition name to original partition names map because external
    // table supports multi partition columns, one converted partition name(mv partition name) may have
    // multi original partition names.
    private Map<Table, Map<String, Set<String>>> externalRefBaseTableMVPartitionMap;

    private String nextPartitionStart = null;
    private String nextPartitionEnd = null;
    // The next list partition values to be processed
    private String nextPartitionValues = null;
    private ExecPlan execPlan = null;

    private int partitionTTLNumber = TableProperty.INVALID;

    public MvTaskRunContext(TaskRunContext context) {
        super(context);
    }

    public Map<Table, Map<String, Set<String>>> getRefBaseTableMVIntersectedPartitions() {
        return refBaseTableMVIntersectedPartitions;
    }

    public void setRefBaseTableMVIntersectedPartitions(
            Map<Table, Map<String, Set<String>>> refBaseTableMVIntersectedPartitions) {
        this.refBaseTableMVIntersectedPartitions = refBaseTableMVIntersectedPartitions;
    }

    public Map<String, Map<Table, Set<String>>> getMvRefBaseTableIntersectedPartitions() {
        return mvRefBaseTableIntersectedPartitions;
    }

    public void setMvRefBaseTableIntersectedPartitions(
            Map<String, Map<Table, Set<String>>> mvRefBaseTableIntersectedPartitions) {
        this.mvRefBaseTableIntersectedPartitions = mvRefBaseTableIntersectedPartitions;
    }

    public boolean hasNextBatchPartition() {
        return (nextPartitionStart != null && nextPartitionEnd != null) || (nextPartitionValues != null);
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

    public String getNextPartitionValues() {
        return nextPartitionValues;
    }

    public void setNextPartitionValues(String nextPartitionValues) {
        this.nextPartitionValues = nextPartitionValues;
    }

    public Map<Table, Map<String, Range<PartitionKey>>> getRefBaseTableRangePartitionMap() {
        return refBaseTableRangePartitionMap;
    }

    public void setRefBaseTableRangePartitionMap(
            Map<Table, Map<String, Range<PartitionKey>>> refBaseTableRangePartitionMap) {
        this.refBaseTableRangePartitionMap = refBaseTableRangePartitionMap;
    }

    public Map<Table, Map<String, PListCell>> getRefBaseTableListPartitionMap() {
        return refBaseTableListPartitionMap;
    }

    public void setRefBaseTableListPartitionMap(Map<Table, Map<String, PListCell>> refBaseTableListPartitionMap) {
        this.refBaseTableListPartitionMap = refBaseTableListPartitionMap;
    }

    public Map<Table, Map<String, Set<String>>> getExternalRefBaseTableMVPartitionMap() {
        return externalRefBaseTableMVPartitionMap;
    }

    public void setExternalRefBaseTableMVPartitionMap(
            Map<Table, Map<String, Set<String>>> externalRefBaseTableMVPartitionMap) {
        this.externalRefBaseTableMVPartitionMap = externalRefBaseTableMVPartitionMap;
    }

    public Map<String, Range<PartitionKey>> getMvRangePartitionMap() {
        return mvRangePartitionMap;
    }

    public void setMvRangePartitionMap(Map<String, Range<PartitionKey>> mvRangePartitionMap) {
        this.mvRangePartitionMap = mvRangePartitionMap;
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

    /**
     * For external table, the partition name is normalized which should convert it into original partition name.
     * <p>
     * For multi-partition columns, `refTableAndPartitionNames` is not fully exact to describe which partitions
     * of ref base table are refreshed, use `getSelectedPartitionInfosOfExternalTable` later if we can solve the multi
     * partition columns problem.
     * eg:
     * partitionName1 : par_col=0/par_date=2020-01-01 => p20200101
     * partitionName2 : par_col=1/par_date=2020-01-01 => p20200101
     */
    public Set<String> getExternalTableRealPartitionName(Table table, String mvPartitionName) {
        if (!table.isNativeTableOrMaterializedView()) {
            Preconditions.checkState(externalRefBaseTableMVPartitionMap.containsKey(table));
            return externalRefBaseTableMVPartitionMap.get(table).get(mvPartitionName);
        } else {
            return Sets.newHashSet(mvPartitionName);
        }
    }
}
