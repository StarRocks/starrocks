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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.pct.PCTPartitionTopology;
import com.starrocks.scheduler.mv.pct.PCTRefreshScope;
import com.starrocks.sql.plan.ExecPlan;

import java.util.Map;
import java.util.Set;

public class MvTaskRunContext extends TaskRunContext {
    public static class MVRefreshRuntimeState {
        private final Map<Long, BaseTableSnapshotInfo> snapshotBaseTables = Maps.newHashMap();

        // Pinned TvrVersionRange per base table, keyed by Table.getTableIdentifier() (stable across
        // connector getTable() calls, unlike tableId for external tables).
        private final Map<String, TvrVersionRange> pinnedTvrMap = Maps.newHashMap();

        public Map<Long, BaseTableSnapshotInfo> getSnapshotBaseTables() {
            return snapshotBaseTables;
        }

        public void replaceSnapshotBaseTables(Map<Long, BaseTableSnapshotInfo> snapshotBaseTables) {
            this.snapshotBaseTables.clear();
            this.snapshotBaseTables.putAll(snapshotBaseTables);
        }

        public Map<String, TvrVersionRange> getPinnedTvrMap() {
            return pinnedTvrMap;
        }

        public void reset() {
            snapshotBaseTables.clear();
            pinnedTvrMap.clear();
        }
    }

    private PCTPartitionTopology partitionTopology;
    private PCTRefreshScope refreshScope;

    private String nextPartitionStart = null;
    private String nextPartitionEnd = null;
    // The next list partition values to be processed
    private String nextPartitionValues = null;
    private ExecPlan execPlan = null;

    private int partitionTTLNumber = TableProperty.INVALID;
    private final MVRefreshRuntimeState refreshRuntimeState = new MVRefreshRuntimeState();

    public MvTaskRunContext(TaskRunContext context) {
        super(context);
    }

    public MVRefreshRuntimeState getRefreshRuntimeState() {
        return refreshRuntimeState;
    }

    public PCTPartitionTopology getPartitionTopology() {
        return partitionTopology;
    }

    public void setPartitionTopology(PCTPartitionTopology partitionTopology) {
        this.partitionTopology = partitionTopology;
    }

    public PCTRefreshScope getRefreshScope() {
        return refreshScope;
    }

    public void setRefreshScope(PCTRefreshScope refreshScope) {
        this.refreshScope = refreshScope;
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
            Preconditions.checkState(partitionTopology != null
                    && partitionTopology.getRefBaseTableToCellMap().containsKey(table));
            return partitionTopology.getRefBaseTableToCellMap().get(table).getSourceNames(mvPartitionName);
        } else {
            return Sets.newHashSet(mvPartitionName);
        }
    }
}
