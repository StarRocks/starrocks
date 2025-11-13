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

package com.starrocks.transaction;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Information about the insert-overwrite job
 */
public class InsertOverwriteJobStats {

    private List<Long> sourcePartitionIds = new ArrayList<>();
    private List<Long> targetPartitionIds = new ArrayList<>();
    private long sourceRows;
    private long targetRows;
    // Map<PartitionId, Map<TabletId, RowCount>>
    private Table<Long, Long, Long> partitionTabletRowCounts = HashBasedTable.create();

    public InsertOverwriteJobStats() {
    }

    public InsertOverwriteJobStats(List<Long> sourcePartitionIds, List<Long> targetPartitionIds, long sourceRows,
                                   long targetRows) {
        this.sourcePartitionIds = sourcePartitionIds;
        this.targetPartitionIds = targetPartitionIds;
        this.sourceRows = sourceRows;
        this.targetRows = targetRows;
    }

    public List<Long> getTargetPartitionIds() {
        return targetPartitionIds;
    }

    public void setTargetPartitionIds(List<Long> targetPartitionIds) {
        this.targetPartitionIds = targetPartitionIds;
    }

    public List<Long> getSourcePartitionIds() {
        return sourcePartitionIds;
    }

    public void setSourcePartitionIds(List<Long> sourcePartitionIds) {
        this.sourcePartitionIds = sourcePartitionIds;
    }

    public long getSourceRows() {
        return sourceRows;
    }

    public void setSourceRows(long sourceRows) {
        this.sourceRows = sourceRows;
    }

    public long getTargetRows() {
        return targetRows;
    }

    public void setTargetRows(long targetRows) {
        this.targetRows = targetRows;
    }

    public Table<Long, Long, Long> getPartitionTabletRowCounts() {
        return partitionTabletRowCounts;
    }

    public void setPartitionTabletRowCounts(Table<Long, Long, Long> partitionTabletRowCounts) {
        this.partitionTabletRowCounts = partitionTabletRowCounts;
    }
}
