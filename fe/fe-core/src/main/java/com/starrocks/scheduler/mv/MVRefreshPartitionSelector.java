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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;

import java.util.Map;
import java.util.Set;

public class MVRefreshPartitionSelector {

    private long currentTotalRows = 0;
    private long currentTotalBytes = 0;
    private int currentTotalSelectedPartitions = 0;

    private final long maxRowsThreshold;
    private final long maxBytesThreshold;
    private final int maxSelectedPartitions;

    // ensure that at least one partition is selected
    private boolean isFirstPartition = true;

    public MVRefreshPartitionSelector(long maxRowsThreshold, long maxBytesThreshold, int maxPartitionNum) {
        this.maxRowsThreshold = maxRowsThreshold;
        this.maxBytesThreshold = maxBytesThreshold;
        this.maxSelectedPartitions = maxPartitionNum;
    }

    /**
     * Check if the incoming partition set can be added based on current total usage and thresholds.
     * Always allows the first partition.
     */
    public boolean canAddPartition(Map<Table, Set<String>> partitionSet) {
        if (isFirstPartition) {
            return true;
        }

        if (currentTotalSelectedPartitions >= maxSelectedPartitions) {
            return false;
        }

        long[] usage = estimatePartitionUsage(partitionSet);
        long incomingRows = usage[0];
        long incomingBytes = usage[1];

        return (currentTotalRows + incomingRows <= maxRowsThreshold) &&
                (currentTotalBytes + incomingBytes <= maxBytesThreshold);
    }

    /**
     * Actually add the given partition set to the total usage.
     * This should be called after canAddPartitionSet() returns true.
     */
    public void addPartition(Map<Table, Set<String>> partitionSet) {
        long[] usage = estimatePartitionUsage(partitionSet);
        currentTotalRows += usage[0];
        currentTotalBytes += usage[1];
        isFirstPartition = false;
        currentTotalSelectedPartitions++;
    }

    /**
     * Estimate total row count and data size of a partition set.
     *
     * @return array of [rows, bytes]
     */
    private long[] estimatePartitionUsage(Map<Table, Set<String>> partitionSet) {
        long totalRows = 0;
        long totalBytes = 0;

        for (Map.Entry<Table, Set<String>> entry : partitionSet.entrySet()) {
            Table table = entry.getKey();
            for (String partitionName : entry.getValue()) {
                Partition partition = table.getPartition(partitionName);
                totalRows += partition.getRowCount();
                totalBytes += partition.getDataSize();
            }
        }
        return new long[] {totalRows, totalBytes};
    }
}