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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.rule.transformation.partition.PartitionSelector;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MVRefreshPartitionSelector {

    private long currentTotalRows = 0;
    private long currentTotalBytes = 0;
    private int currentTotalSelectedPartitions = 0;

    private final long maxRowsThreshold;
    private final long maxBytesThreshold;
    private final int maxSelectedPartitions;

    private final Map<Table, Map<String, Set<String>>> externalPartitionMap;
    /**
     * Minimum required ratio of collected partition statistics.
     * If the collected stats are less than this ratio compared to the expected partitions,
     * the adaptive refresh will be considered unreliable and fallback will be triggered.
     */
    private static final double MIN_COLLECTED_PARTITION_STATS_RATIO = 0.9;


    // ensure that at least one partition is selected
    private boolean isFirstPartition = true;

    public MVRefreshPartitionSelector(long maxRowsThreshold, long maxBytesThreshold, int maxPartitionNum) {
        this(maxRowsThreshold, maxBytesThreshold, maxPartitionNum, Collections.emptyMap());
    }

    public MVRefreshPartitionSelector(long maxRowsThreshold,
                                      long maxBytesThreshold,
                                      int maxPartitionNum,
                                      Map<Table, Map<String, Set<String>>> externalPartitionMap) {
        this.maxRowsThreshold = maxRowsThreshold;
        this.maxBytesThreshold = maxBytesThreshold;
        this.maxSelectedPartitions = maxPartitionNum;
        this.externalPartitionMap = externalPartitionMap != null ? externalPartitionMap : Collections.emptyMap();
    }

    /**
     * Check if the incoming partition set can be added based on current total usage and thresholds.
     * Always allows the first partition.
     */
    public boolean canAddPartition(Map<Table, Set<String>> partitionSet) throws MVAdaptiveRefreshException {
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
    public void addPartition(Map<Table, Set<String>> partitionSet) throws MVAdaptiveRefreshException {
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
    private long[] estimatePartitionUsage(Map<Table, Set<String>> partitionSet) throws MVAdaptiveRefreshException {
        long totalRows = 0;
        long totalBytes = 0;

        for (Map.Entry<Table, Set<String>> entry : partitionSet.entrySet()) {
            Table table = entry.getKey();
            Set<String> refBaseTablePartitions = entry.getValue();
            Set<String> finalPartitionNames = refBaseTablePartitions.stream()
                    .map(p -> resolveFinalPartitionNames(table, p))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            Map<String, Pair<Long, Long>> partitionStats = collectSelectedPartitionStats(table, finalPartitionNames);

            int expectedCount = finalPartitionNames.size();
            int actualCount = partitionStats.size();

            if (actualCount == 0 || actualCount < expectedCount * MIN_COLLECTED_PARTITION_STATS_RATIO) {
                throw new MVAdaptiveRefreshException(
                        String.format("Missing too many partition stats for table %s: expected %d, got %d",
                                table.getName(), expectedCount, actualCount));
            }

            for (String partitionName : finalPartitionNames) {
                Pair<Long, Long> stats = partitionStats.get(partitionName);
                if (stats != null) {
                    totalRows += stats.first;
                    totalBytes += stats.second;
                }
            }
        }

        return new long[] {totalRows, totalBytes};
    }

    /**
     * Resolves the final set of partition names for a given table and logical partition name.
     * <p>
     * For OLAP tables, the mapping is one-to-one, so the original partition name is returned as a singleton set.
     * For external tables (e.g., Hive, Iceberg), the logical partition name may map to multiple actual partitions,
     * and the method returns all corresponding partition names from the precomputed external partition mapping.
     *
     * @param table the table from which to resolve the partition names
     * @param logicalPartitionNames the logical partition name used in the materialized view
     * @return a set of actual partition names in the base table corresponding to the given logical partition name
     */
    private Set<String> resolveFinalPartitionNames(Table table, String logicalPartitionNames) {
        if (table instanceof OlapTable) {
            return Collections.singleton(logicalPartitionNames);
        }
        Map<String, Set<String>> mapping = externalPartitionMap.get(table);
        return mapping != null
                ? mapping.getOrDefault(logicalPartitionNames, Collections.emptySet())
                : Collections.emptySet();
    }

    /**
     * Returns a map of partition statistics for the given table.
     * Each entry in the map represents a partition, with the key being the partition name,
     * and the value being a pair of (rowCount, dataSize).
     * For internal OLAP tables, statistics are retrieved directly from the partition metadata.
     * For external tables, only Hive, Iceberg, Hudi, and Delta Lake are currently supported for statistics collection.
     * Statistics for these external tables are retrieved via the statistic executor and may be partial.
     * Note: For external tables, statistics are aggregated by partition name,
     * since there may be multiple statistic records for the same partition.
     *
     * @param table the table to collect statistics from
     * @return a map of partition name to (rowCount, dataSize)
     */
    private Map<String, Pair<Long, Long>> collectSelectedPartitionStats(Table table, Set<String> selectedPartitions) {
        Map<String, Pair<Long, Long>> result = new HashMap<>();

        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            for (String partitionName : selectedPartitions) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition != null) {
                    long rowCount = partition.getRowCount();
                    long dataSize = partition.getDataSize();
                    result.put(partitionName, Pair.create(rowCount, dataSize));
                }
            }
        } else {
            result = PartitionSelector.getExternalTablePartitionStats(table, selectedPartitions);
        }

        return result;
    }
}