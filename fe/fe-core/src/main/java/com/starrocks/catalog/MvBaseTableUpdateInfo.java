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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PRangeCell;

import java.util.Map;
import java.util.Set;

/**
 * Store the update information of base table for MV
 */
public class MvBaseTableUpdateInfo {
    // The partition names of base table that have been updated
    private final Set<String> toRefreshPartitionNames = Sets.newHashSet();
    // The mapping of partition name to partition range
    private final Map<String, PCell> partitionToCells = Maps.newHashMap();

    // If the base table is a mv, needs to record the mapping of mv partition name to partition range
    private final Map<String, PCell> mvPartitionNameToCellMap = Maps.newHashMap();

    public MvBaseTableUpdateInfo() {
    }

    public Map<String, PCell> getMvPartitionNameToCellMap() {
        return mvPartitionNameToCellMap;
    }

    public void addMVPartitionNameToCellMap(Map<String, PCell> partitionNameToRangeMap) {
        mvPartitionNameToCellMap.putAll(partitionNameToRangeMap);
    }

    public Set<String> getToRefreshPartitionNames() {
        return toRefreshPartitionNames;
    }

    public Map<String, PCell> getPartitionToCells() {
        return partitionToCells;
    }

    /**
     * Add partition names that base table needs to be refreshed
     * @param toRefreshPartitionNames the partition names that need to be refreshed
     */
    public void addToRefreshPartitionNames(Set<String> toRefreshPartitionNames) {
        this.toRefreshPartitionNames.addAll(toRefreshPartitionNames);
    }

    /**
     * Add partition name that needs to be refreshed and its associated range partition key
     * @param partitionName mv partition name
     * @param rangePartitionKey the associated range partition
     */
    public void addRangePartitionKeys(String partitionName,
                                      Range<PartitionKey> rangePartitionKey) {
        partitionToCells.put(partitionName, new PRangeCell(rangePartitionKey));
    }

    /**
     * Add partition name that needs to be refreshed and its associated list partition key
     */
    public void addPartitionCells(Map<String, PCell> cells) {
        partitionToCells.putAll(cells);
    }

    /**
     * Get the partition name with its associated range partition key when the mv is range partitioned.
     */
    public Map<String, Range<PartitionKey>> getPartitionNameWithRanges() {
        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        for (Map.Entry<String, PCell> e : partitionToCells.entrySet()) {
            Preconditions.checkState(e.getValue() instanceof PRangeCell);
            PRangeCell rangeCell = (PRangeCell) e.getValue();
            result.put(e.getKey(), rangeCell.getRange());
        }
        return result;
    }

    /**
     * Get the partition name with its associated list partition key when the mv is list partitioned.
     */
    public Map<String, PListCell> getPartitionNameWithLists() {
        Map<String, PListCell> result = Maps.newHashMap();
        for (Map.Entry<String, PCell> e : partitionToCells.entrySet()) {
            Preconditions.checkState(e.getValue() instanceof PListCell);
            PListCell listCell = (PListCell) e.getValue();
            result.put(e.getKey(), listCell);
        }
        return result;
    }

    @Override
    public String toString() {
        return "BaseTableRefreshInfo{" +
                ", toRefreshPartitionNames=" + toRefreshPartitionNames +
                ", nameToPartKeys=" + partitionToCells +
                '}';
    }
}
