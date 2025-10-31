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

import com.google.common.collect.Sets;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PRangeCell;

import java.util.Set;

/**
 * Store the update information of base table for MV
 */
public class MvBaseTableUpdateInfo {
    // The partition names of base table that have been updated
    private final Set<String> toRefreshPartitionNames = Sets.newHashSet();
    // The mapping of partition name to partition range
    private final PCellSortedSet partitionToCells = PCellSortedSet.of();

    // If the base table is a mv, needs to record the mapping of mv partition name to partition range
    private final PCellSortedSet mvPartitionNameToCellMap = PCellSortedSet.of();

    public MvBaseTableUpdateInfo() {
    }

    public PCellSortedSet getMvPartitionNameToCellMap() {
        return mvPartitionNameToCellMap;
    }

    public void addMVPartitionNameToCellMap(PCellSortedSet partitionNameToRangeMap) {
        mvPartitionNameToCellMap.addAll(partitionNameToRangeMap);
    }

    public Set<String> getToRefreshPartitionNames() {
        return toRefreshPartitionNames;
    }

    public PCellSortedSet getPartitionToCells() {
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
     * @param rangeCell the associated range partition
     */
    public void addRangePartitionKeys(String partitionName,
                                      PRangeCell rangeCell) {
        partitionToCells.add(partitionName, rangeCell);
    }

    /**
     * Add partition name that needs to be refreshed and its associated list partition key
     */
    public void addPartitionCells(PCellSortedSet cells) {
        partitionToCells.addAll(cells);
    }

    @Override
    public String toString() {
        return "{" +
                ", toRefreshPartitionNames=" + toRefreshPartitionNames +
                ", nameToPartKeys=" + partitionToCells +
                '}';
    }
}
