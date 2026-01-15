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

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellUtils;
import com.starrocks.sql.common.PCellWithName;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Map;

/**
 * Store the update information of MV used for mv rewrite(mv refresh can use it later).
 */
public class MvUpdateInfo {
    private final MaterializedView mv;
    // The type of mv refresh later
    private final MvToRefreshType mvToRefreshType;
    // The partition names of mv to refresh
    private final PCellSortedSet mvToRefreshPCells = PCellSortedSet.of();
    // The update information of base table
    private final Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfos = Maps.newHashMap();
    // The mapping: <ref base table, <ref base table partition name, mv partition cell>>
    private final Map<Table, PCellSetMapping> basePartNameToMVPCells = Maps.newHashMap();
    //  The mapping: <mv partition name, <ref base table, ref base partition cell>>
    private final Map<String, Map<Table, PCellSortedSet>> mvPartNameToBasePCells = Maps.newHashMap();
    // The consistency mode of query rewrite
    private final TableProperty.QueryRewriteConsistencyMode queryRewriteConsistencyMode;

    // If the base table is a mv, needs to record the mapping of mv partition name to partition range
    private final PCellSortedSet refBaseNestedMVPCells = PCellSortedSet.of();

    /**
     * Marks the type of mv refresh later.
     */
    public enum MvToRefreshType {
        FULL, // full refresh since non ref base table is updated or mv is invalid
        PARTIAL, // partial refresh since ref base table is updated
        NO_REFRESH, // no need to refresh since ref base table is not updated
        UNKNOWN // unknown type
    }

    public MvUpdateInfo(MaterializedView mv, MvToRefreshType mvToRefreshType) {
        this.mv = mv;
        this.mvToRefreshType = mvToRefreshType;
        this.queryRewriteConsistencyMode = TableProperty.QueryRewriteConsistencyMode.CHECKED;
    }

    public MvUpdateInfo(MaterializedView mv, MvToRefreshType mvToRefreshType,
                        TableProperty.QueryRewriteConsistencyMode queryRewriteConsistencyMode) {
        this.mv = mv;
        this.mvToRefreshType = mvToRefreshType;
        this.queryRewriteConsistencyMode = queryRewriteConsistencyMode;
    }

    public static MvUpdateInfo unknown(MaterializedView mv) {
        return new MvUpdateInfo(mv, MvToRefreshType.UNKNOWN);
    }

    public static MvUpdateInfo noRefresh(MaterializedView mv) {
        return new MvUpdateInfo(mv, MvToRefreshType.NO_REFRESH);
    }

    public static MvUpdateInfo fullRefresh(MaterializedView mv) {
        return new MvUpdateInfo(mv, MvToRefreshType.FULL);
    }

    public static MvUpdateInfo partialRefresh(MaterializedView mv,
                                              TableProperty.QueryRewriteConsistencyMode queryRewriteConsistencyMode) {
        return new MvUpdateInfo(mv, MvToRefreshType.PARTIAL, queryRewriteConsistencyMode);
    }

    public MvToRefreshType getMVToRefreshType() {
        return mvToRefreshType;
    }

    public boolean isValidRewrite() {
        return mvToRefreshType == MvToRefreshType.PARTIAL || mvToRefreshType == MvToRefreshType.NO_REFRESH;
    }

    public void addMVToRefreshPartitionNames(PCellWithName partitionName) {
        mvToRefreshPCells.add(partitionName);
    }

    public void addMVToRefreshPartitionNames(PCellSortedSet partitionNames) {
        mvToRefreshPCells.addAll(partitionNames);
    }

    public PCellSortedSet getMVToRefreshPCells() {
        return mvToRefreshPCells;
    }

    public Map<Table, MvBaseTableUpdateInfo> getBaseTableUpdateInfos() {
        return baseTableUpdateInfos;
    }

    public void addRefBaseTablePCells(Map<Table, PCellSortedSet> refBaseTablePCells) {
        refBaseTablePCells.entrySet()
                .forEach(e -> {
                    baseTableUpdateInfos.computeIfAbsent(e.getKey(), k -> MvBaseTableUpdateInfo.of())
                            .getRefBaseTablePCells().addAll(e.getValue());
                });
    }

    public Map<String, Map<Table, PCellSortedSet>> getMVPartNameToBasePCells() {
        return mvPartNameToBasePCells;
    }

    public Map<Table, PCellSetMapping> getBasePartNameToMVPCells() {
        return basePartNameToMVPCells;
    }

    public TableProperty.QueryRewriteConsistencyMode getQueryRewriteConsistencyMode() {
        return queryRewriteConsistencyMode;
    }

    public void addMVPartitionNameToCellMap(PCellSortedSet m) {
        refBaseNestedMVPCells.addAll(m);
    }

    public PCellSortedSet getRefBaseNestedMVPCells() {
        return refBaseNestedMVPCells;
    }

    public MaterializedView getMv() {
        return this.mv;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("refreshType=").append(mvToRefreshType);
        if (!PCellUtils.isEmpty(mvToRefreshPCells)) {
            sb.append(", mvToRefreshPartitionNames=").append(mvToRefreshPCells);
        }
        if (!CollectionUtils.sizeIsEmpty(basePartNameToMVPCells)) {
            sb.append(", basePartToMvPartNames=").append(basePartNameToMVPCells);
        }
        if (!CollectionUtils.sizeIsEmpty(mvPartNameToBasePCells)) {
            sb.append(", mvPartToBasePartNames=");
            int i = 0;
            for (Map.Entry<String, Map<Table, PCellSortedSet>> entry : mvPartNameToBasePCells.entrySet()) {
                if (i > Config.max_mv_task_run_meta_message_values_length) {
                    sb.append("...");
                    break;
                }
                sb.append("[").append(entry.getKey()).append(":");
                sb.append(entry.getValue());
                sb.append("]");
                i++;
            }
        }
        return sb.toString();
    }

    /**
     * Get the ref base table partition names to refresh for the given mv.
     * @param refBaseTable: the input ref base table
     * @return: the partition names to refresh of the ref base table.
     */
    public PCellSortedSet getBaseTableToRefreshPartitionNames(Table refBaseTable) {
        if (mvToRefreshPCells.isEmpty() || mvToRefreshType == MvToRefreshType.NO_REFRESH) {
            return PCellSortedSet.of();
        }
        if (mvToRefreshType == MvToRefreshType.FULL) {
            return null;
        }
        if (CollectionUtils.sizeIsEmpty(mvPartNameToBasePCells)) {
            return null;
        }
        // MV's partition names to refresh are not only affected by the ref base table, but also other base tables.
        // Deduce the partition names to refresh of the ref base table from the partition names to refresh of the mv.
        PCellSortedSet refBaseTableToRefreshPartitionNames = PCellSortedSet.of();
        for (PCellWithName pCell : mvToRefreshPCells.getPartitions()) {
            String mvPartName = pCell.name();
            Map<Table, PCellSortedSet> baseTableToPartNames = mvPartNameToBasePCells.get(mvPartName);
            // means base table's partitions have already dropped.
            if (baseTableToPartNames == null) {
                continue;
            }
            PCellSortedSet partNames = baseTableToPartNames.get(refBaseTable);
            // Continue since mvPartName to refresh is not triggerred by the base table since multi base tables has been
            // supported.
            if (partNames == null) {
                continue;
            }
            refBaseTableToRefreshPartitionNames.addAll(partNames);
        }
        return refBaseTableToRefreshPartitionNames;
    }
}
