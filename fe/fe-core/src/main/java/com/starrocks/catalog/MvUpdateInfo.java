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
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.sql.common.PCell;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.shrinkToSize;

/**
 * Store the update information of MV used for mv rewrite(mv refresh can use it later).
 */
public class MvUpdateInfo {
    private final MaterializedView mv;
    // The type of mv refresh later
    private final MvToRefreshType mvToRefreshType;
    // The partition names of mv to refresh
    private final Set<String> mvToRefreshPartitionNames = Sets.newHashSet();
    // The update information of base table
    private final Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfos = Maps.newHashMap();
    // The mapping of base partition name to mv partition names
    private final Map<Table, Map<String, Set<String>>> basePartToMvPartNames = Maps.newHashMap();
    //  The mapping of mv partition name to base partition names
    private final Map<String, Map<Table, Set<String>>> mvPartToBasePartNames = Maps.newHashMap();
    // The consistency mode of query rewrite
    private final TableProperty.QueryRewriteConsistencyMode queryRewriteConsistencyMode;

    // If the base table is a mv, needs to record the mapping of mv partition name to partition range
    private final Map<String, PCell> mvPartitionNameToCellMap = Maps.newHashMap();

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

    public MvToRefreshType getMvToRefreshType() {
        return mvToRefreshType;
    }

    public boolean isValidRewrite() {
        return mvToRefreshType == MvToRefreshType.PARTIAL || mvToRefreshType == MvToRefreshType.NO_REFRESH;
    }

    public void addMvToRefreshPartitionNames(String partitionName) {
        mvToRefreshPartitionNames.add(partitionName);
    }

    public void addMvToRefreshPartitionNames(Set<String> partitionNames) {
        mvToRefreshPartitionNames.addAll(partitionNames);
    }

    public Set<String> getMvToRefreshPartitionNames() {
        return mvToRefreshPartitionNames;
    }

    public Map<Table, MvBaseTableUpdateInfo> getBaseTableUpdateInfos() {
        return baseTableUpdateInfos;
    }

    public Map<String, Map<Table, Set<String>>> getMvPartToBasePartNames() {
        return mvPartToBasePartNames;
    }

    public Map<Table, Map<String, Set<String>>> getBasePartToMvPartNames() {
        return basePartToMvPartNames;
    }

    public TableProperty.QueryRewriteConsistencyMode getQueryRewriteConsistencyMode() {
        return queryRewriteConsistencyMode;
    }

    public void addMVPartitionNameToCellMap(Map<String, PCell> m) {
        mvPartitionNameToCellMap.putAll(m);
    }

    public Map<String, PCell> getMvPartitionNameToCellMap() {
        return mvPartitionNameToCellMap;
    }

    public MaterializedView getMv() {
        return this.mv;
    }

    @Override
    public String toString() {
        int maxLength = Config.max_mv_task_run_meta_message_values_length;
        StringBuilder sb = new StringBuilder();
        sb.append("refreshType=").append(mvToRefreshType);
        if (!CollectionUtils.sizeIsEmpty(mvToRefreshPartitionNames)) {
            sb.append(", mvToRefreshPartitionNames=").append(shrinkToSize(mvToRefreshPartitionNames, maxLength));
        }
        if (!CollectionUtils.sizeIsEmpty(basePartToMvPartNames)) {
            sb.append(", basePartToMvPartNames=");
            for (Map.Entry<Table, Map<String, Set<String>>> entry : basePartToMvPartNames.entrySet()) {
                sb.append("[").append(entry.getKey().getName()).append(":");
                for (Map.Entry<String, Set<String>> entry1 : shrinkToSize(entry.getValue(), maxLength).entrySet()) {
                    sb.append(" ").append(entry1.getKey()).append(":").append(shrinkToSize(entry1.getValue(), maxLength));
                }
                sb.append("]");
            }
        }
        if (!CollectionUtils.sizeIsEmpty(mvPartToBasePartNames)) {
            sb.append(", mvPartToBasePartNames=");
            for (Map.Entry<String, Map<Table, Set<String>>> entry : mvPartToBasePartNames.entrySet()) {
                sb.append("[").append(entry.getKey()).append(":");
                for (Map.Entry<Table, Set<String>> entry1 : shrinkToSize(entry.getValue(), maxLength).entrySet()) {
                    sb.append(" ").append(entry1.getKey().getName()).append(":")
                            .append(shrinkToSize(entry1.getValue(), maxLength));
                }
                sb.append("]");
            }
        }
        return sb.toString();
    }

    /**
     * Get the ref base table partition names to refresh for the given mv.
     * @param refBaseTable: the input ref base table
     * @return: the partition names to refresh of the ref base table.
     */
    public Set<String> getBaseTableToRefreshPartitionNames(Table refBaseTable) {
        if (mvToRefreshPartitionNames.isEmpty() || mvToRefreshType == MvToRefreshType.NO_REFRESH) {
            return Sets.newHashSet();
        }
        if (mvToRefreshType == MvToRefreshType.FULL) {
            return null;
        }
        if (CollectionUtils.sizeIsEmpty(mvPartToBasePartNames)) {
            return null;
        }
        // MV's partition names to refresh are not only affected by the ref base table, but also other base tables.
        // Deduce the partition names to refresh of the ref base table from the partition names to refresh of the mv.
        Set<String> refBaseTableToRefreshPartitionNames = Sets.newHashSet();
        for (String mvPartName : mvToRefreshPartitionNames) {
            Map<Table, Set<String>> baseTableToPartNames = mvPartToBasePartNames.get(mvPartName);
            // means base table's partitions have already dropped.
            if (baseTableToPartNames == null) {
                continue;
            }
            Set<String> partNames = baseTableToPartNames.get(refBaseTable);
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
