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

import java.util.Map;
import java.util.Set;

/**
 * Store the update information of MV used for mv rewrite(mv refresh can use it later).
 */
public class MvUpdateInfo {
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

    /**
     * Marks the type of mv refresh later.
     */
    public enum MvToRefreshType {
        FULL, // full refresh since non ref base table is updated or mv is invalid
        PARTIAL, // partial refresh since ref base table is updated
        NO_REFRESH, // no need to refresh since ref base table is not updated
        UNKNOWN // unknown type
    }

    public MvUpdateInfo(MvToRefreshType mvToRefreshType) {
        this.mvToRefreshType = mvToRefreshType;
        this.queryRewriteConsistencyMode = TableProperty.QueryRewriteConsistencyMode.CHECKED;
    }

    public MvUpdateInfo(MvToRefreshType mvToRefreshType, TableProperty.QueryRewriteConsistencyMode queryRewriteConsistencyMode) {
        this.mvToRefreshType = mvToRefreshType;
        this.queryRewriteConsistencyMode = queryRewriteConsistencyMode;
    }

    public MvToRefreshType getMvToRefreshType() {
        return mvToRefreshType;
    }

    public boolean isValidRewrite() {
        return mvToRefreshType == MvToRefreshType.PARTIAL || mvToRefreshType == MvToRefreshType.NO_REFRESH;
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

    @Override
    public String toString() {
        return "MvUpdateInfo{" +
                "refreshType=" + mvToRefreshType +
                ", mvToRefreshPartitionNames=" + mvToRefreshPartitionNames +
                ", basePartToMvPartNames=" + basePartToMvPartNames +
                ", mvPartToBasePartNames=" + mvPartToBasePartNames +
                '}';
    }

    /**
     * @return the detail string of the mv update info
     */
    public String toDetailString() {
        return "MvUpdateInfo{" +
                "refreshType=" + mvToRefreshType +
                ", mvToRefreshPartitionNames=" + mvToRefreshPartitionNames +
                ", baseTableUpdateInfos=" + baseTableUpdateInfos +
                ", basePartToMvPartNames=" + basePartToMvPartNames +
                ", mvPartToBasePartNames=" + mvPartToBasePartNames +
                '}';
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
        if (queryRewriteConsistencyMode == TableProperty.QueryRewriteConsistencyMode.LOOSE) {
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo = baseTableUpdateInfos.get(refBaseTable);
            if (mvBaseTableUpdateInfo == null) {
                return null;
            }
            return mvBaseTableUpdateInfo.getToRefreshPartitionNames();
        }

        if (mvPartToBasePartNames == null || mvPartToBasePartNames.isEmpty()) {
            return null;
        }
        // MV's partition names to refresh is not only affected by the ref base table, but also other base tables.
        // Deduce the partition names to refresh of the ref base table from the partition names to refresh of the mv.
        Set<String> refBaseTableToRefreshPartitionNames = Sets.newHashSet();
        for (String mvPartName : mvToRefreshPartitionNames) {
            Map<Table, Set<String>> baseTableToPartNames = mvPartToBasePartNames.get(mvPartName);
            // means base table's partitions have already dropped.
            if (baseTableToPartNames == null) {
                continue;
            }
            Set<String> partNames = baseTableToPartNames.get(refBaseTable);
            if (partNames == null) {
                return null;
            }
            refBaseTableToRefreshPartitionNames.addAll(partNames);
        }
        return refBaseTableToRefreshPartitionNames;
    }
}
