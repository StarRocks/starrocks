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

package com.starrocks.scheduler.mv.pct;

import com.starrocks.catalog.Table;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PCTRefreshScopeCalculator {

    public Map<BaseTableSnapshotInfo, PCellSortedSet> projectRefTableRefreshPartitions(
            PCTPartitionTopology topology,
            Map<Long, BaseTableSnapshotInfo> snapshotBaseTables,
            PCellSortedSet mvPartitionsToRefresh) {
        Map<BaseTableSnapshotInfo, PCellSortedSet> refTableRefreshPartitions = new LinkedHashMap<>();
        if (topology == null || snapshotBaseTables == null || mvPartitionsToRefresh == null || mvPartitionsToRefresh.isEmpty()) {
            return refTableRefreshPartitions;
        }

        Map<String, Map<Table, PCellSortedSet>> mvToBasePartitions = topology.getMvRefBaseTableIntersectedPartitions();
        if (mvToBasePartitions == null || mvToBasePartitions.isEmpty()) {
            return refTableRefreshPartitions;
        }

        for (BaseTableSnapshotInfo snapshotInfo : snapshotBaseTables.values()) {
            Table snapshotTable = snapshotInfo.getBaseTable();
            PCellSortedSet projectedPartitions = null;
            for (PCellWithName mvPartition : mvPartitionsToRefresh.getPartitions()) {
                Map<Table, PCellSortedSet> refTablePartitions = mvToBasePartitions.get(mvPartition.name());
                if (refTablePartitions == null || !refTablePartitions.containsKey(snapshotTable)) {
                    continue;
                }
                if (projectedPartitions == null) {
                    projectedPartitions = PCellSortedSet.of();
                }
                PCellSortedSet intersectedPartitions = refTablePartitions.get(snapshotTable);
                if (intersectedPartitions != null) {
                    projectedPartitions.addAll(intersectedPartitions);
                }
            }
            if (projectedPartitions != null) {
                refTableRefreshPartitions.put(snapshotInfo, projectedPartitions);
            }
        }
        return refTableRefreshPartitions;
    }

    public PCTRefreshScope buildScope(PCTPartitionTopology topology,
                                      Map<Long, BaseTableSnapshotInfo> snapshotBaseTables,
                                      PCellSortedSet mvPartitionsToRefresh,
                                      boolean completeRefresh,
                                      boolean hasPotentialPartitions) {
        Map<BaseTableSnapshotInfo, PCellSortedSet> refTableRefreshPartitions = projectRefTableRefreshPartitions(
                topology, snapshotBaseTables, mvPartitionsToRefresh);
        PCellSetMapping refTablePartitionNames = PCellSetMapping.of(refTableRefreshPartitions.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().getName(),
                        entry -> PCellSortedSet.of(entry.getValue()))));
        return new PCTRefreshScope(
                mvPartitionsToRefresh,
                refTableRefreshPartitions,
                refTablePartitionNames,
                completeRefresh,
                hasPotentialPartitions);
    }
}
