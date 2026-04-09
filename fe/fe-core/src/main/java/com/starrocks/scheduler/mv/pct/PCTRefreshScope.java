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

import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class PCTRefreshScope {

    /**
     * The MV partitions selected for this task run after refresh-mode decisions and partition filtering.
     * <p>
     * This is the final MV-side refresh target used by planning and metadata update. It may be smaller than
     * the initially detected partition-change set when batching, adaptive refresh, or user-specified partition
     * ranges narrow the execution scope.
     */
    private final PCellSortedSet mvPartitionsToRefresh;

    /**
     * The base-table partitions that this task run should read or refresh, keyed by the snapshot table object.
     * <p>
     * This is the execution-oriented projection of {@link #mvPartitionsToRefresh}: once the MV partitions are
     * fixed, the refresh flow maps them back to the precise partitions of each referenced base table that
     * intersect with those MV partitions.
     */
    private final Map<BaseTableSnapshotInfo, PCellSortedSet> refTableRefreshPartitions;

    /**
     * A table-name keyed view of {@link #refTableRefreshPartitions} for components that do not need the full
     * snapshot object and only require table-name to partition-name mapping.
     * <p>
     * This is the compact representation passed into refresh-plan building and task-run status reporting.
     */
    private final PCellSetMapping refTablePartitionNames;

    /**
     * Whether this task run semantically represents a complete refresh of the MV refresh scope.
     * <p>
     * This flag records the refresh decision made earlier in the pipeline, for example when force refresh or
     * non-ref table changes require treating the current run as a full logical refresh rather than an
     * incrementally reduced partition subset.
     */
    private final boolean completeRefresh;

    /**
     * Whether the refresh computation discovered additional potential MV partitions beyond the directly changed set.
     * <p>
     * This is used to preserve the fact that many-to-many partition relationships expanded the candidate scope,
     * even if later batching or filtering narrowed the final execution set recorded in this object.
     */
    private final boolean hasPotentialPartitions;

    public PCTRefreshScope(PCellSortedSet mvPartitionsToRefresh,
                           Map<BaseTableSnapshotInfo, PCellSortedSet> refTableRefreshPartitions,
                           PCellSetMapping refTablePartitionNames,
                           boolean completeRefresh,
                           boolean hasPotentialPartitions) {
        this.mvPartitionsToRefresh = copyPartitions(mvPartitionsToRefresh);
        this.refTableRefreshPartitions = copyRefTableRefreshPartitions(refTableRefreshPartitions);
        this.refTablePartitionNames = copyRefTablePartitionNames(refTablePartitionNames);
        this.completeRefresh = completeRefresh;
        this.hasPotentialPartitions = hasPotentialPartitions;
    }

    public PCellSortedSet getMvPartitionsToRefresh() {
        return copyPartitions(this.mvPartitionsToRefresh);
    }

    public Map<BaseTableSnapshotInfo, PCellSortedSet> getRefTableRefreshPartitions() {
        return copyRefTableRefreshPartitions(this.refTableRefreshPartitions);
    }

    public PCellSetMapping getRefTablePartitionNames() {
        return copyRefTablePartitionNames(this.refTablePartitionNames);
    }

    public boolean isCompleteRefresh() {
        return completeRefresh;
    }

    public boolean hasPotentialPartitions() {
        return hasPotentialPartitions;
    }

    public boolean isEmpty() {
        return this.mvPartitionsToRefresh.isEmpty()
                && this.refTableRefreshPartitions.isEmpty()
                && this.refTablePartitionNames.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PCTRefreshScope)) {
            return false;
        }
        PCTRefreshScope that = (PCTRefreshScope) o;
        return completeRefresh == that.completeRefresh
                && hasPotentialPartitions == that.hasPotentialPartitions
                && Objects.equals(mvPartitionsToRefresh, that.mvPartitionsToRefresh)
                && Objects.equals(refTableRefreshPartitions, that.refTableRefreshPartitions)
                && Objects.equals(refTablePartitionNames, that.refTablePartitionNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mvPartitionsToRefresh, refTableRefreshPartitions, refTablePartitionNames,
                completeRefresh, hasPotentialPartitions);
    }

    @Override
    public String toString() {
        return "PCTRefreshScope{"
                + "mvPartitionsToRefresh=" + mvPartitionsToRefresh
                + ", refTableRefreshPartitions=" + refTableRefreshPartitions
                + ", refTablePartitionNames=" + refTablePartitionNames
                + ", completeRefresh=" + completeRefresh
                + ", hasPotentialPartitions=" + hasPotentialPartitions
                + '}';
    }

    private static PCellSortedSet copyPartitions(PCellSortedSet partitions) {
        return partitions == null ? PCellSortedSet.of() : PCellSortedSet.of(partitions);
    }

    private static Map<BaseTableSnapshotInfo, PCellSortedSet> copyRefTableRefreshPartitions(
            Map<BaseTableSnapshotInfo, PCellSortedSet> refTableRefreshPartitions) {
        Map<BaseTableSnapshotInfo, PCellSortedSet> copied = new LinkedHashMap<>();
        if (refTableRefreshPartitions != null) {
            refTableRefreshPartitions.forEach((snapshotInfo, partitions) ->
                    copied.put(snapshotInfo, copyPartitions(partitions)));
        }
        return Map.copyOf(copied);
    }

    private static PCellSetMapping copyRefTablePartitionNames(PCellSetMapping refTablePartitionNames) {
        PCellSetMapping copied = PCellSetMapping.of();
        if (refTablePartitionNames != null) {
            refTablePartitionNames.mapping().forEach((tableName, partitions) ->
                    copied.mapping().put(tableName, copyPartitions(partitions)));
        }
        return copied;
    }
}
