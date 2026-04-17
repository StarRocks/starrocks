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
import com.starrocks.mv.pct.BaseToMVPartitionMapping;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;

import java.util.Map;
import java.util.Objects;

public final class PCTPartitionTopology {

    /**
     * All materialized view partitions and their normalized partition cells.
     * <p>
     * Key is the MV partition name, value is the range/list cell of that partition.
     * This is the canonical partition space on the MV side and is used later to:
     * <p>
     * 1. convert MV partition names back to their range/list semantics
     * 2. generate MV-level partition predicates during refresh planning
     * 3. reason about recursive potential refresh partitions
     */
    private final PCellSortedSet mvToCellMap;

    /**
     * For each ref base table, stores its partition mapping including both normalized partition cells
     * and an optional source name mapping that traces MV partition names back to original base partition names.
     * <p>
     * Key is the base table object, value is the {@link BaseToMVPartitionMapping} for that table.
     * This lets the refresh pipeline recover the exact range/list cell behind a base-table partition name
     * when generating predicates or projecting refresh work back to base tables.
     */
    private final Map<Table, BaseToMVPartitionMapping> refBaseTableToCellMap;

    /**
     * Forward intersection mapping from base-table partitions to MV partitions.
     * <p>
     * Key is a ref base table, and the nested mapping answers:
     * "given one partition of this base table, which MV partitions intersect with it?"
     * This is primarily used when the refresh flow starts from changed base-table partitions and needs
     * to derive the candidate MV partitions that must be refreshed.
     */
    private final Map<Table, PCellSetMapping> refBaseTableMVIntersectedPartitions;

    /**
     * Reverse intersection mapping from MV partitions back to ref base-table partitions.
     * <p>
     * Key is an MV partition name, value is a map from each ref base table to the partitions of that
     * table intersecting with the MV partition. This is the core projection structure used when the
     * refresh flow already knows the MV partitions to refresh and needs to derive which base-table
     * partitions should be scanned or refreshed for this task run.
     */
    private final Map<String, Map<Table, PCellSortedSet>> mvRefBaseTableIntersectedPartitions;

    public PCTPartitionTopology(PCellSortedSet mvToCellMap,
                                Map<Table, BaseToMVPartitionMapping> refBaseTableToCellMap,
                                Map<Table, PCellSetMapping> refBaseTableMVIntersectedPartitions,
                                Map<String, Map<Table, PCellSortedSet>> mvRefBaseTableIntersectedPartitions) {
        this.mvToCellMap = mvToCellMap;
        this.refBaseTableToCellMap = refBaseTableToCellMap;
        this.refBaseTableMVIntersectedPartitions = refBaseTableMVIntersectedPartitions;
        this.mvRefBaseTableIntersectedPartitions = mvRefBaseTableIntersectedPartitions;
    }

    public PCellSortedSet getMvToCellMap() {
        return mvToCellMap;
    }

    public Map<Table, BaseToMVPartitionMapping> getRefBaseTableToCellMap() {
        return refBaseTableToCellMap;
    }

    public Map<Table, PCellSetMapping> getRefBaseTableMVIntersectedPartitions() {
        return refBaseTableMVIntersectedPartitions;
    }

    public Map<String, Map<Table, PCellSortedSet>> getMvRefBaseTableIntersectedPartitions() {
        return mvRefBaseTableIntersectedPartitions;
    }

    public static PCTPartitionTopology empty() {
        return new PCTPartitionTopology(PCellSortedSet.of(), Map.of(), Map.of(), Map.of());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PCTPartitionTopology)) {
            return false;
        }
        PCTPartitionTopology that = (PCTPartitionTopology) o;
        return Objects.equals(mvToCellMap, that.mvToCellMap)
                && Objects.equals(refBaseTableToCellMap, that.refBaseTableToCellMap)
                && Objects.equals(refBaseTableMVIntersectedPartitions, that.refBaseTableMVIntersectedPartitions)
                && Objects.equals(mvRefBaseTableIntersectedPartitions, that.mvRefBaseTableIntersectedPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mvToCellMap, refBaseTableToCellMap, refBaseTableMVIntersectedPartitions,
                mvRefBaseTableIntersectedPartitions);
    }

    @Override
    public String toString() {
        return "PCTPartitionTopology{"
                + "mvToCellMap=" + mvToCellMap
                + ", refBaseTableToCellMap=" + refBaseTableToCellMap
                + ", refBaseTableMVIntersectedPartitions=" + refBaseTableMVIntersectedPartitions
                + ", mvRefBaseTableIntersectedPartitions=" + mvRefBaseTableIntersectedPartitions
                + '}';
    }
}
