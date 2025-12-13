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

package com.starrocks.sql.common;

import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link PRangeCellPlus} contains a {@link PRangeCell} and its partition's name to
 * represent a table's partition range info.
 */
public class PRangeCellPlus implements Comparable<PRangeCellPlus> {
    private final PRangeCell cell;
    private final String partitionName;

    public PRangeCellPlus(String partitionName, Range<PartitionKey> partitionKeyRange) {
        this.partitionName = partitionName;
        this.cell = new PRangeCell(partitionKeyRange);
    }

    public PRangeCellPlus(String partitionName, PRangeCell rangeCell) {
        this.partitionName = partitionName;
        this.cell = rangeCell;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public PRangeCell getCell() {
        return cell;
    }

    public boolean isIntersected(PRangeCellPlus o) {
        return cell.isIntersected(o.getCell());
    }

    /**
     * Get intersected cells.
     *
     * @param cell PRangeCellPlus whose intersected cells will be found.
     * @param ranges need to be sorted.
     * @return intersected cells
     */
    public static List<PRangeCellPlus> getIntersectedCells(PRangeCellPlus cell,
                                                           List<PRangeCellPlus> ranges,
                                                           List<PartitionKey> mvLowerPoints,
                                                           List<PartitionKey> mvUpperPoints) {
        if (ranges.isEmpty()) {
            return List.of();
        }

        PartitionKey lower = cell.getCell().getRange().lowerEndpoint();
        PartitionKey upper = cell.getCell().getRange().upperEndpoint();
        // For an interval [l, r], if there exists another interval [li, ri] that intersects with it, this interval
        // must satisfy l ≤ ri and r ≥ li. Therefore, if there exists a pos_a such that for all k < pos_a,
        // ri[k] < l, and there exists a pos_b such that for all k > pos_b, li[k] > r, then all intervals between
        // pos_a and pos_b might potentially intersect with the interval [l, r].
        int posA = PartitionKey.findLastLessEqualInOrderedList(lower, mvUpperPoints);
        int posB = PartitionKey.findLastLessEqualInOrderedList(upper, mvLowerPoints);

        List<PRangeCellPlus> results = new LinkedList<>();
        for (int i = posA; i <= posB; ++i) {
            if (ranges.get(i).isIntersected(cell)) {
                results.add(ranges.get(i));
            }
        }
        return results;
    }

    public static List<PRangeCellPlus> toPRangeCellPlus(Map<String, PCell> rangeMap) {
        return rangeMap.entrySet().stream()
                .map(e -> new PRangeCellPlus(e.getKey(), (PRangeCell) e.getValue()))
                .sorted(PRangeCellPlus::compareTo)
                .collect(Collectors.toList());
    }

    /**
     * Convert range map to list of partition range cell plus which is sorted by range cell.
     * @param rangeMap range map to be converted
     * @param isConvertToDate whether convert to date partition range which is used for base table with string partition column
     * @return sorted list of partition range cell plus
     */
    public static List<PRangeCellPlus> toPRangeCellPlus(Map<String, Range<PartitionKey>> rangeMap,
                                                        boolean isConvertToDate) {
        return rangeMap.entrySet().stream()
                .map(e -> new PRangeCellPlus(e.getKey(),
                        isConvertToDate ? SyncPartitionUtils.convertToDatePartitionRange(e.getValue()) : e.getValue()))
                .sorted(PRangeCellPlus::compareTo)
                .collect(Collectors.toList());
    }

    /**
     * For base table's partition name & partition range cell, its associated pcell cache key should only remapped to one
     * pRangeCellPlus.
     */
    public static class PCellCacheKey {
        private final Table refBaseTable;
        private final String partitionName;
        private final PRangeCell rangeCell;

        public PCellCacheKey(Table refBaseTable, PRangeCellPlus plus) {
            this(refBaseTable, plus.getPartitionName(), plus.getCell());
        }

        public PCellCacheKey(Table refBaseTable,
                             String partitionName, PRangeCell rangeCell) {
            this.refBaseTable = refBaseTable;
            this.partitionName = partitionName;
            this.rangeCell = rangeCell;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PCellCacheKey that = (PCellCacheKey) o;
            return Objects.equals(refBaseTable, that.refBaseTable) &&
                    Objects.equals(partitionName, that.partitionName) &&
                    Objects.equals(rangeCell, that.rangeCell);
        }

        @Override
        public int hashCode() {
            return Objects.hash(refBaseTable, partitionName, rangeCell);
        }
    }

    /**
     * Convert a range map to list of partition range cell plus which is sorted by range cell.
     */
    private static PRangeCellPlus toPRangeCellPlus(MaterializedView mv, Table refBaseTable,
                                                   String partitionName, PRangeCell rangeCell,
                                                   Expr expr) {
        // for simple expr, no need to do conversion
        if (expr == null || expr instanceof SlotRef) {
            return new PRangeCellPlus(partitionName, rangeCell);
        }

        if (Config.enable_mv_global_context_cache) {
            CachingMvPlanContextBuilder.MVCacheEntity mvCacheEntity = CachingMvPlanContextBuilder.getMVCache(mv);
            PCellCacheKey cacheKey = new PCellCacheKey(refBaseTable, partitionName, rangeCell);
            Object cacheValue = mvCacheEntity.get(cacheKey, () -> toPRangeCellPlus(partitionName, rangeCell, expr));
            if (cacheValue != null && cacheValue instanceof PRangeCellPlus) {
                return (PRangeCellPlus) cacheValue;
            } else {
                return toPRangeCellPlus(partitionName, rangeCell, expr);
            }
        } else {
            return toPRangeCellPlus(partitionName, rangeCell, expr);
        }
    }

    private static PRangeCellPlus toPRangeCellPlus(String partitionName, PRangeCell rangeCell,
                                                   Expr expr) {
        if (expr == null || expr instanceof SlotRef) {
            return new PRangeCellPlus(partitionName, rangeCell);
        } else {
            Range<PartitionKey> partitionKeyRanges = rangeCell.getRange();
            Range<PartitionKey> convertRanges = SyncPartitionUtils.convertToDatePartitionRange(partitionKeyRanges);
            return new PRangeCellPlus(partitionName, SyncPartitionUtils.transferRange(convertRanges, expr));
        }
    }

    public static List<PRangeCellPlus> toPRangeCellPlus(MaterializedView mv, Table refBaseTable,
                                                        Map<String, PCell> rangeMap,
                                                        Expr expr) {
        return rangeMap.entrySet().stream()
                .map(e -> toPRangeCellPlus(mv, refBaseTable, e.getKey(), (PRangeCell) e.getValue(), expr))
                .sorted(PRangeCellPlus::compareTo)
                .collect(Collectors.toList());
    }

    @Override
    public int compareTo(PRangeCellPlus o) {
        return cell.compareTo(o.getCell());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || !(o instanceof PRangeCellPlus)) {
            return false;
        }
        PRangeCellPlus range = (PRangeCellPlus) o;
        return this.partitionName.equals(((PRangeCellPlus) o).partitionName) &&
                this.cell.equals(range.cell);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, cell);
    }

    @Override
    public String toString() {
        return "PRangeCellPlus{" +
                "name=" + partitionName +
                "cell=" + cell +
                '}';
    }
}
