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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * PCellWithName is a record that associates a partition name with a PCell.
 */
public record PCellWithName(String name, PCell cell) implements Comparable<PCellWithName> {
    public static PCellWithName of(String partitionName, PCell cell) {
        return new PCellWithName(partitionName, cell);
    }

    @Override
    public String toString() {
        return "name='" + name + '\'' +
                ", cell=" + cell;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, cell);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PCellWithName)) {
            return false;
        }
        PCellWithName that = (PCellWithName) o;
        return name.equals(that.name) && cell.equals(that.cell);
    }

    @Override
    public int compareTo(PCellWithName o) {
        // compare by pcell
        int cellComparison = this.cell.compareTo(o.cell);
        if (cellComparison != 0) {
            return cellComparison;
        }
        // if pcell is equal, compare by partition name
        return this.name.compareTo(o.name);
    }

    /**
     * For base table's partition name & partition range cell, its associated pcell cache key should only remapped to one
     * pRangeCellPlus.
     */
    public static class PCellCacheKey {
        private final Table refBaseTable;
        private final PCellWithName pCellWithName;

        public PCellCacheKey(Table refBaseTable,
                             PCellWithName pCellWithName) {
            this.refBaseTable = refBaseTable;
            this.pCellWithName = pCellWithName;
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
                    Objects.equals(pCellWithName, that.pCellWithName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(refBaseTable, pCellWithName);
        }
    }

    /**
     * This is a help class to keep the original base cell and normalized cell at the same time.
     * @param basePCell original base cell
     * @param normalized normalized cell
     */
    public record PCellWithNorm(PCellWithName basePCell, PCellWithName normalized) {
        public static PCellWithNorm of(PCellWithName basePCell, PCellWithName normalized) {
            return  new PCellWithNorm(basePCell, normalized);
        }
    }

    /**
     * Convert a range map to list of partition range cell plus which is sorted by range cell.
     */
    public static List<PCellWithNorm> normalizePCellWithNames(MaterializedView mv, Table refBaseTable,
                                                              PCellSortedSet rangeMap, Expr expr) {
        if (expr == null || expr instanceof SlotRef) {
            return rangeMap.getPartitions().stream()
                    .map(p -> PCellWithNorm.of(p, p))
                    .collect(Collectors.toList());
        }
        return rangeMap.getPartitions()
                .stream()
                .map(pCell -> PCellWithNorm.of(pCell, toNormalizedCell(mv, refBaseTable, pCell, expr)))
                // sort by normalized pcell
                .sorted(new Comparator<PCellWithNorm>() {
                    @Override
                    public int compare(PCellWithNorm o1, PCellWithNorm o2) {
                        return o1.normalized.compareTo(o2.normalized);
                    }
                })
                .collect(Collectors.toList());
    }

    private static PCellWithName toNormalizedCell(MaterializedView mv, Table refBaseTable,
                                                  PCellWithName pCellWithName, Expr expr) {
        // for simple expr, no need to do conversion
        if (expr == null || expr instanceof SlotRef) {
            return pCellWithName;
        }
        if (Config.enable_mv_global_context_cache) {
            CachingMvPlanContextBuilder.MVCacheEntity mvCacheEntity = CachingMvPlanContextBuilder.getMVCache(mv);
            PCellCacheKey cacheKey = new PCellCacheKey(refBaseTable, pCellWithName);
            Object cacheValue = mvCacheEntity.get(cacheKey, () -> toNormalizedCell(pCellWithName, expr));
            if (cacheValue != null && cacheValue instanceof PCellWithName) {
                return (PCellWithName) cacheValue;
            } else {
                return toNormalizedCell(pCellWithName, expr);
            }
        } else {
            return toNormalizedCell(pCellWithName, expr);
        }
    }

    public static PCellWithName toNormalizedCell(PCellWithName pCellWithName, Expr expr) {
        if (expr == null || expr instanceof SlotRef) {
            return pCellWithName;
        }
        Range<PartitionKey> partitionKeyRanges = ((PRangeCell) pCellWithName.cell()).getRange();
        Range<PartitionKey> convertRanges = SyncPartitionUtils.convertToDatePartitionRange(partitionKeyRanges);
        return new PCellWithName(pCellWithName.name(), new PRangeCell(SyncPartitionUtils.transferRange(convertRanges, expr)));
    }
}
