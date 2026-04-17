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

package com.starrocks.mv.pct;

import com.starrocks.sql.common.PCellSortedSet;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pairs a base table's partition cells (resolved into MV partition space) with
 * an optional source name mapping that traces each MV partition name back to
 * the original base table partition names.
 * <p>
 * For OLAP tables the source mapping is empty — their partition names are
 * directly usable. For external tables (Hive, Iceberg, etc.) the mapping
 * records which original partition names (e.g. {@code par_date=2024-01-01})
 * were resolved into each normalized MV partition name (e.g. {@code p20240101}).
 */
public record BaseToMVPartitionMapping(
        // Base table partitions resolved into MV partition granularity.
        // Each entry's name is a generated MV partition name (e.g. "p20240101"),
        // and its cell is the corresponding range/list value at MV granularity.
        // The original base table partition names and their finer-grained cell
        // values are NOT preserved here — only the MV-level projection.
        PCellSortedSet cells,
        // Reverse lookup: MV partition name → the original base table partition
        // names that were resolved into it.
        // e.g. "p20240101" → {"par_date=2024-01-01", "par_date=2024-01-02"}
        //
        // Empty for OLAP tables (their partition names are directly usable).
        // Populated for external tables (Hive, Iceberg, etc.) where the original
        // partition names differ from the generated MV partition names.
        // Used by MvTaskRunContext.getExternalTableRealPartitionName() to tell
        // connectors which partitions to refresh.
        Map<String, Set<String>> sourceNameMapping
) {
    public static BaseToMVPartitionMapping of(PCellSortedSet cells) {
        return new BaseToMVPartitionMapping(cells, Map.of());
    }

    public static BaseToMVPartitionMapping of(PCellSortedSet cells, Map<String, Set<String>> mapping) {
        return new BaseToMVPartitionMapping(cells, mapping);
    }

    /**
     * Get the original base partition names that were resolved into the given MV partition name.
     * Returns empty set if no mapping exists (OLAP tables or unknown partition name).
     */
    public Set<String> getSourceNames(String mvPartitionName) {
        return sourceNameMapping.getOrDefault(mvPartitionName, Set.of());
    }

    /**
     * Extract a plain {@code Map<K, PCellSortedSet>} from a map of mappings.
     * Used when callers only need the cells (e.g. {@code generateBaseRefMap}).
     */
    public static <K> Map<K, PCellSortedSet> extractCells(Map<K, BaseToMVPartitionMapping> map) {
        return map.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().cells()));
    }
}
