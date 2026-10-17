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

package com.starrocks.alter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SchemaChangeTypeCompatibility;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

public class AlterHelper {
    static Set<String> collectDroppedOrModifiedColumns(List<Column> oldColumns, List<Column> newColumns) {
        Set<Integer> columnUniqueIdSet = new HashSet<>();
        Set<String> modifiedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        Set<Integer> complexColumnUniqueIdSet = new HashSet<>();
        // Collect modified columns
        for (Column column : newColumns) {
            Preconditions.checkState(column.getUniqueId() >= 0);
            columnUniqueIdSet.add(column.getUniqueId());
            if (column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                modifiedColumns.add(column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX, column.getName()));
            }
        }
        
        // Collect dropped columns
        for (Column column : oldColumns) {
            if (!columnUniqueIdSet.contains(column.getUniqueId())) {
                modifiedColumns.add(column.getName());
            } else if (!column.getType().isScalarType()) {
                complexColumnUniqueIdSet.add(column.getUniqueId());
            }
        }

        // If column is struct column, we may add/drop field of column and these operation does not change
        // the uniqueId of struct column. 
        // So we need to do extra check for struct column.
        for (Integer uid : complexColumnUniqueIdSet) {
            Optional<Column> newCol = newColumns.stream().filter(c -> c.getUniqueId() == uid).findFirst();
            Optional<Column> oldCol = oldColumns.stream().filter(c -> c.getUniqueId() == uid).findFirst();
            if (!newCol.isPresent()) {
                continue;
            }
            if (!newCol.get().equals(oldCol.get())) {
                modifiedColumns.add(oldCol.get().getName());
            }
        }
        return modifiedColumns;
    }

    /**
     * Job-level variant used by the schema change jobs at finish time: locates the (origin, shadow)
     * schema pair of the BASE index from the job's shadow-index mappings and diffs only that pair.
     * Must be called before the shadow index replaces the origin one, while the old base schema is
     * still reachable on the table.
     * <p>
     * Only the base pair matters because statistics are table-level and keyed by the base-schema
     * column name, and a type change always shows up there (MODIFY has no per-rollup variant).
     * Rollup pairs are deliberately ignored: a future per-column DROP handling must not misfire on
     * {@code DROP COLUMN ... FROM rollup} (the column still exists on the table and its statistics
     * stay valid), unlike the MV-inactivation collectors which do need per-index diffs.
     *
     * @param indexMetaIdToSchema shadow index meta id -> new schema of that shadow index
     * @param indexMetaIdMap      shadow index meta id -> meta id of the origin index it replaces
     */
    static Set<String> collectStatsInvalidatedColumns(OlapTable tbl,
                                                      Map<Long, List<Column>> indexMetaIdToSchema,
                                                      Map<Long, Long> indexMetaIdMap) {
        long baseIndexMetaId = tbl.getBaseIndexMetaId();
        List<Column> originSchema = tbl.getSchemaByIndexMetaId(baseIndexMetaId);
        if (originSchema == null) {
            return Collections.emptySet();
        }
        for (Map.Entry<Long, List<Column>> entry : indexMetaIdToSchema.entrySet()) {
            Long originIndexMetaId = indexMetaIdMap.get(entry.getKey());
            if (originIndexMetaId != null && originIndexMetaId == baseIndexMetaId) {
                return collectStatsInvalidatedColumns(originSchema, entry.getValue());
            }
        }
        // the job does not touch the base index (e.g. a rollup-only alter): no type change possible
        return Collections.emptySet();
    }

    /**
     * Collect the columns whose type change invalidates their previously collected statistics.
     * <p>
     * Statistics store min/max as strings computed under the ordering semantics of the type at
     * collection time; after a type change the statistics loader casts them to the NEW type. The
     * validity condition is exactly the one of the zonemap index: the conversion must be
     * monotonically non-decreasing so that the old min/max remain valid boundaries (e.g. the
     * lexicographic extremes of a VARCHAR column are wrong bigint boundaries after VARCHAR->BIGINT),
     * so {@link SchemaChangeTypeCompatibility#canReuseZonemapIndex} is reused as the predicate.
     * Ordering-preserving widenings (INT->BIGINT, DATE->DATETIME, VARCHAR length increase, ...) keep
     * their statistics and are not reported.
     * <p>
     * A type-changed column is identified by the {@link SchemaChangeHandler#SHADOW_NAME_PREFIX} on the
     * new schema, which is applied only when the type actually changed; returned names have the prefix
     * stripped.
     */
    static Set<String> collectStatsInvalidatedColumns(List<Column> oldColumns, List<Column> newColumns) {
        Set<String> statsInvalidatedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        Map<String, Column> oldColumnsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : oldColumns) {
            oldColumnsByName.put(column.getName(), column);
        }
        for (Column column : newColumns) {
            if (!column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
                continue;
            }
            String originName = column.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX, column.getName());
            Column oldColumn = oldColumnsByName.get(originName);
            if (oldColumn == null || oldColumn.getType().equals(column.getType())) {
                continue;
            }
            if (!SchemaChangeTypeCompatibility.canReuseZonemapIndex(oldColumn.getType(), column.getType())) {
                statsInvalidatedColumns.add(originName);
            }
        }
        return statsInvalidatedColumns;
    }
}
