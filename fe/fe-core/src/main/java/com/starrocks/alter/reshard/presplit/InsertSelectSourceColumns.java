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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.SlotRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves the full target-&gt;source column-name map (lower-cased target name -&gt; source column
 * name, total over the target's non-generated base columns) for a parsed
 * {@code INSERT INTO <range-dist target> SELECT ... FROM <single OLAP source>}. The sampler uses
 * the map to project any index's sort key (base or rollup) and the partition columns by their
 * source-table column names.
 *
 * <p>Returns {@code null} whenever the projection cannot be cleanly and safely mapped
 * (caller then silently skips pre-split).
 */
final class InsertSelectSourceColumns {

    private InsertSelectSourceColumns() {
    }

    /**
     * Resolves the target-&gt;source column-name map for the INSERT-SELECT projection.
     *
     * @param insertStmt          the parsed INSERT statement
     * @param selectRelation      the SELECT body of the INSERT
     * @param targetTable         the INSERT target (range-distributed)
     * @param sourceTable         the single OLAP source table
     * @param normalizedSourceName fully-qualified source name (catalog/db/tbl) for qualifier matching
     * @param sourceAlias         the FROM-clause alias for the source relation, or {@code null}
     * @param sortKeyColumns      sort-key columns of the target (from MetaUtils)
     * @param partitionColumns    partition columns of the target
     * @return the target-&gt;source column-name map, or {@code null} when the projection is
     *         ambiguous or unsafe
     */
    static Map<String, String> resolve(
            InsertStmt insertStmt, SelectRelation selectRelation,
            OlapTable targetTable, OlapTable sourceTable,
            TableName normalizedSourceName, String sourceAlias,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        boolean byName = insertStmt.isColumnMatchByName();
        List<SelectListItem> items = selectRelation.getSelectList().getItems();
        List<Column> targetCols = targetTable.getBaseSchemaWithoutGeneratedColumn();
        // Use VISIBLE columns: the base schema may include hidden columns that SELECT * does not output.
        List<Column> sourceCols = sourceTable.getVisibleColumnsWithoutGeneratedColumn();

        // Existence is checked via this map, never OlapTable.getColumn (which falls back to VirtualColumnRegistry).
        Map<String, String> sourceColumnMap = new HashMap<>();
        for (Column column : sourceCols) {
            sourceColumnMap.put(column.getName().toLowerCase(), column.getName());
        }

        boolean isStar = items.size() == 1 && items.get(0).isStar();
        Map<String, String> targetToSource = new HashMap<>();
        if (isStar) {
            // A visible generated source column would add an output this mapping cannot see.
            if (sourceTable.hasGeneratedColumn()) {
                return null;
            }
            if (byName) {
                // Exact-set match: reject source columns absent from the target, and vice versa.
                if (!sourceColumnMap.keySet().equals(targetNames(targetCols))) {
                    return null;
                }
                for (Column targetCol : targetCols) {
                    String targetName = targetCol.getName().toLowerCase();
                    targetToSource.put(targetName, sourceColumnMap.get(targetName));
                }
            } else {
                if (sourceCols.size() != targetCols.size()) {
                    return null;
                }
                for (int i = 0; i < targetCols.size(); i++) {
                    if (!targetCols.get(i).getName().equalsIgnoreCase(sourceCols.get(i).getName())) {
                        return null;
                    }
                    targetToSource.put(targetCols.get(i).getName().toLowerCase(), sourceCols.get(i).getName());
                }
            }
        } else {
            List<String[]> outputs = new ArrayList<>(items.size());
            for (SelectListItem item : items) {
                if (item.isStar() || !(item.getExpr() instanceof SlotRef slotRef)) {
                    return null;
                }
                if (slotRef.getTblName() != null
                        && !matchesSource(slotRef.getTblName(), normalizedSourceName, sourceAlias)) {
                    return null;
                }
                String sourceName = sourceColumnMap.get(slotRef.getColName().toLowerCase());
                if (sourceName == null) {
                    return null;
                }
                String outputName = item.getAlias() != null ? item.getAlias() : slotRef.getColName();
                outputs.add(new String[] {outputName, sourceName});
            }
            if (byName) {
                for (String[] output : outputs) {
                    if (targetToSource.put(output[0].toLowerCase(), output[1]) != null) {
                        return null;   // duplicate output name
                    }
                }
                // Exact-set match: output names must be exactly the target non-generated columns.
                if (!targetToSource.keySet().equals(targetNames(targetCols))) {
                    return null;
                }
            } else {
                if (outputs.size() != targetCols.size()) {
                    return null;
                }
                for (int i = 0; i < targetCols.size(); i++) {
                    targetToSource.put(targetCols.get(i).getName().toLowerCase(), outputs.get(i)[1]);
                }
            }
        }

        // A sort-key or partition column with no source mapping cannot be sampled -> skip pre-split.
        // The executor derives both projections from the map at sample time (see mapToSource), so
        // only the presence gate matters here.
        if (lookup(sortKeyColumns, targetToSource) == null || lookup(partitionColumns, targetToSource) == null) {
            return null;
        }
        return Map.copyOf(targetToSource);
    }

    private static Set<String> targetNames(List<Column> targetCols) {
        Set<String> names = new HashSet<>();
        for (Column column : targetCols) {
            names.add(column.getName().toLowerCase());
        }
        return names;
    }

    /**
     * Maps each target column to its source-table column name via {@code targetToSource}
     * (keyed by lower-cased target name), or returns {@code null} when ANY column is absent from
     * the map. The shared primitive for every "are these columns mappable to source names?" gate
     * and for the executor's sample-time remap; package-private so those same-package callers
     * share one implementation of the map-key convention.
     */
    static List<String> lookup(List<Column> columns, Map<String, String> targetToSource) {
        List<String> names = new ArrayList<>(columns.size());
        for (Column column : columns) {
            String sourceName = targetToSource.get(column.getName().toLowerCase());
            if (sourceName == null) {
                return null;
            }
            names.add(sourceName);
        }
        return names;
    }

    /**
     * Returns {@code true} when the slot's table qualifier refers to the source relation.
     *
     * <p>An alias in scope shadows the table name (only the alias is a valid qualifier);
     * otherwise the provided catalog/db parts must match the normalized source name.
     * Package-private so that Task-2 ({@code SamplingPredicateGate}) can reuse it.
     *
     * @param slotTable           the qualifier from the slot reference
     * @param normalizedSourceName the fully-qualified source name
     * @param sourceAlias         the FROM-clause alias, or {@code null}
     */
    static boolean matchesSource(TableName slotTable, TableName normalizedSourceName, String sourceAlias) {
        String slotTbl = slotTable.getTbl();
        if (slotTbl == null) {
            // Defensive guard: an unqualified slot (e.g. struct-subfield SlotRef) has no table to mismatch.
            return true;
        }
        String slotDb = slotTable.getDb();
        String slotCatalog = slotTable.getCatalog();
        if (sourceAlias != null) {
            return slotDb == null && slotCatalog == null && slotTbl.equalsIgnoreCase(sourceAlias);
        }
        if (!slotTbl.equalsIgnoreCase(normalizedSourceName.getTbl())) {
            return false;
        }
        if (slotDb != null && !slotDb.equalsIgnoreCase(normalizedSourceName.getDb())) {
            return false;
        }
        return slotCatalog == null || slotCatalog.equalsIgnoreCase(normalizedSourceName.getCatalog());
    }
}
