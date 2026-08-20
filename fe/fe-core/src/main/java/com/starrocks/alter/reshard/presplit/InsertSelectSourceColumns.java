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

import com.google.common.base.Predicate;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Maps each target sort-key / partition column to the source column name to sample,
 * given a parsed {@code INSERT INTO <range-dist target> SELECT ... FROM <single OLAP source>}.
 * Non-key target columns may be expressions over the source relation and are left unmapped.
 *
 * <p>Returns {@code null} whenever the projection cannot be cleanly and safely mapped
 * (caller then silently skips pre-split).
 */
final class InsertSelectSourceColumns {
    private final List<String> sortKeySourceColumnNames;
    private final List<String> partitionSourceColumnNames;

    private InsertSelectSourceColumns(List<String> sortKeySourceColumnNames,
                                      List<String> partitionSourceColumnNames) {
        this.sortKeySourceColumnNames = sortKeySourceColumnNames;
        this.partitionSourceColumnNames = partitionSourceColumnNames;
    }

    List<String> sortKeySourceColumnNames() {
        return sortKeySourceColumnNames;
    }

    List<String> partitionSourceColumnNames() {
        return partitionSourceColumnNames;
    }

    /**
     * Resolves the source-column names for each sort-key and partition column of the target table.
     *
     * @param insertStmt          the parsed INSERT statement
     * @param selectRelation      the SELECT body of the INSERT
     * @param targetTable         the INSERT target (range-distributed)
     * @param sourceTable         the single OLAP source table
     * @param normalizedSourceName fully-qualified source name (catalog/db/tbl) for qualifier matching
     * @param sourceAlias         the FROM-clause alias for the source relation, or {@code null}
     * @param sortKeyColumns      sort-key columns of the target (from MetaUtils)
     * @param partitionColumns    partition columns of the target
     * @return resolved mapping, or {@code null} when the projection is ambiguous or unsafe
     */
    static InsertSelectSourceColumns resolve(
            InsertStmt insertStmt, SelectRelation selectRelation,
            OlapTable targetTable, Table sourceTable,
            TableName normalizedSourceName, String sourceAlias,
            List<Column> sortKeyColumns, List<Column> partitionColumns) {
        boolean byName = insertStmt.isColumnMatchByName();
        List<SelectListItem> items = selectRelation.getSelectList().getItems();
        List<Column> targetCols = targetTable.getBaseSchemaWithoutGeneratedColumn();
        // Use VISIBLE columns: the base schema may include hidden columns that SELECT * does not output.
        List<Column> sourceCols = sourceTable instanceof OlapTable olapTable
                ? olapTable.getVisibleColumnsWithoutGeneratedColumn()
                : sourceTable.getFullVisibleSchema().stream()
                        .filter(column -> !column.isGeneratedColumn())
                        .toList();

        // Existence is checked via this map, never OlapTable.getColumn (which falls back to VirtualColumnRegistry).
        Map<String, String> sourceColumnMap = new HashMap<>();
        for (Column column : sourceCols) {
            sourceColumnMap.put(column.getName().toLowerCase(), column.getName());
        }

        boolean isStar = items.size() == 1 && items.get(0).isStar();
        Map<String, String> targetToSource = new HashMap<>();
        if (isStar) {
            // A visible generated source column would add an output this mapping cannot see.
            boolean hasGeneratedColumn = sourceTable instanceof OlapTable olapTable
                    ? olapTable.hasGeneratedColumn()
                    : sourceTable.getFullVisibleSchema().stream().anyMatch(Column::isGeneratedColumn);
            if (hasGeneratedColumn) {
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
                if (item.isStar()) {
                    return null;
                }
                String outputName = item.getAlias();
                String sourceName = null;
                if (item.getExpr() instanceof SlotRef slotRef) {
                    if (slotRef.getTblName() != null
                            && !matchesSource(slotRef.getTblName(), normalizedSourceName, sourceAlias)) {
                        return null;
                    }
                    sourceName = sourceColumnMap.get(slotRef.getColName().toLowerCase());
                    if (sourceName == null) {
                        return null;
                    }
                    if (outputName == null) {
                        outputName = slotRef.getColName();
                    }
                } else {
                    if (byName && outputName == null) {
                        // The target position of an expression is unknown for BY NAME without an alias.
                        return null;
                    }
                    if (!referencesOnlySource(item.getExpr(), sourceColumnMap, normalizedSourceName, sourceAlias)) {
                        return null;
                    }
                }
                outputs.add(new String[] {outputName, sourceName});
            }
            if (byName) {
                Set<String> outputNames = new HashSet<>();
                for (String[] output : outputs) {
                    String targetName = output[0].toLowerCase();
                    if (!outputNames.add(targetName)) {
                        return null;   // duplicate output name
                    }
                    if (output[1] != null) {
                        targetToSource.put(targetName, output[1]);
                    }
                }
                // Exact-set match: output names must be exactly the target non-generated columns.
                if (!outputNames.equals(targetNames(targetCols))) {
                    return null;
                }
            } else {
                if (outputs.size() != targetCols.size()) {
                    return null;
                }
                for (int i = 0; i < targetCols.size(); i++) {
                    String sourceName = outputs.get(i)[1];
                    if (sourceName != null) {
                        targetToSource.put(targetCols.get(i).getName().toLowerCase(), sourceName);
                    }
                }
            }
        }

        List<String> sortKeyNames = lookup(sortKeyColumns, targetToSource);
        if (sortKeyNames == null) {
            return null;
        }
        List<String> partitionNames = lookup(partitionColumns, targetToSource);
        if (partitionNames == null) {
            return null;
        }
        return new InsertSelectSourceColumns(sortKeyNames, partitionNames);
    }

    /**
     * Returns {@code true} when every reference inside a non-{@link SlotRef} projection resolves to
     * the source relation the caller already resolved and authorized.
     *
     * <p>The hook runs pre-analysis, so nothing the analyzer would have rejected has been rejected
     * yet when the reshard is submitted. Every SELECT item used to be a bare source column, which
     * made that safe implicitly; now that expressions are admitted, the same invariant is enforced
     * explicitly -- no subquery of any shape, and every slot names a visible source column with the
     * source's own qualifier (or none). Otherwise a projection over an unauthorized table or an
     * unknown column would reshard the target on behalf of a statement that never runs.
     *
     * <p>Function calls themselves stay allowed: the sampler never evaluates a non-key projection,
     * it only projects the mapped source columns, so the expression's own semantics cannot skew the
     * sampled row set.
     */
    private static boolean referencesOnlySource(
            Expr expr, Map<String, String> sourceColumnMap,
            TableName normalizedSourceName, String sourceAlias) {
        List<Expr> rejected = new ArrayList<>();
        expr.collectAll((Predicate<Expr>) e -> isForeignReference(
                e, sourceColumnMap, normalizedSourceName, sourceAlias), rejected);
        return rejected.isEmpty();
    }

    private static boolean isForeignReference(
            Expr expr, Map<String, String> sourceColumnMap,
            TableName normalizedSourceName, String sourceAlias) {
        // Any subquery shape (scalar, IN-subquery, EXISTS holds its Subquery as a child) reads
        // relations the hook never resolved or authorized.
        if (expr instanceof Subquery) {
            return true;
        }
        if (expr instanceof SlotRef slot) {
            if (slot.getTblName() != null
                    && !matchesSource(slot.getTblName(), normalizedSourceName, sourceAlias)) {
                return true;
            }
            // A null column name (e.g. a struct-subfield slot) is not resolvable against the
            // source schema here, so treat it as foreign rather than guessing.
            return slot.getColName() == null
                    || !sourceColumnMap.containsKey(slot.getColName().toLowerCase());
        }
        return false;
    }

    private static Set<String> targetNames(List<Column> targetCols) {
        Set<String> names = new HashSet<>();
        for (Column column : targetCols) {
            names.add(column.getName().toLowerCase());
        }
        return names;
    }

    private static List<String> lookup(List<Column> columns, Map<String, String> targetToSource) {
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
