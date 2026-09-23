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
 * Resolves the target-&gt;source column-name map (lower-cased target name -&gt; source column
 * name) for directly mapped outputs of a parsed
 * {@code INSERT INTO <range-dist target> SELECT ... FROM <single source relation>}, where the
 * source is one OLAP / Iceberg table or one {@code FILES(...)} call. The sampler uses the map to
 * project any index's sort key (base or rollup) and the partition columns by their source column
 * names. Non-key target columns may be expressions over the source relation and are omitted from
 * the map.
 *
 * <p>Outputs are paired against the statement's {@link #effectiveTargetColumns effective} target
 * columns, so an explicit target column list -- partial or reordered -- maps onto the columns it
 * names. A column the list omits simply never enters the map, which makes it a skip when it is a
 * sort-key or partition column and a no-op otherwise.
 *
 * <p>Returns {@code null} whenever the projection cannot be cleanly and safely mapped
 * (caller then silently skips pre-split).
 */
final class InsertSelectSourceColumns {

    private InsertSelectSourceColumns() {
    }

    /**
     * How closely the source's own column set has to mirror the target's for a {@code SELECT *}
     * (or a {@code BY NAME} projection) to be mappable.
     */
    enum SchemaPairing {
        /**
         * The two schemas must be images of each other: same column set under {@code BY NAME},
         * same name at every ordinal under by-position. Right for a table source, where a column
         * on one side and not the other is a statement {@code InsertAnalyzer} rejects outright, so
         * pre-split would be resharding for a load that never runs.
         */
        EXACT,
        /**
         * Pair up whatever columns correspond and leave the target columns the source does not
         * supply alone. Right for a {@code FILES(...)} source, where a file NARROWER than the
         * target is ordinary: the columns it omits are defaulted, which says nothing about the
         * columns that ARE paired, and an unpaired sort-key or partition column is still caught by
         * this method's final presence gate.
         *
         * <p>It is not a licence to admit anything the analyzer would reject. Under {@code BY NAME}
         * the query's output names BECOME the target column names, so an output the target does not
         * have -- a field of a file wider than the target, or an alias naming no target column --
         * still fails analysis, and pre-split must not reshard for a load that never runs.
         */
        PER_COLUMN
    }

    /**
     * Resolves the target-&gt;source column-name map for the INSERT-SELECT projection.
     *
     * @param insertStmt          the parsed INSERT statement
     * @param selectRelation      the SELECT body of the INSERT
     * @param targetCols          the target columns the load writes, in INSERT (by-position) order:
     *                            the explicit target column list when the statement carries one,
     *                            otherwise the target's non-generated base schema
     * @param sourceTable         the single source table (OLAP, Iceberg, or the inferred
     *                            {@code TableFunctionTable} of a {@code FILES(...)} call)
     * @param normalizedSourceName fully-qualified source name (catalog/db/tbl) for qualifier matching
     * @param sourceAlias         the FROM-clause alias for the source relation, or {@code null}
     * @param sortKeyColumns      sort-key columns of the target (from MetaUtils)
     * @param partitionColumns    partition columns of the target
     * @param pairing             how closely the source schema must mirror the target's
     * @return the target-&gt;source column-name map, or {@code null} when the projection is
     *         ambiguous or unsafe
     */
    static Map<String, String> resolve(
            InsertStmt insertStmt, SelectRelation selectRelation,
            List<Column> targetCols, Table sourceTable,
            TableName normalizedSourceName, String sourceAlias,
            List<Column> sortKeyColumns, List<Column> partitionColumns,
            SchemaPairing pairing) {
        boolean byName = insertStmt.isColumnMatchByName();
        List<SelectListItem> items = selectRelation.getSelectList().getItems();
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
                // EXACT: reject source columns absent from the effective target, and vice versa.
                if (pairing == SchemaPairing.EXACT && !sourceColumnMap.keySet().equals(targetNames(targetCols))) {
                    return null;
                }
                // PER_COLUMN still may not admit a file WIDER than the target. Under BY NAME
                // InsertAnalyzer sets the target column names to the query's OUTPUT names, and a
                // `SELECT *` over FILES outputs every file column, so a field the target does not
                // have fails analysis with "Unknown column '<f>' in '<t>'". Only
                // enable_push_down_schema trims `*` down to the target names first
                // (expandStarColumns); that flag is parsed in analyzeProperties, which runs AFTER
                // this hook, so it always reads false here and the default mode is the one to
                // assume. A file NARROWER than the target stays fine -- the columns it does not
                // supply are simply defaulted.
                if (pairing == SchemaPairing.PER_COLUMN
                        && !targetNames(targetCols).containsAll(sourceColumnMap.keySet())) {
                    return null;
                }
                for (Column targetCol : targetCols) {
                    String targetName = targetCol.getName().toLowerCase();
                    String sourceName = sourceColumnMap.get(targetName);
                    // Under EXACT the set check above already proved every target name is present;
                    // under PER_COLUMN a target column the source never supplies stays unmapped.
                    if (sourceName != null) {
                        targetToSource.put(targetName, sourceName);
                    }
                }
            } else {
                if (sourceCols.size() != targetCols.size()) {
                    return null;
                }
                for (int i = 0; i < targetCols.size(); i++) {
                    // By position the load writes source column i into target column i whatever the
                    // two are called, so the ordinal IS the mapping. EXACT additionally demands the
                    // names agree; PER_COLUMN records the pairing the ordinals already establish.
                    if (pairing == SchemaPairing.EXACT
                            && !targetCols.get(i).getName().equalsIgnoreCase(sourceCols.get(i).getName())) {
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
                // EXACT: output names must be exactly the effective target columns.
                if (pairing == SchemaPairing.EXACT && !outputNames.equals(targetNames(targetCols))) {
                    return null;
                }
                // PER_COLUMN accepts a PARTIAL output set -- a target column no output names is
                // simply defaulted -- but every output name must still BE a target column. BY NAME turns
                // each output name into a target column name, so one that names nothing on the
                // target fails analysis the same way a wide `SELECT *` does.
                if (pairing == SchemaPairing.PER_COLUMN && !targetNames(targetCols).containsAll(outputNames)) {
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

        // A sort-key or partition column with no source mapping cannot be sampled -> skip pre-split.
        // The executor derives both projections from the map at sample time (see mapToSource), so
        // only the presence gate matters here.
        if (lookup(sortKeyColumns, targetToSource) == null || lookup(partitionColumns, targetToSource) == null) {
            return null;
        }
        return Map.copyOf(targetToSource);
    }

    /**
     * The target columns the statement actually writes: the explicit target column list in the
     * order it was written, or the whole base (non-generated) schema when there is no list. Both
     * the by-position pairing and the BY NAME exact-set match are against these, so a partial or
     * reordered list maps its outputs onto the columns it names instead of onto the schema.
     *
     * <p>Only resolves names, it does not validate the list: whether the list is admissible at all
     * (no duplicate / unknown / generated name, every omitted column fillable, every visible
     * index's sort key present) is {@link InsertPreSplitHook#targetColumnListIsPreSplitSafe}'s
     * single job, and it has already run for every statement that reaches here. The {@code null}
     * return below is therefore unreachable in practice and exists so that a name this method
     * cannot resolve skips pre-split rather than NPEs.
     */
    static List<Column> effectiveTargetColumns(InsertStmt insertStmt, OlapTable targetTable) {
        List<Column> baseCols = targetTable.getBaseSchemaWithoutGeneratedColumn();
        List<String> targetColumnNames = insertStmt.getTargetColumnNames();
        if (targetColumnNames == null || targetColumnNames.isEmpty()) {
            return baseCols;
        }
        Map<String, Column> baseByName = new HashMap<>();
        for (Column column : baseCols) {
            baseByName.put(column.getName().toLowerCase(), column);
        }
        List<Column> effective = new ArrayList<>(targetColumnNames.size());
        for (String name : targetColumnNames) {
            Column column = baseByName.get(name.toLowerCase());
            if (column == null) {
                return null;
            }
            effective.add(column);
        }
        return effective;
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
