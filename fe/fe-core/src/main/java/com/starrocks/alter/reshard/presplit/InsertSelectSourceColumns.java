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
import com.starrocks.catalog.Function;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.ExprSubstitutionVisitor;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
 * project the sort key and the partition columns by their source column
 * names. Non-key target columns may be expressions over the source relation and are omitted from
 * the map. A column fed by a literal ({@code '20260917' AS dt}) is recorded separately, as the
 * literal's SQL: the sampler projects the literal itself in place of a source column, for a
 * partition column and for a sort-key column alike. A caller whose sampler can evaluate expressions
 * may also admit a safe computed projection ({@code date_trunc('day', ts) AS dt}); it is recorded
 * as the expression's SQL, which the sampler evaluates over the source rows.
 *
 * <p>Outputs are paired against the statement's {@link #effectiveTargetColumns effective} target
 * columns, so an explicit target column list -- partial or reordered -- maps onto the columns it
 * names. A column the list omits simply never enters the map, which makes it a skip when it is a
 * sort-key or partition column and a no-op otherwise.
 *
 * <p>{@link #resolveUngated} returns {@code null} whenever the projection cannot be cleanly and
 * safely mapped. A projection that maps but leaves a sampled column unfed is attributed by
 * {@link #firstUnsampleable}, so the caller records why it declines.
 */
final class InsertSelectSourceColumns {

    private static final Logger LOG = LogManager.getLogger(InsertSelectSourceColumns.class);

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
         * the presence gate the caller applies through {@link #firstUnsampleable}.
         *
         * <p>It is not a licence to admit anything the analyzer would reject. Under {@code BY NAME}
         * the query's output names BECOME the target column names, so an output the target does not
         * have -- a field of a file wider than the target, or an alias naming no target column --
         * still fails analysis, and pre-split must not reshard for a load that never runs.
         */
        PER_COLUMN
    }

    /**
     * The resolved projection: {@code targetToSource} maps each directly projected target column
     * (lower-cased name) to its source column name; {@code targetToConstantSql} maps each target
     * column the SELECT feeds with a non-NULL literal, or with a column-free expression that folds
     * to one, to that constant's SQL; {@code targetToExpressionSql} maps each target column fed by
     * an admitted computed expression over source columns to the expression's SQL, with its
     * plan-time constants already folded. {@code unsupportedProjectionTargets} names outputs supplied
     * by any other expression (including NULL). Those outputs are distinct from target columns
     * omitted from the SELECT entirely. The four key sets are disjoint. {@code targetToSourceType} holds
     * the type of the source column behind each {@code targetToSource} entry, and
     * {@code targetsReadingRetypedSource} the target columns
     * {@link InsertSelectSourceColumns#targetsReadingRetypedSource} describes. A sampled generated column resolved by
     * {@link #withGeneratedColumns} lands in the constant or expression map like a computed projection, or in
     * {@code generatedColumnFailures} with the reason it cannot.
     */
    record Resolved(Map<String, String> targetToSource, Map<String, String> targetToConstantSql,
                    Map<String, String> targetToExpressionSql, Set<String> unsupportedProjectionTargets,
                    Map<String, Type> targetToSourceType, Set<String> targetsReadingRetypedSource,
                    Map<String, String> generatedColumnFailures) {

        /** A projection whose source types are not tracked and that no generated column has been resolved against. */
        Resolved(Map<String, String> targetToSource, Map<String, String> targetToConstantSql,
                 Map<String, String> targetToExpressionSql, Set<String> unsupportedProjectionTargets) {
            this(targetToSource, targetToConstantSql, targetToExpressionSql, unsupportedProjectionTargets,
                    Map.of(), Set.of(), Map.of());
        }
    }

    /**
<<<<<<< HEAD
     * Resolves the target-&gt;source column-name map for the INSERT-SELECT projection.
     *
     * @param insertStmt          the parsed INSERT statement
     * @param selectRelation      the SELECT body of the INSERT
     * @param targetTable         the INSERT target (range-distributed); the effective target
     *                            columns are derived from it by {@link #effectiveTargetColumns}
     * @param sourceTable         the single source table (OLAP, Iceberg, or the inferred
     *                            {@code TableFunctionTable} of a {@code FILES(...)} call)
     * @param normalizedSourceName fully-qualified source name (catalog/db/tbl) for qualifier matching
     * @param sourceAlias         the FROM-clause alias for the source relation, or {@code null}
     * @param sortKeyColumns      sort-key columns of the target (from MetaUtils)
     * @param partitionColumns    partition columns of the target
     * @param pairing             how closely the source schema must mirror the target's
     * @return the resolved projection, or {@code null} when the projection is ambiguous or unsafe
     */
    static Resolved resolve(
            InsertStmt insertStmt, SelectRelation selectRelation,
            OlapTable targetTable, Table sourceTable,
            TableName normalizedSourceName, String sourceAlias,
            List<Column> sortKeyColumns, List<Column> partitionColumns,
            SchemaPairing pairing) {
        Resolved resolved = resolveUngated(insertStmt, selectRelation, targetTable, sourceTable,
                normalizedSourceName, sourceAlias, pairing, /*computedProjectionContext*/ null);
        if (resolved == null) {
            return null;
        }
        if (!sortKeySampleable(sortKeyColumns, resolved) || !partitionColumnsSampleable(partitionColumns, resolved)) {
            return null;
        }
        return resolved;
=======
     * The source relation and projection rules used to resolve one INSERT-SELECT. A non-null
     * {@code computedProjectionContext} admits a safe computed expression when {@link #foldedComputedProjection},
     * folding its plan-time constants in the INSERT user's context, and
     * {@link SamplingPredicateGate#evaluatesAsTheLoad} accept it; {@code null} leaves every computed output
     * unsupported.
     */
    record ResolutionContext(Table sourceTable, TableName normalizedSourceName, String sourceAlias,
                             SchemaPairing pairing, ConnectContext computedProjectionContext) {
    }

    /** How the load hands a generated column the values of the columns its definition reads. */
    enum InputReading {
        /** Converted to each column's own type first: Broker Load ({@code Load} casts every input). */
        AS_COLUMN_TYPE,
        /** As the SELECT outputs them, converted only where a function needs it: INSERT from a table. */
        AS_SELECTED,
        /**
         * As the SELECT outputs them, after FILES column-type push-down
         * ({@code InsertAnalyzer#rewriteFileTableColumnTypes}) may have read a directly projected FILES
         * column at a target column's type: INSERT FROM FILES with no explicit FILES schema.
         */
        AS_SELECTED_AFTER_PUSH_DOWN
>>>>>>> e982e19 ([BugFix] Pre-split loads whose partition or sort-key column is a generated column (#80390))
    }

    /**
     * The projection mapping alone, or {@code null} for a projection SHAPE problem (duplicate output
     * name, by-position arity mismatch, a foreign-qualified slot, ...). A sampled column the mapping
     * leaves unfed is not a shape problem: the caller applies the presence gates itself and names the
     * offending column via {@link #firstUnsampleable}.
     *
     * @param computedProjectionContext the INSERT user's context, for a caller whose sampler can
     *                                  evaluate a computed projection: such a projection is admitted
     *                                  when {@link #foldedComputedProjection} accepts it, with its
     *                                  plan-time constants folded in this context. {@code null}
     *                                  admits none, so every computed output stays unsupported.
     */
    static Resolved resolveUngated(
            InsertStmt insertStmt, SelectRelation selectRelation,
            OlapTable targetTable, Table sourceTable,
            TableName normalizedSourceName, String sourceAlias,
            SchemaPairing pairing, ConnectContext computedProjectionContext) {
        boolean byName = insertStmt.isColumnMatchByName();
        List<SelectListItem> items = selectRelation.getSelectList().getItems();
        List<Column> targetCols = effectiveTargetColumns(insertStmt, targetTable);
        if (targetCols == null) {
            return null;
        }
        // Use VISIBLE columns: the base schema may include hidden columns that SELECT * does not output.
        List<Column> sourceCols = sourceTable instanceof OlapTable olapTable
                ? olapTable.getVisibleColumnsWithoutGeneratedColumn()
                : sourceTable.getFullVisibleSchema().stream()
                        .filter(column -> !column.isGeneratedColumn())
                        .toList();

        // Existence is checked via this map, never OlapTable.getColumn (which falls back to VirtualColumnRegistry).
        Map<String, String> sourceColumnMap = new HashMap<>();
        Map<String, Type> sourceColumnTypes = new HashMap<>();
        for (Column column : sourceCols) {
            sourceColumnMap.put(column.getName().toLowerCase(), column.getName());
            sourceColumnTypes.put(column.getName().toLowerCase(), column.getType());
        }

        boolean isStar = items.size() == 1 && items.get(0).isStar();
        Map<String, String> targetToSource = new HashMap<>();
        Map<String, String> targetToConstantSql = new HashMap<>();
        Map<String, String> targetToExpressionSql = new HashMap<>();
        Set<String> unsupportedProjectionTargets = new HashSet<>();
        Map<String, Set<String>> computedTargetSources = new HashMap<>();
        if (isStar) {
            // A visible generated source column would add an output this mapping cannot see.
            boolean hasGeneratedColumn = sourceTable instanceof OlapTable olapTable
                    ? olapTable.hasGeneratedColumn()
                    : sourceTable.getFullVisibleSchema().stream().anyMatch(Column::isGeneratedColumn);
            if (hasGeneratedColumn) {
                return null;
            }
            if (byName) {
                // EXACT: reject source columns absent from the target, and vice versa.
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
            List<Set<String>> outputSourceColumns = new ArrayList<>(items.size());
            for (SelectListItem item : items) {
                if (item.isStar()) {
                    return null;
                }
                String outputName = item.getAlias();
                String sourceName = null;
                String constantSql = null;
                String expressionSql = null;
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
                    constantSql = constantSqlOf(item.getExpr());
                    // A literal the sampler would read back as another type goes through the computed-projection branch
                    // below instead, which casts it to its own type and declines it unless that restores type and value.
                    if (constantSql != null && computedProjectionContext != null
                            && SamplingPredicateGate.literalReadBackDifferently(item.getExpr(), computedProjectionContext)) {
                        constantSql = null;
                    }
                    // A projection reading a complex column is not sampled: the safety rule keeps out casts to a complex
                    // type, but before analysis an implicit coercion such as ifnull(s, t) is not yet in the expression.
                    if (constantSql == null && computedProjectionContext != null
                            && !readsComplexColumn(item.getExpr(), sourceColumnTypes)) {
                        Expr folded = foldedComputedProjection(item.getExpr(), computedProjectionContext,
                                normalizedSourceName, sourceAlias);
                        // Analyzed against the source's own column types, as the sampler analyzes it.
                        if (folded != null && !SamplingPredicateGate.evaluatesAsTheLoad(
                                folded, sourceCols, normalizedSourceName, sourceAlias, computedProjectionContext)) {
                            folded = null;
                        }
                        if (folded != null && folded.containsSubclass(SlotRef.class)) {
                            expressionSql = SamplingPredicateGate.toSql(folded);
                        } else if (folded != null) {
                            constantSql = SamplingPredicateGate.toSql(folded);
                        }
                    }
                }
                Set<String> readSourceColumns = new HashSet<>();
                if (expressionSql != null) {
                    List<SlotRef> slots = new ArrayList<>();
                    item.getExpr().collect(SlotRef.class, slots);
                    for (SlotRef slot : slots) {
                        readSourceColumns.add(slot.getColName().toLowerCase());
                    }
                }
                outputSourceColumns.add(readSourceColumns);
                outputs.add(new String[] {outputName, sourceName, constantSql, expressionSql});
            }
            if (byName) {
                Set<String> outputNames = new HashSet<>();
                for (int i = 0; i < outputs.size(); i++) {
                    String[] output = outputs.get(i);
                    String targetName = output[0].toLowerCase();
                    if (!outputNames.add(targetName)) {
                        return null;   // duplicate output name
                    }
                    if (output[1] != null) {
                        targetToSource.put(targetName, output[1]);
                    } else if (output[2] != null) {
                        targetToConstantSql.put(targetName, output[2]);
                    } else if (output[3] != null) {
                        targetToExpressionSql.put(targetName, output[3]);
                        computedTargetSources.put(targetName, outputSourceColumns.get(i));
                    } else {
                        unsupportedProjectionTargets.add(targetName);
                    }
                }
                // EXACT: output names must be exactly the target's own columns.
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
                    String targetName = targetCols.get(i).getName().toLowerCase();
                    String[] output = outputs.get(i);
                    if (output[1] != null) {
                        targetToSource.put(targetName, output[1]);
                    } else if (output[2] != null) {
                        targetToConstantSql.put(targetName, output[2]);
                    } else if (output[3] != null) {
                        targetToExpressionSql.put(targetName, output[3]);
                        computedTargetSources.put(targetName, outputSourceColumns.get(i));
                    } else {
                        unsupportedProjectionTargets.add(targetName);
                    }
                }
            }
        }

        // The executors derive every projection from these maps at sample time (see projections),
        // so only the presence gates matter here.
        Map<String, Type> targetToSourceType = new HashMap<>();
        for (Map.Entry<String, String> mapping : targetToSource.entrySet()) {
            targetToSourceType.put(mapping.getKey(), sourceColumnTypes.get(mapping.getValue().toLowerCase()));
        }
        return new Resolved(Map.copyOf(targetToSource), Map.copyOf(targetToConstantSql),
                Map.copyOf(targetToExpressionSql), Set.copyOf(unsupportedProjectionTargets),
                Map.copyOf(targetToSourceType),
                targetsReadingRetypedSource(targetCols, targetToSource, computedTargetSources, sourceColumnTypes),
                Map.of());
    }

    /**
     * The target columns whose projection reads a source column that FILES column-type push-down reads at a
     * type other than the one the sampler reads it at. Push-down reads a source column projected directly into
     * a target column at that column's type -- the last one's when there are several -- so a direct
     * projection is affected when its source column also feeds a column of another type, and a computed one
     * when a source column it reads is projected directly at a type other than its own.
     */
    private static Set<String> targetsReadingRetypedSource(
            List<Column> targetCols, Map<String, String> targetToSource,
            Map<String, Set<String>> computedTargetSources, Map<String, Type> sourceColumnTypes) {
        Map<String, Type> targetTypes = new HashMap<>();
        for (Column column : targetCols) {
            targetTypes.put(column.getName().toLowerCase(), column.getType());
        }
        Map<String, Set<Type>> directReadTypes = new HashMap<>();
        for (Map.Entry<String, String> mapping : targetToSource.entrySet()) {
            directReadTypes.computeIfAbsent(mapping.getValue().toLowerCase(), name -> new HashSet<>())
                    .add(targetTypes.get(mapping.getKey()));
        }
        Set<String> affected = new HashSet<>();
        for (Map.Entry<String, String> mapping : targetToSource.entrySet()) {
            if (directReadTypes.get(mapping.getValue().toLowerCase()).size() > 1) {
                affected.add(mapping.getKey());
            }
        }
        for (Map.Entry<String, Set<String>> computed : computedTargetSources.entrySet()) {
            for (String sourceName : computed.getValue()) {
                Set<Type> readTypes = directReadTypes.get(sourceName);
                if (readTypes != null && (readTypes.size() > 1
                        || !readTypes.iterator().next().matchesType(sourceColumnTypes.get(sourceName)))) {
                    affected.add(computed.getKey());
                }
            }
        }
        return Set.copyOf(affected);
    }

    /**
     * Whether the sort key can be sampled: every column is backed by
     * a source column or fed by a literal or an admitted computed expression, and at least one is
     * backed by a source column or computed from one. A literal column is fine anywhere in the key --
     * every sampled tuple carries the same value there, and the cuts come from the columns that
     * vary -- but a key made only of literals is degenerate: every row has the same key, so no cut
     * can separate them.
     */
    static boolean sortKeySampleable(List<Column> sortKey, Resolved resolved) {
        // Every column must be fed; the key is still degenerate if NONE of them varies.
        return sortKey.stream().allMatch(column -> isFed(column, resolved))
                && (sortKey.isEmpty() || anySourceBacked(sortKey, resolved));
    }

    private static boolean anySourceBacked(List<Column> columns, Resolved resolved) {
        for (Column column : columns) {
            String targetName = column.getName().toLowerCase();
            if (resolved.targetToSource().containsKey(targetName)
                    || resolved.targetToExpressionSql().containsKey(targetName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the sampler can project {@code column}: from a direct source column, a supported literal, or an
     * admitted computed expression. The single definition of "fed" in this class, which {@link #sortKeySampleable}
     * and {@link #firstUnsampleable} share, so the column named in a skip reason cannot drift from the gates that
     * decide. A literal-fed column counts: it is projected as {@code CAST(<literal> AS <type>)} and never read from
     * the source, so declining such a statement as a missing column would misattribute the skip.
     */
    private static boolean isFed(Column column, Resolved resolved) {
        String targetName = column.getName().toLowerCase();
        return resolved.targetToSource().containsKey(targetName)
                || resolved.targetToConstantSql().containsKey(targetName)
                || resolved.targetToExpressionSql().containsKey(targetName);
    }

    /**
     * Every column a sample projects, in the order a skip is attributed: the base sort key, the
     * partition columns, then each visible rollup's sort key.
     */
    static List<Column> sampledColumns(List<Column> sortKeyColumns, List<Column> partitionColumns,
                                       List<SecondaryIndexSpec> secondaryIndexSpecs) {
        List<Column> columns = new ArrayList<>(sortKeyColumns);
        columns.addAll(partitionColumns);
        for (SecondaryIndexSpec spec : secondaryIndexSpecs) {
            columns.addAll(spec.sortKey());
        }
        return columns;
    }

    /** A sampled column the sampler cannot project, with the skip reason and what to tell the operator. */
    record Unsampleable(Column column, SkipReason reason, String detail) {

        /** Records the skip in the eligibility metric, the FE log, and the load's PreSplit profile. */
        void record(String tableName) {
            PreSplitMetrics.recordEligibilitySkip(reason);
            LOG.info("Sample-Based Tablet Pre-Split: table {} column \"{}\" {}; skipping pre-split",
                    tableName, column.getName(), detail);
            PreSplitProfile.recordOutcome("SKIPPED: " + reason + " (" + column.getName() + ")");
        }
    }

    /**
     * The first of {@code columns} the sampler cannot project, with the reason, or {@code null} when every
     * one is sampleable. Under {@link InputReading#AS_SELECTED_AFTER_PUSH_DOWN} a fed column whose projection
     * reads a source column that push-down retypes is unsampleable too: the load reads that column at another
     * column's type, the sampler at its own.
     */
    static Unsampleable firstUnsampleable(List<Column> columns, Resolved resolved, InputReading reading) {
        for (Column column : columns) {
            String targetName = column.getName().toLowerCase();
            if (isFed(column, resolved)) {
                if (reading == InputReading.AS_SELECTED_AFTER_PUSH_DOWN
                        && resolved.targetsReadingRetypedSource().contains(targetName)) {
                    return new Unsampleable(column, SkipReason.UNSUPPORTED_SAMPLED_PROJECTION,
                            "reads a FILES column that column-type push-down reads at another column's type");
                }
                continue;
            }
            String generatedFailure = resolved.generatedColumnFailures().get(targetName);
            if (generatedFailure != null) {
                return new Unsampleable(column, SkipReason.UNSUPPORTED_GENERATED_COLUMN, generatedFailure);
            }
            if (resolved.unsupportedProjectionTargets().contains(targetName)) {
                return new Unsampleable(column, SkipReason.UNSUPPORTED_SAMPLED_PROJECTION,
                        "has a projection the sampler cannot reproduce");
            }
            return new Unsampleable(column, SkipReason.SOURCE_MISSING_SAMPLED_COLUMN,
                    "has no source column or literal projection");
        }
        return null;
    }

    /**
     * {@code resolved} extended with the sampled generated columns ({@link #withGeneratedColumns}), or
     * {@code null} when no sample can be planned. A sampled column the sampler cannot reproduce is recorded
     * against {@code target} ({@link Unsampleable#record}); a sort key made only of literals
     * ({@link #sortKeySampleable}) is declined without attribution, since no column is missing. Only a sort key
     * can fail that way -- being fed is all a partition column needs.
     *
     * @param reading how the load hands a generated column's definition its inputs
     * @param context the INSERT user's context, in which plan-time constants are folded
     */
    static Resolved admitSampledColumns(Resolved resolved, OlapTable target, List<Column> sortKeyColumns,
                                        List<Column> partitionColumns, List<SecondaryIndexSpec> secondaryIndexSpecs,
                                        InputReading reading, ConnectContext context,
                                        TableName sourceName, String sourceAlias) {
        List<Column> sampledColumns = sampledColumns(sortKeyColumns, partitionColumns, secondaryIndexSpecs);
        Resolved extended = withGeneratedColumns(resolved, target, sampledColumns, reading, context,
                sourceName, sourceAlias);
        Unsampleable unsampleable = firstUnsampleable(sampledColumns, extended, reading);
        if (unsampleable != null) {
            unsampleable.record(target.getName());
            return null;
        }
        if (!sortKeySampleable(sortKeyColumns, extended)) {
            return null;
        }
        for (SecondaryIndexSpec spec : secondaryIndexSpecs) {
            if (!sortKeySampleable(spec.sortKey(), extended)) {
                return null;
            }
        }
        return extended;
    }

    /**
     * Resolves each generated column among {@code sampledColumns} to what the sampler evaluates in
     * its place: the column's definition with every column it reads replaced by that column's own
     * projection cast to the column's type, held to the rule {@link #foldedComputedProjection} applies
     * to a computed projection. The result lands in {@code targetToExpressionSql}, or in
     * {@code targetToConstantSql} when every column it reads is fed by a constant; a column that cannot
     * be resolved is recorded in {@code generatedColumnFailures} with the reason, for
     * {@link #firstUnsampleable}. A generated column may not read another generated column (CREATE
     * TABLE rejects it), so one level of replacement is the whole expansion. The sampler parses the SQL rendered
     * here in the load's sql_mode, so a definition holding a literal it would read back as another type is declined
     * ({@link SamplingPredicateGate#literalReadBackDifferently}).
     *
     * @param reading how the load hands the definition its inputs
     * @param context the INSERT user's context, in which plan-time constants are folded and whose sql_mode the
     *                sampler parses in
     */
    static Resolved withGeneratedColumns(Resolved resolved, OlapTable target, List<Column> sampledColumns,
                                         InputReading reading, ConnectContext context,
                                         TableName sourceName, String sourceAlias) {
        Map<String, String> constants = new HashMap<>(resolved.targetToConstantSql());
        Map<String, String> expressions = new HashMap<>(resolved.targetToExpressionSql());
        Map<String, String> failures = new HashMap<>(resolved.generatedColumnFailures());
        for (Column column : sampledColumns) {
            String targetName = column.getName().toLowerCase();
            if (!column.isGeneratedColumn() || constants.containsKey(targetName)
                    || expressions.containsKey(targetName) || failures.containsKey(targetName)) {
                continue;
            }
            Expr definition = column.getGeneratedColumnExpr(target.getIdToColumn());
            ExprSubstitutionMap inputs = new ExprSubstitutionMap();
            String failure = substituteInputs(definition, target, resolved, reading, context, inputs);
            if (failure == null) {
                Expr substituted = ExprSubstitutionVisitor.rewrite(definition, inputs);
                Expr folded = foldedComputedProjection(substituted, context, sourceName, sourceAlias);
                if (folded == null) {
                    failure = substituted.containsSubclass(SlotRef.class)
                            ? "uses an expression the sampler cannot evaluate safely"
                            : "reads only constants and does not fold to a non-NULL constant";
                } else if (SamplingPredicateGate.literalReadBackDifferently(folded, context)) {
                    failure = "has a literal the sampler would parse as another type in the load's sql_mode: a "
                            + "decimal literal as a DOUBLE (every one under DOUBLE_LITERAL, an integer wider than "
                            + "LARGEINT in any mode), or a DOUBLE as a decimal";
                } else if (folded.containsSubclass(SlotRef.class)) {
                    expressions.put(targetName, SamplingPredicateGate.toSql(folded));
                } else {
                    constants.put(targetName, SamplingPredicateGate.toSql(folded));
                }
            }
            if (failure != null) {
                failures.put(targetName, "is generated as " + SamplingPredicateGate.toSql(definition) + " and " + failure);
            }
        }
        return new Resolved(resolved.targetToSource(), Map.copyOf(constants), Map.copyOf(expressions),
                resolved.unsupportedProjectionTargets(), resolved.targetToSourceType(),
                resolved.targetsReadingRetypedSource(), Map.copyOf(failures));
    }

    /**
     * Maps every column a generated column's {@code definition} reads to the value the load hands it --
     * the column's projection converted to the column's type -- into {@code inputs}. Returns why the
     * sampler cannot reproduce one of those values, or {@code null} once {@code inputs} is complete.
     *
     * <p>INSERT does not convert an input to its column type before the definition reads it: it binds
     * the SELECT output as selected and converts it only to the parameter type of the function that
     * reads it ({@code InsertPlanner#fillGeneratedColumns}, then {@code ImplicitCastRule}). Converting to
     * the column type gives the same argument where the selected type already is the column type, up to
     * string length or decimal precision or a widened integer, and where every function reading the
     * input takes the column type itself; any other input is declined rather than sampled as a different
     * value. An input of a complex type is declined outright, and so is a definition that calls anything but a
     * built-in function ({@link #sessionDependentPart}).
     */
    private static String substituteInputs(Expr definition, OlapTable target, Resolved resolved,
                                           InputReading reading, ConnectContext context,
                                           ExprSubstitutionMap inputs) {
        List<SlotRef> references = new ArrayList<>();
        definition.collect(SlotRef.class, references);
        Expr analyzedDefinition = null;
        for (SlotRef reference : references) {
            Column input = target.getIdToColumn().get(reference.getColumnId());
            String inputName = input.getName().toLowerCase();
            Expr projection = sourceProjectionOf(inputName, resolved, context.getSessionVariable().getSqlMode());
            if (projection == null) {
                return resolved.unsupportedProjectionTargets().contains(inputName)
                        ? "reads column \"" + input.getName()
                                + "\", which the load feeds with an expression the sampler cannot reproduce"
                        : "reads column \"" + input.getName() + "\", which the load does not take from the source";
            }
            if (!input.getType().isScalarType()) {
                // The safety rule would reject the cast to a complex type anyway; declining here names the input.
                return "reads column \"" + input.getName() + "\", a " + input.getType().toSql()
                        + "; complex-typed values are not sampled";
            }
            if (reading == InputReading.AS_SELECTED_AFTER_PUSH_DOWN
                    && resolved.targetsReadingRetypedSource().contains(inputName)) {
                return "reads column \"" + input.getName() + "\", whose source column FILES column-type push-down "
                        + "reads at another column's type; the sampler reads it at its own";
            }
            Type selectedType = reading == InputReading.AS_COLUMN_TYPE && resolved.targetToSource().containsKey(inputName)
                    ? input.getType() : selectedTypeOf(inputName, projection, resolved);
            if (!convertsWithoutChange(selectedType, input.getType())) {
                if (analyzedDefinition == null) {
                    analyzedDefinition = analyzedAgainstColumnTypes(definition, target, context);
                    if (analyzedDefinition == null) {
                        return "reads column \"" + input.getName() + "\", whose conversion from "
                                + (selectedType == null ? "a computed value" : selectedType.toSql())
                                + " the sampler cannot check because the definition does not analyze on its own";
                    }
                }
                if (!readOnlyAsColumnTypedParameter(analyzedDefinition, input)) {
                    return "reads column \"" + input.getName() + "\", which the INSERT supplies as "
                            + (selectedType == null ? "a computed value" : selectedType.toSql())
                            + " and converts to " + input.getType().toSql()
                            + " only where a function needs it; the sampler cannot reproduce that conversion";
                }
            }
            inputs.put(reference, new CastExpr(new TypeDef(input.getType()), projection));
        }
        if (analyzedDefinition == null) {
            analyzedDefinition = analyzedAgainstColumnTypes(definition, target, context);
            if (analyzedDefinition == null) {
                return "does not analyze on its own, so the sampler cannot check how the load evaluates it";
            }
        }
        return sessionDependentPart(analyzedDefinition);
    }

    /**
     * Why the analyzed definition may evaluate differently in the sampler than in the load, or {@code null}: a call,
     * at any depth, bound to anything but a built-in function
     * ({@link SamplingPredicateGate#firstCallNotBoundToABuiltin}). The safety rule allows a function by name, and a
     * UDF or a SQL function can share a built-in's name with another signature. The session variables that decide
     * how the rest converts -- {@code cbo_eq_base_type} for a string compared with a number, among others -- are the
     * load's in the sample session as well ({@link SampleSessionSemantics}). The reason names the first such call.
     */
    static String sessionDependentPart(Expr analyzed) {
        FunctionCallExpr call = SamplingPredicateGate.firstCallNotBoundToABuiltin(analyzed);
        return call == null ? null : "calls " + call.getFunctionName()
                + ", which does not bind to a built-in function, so the sample session may resolve it differently";
    }

    /**
     * The expression a target column is fed by in the sampler -- its source column, or its constant or
     * computed projection parsed in the load's {@code sqlMode} as the sampler parses it -- or {@code null}.
     */
    private static Expr sourceProjectionOf(String targetName, Resolved resolved, long sqlMode) {
        String sourceName = resolved.targetToSource().get(targetName);
        if (sourceName != null) {
            return new SlotRef((TableName) null, sourceName);
        }
        String sql = resolved.targetToConstantSql().get(targetName);
        if (sql == null) {
            sql = resolved.targetToExpressionSql().get(targetName);
        }
        return sql == null ? null : SqlParser.parseSqlToExpr(sql, sqlMode);
    }

    /** The type INSERT hands a definition for {@code inputName} before any conversion, or {@code null} when unknown. */
    private static Type selectedTypeOf(String inputName, Expr projection, Resolved resolved) {
        if (resolved.targetToSource().containsKey(inputName)) {
            return resolved.targetToSourceType().get(inputName);
        }
        if (projection instanceof CastExpr cast) {
            return cast.getTargetTypeDef().getType();
        }
        return projection instanceof LiteralExpr ? projection.getType() : null;
    }

    /**
     * Whether converting a value of {@code selected} to {@code column} cannot change what a function reads:
     * the same type up to string length or decimal precision, or an integer widened to a larger integer.
     */
    private static boolean convertsWithoutChange(Type selected, Type column) {
        if (selected == null) {
            return false;
        }
        return selected.matchesType(column)
                || (selected.isIntegerType() && column.isIntegerType() && column.getTypeSize() >= selected.getTypeSize());
    }

    /**
     * The generated column's definition analyzed as INSERT analyzes it: against the target's own column
     * types, which fixes every function overload and so every parameter type an input is converted to.
     * {@code null} when it does not analyze here.
     */
    private static Expr analyzedAgainstColumnTypes(Expr definition, OlapTable target, ConnectContext context) {
        // Analyzed against an unqualified scope, so every reference is made bare first, whatever qualifier
        // (table, or schema and table) the definition was stored with.
        List<SlotRef> references = new ArrayList<>();
        definition.collect(SlotRef.class, references);
        ExprSubstitutionMap bare = new ExprSubstitutionMap();
        for (SlotRef reference : references) {
            bare.put(reference, new SlotRef((TableName) null, reference.getColumnName()));
        }
        return SamplingPredicateGate.analyzedAgainst(ExprSubstitutionVisitor.rewrite(definition, bare),
                target.getBaseSchema(), /*normalizedSourceName*/ null, /*sourceAlias*/ null, context);
    }

    /**
     * Whether every read of {@code input} in the analyzed definition is a direct argument of a function
     * whose parameter takes {@code input}'s own type, so INSERT converts the input to exactly the type the
     * sampler converts it to. A definition that is the input itself reads it with no function at all.
     */
    private static boolean readOnlyAsColumnTypedParameter(Expr node, Column input) {
        if (node instanceof SlotRef) {
            return !((SlotRef) node).getColumnName().equalsIgnoreCase(input.getName());
        }
        for (int i = 0; i < node.getChildren().size(); i++) {
            Expr child = node.getChild(i);
            if (child instanceof SlotRef slot && slot.getColumnName().equalsIgnoreCase(input.getName())) {
                Type parameter = parameterType(node, i);
                if (parameter == null || !parameter.matchesType(input.getType())) {
                    return false;
                }
            } else if (!readOnlyAsColumnTypedParameter(child, input)) {
                return false;
            }
        }
        return true;
    }

    /** The type a resolved function converts its {@code index}-th argument to, or {@code null} for any other node. */
    private static Type parameterType(Expr node, int index) {
        Function fn = node instanceof FunctionCallExpr call ? call.getFn()
                : node instanceof ArithmeticExpr arithmetic ? ExpressionAnalyzer.getArithmeticFunction(arithmetic) : null;
        if (fn == null) {
            return null;
        }
        if (index < fn.getNumArgs()) {
            return fn.getArgs()[index];
        }
        return fn.hasVarArgs() ? fn.getVarArgsType() : null;
    }

    /**
     * The SQL of a non-NULL literal projection, or {@code null} for any other expression. Only a
     * literal qualifies: its value is fixed at plan time, so the sampler reproduces it exactly, which
     * a non-deterministic call ({@code now()}) or an expression over source columns would not. NULL is
     * left out: which partition a NULL key routes to is the partition scheme's business, not a value
     * the sampler can hand the grouper, and a NULL key column is not worth the special case.
     */
    private static String constantSqlOf(Expr expr) {
        if (!(expr instanceof LiteralExpr) || expr instanceof NullLiteral) {
            return null;
        }
        return SamplingPredicateGate.toSql(expr);
    }

    /**
     * A computed projection the sampler can evaluate to the value the INSERT writes, with its
     * plan-time constants folded in the INSERT user's {@code context}, or {@code null} when it has
     * none. The sampling sub-query runs as ROOT in its own session, which carries the load's semantic
     * session variables but not its user, current database or query start time, so the projection is
     * held to the rule {@link SamplingPredicateGate} applies to the WHERE clause: after folding, only
     * source columns, literals, operators, CAST to a scalar type, and {@code SamplingPredicateGate}'s
     * row-level functions remain. That rejects a subquery, a variable, a clock, user or
     * session-time-zone function over a column, and any per-row non-deterministic call. A
     * column-free projection folds to a constant as a whole; one folding to NULL is rejected for the
     * same reason {@link #constantSqlOf} rejects a NULL literal. The caller has already proved every
     * slot names a source column.
     */
    private static Expr foldedComputedProjection(Expr expr, ConnectContext context,
                                              TableName normalizedSourceName, String sourceAlias) {
        Expr folded = SamplingPredicateGate.foldProjection(expr, context);
        if (folded == null || !SamplingPredicateGate.isDeterministicAndSafe(folded, normalizedSourceName, sourceAlias)) {
            return null;
        }
        return folded;
    }

    /**
     * The SQL each target column (sort key or partition) is projected by in a sampling sub-query: the
     * backing source column's quoted identifier, or -- for a column the SELECT feeds with a literal --
     * that literal cast to the TARGET column type. The cast makes the sampled value the one the load
     * writes: the load casts the literal to the column type before routing the row (a STRING
     * {@code '20260917'} becomes the DATE 2026-09-17), so the grouper pre-creates exactly that
     * partition and a boundary carries the key value the rows really have. A computed column is
     * projected as its expression cast to the TARGET column type, for the same reason:
     * {@code date_trunc('day', ts)} is a DATETIME, and the load writes it into a DATE column as the
     * DATE the cast yields. Returns {@code null} when any column is backed by none of these.
     */
    static List<String> projections(
            List<Column> columns, Map<String, String> targetToSource,
            Map<String, String> targetToConstantSql, Map<String, String> targetToExpressionSql) {
        List<String> projections = new ArrayList<>(columns.size());
        for (Column column : columns) {
            String targetName = column.getName().toLowerCase();
            String sourceName = targetToSource.get(targetName);
            String constantSql = targetToConstantSql.get(targetName);
            String expressionSql = targetToExpressionSql.get(targetName);
            if (sourceName != null) {
                projections.add(SqlUtils.getIdentSql(sourceName));
            } else if (constantSql != null) {
                projections.add("CAST(" + constantSql + " AS " + column.getType().toSql() + ")");
            } else if (expressionSql != null) {
                projections.add("CAST(" + expressionSql + " AS " + column.getType().toSql() + ")");
            } else {
                return null;
            }
        }
        return projections;
    }

    /**
     * The target columns the statement actually writes: the explicit target column list in the
     * order it was written, or the whole base (non-generated) schema when there is no list. Both
     * the by-position pairing and the BY NAME exact-set match are against these, so a partial or
     * reordered list maps its outputs onto the columns it names instead of onto the schema.
     *
     * <p>Only resolves names, it does not validate the list: whether the list is admissible at all
     * (no duplicate / unknown / generated name, every omitted column fillable, every non-generated
     * sort-key column of every visible index present) is {@link InsertPreSplitHook#targetColumnListIsPreSplitSafe}'s
     * single job, and it has already run for every statement that reaches here. The {@code null}
     * return below is therefore unreachable in practice and exists so that a name this method
     * cannot resolve skips pre-split rather than NPEs.
     */
    private static List<Column> effectiveTargetColumns(InsertStmt insertStmt, OlapTable targetTable) {
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
     * sampled row set. A computed key projection the sampler does evaluate must also pass
     * {@link #foldedComputedProjection}.
     */
    private static boolean referencesOnlySource(
            Expr expr, Map<String, String> sourceColumnMap,
            TableName normalizedSourceName, String sourceAlias) {
        List<Expr> rejected = new ArrayList<>();
        expr.collectAll((Predicate<Expr>) e -> isForeignReference(
                e, sourceColumnMap, normalizedSourceName, sourceAlias), rejected);
        return rejected.isEmpty();
    }

    /** Whether {@code expr} reads a source column of a complex type (STRUCT, ARRAY, MAP). */
    private static boolean readsComplexColumn(Expr expr, Map<String, Type> sourceColumnTypes) {
        List<SlotRef> slots = new ArrayList<>();
        expr.collect(SlotRef.class, slots);
        for (SlotRef slot : slots) {
            Type type = sourceColumnTypes.get(slot.getColName().toLowerCase());
            if (type != null && type.isComplexType()) {
                return true;
            }
        }
        return false;
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
