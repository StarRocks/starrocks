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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.planner.JDBCScanNode;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorToJDBCSQLVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Builds a dialect-aware SQL string for JDBC join pushdown.
 *
 * <p>Each output column is aliased by its ColumnRefOperator ID (c{id}) to avoid
 * column-name conflicts when multiple tables share the same column name (e.g.,
 * both t0 and t1 have an {@code id} column). The outer wrapping query (built by
 * PlanFragmentBuilder) references columns through these aliases.
 *
 * <p>Example output:
 * <pre>
 *   SELECT t0.`a` AS c1, t0.`b` AS c2, t1.`c` AS c3
 *   FROM `tbl0` t0
 *   INNER JOIN `tbl1` t1 ON t0.`id` = t1.`id`
 *   WHERE t0.`a` > 10
 * </pre>
 */
public class JDBCJoinPushDownSQLBuilder {

    /**
     * Represents a JDBC table with its alias in the pushed-down query.
     */
    public static class TableEntry {
        public final JDBCTable table;
        public final String alias;
        /**
         * Raw base columns the scan reads from the underlying table. Used to populate the SELECT
         * list and per-table predicate references inside the per-scan subquery.
         */
        public final Map<ColumnRefOperator, Column> columnRefMap;
        /**
         * ColumnRefOperators visible to upper operators after the scan — i.e., the keys of the
         * scan's projection if it has one, otherwise {@code columnRefMap.keySet()}.
         */
        public final Set<ColumnRefOperator> visibleOutputCols;
        /** -1 means no limit (same as {@link Operator#DEFAULT_LIMIT}). */
        public final long limit;
        public final List<ScalarOperator> perTablePredicates;

        public TableEntry(JDBCTable table, String alias, LogicalJDBCScanOperator scan) {
            this.table = table;
            this.alias = alias;
            this.columnRefMap = scan.getColRefToColumnMetaMap();
            this.visibleOutputCols = scan.getProjection() == null
                    ? columnRefMap.keySet()
                    : scan.getProjection().getColumnRefMap().keySet();
            this.limit = scan.getLimit();
            this.perTablePredicates = new ArrayList<>(Utils.extractConjuncts(scan.getPredicate()));
        }

        public boolean hasLimit() {
            return limit != Operator.DEFAULT_LIMIT;
        }

        /** True when this scan's predicate was folded into {@link #perTablePredicates}. */
        public boolean hasPredicate() {
            return !perTablePredicates.isEmpty();
        }
    }

    private final String identifierQuote;
    private final List<TableEntry> tableEntries;
    // Maps each ColumnRefOperator to its qualified name: alias.quotedCol
    private final Map<ColumnRefOperator, String> columnRefToQualifiedName;

    public JDBCJoinPushDownSQLBuilder(String identifierQuote, List<TableEntry> tableEntries,
                                       Map<ColumnRefOperator, String> columnRefToQualifiedName) {
        this.identifierQuote = identifierQuote;
        this.tableEntries = tableEntries;
        this.columnRefToQualifiedName = columnRefToQualifiedName;
    }

    /**
     * Build the complete pushdown SQL.
     *
     * @param outputColumns  columns to select (in order)
     * @param joinPredicates predicates for ON clauses (between different tables)
     * @param filterPredicates predicates for WHERE clause (single-table filters)
     * @return the complete SQL string
     */
    public String build(List<ColumnRefOperator> outputColumns,
                        List<ScalarOperator> joinPredicates,
                        List<ScalarOperator> filterPredicates) {
        StringBuilder sql = new StringBuilder();

        // SELECT clause: use ColumnRefOperator.getId() as the alias suffix so each output
        // column has a globally unique name. This lets the outer wrapping query (built by
        // PlanFragmentBuilder) reference columns by ID without depending on any ordering
        // between the two sides.
        sql.append("SELECT ");
        sql.append(outputColumns.stream()
                .map(col -> columnRefToQualifiedName.get(col) + " AS " + outputColumnAlias(col))
                .collect(Collectors.joining(", ")));

        // FROM clause with first table
        sql.append(" FROM ");
        TableEntry first = tableEntries.get(0);
        sql.append(buildTableExpression(first));

        // JOIN clauses for remaining tables.
        // unassignedJoinPreds tracks predicates not yet attached to an ON clause; each
        // iteration may consume some and leave the rest for later JOINs or the final WHERE.
        List<ScalarOperator> unassignedJoinPreds = joinPredicates;
        for (int i = 1; i < tableEntries.size(); i++) {
            TableEntry entry = tableEntries.get(i);

            // Collect applicable ON predicates first, then decide JOIN type
            List<String> onClauses = new ArrayList<>();
            List<ScalarOperator> remaining = new ArrayList<>();
            for (ScalarOperator pred : unassignedJoinPreds) {
                if (involvesTable(pred, entry, tableEntries.subList(0, i))) {
                    onClauses.add(toSQL(pred));
                } else {
                    remaining.add(pred);
                }
            }
            unassignedJoinPreds = remaining;

            if (!onClauses.isEmpty()) {
                sql.append(" INNER JOIN ").append(buildTableExpression(entry));
                sql.append(" ON ").append(Joiner.on(" AND ").join(onClauses));
            } else {
                sql.append(" CROSS JOIN ").append(buildTableExpression(entry));
            }
        }

        // Remaining join predicates (e.g., couldn't be scoped to any ON) go to WHERE
        String whereSql = Stream.concat(unassignedJoinPreds.stream(), filterPredicates.stream())
                .map(this::toSQL)
                .collect(Collectors.joining(" AND "));
        if (!whereSql.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(whereSql);
        }

        return sql.toString();
    }

    /**
     * Builds a single table reference for FROM/JOIN: plain qualified name, or a derived subquery
     * when the scan has a LIMIT and/or a predicate (filter and limit are applied inside the subquery).
     */
    private String buildTableExpression(TableEntry entry) {
        Preconditions.checkState(!entry.table.isQueryTable(), "queryTable cannot be pushed down");
        String tableRef = entry.table.isDerivedTable()
                ? entry.table.getCatalogTableName()
                : JDBCScanNode.wrapWithIdentifier(entry.table.getCatalogTableName(), identifierQuote);
        if (!entry.hasLimit() && !entry.hasPredicate()) {
            if (entry.table.isDerivedTable()) {
                // Direct "tableRef + alias" would produce "(<inner>) sr_merged t0" — double alias.
                // Peel off the inner sr_merged via getPushDownQuery() and apply our own alias.
                return "(" + entry.table.getPushDownQuery() + ") " + entry.alias;
            }
            return tableRef + " " + entry.alias;
        }
        // PruneScanColumnRule guarantees the scan's colRefToColumnMetaMap is non-empty
        // (it adds the smallest column back when no column is required, e.g. COUNT(*))
        Preconditions.checkState(!entry.columnRefMap.isEmpty(),
                "TableEntry %s has empty columnRefMap", entry.alias);
        StringBuilder sb = new StringBuilder();
        Map<ColumnRefOperator, String> rawNames = buildRawColumnNameMap(entry);
        sb.append("(SELECT ");
        sb.append(entry.columnRefMap.keySet().stream()
                .map(rawNames::get)
                .collect(Collectors.joining(", ")));
        sb.append(" FROM ").append(tableRef);
        if (!entry.perTablePredicates.isEmpty()) {
            sb.append(" WHERE ");
            sb.append(entry.perTablePredicates.stream()
                    .map(pred -> toSQL(pred, rawNames))
                    .collect(Collectors.joining(" AND ")));
        }
        if (entry.hasLimit()) {
            sb.append(" LIMIT ").append(entry.limit);
        }
        sb.append(") ").append(entry.alias);
        return sb.toString();
    }

    private Map<ColumnRefOperator, String> buildRawColumnNameMap(TableEntry entry) {
        Map<ColumnRefOperator, String> rawNames = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, Column> e : entry.columnRefMap.entrySet()) {
            rawNames.put(e.getKey(), identifierQuote + e.getValue().getName() + identifierQuote);
        }
        return rawNames;
    }

    /**
     * Decide whether a join predicate can be attached to the ON clause of the JOIN that
     * is about to bring {@code targetTable} in. An ON clause may only reference columns
     * whose tables are already in scope at that point, i.e. {@code targetTable} plus all
     * {@code previousTables}. A predicate that also references a not-yet-joined table
     * must be deferred (it will either fit a later JOIN, or end up in the final WHERE).
     *
     * Returns true only when:
     *   1. the predicate references {@code targetTable} (otherwise it's unrelated here),
     *   2. the predicate references at least one {@code previousTables} entry (otherwise
     *      it's a single-table filter, not a join predicate), and
     *   3. every column referenced by the predicate belongs to target ∪ previous (no
     *      forward references to tables yet to be joined — those would be out-of-scope
     *      for this ON clause and produce invalid SQL).
     */
    private boolean involvesTable(ScalarOperator pred, TableEntry targetTable,
                                   List<TableEntry> previousTables) {
        List<ColumnRefOperator> colRefs = new ArrayList<>();
        pred.getColumnRefs(colRefs);

        boolean involvesTarget = false;
        boolean involvesPrevious = false;
        for (ColumnRefOperator ref : colRefs) {
            if (targetTable.visibleOutputCols.contains(ref)) {
                involvesTarget = true;
                continue;
            }
            boolean foundInPrevious = false;
            for (TableEntry prev : previousTables) {
                if (prev.visibleOutputCols.contains(ref)) {
                    foundInPrevious = true;
                    involvesPrevious = true;
                    break;
                }
            }
            if (!foundInPrevious) {
                // Column belongs to a table that is not yet joined (future table) —
                // defer this predicate to avoid producing an out-of-scope reference.
                return false;
            }
        }
        return involvesTarget && involvesPrevious;
    }

    /**
     * Convert a ScalarOperator to SQL string using the column ref mapping.
     */
    public String toSQL(ScalarOperator op) {
        return toSQL(op, columnRefToQualifiedName);
    }

    /**
     * Convert using an explicit column ref → SQL name map (e.g. unqualified names inside a subquery).
     */
    public String toSQL(ScalarOperator op, Map<ColumnRefOperator, String> nameMap) {
        return op.accept(ScalarOperatorToJDBCSQLVisitor.forDialect(nameMap, dialect()), null);
    }

    private JDBCTable.ProtocolType dialect() {
        return tableEntries.isEmpty()
                ? JDBCTable.ProtocolType.UNKNOWN
                : tableEntries.get(0).table.getProtocolType();
    }

    /**
     * Returns the output column alias used inside the pushdown SELECT list. The alias is
     * derived from the column ref ID (unique within the query), so the mapping is stable
     * regardless of iteration order. All sites that produce or consume these aliases
     * (SQL builder, PlanFragmentBuilder, JDBCScanNode) call this method to stay in sync.
     */
    public static String outputColumnAlias(int columnRefId) {
        return "c" + columnRefId;
    }

    /** Convenience overload accepting a ColumnRefOperator directly. */
    public static String outputColumnAlias(ColumnRefOperator col) {
        return outputColumnAlias(col.getId());
    }
}
