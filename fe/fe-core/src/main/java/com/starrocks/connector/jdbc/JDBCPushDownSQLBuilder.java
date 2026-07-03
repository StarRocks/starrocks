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

package com.starrocks.connector.jdbc;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.planner.JDBCScanNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Builds dialect-aware SQL strings for the JDBC pushdown rules. All optimizer-baked
 * pushdown SQL (join merge and aggregate folding) is assembled here, on top of the single
 * {@link ScalarOperatorToJDBCSQLVisitor} rendering pipeline.
 *
 * <p>Join merge (via {@link #buildJoinQuery}): the builder is handed the group's
 * {@link LogicalJDBCScanOperator}s directly and owns everything downstream — it assigns each scan a positional alias
 * ({@link #tableAlias(int)}: sr_t0, sr_t1, ...) and derives the qualified-name map used by WHERE
 * rendering from each scan's {@code colRefToColumnMetaMap}. Each output column is aliased by its
 * ColumnRefOperator ID (sr_c{id}) to avoid column-name conflicts when multiple tables share the same
 * column name (e.g., both sr_t0 and sr_t1 have an {@code id} column). The outer wrapping query (composed
 * by the BE from JDBCScanNode's column list) references columns through these aliases.
 *
 * <pre>
 *   SELECT sr_t0.`a` AS sr_c1, sr_t0.`b` AS sr_c2, sr_t1.`c` AS sr_c3
 *   FROM `tbl0` sr_t0, `tbl1` sr_t1
 *   WHERE (sr_t0.`id` = sr_t1.`id`) AND (sr_t0.`a` > 10)
 * </pre>
 *
 * <p>Aggregate folding ({@link #buildScalarSelectQuery}): wraps a single scan — base table or
 * a previously merged derived table — in a remote GROUP BY query.
 *
 * <pre>
 *   SELECT `b` AS `b`, sum(`a`) AS `jdbc_agg_0` FROM `tbl0` WHERE (`c` &gt; 10) GROUP BY `b`
 * </pre>
 */
public class JDBCPushDownSQLBuilder {
    private static final String SELECT_QUERY_TEMPLATE =
            "SELECT$topClause $columns FROM $table$whereClause$limitClause";
    private static final String ORACLE_LIMIT_QUERY_TEMPLATE =
            "SELECT * FROM ($query) WHERE ROWNUM <= $limit";
    private static final String JOIN_QUERY_TEMPLATE =
            "SELECT $selectList FROM $fromList$whereClause";
    private static final String AGGREGATE_QUERY_TEMPLATE =
            "SELECT $selectList FROM $tableExpr$whereClause$groupByClause$havingClause";

    /** Alias for the inner LIMIT subquery wrapped around a scan before aggregate folding. */
    private static final String LIMITED_SUBQUERY_ALIAS = "sr_limited";

    // Shared engine, rendered concurrently from the build*Query() helpers. VelocityEngine is
    // thread-safe for evaluate() once initialized (the static block below runs once at class
    // load), so concurrent renders need no extra synchronization.
    private static final VelocityEngine DEFAULT_VELOCITY_ENGINE;

    static {
        DEFAULT_VELOCITY_ENGINE = new VelocityEngine();
        // close velocity log
        DEFAULT_VELOCITY_ENGINE.setProperty(VelocityEngine.RUNTIME_LOG_REFERENCE_LOG_INVALID, false);
    }

    /**
     * Build a plain JDBC SELECT from already-rendered filter SQL. This is used by JDBCScanNode's
     * explain/query path, where predicates have already been converted to strings.
     */
    public static String buildSelectQuery(String jdbcUri, List<String> columns, String tableName,
                                          List<String> filters, long limit) {
        return buildSelectQuery(jdbcUri, columns, tableName, renderFilterSql(filters), limit);
    }

    /**
     * Render a base-table JDBC scan's pushed-down predicates into individual dialect-aware SQL
     * predicate strings for {@link JDBCScanNode}. The predicates were already vetted by
     * {@link com.starrocks.sql.optimizer.rewrite.CanPushDownPredicateVisitor} (in
     * ExternalTablePredicateExtractor), the matched gate for this same renderer, so every one
     * renders cleanly. Rendering from ScalarOperator keeps the scan path on the one
     * {@link ScalarOperatorToJDBCSQLVisitor} pipeline shared with join/aggregate pushdown -- and so
     * dialect-aware -- instead of going through the AST. Each predicate keeps the visitor's outer
     * parentheses; the FE explain path and the BE both re-wrap each filter, so a harmless extra
     * paren layer is fine.
     */
    public static List<String> renderScanFilters(JDBCTable table,
                                                 Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                 List<ScalarOperator> predicates) {
        if (predicates.isEmpty()) {
            return List.of();
        }
        String quote = JDBCScanNode.getIdentifierSymbol(table.getJdbcUri());
        Map<ColumnRefOperator, String> columnNames = buildRawColumnNameMap(colRefToColumnMetaMap, quote);
        ScalarOperatorToJDBCSQLVisitor renderer =
                ScalarOperatorToJDBCSQLVisitor.forScan(table, colRefToColumnMetaMap, columnNames);
        return predicates.stream()
                .map(op -> op.accept(renderer, null))
                .collect(Collectors.toList());
    }

    /**
     * Build a plain JDBC SELECT (base-column references only, no aliases). Table naming, column
     * naming, predicate rendering, and limit are all derived from the scan.
     */
    private static String buildSelectQuery(LogicalJDBCScanOperator scan, List<ColumnRefOperator> columns) {
        JDBCTable table = (JDBCTable) scan.getTable();
        String jdbcUri = table.getJdbcUri();
        String quote = JDBCScanNode.getIdentifierSymbol(jdbcUri);
        Map<ColumnRefOperator, String> columnNames = buildRawColumnNameMap(scan, quote);
        ScalarOperatorToJDBCSQLVisitor renderer = renderer(scan, columnNames);
        List<String> selectColumns = columns.stream()
                .map(col -> {
                    String columnName = columnNames.get(col);
                    Preconditions.checkState(columnName != null, "column %s is not in scan", col);
                    return columnName;
                })
                .collect(Collectors.toList());
        return buildSelectQuery(jdbcUri, selectColumns, buildTableRef(table, quote),
                renderFilterSql(Utils.extractConjuncts(scan.getPredicate()), renderer), scan.getLimit());
    }

    private static String buildSelectQuery(String jdbcUri, List<String> columns, String tableName,
                                           String filterSql, long limit) {
        boolean hasLimit = limit != Operator.DEFAULT_LIMIT;
        boolean isSqlServer = isJdbcUri(jdbcUri, "jdbc:sqlserver");
        boolean isOracle = isJdbcUri(jdbcUri, "jdbc:oracle");

        VelocityContext context = new VelocityContext();
        context.put("topClause", hasLimit && isSqlServer ? " TOP(" + limit + ")" : "");
        context.put("columns", Joiner.on(", ").join(columns));
        context.put("table", tableName);
        context.put("whereClause", filterSql == null || filterSql.isEmpty() ? "" : " WHERE " + filterSql);
        context.put("limitClause", hasLimit && !isSqlServer && !isOracle ? " LIMIT " + limit : "");
        String sql = build(context, SELECT_QUERY_TEMPLATE);

        if (!hasLimit || isSqlServer) {
            return sql;
        }
        if (isOracle) {
            VelocityContext oracleContext = new VelocityContext();
            oracleContext.put("query", sql);
            oracleContext.put("limit", limit);
            return build(oracleContext, ORACLE_LIMIT_QUERY_TEMPLATE);
        }
        return sql;
    }

    private static String renderFilterSql(List<String> filters) {
        return filters.stream()
                .map(JDBCPushDownSQLBuilder::parenthesizeFilter)
                .collect(Collectors.joining(" AND "));
    }

    private static String renderFilterSql(List<ScalarOperator> filters, ScalarOperatorToJDBCSQLVisitor renderer) {
        // The visitor already wraps each predicate in parentheses, so join directly with AND —
        // re-parenthesizing here (as the String overload does for raw, unparenthesized BE
        // conjuncts) would double-wrap into ((expr)) AND ((expr)).
        return filters.stream()
                .map(op -> op.accept(renderer, null))
                .collect(Collectors.joining(" AND "));
    }

    /**
     * Build the merged join pushdown SQL for {@code scans} (same-catalog base-table scans),
     * rendered as an order-insensitive remote comma join.
     *
     * <p>Only valid for INNER/CROSS merges: with no null-padding side, a comma join plus a single
     * WHERE clause is semantically equivalent to an explicit INNER JOIN, so operand order and
     * ON-vs-WHERE predicate placement do not matter. For order-sensitive joins (LEFT/RIGHT/FULL
     * OUTER), use {@link #buildJoinQuery(List, List, JoinOperator, List, List)} instead, which
     * emits an explicit {@code <type> JOIN ... ON ...} chain.
     *
     * @param outputColumns    columns to select (in order)
     * @param wherePredicates  all predicates for the merged query's WHERE clause (join + single-table)
     */
    public static String buildJoinQuery(List<LogicalJDBCScanOperator> scans,
                                        List<ColumnRefOperator> outputColumns,
                                        List<ScalarOperator> wherePredicates) {
        Preconditions.checkArgument(!scans.isEmpty(), "scans must not be empty");
        String identifierQuote = identifierQuote(scans);
        Map<ColumnRefOperator, String> qualifiedNames = buildQualifiedNameMap(scans, identifierQuote);
        ScalarOperatorToJDBCSQLVisitor joinRenderer = renderer(scans, qualifiedNames);

        List<String> tableExpressions = new ArrayList<>();
        for (int i = 0; i < scans.size(); i++) {
            tableExpressions.add(buildTableExpression(scans.get(i), tableAlias(i), identifierQuote));
        }

        // Keep scan-local predicates inside buildTableExpression() so predicate/LIMIT order on
        // each atom stays unchanged.
        VelocityContext context = new VelocityContext();
        context.put("selectList", buildSelectList(outputColumns, qualifiedNames));
        context.put("fromList", Joiner.on(", ").join(tableExpressions));
        context.put("whereClause", renderClause(" WHERE ", " AND ", wherePredicates, joinRenderer));
        return build(context, JOIN_QUERY_TEMPLATE);
    }

    /**
     * Build the merged join pushdown SQL for {@code scans} as an explicit left-deep
     * {@code <joinType> JOIN ... ON ...} chain, e.g.
     * {@code FROM s0 LEFT OUTER JOIN s1 ON (...) LEFT OUTER JOIN s2 ON (...) [WHERE ...]}.
     *
     * <p>Required for order-sensitive joins (LEFT/RIGHT/FULL OUTER), whose result rows depend on
     * operand order and on whether a predicate sits in ON or WHERE. The caller therefore MUST:
     * <ul>
     *   <li>pass {@code scans} already in join order — left operand first, then each subsequent
     *       scan in the order it is joined in (the builder preserves this order verbatim);</li>
     *   <li>split predicates into {@code onPredicates} (join conditions, rendered into ON clauses)
     *       and {@code filterPredicates} (post-join filters, rendered into the trailing WHERE) —
     *       moving an ON predicate to WHERE would silently turn an outer join into an inner one.</li>
     * </ul>
     *
     * <p>Each ON predicate is attached to the chain step that first brings all of its referenced
     * tables into scope (a predicate over {@code s0} and {@code s2} lands on the step that joins
     * {@code s2}). Scan-local predicates and per-table LIMITs stay inside each scan's derived
     * subquery (see {@link #buildTableExpression}), i.e. they pre-filter that base table before the
     * join — the standard equivalent of an ON-clause filter on the null-producing side.
     *
     * @param joinType         join type applied at every step of the chain (its
     *                         {@link JoinOperator#toString()} is the SQL keyword: {@code INNER JOIN},
     *                         {@code LEFT OUTER JOIN}, ...)
     * @param onPredicates     join conditions distributed across the chain's ON clauses
     * @param filterPredicates post-join filters rendered in the trailing WHERE clause
     */
    public static String buildJoinQuery(List<LogicalJDBCScanOperator> scans,
                                        List<ColumnRefOperator> outputColumns,
                                        JoinOperator joinType,
                                        List<ScalarOperator> onPredicates,
                                        List<ScalarOperator> filterPredicates) {
        Preconditions.checkArgument(!scans.isEmpty(), "scans must not be empty");
        Preconditions.checkArgument(scans.size() >= 2 || onPredicates.isEmpty(),
                "a single-scan query cannot carry join ON predicates");
        String identifierQuote = identifierQuote(scans);
        Map<ColumnRefOperator, String> qualifiedNames = buildQualifiedNameMap(scans, identifierQuote);
        ScalarOperatorToJDBCSQLVisitor joinRenderer = renderer(scans, qualifiedNames);

        VelocityContext context = new VelocityContext();
        context.put("selectList", buildSelectList(outputColumns, qualifiedNames));
        context.put("fromList", buildJoinChainFrom(scans, joinType, onPredicates, identifierQuote, joinRenderer));
        context.put("whereClause", renderClause(" WHERE ", " AND ", filterPredicates, joinRenderer));
        return build(context, JOIN_QUERY_TEMPLATE);
    }

    /**
     * SELECT clause: use ColumnRefOperator.getId() as the alias suffix so each output column has a
     * globally unique name. This lets the outer wrapping query reference columns by ID without
     * depending on any ordering between the merged tables.
     */
    private static String buildSelectList(List<ColumnRefOperator> outputColumns,
                                          Map<ColumnRefOperator, String> qualifiedNames) {
        return outputColumns.stream()
                .map(col -> qualifiedNames.get(col) + " AS " + outputColumnAlias(col))
                .collect(Collectors.joining(", "));
    }

    /**
     * Render the FROM clause as a left-deep {@code <joinType> JOIN ... ON ...} chain over
     * {@code scans} in the given order. Each ON predicate is placed on the earliest step whose
     * accumulated scope covers all the columns the predicate references.
     */
    private static String buildJoinChainFrom(List<LogicalJDBCScanOperator> scans, JoinOperator joinType,
                                             List<ScalarOperator> onPredicates, String identifierQuote,
                                             ScalarOperatorToJDBCSQLVisitor renderer) {
        List<ColumnRefSet> scanColumns = new ArrayList<>();
        for (LogicalJDBCScanOperator scan : scans) {
            scanColumns.add(new ColumnRefSet(new ArrayList<>(scan.getColRefToColumnMetaMap().keySet())));
        }

        String keyword = " " + joinType + " ";
        StringBuilder from = new StringBuilder(buildTableExpression(scans.get(0), tableAlias(0), identifierQuote));
        ColumnRefSet inScope = new ColumnRefSet();
        inScope.union(scanColumns.get(0));
        boolean[] placed = new boolean[onPredicates.size()];
        for (int i = 1; i < scans.size(); i++) {
            inScope.union(scanColumns.get(i));
            List<ScalarOperator> stepOn = new ArrayList<>();
            for (int p = 0; p < onPredicates.size(); p++) {
                if (!placed[p] && inScope.containsAll(onPredicates.get(p).getUsedColumns())) {
                    stepOn.add(onPredicates.get(p));
                    placed[p] = true;
                }
            }
            String tableExpr = buildTableExpression(scans.get(i), tableAlias(i), identifierQuote);
            if (stepOn.isEmpty()) {
                // No join condition reaches this step yet. An outer join's rows depend on its ON
                // clause and it is invalid without one, so that is an error; for INNER/CROSS fall
                // back to a portable CROSS JOIN (valid in every dialect, unlike a bare INNER JOIN
                // with no ON). A later step's ON re-applies the connecting predicate, and inner
                // joins are associative, so the result is unchanged.
                Preconditions.checkState(!joinType.isOuterJoin(),
                        "outer join step for %s has no ON predicate", tableAlias(i));
                from.append(" CROSS JOIN ").append(tableExpr);
            } else {
                from.append(keyword).append(tableExpr).append(" ON ").append(stepOn.stream()
                        .map(op -> op.accept(renderer, null))
                        .collect(Collectors.joining(" AND ")));
            }
        }
        // Every ON predicate must land on some step; an unplaced one references a column outside
        // the merged scan set, which would produce an out-of-scope reference in the rendered SQL.
        for (int p = 0; p < placed.length; p++) {
            Preconditions.checkState(placed[p],
                    "join predicate references columns outside the merged scans: %s", onPredicates.get(p));
        }
        return from.toString();
    }

    private static String identifierQuote(List<LogicalJDBCScanOperator> scans) {
        return JDBCScanNode.getIdentifierSymbol(((JDBCTable) scans.get(0).getTable()).getJdbcUri());
    }

    // ------------------------------------------------------------------------------------
    // Aggregate pushdown assembly
    // ------------------------------------------------------------------------------------

    /**
     * Build the remote SQL for folding an aggregation into a single JDBC scan:
     * {@code SELECT item AS alias, ... FROM tableExpr [WHERE ...] [GROUP BY ...] [HAVING ...]}.
     *
     * <p>{@code selectItems} are group-by column refs and aggregate calls vetted by
     * {@code PushDownAggToJDBCScanRule}; {@code filters} are the scan's own pushed
     * predicates. The scan may be a base table or an inline table — a native_query pass-through or
     * a derived table produced by a previous pushdown — which {@link #buildTableRef} renders as its
     * raw subquery body wrapped as {@code (<body>) sr_inline}.
     */
    public static String buildScalarSelectQuery(LogicalJDBCScanOperator scan,
                                             List<ScalarOperator> selectItems, List<String> selectAliases,
                                             List<ScalarOperator> groupBys, List<ScalarOperator> havings) {
        Preconditions.checkArgument(selectItems.size() == selectAliases.size(),
                "selectItems and selectAliases must align");
        JDBCTable table = (JDBCTable) scan.getTable();
        Map<ColumnRefOperator, Column> scanColumns = scan.getColRefToColumnMetaMap();
        List<ScalarOperator> filters = Utils.extractConjuncts(scan.getPredicate());
        String quote = JDBCScanNode.getIdentifierSymbol(table.getJdbcUri());
        Map<ColumnRefOperator, String> columnNames = buildRawColumnNameMap(scan, quote);
        ScalarOperatorToJDBCSQLVisitor renderer = renderer(scan, columnNames);

        List<String> items = new ArrayList<>();
        for (int i = 0; i < selectItems.size(); i++) {
            items.add(selectItems.get(i).accept(renderer, null)
                    + " AS " + quoteIdentifier(selectAliases.get(i), quote));
        }

        String tableExpr = buildTableRef(table, quote);
        String whereClause = "";
        if (scan.getLimit() == Operator.DEFAULT_LIMIT) {
            whereClause = renderClause(" WHERE ", " AND ", filters, renderer);
        } else {
            String tableQuery = buildSelectQuery(scan, new ArrayList<>(scanColumns.keySet()));
            tableExpr = "(" + tableQuery + ") " + LIMITED_SUBQUERY_ALIAS;
        }

        VelocityContext context = new VelocityContext();
        context.put("selectList", Joiner.on(", ").join(items));
        context.put("tableExpr", tableExpr);
        context.put("whereClause", whereClause);
        context.put("groupByClause", renderClause(" GROUP BY ", ", ", groupBys, renderer));
        context.put("havingClause", renderClause(" HAVING ", " AND ", havings, renderer));
        return build(context, AGGREGATE_QUERY_TEMPLATE);
    }

    private static Map<ColumnRefOperator, String> buildQualifiedNameMap(List<LogicalJDBCScanOperator> scans,
                                                                        String identifierQuote) {
        // Derive the qualified-name map (alias.col for every scan column), keyed by
        // ColumnRefOperator across all scans. The Oracle temporal-column map is the visitor's
        // concern and is built per render in renderer(), not stored here.
        Map<ColumnRefOperator, String> qualifiedNames = new HashMap<>();
        for (int i = 0; i < scans.size(); i++) {
            LogicalJDBCScanOperator scan = scans.get(i);
            String alias = tableAlias(i);
            for (Map.Entry<ColumnRefOperator, Column> e : scan.getColRefToColumnMetaMap().entrySet()) {
                qualifiedNames.put(e.getKey(),
                        alias + "." + identifierQuote + e.getValue().getName() + identifierQuote);
            }
        }
        return qualifiedNames;
    }

    /**
     * Builds a single table operand for FROM/JOIN under the join-local alias {@code alias}. A base
     * table becomes its quoted name; an inline table (native_query pass-through or a prior pushdown)
     * becomes its raw subquery body parenthesized. When the scan carries a per-table predicate or
     * limit, the operand is instead a derived subquery that pre-filters that scan before the join.
     */
    private static String buildTableExpression(LogicalJDBCScanOperator scan, String alias, String identifierQuote) {
        JDBCTable table = (JDBCTable) scan.getTable();

        List<ScalarOperator> perTablePredicates = Utils.extractConjuncts(scan.getPredicate());
        if (perTablePredicates.isEmpty() && scan.getLimit() == Operator.DEFAULT_LIMIT) {
            // Reference the table directly under the join-local alias. buildBaseTableRef parenthesizes
            // an inline table's body (a complete SELECT) so the sr_t{i} alias attaches to a derived
            // table — quoting it as an identifier would be invalid SQL, and reusing getInlineTableExpr()
            // would emit a redundant inner sr_inline alias underneath the sr_t{i} one.
            return buildBaseTableRef(table, identifierQuote) + " " + alias;
        }
        // Per-table predicate or limit: pre-filter inside a derived subquery taking the join-local
        // alias. For an inline table this nests as (SELECT ... FROM (<body>) sr_inline WHERE ...) sr_t{i}.
        // PruneScanColumnRule guarantees the scan's colRefToColumnMetaMap is non-empty
        // (it adds the smallest column back when no column is required, e.g. COUNT(*)).
        Map<ColumnRefOperator, Column> columnRefMap = scan.getColRefToColumnMetaMap();
        Preconditions.checkState(!columnRefMap.isEmpty(),
                "scan %s has empty columnRefMap", alias);
        String tableQuery = buildSelectQuery(scan, new ArrayList<>(columnRefMap.keySet()));
        return "(" + tableQuery + ") " + alias;
    }

    /**
     * The FROM operand for a single-scan query: an inline table carries the {@code sr_inline}
     * derived-table alias (a bare subquery is invalid SQL in most dialects), while a base table
     * stands alone — with no join there is no need to alias it for column qualification.
     */
    private static String buildTableRef(JDBCTable table, String identifierQuote) {
        return table.isInlineTable()
                ? table.getInlineTableExpr()
                : buildBaseTableRef(table, identifierQuote);
    }

    /**
     * The FROM operand for a JDBC table <em>without</em> an alias: an inline table's raw subquery
     * body (a complete SELECT) parenthesized as a derived table, or a quoted base-table name. The
     * caller attaches whatever alias the surrounding query needs — {@code sr_t{i}} for a join atom.
     */
    private static String buildBaseTableRef(JDBCTable table, String identifierQuote) {
        return table.isInlineTable()
                ? "(" + table.getCatalogTableName() + ")"
                : JDBCScanNode.wrapWithIdentifier(table.getCatalogTableName(), identifierQuote);
    }

    private static Map<ColumnRefOperator, String> buildRawColumnNameMap(LogicalJDBCScanOperator scan,
                                                                        String identifierQuote) {
        return buildRawColumnNameMap(scan.getColRefToColumnMetaMap(), identifierQuote);
    }

    private static Map<ColumnRefOperator, String> buildRawColumnNameMap(
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap, String identifierQuote) {
        Map<ColumnRefOperator, String> rawNames = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, Column> e : colRefToColumnMetaMap.entrySet()) {
            rawNames.put(e.getKey(), quoteIdentifier(e.getValue().getName(), identifierQuote));
        }
        return rawNames;
    }

    /**
     * Build a renderer for the given scans and column-name map. Dialect selection and the Oracle
     * temporal-column classification both live in the visitor
     * ({@link ScalarOperatorToJDBCSQLVisitor#forDialect}); the builder just hands over the scans.
     */
    private static ScalarOperatorToJDBCSQLVisitor renderer(List<LogicalJDBCScanOperator> scans,
                                                           Map<ColumnRefOperator, String> nameMap) {
        return ScalarOperatorToJDBCSQLVisitor.forDialect(scans, nameMap);
    }

    private static ScalarOperatorToJDBCSQLVisitor renderer(LogicalJDBCScanOperator scan,
                                                           Map<ColumnRefOperator, String> nameMap) {
        return renderer(List.of(scan), nameMap);
    }

    /**
     * Positional alias for the {@code index}-th merged table inside the pushdown SQL
     * ({@code sr_t0}, {@code sr_t1}, ...). The builder owns this convention so FROM table
     * references and qualified-name maps stay in sync; mirrors {@link #outputColumnAlias(int)}.
     */
    private static String tableAlias(int index) {
        return "sr_t" + index;
    }

    /**
     * Returns the output column alias used inside the pushdown SELECT list. The alias is
     * derived from the column ref ID (unique within the query), so the mapping is stable
     * regardless of iteration order. All sites that produce or consume these aliases
     * call this method to stay in sync.
     */
    public static String outputColumnAlias(int columnRefId) {
        return "sr_c" + columnRefId;
    }

    /** Convenience overload accepting a ColumnRefOperator directly. */
    public static String outputColumnAlias(ColumnRefOperator col) {
        return outputColumnAlias(col.getId());
    }

    private static String renderClause(String keyword, String separator,
                                       List<ScalarOperator> operators, ScalarOperatorToJDBCSQLVisitor renderer) {
        if (operators.isEmpty()) {
            return "";
        }
        return keyword + operators.stream()
                .map(op -> op.accept(renderer, null))
                .collect(Collectors.joining(separator));
    }

    private static String build(VelocityContext context, String template) {
        StringWriter sw = new StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }

    private static String quoteIdentifier(String name, String quote) {
        if (name == null || quote.isEmpty() || (name.startsWith(quote) && name.endsWith(quote))) {
            return name;
        }
        return quote + name + quote;
    }

    private static String parenthesizeFilter(String filter) {
        return "(" + filter + ")";
    }

    private static boolean isJdbcUri(String jdbcUri, String prefix) {
        return jdbcUri != null && jdbcUri.toLowerCase(Locale.ROOT).startsWith(prefix);
    }
}
