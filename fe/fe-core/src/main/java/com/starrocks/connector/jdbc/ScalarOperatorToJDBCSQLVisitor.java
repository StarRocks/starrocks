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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.CanPushDownPredicateVisitor;
import com.starrocks.sql.optimizer.rewrite.JDBCCastTypeMapper;
import com.starrocks.sql.optimizer.rewrite.PostgresCollation;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Render a {@link ScalarOperator} tree into a JDBC-dialect SQL fragment. The base class
 * handles the dialect-agnostic shape; each per-dialect subclass overrides only the visit
 * methods where the emitted SQL diverges. The caller is expected to have gated the
 * expression through {@link CanPushDownPredicateVisitor#canPushDown} for the same dialect.
 *
 * <p>Aggregate calls ({@code count/sum/min/max/avg}, including {@code count(*)} and
 * {@code count(DISTINCT col)}) are the one exception to that gating contract: they are
 * admitted by {@code PushDownAggToJDBCScanRule}'s own checks instead, and only ever appear
 * in pushed-down SELECT items (an aggregate inside a HAVING predicate is rejected by the
 * gate before rendering).
 */
public abstract class ScalarOperatorToJDBCSQLVisitor extends ScalarOperatorVisitor<String, Void> {

    /** Aggregates the JDBC pushdown rules may emit; rendering is dialect-uniform. */
    private static final Set<String> AGGREGATE_FUNCTIONS = Set.of("count", "sum", "min", "max", "avg");

    private final Map<ColumnRefOperator, String> columnNames;

    protected ScalarOperatorToJDBCSQLVisitor(Map<ColumnRefOperator, String> columnNames) {
        this.columnNames = columnNames;
    }

    /**
     * Build a renderer for a set of JDBC scans being pushed into one remote query, dispatched on
     * the scans' (shared) dialect. For Oracle it additionally classifies, in the {@code ORACLE}
     * case, which scan columns are remotely DATE/TIMESTAMP (even when StarRocks maps them to
     * VARCHAR) so string-literal comparisons get wrapped in {@code DATE '...'} / {@code TIMESTAMP
     * '...'}; every other dialect ignores temporal columns entirely.
     */
    public static ScalarOperatorToJDBCSQLVisitor forDialect(List<LogicalJDBCScanOperator> scans,
                                                            Map<ColumnRefOperator, String> columnNames) {
        JDBCTable.ProtocolType dialect = ((JDBCTable) scans.get(0).getTable()).getProtocolType();
        return forDialect(dialect, columnNames,
                dialect == JDBCTable.ProtocolType.ORACLE ? oracleTemporalColumns(scans) : Collections.emptyMap(),
                postgresCollatableColumns(scans));
    }

    /**
     * The scans' columns a comparison may be pushed down for under {@code COLLATE "C"}, unioned
     * across all scans. Empty for every dialect but PostgreSQL (see {@link PostgresCollation}), so
     * only the PostgreSQL renderer ever finds anything here.
     */
    private static Set<ColumnRefOperator> postgresCollatableColumns(List<LogicalJDBCScanOperator> scans) {
        Set<ColumnRefOperator> collatable = new HashSet<>();
        for (LogicalJDBCScanOperator scan : scans) {
            collatable.addAll(PostgresCollation.collatableColumns(
                    (JDBCTable) scan.getTable(), scan.getColRefToColumnMetaMap()));
        }
        return collatable;
    }

    /**
     * Renderer for a single base-table JDBC scan -- the {@code JDBCScanNode} filter path. The Oracle
     * temporal-column map is derived from this one scan's columns, mirroring
     * {@link #forDialect(List, Map)} for the merged-pushdown path so a temporal predicate renders
     * identical SQL whether or not aggregate/join pushdown fires.
     */
    public static ScalarOperatorToJDBCSQLVisitor forScan(JDBCTable table,
                                                         Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                         Map<ColumnRefOperator, String> columnNames) {
        JDBCTable.ProtocolType dialect = table.getProtocolType();
        return forDialect(dialect, columnNames,
                dialect == JDBCTable.ProtocolType.ORACLE
                        ? oracleTemporalColumns(table, colRefToColumnMetaMap) : Collections.emptyMap(),
                PostgresCollation.collatableColumns(table, colRefToColumnMetaMap));
    }

    private static ScalarOperatorToJDBCSQLVisitor forDialect(JDBCTable.ProtocolType dialect,
                                                             Map<ColumnRefOperator, String> columnNames,
                                                             Map<ColumnRefOperator, String> oracleTemporalColumns,
                                                             Set<ColumnRefOperator> collatableColumns) {
        switch (dialect) {
            case MYSQL:
            case MARIADB:
                return new MySQLLikeSQLRenderer(columnNames);
            case POSTGRES:
                return new PostgresSQLRenderer(columnNames, collatableColumns, arrayLowerBoundCorrectionEnabled());
            case ORACLE:
                return new OracleSQLRenderer(columnNames, oracleTemporalColumns);
            case CLICKHOUSE:
                return new ClickHouseSQLRenderer(columnNames);
            case UNKNOWN:
            default:
                return new UnknownSQLRenderer(columnNames);
        }
    }

    /**
     * Whether {@code enable_jdbc_array_lower_bound_correction} is on for the statement being
     * planned. Read here, at the one point every renderer is built, so the scan-filter path and the
     * merged join/aggregate path cannot disagree about it within a statement. Falls back to the
     * variable's default (off) when there is no session, which is also the conservative answer: it
     * is the rendering every release before this one emitted.
     */
    private static boolean arrayLowerBoundCorrectionEnabled() {
        return ConnectContext.getSessionVariableOrDefault().isEnableJdbcArrayLowerBoundCorrection();
    }

    /**
     * Classify which of the scans' columns are remotely DATE/TIMESTAMP (even when StarRocks maps
     * them to VARCHAR), unioned across all scans, as a colRef -> "DATE"|"TIMESTAMP" map. Uses the
     * same temporal source of truth ({@link #temporalColumnsByNormalizedName}) as the
     * physical-conjunct rewrite in JDBCScanNode, so both stay in sync. Empty when no scan column
     * is temporal; only the Oracle renderer consumes it.
     */
    private static Map<ColumnRefOperator, String> oracleTemporalColumns(List<LogicalJDBCScanOperator> scans) {
        Map<ColumnRefOperator, String> temporal = new HashMap<>();
        for (LogicalJDBCScanOperator scan : scans) {
            temporal.putAll(oracleTemporalColumns((JDBCTable) scan.getTable(), scan.getColRefToColumnMetaMap()));
        }
        return temporal;
    }

    /** Single-scan version of {@link #oracleTemporalColumns(List)}, keyed by this scan's columns. */
    private static Map<ColumnRefOperator, String> oracleTemporalColumns(
            JDBCTable table, Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        Map<String, TemporalKind> byName = temporalColumnsByNormalizedName(table);
        if (byName.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<ColumnRefOperator, String> temporal = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumnMetaMap.entrySet()) {
            TemporalKind kind = byName.get(normalizeColumnName(entry.getValue().getName()));
            if (kind != null) {
                temporal.put(entry.getKey(), kind == TemporalKind.DATE ? "DATE" : "TIMESTAMP");
            }
        }
        return temporal;
    }

    /**
     * Temporal classification of a remote JDBC column. Single source of truth shared by the
     * push-down renderer here and the physical-conjunct rewrite in {@code JDBCScanNode}, so both
     * agree on whether a column is DATE/TIMESTAMP.
     */
    public enum TemporalKind {
        DATE, TIMESTAMP
    }

    /** Which Oracle literal keyword a temporal string literal needs, decided by its textual shape. */
    public enum OracleTemporalLiteralKind {
        DATE, TIMESTAMP, NONE
    }

    private static final Pattern ORACLE_DATE_ONLY_LITERAL = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}$");

    /**
     * Classify an Oracle temporal string literal by its textual shape -- NOT by the column's
     * declared type. Oracle's DATE literal must be exactly {@code 'YYYY-MM-DD'} (a time component
     * raises ORA-01861), so a bare date ({@code <= 10} chars matching {@code YYYY-M-D}) is a
     * {@code DATE} literal, anything longer is a {@code TIMESTAMP} literal, and a short non-date
     * string needs no keyword. The shape decision used by {@link #renderOracleTemporalLiteral}.
     */
    public static OracleTemporalLiteralKind classifyOracleTemporalLiteral(String literal) {
        if (literal.length() <= "0000-00-00".length()) {
            return ORACLE_DATE_ONLY_LITERAL.matcher(literal).matches()
                    ? OracleTemporalLiteralKind.DATE : OracleTemporalLiteralKind.NONE;
        }
        return OracleTemporalLiteralKind.TIMESTAMP;
    }

    /**
     * Render {@code literal} as an Oracle temporal literal, choosing the keyword from the literal's
     * textual shape ({@link #classifyOracleTemporalLiteral}): {@code DATE 'YYYY-MM-DD'},
     * {@code TIMESTAMP '...'}, or a bare quoted string when it is not a temporal shape. The single
     * quote is the only character Oracle needs escaped inside a string literal. Shared by the
     * aggregate-pushdown renderer ({@link OracleSQLRenderer#renderTemporalLiteral}) and the scan
     * path ({@code JDBCScanNode.buildOracleTemporalLiteralExpr}) so a temporal predicate renders
     * byte-identical SQL whether or not aggregate pushdown fires.
     */
    public static String renderOracleTemporalLiteral(String literal) {
        String quoted = "'" + literal.replace("'", "''") + "'";
        switch (classifyOracleTemporalLiteral(literal)) {
            case DATE:
                return "DATE " + quoted;
            case TIMESTAMP:
                return "TIMESTAMP " + quoted;
            default:
                return quoted;
        }
    }

    // Oracle's java.sql.Types vendor extensions (not part of the JDBC standard).
    private static final int ORACLE_TIMESTAMP_WITH_LOCAL_TZ = -102;
    private static final int ORACLE_TIMESTAMP_WITH_TZ = -101;

    /** Map a raw JDBC type code (plus Oracle extensions) to its temporal kind, or null. */
    private static TemporalKind temporalKindOf(int jdbcType) {
        switch (jdbcType) {
            case Types.DATE:
                return TemporalKind.DATE;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case ORACLE_TIMESTAMP_WITH_LOCAL_TZ:
            case ORACLE_TIMESTAMP_WITH_TZ:
                return TemporalKind.TIMESTAMP;
            default:
                return null;
        }
    }

    /**
     * Normalized remote-column-name -> {@link TemporalKind} for the table's original JDBC schema
     * (the types as the external DB reports them, which may differ from the StarRocks column
     * types -- e.g. Oracle DATE/TIMESTAMP surfaced as VARCHAR). Empty when no original type info
     * is available, so callers can union it across scans unconditionally.
     */
    public static Map<String, TemporalKind> temporalColumnsByNormalizedName(JDBCTable table) {
        Map<String, TemporalKind> result = new HashMap<>();
        for (Map.Entry<String, Integer> entry : table.getOriginalJdbcColumnTypes().entrySet()) {
            TemporalKind kind = temporalKindOf(entry.getValue());
            if (kind != null) {
                result.put(normalizeColumnName(entry.getKey()), kind);
            }
        }
        return result;
    }

    /** Strip a single pair of surrounding {@code "} or {@code `} quotes, then lower-case. */
    public static String normalizeColumnName(String columnName) {
        if (columnName == null) {
            return "";
        }
        if (columnName.length() >= 2) {
            char first = columnName.charAt(0);
            char last = columnName.charAt(columnName.length() - 1);
            if ((first == '"' && last == '"') || (first == '`' && last == '`')) {
                columnName = columnName.substring(1, columnName.length() - 1);
            }
        }
        return columnName.toLowerCase(Locale.ROOT);
    }

    protected abstract JDBCTable.ProtocolType dialect();

    /**
     * Whether {@code AVG} over an integer column must be rendered as {@code AVG(col * 1.0)} so the
     * remote engine computes a floating-point average. Engines whose {@code AVG(<integer>)} truncates
     * via integer division (e.g. SQL Server: {@code AVG} of {@code {1,1,2,3}} is {@code 1}, not
     * {@code 1.75}) opt in; dialects whose AVG is already fractional (MySQL/PostgreSQL/Oracle/
     * ClickHouse) leave it {@code false} and emit {@code AVG(col)} unchanged.
     */
    protected boolean avgIntegerNeedsFloatCast() {
        return false;
    }

    @Override
    public String visit(ScalarOperator scalarOperator, Void context) {
        // Fallback: should not happen for validated predicates
        return scalarOperator.toString();
    }

    @Override
    public String visitVariableReference(ColumnRefOperator op, Void context) {
        String name = columnNames.get(op);
        return name != null ? name : op.getName();
    }

    @Override
    public String visitConstant(ConstantOperator op, Void context) {
        if (op.isNull()) {
            return "NULL";
        }
        if (op.getType().isStringType()) {
            return "'" + op.toString().replace("'", "''") + "'";
        }
        if (op.getType().isDateType()) {
            return "'" + op.toString() + "'";
        }
        if (op.getType().isBoolean()) {
            return Boolean.TRUE.equals(op.getValue()) ? "TRUE" : "FALSE";
        }
        return op.toString();
    }

    @Override
    public String visitBinaryPredicate(BinaryPredicateOperator op, Void context) {
        String left = op.getChild(0).accept(this, null);
        String right = op.getChild(1).accept(this, null);
        return "(" + left + " " + op.getBinaryType().toString() + " " + right + ")";
    }

    @Override
    public String visitCompoundPredicate(CompoundPredicateOperator op, Void context) {
        switch (op.getCompoundType()) {
            case AND: {
                List<String> parts = new ArrayList<>();
                for (ScalarOperator child : op.getChildren()) {
                    parts.add(child.accept(this, null));
                }
                return "(" + Joiner.on(" AND ").join(parts) + ")";
            }
            case OR: {
                List<String> parts = new ArrayList<>();
                for (ScalarOperator child : op.getChildren()) {
                    parts.add(child.accept(this, null));
                }
                return "(" + Joiner.on(" OR ").join(parts) + ")";
            }
            case NOT:
                return "(NOT " + op.getChild(0).accept(this, null) + ")";
            default:
                return op.toString();
        }
    }

    @Override
    public String visitInPredicate(InPredicateOperator op, Void context) {
        String col = op.getChild(0).accept(this, null);
        List<String> values = new ArrayList<>();
        for (int i = 1; i < op.getChildren().size(); i++) {
            values.add(op.getChild(i).accept(this, null));
        }
        String inClause = op.isNotIn() ? " NOT IN " : " IN ";
        return "(" + col + inClause + "(" + Joiner.on(", ").join(values) + "))";
    }

    @Override
    public String visitIsNullPredicate(IsNullPredicateOperator op, Void context) {
        String col = op.getChild(0).accept(this, null);
        String expr = op.isNotNull() ? col + " IS NOT NULL" : col + " IS NULL";
        return "(" + expr + ")";
    }

    @Override
    public String visitBetweenPredicate(BetweenPredicateOperator op, Void context) {
        String col = op.getChild(0).accept(this, null);
        String lower = op.getChild(1).accept(this, null);
        String upper = op.getChild(2).accept(this, null);
        String betweenClause = op.isNotBetween() ? " NOT BETWEEN " : " BETWEEN ";
        return "(" + col + betweenClause + lower + " AND " + upper + ")";
    }

    @Override
    public String visitCastOperator(CastOperator op, Void context) {
        String child = op.getChild(0).accept(this, null);
        if (op.isImplicit()) {
            return child;
        }
        // Gate has already verified the (type, dialect) pair is supported; fall back to the
        // StarRocks-internal toSql() form only defensively if the mapper returns empty.
        String typeName = JDBCCastTypeMapper.renderCastType(op.getType(), dialect())
                .orElseGet(() -> op.getType().toSql());
        return "CAST(" + child + " AS " + typeName + ")";
    }

    @Override
    public String visitCall(CallOperator op, Void context) {
        String fnName = op.getFnName().toLowerCase(Locale.ROOT);
        String sqlOp = CanPushDownPredicateVisitor.BINARY_INFIX_FUNCTIONS.get(fnName);
        if (sqlOp != null && op.getChildren().size() == 2) {
            String left = op.getChild(0).accept(this, null);
            String right = op.getChild(1).accept(this, null);
            return "(" + left + " " + sqlOp + " " + right + ")";
        }
        if ("concat".equals(fnName) && op.getChildren().size() >= 2) {
            // CanPushDownPredicateVisitor restricts concat push-down to MySQL-compatible dialects,
            // so always emit MySQL CONCAT(...) here.
            List<String> args = op.getChildren().stream()
                    .map(child -> child.accept(this, null))
                    .collect(Collectors.toList());
            return "CONCAT(" + Joiner.on(", ").join(args) + ")";
        }
        if (op.isAggregate() && AGGREGATE_FUNCTIONS.contains(fnName)) {
            if (op.isCountStar()) {
                return fnName + "(*)";
            }
            String arg = renderAggregateArgument(fnName, op.getChild(0));
            // AVG over an integer column integer-divides on some engines (e.g. SQL Server) and may
            // return a narrower JDBC type than the DOUBLE avg slot; multiplying by 1.0 forces the
            // remote AVG into floating point, matching StarRocks' avg semantics. Only dialects that
            // need it opt in (see avgIntegerNeedsFloatCast); the others emit AVG(col) unchanged.
            if ("avg".equals(fnName) && avgIntegerNeedsFloatCast() && op.getChild(0).getType().isIntegerType()) {
                arg = "(" + arg + " * 1.0)";
            }
            return fnName + "(" + (op.isDistinct() ? "DISTINCT " : "") + arg + ")";
        }
        // Fallback for unknown functions — shouldn't reach here if CanPushDownPredicateVisitor was checked
        return op.toString();
    }

    /**
     * Renders an aggregate's argument. A dialect overrides this when the remote aggregate needs the
     * argument qualified — PostgreSQL's MIN/MAX pick the extreme under the argument's collation, so
     * they have to name the same one the comparisons do.
     */
    protected String renderAggregateArgument(String fnName, ScalarOperator arg) {
        return arg.accept(this, null);
    }

    /** MYSQL / MARIADB: base behavior. */
    public static class MySQLLikeSQLRenderer extends ScalarOperatorToJDBCSQLVisitor {
        public MySQLLikeSQLRenderer(Map<ColumnRefOperator, String> columnNames) {
            super(columnNames);
        }

        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.MYSQL;
        }
    }

    /**
     * POSTGRES: {@code <=>} (EQ_FOR_NULL) renders as the SQL-standard
     * {@code IS NOT DISTINCT FROM}; Postgres has no MySQL-style operator.
     */
    public static class PostgresSQLRenderer extends ScalarOperatorToJDBCSQLVisitor {
        // Columns whose remote comparison order is made to match StarRocks' by COLLATE "C" --
        // PostgresCollation.collatableColumns, the narrow set, never the gate's wider one.
        // CanPushDownPredicateVisitor kept every other string comparison local except the ones
        // PostgresCollation.orderSafeColumns admits without a collation, so anything reaching this
        // renderer that is not in here compares under an order both sides agree on already. A uuid
        // column is the second kind, and naming a collation for it is an error on PostgreSQL's
        // side, not a redundant one -- which is why the gate's set must not be passed here.
        private final Set<ColumnRefOperator> collatableColumns;
        // enable_jdbc_array_lower_bound_correction; see visitCollectionElement.
        private final boolean correctArrayLowerBound;

        public PostgresSQLRenderer(Map<ColumnRefOperator, String> columnNames,
                                   Set<ColumnRefOperator> collatableColumns) {
            this(columnNames, collatableColumns, false);
        }

        public PostgresSQLRenderer(Map<ColumnRefOperator, String> columnNames,
                                   Set<ColumnRefOperator> collatableColumns,
                                   boolean correctArrayLowerBound) {
            super(columnNames);
            this.collatableColumns = collatableColumns;
            this.correctArrayLowerBound = correctArrayLowerBound;
        }

        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.POSTGRES;
        }

        @Override
        public String visitBinaryPredicate(BinaryPredicateOperator op, Void context) {
            if (op.getBinaryType() == BinaryType.EQ_FOR_NULL) {
                String left = op.getChild(0).accept(this, null);
                String right = op.getChild(1).accept(this, null);
                return "(" + left + " IS NOT DISTINCT FROM " + right + ")";
            }
            if (op.getBinaryType().isRange()) {
                // PostgreSQL derives one collation for the whole comparison, and an explicit one
                // wins over the columns' implicit collations. Naming it on both collatable sides is
                // accepted because they agree; two different explicit collations would be an error.
                return "(" + collated(op.getChild(0)) + " " + op.getBinaryType().toString() + " "
                        + collated(op.getChild(1)) + ")";
            }
            return super.visitBinaryPredicate(op, context);
        }

        @Override
        public String visitBetweenPredicate(BetweenPredicateOperator op, Void context) {
            // The collation has to sit on the value: naming it on a bound would only govern that
            // one comparison and leave value-vs-other-bound under the column's own collation.
            String betweenClause = op.isNotBetween() ? " NOT BETWEEN " : " BETWEEN ";
            return "(" + collated(op.getChild(0)) + betweenClause
                    + op.getChild(1).accept(this, null) + " AND " + op.getChild(2).accept(this, null) + ")";
        }

        @Override
        protected String renderAggregateArgument(String fnName, ScalarOperator arg) {
            // MIN/MAX return the extreme under the comparison order, so they need the same
            // collation the comparisons carry. SUM/AVG/COUNT do not depend on string order.
            if (FunctionSet.MIN.equals(fnName) || FunctionSet.MAX.equals(fnName)) {
                return collated(arg);
            }
            return super.renderAggregateArgument(fnName, arg);
        }

        /**
         * An array subscript, sent to PostgreSQL. What is emitted depends on
         * {@code enable_jdbc_array_lower_bound_correction}, which governs this rendering and nothing
         * else -- the same expressions are pushed down under either of its values, because the gate,
         * {@code CanPushDownPredicateVisitor.PostgresPushDownGate#visitCollectionElement}, does not
         * read it. Whether a subscript is pushed down at all is the other variable's job:
         * {@code enable_jdbc_array_subscript_push_down}, read by that gate. With it off no subscript
         * reaches this method, and the correction variable has nothing left to govern.
         *
         * <p>A PostgreSQL array carries its lower bound per value, not per column: the same column
         * can hold {@code '[0:2]={zero,one,two}'} on one row and {@code '{a,b}'} (lower bound 1) on
         * the next. Reading rebases every value to 1 -- the JDBC driver hands over a plain Java array
         * and drops the bound -- so StarRocks' element {@code k} is PostgreSQL's element
         * {@code array_lower(a, 1) + k - 1}, not its element {@code k}.
         *
         * <p><b>Off (the default).</b> The plain {@code a[k]} goes out, assuming a lower bound of 1.
         * Every array PostgreSQL builds for itself has one, so this is right for all but arrays
         * explicitly given another bound; <b>on a row whose lower bound is not 1 the pushed-down
         * subscript then quietly answers something other than the same subscript evaluated locally</b>
         * -- measured on {@code '[0:2]={zero,one,two}'}, remote {@code a[1]} is 'one' where local is
         * 'zero', and on {@code '[5:7]={a,b,c}'} remote {@code a[1]} is NULL where local is 'a'. That
         * is the trade this default accepts, and it matches what Starburst emits. Turn the variable on
         * to get the answer local evaluation gives, on every row.
         *
         * <p>No guard is needed around the plain subscript: with no arithmetic on the index there is
         * nothing to overflow, and PostgreSQL answers NULL for an out-of-range, zero or negative
         * subscript exactly as StarRocks does. Verified against PostgreSQL 16 and a live catalog, on
         * lower-bound-1 rows, for k in range, k &gt; length, k = 0, k = -1, k = -2147483648,
         * k = 2147483647, and for NULL and empty arrays: same answer on both sides, no error raised.
         *
         * <p><b>On.</b> The subscript is corrected per row, {@code array_lower(a, 1) + k - 1}, so the
         * push-down cannot change the answer. The correction is wrapped in a range test because the
         * corrected subscript is int4 arithmetic inside PostgreSQL and can overflow where StarRocks
         * simply answers NULL: {@code a[2147483647]} over a row whose lower bound is 5 asks
         * PostgreSQL for element 2147483651 and raises "integer out of range", while the same
         * expression evaluated locally is NULL. Guarding on the length settles it, because StarRocks'
         * index {@code k} addresses an element exactly when {@code 1 <= k <= array_length(a, 1)}, and
         * in that case the corrected subscript lies between the array's own lower and upper bounds
         * and is therefore an int4 by construction. Outside it the answer is NULL, which is what
         * StarRocks gives for an out-of-range, zero or negative subscript. NULL and empty arrays need
         * no special case: {@code array_length} is NULL for both, so the test fails and the result is
         * NULL.
         *
         * <p>The offset is folded into a literal at render time so the emitted subscript is a single
         * addition. The gate guarantees the collection is a bare column reference (so naming it three
         * times re-reads a column rather than evaluating anything repeatedly) and the subscript an
         * int4 constant.
         */
        @Override
        public String visitCollectionElement(CollectionElementOperator op, Void context) {
            String array = op.getChild(0).accept(this, null);
            ScalarOperator subscript = op.getChild(1);
            OptionalLong index = CanPushDownPredicateVisitor.constantSubscript(subscript);
            String renderedIndex = index.isPresent()
                    ? Long.toString(index.getAsLong()) : subscript.accept(this, null);
            if (!correctArrayLowerBound) {
                return array + "[" + renderedIndex + "]";
            }
            String shift;
            if (index.isPresent()) {
                long offset = index.getAsLong() - 1;
                shift = offset < 0 ? " - " + Math.abs(offset) : " + " + offset;
            } else {
                shift = " + " + renderedIndex + " - 1";
            }
            return "(CASE WHEN " + renderedIndex + " BETWEEN 1 AND array_length(" + array + ", 1)"
                    + " THEN " + array + "[array_lower(" + array + ", 1)" + shift + "]"
                    + " ELSE NULL END)";
        }

        /** Renders {@code operand}, appending {@code COLLATE "C"} when it is a collatable column. */
        private String collated(ScalarOperator operand) {
            String sql = operand.accept(this, null);
            return collatableColumns.contains(operand) ? sql + PostgresCollation.COLLATE_C : sql;
        }
    }

    /**
     * ORACLE: DATE/DATETIME literals are wrapped in ANSI {@code DATE '...'} /
     * {@code TIMESTAMP '...'} to avoid NLS_DATE_FORMAT-dependent parsing. The same wrapping
     * applies to string literals compared against {@code oracleTemporalColumns} — columns
     * whose remote type is temporal but which StarRocks maps to VARCHAR, so the constant
     * reaches the renderer as a plain string.
     */
    public static class OracleSQLRenderer extends ScalarOperatorToJDBCSQLVisitor {
        // ColumnRef -> "DATE" | "TIMESTAMP" for columns whose remote Oracle type is temporal.
        private final Map<ColumnRefOperator, String> temporalColumns;

        public OracleSQLRenderer(Map<ColumnRefOperator, String> columnNames) {
            super(columnNames);
            this.temporalColumns = Collections.emptyMap();
        }

        public OracleSQLRenderer(Map<ColumnRefOperator, String> columnNames,
                                 Map<ColumnRefOperator, String> temporalColumns) {
            super(columnNames);
            this.temporalColumns = temporalColumns;
        }

        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.ORACLE;
        }

        @Override
        public String visitConstant(ConstantOperator op, Void context) {
            if (!op.isNull() && op.getType().isDateType()) {
                String keyword = op.getType().isDatetime() ? "TIMESTAMP" : "DATE";
                return keyword + " '" + op.toString() + "'";
            }
            return super.visitConstant(op, context);
        }

        @Override
        public String visitCastOperator(CastOperator op, Void context) {
            // `dt = '2024-01-15'` on a DATE column reaches the renderer as
            // ImplicitCast<DATE>(ConstantOperator<VARCHAR>); the base class would strip
            // the cast and emit a bare string, which Oracle parses via NLS_DATE_FORMAT.
            if (op.isImplicit() && op.getType().isDateType()
                    && op.getChild(0) instanceof ConstantOperator) {
                ConstantOperator child = (ConstantOperator) op.getChild(0);
                if (!child.isNull()) {
                    String keyword = op.getType().isDatetime() ? "TIMESTAMP" : "DATE";
                    return keyword + " '" + child.toString().replace("'", "''") + "'";
                }
            }
            return super.visitCastOperator(op, context);
        }

        @Override
        public String visitBinaryPredicate(BinaryPredicateOperator op, Void context) {
            String keyword = temporalKeywordFor(op.getChild(0));
            String wrapped = renderTemporalLiteral(op.getChild(1), keyword);
            if (wrapped != null) {
                String left = op.getChild(0).accept(this, null);
                return "(" + left + " " + op.getBinaryType().toString() + " " + wrapped + ")";
            }
            return super.visitBinaryPredicate(op, context);
        }

        @Override
        public String visitBetweenPredicate(BetweenPredicateOperator op, Void context) {
            String keyword = temporalKeywordFor(op.getChild(0));
            String lower = renderTemporalLiteral(op.getChild(1), keyword);
            String upper = renderTemporalLiteral(op.getChild(2), keyword);
            if (lower != null && upper != null) {
                String col = op.getChild(0).accept(this, null);
                String betweenClause = op.isNotBetween() ? " NOT BETWEEN " : " BETWEEN ";
                return "(" + col + betweenClause + lower + " AND " + upper + ")";
            }
            return super.visitBetweenPredicate(op, context);
        }

        @Override
        public String visitInPredicate(InPredicateOperator op, Void context) {
            String keyword = temporalKeywordFor(op.getChild(0));
            if (keyword != null) {
                List<String> values = new ArrayList<>();
                for (int i = 1; i < op.getChildren().size(); i++) {
                    String wrapped = renderTemporalLiteral(op.getChild(i), keyword);
                    if (wrapped == null) {
                        return super.visitInPredicate(op, context);
                    }
                    values.add(wrapped);
                }
                String col = op.getChild(0).accept(this, null);
                String inClause = op.isNotIn() ? " NOT IN " : " IN ";
                return "(" + col + inClause + "(" + Joiner.on(", ").join(values) + "))";
            }
            return super.visitInPredicate(op, context);
        }

        private String temporalKeywordFor(ScalarOperator columnSide) {
            return columnSide instanceof ColumnRefOperator
                    ? temporalColumns.get((ColumnRefOperator) columnSide) : null;
        }

        private String renderTemporalLiteral(ScalarOperator value, String keyword) {
            if (keyword == null || !(value instanceof ConstantOperator)) {
                return null;
            }
            ConstantOperator constant = (ConstantOperator) value;
            if (constant.isNull() || !constant.getType().isStringType()) {
                return null;
            }
            // `keyword` only gates that the COLUMN is temporal; renderOracleTemporalLiteral then picks
            // the literal keyword from the literal's shape (see there). Following the column type would
            // emit DATE '2024-01-15 12:34:56' for a DATE column compared to a datetime string, which
            // Oracle rejects (ORA-01861).
            return renderOracleTemporalLiteral(constant.toString());
        }
    }

    /** CLICKHOUSE: base behavior is sufficient; {@code <=>} is accepted natively. */
    public static class ClickHouseSQLRenderer extends ScalarOperatorToJDBCSQLVisitor {
        public ClickHouseSQLRenderer(Map<ColumnRefOperator, String> columnNames) {
            super(columnNames);
        }

        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.CLICKHOUSE;
        }
    }

    /**
     * UNKNOWN: base behavior — the gate has already rejected anything dialect-specific
     * (boolean constants, {@code <=>}, {@code divide}/{@code mod}, {@code concat},
     * non-implicit casts), so this renderer only sees safe, dialect-agnostic nodes.
     */
    public static class UnknownSQLRenderer extends ScalarOperatorToJDBCSQLVisitor {
        public UnknownSQLRenderer(Map<ColumnRefOperator, String> columnNames) {
            super(columnNames);
        }

        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.UNKNOWN;
        }

        @Override
        protected boolean avgIntegerNeedsFloatCast() {
            // SQL Server (whose jdbc:sqlserver: URI resolves to UNKNOWN) integer-divides
            // AVG(<integer>); force a floating-point average so the result matches StarRocks.
            return true;
        }
    }
}
