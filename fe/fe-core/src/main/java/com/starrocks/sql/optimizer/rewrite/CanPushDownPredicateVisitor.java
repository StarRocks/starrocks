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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
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
import com.starrocks.type.ArrayType;
import com.starrocks.type.Type;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Whether a {@link ScalarOperator} expression can be fully rendered as a simple
 * external-database SQL fragment. The base class handles the dialect-agnostic shape
 * (column refs, compound / IS NULL / BETWEEN children, plain arithmetic via
 * {@link #BINARY_INFIX_FUNCTIONS}); each per-dialect subclass adds or removes
 * capabilities by overriding individual visit methods.
 */
public abstract class CanPushDownPredicateVisitor extends ScalarOperatorVisitor<Boolean, Void> {

    /**
     * StarRocks function names that map to a plain SQL infix operator. Shared with the
     * JDBC SQL renderer so push-down gating and SQL emission stay in sync.
     */
    public static final Map<String, String> BINARY_INFIX_FUNCTIONS = Map.of(
            "add", "+",
            "subtract", "-",
            "multiply", "*",
            "divide", "/",
            "mod", "%"
    );

    /**
     * Aggregate functions the JDBC pushdown renders remotely. Accepted as leaves only inside a
     * HAVING predicate (see {@link #canPushDownHaving}); the single source of truth for which
     * aggregates {@code PushDownAggToJDBCScanRule} folds into the remote SELECT.
     */
    public static final Set<String> PUSHABLE_AGGREGATE_FUNCTIONS = Set.of(
            FunctionSet.COUNT,
            FunctionSet.SUM,
            FunctionSet.MIN,
            FunctionSet.MAX,
            FunctionSet.AVG);

    // Whether aggregate calls (PUSHABLE_AGGREGATE_FUNCTIONS) count as pushable leaves. Off for WHERE
    // and projection predicates (which never contain aggregates); on for a HAVING predicate, whose
    // aggregate references were already validated and folded into the remote SELECT.
    private boolean allowAggregateCalls = false;

    // Max items allowed in a pushed-down literal IN list, from the session variable
    // jdbc_predicate_pushdown_max_in_list_size: -1 = no limit; 0 = never push an IN; N > 0 = cap at N.
    private int maxInListSize = -1;

    // Scan columns a pushed ordering comparison returns the local answer for -- either because the
    // renderer will name COLLATE "C" on them, or because the remote type's own order already
    // matches StarRocks' byte order; see PostgresCollation.orderSafeColumns, which is what callers
    // pass. Only the PostgreSQL gate reads it, and only an ordering comparison (< <= > >=) over a
    // string consults it -- equality compares equal exactly when the bytes do.
    private Set<ColumnRefOperator> orderSafeColumns = Collections.emptySet();

    // enable_jdbc_array_subscript_push_down, read once per verdict so that every path asking this
    // gate about one statement gets the same answer. Only the PostgreSQL gate consults it, in
    // visitCollectionElement; no other dialect calls a subscript pushable in the first place.
    protected boolean arraySubscriptPushDown = true;

    public static boolean canPushDown(ScalarOperator op, JDBCTable.ProtocolType dialect,
                                      Set<ColumnRefOperator> orderSafeColumns) {
        return accept(op, dialect, false, sessionMaxInListSize(), orderSafeColumns);
    }

    /** Overload with an explicit IN-list cap (0 = unlimited), bypassing the session variable. */
    public static boolean canPushDown(ScalarOperator op, JDBCTable.ProtocolType dialect, int maxInListSize,
                                      Set<ColumnRefOperator> orderSafeColumns) {
        return accept(op, dialect, false, maxInListSize, orderSafeColumns);
    }

    /**
     * As {@link #canPushDown}, but also accepts the JDBC-pushable aggregate calls
     * ({@link #PUSHABLE_AGGREGATE_FUNCTIONS}) as leaves — for vetting a HAVING predicate that
     * references aggregates already pushed into the remote {@code SELECT} (e.g.
     * {@code HAVING MAX(c) > 5}).
     */
    public static boolean canPushDownHaving(ScalarOperator op, JDBCTable.ProtocolType dialect,
                                            Set<ColumnRefOperator> orderSafeColumns) {
        return accept(op, dialect, true, sessionMaxInListSize(), orderSafeColumns);
    }

    private static boolean accept(ScalarOperator op, JDBCTable.ProtocolType dialect,
                                  boolean allowAggregateCalls, int maxInListSize,
                                  Set<ColumnRefOperator> orderSafeColumns) {
        CanPushDownPredicateVisitor gate = forDialect(dialect);
        gate.allowAggregateCalls = allowAggregateCalls;
        gate.maxInListSize = maxInListSize;
        gate.orderSafeColumns = orderSafeColumns == null ? Collections.emptySet() : orderSafeColumns;
        gate.arraySubscriptPushDown = sessionArraySubscriptPushDown();
        return op.accept(gate, null);
    }

    /**
     * Whether an ordering comparison ({@code < <= > >=}) over these operands returns, remotely, the
     * answer StarRocks would have produced locally. Operands that are not strings order the same
     * way on both sides already, so they pass; every string operand has to be one the remote order
     * is known for (see {@link #operandOrdersSafely}) or a literal, and at least one non-literal has
     * to be present -- a comparison of two literals would be evaluated under the database's own
     * collation.
     */
    protected boolean orderingComparisonIsOrderSafe(ScalarOperator op) {
        boolean comparesStrings = false;
        boolean collatesAnOperand = false;
        for (ScalarOperator child : op.getChildren()) {
            if (!child.getType().isStringType()) {
                continue;
            }
            comparesStrings = true;
            if (child instanceof ConstantOperator) {
                continue;
            }
            if (!operandOrdersSafely(child)) {
                return false;
            }
            collatesAnOperand = true;
        }
        return !comparesStrings || collatesAnOperand;
    }

    /**
     * Whether this operand's comparison order is one both sides agree on. A column qualifies when
     * {@link PostgresCollation} vouched for it, by either of the two routes that set vouches for:
     * the renderer will name {@code COLLATE "C"} on it, or the remote type orders like StarRocks'
     * bytes on its own and must not be given a collation at all ({@code uuid}).
     *
     * <p>A MIN/MAX over such a column qualifies too: the renderer collates the aggregate's
     * argument and PostgreSQL derives the aggregate's result collation from it, so the comparison
     * inherits the explicit collation without needing one of its own. The argument still has to be
     * vouched for -- MIN/MAX over a bpchar or over a numeric that merely maps to VARCHAR is exactly
     * the case this gate exists to keep local. Only {@code canPushDownHaving} can reach the
     * aggregate branch, since {@link #visitCall} rejects aggregates everywhere else.
     *
     * <p>The aggregate branch does not have to re-ask whether the argument is one PostgreSQL will
     * aggregate at all: a uuid column is order-safe but has no remote {@code min}/{@code max}, and
     * PushDownAggToJDBCScanRule refuses that aggregate outright (see its
     * {@code PostgresCollation.minMaxPushableColumns} check), abandoning the whole aggregate
     * push-down, so no HAVING over one ever reaches this gate.
     */
    private boolean operandOrdersSafely(ScalarOperator operand) {
        if (operand instanceof ColumnRefOperator) {
            return orderSafeColumns.contains(operand);
        }
        if (allowAggregateCalls && operand instanceof CallOperator) {
            CallOperator call = (CallOperator) operand;
            String fnName = call.getFnName().toLowerCase(Locale.ROOT);
            if ((FunctionSet.MIN.equals(fnName) || FunctionSet.MAX.equals(fnName))
                    && call.getChildren().size() == 1) {
                return operandOrdersSafely(call.getChild(0));
            }
        }
        return false;
    }


    private static int sessionMaxInListSize() {
        ConnectContext ctx = ConnectContext.get();
        return ctx == null ? -1 : ctx.getSessionVariable().getJdbcPredicatePushdownMaxInListSize();
    }

    /**
     * Whether {@code enable_jdbc_array_subscript_push_down} is on for the statement being planned.
     * Falls back to the variable's own default (on) when there is no session, so that a planning
     * path without a ConnectContext behaves like an ordinary session rather than like one that
     * rolled the feature back.
     */
    private static boolean sessionArraySubscriptPushDown() {
        return ConnectContext.getSessionVariableOrDefault().isEnableJdbcArraySubscriptPushDown();
    }

    public static CanPushDownPredicateVisitor forDialect(JDBCTable.ProtocolType dialect) {
        switch (dialect) {
            case MYSQL:
            case MARIADB:
                return new MySQLLikePushDownGate();
            case POSTGRES:
                return new PostgresPushDownGate();
            case ORACLE:
                return new OraclePushDownGate();
            case CLICKHOUSE:
                return new ClickHousePushDownGate();
            case UNKNOWN:
            default:
                return new UnknownPushDownGate();
        }
    }

    protected abstract JDBCTable.ProtocolType dialect();

    /**
     * The value of an array subscript that is a plain integer literal, or empty when it is anything
     * else (a variable, NULL, or a LARGEINT whose value would not survive the narrowing). Shared with
     * the JDBC SQL renderer so the gate and the emitted SQL agree on which subscripts are literals
     * and on what each one reads as.
     */
    public static OptionalLong constantSubscript(ScalarOperator subscript) {
        if (!(subscript instanceof ConstantOperator)) {
            return OptionalLong.empty();
        }
        ConstantOperator constant = (ConstantOperator) subscript;
        if (constant.isNull() || !constant.getType().isIntegerType()
                || !(constant.getValue() instanceof Number)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(((Number) constant.getValue()).longValue());
    }

    @Override
    public Boolean visit(ScalarOperator op, Void ctx) {
        return false;
    }

    protected Boolean allChildrenPushable(ScalarOperator op, Void ctx) {
        for (ScalarOperator child : op.getChildren()) {
            if (!child.accept(this, ctx)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visitVariableReference(ColumnRefOperator op, Void ctx) {
        return true;
    }

    @Override
    public Boolean visitConstant(ConstantOperator op, Void ctx) {
        return true;
    }

    @Override
    public Boolean visitCall(CallOperator op, Void ctx) {
        String fnName = op.getFnName().toLowerCase(Locale.ROOT);
        int arity = op.getChildren().size();
        if (BINARY_INFIX_FUNCTIONS.containsKey(fnName)) {
            return arity == 2 && allChildrenPushable(op, ctx);
        }
        if (allowAggregateCalls && PUSHABLE_AGGREGATE_FUNCTIONS.contains(fnName)) {
            // A HAVING predicate may reference the aggregates already folded into the remote SELECT
            // (e.g. MAX(c) in `HAVING MAX(c) > 5`); their arguments were vetted by the pushdown rule.
            return allChildrenPushable(op, ctx);
        }
        return false;
    }

    @Override
    public Boolean visitCastOperator(CastOperator op, Void ctx) {
        if (!op.isImplicit() && JDBCCastTypeMapper.renderCastType(op.getType(), dialect()).isEmpty()) {
            return false;
        }
        return allChildrenPushable(op, ctx);
    }

    @Override
    public Boolean visitBinaryPredicate(BinaryPredicateOperator op, Void ctx) {
        return allChildrenPushable(op, ctx);
    }

    @Override
    public Boolean visitCompoundPredicate(CompoundPredicateOperator op, Void ctx) {
        return allChildrenPushable(op, ctx);
    }

    @Override
    public Boolean visitInPredicate(InPredicateOperator op, Void ctx) {
        // children = [LHS, item1, ..., itemN]. maxInListSize: -1 = no limit; 0 = never push an IN;
        // N > 0 = push only when the list has at most N items. Oversized lists stay local.
        if (maxInListSize >= 0 && op.getChildren().size() - 1 > maxInListSize) {
            return false;
        }
        return allChildrenPushable(op, ctx);
    }

    @Override
    public Boolean visitIsNullPredicate(IsNullPredicateOperator op, Void ctx) {
        return allChildrenPushable(op, ctx);
    }

    @Override
    public Boolean visitBetweenPredicate(BetweenPredicateOperator op, Void ctx) {
        return allChildrenPushable(op, ctx);
    }

    /**
     * MYSQL / MARIADB: base behavior plus {@code concat(...)}, minus {@code divide}. MySQL/MariaDB
     * evaluate {@code /} as DECIMAL whose scale is bounded by {@code div_precision_increment}
     * (default 4), which diverges from StarRocks' DOUBLE division (e.g. {@code 10/3} renders as
     * {@code 3.3333} remotely vs {@code 3.3333333...} locally), so {@code a / b} is kept local on
     * both the filter and projection paths -- mirroring the Postgres/Unknown gates.
     */
    public static class MySQLLikePushDownGate extends CanPushDownPredicateVisitor {
        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.MYSQL;
        }

        @Override
        public Boolean visitCall(CallOperator op, Void ctx) {
            String fnName = op.getFnName().toLowerCase(Locale.ROOT);
            if ("divide".equals(fnName)) {
                return false;
            }
            if ("concat".equals(fnName)) {
                return op.getChildren().size() >= 2 && allChildrenPushable(op, ctx);
            }
            return super.visitCall(op, ctx);
        }
    }

    /**
     * POSTGRES: rejects {@code divide} — the renderer strips implicit casts, so
     * {@code int / int} silently truncates on PG and diverges from StarRocks semantics. Also keeps
     * a string {@code < <= > >=} or BETWEEN local unless the renderer can put {@code COLLATE "C"}
     * on every string operand: PostgreSQL would otherwise answer under the column's own collation,
     * which orders 'B' before 'a' in a linguistic locale and returns different rows than StarRocks'
     * byte order. Equality and IN are unaffected — a deterministic collation compares equal exactly
     * when the bytes are equal — and keep whatever index the column already has.
     */
    public static class PostgresPushDownGate extends CanPushDownPredicateVisitor {
        @Override
        public Boolean visitVariableReference(ColumnRefOperator op, Void ctx) {
            // Reading rebases PostgreSQL array bounds to 1 and keeps no dimension information, while
            // PostgreSQL compares whole arrays taking both into account. A bare array column therefore
            // stays local wherever it is compared, joined, grouped or ordered. Indexing is the one
            // exception and it is opened separately, with the bound correction, in
            // visitCollectionElement below -- not by relaxing this.
            return !op.getType().isArrayType();
        }

        @Override
        public Boolean visitConstant(ConstantOperator op, Void ctx) {
            return !op.getType().isArrayType();
        }

        /**
         * A constant subscript over an array column ({@code a[1]}), the one shape that reads an array
         * yet is safe remotely: the result is a scalar element, so none of the whole-array comparison
         * hazards that keep {@link #visitVariableReference} shut apply to it. Deliberately does NOT
         * call {@link #allChildrenPushable} -- that would ask the array child about itself and this
         * gate's own override would veto it. Opening one composite shape is not the same as opening
         * the array type: {@code a = b}, {@code a < b} and {@code ORDER BY a} stay local as before.
         *
         * <p>Accepted only when every part is nailed down:
         * <ul>
         *   <li>the collection is a bare column reference of array type -- the renderer emits it
         *       twice (once inside {@code array_lower}), so it has to be side-effect free and cheap;
         *   <li>its element type is scalar -- a nested array would make the subscript itself an
         *       array, which is the whole-array comparison case again, and matches the reader, which
         *       only maps one-dimensional {@code _text}/{@code _varchar} columns;
         *   <li>the subscript is a non-null integer constant. A variable subscript would need the
         *       remote query to materialise the index in an inner derived table first, which buys
         *       nothing for the constant-subscript workload this exists for;
         *   <li>both {@code k} and the rendered offset {@code k - 1} fit in int4. PostgreSQL raises
         *       "integer out of range" for a wider subscript where StarRocks answers NULL. The
         *       renderer closes the rest of that gap: with
         *       {@code enable_jdbc_array_lower_bound_correction} on, the bound correction is int4
         *       arithmetic inside PostgreSQL, so even an in-range {@code k} can overflow once a
         *       row's own lower bound is added to it, and the emitted SQL guards the subscript on
         *       the array's length.
         * </ul>
         *
         * <p>Two session variables sit on this shape, and they are deliberately read in two
         * different places, because they decide two different things:
         * <ul>
         *   <li>{@code enable_jdbc_array_subscript_push_down} decides <b>whether the shape is
         *       pushable at all</b>, so it is read here, in the gate. Off, this method answers
         *       false and the subscript is unpushable like any other shape this gate refuses --
         *       which shuts both paths that could send it, since both ask this same gate: the scan
         *       filter ({@code PushDownPredicateToExternalTableScanRule}) leaves the predicate
         *       local, and {@code PushDownProjectToJDBCScanRule} declines to fold the projection.
         *       The query then reads the array column and takes the subscript in StarRocks. Putting
         *       it anywhere else -- in the renderer, say -- would close only the path that happens
         *       to run through there and leave the other one pushing.
         *   <li>{@code enable_jdbc_array_lower_bound_correction} decides <b>how an already-pushable
         *       subscript is spelled</b> -- plain {@code a[k]} or corrected against
         *       {@code array_lower} -- so it is read in the renderer and <b>not here</b>. The set
         *       of shapes that reach the renderer is the same under either of its values,
         *       including the int4 bound above, which is kept even though the uncorrected rendering
         *       does no arithmetic, so that turning the correction on can never turn a pushed-down
         *       expression into an error.
         * </ul>
         *
         * <p>The two compose in one direction only: with the push-down off, the correction variable
         * has nothing left to govern, because no subscript reaches the renderer.
         */
        @Override
        public Boolean visitCollectionElement(CollectionElementOperator op, Void ctx) {
            if (!arraySubscriptPushDown) {
                return false;
            }
            if (op.getChildren().size() != 2) {
                return false;
            }
            ScalarOperator collection = op.getChild(0);
            if (!(collection instanceof ColumnRefOperator) || !collection.getType().isArrayType()) {
                return false;
            }
            Type itemType = ((ArrayType) collection.getType()).getItemType();
            if (itemType == null || !itemType.isScalarType()) {
                return false;
            }
            OptionalLong index = constantSubscript(op.getChild(1));
            return index.isPresent()
                    && index.getAsLong() > Integer.MIN_VALUE && index.getAsLong() <= Integer.MAX_VALUE;
        }

        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.POSTGRES;
        }

        @Override
        public Boolean visitCall(CallOperator op, Void ctx) {
            String fnName = op.getFnName().toLowerCase(Locale.ROOT);
            if ("divide".equals(fnName)) {
                return false;
            }
            return super.visitCall(op, ctx);
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator op, Void ctx) {
            if (op.getBinaryType().isRange() && !orderingComparisonIsOrderSafe(op)) {
                return false;
            }
            return super.visitBinaryPredicate(op, ctx);
        }

        @Override
        public Boolean visitBetweenPredicate(BetweenPredicateOperator op, Void ctx) {
            // The optimizer usually rewrites BETWEEN into two comparisons before a scan predicate
            // reaches here, but the operator survives on some paths and orders the same way.
            if (!orderingComparisonIsOrderSafe(op)) {
                return false;
            }
            return super.visitBetweenPredicate(op, ctx);
        }
    }

    /**
     * ORACLE: no BOOLEAN type at the SQL layer, no {@code <=>}, and no {@code %} operator
     * (the single-table scan path emits {@code mod} as {@code %}). The literal {@code IN (...)}
     * list size is governed by {@code jdbc_predicate_pushdown_max_in_list_size}, like every dialect
     * (Oracle's ORA-01795 limit is version-specific, so it is not hardcoded here).
     */
    public static class OraclePushDownGate extends CanPushDownPredicateVisitor {

        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.ORACLE;
        }

        @Override
        public Boolean visitConstant(ConstantOperator op, Void ctx) {
            if (op.getType().isBoolean()) {
                return false;
            }
            return super.visitConstant(op, ctx);
        }

        @Override
        public Boolean visitCall(CallOperator op, Void ctx) {
            String fnName = op.getFnName().toLowerCase(Locale.ROOT);
            if ("mod".equals(fnName)) {
                return false;
            }
            return super.visitCall(op, ctx);
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator op, Void ctx) {
            if (op.getBinaryType() == BinaryType.EQ_FOR_NULL) {
                return false;
            }
            return super.visitBinaryPredicate(op, ctx);
        }

    }

    /** CLICKHOUSE: base behavior is sufficient; {@code <=>} is accepted natively. */
    public static class ClickHousePushDownGate extends CanPushDownPredicateVisitor {
        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.CLICKHOUSE;
        }
    }

    /**
     * UNKNOWN: most conservative — the union of dialect-specific restrictions, so an
     * unrecognised JDBC catalog never receives dialect-specific syntax.
     */
    public static class UnknownPushDownGate extends CanPushDownPredicateVisitor {
        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.UNKNOWN;
        }

        @Override
        public Boolean visitConstant(ConstantOperator op, Void ctx) {
            if (op.getType().isBoolean()) {
                return false;
            }
            return super.visitConstant(op, ctx);
        }

        @Override
        public Boolean visitCall(CallOperator op, Void ctx) {
            String fnName = op.getFnName().toLowerCase(Locale.ROOT);
            if ("divide".equals(fnName) || "mod".equals(fnName)) {
                return false;
            }
            return super.visitCall(op, ctx);
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator op, Void ctx) {
            if (op.getBinaryType() == BinaryType.EQ_FOR_NULL) {
                return false;
            }
            return super.visitBinaryPredicate(op, ctx);
        }
    }
}
