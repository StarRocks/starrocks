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

import com.starrocks.catalog.JDBCTable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.Locale;
import java.util.Map;

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

    public static boolean canPushDown(ScalarOperator op, JDBCTable.ProtocolType dialect) {
        return op.accept(forDialect(dialect), null);
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

    /** MYSQL / MARIADB: base behavior plus {@code concat(...)}. */
    public static class MySQLLikePushDownGate extends CanPushDownPredicateVisitor {
        @Override
        protected JDBCTable.ProtocolType dialect() {
            return JDBCTable.ProtocolType.MYSQL;
        }

        @Override
        public Boolean visitCall(CallOperator op, Void ctx) {
            String fnName = op.getFnName().toLowerCase(Locale.ROOT);
            if ("concat".equals(fnName)) {
                return op.getChildren().size() >= 2 && allChildrenPushable(op, ctx);
            }
            return super.visitCall(op, ctx);
        }
    }

    /**
     * POSTGRES: rejects {@code divide} — the renderer strips implicit casts, so
     * {@code int / int} silently truncates on PG and diverges from StarRocks semantics.
     */
    public static class PostgresPushDownGate extends CanPushDownPredicateVisitor {
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
    }

    /**
     * ORACLE: no BOOLEAN type at the SQL layer, no {@code <=>}, no {@code %} operator
     * (the single-table scan path emits {@code mod} as {@code %}), and a 1000-item
     * limit on a single {@code IN (...)} list.
     */
    public static class OraclePushDownGate extends CanPushDownPredicateVisitor {
        private static final int IN_LIST_LIMIT = 1000;

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

        @Override
        public Boolean visitInPredicate(InPredicateOperator op, Void ctx) {
            // children = [LHS, item1, item2, ..., itemN]; Oracle rejects N > 1000.
            if (op.getChildren().size() - 1 > IN_LIST_LIMIT) {
                return false;
            }
            return super.visitInPredicate(op, ctx);
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
