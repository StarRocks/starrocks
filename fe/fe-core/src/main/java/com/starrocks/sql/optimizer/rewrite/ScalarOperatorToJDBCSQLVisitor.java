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

import com.google.common.base.Joiner;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Render a {@link ScalarOperator} tree into a JDBC-dialect SQL fragment. The base class
 * handles the dialect-agnostic shape; each per-dialect subclass overrides only the visit
 * methods where the emitted SQL diverges. The caller is expected to have gated the
 * expression through {@link CanPushDownPredicateVisitor#canPushDown} for the same dialect.
 */
public abstract class ScalarOperatorToJDBCSQLVisitor extends ScalarOperatorVisitor<String, Void> {

    private final Map<ColumnRefOperator, String> columnNames;

    protected ScalarOperatorToJDBCSQLVisitor(Map<ColumnRefOperator, String> columnNames) {
        this.columnNames = columnNames;
    }

    public static ScalarOperatorToJDBCSQLVisitor forDialect(Map<ColumnRefOperator, String> columnNames,
                                                            JDBCTable.ProtocolType dialect) {
        switch (dialect) {
            case MYSQL:
            case MARIADB:
                return new MySQLLikeSQLRenderer(columnNames);
            case POSTGRES:
                return new PostgresSQLRenderer(columnNames);
            case ORACLE:
                return new OracleSQLRenderer(columnNames);
            case CLICKHOUSE:
                return new ClickHouseSQLRenderer(columnNames);
            case UNKNOWN:
            default:
                return new UnknownSQLRenderer(columnNames);
        }
    }

    protected abstract JDBCTable.ProtocolType dialect();

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
        // Fallback for unknown functions — shouldn't reach here if CanPushDownPredicateVisitor was checked
        return op.toString();
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
        public PostgresSQLRenderer(Map<ColumnRefOperator, String> columnNames) {
            super(columnNames);
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
            return super.visitBinaryPredicate(op, context);
        }
    }

    /**
     * ORACLE: DATE/DATETIME literals are wrapped in ANSI {@code DATE '...'} /
     * {@code TIMESTAMP '...'} to avoid NLS_DATE_FORMAT-dependent parsing.
     */
    public static class OracleSQLRenderer extends ScalarOperatorToJDBCSQLVisitor {
        public OracleSQLRenderer(Map<ColumnRefOperator, String> columnNames) {
            super(columnNames);
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
    }
}
