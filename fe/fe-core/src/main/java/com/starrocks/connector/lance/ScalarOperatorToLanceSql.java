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

package com.starrocks.connector.lance;

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Translates a StarRocks {@link ScalarOperator} predicate tree into a Lance filter string in
 * DataFusion SQL dialect, which the native Lance reader accepts via {@code Scanner::filter(&str)}.
 *
 * <p>The translation is <b>partial and best-effort</b>: only operators whose DataFusion rendering
 * has identical semantics to StarRocks are emitted; anything unsupported yields {@code null} and
 * is left for StarRocks to evaluate post-decode (the residual predicate). Because StarRocks always
 * re-checks the full predicate, a filter that prunes too little is harmless while one that prunes
 * too much is impossible by construction.
 *
 * <p>Modeled on {@link com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr} (visitor
 * structure, {@code insideNot} invariant) and {@code com.starrocks.sql.ExpressionPrinter}
 * (SQL emission). v1 supports top-level columns and the scalar type gate only; nested fields,
 * LIKE, REGEXP, {@code <=>}, DECIMAL/TIME/LARGEINT fall through to the residual.
 */
public class ScalarOperatorToLanceSql {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATETIME_MICROS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    /**
     * Translates a conjunction of predicates into a Lance SQL filter string. Each conjunct is
     * converted independently; successfully converted conjuncts are AND-joined. Returns empty if
     * nothing is pushable (the caller then pushes no filter and relies on post-decode evaluation).
     */
    public Optional<String> translate(List<ScalarOperator> conjuncts) {
        LanceSqlVisitor visitor = new LanceSqlVisitor();
        List<String> pushed = new ArrayList<>();
        for (ScalarOperator conjunct : conjuncts) {
            String sql = conjunct.accept(visitor, new Context(false));
            if (sql != null) {
                pushed.add(sql);
            }
        }
        if (pushed.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.join(" AND ", pushed));
    }

    /** Visitor context; {@code insideNot} tracks whether we are under a logical NOT. */
    private record Context(boolean insideNot) {
        Context withInsideNot() {
            return new Context(true);
        }
    }

    private static class LanceSqlVisitor extends ScalarOperatorVisitor<String, Context> {

        @Override
        public String visit(ScalarOperator scalarOperator, Context context) {
            // Default: not pushable, leave as residual.
            return null;
        }

        @Override
        public String visitCompoundPredicate(CompoundPredicateOperator operator, Context context) {
            switch (operator.getCompoundType()) {
                case NOT:
                    return visitNot(operator, context);
                case AND:
                    return visitAnd(operator, context);
                case OR:
                    return visitOr(operator, context);
                default:
                    return null;
            }
        }

        private String visitNot(CompoundPredicateOperator operator, Context context) {
            String child = operator.getChild(0).accept(this, context.withInsideNot());
            return child == null ? null : "NOT (" + child + ")";
        }

        private String visitAnd(CompoundPredicateOperator operator, Context context) {
            String left = operator.getChild(0).accept(this, context);
            String right = operator.getChild(1).accept(this, context);
            if (left != null && right != null) {
                return "(" + left + " AND " + right + ")";
            }
            // Partial pushdown of AND is safe only outside a NOT: AND(a, b) is more restrictive
            // than either side, so pushing one side still correctly over-approximates. Inside a
            // NOT this would over-filter, since NOT(AND(a, b)) = OR(NOT a, NOT b).
            if (!context.insideNot()) {
                if (left != null) {
                    return left;
                }
                if (right != null) {
                    return right;
                }
            }
            return null;
        }

        private String visitOr(CompoundPredicateOperator operator, Context context) {
            String left = operator.getChild(0).accept(this, context);
            String right = operator.getChild(1).accept(this, context);
            // OR requires both sides to convert; a dropped disjunct would wrongly narrow the result.
            if (left == null || right == null) {
                return null;
            }
            return "(" + left + " OR " + right + ")";
        }

        @Override
        public String visitBinaryPredicate(BinaryPredicateOperator operator, Context context) {
            String op = binaryOperator(operator.getBinaryType());
            if (op == null) {
                return null;
            }
            String column = columnName(operator.getChild(0));
            String literal = formatLiteral(operator.getChild(1));
            if (column == null || literal == null) {
                return null;
            }
            return column + " " + op + " " + literal;
        }

        @Override
        public String visitIsNullPredicate(IsNullPredicateOperator operator, Context context) {
            String column = columnName(operator.getChild(0));
            if (column == null) {
                return null;
            }
            return column + (operator.isNotNull() ? " IS NOT NULL" : " IS NULL");
        }

        @Override
        public String visitInPredicate(InPredicateOperator operator, Context context) {
            String column = columnName(operator.getChild(0));
            if (column == null) {
                return null;
            }
            List<String> values = new ArrayList<>();
            for (ScalarOperator child : operator.getListChildren()) {
                String literal = formatLiteral(child);
                if (literal == null) {
                    return null;
                }
                values.add(literal);
            }
            if (values.isEmpty()) {
                return null;
            }
            String keyword = operator.isNotIn() ? " NOT IN (" : " IN (";
            return column + keyword + String.join(", ", values) + ")";
        }
    }

    /** Maps a StarRocks binary operator to its DataFusion SQL form, or null if not pushable. */
    private static String binaryOperator(BinaryType type) {
        switch (type) {
            case EQ:
                return "=";
            case NE:
                return "<>";
            case LT:
                return "<";
            case LE:
                return "<=";
            case GT:
                return ">";
            case GE:
                return ">=";
            default:
                // EQ_FOR_NULL (<=>) has no safe DataFusion equivalent here.
                return null;
        }
    }

    /** Resolves a top-level column name, stripping only identity casts; null otherwise. */
    private static String columnName(ScalarOperator operator) {
        if (operator instanceof ColumnRefOperator) {
            return quoteIdentifier(((ColumnRefOperator) operator).getName());
        }
        if (operator instanceof CastOperator) {
            // A non-identity cast changes semantics and must stay in the residual predicate.
            ScalarOperator child = operator.getChild(0);
            if (operator.getType().equals(child.getType())) {
                return columnName(child);
            }
        }
        return null;
    }

    /** Formats a constant operand as a DataFusion SQL literal, or null if unsupported. */
    private static String formatLiteral(ScalarOperator operator) {
        if (!(operator instanceof ConstantOperator)) {
            return null;
        }
        ConstantOperator constant = (ConstantOperator) operator;
        if (constant.isNull()) {
            return null;
        }
        switch (constant.getType().getPrimitiveType()) {
            case BOOLEAN:
                return constant.getBoolean() ? "TRUE" : "FALSE";
            case TINYINT:
                return Byte.toString(constant.getTinyInt());
            case SMALLINT:
                return Short.toString(constant.getSmallint());
            case INT:
                return Integer.toString(constant.getInt());
            case BIGINT:
                return Long.toString(constant.getBigint());
            case FLOAT:
                return Double.toString(constant.getFloat());
            case DOUBLE:
                return Double.toString(constant.getDouble());
            case CHAR:
            case VARCHAR:
                return quoteString(constant.getVarchar());
            case DATE:
                return "DATE '" + constant.getDate().format(DATE_FORMAT) + "'";
            case DATETIME:
                return "TIMESTAMP '" + formatDatetime(constant.getDatetime()) + "'";
            default:
                // DECIMAL / TIME / LARGEINT / BINARY etc. are not pushed in v1.
                return null;
        }
    }

    private static String formatDatetime(LocalDateTime value) {
        return value.getNano() == 0 ? value.format(DATETIME_FORMAT) : value.format(DATETIME_MICROS_FORMAT);
    }

    /** Single-quotes a string literal, escaping embedded single quotes by doubling. */
    private static String quoteString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** Emits a bare identifier when safe, otherwise a double-quoted (and escaped) identifier. */
    private static String quoteIdentifier(String name) {
        if (name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return name;
        }
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
