// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.iceberg;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.iceberg.expressions.Expression;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;

/**
 * Expressions currently supported in Iceberg, maps to StarRocks:
 * <p>
 * Supported predicate expressions are:
 * isNull ->IsNullPredicate(..., false)
 * notNull ->IsNullPredicate(..., true)
 * equal -> BinaryPredicate(BinaryPredicate.Operator.EQ, ..., ...)
 * notEqual -> BinaryPredicate(BinaryPredicate.Operator.NE, ..., ...)
 * lessThan -> BinaryPredicate(BinaryPredicate.Operator.LT, ..., ...)
 * lessThanOrEqual -> BinaryPredicate(BinaryPredicate.Operator.LE, ..., ...)
 * greaterThan -> BinaryPredicate(BinaryPredicate.Operator.GT, ..., ...)
 * greaterThanOrEqual -> BinaryPredicate(BinaryPredicate.Operator.GE, ..., ...)
 * in -> InPredicate(..., ..., false)
 * notIn -> InPredicate(..., ..., true)
 * startsWith -> FunctionCallExpr("starts_with", ...) or LikePredicate(Operator.LIKE, ..., "prefix%")
 * <p>
 * Supported expression operations are:
 * and -> CompoundPredicate(and, ..., ...)
 * or -> CompoundPredicate(or, ..., ...)
 * not -> CompoundPredicate(not, ...)
 * <p>
 * Constant expressions are(Automatically merged in StarRocks, can be ignored):
 * alwaysTrue
 * alwaysFalse
 */
public class ExpressionConverter extends AstVisitor<Expression, Void> {

    public Expression convert(Expr expr) {
        if (expr == null) {
            return null;
        }
        return visit(expr);
    }

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate node, Void context) {
        CompoundPredicate.Operator op = node.getOp();
        if (op == CompoundPredicate.Operator.NOT) {
            if (node.getChild(0) instanceof FunctionCallExpr ||
                    node.getChild(0) instanceof LikePredicate) {
                // TODO: No negation for operation: STARTS_WITH in the Apache Iceberg 0.12.1
                return null;
            }
            Expression expression = node.getChild(0).accept(this, null);
            if (expression != null) {
                return not(expression);
            }
        } else {
            Expression left = node.getChild(0).accept(this, null);
            Expression right = node.getChild(1).accept(this, null);
            if (left != null && right != null) {
                return (op == CompoundPredicate.Operator.OR) ? or(left, right) : and(left, right);
            }
        }
        return null;
    }

    @Override
    public Expression visitIsNullPredicate(IsNullPredicate node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }
        if (node.isNotNull()) {
            return notNull(columnName);
        } else {
            return isNull(columnName);
        }
    }

    @Override
    public Expression visitBinaryPredicate(BinaryPredicate node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }
        Object literalValue = getLiteralValue(node.getChild(1));
        if (literalValue == null) {
            return null;
        }
        switch (node.getOp()) {
            case LT:
                return lessThan(columnName, literalValue);
            case LE:
                return lessThanOrEqual(columnName, literalValue);
            case GT:
                return greaterThan(columnName, literalValue);
            case GE:
                return greaterThanOrEqual(columnName, literalValue);
            case EQ:
                return equal(columnName, literalValue);
            case NE:
                return notEqual(columnName, literalValue);
            default:
                return null;
        }
    }

    @Override
    public Expression visitInPredicate(InPredicate node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }
        List<Expr> valuesExprList = node.getListChildren();
        List<Object> literalValues = new ArrayList<>(valuesExprList.size());
        for (Expr valueExpr : valuesExprList) {
            Object value = getLiteralValue(valueExpr);
            if (value == null) {
                return null;
            }
            literalValues.add(value);
        }
        if (node.isNotIn()) {
            return notIn(columnName, literalValues);
        } else {
            return in(columnName, literalValues);
        }
    }

    @Override
    public Expression visitFunctionCall(FunctionCallExpr node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }
        if (!node.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STARTS_WITH)) {
            return null;
        }
        if (node.getChild(1) instanceof StringLiteral) {
            StringLiteral stringLiteral = (StringLiteral) node.getChild(1);
            return startsWith(columnName, stringLiteral.getStringValue());
        }
        return null;
    }

    @Override
    public Expression visitLikePredicate(LikePredicate node, Void context) {
        String columnName = getColumnName(node.getChild(0));
        if (columnName == null) {
            return null;
        }
        if (node.getOp().equals(LikePredicate.Operator.LIKE)) {
            if (node.getChild(1) instanceof StringLiteral) {
                StringLiteral stringLiteral = (StringLiteral) node.getChild(1);
                String literal = stringLiteral.getUnescapedValue();
                if (literal.indexOf("%") == literal.length() - 1) {
                    return startsWith(columnName, literal.substring(0, literal.length() - 1));
                }
            }
        }
        return null;
    }

    private static Object getLiteralValue(Expr expr) {
        if (expr == null || !(expr instanceof LiteralExpr)) {
            return null;
        }
        LiteralExpr literalExpr = (LiteralExpr) expr;
        switch (literalExpr.getType().getPrimitiveType()) {
            case BOOLEAN:
                return ((BoolLiteral) literalExpr).getValue();
            case TINYINT:
            case SMALLINT:
            case INT:
                return (int) ((IntLiteral) literalExpr).getValue();
            case BIGINT:
                return ((IntLiteral) literalExpr).getValue();
            case FLOAT:
                return (float) ((FloatLiteral) literalExpr).getValue();
            case DOUBLE:
                return ((FloatLiteral) literalExpr).getValue();
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return ((DecimalLiteral) literalExpr).getValue();
            case HLL:
            case VARCHAR:
            case CHAR:
                return ((StringLiteral) literalExpr).getUnescapedValue();
            case DATE:
                return Math.toIntExact(((DateLiteral) literalExpr).toLocalDateTime().toLocalDate().toEpochDay());
            case DATETIME:
                // TODO: get more precision epoch microseconds
                long value = ((DateLiteral) literalExpr).toLocalDateTime()
                        .toEpochSecond(OffsetDateTime.now().getOffset());
                return TimeUnit.MICROSECONDS.convert(value, TimeUnit.SECONDS);
            default:
                return null;
        }
    }

    private static String getColumnName(Expr expr) {
        if (expr == null) {
            return null;
        }
        String columnName = new ExtractColumnName().visit(expr, null);
        if (columnName == null || columnName.isEmpty()) {
            return null;
        }
        return columnName;
    }

    private static class ExtractColumnName extends AstVisitor<String, Void> {
        @Override
        public String visitCastExpr(CastExpr node, Void context) {
            return node.getChild(0).accept(this, null);
        }

        @Override
        public String visitSlot(SlotRef node, Void context) {
            return node.getColumn().getName();
        }
    }
}