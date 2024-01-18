// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
 * isNull ->IsNullPredicateOperator(..., false)
 * notNull ->IsNullPredicateOperator(..., true)
 * equal -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, ..., ...)
 * notEqual -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.NE, ..., ...)
 * lessThan -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, ..., ...)
 * lessThanOrEqual -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, ..., ...)
 * greaterThan -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, ..., ...)
 * greaterThanOrEqual -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, ..., ...)
 * in -> InPredicateOperator(..., ..., false)
 * notIn -> InPredicateOperator(..., ..., true)
 * startWith -> LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, ..., "prefix%")
 * <p>
 * Supported expression operations are:
 * and -> CompoundPredicateOperator(and, ..., ...)
 * or -> CompoundPredicateOperator(or, ..., ...)
 * not -> CompoundPredicateOperator(not, ...)
 * <p>
 */

public class ScalarOperatorToIcebergExpr {
    private static final Logger LOG = LogManager.getLogger(ScalarOperatorToIcebergExpr.class);

    public Expression convert(List<ScalarOperator> operators, IcebergContext context) {
        IcebergExprVisitor visitor = new IcebergExprVisitor();
        List<Expression> expressions = Lists.newArrayList();
        for (ScalarOperator operator : operators) {
            Expression filterExpr = operator.accept(visitor, context);
            if (filterExpr != null) {
                try {
                    Binder.bind(context.getSchema(), filterExpr, false);
                    expressions.add(filterExpr);
                } catch (ValidationException e) {
                    LOG.error("binding to the table schema failed, cannot be pushed down scanOperator: {}",
                            operator.debugString());
                }
            }
        }

        LOG.debug("Number of predicates pushed down / Total number of predicates: {}/{}",
                expressions.size(), operators.size());
        return expressions.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
    }

    public static class IcebergContext {
        private final Types.StructType schema;

        public IcebergContext(Types.StructType schema) {
            this.schema = schema;
        }

        public Types.StructType getSchema() {
            return schema;
        }
    }

    private static class IcebergExprVisitor extends ScalarOperatorVisitor<Expression, IcebergContext> {

        @Override
        public Expression visitCompoundPredicate(CompoundPredicateOperator operator, IcebergContext context) {
            CompoundPredicateOperator.CompoundType op = operator.getCompoundType();
            if (op == CompoundPredicateOperator.CompoundType.NOT) {
                if (operator.getChild(0) instanceof LikePredicateOperator) {
                    return null;
                }
                Expression expression = operator.getChild(0).accept(this, null);

                if (expression != null) {
                    return not(expression);
                }
            } else {
                Expression left = operator.getChild(0).accept(this, null);
                Expression right = operator.getChild(1).accept(this, null);
                if (left != null && right != null) {
                    return (op == CompoundPredicateOperator.CompoundType.OR) ? or(left, right) : and(left, right);
                }
            }
            return null;
        }

        @Override
        public Expression visitIsNullPredicate(IsNullPredicateOperator operator, IcebergContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }
            if (operator.isNotNull()) {
                return notNull(columnName);
            } else {
                return isNull(columnName);
            }
        }

        @Override
        public Expression visitBinaryPredicate(BinaryPredicateOperator operator, IcebergContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }

            Object literalValue = getLiteralValue(operator.getChild(1));
            if (context != null && context.getSchema().fieldType(columnName).typeId() == Type.TypeID.BOOLEAN) {
                literalValue = convertBoolLiteralValue(literalValue);
            }

            if (literalValue == null) {
                return null;
            }
            switch (operator.getBinaryType()) {
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
        public Expression visitInPredicate(InPredicateOperator operator, IcebergContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }

            List<Object> literalValues = operator.getListChildren().stream()
                    .map(childoperator -> {
                        Object literalValue = ScalarOperatorToIcebergExpr.getLiteralValue(childoperator);
                        if (context != null && context.getSchema().fieldType(columnName).typeId() == Type.TypeID.BOOLEAN) {
                            literalValue = convertBoolLiteralValue(literalValue);
                        }
                        return literalValue;
                    }).collect(Collectors.toList());

            if (operator.isNotIn()) {
                return notIn(columnName, literalValues);
            } else {
                return in(columnName, literalValues);
            }
        }

        @Override
        public Expression visitLikePredicateOperator(LikePredicateOperator operator, IcebergContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }

            if (operator.getLikeType() == LikePredicateOperator.LikeType.LIKE) {
                if (operator.getChild(1).getType().isStringType()) {
                    String literal = (String) getLiteralValue(operator.getChild(1));
                    if (literal.indexOf("%") == literal.length() - 1) {
                        return startsWith(columnName, literal.substring(0, literal.length() - 1));
                    }
                }
            }
            return null;
        }

        @Override
        public Expression visit(ScalarOperator scalarOperator, IcebergContext context) {
            return null;
        }
    }

    private static Object getLiteralValue(ScalarOperator operator) {
        if (operator == null) {
            return null;
        }

        return operator.accept(new ExtractLiteralValue(), null);
    }

    private static Object convertBoolLiteralValue(Object literalValue) {
        try {
            return new BoolLiteral(String.valueOf(literalValue)).getValue();
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to convert %s to boolean type", literalValue);
        }
    }

    private static class ExtractLiteralValue extends ScalarOperatorVisitor<Object, Void> {
        @Override
        public Object visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        @Override
        public Object visitConstant(ConstantOperator operator, Void context) {
            switch (operator.getType().getPrimitiveType()) {
                case BOOLEAN:
                    return operator.getBoolean();
                case TINYINT:
                    return operator.getTinyInt();
                case SMALLINT:
                    return operator.getSmallint();
                case INT:
                    return operator.getInt();
                case BIGINT:
                    return operator.getBigint();
                case FLOAT:
                    return operator.getFloat();
                case DOUBLE:
                    return operator.getDouble();
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return operator.getDecimal();
                case HLL:
                case VARCHAR:
                case CHAR:
                    return operator.getVarchar();
                case DATE:
                    return operator.getDate().toLocalDate().toEpochDay();
                case DATETIME:
                    long value = operator.getDatetime().toEpochSecond(OffsetDateTime.now().getOffset()) * 1000
                            * 1000 * 1000 + operator.getDatetime().getNano();
                    return TimeUnit.MICROSECONDS.convert(value, TimeUnit.NANOSECONDS);
                default:
                    return null;
            }
        }
    }

    private static String getColumnName(ScalarOperator operator) {
        if (operator == null) {
            return null;
        }

        String columnName = operator.accept(new ExtractColumnName(), null);
        if (columnName == null || columnName.isEmpty()) {
            return null;
        }
        return columnName;
    }

    private static class ExtractColumnName extends ScalarOperatorVisitor<String, Void> {

        @Override
        public String visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        public String visitVariableReference(ColumnRefOperator operator, Void context) {
            return operator.getName();
        }

        public String visitCastOperator(CastOperator operator, Void context) {
            return operator.getChild(0).accept(this, context);
        }
    }
}