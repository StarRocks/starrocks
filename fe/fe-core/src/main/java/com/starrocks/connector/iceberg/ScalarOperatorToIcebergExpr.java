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


package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.catalog.PrimitiveType;
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
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
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

        private static Type.TypeID getResultTypeID(String columnName, IcebergContext context) {
            Preconditions.checkNotNull(context);
            return getColumnType(columnName, context).typeId();
        }

        private static Type getColumnType(String qualifiedName, IcebergContext context) {
            String[] paths = qualifiedName.split("\\.");
            Type type = context.getSchema();
            for (String path : paths) {
                type = type.asStructType().fieldType(path);
            }
            return type;
        }

        @Override
        public Expression visitCompoundPredicate(CompoundPredicateOperator operator, IcebergContext context) {
            CompoundPredicateOperator.CompoundType op = operator.getCompoundType();
            if (op == CompoundPredicateOperator.CompoundType.NOT) {
                if (operator.getChild(0) instanceof LikePredicateOperator) {
                    return null;
                }
                Expression expression = operator.getChild(0).accept(this, context);

                if (expression != null) {
                    return not(expression);
                }
            } else {
                Expression left = operator.getChild(0).accept(this, context);
                Expression right = operator.getChild(1).accept(this, context);
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

            Type.TypeID typeID = getResultTypeID(columnName, context);
            Object literalValue = getLiteralValue(operator.getChild(1), typeID);
            if (literalValue == null) {
                return null;
            }
            if (typeID == Type.TypeID.BOOLEAN) {
                literalValue = convertBoolLiteralValue(literalValue);
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
                        Type.TypeID typeID = getResultTypeID(columnName, context);
                        Object literalValue = ScalarOperatorToIcebergExpr.getLiteralValue(childoperator, typeID);
                        if (typeID == Type.TypeID.BOOLEAN) {
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
                    String literal = (String) getLiteralValue(operator.getChild(1), getResultTypeID(columnName, context));
                    if (literal == null) {
                        return null;
                    }
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

    private static Object getLiteralValue(ScalarOperator operator, Type.TypeID resultTypeID) {
        if (operator == null) {
            return null;
        }

        return operator.accept(new ExtractLiteralValue(), resultTypeID);
    }

    private static Object convertBoolLiteralValue(Object literalValue) {
        try {
            return new BoolLiteral(String.valueOf(literalValue)).getValue();
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to convert %s to boolean type", literalValue);
        }
    }

    private static class ExtractLiteralValue extends ScalarOperatorVisitor<Object, Type.TypeID> {
        private boolean needCast(PrimitiveType sourceType, Type.TypeID dstTypeID) {
            switch (sourceType) {
                case BOOLEAN:
                    return dstTypeID != Type.TypeID.BOOLEAN;
                case TINYINT:
                case SMALLINT:
                case INT:
                    return dstTypeID != Type.TypeID.INTEGER;
                case BIGINT:
                    return dstTypeID != Type.TypeID.LONG;
                case FLOAT:
                    return dstTypeID != Type.TypeID.FLOAT;
                case DOUBLE:
                    return dstTypeID != Type.TypeID.DOUBLE;
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return dstTypeID != Type.TypeID.DECIMAL;
                case HLL:
                case VARCHAR:
                case CHAR:
                    return dstTypeID != Type.TypeID.STRING;
                case DATE:
                    return dstTypeID != Type.TypeID.DATE;
                case DATETIME:
                    return dstTypeID != Type.TypeID.TIMESTAMP;
                default:
                    return true;
            }
        }

        private ConstantOperator tryCastToResultType(ConstantOperator operator, Type.TypeID resultTypeID) {
            try {
                switch (resultTypeID) {
                    case BOOLEAN:
                        return operator.castTo(com.starrocks.catalog.Type.BOOLEAN);
                    case DATE:
                        return operator.castTo(com.starrocks.catalog.Type.DATE);
                    case TIMESTAMP:
                        return operator.castTo(com.starrocks.catalog.Type.DATETIME);
                    case STRING:
                    case UUID:
                        // num and string has different comparator
                        if (operator.getType().isNumericType()) {
                            return null;
                        }
                        return operator.castTo(com.starrocks.catalog.Type.VARCHAR);
                    case BINARY:
                        return operator.castTo(com.starrocks.catalog.Type.VARBINARY);
                    // num usually don't need cast, and num and string has different comparator
                    // cast is dangerous.
                    case INTEGER:
                    case LONG:
                    // usually not used as partition column, don't do much work
                    case DECIMAL:
                    case FLOAT:
                    case DOUBLE:
                    case STRUCT:
                    case LIST:
                    case MAP:
                    // not supported
                    case FIXED:
                    case TIME:
                        return null;
                }
            } catch (Exception e) {
                return null;
            }
            return operator;
        }

        @Override
        public Object visit(ScalarOperator scalarOperator, Type.TypeID context) {
            return null;
        }

        @Override
        public Object visitConstant(ConstantOperator operator, Type.TypeID context) {
            if (context != null && needCast(operator.getType().getPrimitiveType(), context)) {
                operator = tryCastToResultType(operator, context);
            }
            if (operator == null) {
                return null;
            }
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

        public String visitSubfield(SubfieldOperator operator, Void context) {
            ScalarOperator child = operator.getChild(0);
            if (!(child instanceof ColumnRefOperator)) {
                return null;
            }
            ColumnRefOperator columnRefChild = ((ColumnRefOperator) child);
            List<String> paths = new ImmutableList.Builder<String>()
                    .add(columnRefChild.getName()).addAll(operator.getFieldNames())
                    .build();
            return String.join(".", paths);
        }
    }
}