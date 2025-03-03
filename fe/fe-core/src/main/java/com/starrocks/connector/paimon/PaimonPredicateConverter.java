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


package com.starrocks.connector.paimon;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.data.Timestamp.fromSQLTimestamp;

public class PaimonPredicateConverter extends ScalarOperatorVisitor<Predicate, Void> {
    private static final Logger LOG = LogManager.getLogger(PaimonPredicateConverter.class);
    private final PredicateBuilder builder;
    private final List<String> fieldNames;
    private final List<DataType> fieldTypes;

    public PaimonPredicateConverter(RowType rowType) {
        this.builder = new PredicateBuilder(rowType);
        this.fieldTypes = rowType.getFieldTypes();
        this.fieldNames = rowType.getFields().stream().map(DataField::name).collect(Collectors.toList());
    }

    public Predicate convert(ScalarOperator operator) {
        if (operator == null) {
            return null;
        }
        return operator.accept(this, null);
    }

    @Override
    public Predicate visit(ScalarOperator scalarOperator, Void context) {
        return null;
    }

    @Override
    public Predicate visitCompoundPredicate(CompoundPredicateOperator operator, Void context) {
        CompoundPredicateOperator.CompoundType op = operator.getCompoundType();
        if (op == CompoundPredicateOperator.CompoundType.NOT) {
            if (operator.getChild(0) instanceof LikePredicateOperator) {
                return null;
            }
            Predicate expression = operator.getChild(0).accept(this, null);

            if (expression != null) {
                return expression.negate().orElse(null);
            }
        } else {
            Predicate left = operator.getChild(0).accept(this, null);
            Predicate right = operator.getChild(1).accept(this, null);
            if (left != null && right != null) {
                return (op == CompoundPredicateOperator.CompoundType.OR) ? PredicateBuilder.or(left, right) :
                        PredicateBuilder.and(left, right);
            }
        }
        return null;
    }

    @Override
    public Predicate visitIsNullPredicate(IsNullPredicateOperator operator, Void context) {
        String columnName = getColumnName(operator.getChild(0));
        if (columnName == null) {
            return null;
        }
        int idx = fieldNames.indexOf(columnName);
        if (operator.isNotNull()) {
            return builder.isNotNull(idx);
        } else {
            return builder.isNull(idx);
        }
    }

    @Override
    public Predicate visitBinaryPredicate(BinaryPredicateOperator operator, Void context) {
        String columnName = getColumnName(operator.getChild(0));
        if (columnName == null) {
            return null;
        }
        int idx = fieldNames.indexOf(columnName);
        Object literal = getLiteral(operator.getChild(1), fieldTypes.get(idx));
        if (literal == null) {
            return null;
        }
        if (fieldTypes.get(idx) instanceof BooleanType) {
            literal = convertBoolLiteralValue(literal);
        }
        switch (operator.getBinaryType()) {
            case LT:
                return builder.lessThan(idx, literal);
            case LE:
                return builder.lessOrEqual(idx, literal);
            case GT:
                return builder.greaterThan(idx, literal);
            case GE:
                return builder.greaterOrEqual(idx, literal);
            case EQ:
                return builder.equal(idx, literal);
            case NE:
                return builder.notEqual(idx, literal);
            default:
                return null;
        }
    }

    @Override
    public Predicate visitInPredicate(InPredicateOperator operator, Void context) {
        String columnName = getColumnName(operator.getChild(0));
        if (columnName == null) {
            return null;
        }
        int idx = fieldNames.indexOf(columnName);
        List<ScalarOperator> valuesOperatorList = operator.getListChildren();
        List<Object> literalValues = new ArrayList<>(valuesOperatorList.size());
        for (ScalarOperator valueOperator : valuesOperatorList) {
            Object value = getLiteral(valueOperator, fieldTypes.get(idx));
            if (value == null) {
                return null;
            }
            if (fieldTypes.get(idx) instanceof BooleanType) {
                value = convertBoolLiteralValue(value);
            }
            literalValues.add(value);
        }

        if (operator.isNotIn()) {
            return builder.notIn(idx, literalValues);
        } else {
            return builder.in(idx, literalValues);
        }
    }

    @Override
    public Predicate visitLikePredicateOperator(LikePredicateOperator operator, Void context) {
        String columnName = getColumnName(operator.getChild(0));
        if (columnName == null) {
            return null;
        }

        int idx = fieldNames.indexOf(columnName);
        if (operator.getLikeType() == LikePredicateOperator.LikeType.LIKE) {
            if (operator.getChild(1).getType().isStringType()) {
                Object objectLiteral = getLiteral(operator.getChild(1), fieldTypes.get(idx));
                if (objectLiteral == null) {
                    return null;
                }
                String literal = ((BinaryString) objectLiteral).toString();
                if (literal.length() > 1 && literal.indexOf("%") == literal.length() - 1 && literal.charAt(0) != '%') {
                    return builder.startsWith(idx, BinaryString.fromString(literal.substring(0, literal.length() - 1)));
                }
            }
        }
        return null;
    }

    private Object getLiteral(ScalarOperator operator, DataType dataType) {
        if (operator == null) {
            return null;
        }

        return operator.accept(new PaimonPredicateConverter.ExtractLiteralValue(), dataType);
    }

    private static class ExtractLiteralValue extends ScalarOperatorVisitor<Object, DataType> {
        @Override
        public Object visit(ScalarOperator scalarOperator, DataType dataType) {
            return null;
        }

        @Override
        public Object visitConstant(ConstantOperator operator, DataType dataType) {
            if (needCast(operator.getType().getPrimitiveType(), dataType)) {
                operator = tryCastToResultType(operator, dataType);
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
                    BigDecimal bigDecimal = operator.getDecimal();
                    PrimitiveType type = operator.getType().getPrimitiveType();
                    return Decimal.fromBigDecimal(bigDecimal, PrimitiveType.getMaxPrecisionOfDecimal(type),
                            PrimitiveType.getDefaultScaleOfDecimal(type));
                case HLL:
                case VARCHAR:
                case CHAR:
                    return BinaryString.fromString(operator.getVarchar());
                case DATE:
                    LocalDate localDate = operator.getDate().toLocalDate();
                    LocalDate epochDay = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();
                    return (int) ChronoUnit.DAYS.between(epochDay, localDate);
                case DATETIME:
                    LocalDateTime localDateTime = operator.getDatetime();
                    return fromSQLTimestamp(Timestamp.valueOf((localDateTime)));
                default:
                    return null;
            }
        }

        @Override
        public Object visitCastOperator(CastOperator operator, DataType dataType) {
            return operator.getChild(0).accept(this, dataType);
        }

        private boolean needCast(PrimitiveType sourceType, DataType dataType) {
            return switch (sourceType) {
                case BOOLEAN -> !(dataType instanceof BooleanType);
                case TINYINT -> !(dataType instanceof TinyIntType);
                case SMALLINT -> !(dataType instanceof SmallIntType);
                case INT -> !(dataType instanceof IntType);
                case BIGINT -> !(dataType instanceof BigIntType);
                case FLOAT -> !(dataType instanceof FloatType);
                case DOUBLE -> !(dataType instanceof DoubleType);
                case DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128 -> !(dataType instanceof DecimalType);
                case HLL, VARCHAR -> !(dataType instanceof VarCharType);
                case CHAR -> !(dataType instanceof CharType);
                case DATE -> !(dataType instanceof DateType);
                case DATETIME -> !(dataType instanceof TimestampType);
                default -> true;
            };
        }

        private ConstantOperator tryCastToResultType(ConstantOperator operator, DataType dataType) {
            Optional<ConstantOperator> res = Optional.empty();
            if (dataType instanceof BooleanType) {
                res = operator.castTo(com.starrocks.catalog.Type.BOOLEAN);
            } else if (dataType instanceof DateType) {
                res = operator.castTo(com.starrocks.catalog.Type.DATE);
            } else if (dataType instanceof TimestampType) {
                res = operator.castTo(com.starrocks.catalog.Type.DATETIME);
            } else if (dataType instanceof VarCharType) {
                res = operator.castTo(com.starrocks.catalog.Type.STRING);
            } else if (dataType instanceof CharType) {
                res = operator.castTo(com.starrocks.catalog.Type.CHAR);
            } else if (dataType instanceof BinaryType) {
                res = operator.castTo(com.starrocks.catalog.Type.VARBINARY);
            } else if (dataType instanceof IntType) {
                res = operator.castTo(com.starrocks.catalog.Type.INT);
            } else if (dataType instanceof BigIntType) {
                res = operator.castTo(com.starrocks.catalog.Type.BIGINT);
            } else if (dataType instanceof TinyIntType) {
                res = operator.castTo(com.starrocks.catalog.Type.TINYINT);
            } else if (dataType instanceof SmallIntType) {
                res = operator.castTo(com.starrocks.catalog.Type.SMALLINT);
            } else if (dataType instanceof FloatType) {
                res = operator.castTo(com.starrocks.catalog.Type.FLOAT);
            } else if (dataType instanceof DoubleType) {
                res = operator.castTo(com.starrocks.catalog.Type.DOUBLE);
            } else if (dataType instanceof DecimalType) {
                res = operator.castTo(com.starrocks.catalog.Type.DEFAULT_DECIMAL128);
            }
            return res.orElse(operator);
        }
    }

    // Support both 0/1 and true/false for boolean type
    private static Object convertBoolLiteralValue(Object literalValue) {
        try {
            return new BoolLiteral(String.valueOf(literalValue)).getValue();
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to convert %s to boolean type", literalValue);
        }
    }

    private String getColumnName(ScalarOperator operator) {
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