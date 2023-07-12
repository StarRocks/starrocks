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

import com.starrocks.catalog.PrimitiveType;
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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.data.Timestamp.fromSQLTimestamp;

public class PaimonPredicateConverter extends ScalarOperatorVisitor<Predicate, Void> {
    private static final Logger LOG = LogManager.getLogger(PaimonPredicateConverter.class);
    private final PredicateBuilder builder;
    private final List<String> fieldNames;

    public PaimonPredicateConverter(RowType rowType) {
        this.builder = new PredicateBuilder(rowType);
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
        Object literal = getLiteral(operator.getChild(1));
        if (literal == null) {
            return null;
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
            Object value = getLiteral(valueOperator);
            if (value == null) {
                return null;
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
                String literal = ((BinaryString) getLiteral(operator.getChild(1))).toString();
                if (literal != null && literal.length() > 1 &&
                        literal.indexOf("%") == literal.length() - 1 &&
                        literal.charAt(0) != '%') {
                    return builder.startsWith(idx, BinaryString.fromString(literal.substring(0, literal.length() - 1)));
                }
            }
        }
        return null;
    }

    private Object getLiteral(ScalarOperator operator) {
        if (!(operator instanceof ConstantOperator)) {
            return null;
        }

        ConstantOperator constValue = (ConstantOperator) operator;
        switch (constValue.getType().getPrimitiveType()) {
            case BOOLEAN:
                return constValue.getBoolean();
            case TINYINT:
                return constValue.getTinyInt();
            case SMALLINT:
                return constValue.getSmallint();
            case INT:
                return constValue.getInt();
            case BIGINT:
                return constValue.getBigint();
            case FLOAT:
                return constValue.getFloat();
            case DOUBLE:
                return constValue.getDouble();
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                BigDecimal bigDecimal = constValue.getDecimal();
                PrimitiveType type = constValue.getType().getPrimitiveType();
                return Decimal.fromBigDecimal(bigDecimal, PrimitiveType.getMaxPrecisionOfDecimal(type),
                        PrimitiveType.getDefaultScaleOfDecimal(type));
            case HLL:
            case VARCHAR:
            case CHAR:
                return BinaryString.fromString(constValue.getVarchar());
            case DATE:
                LocalDate localDate = constValue.getDate().toLocalDate();
                LocalDate epochDay = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();
                return (int) ChronoUnit.DAYS.between(epochDay, localDate);
            case DATETIME:
                LocalDateTime localDateTime = constValue.getDatetime();
                return fromSQLTimestamp(Timestamp.valueOf((localDateTime)));
            default:
                return null;
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