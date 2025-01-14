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

package com.starrocks.connector.kudu;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
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
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KuduPredicateConverter extends ScalarOperatorVisitor<List<KuduPredicate>, Void> {
    private static final Logger LOG = LogManager.getLogger(KuduPredicateConverter.class);
    private final Schema schema;

    public KuduPredicateConverter(Schema schema) {
        this.schema = schema;
    }

    public List<KuduPredicate> convert(ScalarOperator operator) {
        if (operator == null) {
            return null;
        }
        return operator.accept(this, null);
    }

    @Override
    public List<KuduPredicate> visit(ScalarOperator scalarOperator, Void context) {
        return null;
    }

    @Override
    public List<KuduPredicate> visitCompoundPredicate(CompoundPredicateOperator operator, Void context) {
        CompoundPredicateOperator.CompoundType op = operator.getCompoundType();
        if (op == CompoundPredicateOperator.CompoundType.NOT) {
            return Lists.newArrayList();
        }
        Optional<List<KuduPredicate>> left = Optional.ofNullable(operator.getChild(0).accept(this, null));
        Optional<List<KuduPredicate>> right = Optional.ofNullable(operator.getChild(1).accept(this, null));
        if (op == CompoundPredicateOperator.CompoundType.OR) {
            if (left.isPresent() && right.isPresent()) {
                return Lists.newArrayList();
            }
            return left.orElse(right.orElse(Lists.newArrayList()));
        }
        return Stream.of(left, right)
                .filter(Optional::isPresent)
                .flatMap(e -> e.get().stream())
                .collect(Collectors.toList());
    }

    @Override
    public List<KuduPredicate> visitIsNullPredicate(IsNullPredicateOperator operator, Void context) {
        String columnName = getColumnName(operator.getChild(0));
        if (columnName == null) {
            return Lists.newArrayList();
        }
        ColumnSchema columnSchema = schema.getColumn(columnName);
        if (operator.isNotNull()) {
            return Lists.newArrayList(KuduPredicate.newIsNotNullPredicate(columnSchema));
        } else {
            return Lists.newArrayList(KuduPredicate.newIsNullPredicate(columnSchema));
        }
    }

    @Override
    public List<KuduPredicate> visitBinaryPredicate(BinaryPredicateOperator operator, Void context) {
        String columnName = getColumnName(operator.getChild(0));
        if (columnName == null) {
            return Lists.newArrayList();
        }
        ColumnSchema column = schema.getColumn(columnName);
        Type targetType = ColumnTypeConverter.fromKuduType(column);
        Object literal = getLiteral(operator.getChild(1), targetType);
        if (literal == null) {
            return Lists.newArrayList();
        }
        KuduPredicate.ComparisonOp op = transComparisonOp(operator.getBinaryType());
        if (op != null) {
            return Lists.newArrayList(KuduPredicate.newComparisonPredicate(schema.getColumn(columnName), op, literal));
        }
        return Lists.newArrayList();
    }

    public KuduPredicate.ComparisonOp transComparisonOp(BinaryType binaryType) {
        switch (binaryType) {
            case LT:
                return KuduPredicate.ComparisonOp.LESS;
            case LE:
                return KuduPredicate.ComparisonOp.LESS_EQUAL;
            case GT:
                return KuduPredicate.ComparisonOp.GREATER;
            case GE:
                return KuduPredicate.ComparisonOp.GREATER_EQUAL;
            case EQ:
                return KuduPredicate.ComparisonOp.EQUAL;
            default:
                return null;
        }
    }

    @Override
    public List<KuduPredicate> visitInPredicate(InPredicateOperator operator, Void context) {
        String columnName = getColumnName(operator.getChild(0));
        if (columnName == null) {
            return Lists.newArrayList();
        }
        ColumnSchema column = schema.getColumn(columnName);
        Type targetType = ColumnTypeConverter.fromKuduType(column);
        List<ScalarOperator> valuesOperatorList = operator.getListChildren();
        List<Object> literalValues = new ArrayList<>(valuesOperatorList.size());
        for (ScalarOperator valueOperator : valuesOperatorList) {
            Object literal = getLiteral(valueOperator, targetType);
            if (literal == null) {
                return Lists.newArrayList();
            }
            literalValues.add(literal);
        }
        if (!operator.isNotIn()) {
            KuduPredicate kuduPredicate =
                    KuduPredicate.newInListPredicate(schema.getColumn(columnName), literalValues);
            return Lists.newArrayList(kuduPredicate);
        }
        return Lists.newArrayList();
    }

    private Object getLiteral(ScalarOperator operator, Type targetType) {
        if (!(operator instanceof ConstantOperator)) {
            return null;
        }

        ConstantOperator constValue = (ConstantOperator) operator;
        Optional<ConstantOperator> casted = constValue.castTo(targetType);
        if (!casted.isPresent()) {
            // Illegal cast. Do not push down the predicate.
            return null;
        }
        constValue = casted.get();
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
                return constValue.getDecimal();
            case HLL:
            case VARCHAR:
            case CHAR:
                return constValue.getVarchar();
            case DATE:
                LocalDate localDate = constValue.getDate().toLocalDate();
                LocalDate epochDay = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();
                return (int) ChronoUnit.DAYS.between(epochDay, localDate);
            case DATETIME:
                LocalDateTime localDateTime = constValue.getDatetime();
                LocalDateTime epochDateTime = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDateTime();
                return ChronoUnit.MICROS.between(epochDateTime, localDateTime);
            default:
                return null;
        }
    }

    @Override
    public List<KuduPredicate> visitLikePredicateOperator(LikePredicateOperator operator, Void context) {
        return Lists.newArrayList();
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