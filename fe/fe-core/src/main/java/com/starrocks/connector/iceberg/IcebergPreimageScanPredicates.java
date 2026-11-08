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

import com.starrocks.catalog.Column;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Plain {@code column op constant} predicates implied by the function predicates of an Iceberg scan, for the BE to
 * prune row groups (Parquet statistics and page index) with. The BE only prunes on predicates over a bare column, so
 * {@code year(c) = 2024} prunes Iceberg files (through {@link IcebergFunctionPreimage}) but no row group inside them;
 * adding {@code c >= 2024-01-01 and c < 2025-01-01} next to it does.
 * <p>
 * Each derived predicate is a superset of the conjunct it came from, so adding it to the scan conjuncts keeps the
 * result. They are added to the BE conjuncts only, never to the optimizer's predicate, so the selectivity of a
 * conjunct is not counted twice. Only DATE and DATETIME columns are handled.
 */
public final class IcebergPreimageScanPredicates {
    private IcebergPreimageScanPredicates() {
    }

    /**
     * @param conjuncts the scan predicate's conjuncts
     * @param schema the Iceberg schema the scan reads
     * @param colRefToColumn the scan's column refs
     * @return derived predicates not already among {@code conjuncts}
     */
    public static List<ScalarOperator> derive(List<ScalarOperator> conjuncts, Types.StructType schema,
                                              Map<ColumnRefOperator, Column> colRefToColumn) {
        List<Expression> preimages = new ScalarOperatorToIcebergExpr().derivePreimages(conjuncts,
                new ScalarOperatorToIcebergExpr.IcebergContext(schema));
        if (preimages.isEmpty()) {
            return List.of();
        }
        Map<String, ColumnRefOperator> columns = new HashMap<>();
        colRefToColumn.forEach((ref, column) -> columns.put(column.getName().toLowerCase(Locale.ROOT), ref));
        Set<ScalarOperator> existing = new HashSet<>(conjuncts);
        List<ScalarOperator> result = new ArrayList<>();
        for (Expression preimage : preimages) {
            ScalarOperator predicate = toScalar(preimage, schema, columns);
            if (predicate == null) {
                continue;
            }
            for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
                if (existing.add(conjunct)) {
                    result.add(conjunct);
                }
            }
        }
        return result;
    }

    /**
     * @return a superset of {@code expr}, or null when nothing can be expressed.
     */
    private static ScalarOperator toScalar(Expression expr, Types.StructType schema,
                                           Map<String, ColumnRefOperator> columns) {
        if (expr instanceof And and) {
            ScalarOperator left = toScalar(and.left(), schema, columns);
            ScalarOperator right = toScalar(and.right(), schema, columns);
            // dropping one side of an AND still leaves a superset
            if (left == null || right == null) {
                return left == null ? right : left;
            }
            return CompoundPredicateOperator.and(left, right);
        }
        if (expr instanceof Or or) {
            ScalarOperator left = toScalar(or.left(), schema, columns);
            ScalarOperator right = toScalar(or.right(), schema, columns);
            return left == null || right == null ? null : CompoundPredicateOperator.or(left, right);
        }
        if (expr instanceof UnboundPredicate<?> predicate && predicate.term() instanceof NamedReference<?> ref) {
            BinaryType op = toBinaryType(predicate.op());
            ColumnRefOperator column = columns.get(ref.name().toLowerCase(Locale.ROOT));
            Types.NestedField field = schema.caseInsensitiveField(ref.name());
            if (op == null || column == null || field == null || predicate.literal() == null) {
                return null;
            }
            ConstantOperator constant = toConstant(predicate.literal().value(), field.type(), column);
            return constant == null ? null : new BinaryPredicateOperator(op, column, constant);
        }
        return null;
    }

    private static BinaryType toBinaryType(Expression.Operation op) {
        switch (op) {
            case EQ:
                return BinaryType.EQ;
            case LT:
                return BinaryType.LT;
            case LT_EQ:
                return BinaryType.LE;
            case GT:
                return BinaryType.GT;
            case GT_EQ:
                return BinaryType.GE;
            default:
                return null;
        }
    }

    /**
     * The Iceberg literal of a DATE (epoch days) or TIMESTAMP (epoch micros) bound as a constant of the column's type.
     * A timestamptz is read as a local time in the session time zone, the inverse of how the preimage encoded it.
     */
    private static ConstantOperator toConstant(Object value, Type icebergType, ColumnRefOperator column) {
        if (!(value instanceof Number number)) {
            return null;
        }
        long v = number.longValue();
        if (icebergType.typeId() == Type.TypeID.DATE && column.getType().isDate()) {
            return ConstantOperator.createDate(LocalDate.ofEpochDay(v).atStartOfDay());
        }
        if (icebergType.typeId() == Type.TypeID.TIMESTAMP && column.getType().isDatetime()) {
            ZoneId zone = Types.TimestampType.withZone().equals(icebergType)
                    ? TimeUtils.getTimeZone().toZoneId() : ZoneOffset.UTC;
            Instant instant = Instant.ofEpochSecond(Math.floorDiv(v, 1_000_000L),
                    Math.floorMod(v, 1_000_000L) * 1_000L);
            return ConstantOperator.createDatetime(LocalDateTime.ofInstant(instant, zone));
        }
        return null;
    }
}
