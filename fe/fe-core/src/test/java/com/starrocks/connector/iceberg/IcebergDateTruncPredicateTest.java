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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

public class IcebergDateTruncPredicateTest {
    private static final ColumnRefOperator TS = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private static final Schema TIMESTAMP_SCHEMA = new Schema(
            Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()));
    private static final Schema TIMESTAMPTZ_SCHEMA = new Schema(
            Types.NestedField.optional(1, "ts", Types.TimestampType.withZone()));
    private final ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();

    @BeforeEach
    public void setUp() {
        // preimages are only derived inside a session
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testTimestampDayRangeAndMicrosecondBoundaries() {
        withSessionZone("Asia/Shanghai", () -> {
            LocalDateTime day = LocalDateTime.of(2026, 10, 28, 0, 0);
            ScalarOperator predicate = dayPredicate(TS, "day", day);
            Expression expression = convert(predicate, TIMESTAMP_SCHEMA);
            // A timestamp without zone must not depend on the session time zone.
            assertDayRange(expression, TIMESTAMP_SCHEMA, day, ZoneOffset.UTC);
            // the range only selects files, so a metadata-only delete must not use it
            Assertions.assertNull(converter.convertStrict(List.of(predicate),
                    new ScalarOperatorToIcebergExpr.IcebergContext(TIMESTAMP_SCHEMA.asStruct())));
        });
    }

    @Test
    public void testTimestampWithZoneUsesSessionTimeZone() {
        for (String zone : List.of("UTC", "+05:30", "+08:00")) {
            withSessionZone(zone, () -> {
                LocalDateTime day = LocalDateTime.of(2026, 10, 28, 0, 0);
                assertDayRange(convert(dayPredicate(TS, "DAY", day), TIMESTAMPTZ_SCHEMA),
                        TIMESTAMPTZ_SCHEMA, day, ZoneId.of(zone));
            });
        }
    }

    @Test
    public void testTimestampWithZoneDaylightSavingDays() {
        ZoneId zone = ZoneId.of("America/Los_Angeles");
        withSessionZone(zone.getId(), () -> {
            // 23-hour and 25-hour days: the BE reads each value with its own offset, so the bounds use the zone rules
            for (LocalDateTime day : List.of(LocalDateTime.of(2026, 3, 8, 0, 0),
                    LocalDateTime.of(2026, 11, 1, 0, 0))) {
                assertDayRange(convert(dayPredicate(TS, "day", day), TIMESTAMPTZ_SCHEMA), TIMESTAMPTZ_SCHEMA, day, zone);
            }
        });
    }

    @Test
    public void testTimestampRangeOutsideNanosecondEpochRange() {
        for (LocalDateTime day : List.of(LocalDateTime.of(1600, 1, 1, 0, 0),
                LocalDateTime.of(9999, 12, 30, 0, 0))) {
            assertDayRange(convert(dayPredicate(TS, "day", day), TIMESTAMP_SCHEMA),
                    TIMESTAMP_SCHEMA, day, ZoneOffset.UTC);
        }
    }

    @Test
    public void testNullAndNonMidnightConstantsRemainResidual() {
        ScalarOperator call = dateTrunc(TS, "day");
        assertResidual(new BinaryPredicateOperator(BinaryType.EQ, call, ConstantOperator.createNull(DateType.DATETIME)),
                TIMESTAMP_SCHEMA);
        assertResidual(new BinaryPredicateOperator(BinaryType.EQ, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 10, 28, 0, 0, 1))), TIMESTAMP_SCHEMA);
        assertResidual(new BinaryPredicateOperator(BinaryType.EQ, call,
                ConstantOperator.createDatetime(LocalDateTime.of(2026, 10, 28, 0, 0, 0, 1000))), TIMESTAMP_SCHEMA);
        assertResidual(new BinaryPredicateOperator(BinaryType.EQ, call, ConstantOperator.createVarchar("2026-10-28")),
                TIMESTAMP_SCHEMA);
    }

    @Test
    public void testOtherUnitsComparisonsAndCasts() {
        LocalDateTime day = LocalDateTime.of(2026, 10, 28, 0, 0);
        // no value truncates to a month start of 10-28
        assertResidual(dayPredicate(TS, "month", day), TIMESTAMP_SCHEMA);
        assertResidual(new BinaryPredicateOperator(BinaryType.NE, dateTrunc(TS, "day"),
                ConstantOperator.createDatetime(day)), TIMESTAMP_SCHEMA);
        long lower = micros(day, ZoneOffset.UTC);
        long upper = micros(day.plusDays(1), ZoneOffset.UTC);
        assertConverted(BinaryType.GE, day, Expressions.greaterThanOrEqual("ts", lower));
        assertConverted(BinaryType.GT, day, Expressions.greaterThanOrEqual("ts", upper));
        assertConverted(BinaryType.LE, day, Expressions.lessThan("ts", upper));
        assertConverted(BinaryType.LT, day, Expressions.lessThan("ts", lower));
        ColumnRefOperator text = new ColumnRefOperator(2, VarcharType.VARCHAR, "text", true);
        Schema textSchema = new Schema(Types.NestedField.optional(2, "text", Types.StringType.get()));
        assertResidual(dayPredicate(new CastOperator(DateType.DATETIME, text), "day", day), textSchema);
        assertResidual(dayPredicate(new CastOperator(DateType.DATETIME, new CastOperator(DateType.DATE, TS)),
                "day", day), TIMESTAMP_SCHEMA);
        Schema dateSchema = new Schema(Types.NestedField.optional(1, "ts", Types.DateType.get()));
        long epochDay = day.toLocalDate().toEpochDay();
        Expression expected = Expressions.and(Expressions.greaterThanOrEqual("ts", epochDay),
                Expressions.lessThan("ts", epochDay + 1));
        Assertions.assertEquals(expected.toString(), convert(dayPredicate(TS, "day", day), dateSchema).toString());
    }

    private void assertConverted(BinaryType type, LocalDateTime day, Expression expected) {
        Assertions.assertEquals(expected.toString(), convert(new BinaryPredicateOperator(type, dateTrunc(TS, "day"),
                ConstantOperator.createDatetime(day)), TIMESTAMP_SCHEMA).toString());
    }

    @Test
    public void testMidnightTimeZoneTransitions() {
        // Midnight is nonexistent in Sao Paulo on this day: the day starts at the transition instant (01:00 local).
        withSessionZone("America/Sao_Paulo", () -> {
            ZoneId zone = ZoneId.of("America/Sao_Paulo");
            LocalDateTime day = LocalDateTime.of(2018, 11, 4, 0, 0);
            long lower = micros(LocalDateTime.of(2018, 11, 4, 1, 0), zone);
            long upper = micros(day.plusDays(1), zone);
            assertRange(convert(dayPredicate(TS, "day", day), TIMESTAMPTZ_SCHEMA), TIMESTAMPTZ_SCHEMA, lower, upper);
        });
        // Midnight occurs twice in Havana on this day: the day starts at the earlier one.
        withSessionZone("America/Havana", () -> {
            ZoneId zone = ZoneId.of("America/Havana");
            LocalDateTime day = LocalDateTime.of(2020, 11, 1, 0, 0);
            long lower = day.atZone(zone).withEarlierOffsetAtOverlap().toEpochSecond() * 1_000_000L;
            long upper = micros(day.plusDays(1), zone);
            assertRange(convert(dayPredicate(TS, "day", day), TIMESTAMPTZ_SCHEMA), TIMESTAMPTZ_SCHEMA, lower, upper);
        });
    }

    @Test
    public void testConjunctionDisjunctionAndNot() {
        LocalDateTime firstDay = LocalDateTime.of(2026, 10, 28, 0, 0);
        ScalarOperator first = dayPredicate(TS, "day", firstDay);
        ScalarOperator second = dayPredicate(TS, "day", firstDay.plusDays(1));
        for (CompoundPredicateOperator.CompoundType type : List.of(CompoundPredicateOperator.CompoundType.AND,
                CompoundPredicateOperator.CompoundType.OR)) {
            Expression left = convert(first, TIMESTAMP_SCHEMA);
            Expression right = convert(second, TIMESTAMP_SCHEMA);
            Expression expected = type == CompoundPredicateOperator.CompoundType.AND
                    ? Expressions.and(left, right) : Expressions.or(left, right);
            Expression actual = convert(new CompoundPredicateOperator(type, first, second), TIMESTAMP_SCHEMA);
            Assertions.assertEquals(expected.toString(), actual.toString());
        }
        // Keep NOT residual rather than interpreting SQL UNKNOWN as a match in Iceberg's two-valued logic.
        assertResidual(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, first), TIMESTAMP_SCHEMA);
    }

    private static ScalarOperator dateTrunc(ScalarOperator column, String unit) {
        return new CallOperator(FunctionSet.DATE_TRUNC, DateType.DATETIME,
                List.of(ConstantOperator.createVarchar(unit), column));
    }

    private static ScalarOperator dayPredicate(ScalarOperator column, String unit, LocalDateTime value) {
        return new BinaryPredicateOperator(BinaryType.EQ, dateTrunc(column, unit), ConstantOperator.createDatetime(value));
    }

    private Expression convert(ScalarOperator predicate, Schema schema) {
        return converter.convert(List.of(predicate), new ScalarOperatorToIcebergExpr.IcebergContext(schema.asStruct()));
    }

    private void assertResidual(ScalarOperator predicate, Schema schema) {
        Assertions.assertEquals(Expression.Operation.TRUE, convert(predicate, schema).op());
        Assertions.assertNull(converter.convertStrict(List.of(predicate),
                new ScalarOperatorToIcebergExpr.IcebergContext(schema.asStruct())));
    }

    private void assertDayRange(Expression expression, Schema schema, LocalDateTime day, ZoneId zone) {
        assertRange(expression, schema, micros(day, zone), micros(day.plusDays(1), zone));
    }

    private void assertRange(Expression expression, Schema schema, long lower, long upper) {
        Expression expected = Expressions.and(Expressions.greaterThanOrEqual("ts", lower), Expressions.lessThan("ts", upper));
        Assertions.assertEquals(expected.toString(), expression.toString());
        Evaluator evaluator = new Evaluator(schema.asStruct(), expression);
        Assertions.assertFalse(evaluator.eval(row(lower - 1)));
        Assertions.assertTrue(evaluator.eval(row(lower)));
        Assertions.assertTrue(evaluator.eval(row(upper - 1)));
        Assertions.assertFalse(evaluator.eval(row(upper)));
        Assertions.assertFalse(evaluator.eval(row(null)));
    }

    private static long micros(LocalDateTime value, ZoneId zone) {
        return Math.addExact(Math.multiplyExact(value.atZone(zone).toEpochSecond(), 1_000_000L), value.getNano() / 1000);
    }

    private static StructLike row(Long value) {
        return new StructLike() {
            @Override
            public int size() {
                return 1;
            }

            @Override
            public <T> T get(int pos, Class<T> javaClass) {
                return javaClass.cast(value);
            }

            @Override
            public <T> void set(int pos, T v) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static void withSessionZone(String zone, Runnable action) {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone(zone);
        context.setThreadLocalInfo();
        try {
            action.run();
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }
}
