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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class IcebergFunctionPreimageTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "d", Types.DateType.get()),
            Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(3, "tz", Types.TimestampType.withZone()),
            Types.NestedField.optional(4, "i", Types.IntegerType.get()),
            Types.NestedField.optional(5, "s", Types.StringType.get()));

    private static final ColumnRefOperator D = new ColumnRefOperator(1, DateType.DATE, "d", true, false);
    private static final ColumnRefOperator TS = new ColumnRefOperator(2, DateType.DATETIME, "ts", true, false);
    private static final ColumnRefOperator TZ = new ColumnRefOperator(3, DateType.DATETIME, "tz", true, false);
    private static final ColumnRefOperator I = new ColumnRefOperator(4, IntegerType.INT, "i", true, false);
    private static final ColumnRefOperator S = new ColumnRefOperator(5, VarcharType.VARCHAR, "s", true, false);

    private static final BinaryType[] OPS = {BinaryType.EQ, BinaryType.GE, BinaryType.GT, BinaryType.LT, BinaryType.LE};
    private static final String[] DATE_UNITS = {"day", "week", "month", "quarter", "year"};
    private static final String[] DATETIME_UNITS = {"second", "minute", "hour", "day", "week", "month", "quarter", "year"};

    private final ScalarOperatorToIcebergExpr converter = new ScalarOperatorToIcebergExpr();
    private final ScalarOperatorToIcebergExpr.IcebergContext context =
            new ScalarOperatorToIcebergExpr.IcebergContext(SCHEMA.asStruct());

    @BeforeEach
    public void setUp() {
        // preimages are only derived inside a session
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static CallOperator year(ScalarOperator arg) {
        return new CallOperator("year", IntegerType.SMALLINT, List.of(arg));
    }

    private static CallOperator toDate(ScalarOperator arg) {
        return new CallOperator("to_date", DateType.DATE, List.of(arg));
    }

    private static CallOperator dateTrunc(String unit, ScalarOperator arg) {
        return new CallOperator("date_trunc", arg.getType(), List.of(ConstantOperator.createVarchar(unit), arg));
    }

    private static CastOperator toDatetime(ScalarOperator arg) {
        return new CastOperator(DateType.DATETIME, arg);
    }

    private Expression convert(ScalarOperator predicate) {
        return converter.convert(List.of(predicate), context);
    }

    /**
     * The reference truncation, written independently of the code under test.
     */
    private static LocalDateTime trunc(String unit, LocalDateTime t) {
        switch (unit) {
            case "second":
                return t.truncatedTo(ChronoUnit.SECONDS);
            case "minute":
                return t.truncatedTo(ChronoUnit.MINUTES);
            case "hour":
                return t.truncatedTo(ChronoUnit.HOURS);
            case "day":
                return t.truncatedTo(ChronoUnit.DAYS);
            case "week":
                return t.truncatedTo(ChronoUnit.DAYS).minusDays(t.getDayOfWeek().getValue() - DayOfWeek.MONDAY.getValue());
            case "month":
                return t.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
            case "quarter":
                return t.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).withMonth((t.getMonthValue() - 1) / 3 * 3 + 1);
            case "year":
                return t.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
            default:
                throw new IllegalArgumentException(unit);
        }
    }

    private static boolean compare(BinaryType op, int cmp) {
        switch (op) {
            case EQ:
                return cmp == 0;
            case GE:
                return cmp >= 0;
            case GT:
                return cmp > 0;
            case LT:
                return cmp < 0;
            case LE:
                return cmp <= 0;
            default:
                throw new IllegalArgumentException(op.toString());
        }
    }

    private static StructLike row(Object value) {
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

    private static List<PartitionSpec> specsOf(String column) {
        List<PartitionSpec> specs = new ArrayList<>();
        if (column.equals("s")) {
            specs.add(PartitionSpec.builderFor(SCHEMA).identity(column).build());
            specs.add(PartitionSpec.builderFor(SCHEMA).truncate(column, 2).build());
            specs.add(PartitionSpec.builderFor(SCHEMA).truncate(column, 3).build());
            specs.add(PartitionSpec.builderFor(SCHEMA).truncate(column, 5).build());
            return specs;
        }
        specs.add(PartitionSpec.builderFor(SCHEMA).year(column).build());
        specs.add(PartitionSpec.builderFor(SCHEMA).month(column).build());
        specs.add(PartitionSpec.builderFor(SCHEMA).day(column).build());
        if (!column.equals("d")) {
            specs.add(PartitionSpec.builderFor(SCHEMA).hour(column).build());
        }
        return specs;
    }

    /**
     * For every value: if the original predicate holds, the derived expression must hold on the value itself and on
     * its partition tuple under every time transform of the column (inclusive projection). The derived range is also
     * exact on the value. Returns whether anything was derived.
     */
    private <T> boolean checkSound(String column, Expression derived, List<T> values,
                                   Function<T, Object> toIceberg, Function<T, Boolean> expected, String what) {
        boolean pruning = !derived.equals(Expressions.alwaysTrue());
        Types.NestedField field = SCHEMA.findField(column);
        Evaluator valueEvaluator = new Evaluator(Types.StructType.of(field), derived);
        List<PartitionSpec> specs = specsOf(column);
        List<Evaluator> partitionEvaluators = new ArrayList<>();
        for (PartitionSpec spec : specs) {
            partitionEvaluators.add(new Evaluator(spec.partitionType(), Projections.inclusive(spec).project(derived)));
        }
        for (T value : values) {
            Object icebergValue = toIceberg.apply(value);
            boolean match = expected.apply(value);
            boolean derivedMatch = valueEvaluator.eval(row(icebergValue));
            if (match) {
                Assertions.assertTrue(derivedMatch, what + " pruned matching value " + value + ": " + derived);
            } else if (pruning) {
                Assertions.assertFalse(derivedMatch, what + " is not exact on " + value + ": " + derived);
            }
            for (int s = 0; s < specs.size() && match; s++) {
                PartitionField partitionField = specs.get(s).fields().get(0);
                @SuppressWarnings("unchecked")
                Transform<Object, Object> transform = (Transform<Object, Object>) partitionField.transform();
                Object partitionValue = transform.bind(field.type()).apply(icebergValue);
                Assertions.assertTrue(partitionEvaluators.get(s).eval(row(partitionValue)),
                        what + " pruned the " + partitionField + " partition of " + value);
            }
        }
        return pruning;
    }

    private static long epochDay(LocalDateTime t) {
        return t.toLocalDate().toEpochDay();
    }

    // Iceberg represents a DATE value as an int
    private static Object dateValue(LocalDateTime t) {
        return (int) t.toLocalDate().toEpochDay();
    }

    private static long micros(LocalDateTime t, ZoneId zone) {
        return ChronoUnit.MICROS.between(Instant.EPOCH, t.atZone(zone).toInstant());
    }

    private static List<LocalDateTime> dates() {
        List<LocalDateTime> values = new ArrayList<>();
        for (LocalDateTime t = LocalDateTime.of(2023, 12, 1, 0, 0); t.isBefore(LocalDateTime.of(2025, 2, 1, 0, 0));
                t = t.plusDays(1)) {
            values.add(t);
        }
        return values;
    }

    private static List<LocalDateTime> datetimes() {
        List<LocalDateTime> values = new ArrayList<>();
        LocalDateTime[][] windows = {
                {LocalDateTime.of(2023, 12, 31, 22, 0), LocalDateTime.of(2024, 1, 1, 2, 0)},
                {LocalDateTime.of(2024, 3, 31, 23, 0), LocalDateTime.of(2024, 4, 1, 1, 0)},
                {LocalDateTime.of(2024, 2, 15, 9, 58), LocalDateTime.of(2024, 2, 15, 10, 3)}};
        for (LocalDateTime[] w : windows) {
            for (LocalDateTime t = w[0]; t.isBefore(w[1]); t = t.plusSeconds(7)) {
                values.add(t);
            }
        }
        for (LocalDateTime t = LocalDateTime.of(2023, 12, 1, 0, 0); t.isBefore(LocalDateTime.of(2025, 2, 1, 0, 0));
                t = t.plusMinutes(397)) {
            values.add(t);
        }
        return values;
    }

    @Test
    public void testYearOnDate() {
        for (ScalarOperator arg : List.of(D, toDatetime(D))) {
            for (int y = 2023; y <= 2025; y++) {
                for (BinaryType op : OPS) {
                    int k = y;
                    Expression derived = convert(new BinaryPredicateOperator(op, year(arg),
                            ConstantOperator.createSmallInt((short) y)));
                    Assertions.assertTrue(checkSound("d", derived, dates(), IcebergFunctionPreimageTest::dateValue,
                            t -> compare(op, Integer.compare(t.getYear(), k)), "year(d) " + op + " " + y));
                }
            }
        }
        // 2024 = [2024-01-01, 2025-01-01)
        Assertions.assertEquals(Expressions.and(
                        Expressions.greaterThanOrEqual("d", epochDay(LocalDateTime.of(2024, 1, 1, 0, 0))),
                        Expressions.lessThan("d", epochDay(LocalDateTime.of(2025, 1, 1, 0, 0)))).toString(),
                convert(new BinaryPredicateOperator(BinaryType.EQ, year(D), ConstantOperator.createInt(2024))).toString());
    }

    @Test
    public void testDateTruncOnDate() {
        List<LocalDateTime> constants = List.of(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2024, 2, 15, 0, 0),
                LocalDateTime.of(2024, 4, 1, 0, 0), LocalDateTime.of(2024, 3, 31, 0, 0),
                LocalDateTime.of(2024, 12, 30, 0, 0));
        for (String unit : DATE_UNITS) {
            for (LocalDateTime k : constants) {
                for (BinaryType op : OPS) {
                    Expression derived = convert(new BinaryPredicateOperator(op, dateTrunc(unit, D),
                            ConstantOperator.createDate(k)));
                    boolean pruning = checkSound("d", derived, dates(), IcebergFunctionPreimageTest::dateValue,
                            t -> compare(op, trunc(unit, t).compareTo(k)), "date_trunc(" + unit + ", d) " + op + " " + k);
                    // only an equality with a non-aligned constant is left unpruned
                    Assertions.assertEquals(op != BinaryType.EQ || trunc(unit, k).equals(k), pruning, unit + op + k);
                }
            }
        }
    }

    @Test
    public void testFunctionsOnDatetime() {
        List<LocalDateTime> constants = List.of(LocalDateTime.of(2024, 1, 1, 0, 0),
                LocalDateTime.of(2024, 2, 15, 10, 0, 0), LocalDateTime.of(2024, 2, 15, 10, 0, 30),
                LocalDateTime.of(2024, 4, 1, 0, 0), LocalDateTime.of(2024, 2, 15, 10, 1, 7));
        List<LocalDateTime> values = datetimes();
        Function<LocalDateTime, Object> toMicros = t -> micros(t, ZoneOffset.UTC);
        for (String unit : DATETIME_UNITS) {
            for (LocalDateTime k : constants) {
                for (BinaryType op : OPS) {
                    Expression derived = convert(new BinaryPredicateOperator(op, dateTrunc(unit, TS),
                            ConstantOperator.createDatetime(k)));
                    checkSound("ts", derived, values, toMicros,
                            t -> compare(op, trunc(unit, t).compareTo(k)), "date_trunc(" + unit + ", ts) " + op + " " + k);
                }
            }
        }
        for (BinaryType op : OPS) {
            LocalDateTime k = LocalDateTime.of(2024, 1, 1, 0, 0);
            Expression derived = convert(new BinaryPredicateOperator(op, toDate(TS), ConstantOperator.createDate(k)));
            Assertions.assertTrue(checkSound("ts", derived, values, toMicros,
                    t -> compare(op, trunc("day", t).compareTo(k)), "to_date(ts) " + op));
            derived = convert(new BinaryPredicateOperator(op, year(TS), ConstantOperator.createInt(2024)));
            Assertions.assertTrue(checkSound("ts", derived, values, toMicros,
                    t -> compare(op, Integer.compare(t.getYear(), 2024)), "year(ts) " + op));
        }
    }

    @Test
    public void testTimestampTzAcrossDst() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("America/New_York");
        ctx.setThreadLocalInfo();
        ZoneId zone = ZoneId.of("America/New_York");
        // the session sees each instant as its local time; 2024-11-03 01:xx happens twice, 2024-03-10 02:xx never
        List<Instant> instants = new ArrayList<>();
        for (Instant t = Instant.parse("2024-11-02T22:00:00Z"); t.isBefore(Instant.parse("2024-11-03T12:00:00Z"));
                t = t.plusSeconds(600)) {
            instants.add(t);
        }
        for (Instant t = Instant.parse("2024-03-10T03:00:00Z"); t.isBefore(Instant.parse("2024-03-10T12:00:00Z"));
                t = t.plusSeconds(600)) {
            instants.add(t);
        }
        // 2024-03-10 02:00 and 02:30 fall in the spring-forward gap; 2024-11-03 01:30 in the fall-back overlap
        List<LocalDateTime> constants = List.of(LocalDateTime.of(2024, 11, 3, 1, 0), LocalDateTime.of(2024, 11, 3, 0, 0),
                LocalDateTime.of(2024, 11, 3, 2, 0), LocalDateTime.of(2024, 11, 3, 1, 30),
                LocalDateTime.of(2024, 3, 10, 3, 0), LocalDateTime.of(2024, 3, 10, 0, 0),
                LocalDateTime.of(2024, 3, 10, 2, 0), LocalDateTime.of(2024, 3, 10, 2, 30));
        for (String unit : List.of("minute", "hour", "day")) {
            for (LocalDateTime k : constants) {
                for (BinaryType op : OPS) {
                    Expression derived = convert(new BinaryPredicateOperator(op, dateTrunc(unit, TZ),
                            ConstantOperator.createDatetime(k)));
                    Evaluator evaluator = new Evaluator(Types.StructType.of(SCHEMA.findField("tz")), derived);
                    for (Instant instant : instants) {
                        LocalDateTime local = LocalDateTime.ofInstant(instant, zone);
                        if (compare(op, trunc(unit, local).compareTo(k))) {
                            Assertions.assertTrue(
                                    evaluator.eval(row(ChronoUnit.MICROS.between(Instant.EPOCH, instant))),
                                    "date_trunc(" + unit + ", tz) " + op + " " + k + " pruned " + instant);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testInList() {
        Expression derived = convert(new InPredicateOperator(false, year(D),
                ConstantOperator.createInt(2023), ConstantOperator.createInt(2025)));
        Assertions.assertTrue(checkSound("d", derived, dates(), IcebergFunctionPreimageTest::dateValue,
                t -> t.getYear() == 2023 || t.getYear() == 2025, "year(d) in (2023, 2025)"));

        // a value that cannot be mapped (non-aligned) makes the whole list unpruned
        derived = convert(new InPredicateOperator(false, dateTrunc("month", D),
                ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0)),
                ConstantOperator.createDate(LocalDateTime.of(2024, 2, 15, 0, 0))));
        Assertions.assertEquals(Expressions.alwaysTrue(), derived);

        derived = convert(new InPredicateOperator(true, year(D), ConstantOperator.createInt(2024)));
        Assertions.assertEquals(Expressions.alwaysTrue(), derived);
    }

    @Test
    public void testNotDerived() {
        BinaryPredicateOperator yearEq = new BinaryPredicateOperator(BinaryType.EQ, year(D), ConstantOperator.createInt(2024));
        // under NOT the range would turn into a subset
        Assertions.assertEquals(Expressions.alwaysTrue(),
                convert(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, yearEq)));
        // strict (exact) conversion cannot use a pruning-only range
        Assertions.assertNull(converter.convertStrict(List.of(yearEq), context));
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(
                new BinaryPredicateOperator(BinaryType.NE, year(D), ConstantOperator.createInt(2024))));
        // year(d) = 2024.5 never holds, and truncating the constant would prune wrongly for < / >=
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(
                new BinaryPredicateOperator(BinaryType.LT, year(D), ConstantOperator.createDouble(2024.5))));
        // an INT holding yyyymmdd is cast to DATETIME by parsing, which is not order preserving
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(
                new BinaryPredicateOperator(BinaryType.EQ, year(new CastOperator(DateType.DATETIME, I)),
                        ConstantOperator.createInt(2024))));
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(
                new BinaryPredicateOperator(BinaryType.EQ, dateTrunc("millennium", D),
                        ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0)))));
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(
                new BinaryPredicateOperator(BinaryType.EQ, year(D), ConstantOperator.createInt(10000))));
        // non-constant right side
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(new BinaryPredicateOperator(BinaryType.EQ, year(D),
                new CallOperator("abs", FloatType.DOUBLE, List.of(I)))));
    }

    @Test
    public void testOrAndSessionSwitch() {
        ScalarOperator yearEq = new BinaryPredicateOperator(BinaryType.EQ, year(D), ConstantOperator.createInt(2024));
        ScalarOperator dEq = new BinaryPredicateOperator(BinaryType.EQ, D,
                ConstantOperator.createDate(LocalDateTime.of(2025, 1, 5, 0, 0)));
        Expression derived = convert(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, yearEq, dEq));
        Assertions.assertTrue(checkSound("d", derived, dates(), IcebergFunctionPreimageTest::dateValue,
                t -> t.getYear() == 2024 || t.equals(LocalDateTime.of(2025, 1, 5, 0, 0)), "year(d) = 2024 or d = ..."));

        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        Assertions.assertNotEquals(Expressions.alwaysTrue(), convert(yearEq));
        ctx.getSessionVariable().setEnableIcebergFunctionPredicatePruning(false);
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(yearEq));
    }

    @Test
    public void testUnsupportedColumnType() {
        Type varchar = VarcharType.VARCHAR;
        ColumnRefOperator s = new ColumnRefOperator(5, varchar, "i", true, false);
        // the source column is an INT in Iceberg: no time range can be put on it
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(new BinaryPredicateOperator(BinaryType.EQ,
                year(s), ConstantOperator.createInt(2024))));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // the expected values below come from the FE constant-folding implementations of the functions themselves
    // ---------------------------------------------------------------------------------------------------------------

    private static ConstantOperator datetime(LocalDateTime t) {
        return ConstantOperator.createDatetime(t);
    }

    @Test
    public void testDateFormat() {
        List<String[]> cases = List.of(
                new String[] {"%Y", "2024"}, new String[] {"%Y-%m", "2024-03"}, new String[] {"%Y%m", "202412"},
                new String[] {"%Y-%m-%d", "2024-02-29"}, new String[] {"%Y%m%d", "20240101"},
                new String[] {"%Y-%m-%d %H", "2024-02-15 10"}, new String[] {"%Y%m%d%H", "2024010100"},
                new String[] {"%Y-%m-%d %H:%i", "2024-02-15 10:01"}, new String[] {"%Y-%m-%d %H:%i:%s", "2024-04-01 00:00:00"});
        for (String[] c : cases) {
            ConstantOperator format = ConstantOperator.createVarchar(c[0]);
            for (BinaryType op : OPS) {
                for (ScalarOperator arg : List.of(TS, toDatetime(D))) {
                    boolean isDate = arg != TS;
                    Expression derived = convert(new BinaryPredicateOperator(op,
                            new CallOperator("date_format", VarcharType.VARCHAR, List.of(arg, format)),
                            ConstantOperator.createVarchar(c[1])));
                    Function<LocalDateTime, Boolean> expected = t -> compare(op,
                            ScalarOperatorFunctions.dateFormat(datetime(t), format).getVarchar().compareTo(c[1]));
                    String what = "date_format(" + arg + ", " + c[0] + ") " + op + " " + c[1];
                    boolean pruning = isDate
                            ? checkSound("d", derived, dates(), IcebergFunctionPreimageTest::dateValue, expected, what)
                            : checkSound("ts", derived, datetimes(), t -> micros(t, ZoneOffset.UTC), expected, what);
                    Assertions.assertTrue(pruning || (isDate && op == BinaryType.EQ && c[0].contains("%H")), what);
                }
            }
        }
        // a non-canonical spelling does not sort like the time it names
        for (String text : List.of("2024-3", "24-03", "2024-13", "2024-03 ")) {
            Assertions.assertEquals(Expressions.alwaysTrue(), convert(new BinaryPredicateOperator(BinaryType.GE,
                    new CallOperator("date_format", VarcharType.VARCHAR,
                            List.of(TS, ConstantOperator.createVarchar("%Y-%m"))), ConstantOperator.createVarchar(text))));
        }
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("date_format", VarcharType.VARCHAR, List.of(TS, ConstantOperator.createVarchar("%m"))),
                ConstantOperator.createVarchar("03"))));
    }

    @Test
    public void testDateDiff() {
        List<LocalDateTime> constants = List.of(LocalDateTime.of(2024, 3, 1, 0, 0), LocalDateTime.of(2024, 2, 15, 23, 59, 59));
        for (LocalDateTime k : constants) {
            for (int n : new int[] {-3, 0, 1, 30}) {
                for (BinaryType op : OPS) {
                    for (boolean columnFirst : new boolean[] {true, false}) {
                        for (ScalarOperator arg : List.of(TS, toDatetime(D))) {
                            List<ScalarOperator> args = columnFirst ? List.of(arg, datetime(k)) : List.of(datetime(k), arg);
                            Expression derived = convert(new BinaryPredicateOperator(op,
                                    new CallOperator("datediff", IntegerType.INT, args), ConstantOperator.createInt(n)));
                            Function<LocalDateTime, Boolean> expected = t -> compare(op, Integer.compare(columnFirst
                                    ? ScalarOperatorFunctions.dateDiff(datetime(t), datetime(k)).getInt()
                                    : ScalarOperatorFunctions.dateDiff(datetime(k), datetime(t)).getInt(), n));
                            String what = "datediff " + columnFirst + " " + arg + " " + op + " " + n + " " + k;
                            Assertions.assertTrue(arg == TS
                                    ? checkSound("ts", derived, datetimes(), t -> micros(t, ZoneOffset.UTC), expected, what)
                                    : checkSound("d", derived, dates(), IcebergFunctionPreimageTest::dateValue, expected,
                                    what), what);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testShift() {
        String[] fns = {"weeks_add", "weeks_sub", "hours_add", "hours_sub", "minutes_add", "minutes_sub",
                "seconds_add", "seconds_sub"};
        ChronoUnit[] units = {ChronoUnit.WEEKS, ChronoUnit.WEEKS, ChronoUnit.HOURS, ChronoUnit.HOURS,
                ChronoUnit.MINUTES, ChronoUnit.MINUTES, ChronoUnit.SECONDS, ChronoUnit.SECONDS};
        LocalDateTime k = LocalDateTime.of(2024, 2, 15, 10, 0, 30);
        for (int f = 0; f < fns.length; f++) {
            String fn = fns[f];
            ChronoUnit unit = units[f];
            int n = 3;
            for (BinaryType op : OPS) {
                for (ScalarOperator arg : List.of(TS, toDatetime(D))) {
                    Expression derived = convert(new BinaryPredicateOperator(op,
                            new CallOperator(fn, DateType.DATETIME, List.of(arg, ConstantOperator.createInt(n))), datetime(k)));
                    Function<LocalDateTime, Boolean> expected = t -> compare(op,
                            (fn.endsWith("_add") ? t.plus(n, unit) : t.minus(n, unit)).compareTo(k));
                    String what = fn + "(" + arg + ", 3) " + op + " " + k;
                    boolean pruning = arg == TS
                            ? checkSound("ts", derived, datetimes(), t -> micros(t, ZoneOffset.UTC), expected, what)
                            : checkSound("d", derived, dates(), IcebergFunctionPreimageTest::dateValue, expected, what);
                    Assertions.assertTrue(pruning || op == BinaryType.EQ, what);
                }
            }
        }
        // a subtracted INT_MIN overflows in the BE, so it is left unpruned
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(new BinaryPredicateOperator(BinaryType.LT,
                new CallOperator("hours_sub", DateType.DATETIME, List.of(TS, ConstantOperator.createInt(Integer.MIN_VALUE))),
                datetime(k))));
        // the reference: the FE folding agrees with the java arithmetic used above
        Assertions.assertEquals(k.plusHours(3),
                ScalarOperatorFunctions.hoursAdd(datetime(k), ConstantOperator.createInt(3)).getDatetime());
        Assertions.assertEquals(k.minusSeconds(3),
                ScalarOperatorFunctions.secondsSub(datetime(k), ConstantOperator.createInt(3)).getDatetime());
    }

    private static final List<String> STRINGS = List.of("", "U", "UN", "UNI", "UNIT", "UNITED", "UNJ", "UMZ", "CHINA",
            "中", "中国", "中国人", "中国人民", "中华", "Ünï", "Ünïx");

    @Test
    public void testStringPrefix() {
        for (String x : List.of("", "U", "UN", "UNI", "UNIT", "中国", "中国人", "Ünï")) {
            ConstantOperator value = ConstantOperator.createVarchar(x);
            for (int n = 1; n <= 4; n++) {
                ConstantOperator len = ConstantOperator.createInt(n);
                List<CallOperator> calls = List.of(new CallOperator("left", VarcharType.VARCHAR, List.of(S, len)),
                        new CallOperator("substr", VarcharType.VARCHAR, List.of(S, ConstantOperator.createInt(1), len)),
                        new CallOperator("substring", VarcharType.VARCHAR, List.of(S, ConstantOperator.createInt(1), len)));
                for (CallOperator call : calls) {
                    Expression derived = convert(new BinaryPredicateOperator(BinaryType.EQ, call, value));
                    int width = n;
                    String what = call + " = '" + x + "'";
                    boolean pruning = checkSound("s", derived, STRINGS, v -> v, v -> ScalarOperatorFunctions.substring(
                            ConstantOperator.createVarchar(v), ConstantOperator.createInt(1),
                            ConstantOperator.createInt(width)).getVarchar().equals(x), what);
                    Assertions.assertEquals(x.codePointCount(0, x.length()) <= n, pruning, what);
                }
            }
            Expression derived = convert(new CallOperator("starts_with", BooleanType.BOOLEAN, List.of(S, value)));
            Assertions.assertTrue(checkSound("s", derived, STRINGS, v -> v, v -> v.startsWith(x), "starts_with " + x));
        }
        // substr from another position, or a range comparison, is not a prefix test
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("substr", VarcharType.VARCHAR, List.of(S, ConstantOperator.createInt(2),
                        ConstantOperator.createInt(2))), ConstantOperator.createVarchar("NI"))));
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(new BinaryPredicateOperator(BinaryType.GE,
                new CallOperator("left", VarcharType.VARCHAR, List.of(S, ConstantOperator.createInt(2))),
                ConstantOperator.createVarchar("UN"))));
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT,
                new CallOperator("starts_with", BooleanType.BOOLEAN, List.of(S, ConstantOperator.createVarchar("U"))))));
    }

    @Test
    public void testYearAndMonth() {
        ScalarOperator year2024 = new BinaryPredicateOperator(BinaryType.EQ, year(D), ConstantOperator.createInt(2024));
        ScalarOperator monthIn = new InPredicateOperator(false, new CallOperator("month", IntegerType.TINYINT,
                List.of(D)), ConstantOperator.createInt(2), ConstantOperator.createInt(12));
        ScalarOperator quarter2 = new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("quarter", IntegerType.INT, List.of(D)), ConstantOperator.createInt(2));

        Expression derived = converter.convert(List.of(year2024, monthIn), context);
        Assertions.assertTrue(checkSound("d", derived, dates(), IcebergFunctionPreimageTest::dateValue,
                t -> t.getYear() == 2024 && (t.getMonthValue() == 2 || t.getMonthValue() == 12), "year and month in"));

        derived = converter.convert(List.of(quarter2, year2024), context);
        Assertions.assertTrue(checkSound("d", derived, dates(), IcebergFunctionPreimageTest::dateValue,
                t -> t.getYear() == 2024 && t.getMonthValue() >= 4 && t.getMonthValue() <= 6, "quarter and year"));
        Assertions.assertTrue(derived.toString().contains(String.valueOf(dateValue(LocalDateTime.of(2024, 7, 1, 0, 0)))),
                derived.toString());

        // a month without a year only gets nothing beyond what each conjunct gives
        Assertions.assertEquals(Expressions.alwaysTrue(), converter.convert(List.of(monthIn), context));
        // an impossible month leaves the year range alone
        ScalarOperator month13 = new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("month", IntegerType.TINYINT, List.of(D)), ConstantOperator.createInt(13));
        Assertions.assertEquals(convert(year2024).toString(),
                converter.convert(List.of(year2024, month13), context).toString());
        // strict conversion never uses preimages
        Assertions.assertNull(converter.convertStrict(List.of(year2024, monthIn), context));
    }

    @Test
    public void testNoSessionAndLongInList() {
        ScalarOperator yearEq = new BinaryPredicateOperator(BinaryType.EQ, year(D), ConstantOperator.createInt(2024));
        Assertions.assertNotEquals(Expressions.alwaysTrue(), convert(yearEq));
        // e.g. a metadata-prefetch thread: neither the switch nor the session time zone is known
        ConnectContext.remove();
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(yearEq));
        new ConnectContext().setThreadLocalInfo();

        // the IN operator's children: the operand, then the values
        List<ScalarOperator> in129 = new ArrayList<>();
        in129.add(year(D));
        for (int y = 1000; y < 1000 + 129; y++) {
            in129.add(ConstantOperator.createInt(y));
        }
        Assertions.assertEquals(Expressions.alwaysTrue(), convert(new InPredicateOperator(false, in129)));
        Assertions.assertNotEquals(Expressions.alwaysTrue(),
                convert(new InPredicateOperator(false, in129.subList(0, 129))));
    }
}
