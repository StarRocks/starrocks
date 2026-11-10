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
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class IcebergScanPreimagesTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "d", Types.DateType.get()),
            Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(3, "tz", Types.TimestampType.withZone()),
            Types.NestedField.optional(4, "s", Types.StringType.get()));

    private static final ColumnRefOperator D = new ColumnRefOperator(1, DateType.DATE, "d", true, false);
    private static final ColumnRefOperator TS = new ColumnRefOperator(2, DateType.DATETIME, "ts", true, false);
    private static final ColumnRefOperator TZ = new ColumnRefOperator(3, DateType.DATETIME, "tz", true, false);
    private static final ColumnRefOperator S = new ColumnRefOperator(4, VarcharType.VARCHAR, "s", true, false);
    private static final Map<ColumnRefOperator, Column> COLUMNS = Map.of(
            D, new Column("d", DateType.DATE),
            TS, new Column("ts", DateType.DATETIME),
            TZ, new Column("tz", DateType.DATETIME),
            S, new Column("s", VarcharType.VARCHAR));

    private static final BinaryType[] OPS = {BinaryType.EQ, BinaryType.GE, BinaryType.GT, BinaryType.LT, BinaryType.LE};

    @BeforeEach
    public void setUp() {
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static List<ScalarOperator> derive(ScalarOperator... conjuncts) {
        return ScalarOperatorToIcebergExpr.scanPreimages(List.of(conjuncts), SCHEMA.asStruct(), COLUMNS);
    }

    private static List<String> strings(List<ScalarOperator> predicates) {
        return predicates.stream().map(ScalarOperator::toString).collect(Collectors.toList());
    }

    private static CallOperator year(ScalarOperator arg) {
        return new CallOperator("year", IntegerType.SMALLINT, List.of(arg));
    }

    private static CallOperator month(ScalarOperator arg) {
        return new CallOperator("month", IntegerType.TINYINT, List.of(arg));
    }

    private static CallOperator dateTrunc(String unit, ScalarOperator arg) {
        return new CallOperator("date_trunc", arg.getType(), List.of(ConstantOperator.createVarchar(unit), arg));
    }

    private static ConstantOperator datetime(LocalDateTime t) {
        return ConstantOperator.createDatetime(t);
    }

    @Test
    public void testYearOnDate() {
        Assertions.assertEquals(List.of("1: d >= 2024-01-01", "1: d < 2025-01-01"),
                strings(derive(new BinaryPredicateOperator(BinaryType.EQ, year(D), ConstantOperator.createInt(2024)))));
    }

    @Test
    public void testDateTruncOnTimestamp() {
        Assertions.assertEquals(List.of("2: ts >= 2024-11-01 00:00:00", "2: ts < 2024-11-02 00:00:00"),
                strings(derive(new BinaryPredicateOperator(BinaryType.EQ, dateTrunc("day", TS),
                        datetime(LocalDateTime.of(2024, 11, 1, 0, 0))))));
    }

    @Test
    public void testInListAndConjunctPair() {
        // the OR over the IN values would be evaluated on every row next to the IN itself, so it is not derived
        Assertions.assertTrue(derive(new InPredicateOperator(false, year(D),
                ConstantOperator.createInt(2023), ConstantOperator.createInt(2025))).isEmpty());

        // year(d) = 2024 and month(d) = 3 narrows to March, and the year range is not duplicated
        List<String> pair = strings(derive(
                new BinaryPredicateOperator(BinaryType.EQ, year(D), ConstantOperator.createInt(2024)),
                new BinaryPredicateOperator(BinaryType.EQ, month(D), ConstantOperator.createInt(3))));
        Assertions.assertTrue(pair.contains("1: d >= 2024-03-01"), pair.toString());
        Assertions.assertTrue(pair.contains("1: d < 2024-04-01"), pair.toString());
        Assertions.assertEquals(pair.size(), pair.stream().distinct().count(), pair.toString());
    }

    @Test
    public void testNothingDerived() {
        // a bare column predicate is already usable by the BE
        Assertions.assertTrue(derive(new BinaryPredicateOperator(BinaryType.GE, D,
                ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0)))).isEmpty());
        // NOT IN has no superset to derive
        Assertions.assertTrue(derive(new InPredicateOperator(true, year(D), ConstantOperator.createInt(2024)))
                .isEmpty());
        // string prefixes are not handled
        Assertions.assertTrue(derive(new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("left", VarcharType.VARCHAR, List.of(S, ConstantOperator.createInt(3))),
                ConstantOperator.createVarchar("abc"))).isEmpty());
        // switched off with the function predicate pruning switch
        ConnectContext.get().getSessionVariable().setEnableIcebergFunctionPredicatePruning(false);
        Assertions.assertTrue(derive(new BinaryPredicateOperator(BinaryType.EQ, year(D),
                ConstantOperator.createInt(2024))).isEmpty());
    }

    /**
     * Every value matching the original predicate must match all derived predicates.
     */
    private static void assertSound(ScalarOperator predicate, ColumnRefOperator column, List<LocalDateTime> values,
                                    Predicate<LocalDateTime> matches) {
        List<ScalarOperator> derived = derive(predicate);
        Assertions.assertFalse(derived.isEmpty(), predicate.toString());
        for (LocalDateTime v : values) {
            if (!matches.test(v)) {
                continue;
            }
            ConstantOperator value = column.getType().isDate()
                    ? ConstantOperator.createDate(v.truncatedTo(ChronoUnit.DAYS)) : datetime(v);
            for (ScalarOperator p : derived) {
                Assertions.assertTrue(eval(p, value), predicate + " matches " + v + " but " + p + " does not");
            }
        }
    }

    private static boolean eval(ScalarOperator p, ConstantOperator value) {
        if (p instanceof CompoundPredicateOperator compound) {
            boolean left = eval(compound.getChild(0), value);
            boolean right = eval(compound.getChild(1), value);
            return compound.isAnd() ? left && right : left || right;
        }
        BinaryPredicateOperator binary = (BinaryPredicateOperator) p;
        return compare(binary.getBinaryType(), value.compareTo((ConstantOperator) binary.getChild(1)));
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

    @Test
    public void testSoundOnDateAndTimestamp() {
        List<LocalDateTime> values = new ArrayList<>();
        for (LocalDateTime t = LocalDateTime.of(2023, 12, 30, 0, 0); t.isBefore(LocalDateTime.of(2024, 1, 3, 0, 0));
                t = t.plusMinutes(37)) {
            values.add(t);
        }
        for (BinaryType op : OPS) {
            for (int y = 2023; y <= 2024; y++) {
                int k = y;
                assertSound(new BinaryPredicateOperator(op, year(D), ConstantOperator.createInt(y)), D, values,
                        t -> compare(op, Integer.compare(t.getYear(), k)));
                assertSound(new BinaryPredicateOperator(op, year(TS), ConstantOperator.createInt(y)), TS, values,
                        t -> compare(op, Integer.compare(t.getYear(), k)));
            }
            for (LocalDateTime c : List.of(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2024, 1, 1, 5, 0))) {
                assertSound(new BinaryPredicateOperator(op, dateTrunc("hour", TS), datetime(c)), TS, values,
                        t -> compare(op, t.truncatedTo(ChronoUnit.HOURS).compareTo(c)));
            }
        }
    }

    @Test
    public void testSoundOnTimestampTzAcrossDst() {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("America/New_York");
        ctx.setThreadLocalInfo();
        ZoneId zone = ZoneId.of("America/New_York");
        // the session reads each instant as its local time: 2024-11-03 01:xx happens twice, 2024-03-10 02:xx never
        List<LocalDateTime> values = new ArrayList<>();
        for (List<String> range : List.of(List.of("2024-11-03T03:00:00Z", "2024-11-03T09:00:00Z"),
                List.of("2024-03-10T04:00:00Z", "2024-03-10T10:00:00Z"))) {
            for (Instant t = Instant.parse(range.get(0)); t.isBefore(Instant.parse(range.get(1)));
                    t = t.plusSeconds(600)) {
                values.add(LocalDateTime.ofInstant(t, zone));
            }
        }
        List<LocalDateTime> constants = List.of(LocalDateTime.of(2024, 11, 3, 1, 0), LocalDateTime.of(2024, 11, 3, 2, 0),
                LocalDateTime.of(2024, 3, 10, 2, 0), LocalDateTime.of(2024, 3, 10, 3, 0));
        for (BinaryType op : OPS) {
            for (LocalDateTime c : constants) {
                assertSound(new BinaryPredicateOperator(op, dateTrunc("hour", TZ), datetime(c)), TZ, values,
                        t -> compare(op, t.truncatedTo(ChronoUnit.HOURS).compareTo(c)));
            }
        }
    }
}
