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

package com.starrocks.connector.delta;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import io.delta.kernel.expressions.AlwaysTrue;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DeltaLakePreimagePredicatesTest {
    private static final StructType SCHEMA = new StructType()
            .add("d", DateType.DATE)
            .add("ts", TimestampType.TIMESTAMP)
            .add("s", StringType.STRING);

    private static final ColumnRefOperator D = new ColumnRefOperator(1, com.starrocks.type.DateType.DATE, "d", true);
    private static final ColumnRefOperator TS =
            new ColumnRefOperator(2, com.starrocks.type.DateType.DATETIME, "ts", true);
    private static final ColumnRefOperator S = new ColumnRefOperator(3, VarcharType.VARCHAR, "s", true);

    @BeforeEach
    public void setUp() {
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static ScalarOperator yearEq(ColumnRefOperator column, int year) {
        return new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("year", IntegerType.SMALLINT, List.of(column)), ConstantOperator.createInt(year));
    }

    private static List<String> strings(List<ScalarOperator> predicates) {
        return predicates.stream().map(ScalarOperator::toString).collect(Collectors.toList());
    }

    @Test
    public void testFileSkipping() {
        Assertions.assertEquals(List.of("1: d >= 2024-01-01", "1: d < 2025-01-01"),
                strings(DeltaLakePreimagePredicates.forFileSkipping(List.of(yearEq(D, 2024)), SCHEMA)));
        // a shorter left() result is the whole string
        ScalarOperator left = new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("left", VarcharType.VARCHAR, List.of(S, ConstantOperator.createInt(5))),
                ConstantOperator.createVarchar("ab"));
        Assertions.assertEquals(List.of("3: s = ab"),
                strings(DeltaLakePreimagePredicates.forFileSkipping(List.of(left), SCHEMA)));
        // a column the Delta schema does not have
        ColumnRefOperator other = new ColumnRefOperator(4, com.starrocks.type.DateType.DATE, "other", true);
        Assertions.assertTrue(DeltaLakePreimagePredicates.forFileSkipping(List.of(yearEq(other, 2024)), SCHEMA)
                .isEmpty());
    }

    @Test
    public void testScanIsTemporalOnly() {
        Map<ColumnRefOperator, com.starrocks.catalog.Column> columns = Map.of(
                D, new com.starrocks.catalog.Column("d", com.starrocks.type.DateType.DATE),
                S, new com.starrocks.catalog.Column("s", VarcharType.VARCHAR));
        Assertions.assertEquals(List.of("1: d >= 2024-01-01", "1: d < 2025-01-01"),
                strings(DeltaLakePreimagePredicates.forScan(List.of(yearEq(D, 2024)), columns)));
        ScalarOperator left = new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("left", VarcharType.VARCHAR, List.of(S, ConstantOperator.createInt(5))),
                ConstantOperator.createVarchar("ab"));
        Assertions.assertTrue(DeltaLakePreimagePredicates.forScan(List.of(left), columns).isEmpty());
    }

    @Test
    public void testInListSkipsFilesOnly() {
        ScalarOperator in = new InPredicateOperator(false,
                new CallOperator("year", IntegerType.SMALLINT, List.of(D)),
                ConstantOperator.createInt(2023), ConstantOperator.createInt(2025));
        // the kernel skips files on the OR of the IN ranges
        List<ScalarOperator> files = DeltaLakePreimagePredicates.forFileSkipping(List.of(in), SCHEMA);
        Assertions.assertEquals(1, files.size(), files.toString());
        Assertions.assertTrue(((CompoundPredicateOperator) files.get(0)).isOr(), files.toString());
        // the BE would evaluate that OR on every row next to the IN itself
        Assertions.assertTrue(DeltaLakePreimagePredicates.forScan(List.of(in),
                Map.of(D, new com.starrocks.catalog.Column("d", com.starrocks.type.DateType.DATE))).isEmpty());
    }

    @Test
    public void testSwitchAndSession() {
        ConnectContext.get().getSessionVariable().setEnableDeltaLakeFunctionPredicatePruning(false);
        Assertions.assertTrue(DeltaLakePreimagePredicates.forFileSkipping(List.of(yearEq(D, 2024)), SCHEMA)
                .isEmpty());
        Assertions.assertTrue(DeltaLakePreimagePredicates.forScan(List.of(yearEq(D, 2024)),
                Map.of(D, new com.starrocks.catalog.Column("d", com.starrocks.type.DateType.DATE))).isEmpty());
        ConnectContext.remove();
        Assertions.assertTrue(DeltaLakePreimagePredicates.forFileSkipping(List.of(yearEq(D, 2024)), SCHEMA)
                .isEmpty());
    }

    /**
     * The derived predicates reach the kernel through the existing converter: a DATE range, and on a TIMESTAMP column
     * a local time at a DST overlap widened to both instants.
     */
    @Test
    public void testConvertedForKernel() {
        ScalarOperationToDeltaLakeExpr.DeltaLakeContext context =
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(SCHEMA, Set.of());
        List<ScalarOperator> derived = DeltaLakePreimagePredicates.forFileSkipping(List.of(yearEq(D, 2024)), SCHEMA);
        Predicate predicate = new ScalarOperationToDeltaLakeExpr().convert(derived, context);
        Column d = new Column("d");
        Assertions.assertEquals(new And(
                new Predicate(">=", d, Literal.ofDate((int) LocalDate.of(2024, 1, 1).toEpochDay())),
                new Predicate("<", d, Literal.ofDate((int) LocalDate.of(2025, 1, 1).toEpochDay()))).toString(),
                predicate.toString());

        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("America/New_York");
        ctx.setThreadLocalInfo();
        // date_trunc('hour', ts) = '2024-11-03 01:00:00' holds from 01:00 EDT (05:00Z) to 02:00 EST (07:00Z)
        ScalarOperator hour = new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("date_trunc", com.starrocks.type.DateType.DATETIME,
                        List.of(ConstantOperator.createVarchar("hour"), TS)),
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 11, 3, 1, 0)));
        derived = new ArrayList<>(DeltaLakePreimagePredicates.forFileSkipping(List.of(hour), SCHEMA));
        Assertions.assertEquals(2, derived.size(), derived.toString());
        String converted = new ScalarOperationToDeltaLakeExpr().convert(derived, context).toString();
        Assertions.assertTrue(converted.contains(">= " + micros("2024-11-03T05:00:00Z")), converted);
        Assertions.assertTrue(converted.contains("< " + micros("2024-11-03T07:00:00Z")), converted);
    }

    /**
     * left(s, 3) = 'abc' derives starts_with(s, 'abc'), which reaches the kernel as STARTS_WITH on a partition column;
     * the kernel's data skipping does not evaluate STARTS_WITH, so on a data column nothing is pushed.
     */
    @Test
    public void testPrefixConvertedForKernel() {
        ScalarOperator left = new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("left", VarcharType.VARCHAR, List.of(S, ConstantOperator.createInt(3))),
                ConstantOperator.createVarchar("abc"));
        List<ScalarOperator> derived = DeltaLakePreimagePredicates.forFileSkipping(List.of(left), SCHEMA);
        Assertions.assertEquals(List.of("starts_with(3: s, abc)"), strings(derived));

        Predicate partition = new ScalarOperationToDeltaLakeExpr().convert(derived,
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(SCHEMA, Set.of("s")));
        Assertions.assertEquals(new Predicate("STARTS_WITH", new Column("s"), Literal.ofString("abc")).toString(),
                partition.toString());
        Predicate data = new ScalarOperationToDeltaLakeExpr().convert(derived,
                new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(SCHEMA, Set.of()));
        Assertions.assertEquals(AlwaysTrue.ALWAYS_TRUE, data);
    }

    private static long micros(String instant) {
        return Instant.parse(instant).getEpochSecond() * 1_000_000L;
    }
}
