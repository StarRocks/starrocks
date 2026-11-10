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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.Column;
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
import com.starrocks.sql.optimizer.rewrite.ExternalScanPredicates.Connector;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExternalScanPredicatesTest {
    // dt: DATE partition column, ts: DATETIME data column, sp: STRING identity partition column
    private static final ColumnRefOperator DT = new ColumnRefOperator(1, DateType.DATE, "dt", true);
    private static final ColumnRefOperator TS = new ColumnRefOperator(2, DateType.DATETIME, "ts", true);
    private static final ColumnRefOperator SP = new ColumnRefOperator(3, VarcharType.VARCHAR, "sp", true);
    private static final Map<ColumnRefOperator, Column> COLUMNS = Map.of(
            DT, new Column("dt", DateType.DATE),
            TS, new Column("ts", DateType.DATETIME),
            SP, new Column("sp", VarcharType.VARCHAR));

    // year(dt) IN (2023, 2025): an OR on a partition column
    private static final ScalarOperator YEAR_IN = new InPredicateOperator(false,
            new CallOperator("year", IntegerType.SMALLINT, List.of(DT)),
            ConstantOperator.createInt(2023), ConstantOperator.createInt(2025));
    // year(ts) = 2024: a range on a data column
    private static final ScalarOperator YEAR_EQ = new BinaryPredicateOperator(BinaryType.EQ,
            new CallOperator("year", IntegerType.SMALLINT, List.of(TS)), ConstantOperator.createInt(2024));
    // CAST(sp AS DATETIME) = '2020-06-14 00:00:00': a string partition column against a temporal value
    private static final ScalarOperator CAST_EQ = new BinaryPredicateOperator(BinaryType.EQ,
            new CastOperator(DateType.DATETIME, SP),
            ConstantOperator.createDatetime(LocalDateTime.of(2020, 6, 14, 0, 0, 0)));
    // NOT year(ts) = 2024: nothing may be derived under NOT
    private static final ScalarOperator NOT_YEAR_EQ = new CompoundPredicateOperator(
            CompoundPredicateOperator.CompoundType.NOT, YEAR_EQ);
    private static final List<ScalarOperator> CONJUNCTS = List.of(YEAR_IN, YEAR_EQ, CAST_EQ, NOT_YEAR_EQ);

    @BeforeEach
    public void setUp() {
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static List<String> strings(List<ScalarOperator> predicates) {
        return predicates.stream().map(ScalarOperator::toString).collect(Collectors.toList());
    }

    private static boolean onlyOn(List<ScalarOperator> predicates, ColumnRefOperator... columns) {
        Set<ColumnRefOperator> allowed = Set.of(columns);
        return predicates.stream().allMatch(p -> allowed.containsAll(p.getColumnRefs()));
    }

    @Test
    public void testPerPruningLevel() {
        FunctionPreimage.ColumnResolver scan = ExternalScanPredicates.scanTemporalColumns(COLUMNS);

        // file skipping: the IN ranges (one OR on dt) and the year range on ts
        List<ScalarOperator> files = ExternalScanPredicates.forFileSkipping(Connector.ICEBERG, CONJUNCTS, scan);
        Assertions.assertEquals(3, files.size(), files.toString());
        Assertions.assertTrue(FunctionPreimage.isDisjunction(files.get(0)), files.toString());
        Assertions.assertEquals(List.of("2: ts >= 2024-01-01 00:00:00", "2: ts < 2025-01-01 00:00:00"),
                strings(files.subList(1, 3)));

        // partition pruning keeps the OR only because it is on the partition column alone
        Assertions.assertEquals(files,
                ExternalScanPredicates.forPartitionPruning(Connector.HIVE, CONJUNCTS, scan, Set.of(DT)));
        Assertions.assertEquals(files.subList(1, 3),
                ExternalScanPredicates.forPartitionPruning(Connector.HIVE, CONJUNCTS, scan, Set.of(TS)));

        // BE conjuncts: never an OR
        Assertions.assertEquals(files.subList(1, 3),
                ExternalScanPredicates.forBeConjuncts(Connector.DELTA_LAKE, CONJUNCTS, scan));

        // nothing lands on the string partition column that has a cast residual
        Assertions.assertTrue(onlyOn(files, DT, TS), files.toString());
    }

    @Test
    public void testStringPartitionCastSplit() {
        ExternalScanPredicates.PartitionResidual split =
                ExternalScanPredicates.splitStringPartitionCast(CONJUNCTS, Set.of("SP"));
        Assertions.assertEquals(List.of(YEAR_IN, YEAR_EQ, NOT_YEAR_EQ), split.pushable);
        Assertions.assertEquals(List.of(CAST_EQ), split.residual);

        // derivation on the pushable part puts nothing on the string partition column either
        List<ScalarOperator> files = ExternalScanPredicates.forFileSkipping(Connector.ICEBERG, split.pushable,
                ExternalScanPredicates.scanTemporalColumns(COLUMNS));
        Assertions.assertTrue(onlyOn(files, DT, TS), files.toString());
    }

    private static ScalarOperator substrDate(ColumnRefOperator column) {
        return new CallOperator("substr", VarcharType.VARCHAR, List.of(new CastOperator(VarcharType.VARCHAR, column),
                ConstantOperator.createInt(1), ConstantOperator.createInt(10)));
    }

    @Test
    public void testDateTextShapes() {
        FunctionPreimage.ColumnResolver scan = ExternalScanPredicates.scanTemporalColumns(COLUMNS);
        // substr(cast(ts as varchar), 1, 10) = '2023-03-27': left alone by SimplifiedDateColumnPredicateRule on a
        // DATETIME, so only the preimage prunes on it
        ScalarOperator substrEq = new BinaryPredicateOperator(BinaryType.EQ, substrDate(TS),
                ConstantOperator.createVarchar("2023-03-27"));
        Assertions.assertEquals(List.of("2: ts >= 2023-03-27 00:00:00", "2: ts < 2023-03-28 00:00:00"),
                strings(ExternalScanPredicates.forBeConjuncts(Connector.ICEBERG, List.of(substrEq), scan)));
        // replace(substr(cast(dt as varchar), 1, 10), '-', '') = '20230327'
        ScalarOperator replaceEq = new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("replace", VarcharType.VARCHAR, List.of(substrDate(DT),
                        ConstantOperator.createVarchar("-"), ConstantOperator.createVarchar(""))),
                ConstantOperator.createVarchar("20230327"));
        Assertions.assertEquals(List.of("1: dt >= 2023-03-27", "1: dt < 2023-03-28"),
                strings(ExternalScanPredicates.forBeConjuncts(Connector.ICEBERG, List.of(replaceEq), scan)));
        // a substr that is not the date part, or a non-canonical constant, derives nothing
        ScalarOperator substr7 = new BinaryPredicateOperator(BinaryType.EQ,
                new CallOperator("substr", VarcharType.VARCHAR, List.of(new CastOperator(VarcharType.VARCHAR, TS),
                        ConstantOperator.createInt(1), ConstantOperator.createInt(7))),
                ConstantOperator.createVarchar("2023-03"));
        ScalarOperator loose = new BinaryPredicateOperator(BinaryType.EQ, substrDate(TS),
                ConstantOperator.createVarchar("2023-3-27"));
        Assertions.assertTrue(ExternalScanPredicates.forFileSkipping(Connector.ICEBERG, List.of(substr7, loose), scan)
                .isEmpty());
    }

    @Test
    public void testSwitchPerConnector() {
        FunctionPreimage.ColumnResolver scan = ExternalScanPredicates.scanTemporalColumns(COLUMNS);
        ConnectContext.get().getSessionVariable().setEnableHiveFunctionPredicatePruning(false);
        Assertions.assertTrue(ExternalScanPredicates.forPartitionPruning(Connector.HIVE, CONJUNCTS, scan, Set.of(DT))
                .isEmpty());
        Assertions.assertFalse(ExternalScanPredicates.forBeConjuncts(Connector.ICEBERG, CONJUNCTS, scan).isEmpty());

        ConnectContext.remove();
        for (Connector connector : Connector.values()) {
            Assertions.assertFalse(ExternalScanPredicates.isFunctionPruningEnabled(connector));
            Assertions.assertTrue(ExternalScanPredicates.forFileSkipping(connector, CONJUNCTS, scan).isEmpty());
        }
    }

    @Test
    public void testResolverErrorDerivesNothing() {
        FunctionPreimage.ColumnResolver failing = (operand, kind) -> {
            throw new IllegalStateException("boom");
        };
        Assertions.assertTrue(ExternalScanPredicates.forFileSkipping(Connector.DELTA_LAKE, CONJUNCTS, failing)
                .isEmpty());
        Assertions.assertTrue(ExternalScanPredicates.forBeConjuncts(Connector.DELTA_LAKE, CONJUNCTS, failing)
                .isEmpty());
    }
}
