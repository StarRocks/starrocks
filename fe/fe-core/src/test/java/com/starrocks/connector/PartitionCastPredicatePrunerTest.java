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

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.connector.PartitionCastPredicatePruner.PartitionResidual;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionCastPredicatePrunerTest {

    private final ColumnRefOperator strPartCol = new ColumnRefOperator(1, VarcharType.VARCHAR, "c2", true);
    private final ColumnRefOperator dateCol = new ColumnRefOperator(2, DateType.DATE, "d", true);
    private final ColumnRefOperator intCol = new ColumnRefOperator(3, IntegerType.INT, "id", true);

    private ConstantOperator datetime(String v) {
        // v like "2020-06-14 00:00:00"
        String[] parts = v.split("[ :-]");
        return ConstantOperator.createDatetime(LocalDateTime.of(
                Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
                Integer.parseInt(parts[3]), Integer.parseInt(parts[4]), Integer.parseInt(parts[5])));
    }

    private BinaryPredicateOperator eq(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, right);
    }

    private CastOperator castTo(ColumnRefOperator col, DateType type) {
        return new CastOperator(type, col);
    }

    private Set<String> partitionColumns(String... names) {
        return new HashSet<>(Arrays.asList(names));
    }

    @Test
    public void testContainsStringToTemporalCast() {
        // CAST(<string col> AS DATETIME/DATE) -> true
        Assertions.assertTrue(PartitionCastPredicatePruner.containsStringToTemporalCast(
                eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00"))));
        Assertions.assertTrue(PartitionCastPredicatePruner.containsStringToTemporalCast(
                eq(castTo(strPartCol, DateType.DATE), ConstantOperator.createVarchar("2020-06-14"))));

        // CAST on a DATE column (not a string) -> false; DATE->DATETIME is native-safe.
        Assertions.assertFalse(PartitionCastPredicatePruner.containsStringToTemporalCast(
                eq(castTo(dateCol, DateType.DATETIME), datetime("2020-06-14 00:00:00"))));

        // bare string column, no cast -> false
        Assertions.assertFalse(PartitionCastPredicatePruner.containsStringToTemporalCast(
                eq(strPartCol, ConstantOperator.createVarchar("2020-06-14"))));

        // nested inside OR -> found by recursion
        ScalarOperator or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00")),
                eq(intCol, ConstantOperator.createInt(5)));
        Assertions.assertTrue(PartitionCastPredicatePruner.containsStringToTemporalCast(or));
    }

    @Test
    public void testSplitResidualPushableDropped() {
        Set<String> partCols = partitionColumns("c2");

        // pure-partition cast -> residual
        ScalarOperator residualConjunct = eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00"));
        // no cast -> pushable
        ScalarOperator pushableConjunct = eq(strPartCol, ConstantOperator.createVarchar("2020-06-14"));
        // cast but references a non-partition column (mixed OR) -> dropped (neither pushed nor residual)
        ScalarOperator mixedOr = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00")),
                eq(intCol, ConstantOperator.createInt(5)));

        PartitionResidual result = PartitionCastPredicatePruner.split(
                Lists.newArrayList(residualConjunct, pushableConjunct, mixedOr), partCols);

        Assertions.assertEquals(1, result.residual.size());
        Assertions.assertSame(residualConjunct, result.residual.get(0));
        Assertions.assertEquals(1, result.pushable.size());
        Assertions.assertSame(pushableConjunct, result.pushable.get(0));
        Assertions.assertTrue(result.hasResidual());
        // mixedOr is in neither list
        Assertions.assertFalse(result.pushable.contains(mixedOr));
        Assertions.assertFalse(result.residual.contains(mixedOr));
    }

    @Test
    public void testSplitDataColumnCastDropped() {
        // A string data column (not in the partition set) with a temporal cast is dropped, not pushed.
        ColumnRefOperator strDataCol = new ColumnRefOperator(4, VarcharType.VARCHAR, "s", true);
        ScalarOperator conjunct = eq(castTo(strDataCol, DateType.DATETIME), datetime("2020-06-14 00:00:00"));

        PartitionResidual result = PartitionCastPredicatePruner.split(
                Lists.newArrayList(conjunct), partitionColumns("c2"));

        Assertions.assertTrue(result.pushable.isEmpty());
        Assertions.assertTrue(result.residual.isEmpty());
    }

    @Test
    public void testSplitPartitionColumnsCaseInsensitive() {
        // The column ref is "c2"; the partition set is passed upper-cased. split() must still classify it as
        // residual (membership is case-insensitive; callers need not pre-normalize).
        ScalarOperator conjunct = eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00"));
        PartitionResidual result = PartitionCastPredicatePruner.split(
                Lists.newArrayList(conjunct), partitionColumns("C2"));
        Assertions.assertEquals(1, result.residual.size());
        Assertions.assertTrue(result.pushable.isEmpty());
    }

    @Test
    public void testPurePartitionOrIsResidual() {
        Set<String> partCols = partitionColumns("c2");
        ScalarOperator or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00")),
                eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-15 00:00:00")));

        PartitionResidual result = PartitionCastPredicatePruner.split(Lists.newArrayList(or), partCols);
        Assertions.assertEquals(1, result.residual.size());
        Assertions.assertTrue(result.pushable.isEmpty());
    }

    @Test
    public void testPartitionMayMatch() {
        List<ScalarOperator> residual = Lists.newArrayList(
                eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00")));

        // partition value casts to the constant -> keep
        Assertions.assertTrue(PartitionCastPredicatePruner.partitionMayMatch(
                residual, singletonMap("c2", "2020-06-14")));
        // case-insensitive column name lookup
        Assertions.assertTrue(PartitionCastPredicatePruner.partitionMayMatch(
                residual, singletonMap("C2", "2020-06-14")));

        // partition value does not match -> prune
        Assertions.assertFalse(PartitionCastPredicatePruner.partitionMayMatch(
                residual, singletonMap("c2", "2020-06-15")));
    }

    @Test
    public void testPartitionMayMatchConservativeKeep() {
        List<ScalarOperator> residual = Lists.newArrayList(
                eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00")));

        // empty residual -> keep
        Assertions.assertTrue(PartitionCastPredicatePruner.partitionMayMatch(
                Collections.emptyList(), singletonMap("c2", "2020-06-15")));

        // unparseable partition value (strict parse fails) -> conservatively keep
        Assertions.assertTrue(PartitionCastPredicatePruner.partitionMayMatch(
                residual, singletonMap("c2", "not-a-date")));

        // partition value missing for the referenced column -> unbound -> conservatively keep
        Assertions.assertTrue(PartitionCastPredicatePruner.partitionMayMatch(
                residual, singletonMap("other", "2020-06-14")));

        // null partition value -> conservatively keep
        Map<String, String> withNull = new HashMap<>();
        withNull.put("c2", null);
        Assertions.assertTrue(PartitionCastPredicatePruner.partitionMayMatch(residual, withNull));
    }

    private LocalDateTime dt(int y, int m, int d) {
        return LocalDateTime.of(y, m, d, 0, 0, 0);
    }

    private Map<String, LocalDateTime[]> range(String col, LocalDateTime lo, LocalDateTime hi) {
        Map<String, LocalDateTime[]> map = new HashMap<>();
        map.put(col, new LocalDateTime[] {lo, hi});
        return map;
    }

    private boolean mayMatch(ScalarOperator conjunct, Map<String, LocalDateTime[]> ranges) {
        return PartitionCastPredicatePruner.rangeMayMatch(Lists.newArrayList(conjunct), ranges);
    }

    @Test
    public void testRangeMayMatchEquality() {
        ScalarOperator eqConj = eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00"));
        // single-value range == the value / range contains the value -> may match
        Assertions.assertTrue(mayMatch(eqConj, range("c2", dt(2020, 6, 14), dt(2020, 6, 14))));
        Assertions.assertTrue(mayMatch(eqConj, range("c2", dt(2020, 6, 10), dt(2020, 6, 20))));
        // value above / below the range -> prune
        Assertions.assertFalse(mayMatch(eqConj, range("c2", dt(2020, 6, 15), dt(2020, 6, 20))));
        Assertions.assertFalse(mayMatch(eqConj, range("c2", dt(2020, 1, 1), dt(2020, 6, 13))));
    }

    @Test
    public void testRangeMayMatchRangeComparison() {
        // CAST(c2 AS DATETIME) >= '2020-06-15'
        ScalarOperator geConj = new BinaryPredicateOperator(BinaryType.GE,
                castTo(strPartCol, DateType.DATETIME), datetime("2020-06-15 00:00:00"));
        Assertions.assertTrue(mayMatch(geConj, range("c2", dt(2020, 6, 10), dt(2020, 6, 20))));
        // whole range below the bound -> prune
        Assertions.assertFalse(mayMatch(geConj, range("c2", dt(2020, 1, 1), dt(2020, 6, 10))));
    }

    @Test
    public void testRangeMayMatchConservativeKeep() {
        ScalarOperator eqConj = eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00"));
        // no range known for the referenced column -> keep
        Assertions.assertTrue(mayMatch(eqConj, range("other", dt(2020, 1, 1), dt(2020, 1, 1))));
        // non-binary conjunct (OR) -> keep
        ScalarOperator or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, eqConj, eqConj);
        Assertions.assertTrue(mayMatch(or, range("c2", dt(2020, 1, 1), dt(2020, 1, 1))));
        // inverted range (lo > hi) -> treat as unknown and keep, so pruning stays a safe over-estimate.
        // Without the guard the EQ check would collapse to an empty interval and wrongly prune.
        Assertions.assertTrue(mayMatch(eqConj, range("c2", dt(2020, 12, 1), dt(2020, 1, 1))));
    }

    @Test
    public void testRangeMayMatchDateCastConservative() {
        // CAST(c2 AS DATE) compares with day-truncation semantics; range pruning uses full LocalDateTimes,
        // so a DATE cast must be conservatively kept. A noon bound [12:00,12:00] compared as full datetime
        // against the midnight constant would falsely prune, but the row's date still matches.
        LocalDateTime noon = LocalDateTime.of(2020, 6, 14, 12, 0);
        ScalarOperator dateEq = eq(castTo(strPartCol, DateType.DATE), datetime("2020-06-14 00:00:00"));
        Assertions.assertTrue(mayMatch(dateEq, range("c2", noon, noon)));
        // Sanity: the guard is DATE-specific. A DATETIME cast still range-prunes (12:00 != midnight).
        ScalarOperator dtEq = eq(castTo(strPartCol, DateType.DATETIME), datetime("2020-06-14 00:00:00"));
        Assertions.assertFalse(mayMatch(dtEq, range("c2", noon, noon)));
    }

    private Map<String, String> singletonMap(String k, String v) {
        Map<String, String> map = new HashMap<>();
        map.put(k, v);
        return map;
    }
}