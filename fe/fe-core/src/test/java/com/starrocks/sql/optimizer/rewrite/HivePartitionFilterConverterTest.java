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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

public class HivePartitionFilterConverterTest {
    private final ColumnRefOperator dateRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "s_date", true);
    private final ColumnRefOperator pidRef = new ColumnRefOperator(2, IntegerType.INT, "u_pid", true);
    private final ColumnRefOperator eventRef = new ColumnRefOperator(3, VarcharType.VARCHAR, "b_event", true);
    private final ColumnRefOperator userRef = new ColumnRefOperator(4, VarcharType.VARCHAR, "u_id", true);

    private final Map<ColumnRefOperator, Column> partitionColumns = ImmutableMap.of(
            dateRef, new Column("s_date", VarcharType.VARCHAR),
            pidRef, new Column("u_pid", IntegerType.INT),
            eventRef, new Column("b_event", VarcharType.VARCHAR));

    @Test
    public void testConvertRangeAndInPredicates() {
        ScalarOperator predicate = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND,
                Lists.newArrayList(
                        BinaryPredicateOperator.ge(dateRef, ConstantOperator.createVarchar("2026-05-25")),
                        BinaryPredicateOperator.le(dateRef, ConstantOperator.createVarchar("2026-05-26")),
                        new InPredicateOperator(false, pidRef, ConstantOperator.createInt(3091)),
                        new InPredicateOperator(false, eventRef,
                                ConstantOperator.createVarchar("app_create"),
                                ConstantOperator.createVarchar("app_timer")),
                        BinaryPredicateOperator.ne(userRef, ConstantOperator.createVarchar(""))));

        Optional<HivePartitionFilterConverter.Result> result =
                HivePartitionFilterConverter.convert(predicate, partitionColumns);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get().requiresFilterApi());
        Assertions.assertTrue(result.get().getFilter().contains("s_date >= \"2026-05-25\""));
        Assertions.assertTrue(result.get().getFilter().contains("s_date <= \"2026-05-26\""));
        Assertions.assertTrue(result.get().getFilter().contains("u_pid = 3091"));
        Assertions.assertTrue(result.get().getFilter().contains("b_event = \"app_create\""));
        Assertions.assertTrue(result.get().getFilter().contains("b_event = \"app_timer\""));
        Assertions.assertFalse(result.get().getFilter().contains("u_id"));
    }

    @Test
    public void testStringEqualityKeepsPartitionValuesFastPath() {
        Optional<HivePartitionFilterConverter.Result> result = HivePartitionFilterConverter.convert(
                BinaryPredicateOperator.eq(dateRef, ConstantOperator.createVarchar("2026-05-25")),
                partitionColumns);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertFalse(result.get().requiresFilterApi());
        Assertions.assertEquals("s_date = \"2026-05-25\"", result.get().getFilter());
    }

    @Test
    public void testIntegerEqualityAndInRequireFilterApi() {
        Optional<HivePartitionFilterConverter.Result> equality = HivePartitionFilterConverter.convert(
                BinaryPredicateOperator.eq(pidRef, ConstantOperator.createInt(3091)), partitionColumns);
        Optional<HivePartitionFilterConverter.Result> in = HivePartitionFilterConverter.convert(
                new InPredicateOperator(false, eventRef,
                        ConstantOperator.createVarchar("app_create"),
                        ConstantOperator.createVarchar("app_timer")),
                partitionColumns);

        Assertions.assertTrue(equality.isPresent());
        Assertions.assertTrue(equality.get().requiresFilterApi());
        Assertions.assertTrue(in.isPresent());
        Assertions.assertTrue(in.get().requiresFilterApi());
    }

    @Test
    public void testCommuteBinaryPredicate() {
        Optional<HivePartitionFilterConverter.Result> result = HivePartitionFilterConverter.convert(
                BinaryPredicateOperator.le(ConstantOperator.createInt(3091), pidRef), partitionColumns);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get().requiresFilterApi());
        Assertions.assertEquals("u_pid >= 3091", result.get().getFilter());
    }

    @Test
    public void testOrRequiresEveryBranchToBeConvertible() {
        ScalarOperator predicate = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR,
                BinaryPredicateOperator.gt(dateRef, ConstantOperator.createVarchar("2026-05-25")),
                BinaryPredicateOperator.eq(userRef, ConstantOperator.createVarchar("user1")));

        Assertions.assertTrue(HivePartitionFilterConverter.convert(predicate, partitionColumns).isEmpty());
    }

    @Test
    public void testBackslashLiteralFallsBackSafely() {
        ScalarOperator unsafePredicate = BinaryPredicateOperator.ge(
                dateRef, ConstantOperator.createVarchar("2026-05-25\\"));
        Assertions.assertTrue(HivePartitionFilterConverter.convert(
                unsafePredicate, partitionColumns).isEmpty());

        ScalarOperator safePredicate = BinaryPredicateOperator.ge(
                dateRef, ConstantOperator.createVarchar("2026-05-01"));
        ScalarOperator andPredicate = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, safePredicate, unsafePredicate);
        Optional<HivePartitionFilterConverter.Result> andResult =
                HivePartitionFilterConverter.convert(andPredicate, partitionColumns);
        Assertions.assertTrue(andResult.isPresent());
        Assertions.assertEquals("(s_date >= \"2026-05-01\")", andResult.get().getFilter());

        ScalarOperator orPredicate = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, safePredicate, unsafePredicate);
        Assertions.assertTrue(HivePartitionFilterConverter.convert(
                orPredicate, partitionColumns).isEmpty());
    }
}
