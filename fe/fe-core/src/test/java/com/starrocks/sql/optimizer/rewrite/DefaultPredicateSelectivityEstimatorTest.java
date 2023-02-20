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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.tree.DefaultPredicateSelectivityEstimator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class DefaultPredicateSelectivityEstimatorTest {

    private static ColumnRefFactory columnRefFactory;

    private static Statistics statistics;

    private static ColumnRefOperator v1;
    private static ColumnRefOperator v2;
    private static ColumnRefOperator v3;
    private static ColumnRefOperator v4;
    private static ColumnRefOperator v5;
    private static ColumnRefOperator v6;
    private static ColumnRefOperator v7;
    private static ColumnRefOperator v8;
    private static ColumnRefOperator v9;

    @BeforeClass
    public static void beforeClass() throws Exception {
        columnRefFactory = new ColumnRefFactory();
        v1 = columnRefFactory.create("v1", Type.INT, true);
        v2 = columnRefFactory.create("v2", Type.DOUBLE, true);
        v3 = columnRefFactory.create("v3", Type.DECIMALV2, true);
        v4 = columnRefFactory.create("v4", Type.BOOLEAN, true);
        v5 = columnRefFactory.create("v5", Type.BOOLEAN, false);
        v6 = columnRefFactory.create("v6", Type.BOOLEAN, false);
        v7 = columnRefFactory.create("v7", Type.DATETIME, false);
        v8 = columnRefFactory.create("v8", Type.VARCHAR, false);
        v9 = columnRefFactory.create("v8", Type.INT, false);

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(10000);
        builder.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(10, 21, 0, 10, 100)));
        builder.addColumnStatistics(ImmutableMap.of(v3, new ColumnStatistic(15.23, 300.12, 0, 10, 200)));
        builder.addColumnStatistics(ImmutableMap.of(v4, new ColumnStatistic(1, 1, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v5, new ColumnStatistic(0, 0, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v6, new ColumnStatistic(0, 1, 0, 10, 50)));
        //2003-10-11 - 2003-10-12
        builder.addColumnStatistics(ImmutableMap.of(v7, new ColumnStatistic(1065801600, 1065888000, 0, 10, 200)));
        builder.addColumnStatistics(ImmutableMap.of(v8, ColumnStatistic.unknown()));
        builder.addColumnStatistics(ImmutableMap.of(v9, new ColumnStatistic(Double.NaN, Double.NaN, 0, 0, 0)));
        statistics = builder.build();
    }

    @Test
    public void testOtherPredicate() {
        DefaultPredicateSelectivityEstimator defaultPredicateSelectivityEstimator =
                new DefaultPredicateSelectivityEstimator();
        ConstantOperator constantOperatorInt1 = ConstantOperator.createInt(1);
        ConstantOperator constantOperatorInt2 = ConstantOperator.createInt(15);
        BetweenPredicateOperator betweenPredicateOperator =
                new BetweenPredicateOperator(false, v1, constantOperatorInt1, constantOperatorInt2);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(betweenPredicateOperator, statistics), 0.25, 0.0);
    }

    @Test
    public void testEqualAndNotEqualBinaryPredicate() {
        DefaultPredicateSelectivityEstimator defaultPredicateSelectivityEstimator =
                new DefaultPredicateSelectivityEstimator();

        ConstantOperator constantOperatorInt0 = ConstantOperator.createInt(-1);
        ConstantOperator constantOperatorInt1 = ConstantOperator.createInt(0);
        ConstantOperator constantOperatorInt2 = ConstantOperator.createInt(15);
        ConstantOperator constantOperatorInt3 = ConstantOperator.createInt(100);
        ConstantOperator constantOperatorInt4 = ConstantOperator.createInt(101);

        BinaryPredicateOperator intEq0 = BinaryPredicateOperator.eq(v1, constantOperatorInt0);
        BinaryPredicateOperator intEq1 = BinaryPredicateOperator.eq(v1, constantOperatorInt1);
        BinaryPredicateOperator intEq2 = BinaryPredicateOperator.eq(v1, constantOperatorInt2);
        BinaryPredicateOperator intEq3 = BinaryPredicateOperator.eq(v1, constantOperatorInt3);
        BinaryPredicateOperator intEq4 = BinaryPredicateOperator.eq(v1, constantOperatorInt4);

        BinaryPredicateOperator intNq0 = BinaryPredicateOperator.ne(v1, constantOperatorInt0);
        BinaryPredicateOperator intNq1 = BinaryPredicateOperator.ne(v1, constantOperatorInt1);
        BinaryPredicateOperator intNq2 = BinaryPredicateOperator.ne(v1, constantOperatorInt2);
        BinaryPredicateOperator intNq3 = BinaryPredicateOperator.ne(v1, constantOperatorInt3);
        BinaryPredicateOperator intNq4 = BinaryPredicateOperator.ne(v1, constantOperatorInt4);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEq0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEq1, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEq2, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEq3, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEq4, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intNq0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intNq1, statistics), 0.98, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intNq2, statistics), 0.98, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intNq3, statistics), 0.98, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intNq4, statistics), 1.0, 0.0);

        //datetime
        ConstantOperator constantOperatorDt0 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 10, 13, 15, 25));
        ConstantOperator constantOperatorDt1 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 11, 00, 00, 00));
        ConstantOperator constantOperatorDt2 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 11, 23, 56, 25));
        ConstantOperator constantOperatorDt3 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 12, 00, 00, 00));
        ConstantOperator constantOperatorDt4 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 12, 13, 15, 25));

        BinaryPredicateOperator dtEq0 = BinaryPredicateOperator.eq(v7, constantOperatorDt0);
        BinaryPredicateOperator dtEq1 = BinaryPredicateOperator.eq(v7, constantOperatorDt1);
        BinaryPredicateOperator dtEq2 = BinaryPredicateOperator.eq(v7, constantOperatorDt2);
        BinaryPredicateOperator dtEq3 = BinaryPredicateOperator.eq(v7, constantOperatorDt3);
        BinaryPredicateOperator dtEq4 = BinaryPredicateOperator.eq(v7, constantOperatorDt4);

        BinaryPredicateOperator dtNq0 = BinaryPredicateOperator.ne(v7, constantOperatorDt0);
        BinaryPredicateOperator dtNq1 = BinaryPredicateOperator.ne(v7, constantOperatorDt1);
        BinaryPredicateOperator dtNq2 = BinaryPredicateOperator.ne(v7, constantOperatorDt2);
        BinaryPredicateOperator dtNq3 = BinaryPredicateOperator.ne(v7, constantOperatorDt3);
        BinaryPredicateOperator dtNq4 = BinaryPredicateOperator.ne(v7, constantOperatorDt4);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEq0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEq1, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEq2, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEq3, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEq4, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtNq0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtNq1, statistics), 0.995, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtNq2, statistics), 0.995, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtNq3, statistics), 0.995, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtNq4, statistics), 1.0, 0.0);

        //boolean
        ConstantOperator constantOperatorBool0 = ConstantOperator.createBoolean(false);
        ConstantOperator constantOperatorBool1 = ConstantOperator.createBoolean(true);

        BinaryPredicateOperator boolEq0 = BinaryPredicateOperator.eq(v4, constantOperatorBool0);
        BinaryPredicateOperator boolEq1 = BinaryPredicateOperator.eq(v4, constantOperatorBool1);

        BinaryPredicateOperator boolNq0 = BinaryPredicateOperator.ne(v4, constantOperatorBool0);
        BinaryPredicateOperator boolNq1 = BinaryPredicateOperator.ne(v4, constantOperatorBool1);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolEq0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolEq1, statistics), 0.02, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolNq0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolNq1, statistics), 0.98, 0.0);

        //expression
        BinaryPredicateOperator expressionEq0 = BinaryPredicateOperator.eq(v1, v4);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(expressionEq0, statistics), 0.33, 0.004);

    }

    @Test
    public void testSimpleEqualForNullBinaryPredicate() {
        DefaultPredicateSelectivityEstimator defaultPredicateSelectivityEstimator =
                new DefaultPredicateSelectivityEstimator();

        ConstantOperator constantOperatorInt0 = ConstantOperator.createInt(-1);
        ConstantOperator constantOperatorInt1 = ConstantOperator.createInt(0);
        ConstantOperator constantOperatorInt2 = ConstantOperator.createInt(15);
        ConstantOperator constantOperatorInt3 = ConstantOperator.createInt(100);
        ConstantOperator constantOperatorInt4 = ConstantOperator.createInt(101);

        BinaryPredicateOperator intEfn0 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v1, constantOperatorInt0);
        BinaryPredicateOperator intEfn1 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v1, constantOperatorInt1);
        BinaryPredicateOperator intEfn2 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v1, constantOperatorInt2);
        BinaryPredicateOperator intEfn3 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v1, constantOperatorInt3);
        BinaryPredicateOperator intEfn4 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v1, constantOperatorInt4);
        BinaryPredicateOperator intEfn5 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v1, ConstantOperator.createNull(Type.NULL));

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEfn0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEfn1, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEfn2, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEfn3, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEfn4, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intEfn5, statistics), 0.02, 0.0);

        //datetime
        ConstantOperator constantOperatorDt0 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 10, 13, 15, 25));
        ConstantOperator constantOperatorDt1 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 11, 00, 00, 00));
        ConstantOperator constantOperatorDt2 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 11, 23, 56, 25));
        ConstantOperator constantOperatorDt3 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 12, 00, 00, 00));
        ConstantOperator constantOperatorDt4 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 12, 13, 15, 25));

        BinaryPredicateOperator dtEfn0 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v7, constantOperatorDt0);
        BinaryPredicateOperator dtEfn1 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v7, constantOperatorDt1);
        BinaryPredicateOperator dtEfn2 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v7, constantOperatorDt2);
        BinaryPredicateOperator dtEfn3 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v7, constantOperatorDt3);
        BinaryPredicateOperator dtEfn4 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v7, constantOperatorDt4);
        BinaryPredicateOperator dtEfn5 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v7, ConstantOperator.createNull(Type.NULL));

        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEfn0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEfn1, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEfn2, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEfn3, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEfn4, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtEfn5, statistics), 0.005, 0.0);

        //boolean
        ConstantOperator constantOperatorBool0 = ConstantOperator.createBoolean(false);
        ConstantOperator constantOperatorBool1 = ConstantOperator.createBoolean(true);
        //e.g. v4 <=> false(0)
        BinaryPredicateOperator boolEfn0 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v4, constantOperatorBool0);
        BinaryPredicateOperator boolEfn1 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v4, constantOperatorBool1);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolEfn0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolEfn1, statistics), 0.02, 0.0);

        //expression
        BinaryPredicateOperator expressionEfn0 =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                        v1, v4);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(expressionEfn0, statistics), 0.33, 0.004);
    }

    @Test
    public void testLessAndGreatBinaryPredicate() {
        DefaultPredicateSelectivityEstimator defaultPredicateSelectivityEstimator =
                new DefaultPredicateSelectivityEstimator();

        //test ColumnRefOperator compare with ConstantOperator which type is Int
        ConstantOperator constantOperatorInt0 = ConstantOperator.createInt(-1);
        ConstantOperator constantOperatorInt1 = ConstantOperator.createInt(0);
        ConstantOperator constantOperatorInt2 = ConstantOperator.createInt(15);
        ConstantOperator constantOperatorInt3 = ConstantOperator.createInt(100);
        ConstantOperator constantOperatorInt4 = ConstantOperator.createInt(101);

        BinaryPredicateOperator intLt0 = BinaryPredicateOperator.lt(v1, constantOperatorInt0);
        BinaryPredicateOperator intLt1 = BinaryPredicateOperator.lt(v1, constantOperatorInt1);
        BinaryPredicateOperator intLt2 = BinaryPredicateOperator.lt(v1, constantOperatorInt2);
        BinaryPredicateOperator intLt3 = BinaryPredicateOperator.lt(v1, constantOperatorInt3);
        BinaryPredicateOperator intLt4 = BinaryPredicateOperator.lt(v1, constantOperatorInt4);

        BinaryPredicateOperator intLe0 = BinaryPredicateOperator.le(v1, constantOperatorInt0);
        BinaryPredicateOperator intLe1 = BinaryPredicateOperator.le(v1, constantOperatorInt1);
        BinaryPredicateOperator intLe2 = BinaryPredicateOperator.le(v1, constantOperatorInt2);
        BinaryPredicateOperator intLe3 = BinaryPredicateOperator.le(v1, constantOperatorInt3);
        BinaryPredicateOperator intLe4 = BinaryPredicateOperator.le(v1, constantOperatorInt4);

        BinaryPredicateOperator intGt0 = BinaryPredicateOperator.gt(v1, constantOperatorInt0);
        BinaryPredicateOperator intGt1 = BinaryPredicateOperator.gt(v1, constantOperatorInt1);
        BinaryPredicateOperator intGt2 = BinaryPredicateOperator.gt(v1, constantOperatorInt2);
        BinaryPredicateOperator intGt3 = BinaryPredicateOperator.gt(v1, constantOperatorInt3);
        BinaryPredicateOperator intGt4 = BinaryPredicateOperator.gt(v1, constantOperatorInt4);

        BinaryPredicateOperator intGe0 = BinaryPredicateOperator.ge(v1, constantOperatorInt0);
        BinaryPredicateOperator intGe1 = BinaryPredicateOperator.ge(v1, constantOperatorInt1);
        BinaryPredicateOperator intGe2 = BinaryPredicateOperator.ge(v1, constantOperatorInt2);
        BinaryPredicateOperator intGe3 = BinaryPredicateOperator.ge(v1, constantOperatorInt3);
        BinaryPredicateOperator intGe4 = BinaryPredicateOperator.ge(v1, constantOperatorInt4);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt1, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt2, statistics), 0.15, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt3, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt4, statistics), 1.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLe0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLe1, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLe2, statistics), 0.15, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLe3, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLe4, statistics), 1.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt1, statistics), 0.98, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt2, statistics), 0.85, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt3, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt4, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe1, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe2, statistics), 0.85, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe3, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe4, statistics), 0.0, 0.0);

        //test ColumnRefOperator compare with ConstantOperator which type is Double
        ConstantOperator constantOperatorDouble0 = ConstantOperator.createDouble(9.0);
        ConstantOperator constantOperatorDouble1 = ConstantOperator.createDouble(10.0);
        ConstantOperator constantOperatorDouble2 = ConstantOperator.createDouble(13.0);
        ConstantOperator constantOperatorDouble3 = ConstantOperator.createDouble(21.0);
        ConstantOperator constantOperatorDouble4 = ConstantOperator.createDouble(25.0);
        //e,g. v2 < 9
        BinaryPredicateOperator doubleLt0 = BinaryPredicateOperator.lt(v2, constantOperatorDouble0);
        BinaryPredicateOperator doubleLt1 = BinaryPredicateOperator.lt(v2, constantOperatorDouble1);
        BinaryPredicateOperator doubleLt2 = BinaryPredicateOperator.lt(v2, constantOperatorDouble2);
        BinaryPredicateOperator doubleLt3 = BinaryPredicateOperator.lt(v2, constantOperatorDouble3);
        BinaryPredicateOperator doubleLt4 = BinaryPredicateOperator.lt(v2, constantOperatorDouble4);
        //e,g. v2 > 9
        BinaryPredicateOperator doubleGt0 = BinaryPredicateOperator.gt(v2, constantOperatorDouble0);
        BinaryPredicateOperator doubleGt1 = BinaryPredicateOperator.gt(v2, constantOperatorDouble1);
        BinaryPredicateOperator doubleGt2 = BinaryPredicateOperator.gt(v2, constantOperatorDouble2);
        BinaryPredicateOperator doubleGt3 = BinaryPredicateOperator.gt(v2, constantOperatorDouble3);
        BinaryPredicateOperator doubleGt4 = BinaryPredicateOperator.gt(v2, constantOperatorDouble4);
        //e,g. v2 >= 9
        BinaryPredicateOperator doubleGe0 = BinaryPredicateOperator.ge(v2, constantOperatorDouble0);
        BinaryPredicateOperator doubleGe1 = BinaryPredicateOperator.ge(v2, constantOperatorDouble1);
        BinaryPredicateOperator doubleGe2 = BinaryPredicateOperator.ge(v2, constantOperatorDouble2);
        BinaryPredicateOperator doubleGe3 = BinaryPredicateOperator.ge(v2, constantOperatorDouble3);
        BinaryPredicateOperator doubleGe4 = BinaryPredicateOperator.ge(v2, constantOperatorDouble4);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleLt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleLt1, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleLt2, statistics), 0.273, 0.003);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleLt3, statistics), 0.01, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleLt4, statistics), 1.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGt0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGt1, statistics), 0.99, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGt2, statistics), 0.727, 0.002);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGt3, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGt4, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGe1, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGe2, statistics), 0.727, 0.002);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGe3, statistics), 0.01, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(doubleGe4, statistics), 0.0, 0.0);

        //test ColumnRefOperator compare with ConstantOperator which type is Decimal
        ConstantOperator constantOperatorDec0 = ConstantOperator.createDecimal(new BigDecimal(14.11), Type.DECIMALV2);
        ConstantOperator constantOperatorDec1 = ConstantOperator.createDecimal(new BigDecimal(15.23), Type.DECIMALV2);
        ConstantOperator constantOperatorDec2 = ConstantOperator.createDecimal(new BigDecimal(20.0), Type.DECIMALV2);
        ConstantOperator constantOperatorDec3 = ConstantOperator.createDecimal(new BigDecimal(300.12), Type.DECIMALV2);
        ConstantOperator constantOperatorDec4 = ConstantOperator.createDecimal(new BigDecimal(301.0), Type.DECIMALV2);
        //e.g. v3 < 14.11
        BinaryPredicateOperator decLt0 = BinaryPredicateOperator.lt(v3, constantOperatorDec0);
        BinaryPredicateOperator decLt1 = BinaryPredicateOperator.lt(v3, constantOperatorDec1);
        BinaryPredicateOperator decLt2 = BinaryPredicateOperator.lt(v3, constantOperatorDec2);
        BinaryPredicateOperator decLt3 = BinaryPredicateOperator.lt(v3, constantOperatorDec3);
        BinaryPredicateOperator decLt4 = BinaryPredicateOperator.lt(v3, constantOperatorDec4);
        //e.g. v3 > 14.11
        BinaryPredicateOperator decGt0 = BinaryPredicateOperator.gt(v3, constantOperatorDec0);
        BinaryPredicateOperator decGt1 = BinaryPredicateOperator.gt(v3, constantOperatorDec1);
        BinaryPredicateOperator decGt2 = BinaryPredicateOperator.gt(v3, constantOperatorDec2);
        BinaryPredicateOperator decGt3 = BinaryPredicateOperator.gt(v3, constantOperatorDec3);
        BinaryPredicateOperator decGt4 = BinaryPredicateOperator.gt(v3, constantOperatorDec4);
        //e.g. v3 >= 14.11
        BinaryPredicateOperator decGe0 = BinaryPredicateOperator.ge(v3, constantOperatorDec0);
        BinaryPredicateOperator decGe1 = BinaryPredicateOperator.ge(v3, constantOperatorDec1);
        BinaryPredicateOperator decGe2 = BinaryPredicateOperator.ge(v3, constantOperatorDec2);
        BinaryPredicateOperator decGe3 = BinaryPredicateOperator.ge(v3, constantOperatorDec3);
        BinaryPredicateOperator decGe4 = BinaryPredicateOperator.ge(v3, constantOperatorDec4);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(decLt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decLt1, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decLt2, statistics), 0.017, 0.003);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decLt3, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decLt4, statistics), 1.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGt0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGt1, statistics), 0.995, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGt2, statistics), 0.983, 0.001);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGt3, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGt4, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGe1, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGe2, statistics), 0.983, 0.001);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGe3, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(decGe4, statistics), 0.0, 0.0);

        //test ColumnRefOperator compare with ConstantOperator which type is Boolean
        ConstantOperator constantOperatorBool0 = ConstantOperator.createBoolean(false);
        ConstantOperator constantOperatorBool1 = ConstantOperator.createBoolean(true);
        //e.g. v4 < false(0)
        BinaryPredicateOperator boolLt0 = BinaryPredicateOperator.lt(v4, constantOperatorBool0);
        BinaryPredicateOperator boolLt1 = BinaryPredicateOperator.lt(v4, constantOperatorBool1);

        BinaryPredicateOperator boolLe0 = BinaryPredicateOperator.le(v4, constantOperatorBool0);
        BinaryPredicateOperator boolLe1 = BinaryPredicateOperator.le(v4, constantOperatorBool1);
        //e.g. v4 > false(0)
        BinaryPredicateOperator boolGt0 = BinaryPredicateOperator.gt(v4, constantOperatorBool0);
        BinaryPredicateOperator boolGt1 = BinaryPredicateOperator.gt(v4, constantOperatorBool1);
        //e.g. v4 >= false(0)
        BinaryPredicateOperator boolGe0 = BinaryPredicateOperator.ge(v4, constantOperatorBool0);
        BinaryPredicateOperator boolGe1 = BinaryPredicateOperator.ge(v4, constantOperatorBool1);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolLt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolLt1, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolLe0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolLe1, statistics), 1.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGt0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGt1, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGe1, statistics), 1.0, 0.0);

        //e.g. v5 < false(0)
        boolLt0 = BinaryPredicateOperator.lt(v5, constantOperatorBool0);
        boolLt1 = BinaryPredicateOperator.lt(v5, constantOperatorBool1);
        //e.g. v5 > false(0)
        boolGt0 = BinaryPredicateOperator.gt(v5, constantOperatorBool0);
        boolGt1 = BinaryPredicateOperator.gt(v5, constantOperatorBool1);
        //e.g. v5 >= false(0)
        boolGe0 = BinaryPredicateOperator.ge(v5, constantOperatorBool0);
        boolGe1 = BinaryPredicateOperator.ge(v5, constantOperatorBool1);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolLt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolLt1, statistics), 1.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGt1, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGe1, statistics), 0.0, 0.0);

        //e.g. v6 < false(0) false >= false false >= true
        boolLt0 = BinaryPredicateOperator.lt(v6, constantOperatorBool0);
        boolLt1 = BinaryPredicateOperator.lt(v6, constantOperatorBool1);
        //e.g. v6 > false(0) true
        boolGt0 = BinaryPredicateOperator.gt(v6, constantOperatorBool0);
        boolGt1 = BinaryPredicateOperator.gt(v6, constantOperatorBool1);
        //e.g. v6 >= false(0)
        boolGe0 = BinaryPredicateOperator.ge(v6, constantOperatorBool0);
        boolGe1 = BinaryPredicateOperator.ge(v6, constantOperatorBool1);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolLt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolLt1, statistics), 0.02, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGt0, statistics), 0.98, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGt1, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(boolGe1, statistics), 0.02, 0.0);

        //test ColumnRefOperator compare with ConstantOperator which type is Datetime
        ConstantOperator constantOperatorDt0 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 10, 13, 15, 25));
        ConstantOperator constantOperatorDt1 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 11, 00, 00, 00));
        ConstantOperator constantOperatorDt2 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 11, 23, 56, 25));
        ConstantOperator constantOperatorDt3 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 12, 00, 00, 00));
        ConstantOperator constantOperatorDt4 =
                ConstantOperator.createDatetime(LocalDateTime.of(2003, 10, 12, 13, 15, 25));
        //e.g. v7 < 2003-10-10 13:15:25
        BinaryPredicateOperator dtLt0 = BinaryPredicateOperator.lt(v7, constantOperatorDt0);
        BinaryPredicateOperator dtLt1 = BinaryPredicateOperator.lt(v7, constantOperatorDt1);
        BinaryPredicateOperator dtLt2 = BinaryPredicateOperator.lt(v7, constantOperatorDt2);
        BinaryPredicateOperator dtLt3 = BinaryPredicateOperator.lt(v7, constantOperatorDt3);
        BinaryPredicateOperator dtLt4 = BinaryPredicateOperator.lt(v7, constantOperatorDt4);
        //e.g. v7 <= 2003-10-10 13:15:25
        BinaryPredicateOperator dtLe0 = BinaryPredicateOperator.le(v7, constantOperatorDt0);
        BinaryPredicateOperator dtLe1 = BinaryPredicateOperator.le(v7, constantOperatorDt1);
        BinaryPredicateOperator dtLe2 = BinaryPredicateOperator.le(v7, constantOperatorDt2);
        BinaryPredicateOperator dtLe3 = BinaryPredicateOperator.le(v7, constantOperatorDt3);
        BinaryPredicateOperator dtLe4 = BinaryPredicateOperator.le(v7, constantOperatorDt4);
        //e.g. v7 > 2003-10-10 13:15:25
        BinaryPredicateOperator dtGt0 = BinaryPredicateOperator.gt(v7, constantOperatorDt0);
        BinaryPredicateOperator dtGt1 = BinaryPredicateOperator.gt(v7, constantOperatorDt1);
        BinaryPredicateOperator dtGt2 = BinaryPredicateOperator.gt(v7, constantOperatorDt2);
        BinaryPredicateOperator dtGt3 = BinaryPredicateOperator.gt(v7, constantOperatorDt3);
        BinaryPredicateOperator dtGt4 = BinaryPredicateOperator.gt(v7, constantOperatorDt4);
        //e.g. v7 >= 2003-10-10 13:15:25
        BinaryPredicateOperator dtGe0 = BinaryPredicateOperator.ge(v7, constantOperatorDt0);
        BinaryPredicateOperator dtGe1 = BinaryPredicateOperator.ge(v7, constantOperatorDt1);
        BinaryPredicateOperator dtGe2 = BinaryPredicateOperator.ge(v7, constantOperatorDt2);
        BinaryPredicateOperator dtGe3 = BinaryPredicateOperator.ge(v7, constantOperatorDt3);
        BinaryPredicateOperator dtGe4 = BinaryPredicateOperator.ge(v7, constantOperatorDt4);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLt1, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLt2, statistics), 0.998, 0.005);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLt3, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLt4, statistics), 1.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLe0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLe1, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLe2, statistics), 0.998, 0.005);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLe3, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtLe4, statistics), 1.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGt0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGt1, statistics), 0.995, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGt2, statistics), 0.002, 0.001);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGt3, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGt4, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGe1, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGe2, statistics), 0.002, 0.1);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGe3, statistics), 0.005, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(dtGe4, statistics), 0.0, 0.0);

        //expression
        BinaryPredicateOperator expressionLt0 = BinaryPredicateOperator.lt(v1, v4);
        BinaryPredicateOperator expressionLe0 = BinaryPredicateOperator.le(v1, v4);
        BinaryPredicateOperator expressionGt0 = BinaryPredicateOperator.gt(v1, v4);
        BinaryPredicateOperator expressionGe0 = BinaryPredicateOperator.ge(v1, v4);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(expressionLt0, statistics), 0.33, 0.004);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(expressionLe0, statistics), 0.33, 0.004);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(expressionGt0, statistics), 0.33, 0.004);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(expressionGe0, statistics), 0.33, 0.004);
    }

    @Test
    public void testStatisticsUnknown() {
        //v8 varchar
        DefaultPredicateSelectivityEstimator defaultPredicateSelectivityEstimator =
                new DefaultPredicateSelectivityEstimator();

        //test ColumnRefOperator compare with ConstantOperator which type is Int
        ConstantOperator constantOperatorStr0 = ConstantOperator.createVarchar("me");

        BinaryPredicateOperator strEq0 = BinaryPredicateOperator.eq(v8, constantOperatorStr0);
        BinaryPredicateOperator strNe0 = BinaryPredicateOperator.ne(v8, constantOperatorStr0);
        BinaryPredicateOperator strLt0 = BinaryPredicateOperator.lt(v8, constantOperatorStr0);
        BinaryPredicateOperator strLg0 = BinaryPredicateOperator.le(v8, constantOperatorStr0);
        BinaryPredicateOperator strGt0 = BinaryPredicateOperator.gt(v8, constantOperatorStr0);
        BinaryPredicateOperator strGe0 = BinaryPredicateOperator.ge(v8, constantOperatorStr0);
        BinaryPredicateOperator strEfn0 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v8, constantOperatorStr0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(strEq0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strNe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strLt0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strLg0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strGt0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strGe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strEfn0, statistics), 1.0, 0.0);

    }

    @Test
    public void testStatisticsNaN() {
        //v9 int
        DefaultPredicateSelectivityEstimator defaultPredicateSelectivityEstimator =
                new DefaultPredicateSelectivityEstimator();

        //test ColumnRefOperator compare with ConstantOperator which type is Int
        ConstantOperator constantOperatorStr0 = ConstantOperator.createInt(5);

        BinaryPredicateOperator strEq0 = BinaryPredicateOperator.eq(v9, constantOperatorStr0);
        BinaryPredicateOperator strNe0 = BinaryPredicateOperator.ne(v9, constantOperatorStr0);
        BinaryPredicateOperator strLt0 = BinaryPredicateOperator.lt(v9, constantOperatorStr0);
        BinaryPredicateOperator strLg0 = BinaryPredicateOperator.le(v9, constantOperatorStr0);
        BinaryPredicateOperator strGt0 = BinaryPredicateOperator.gt(v9, constantOperatorStr0);
        BinaryPredicateOperator strGe0 = BinaryPredicateOperator.ge(v9, constantOperatorStr0);
        BinaryPredicateOperator strEfn0 = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ_FOR_NULL,
                v9, constantOperatorStr0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(strEq0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strNe0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strLt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strLg0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strGt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strGe0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(strEfn0, statistics), 0.0, 0.0);
    }

    @Test
    public void testExpressionBinaryPredicate() {
        DefaultPredicateSelectivityEstimator defaultPredicateSelectivityEstimator =
                new DefaultPredicateSelectivityEstimator();
        CallOperator callOperator = new CallOperator(FunctionSet.MAX, Type.INT, Lists.newArrayList(v1));

        ConstantOperator constantOperatorInt0 = ConstantOperator.createInt(-1);
        ConstantOperator constantOperatorInt1 = ConstantOperator.createInt(0);
        ConstantOperator constantOperatorInt2 = ConstantOperator.createInt(15);
        ConstantOperator constantOperatorInt3 = ConstantOperator.createInt(100);
        ConstantOperator constantOperatorInt4 = ConstantOperator.createInt(101);

        BinaryPredicateOperator intLt0 = BinaryPredicateOperator.lt(callOperator, constantOperatorInt0);
        BinaryPredicateOperator intLt1 = BinaryPredicateOperator.lt(callOperator, constantOperatorInt1);
        BinaryPredicateOperator intLt2 = BinaryPredicateOperator.lt(callOperator, constantOperatorInt2);
        BinaryPredicateOperator intLt3 = BinaryPredicateOperator.lt(callOperator, constantOperatorInt3);
        BinaryPredicateOperator intLt4 = BinaryPredicateOperator.lt(callOperator, constantOperatorInt4);

        BinaryPredicateOperator intGt0 = BinaryPredicateOperator.gt(callOperator, constantOperatorInt0);
        BinaryPredicateOperator intGt1 = BinaryPredicateOperator.gt(callOperator, constantOperatorInt1);
        BinaryPredicateOperator intGt2 = BinaryPredicateOperator.gt(callOperator, constantOperatorInt2);
        BinaryPredicateOperator intGt3 = BinaryPredicateOperator.gt(callOperator, constantOperatorInt3);
        BinaryPredicateOperator intGt4 = BinaryPredicateOperator.gt(callOperator, constantOperatorInt4);

        BinaryPredicateOperator intGe0 = BinaryPredicateOperator.ge(callOperator, constantOperatorInt0);
        BinaryPredicateOperator intGe1 = BinaryPredicateOperator.ge(callOperator, constantOperatorInt1);
        BinaryPredicateOperator intGe2 = BinaryPredicateOperator.ge(callOperator, constantOperatorInt2);
        BinaryPredicateOperator intGe3 = BinaryPredicateOperator.ge(callOperator, constantOperatorInt3);
        BinaryPredicateOperator intGe4 = BinaryPredicateOperator.ge(callOperator, constantOperatorInt4);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt0, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt1, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt2, statistics), 0.15, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt3, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intLt4, statistics), 1.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt1, statistics), 0.98, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt2, statistics), 0.85, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt3, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGt4, statistics), 0.0, 0.0);

        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe0, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe1, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe2, statistics), 0.85, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe3, statistics), 0.02, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(intGe4, statistics), 0.0, 0.0);
    }

    @Test
    public void testCompoundPredicate() {
        DefaultPredicateSelectivityEstimator defaultPredicateSelectivityEstimator =
                new DefaultPredicateSelectivityEstimator();

        ConstantOperator constantOperatorInt0 = ConstantOperator.createInt(-1);
        ConstantOperator constantOperatorDouble0 = ConstantOperator.createDouble(9.0);
        // v1 < -1 0.0
        BinaryPredicateOperator intLt0 = BinaryPredicateOperator.lt(v1, constantOperatorInt0);
        // v2 > 9 1.0
        BinaryPredicateOperator doubleGt0 = BinaryPredicateOperator.gt(v2, constantOperatorDouble0);

        ScalarOperator and = CompoundPredicateOperator.and(intLt0, doubleGt0);
        ScalarOperator or = CompoundPredicateOperator.or(intLt0, doubleGt0);
        ScalarOperator notLt = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                intLt0);
        ScalarOperator notGt = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                doubleGt0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(and, statistics), 0.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(or, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(notLt, statistics), 1.0, 0.0);
        assertEquals(defaultPredicateSelectivityEstimator.estimate(notGt, statistics), 0.0, 0.0);
    }

}
