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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class PredicateStatisticsCalculatorTest {
    @Test
    public void testDateBinaryPredicate() {
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(1000);
        double min = Utils.getLongFromDateTime(LocalDateTime.of(2020, 1, 1, 0, 0, 0));
        double max = Utils.getLongFromDateTime(LocalDateTime.of(2021, 6, 1, 0, 0, 0));
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, DateType.DATE, "id_date", true);
        Statistics statistics = builder.addColumnStatistic(columnRefOperator,
                ColumnStatistic.builder().setMinValue(min).setMaxValue(max).
                        setDistinctValuesCount(100).setNullsFraction(0).setAverageRowSize(10).build()).build();

        BinaryPredicateOperator binaryPredicateOperator =
                new BinaryPredicateOperator(BinaryType.GE,
                        columnRefOperator, ConstantOperator.createDate(LocalDateTime.of(2021, 5, 1, 0, 0, 0)));
        Statistics estimatedStatistics =
                PredicateStatisticsCalculator.statisticsCalculate(binaryPredicateOperator, statistics);
        Assertions.assertEquals(59.9613, estimatedStatistics.getOutputRowCount(), 0.001);
    }

    @Test
    public void testDateCompoundPredicate() {
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(1000);
        double min = Utils.getLongFromDateTime(LocalDateTime.of(2020, 1, 1, 0, 0, 0));
        double max = Utils.getLongFromDateTime(LocalDateTime.of(2021, 6, 1, 0, 0, 0));
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, DateType.DATE, "id_date", true);
        Statistics statistics = builder.addColumnStatistic(columnRefOperator,
                ColumnStatistic.builder().setMinValue(min).setMaxValue(max).
                        setDistinctValuesCount(100).setNullsFraction(0).setAverageRowSize(10).build()).build();

        BinaryPredicateOperator binaryPredicateOperator1 =
                new BinaryPredicateOperator(BinaryType.GE,
                        columnRefOperator, ConstantOperator.createDate(LocalDateTime.of(2021, 4, 1, 0, 0, 0)));
        BinaryPredicateOperator binaryPredicateOperator2 =
                new BinaryPredicateOperator(BinaryType.LT,
                        columnRefOperator, ConstantOperator.createDate(LocalDateTime.of(2021, 5, 1, 0, 0, 0)));
        CompoundPredicateOperator compoundPredicateOperator =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        binaryPredicateOperator1, binaryPredicateOperator2);

        Statistics estimatedStatistics =
                PredicateStatisticsCalculator.statisticsCalculate(compoundPredicateOperator, statistics);
        Assertions.assertEquals(58.0270, estimatedStatistics.getOutputRowCount(), 0.001);
    }

    @Test
    public void testColumnEqualToColumn() {
        ColumnRefOperator c1 = new ColumnRefOperator(0, IntegerType.INT, "c1", true);
        ColumnRefOperator c2 = new ColumnRefOperator(1, IntegerType.INT, "c2", true);

        Statistics statistics = Statistics.builder()
                .addColumnStatistic(c1,
                        ColumnStatistic.builder().setNullsFraction(0.5).setDistinctValuesCount(10).build())
                .addColumnStatistic(c2,
                        ColumnStatistic.builder().setNullsFraction(0.8).setDistinctValuesCount(80).build())
                .setOutputRowCount(10000).build();

        BinaryPredicateOperator binaryPredicateOperator =
                new BinaryPredicateOperator(BinaryType.EQ, c1, c2);
        Statistics estimatedStatistics =
                PredicateStatisticsCalculator.statisticsCalculate(binaryPredicateOperator, statistics);

        Assertions.assertEquals(12.49, estimatedStatistics.getOutputRowCount(), 0.1);
        Assertions.assertEquals(10, estimatedStatistics.getColumnStatistic(c1).getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, estimatedStatistics.getColumnStatistic(c1).getNullsFraction(), 0.001);
        Assertions.assertEquals(10, estimatedStatistics.getColumnStatistic(c2).getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, estimatedStatistics.getColumnStatistic(c2).getNullsFraction(), 0.001);
    }

    @Test
    public void testNullEqStatistic() throws Exception {
        ColumnRefOperator c1 = new ColumnRefOperator(0, IntegerType.INT, "c1", true);
        Statistics statistics = Statistics.builder()
                .addColumnStatistic(c1, ColumnStatistic.builder().setNullsFraction(0.5).setDistinctValuesCount(10).build())
                .setOutputRowCount(10000).build();

        BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(
                BinaryType.EQ_FOR_NULL, c1, ConstantOperator.createNull(IntegerType.INT));
        Statistics estimatedStatistics =
                PredicateStatisticsCalculator.statisticsCalculate(binaryPredicateOperator, statistics);
        Assertions.assertEquals(5000, estimatedStatistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(1, estimatedStatistics.getColumnStatistic(c1).getNullsFraction(), 0.001);
    }

    @Test
    public void testConcatExpressionCalculate() {
        ColumnRefOperator c1 = new ColumnRefOperator(0, VarcharType.VARCHAR, "c1", true);
        ConstantOperator c2 = new ConstantOperator("-", VarcharType.VARCHAR);
        ColumnRefOperator c3 = new ColumnRefOperator(1, VarcharType.VARCHAR, "c3", true);
        CallOperator concat = new CallOperator("concat", VarcharType.VARCHAR, Lists.newArrayList(c1, c2, c3));
        Statistics statistics = Statistics.builder()
                .addColumnStatistic(c1, ColumnStatistic.builder().setNullsFraction(0.2).setDistinctValuesCount(10).build())
                .addColumnStatistic(c3, ColumnStatistic.builder().setNullsFraction(0.4).setDistinctValuesCount(10).build())
                .setOutputRowCount(10000).build();
        ColumnStatistic estimatedStatistics = ExpressionStatisticCalculator.calculate(concat, statistics);
        Assertions.assertEquals(0.52, estimatedStatistics.getNullsFraction(), 0.001);
    }

    @Test
    public void testEstimateInPredicateWithHistogramNumeric() {
        ColumnRefOperator columnRef = new ColumnRefOperator(1, IntegerType.INT, "c1", true);

        List<Bucket> buckets = Lists.newArrayList(
                new Bucket(1, 9, 100L, 20L),
                new Bucket(11, 19, 200L, 30L),
                new Bucket(21, 29, 300L, 40L)
        );

        Map<String, Long> mcv = new java.util.HashMap<>();
        mcv.put("10", 50L);
        mcv.put("20", 60L);
        mcv.put("30", 70L);
        mcv.put("35", 80L);

        Histogram histogram = new Histogram(buckets, mcv);

        ColumnStatistic columnStatistic = ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(40)
                .setDistinctValuesCount(30)
                .setNullsFraction(0.1)
                .setHistogram(histogram)
                .build();

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(columnRef, columnStatistic)
                .build();

        List<ConstantOperator> constants = Lists.newArrayList(
                ConstantOperator.createInt(10),
                ConstantOperator.createInt(15),
                ConstantOperator.createInt(30),
                ConstantOperator.createInt(50)
        );

        Statistics result = HistogramStatisticsUtils.estimateInPredicateWithHistogram(
                columnRef, columnStatistic, constants, false, statistics);

        Assertions.assertEquals(207, (int) result.getOutputRowCount());
        Assertions.assertEquals(3, result.getColumnStatistic(columnRef).getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, result.getColumnStatistic(columnRef).getNullsFraction(), 0.001);
        Assertions.assertEquals(10, result.getColumnStatistic(columnRef).getMinValue(), 0.001);
        Assertions.assertEquals(30, result.getColumnStatistic(columnRef).getMaxValue(), 0.001);
    }

    @Test
    public void testEstimateNotInPredicateWithHistogram() {
        ColumnRefOperator columnRef = new ColumnRefOperator(3, IntegerType.BIGINT, "c3", true);

        List<Bucket> buckets = List.of(
                new Bucket(101, 149, 150L, 25L),
                new Bucket(151, 199, 200L, 30L),
                new Bucket(201, 249, 250L, 35L),
                new Bucket(251, 299, 300L, 40L)
        );

        Map<String, Long> mcv = Map.of(
                "100", 100L,
                "150", 120L,
                "200", 140L,
                "250", 160L,
                "300", 180L
        );

        Histogram histogram = new Histogram(buckets, mcv);

        ColumnStatistic columnStatistic = ColumnStatistic.builder()
                .setMinValue(100)
                .setMaxValue(300)
                .setDistinctValuesCount(50)
                .setNullsFraction(0.05)
                .setHistogram(histogram)
                .build();

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(columnRef, columnStatistic)
                .build();

        List<ConstantOperator> constants = List.of(
                ConstantOperator.createBigint(100),
                ConstantOperator.createBigint(150),
                ConstantOperator.createBigint(225)
        );

        Statistics result = HistogramStatisticsUtils.estimateInPredicateWithHistogram(
                columnRef, columnStatistic, constants, true, statistics);

        Assertions.assertEquals(740, (int) result.getOutputRowCount());
        Assertions.assertEquals(47, (int) result.getColumnStatistic(columnRef).getDistinctValuesCount());
        Assertions.assertEquals(0, result.getColumnStatistic(columnRef).getNullsFraction(), 0.001);
    }

    @Test
    public void testEstimateInPredicateWithStringType() {
        ColumnRefOperator columnRef = new ColumnRefOperator(2, VarcharType.VARCHAR, "c2", true);

        Map<String, Long> mcv = Map.of(
                "apple", 50L,
                "banana", 60L,
                "orange", 70L,
                "peach", 80L,
                "grape", 90L
        );

        Histogram histogram = new Histogram(List.of(), mcv);

        ColumnStatistic columnStatistic = ColumnStatistic.builder()
                .setDistinctValuesCount(40)
                .setNullsFraction(0.08)
                .setHistogram(histogram)
                .build();

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1200)
                .addColumnStatistic(columnRef, columnStatistic)
                .build();

        List<ConstantOperator> constants = List.of(
                ConstantOperator.createVarchar("apple"),
                ConstantOperator.createVarchar("banana"),
                ConstantOperator.createVarchar("cherry")
        );

        Statistics result = HistogramStatisticsUtils.estimateInPredicateWithHistogram(
                columnRef, columnStatistic, constants, false, statistics);

        Assertions.assertEquals(350, (int) result.getOutputRowCount());
        Assertions.assertEquals(3, result.getColumnStatistic(columnRef).getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, result.getColumnStatistic(columnRef).getNullsFraction(), 0.001);
    }

    @Test
    public void testEstimateInPredicateWithBooleanType() {
        ColumnRefOperator columnRef = new ColumnRefOperator(6, BooleanType.BOOLEAN, "c6", true);

        Map<String, Long> mcv = Map.of(
                "1", 600L,
                "0", 300L
        );

        Histogram histogram = new Histogram(List.of(), mcv);

        ColumnStatistic columnStatistic = ColumnStatistic.builder()
                .setDistinctValuesCount(2)
                .setNullsFraction(0.1)
                .setHistogram(histogram)
                .build();

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(columnRef, columnStatistic)
                .build();

        List<ConstantOperator> constants = List.of(
                ConstantOperator.createBoolean(true)
        );

        Statistics result = HistogramStatisticsUtils.estimateInPredicateWithHistogram(
                columnRef, columnStatistic, constants, false, statistics);

        Assertions.assertEquals(600, (int) result.getOutputRowCount());
        Assertions.assertEquals(1, result.getColumnStatistic(columnRef).getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, result.getColumnStatistic(columnRef).getNullsFraction(), 0.001);
    }

    @Test
    public void testEstimateInPredicateWithOutOfRangeConstants() {
        ColumnRefOperator columnRef = new ColumnRefOperator(7, IntegerType.INT, "c7", true);

        List<Bucket> buckets = Lists.newArrayList(
                new Bucket(10, 19, 100L, 10L),
                new Bucket(21, 29, 200L, 20L)
        );

        Map<String, Long> mcv = Map.of(
                "20", 50L,
                "30", 60L
        );

        Histogram histogram = new Histogram(buckets, mcv);

        ColumnStatistic columnStatistic = ColumnStatistic.builder()
                .setMinValue(10)
                .setMaxValue(30)
                .setDistinctValuesCount(20)
                .setNullsFraction(0.1)
                .setHistogram(histogram)
                .build();

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(columnRef, columnStatistic)
                .build();

        List<ConstantOperator> constants = Lists.newArrayList(
                ConstantOperator.createInt(5),
                ConstantOperator.createInt(35)
        );

        Statistics result = HistogramStatisticsUtils.estimateInPredicateWithHistogram(
                columnRef, columnStatistic, constants, false, statistics);

        Assertions.assertEquals(1, (int) result.getOutputRowCount());
        Assertions.assertEquals(0, result.getColumnStatistic(columnRef).getNullsFraction(), 0.001);
    }

    @Test
    public void testEstimateSimpleIfPredicate() {
        ColumnRefOperator columnRef1 = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        ColumnRefOperator columnRef2 = new ColumnRefOperator(2, IntegerType.INT, "c2", true);
        ColumnRefOperator columnRef3 = new ColumnRefOperator(3, IntegerType.INT, "c3", true);

        ColumnStatistic columnStatistic1 = ColumnStatistic.builder()
                .setMinValue(10)
                .setMaxValue(30)
                .setDistinctValuesCount(20)
                .setNullsFraction(0.1)
                .build();
        ColumnStatistic columnStatistic2 = ColumnStatistic.builder()
                .setMinValue(40)
                .setMaxValue(80)
                .setDistinctValuesCount(30)
                .setNullsFraction(0.2)
                .build();
        ColumnStatistic columnStatistic3 = ColumnStatistic.builder()
                .setMinValue(10)
                .setMaxValue(90)
                .setDistinctValuesCount(50)
                .setNullsFraction(0.4)
                .build();
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(columnRef1, columnStatistic1)
                .addColumnStatistic(columnRef2, columnStatistic2)
                .addColumnStatistic(columnRef3, columnStatistic3)
                .build();

        // IF ( c1 >= 20 , c2 = 50 , c3 = 80 )
        BinaryPredicateOperator condition =
                new BinaryPredicateOperator(BinaryType.GE, columnRef1, ConstantOperator.createInt(20));
        BinaryPredicateOperator leftPredicate =
                new BinaryPredicateOperator(BinaryType.EQ, columnRef2, ConstantOperator.createInt(50));
        BinaryPredicateOperator rightPredicate =
                new BinaryPredicateOperator(BinaryType.EQ, columnRef3, ConstantOperator.createInt(80));
        CallOperator ifPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN, List.of(condition, leftPredicate, rightPredicate));

        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(ifPredicate, statistics);
        Assertions.assertEquals(16.0, (int) result.getOutputRowCount());
    }

    @Test
    public void testEstimateNestedIfPredicate() {
        ColumnRefOperator columnRef1 = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        ColumnRefOperator columnRef2 = new ColumnRefOperator(2, IntegerType.INT, "c2", true);
        ColumnRefOperator columnRef3 = new ColumnRefOperator(3, IntegerType.INT, "c3", true);

        ColumnStatistic columnStatistic1 = ColumnStatistic.builder()
                .setMinValue(10)
                .setMaxValue(40)
                .setDistinctValuesCount(20)
                .setNullsFraction(0.1)
                .build();
        ColumnStatistic columnStatistic2 = ColumnStatistic.builder()
                .setMinValue(40)
                .setMaxValue(80)
                .setDistinctValuesCount(30)
                .setNullsFraction(0.2)
                .build();
        ColumnStatistic columnStatistic3 = ColumnStatistic.builder()
                .setMinValue(10)
                .setMaxValue(90)
                .setDistinctValuesCount(50)
                .setNullsFraction(0.4)
                .build();
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(columnRef1, columnStatistic1)
                .addColumnStatistic(columnRef2, columnStatistic2)
                .addColumnStatistic(columnRef3, columnStatistic3)
                .build();

        // IF ( c1 >= 20 , IF ( c1 >= 30 , c2 = 50 , c3 = 80 ) , c3 = 70 )
        BinaryPredicateOperator c1Ge30Condition =
                new BinaryPredicateOperator(BinaryType.GE, columnRef1, ConstantOperator.createInt(30));
        BinaryPredicateOperator c2Eq50Predicate =
                new BinaryPredicateOperator(BinaryType.EQ, columnRef2, ConstantOperator.createInt(50));
        BinaryPredicateOperator c3Eq80Predicate =
                new BinaryPredicateOperator(BinaryType.EQ, columnRef3, ConstantOperator.createInt(80));
        CallOperator innerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN, List.of(c1Ge30Condition, c2Eq50Predicate, c3Eq80Predicate));
        BinaryPredicateOperator c1Ge20Condition =
                new BinaryPredicateOperator(BinaryType.GE, columnRef1, ConstantOperator.createInt(20));
        BinaryPredicateOperator c3Eq70Predicate =
                new BinaryPredicateOperator(BinaryType.EQ, columnRef3, ConstantOperator.createInt(70));
        CallOperator outerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN,
                        List.of(c1Ge20Condition, innerIfPredicate, c3Eq70Predicate));

        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(outerIfPredicate, statistics);
        Assertions.assertEquals(13.0, (int) result.getOutputRowCount());

        // IF ( c1 >= 30 , c2 = 50 , IF ( c1 >= 20 , c3 = 80 , c3 = 70 ) )
        innerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN, List.of(c1Ge20Condition, c3Eq80Predicate, c3Eq70Predicate));
        outerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN,
                        List.of(c1Ge30Condition, c2Eq50Predicate, innerIfPredicate));

        result = PredicateStatisticsCalculator.statisticsCalculate(outerIfPredicate, statistics);
        Assertions.assertEquals(15.0, (int) result.getOutputRowCount());

        // IF ( IF ( c1 >= 20 , c2 >= 50 , c2 <= 70 ) , c3 = 80 , c3 = 70 )
        BinaryPredicateOperator c1Ge20Predicate =
                new BinaryPredicateOperator(BinaryType.GE, columnRef1, ConstantOperator.createInt(20));
        BinaryPredicateOperator c2Ge50Predicate =
                new BinaryPredicateOperator(BinaryType.GE, columnRef2, ConstantOperator.createInt(50));
        BinaryPredicateOperator c2Le70Predicate =
                new BinaryPredicateOperator(BinaryType.LE, columnRef2, ConstantOperator.createInt(70));
        innerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN, List.of(c1Ge20Predicate, c2Ge50Predicate, c2Le70Predicate));
        outerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN,
                        List.of(innerIfPredicate, c3Eq80Predicate, c3Eq70Predicate));

        result = PredicateStatisticsCalculator.statisticsCalculate(outerIfPredicate, statistics);
        Assertions.assertEquals(11.0, (int) result.getOutputRowCount());
    }

    @Test
    public void testEstimateCompoundNestedIfPredicate() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();

        ColumnRefOperator columnRef1 = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        ColumnRefOperator columnRef2 = new ColumnRefOperator(2, IntegerType.INT, "c2", true);
        ColumnRefOperator columnRef3 = new ColumnRefOperator(3, IntegerType.INT, "c3", true);
        ColumnRefOperator columnRef4 = new ColumnRefOperator(4, IntegerType.INT, "c4", true);

        ColumnStatistic columnStatistic1 = ColumnStatistic.builder()
                .setMinValue(10)
                .setMaxValue(40)
                .setDistinctValuesCount(20)
                .setNullsFraction(0.1)
                .build();
        ColumnStatistic columnStatistic2 = ColumnStatistic.builder()
                .setMinValue(40)
                .setMaxValue(80)
                .setDistinctValuesCount(30)
                .setNullsFraction(0.2)
                .build();
        ColumnStatistic columnStatistic3 = ColumnStatistic.builder()
                .setMinValue(10)
                .setMaxValue(90)
                .setDistinctValuesCount(50)
                .setNullsFraction(0.4)
                .build();
        ColumnStatistic columnStatistic4 = ColumnStatistic.builder()
                .setMinValue(0)
                .setMaxValue(100)
                .setDistinctValuesCount(70)
                .setNullsFraction(0.15)
                .build();
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(columnRef1, columnStatistic1)
                .addColumnStatistic(columnRef2, columnStatistic2)
                .addColumnStatistic(columnRef3, columnStatistic3)
                .addColumnStatistic(columnRef4, columnStatistic4)
                .build();

        // IF ( c1 >= 20 , NOT IF ( c1 >= 30 , c2 = 50 , c3 = 80 ) , c3 = 70 )
        BinaryPredicateOperator c1Ge30Condition =
                new BinaryPredicateOperator(BinaryType.GE, columnRef1, ConstantOperator.createInt(30));
        BinaryPredicateOperator c2Eq50Predicate =
                new BinaryPredicateOperator(BinaryType.EQ, columnRef2, ConstantOperator.createInt(50));
        BinaryPredicateOperator c3Eq80Predicate =
                new BinaryPredicateOperator(BinaryType.EQ, columnRef3, ConstantOperator.createInt(80));
        CallOperator innerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN, List.of(c1Ge30Condition, c2Eq50Predicate, c3Eq80Predicate));
        CompoundPredicateOperator compoundInnerIfPredicate =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, innerIfPredicate);
        BinaryPredicateOperator c1Ge20Condition =
                new BinaryPredicateOperator(BinaryType.GE, columnRef1, ConstantOperator.createInt(20));
        BinaryPredicateOperator c3Eq70Predicate =
                new BinaryPredicateOperator(BinaryType.EQ, columnRef3, ConstantOperator.createInt(70));
        CallOperator outerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN,
                        List.of(c1Ge20Condition, compoundInnerIfPredicate, c3Eq70Predicate));

        Statistics result = PredicateStatisticsCalculator.statisticsCalculate(outerIfPredicate, statistics);
        Assertions.assertEquals(592.0, (int) result.getOutputRowCount());

        // IF ( c1 >= 20 , c4 = 10 AND IF ( c1 >= 30 , c2 = 50 , c3 = 80 ) , c3 = 70 )
        BinaryPredicateOperator c4Eq10Predicate =
                new BinaryPredicateOperator(BinaryType.EQ, columnRef4, ConstantOperator.createInt(10));
        compoundInnerIfPredicate = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                c4Eq10Predicate, innerIfPredicate);
        outerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN,
                        List.of(c1Ge20Condition, compoundInnerIfPredicate, c3Eq70Predicate));

        result = PredicateStatisticsCalculator.statisticsCalculate(outerIfPredicate, statistics);
        Assertions.assertEquals(4.0, (int) result.getOutputRowCount());

        // IF ( c1 >= 20 , c4 = 10 OR IF ( c1 >= 30 , c2 = 50 , c3 = 80 ) , c3 = 70 )
        compoundInnerIfPredicate = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                c4Eq10Predicate, innerIfPredicate);
        outerIfPredicate = new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN,
                List.of(c1Ge20Condition, compoundInnerIfPredicate, c3Eq70Predicate));

        result = PredicateStatisticsCalculator.statisticsCalculate(outerIfPredicate, statistics);
        Assertions.assertEquals(20.0, (int) result.getOutputRowCount());

        // IF ( NOT IF ( c1 >= 20 , c2 >= 50 , c2 <= 70 ) , c3 = 80 , c3 = 70 )
        BinaryPredicateOperator c1Ge20Predicate =
                new BinaryPredicateOperator(BinaryType.GE, columnRef1, ConstantOperator.createInt(20));
        BinaryPredicateOperator c2Ge50Predicate =
                new BinaryPredicateOperator(BinaryType.GE, columnRef2, ConstantOperator.createInt(50));
        BinaryPredicateOperator c2Le70Predicate =
                new BinaryPredicateOperator(BinaryType.LE, columnRef2, ConstantOperator.createInt(70));
        innerIfPredicate =
                new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN, List.of(c1Ge20Predicate, c2Ge50Predicate, c2Le70Predicate));
        compoundInnerIfPredicate =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, innerIfPredicate);
        outerIfPredicate = new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN,
                List.of(compoundInnerIfPredicate, c3Eq80Predicate, c3Eq70Predicate));

        result = PredicateStatisticsCalculator.statisticsCalculate(outerIfPredicate, statistics);
        Assertions.assertEquals(11.0, (int) result.getOutputRowCount());

        // IF ( c4 = 10 AND IF ( c1 >= 20 , c2 >= 50 , c2 <= 70 ) , c3 = 80 , c3 = 70 )
        compoundInnerIfPredicate =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, c4Eq10Predicate, innerIfPredicate);
        outerIfPredicate = new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN,
                List.of(compoundInnerIfPredicate, c3Eq80Predicate, c3Eq70Predicate));

        result = PredicateStatisticsCalculator.statisticsCalculate(outerIfPredicate, statistics);
        Assertions.assertEquals(11.0, (int) result.getOutputRowCount());

        // IF ( c4 = 10 OR IF ( c1 >= 20 , c2 >= 50 , c2 <= 70 ) , c3 = 80 , c3 = 70 )
        compoundInnerIfPredicate =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, c4Eq10Predicate, innerIfPredicate);
        outerIfPredicate = new CallOperator(FunctionSet.IF, BooleanType.BOOLEAN,
                List.of(compoundInnerIfPredicate, c3Eq80Predicate, c3Eq70Predicate));

        result = PredicateStatisticsCalculator.statisticsCalculate(outerIfPredicate, statistics);
        Assertions.assertEquals(11.0, (int) result.getOutputRowCount());
    }

    @Test
    public void testOrPredicateShouldCorrectlyModifyNullsFractionWithoutNulls() {
        // GIVEN
        final var aColumn = new ColumnRefOperator(0, VarcharType.VARCHAR, "a", true);
        final var bColumn = new ColumnRefOperator(1, VarcharType.VARCHAR, "b", true);

        double origRowCount = 10_000_000;
        final var statistics = Statistics.builder()
                .setOutputRowCount(origRowCount)
                .addColumnStatistic(aColumn, ColumnStatistic.builder()
                        .setNullsFraction(0.0)
                        .setDistinctValuesCount(1_000_000)
                        .setAverageRowSize(10)
                        .build())
                .addColumnStatistic(bColumn, ColumnStatistic.builder()
                        .setNullsFraction(0.0)
                        .setDistinctValuesCount(1_000_000)
                        .setAverageRowSize(10)
                        .build())
                .build();

        // WHEN
        // (a IS NULL) OR (b IS NOT NULL)
        final var or1 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new IsNullPredicateOperator(false, aColumn), new IsNullPredicateOperator(true, bColumn));
        final var afterOr1 = PredicateStatisticsCalculator.statisticsCalculate(or1, statistics);

        // THEN
        Assertions.assertEquals(origRowCount, afterOr1.getOutputRowCount(), 0.001);
        Assertions.assertEquals(0.0, afterOr1.getColumnStatistic(aColumn).getNullsFraction(), 0.001);

        // GIVEN
        // (a IS NOT NULL) OR (b IS NULL)
        final var or2 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new IsNullPredicateOperator(true, aColumn), new IsNullPredicateOperator(false, bColumn));

        // WHEN
        final var afterOr2 = PredicateStatisticsCalculator.statisticsCalculate(or2, statistics);

        // THEN
        Assertions.assertEquals(origRowCount, afterOr2.getOutputRowCount(), 0.001);
        Assertions.assertEquals(0.0, afterOr2.getColumnStatistic(bColumn).getNullsFraction(), 0.001);

        // GIVEN
        // ((a IS NULL) OR (b IS NOT NULL)) AND ((a IS NOT NULL) OR (b IS NULL))
        final var andOfOrs = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, or1, or2);

        // WHEN
        final var afterAnd = PredicateStatisticsCalculator.statisticsCalculate(andOfOrs, statistics);

        // THEN
        Assertions.assertEquals(origRowCount, afterAnd.getOutputRowCount(), 0.001);
        Assertions.assertEquals(0.0, afterAnd.getColumnStatistic(aColumn).getNullsFraction(), 0.001);
        Assertions.assertEquals(0.0, afterAnd.getColumnStatistic(bColumn).getNullsFraction(), 0.001);
    }

    @Test
    public void testOrPredicateShouldCorrectlyModifyNullsFractionWithNulls() {
        final var aColumn = new ColumnRefOperator(0, VarcharType.VARCHAR, "a", true);
        final var bColumn = new ColumnRefOperator(1, VarcharType.VARCHAR, "b", true);

        double origRowCount = 10_000_000;
        final var statistics = Statistics.builder()
                .setOutputRowCount(origRowCount)
                .addColumnStatistic(aColumn, ColumnStatistic.builder()
                        .setNullsFraction(0.2)
                        .setDistinctValuesCount(1_000_000)
                        .setAverageRowSize(10)
                        .build())
                .addColumnStatistic(bColumn, ColumnStatistic.builder()
                        .setNullsFraction(0.3).
                        setDistinctValuesCount(1_000_000)
                        .setAverageRowSize(10)
                        .build())
                .build();

        // (a IS NULL) OR (b IS NOT NULL)
        CompoundPredicateOperator or1 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new IsNullPredicateOperator(false, aColumn), new IsNullPredicateOperator(true, bColumn));

        // WHEN
        Statistics afterOr = PredicateStatisticsCalculator.statisticsCalculate(or1, statistics);

        // THEN
        double origNulls = 0.2 * origRowCount;
        double afterOrNulls = afterOr.getColumnStatistic(aColumn).getNullsFraction() * afterOr.getOutputRowCount();
        Assertions.assertTrue(afterOrNulls <= origNulls);
    }

    @Test
    public void testOrPredicateShouldNotDoubleCountOverlappingNulls() {
        // GIVEN a 1000-row table where:
        //   - a has 20% nulls -> 200 rows have a = NULL
        //   - b and c each match half the rows on "= 1" (2 distinct values 0/1, no nulls)
        final var aColumn = new ColumnRefOperator(0, VarcharType.VARCHAR, "a", true);
        final var bColumn = new ColumnRefOperator(1, IntegerType.INT, "b", true);
        final var cColumn = new ColumnRefOperator(2, IntegerType.INT, "c", true);

        double rows = 1000;
        final var statistics = Statistics.builder()
                .setOutputRowCount(rows)
                .addColumnStatistic(aColumn, ColumnStatistic.builder()
                        .setNullsFraction(0.2).setDistinctValuesCount(100).setAverageRowSize(10).build())
                .addColumnStatistic(bColumn, ColumnStatistic.builder()
                        .setNullsFraction(0.0).setMinValue(0).setMaxValue(1).setDistinctValuesCount(2).build())
                .addColumnStatistic(cColumn, ColumnStatistic.builder()
                        .setNullsFraction(0.0).setMinValue(0).setMaxValue(1).setDistinctValuesCount(2).build())
                .build();

        // WHEN
        // (a IS NULL AND b = 1) OR (c = 1)
        final var leftArm = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new IsNullPredicateOperator(false, aColumn),
                new BinaryPredicateOperator(BinaryType.EQ, bColumn, ConstantOperator.createInt(1)));
        final var rightArm = new BinaryPredicateOperator(BinaryType.EQ, cColumn, ConstantOperator.createInt(1));
        final var or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, leftArm, rightArm);
        final var afterOr = PredicateStatisticsCalculator.statisticsCalculate(or, statistics);

        // THEN
        // The two arms' null rows are unioned, counting the rows they share only once:
        //   left arm  (a IS NULL AND b = 1): 100 null rows
        //   right arm (c = 1):               100 null rows  (20% of its 500 rows)
        //   shared    (both, b = 1 AND c = 1): 50 null rows
        //   => 100 + 100 - 50 = 150 null rows
        double nullRows = afterOr.getColumnStatistic(aColumn).getNullsFraction() * afterOr.getOutputRowCount();
        Assertions.assertEquals(150, nullRows, 0.001);
    }
}
