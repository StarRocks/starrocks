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
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
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
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.DATE, "id_date", true);
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
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.DATE, "id_date", true);
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
        ColumnRefOperator c1 = new ColumnRefOperator(0, Type.INT, "c1", true);
        ColumnRefOperator c2 = new ColumnRefOperator(1, Type.INT, "c2", true);

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
        ColumnRefOperator c1 = new ColumnRefOperator(0, Type.INT, "c1", true);
        Statistics statistics = Statistics.builder()
                .addColumnStatistic(c1, ColumnStatistic.builder().setNullsFraction(0.5).setDistinctValuesCount(10).build())
                .setOutputRowCount(10000).build();

        BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(
                BinaryType.EQ_FOR_NULL, c1, ConstantOperator.createNull(Type.INT));
        Statistics estimatedStatistics =
                PredicateStatisticsCalculator.statisticsCalculate(binaryPredicateOperator, statistics);
        Assertions.assertEquals(5000, estimatedStatistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(1, estimatedStatistics.getColumnStatistic(c1).getNullsFraction(), 0.001);
    }

    @Test
    public void testConcatExpressionCalculate() {
        ColumnRefOperator c1 = new ColumnRefOperator(0, Type.VARCHAR, "c1", true);
        ConstantOperator c2 = new ConstantOperator("-", Type.VARCHAR);
        ColumnRefOperator c3 = new ColumnRefOperator(1, Type.VARCHAR, "c3", true);
        CallOperator concat = new CallOperator("concat", Type.VARCHAR, Lists.newArrayList(c1, c2, c3));
        Statistics statistics = Statistics.builder()
                .addColumnStatistic(c1, ColumnStatistic.builder().setNullsFraction(0.2).setDistinctValuesCount(10).build())
                .addColumnStatistic(c3, ColumnStatistic.builder().setNullsFraction(0.4).setDistinctValuesCount(10).build())
                .setOutputRowCount(10000).build();
        ColumnStatistic estimatedStatistics = ExpressionStatisticCalculator.calculate(concat, statistics);
        Assertions.assertEquals(0.52, estimatedStatistics.getNullsFraction(), 0.001);
    }

    @Test
    public void testEstimateInPredicateWithHistogramNumeric() {
        ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.INT, "c1", true);

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
        ColumnRefOperator columnRef = new ColumnRefOperator(3, Type.BIGINT, "c3", true);

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
        ColumnRefOperator columnRef = new ColumnRefOperator(2, Type.VARCHAR, "c2", true);

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
        ColumnRefOperator columnRef = new ColumnRefOperator(6, Type.BOOLEAN, "c6", true);

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
        ColumnRefOperator columnRef = new ColumnRefOperator(7, Type.INT, "c7", true);

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
}
