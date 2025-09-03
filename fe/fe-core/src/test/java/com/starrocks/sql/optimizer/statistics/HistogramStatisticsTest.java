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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HistogramStatisticsTest {
    @Test
    public void testColumnToConstant() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.BIGINT, "v1", true);

        List<Bucket> bucketList = new ArrayList<>();
        bucketList.add(new Bucket(1D, 10D, 100L, 20L));
        bucketList.add(new Bucket(15D, 20D, 200L, 20L));
        bucketList.add(new Bucket(21D, 36D, 300L, 20L));
        bucketList.add(new Bucket(40D, 45D, 400L, 20L));
        bucketList.add(new Bucket(46D, 46D, 500L, 100L));
        bucketList.add(new Bucket(47D, 47D, 600L, 100L));
        bucketList.add(new Bucket(48D, 60D, 700L, 20L));
        bucketList.add(new Bucket(61D, 65D, 800L, 20L));
        bucketList.add(new Bucket(66D, 99D, 900L, 20L));
        bucketList.add(new Bucket(100D, 100D, 1000L, 100L));
        Histogram histogram = new Histogram(bucketList, Maps.newHashMap());

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(1000);
        builder.addColumnStatistic(columnRefOperator, ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(100)
                .setNullsFraction(0)
                .setAverageRowSize(20)
                .setDistinctValuesCount(20)
                .setHistogram(histogram)
                .build());
        Statistics statistics = builder.build();

        check(columnRefOperator, "GT", 0, statistics, 1000);
        check(columnRefOperator, "GT", 1, statistics, 1000);
        check(columnRefOperator, "GT", 10, statistics, 900);
        check(columnRefOperator, "GT", 12, statistics, 900);
        check(columnRefOperator, "GT", 15, statistics, 900);
        check(columnRefOperator, "GT", 20, statistics, 800);
        check(columnRefOperator, "GT", 25, statistics, 773);
        check(columnRefOperator, "GT", 37, statistics, 700);
        check(columnRefOperator, "GT", 48, statistics, 400);
        check(columnRefOperator, "GT", 49, statistics, 391);
        check(columnRefOperator, "GT", 99, statistics, 100);
        check(columnRefOperator, "GT", 100, statistics, 50);
        check(columnRefOperator, "GT", 105, statistics, 1);

        check(columnRefOperator, "GE", 0, statistics, 1000);
        check(columnRefOperator, "GE", 1, statistics, 1000);
        check(columnRefOperator, "GE", 10, statistics, 920);
        check(columnRefOperator, "GE", 12, statistics, 900);
        check(columnRefOperator, "GE", 15, statistics, 900);
        check(columnRefOperator, "GE", 20, statistics, 820);
        check(columnRefOperator, "GE", 25, statistics, 773);
        check(columnRefOperator, "GE", 37, statistics, 700);
        check(columnRefOperator, "GE", 48, statistics, 400);
        check(columnRefOperator, "GE", 49, statistics, 391);
        check(columnRefOperator, "GE", 99, statistics, 120);
        check(columnRefOperator, "GE", 100, statistics, 100);
        check(columnRefOperator, "GE", 105, statistics, 1);

        check(columnRefOperator, "LT", 0, statistics, 1);
        check(columnRefOperator, "LT", 1, statistics, 1);
        check(columnRefOperator, "LT", 10, statistics, 80);
        check(columnRefOperator, "LT", 12, statistics, 100);
        check(columnRefOperator, "LT", 15, statistics, 100);
        check(columnRefOperator, "LT", 20, statistics, 180);
        check(columnRefOperator, "LT", 25, statistics, 221);
        check(columnRefOperator, "LT", 37, statistics, 300);
        check(columnRefOperator, "LT", 46, statistics, 400);
        check(columnRefOperator, "LT", 48, statistics, 600);
        check(columnRefOperator, "LT", 49, statistics, 606);
        check(columnRefOperator, "LT", 99, statistics, 880);
        check(columnRefOperator, "LT", 100, statistics, 900);
        check(columnRefOperator, "LT", 105, statistics, 1000);

        check(columnRefOperator, "LE", 0, statistics, 1);
        check(columnRefOperator, "LE", 1, statistics, 1);
        check(columnRefOperator, "LE", 10, statistics, 100);
        check(columnRefOperator, "LE", 12, statistics, 100);
        check(columnRefOperator, "LE", 15, statistics, 100);
        check(columnRefOperator, "LE", 20, statistics, 200);
        check(columnRefOperator, "LE", 25, statistics, 221);
        check(columnRefOperator, "LE", 37, statistics, 300);
        check(columnRefOperator, "LE", 48, statistics, 600);
        check(columnRefOperator, "LE", 49, statistics, 606);
        check(columnRefOperator, "LE", 99, statistics, 900);
        check(columnRefOperator, "LE", 100, statistics, 1000);
        check(columnRefOperator, "LE", 105, statistics, 1000);

        between(columnRefOperator, "GT", 1, "LT", 10, statistics, 80);
        between(columnRefOperator, "GT", 1, "LT", 16, statistics, 116);
        between(columnRefOperator, "GT", 1, "LT", 36, statistics, 280);
        between(columnRefOperator, "GT", 1, "LT", 43, statistics, 348);
        between(columnRefOperator, "GT", 16, "LT", 47, statistics, 380);
        between(columnRefOperator, "GT", 16, "LT", 53, statistics, 513);
        between(columnRefOperator, "GT", 46, "LT", 47, statistics, 1);
        between(columnRefOperator, "GT", 60, "LT", 99, statistics, 180);
        between(columnRefOperator, "GT", 1, "LT", 100, statistics, 900);

        between(columnRefOperator, "GE", 1, "LE", 10, statistics, 100);
        between(columnRefOperator, "GE", 1, "LE", 16, statistics, 116);
        between(columnRefOperator, "GE", 1, "LE", 36, statistics, 300);
        between(columnRefOperator, "GE", 1, "LE", 43, statistics, 348);
        between(columnRefOperator, "GE", 16, "LE", 47, statistics, 480);
        between(columnRefOperator, "GE", 16, "LE", 53, statistics, 513);
        between(columnRefOperator, "GE", 46, "LE", 47, statistics, 200);
        between(columnRefOperator, "GE", 60, "LE", 99, statistics, 220);
        between(columnRefOperator, "GE", 1, "LE", 100, statistics, 1000);
        between(columnRefOperator, "GE", 1, "LE", 1000, statistics, 1000);
    }

    void check(ColumnRefOperator columnRefOperator, String type, int constant, Statistics statistics, int rowCount) {
        BinaryPredicateOperator binaryPredicateOperator
                = new BinaryPredicateOperator(BinaryType.valueOf(type),
                columnRefOperator, ConstantOperator.createBigint(constant));
        Statistics estimated = PredicateStatisticsCalculator.statisticsCalculate(binaryPredicateOperator, statistics);
        Assertions.assertEquals(rowCount, estimated.getOutputRowCount(), 0.1);
    }

    void between(ColumnRefOperator columnRefOperator, String greaterType, int min, String lessType,
                 int max, Statistics statistics, int rowCount) {
        BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(
                BinaryType.valueOf(greaterType),
                columnRefOperator,
                ConstantOperator.createBigint(min));
        Statistics estimated = PredicateStatisticsCalculator.statisticsCalculate(binaryPredicateOperator, statistics);

        binaryPredicateOperator = new BinaryPredicateOperator(BinaryType.valueOf(lessType),
                columnRefOperator,
                ConstantOperator.createBigint(max));
        estimated = PredicateStatisticsCalculator.statisticsCalculate(binaryPredicateOperator, estimated);

        Assertions.assertEquals(rowCount, estimated.getOutputRowCount(), 0.1);
    }

    @Test
    public void testColumnToColumn() {
        ColumnRefOperator leftColumnRefOperator = new ColumnRefOperator(0, Type.BIGINT, "v1", true);
        List<Bucket> leftBucketList = new ArrayList<>();
        leftBucketList.add(new Bucket(1D, 10D, 100L, 20L));
        leftBucketList.add(new Bucket(15D, 20D, 200L, 20L));
        leftBucketList.add(new Bucket(21D, 36D, 300L, 20L));
        leftBucketList.add(new Bucket(40D, 45D, 400L, 20L));
        leftBucketList.add(new Bucket(46D, 46D, 500L, 100L));
        leftBucketList.add(new Bucket(47D, 47D, 600L, 100L));
        leftBucketList.add(new Bucket(48D, 58D, 700L, 20L));
        leftBucketList.add(new Bucket(61D, 65D, 800L, 20L));
        leftBucketList.add(new Bucket(66D, 99D, 900L, 20L));
        leftBucketList.add(new Bucket(100D, 100D, 1000L, 100L));
        HashMap<String, Long> leftMcv = new HashMap<>();
        leftMcv.put("59", 500L);
        leftMcv.put("38", 300L);
        leftMcv.put("17", 200L);
        Histogram leftHistogram = new Histogram(leftBucketList, leftMcv);

        ColumnRefOperator rightColumnRefOperator = new ColumnRefOperator(1, Type.BIGINT, "v2", true);
        List<Bucket> rightBucketList = new ArrayList<>();
        rightBucketList.add(new Bucket(1D, 15D, 200L, 20L));
        rightBucketList.add(new Bucket(18D, 38D, 300L, 20L));
        rightBucketList.add(new Bucket(41D, 55D, 400L, 20L));
        rightBucketList.add(new Bucket(56D, 56D, 500L, 100L));
        rightBucketList.add(new Bucket(57D, 57D, 600L, 100L));
        rightBucketList.add(new Bucket(58D, 67D, 700L, 20L));
        rightBucketList.add(new Bucket(70D, 98D, 900L, 20L));
        rightBucketList.add(new Bucket(100D, 100D, 1000L, 100L));
        HashMap<String, Long> rightMcv = new HashMap<>();
        rightMcv.put("99", 500L);
        rightMcv.put("16", 300L);
        rightMcv.put("17", 200L);
        Histogram rightHistogram = new Histogram(rightBucketList, rightMcv);

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(2000 * 2000);
        builder.addColumnStatistic(leftColumnRefOperator, ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(100)
                .setNullsFraction(0)
                .setAverageRowSize(20)
                .setDistinctValuesCount(20)
                .setHistogram(leftHistogram)
                .build());
        builder.addColumnStatistic(rightColumnRefOperator, ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(100)
                .setNullsFraction(0)
                .setAverageRowSize(20)
                .setDistinctValuesCount(20)
                .setHistogram(rightHistogram)
                .build());
        Statistics statistics = builder.build();
        BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(BinaryType.EQ,
                leftColumnRefOperator, rightColumnRefOperator);

        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.getSessionVariable().setCboEnableHistogramJoinEstimation(false);
        Statistics estimated = PredicateStatisticsCalculator.statisticsCalculate(binaryPredicateOperator, statistics);
        Assertions.assertEquals(2, estimated.getColumnStatistics().size());
        Assertions.assertEquals(estimated.getColumnStatistic(leftColumnRefOperator).getHistogram(), leftHistogram);
        Assertions.assertEquals(estimated.getColumnStatistic(rightColumnRefOperator).getHistogram(), rightHistogram);
        Assertions.assertEquals(200000, estimated.getOutputRowCount(), 0.1);

        connectContext.getSessionVariable().setCboEnableHistogramJoinEstimation(true);
        estimated = PredicateStatisticsCalculator.statisticsCalculate(binaryPredicateOperator, statistics);
        Assertions.assertEquals(2, estimated.getColumnStatistics().size());
        Assertions.assertEquals(5, estimated.getColumnStatistic(leftColumnRefOperator).getHistogram().getMCV().size());
        Assertions.assertEquals(15, estimated.getColumnStatistic(leftColumnRefOperator).getHistogram().getBuckets().size());
        Assertions.assertEquals(5, estimated.getColumnStatistic(rightColumnRefOperator).getHistogram().getMCV().size());
        Assertions.assertEquals(15, estimated.getColumnStatistic(rightColumnRefOperator).getHistogram().getBuckets().size());
        Assertions.assertNotEquals(estimated.getColumnStatistic(leftColumnRefOperator).getHistogram(), leftHistogram);
        Assertions.assertNotEquals(estimated.getColumnStatistic(rightColumnRefOperator).getHistogram(), rightHistogram);
        Assertions.assertEquals(83576, estimated.getOutputRowCount(), 0.1);
    }

    @Test
    public void testNotHitBucketInHist() {
        List<Bucket> bucketList = new ArrayList<>();
        bucketList.add(new Bucket(1D, 10D, 100L, 20L));
        bucketList.add(new Bucket(15D, 20D, 200L, 20L));
        Histogram histogram = new Histogram(bucketList, Maps.newHashMap());

        // histogram doesn't contain the predicate range
        ColumnStatistic columnStatistic = new ColumnStatistic(1, 50, 0, 4, 500,
                histogram, ColumnStatistic.StatisticType.ESTIMATE);
        Optional<Histogram> notExist = BinaryPredicateStatisticCalculator.updateHistWithGreaterThan(columnStatistic,
                Optional.of(new ConstantOperator(400, Type.BIGINT)), true);
        Assertions.assertFalse(notExist.isPresent());

        notExist = BinaryPredicateStatisticCalculator.updateHistWithLessThan(columnStatistic,
                Optional.of(new ConstantOperator(-1, Type.BIGINT)), true);
        Assertions.assertFalse(notExist.isPresent());

        // only one bucket in histogram can cover the predicate range
        Optional<Histogram> exist = BinaryPredicateStatisticCalculator.updateHistWithGreaterThan(columnStatistic,
                Optional.of(new ConstantOperator(18, Type.BIGINT)), true);
        Assertions.assertEquals(exist.get().getBuckets().size(), 1);
        exist = BinaryPredicateStatisticCalculator.updateHistWithLessThan(columnStatistic,
                Optional.of(new ConstantOperator(3, Type.BIGINT)), true);
        Assertions.assertEquals(exist.get().getBuckets().size(), 1);

        // all the two bucket in histogram can cover the predicate range
        exist = BinaryPredicateStatisticCalculator.updateHistWithGreaterThan(columnStatistic,
                Optional.of(new ConstantOperator(3, Type.BIGINT)), true);
        Assertions.assertEquals(exist.get().getBuckets().size(), 2);
        exist = BinaryPredicateStatisticCalculator.updateHistWithLessThan(columnStatistic,
                Optional.of(new ConstantOperator(18, Type.BIGINT)), true);
        Assertions.assertEquals(exist.get().getBuckets().size(), 2);
    }

    @Test
    public void testUpdateHistWithEqual() {
        List<Bucket> bucketList = new ArrayList<>();
        bucketList.add(new Bucket(1D, 10D, 100L, 20L));
        bucketList.add(new Bucket(15D, 20D, 200L, 20L));
        Map<String, Long> mcv = new HashMap<>();
        mcv.put("22", 100L);
        Histogram histogram = new Histogram(bucketList, mcv);
        ColumnStatistic columnStatistic = new ColumnStatistic(1, 50, 0, 4, 500,
                histogram, ColumnStatistic.StatisticType.ESTIMATE);

        // histogram doesn't contain the constant
        Optional<Histogram> notExist = BinaryPredicateStatisticCalculator.updateHistWithEqual(columnStatistic,
                Optional.of(new ConstantOperator(12, Type.BIGINT)));
        Assertions.assertFalse(notExist.isPresent());

        // histogram contains the constant in the mcv
        Optional<Histogram> exist = BinaryPredicateStatisticCalculator.updateHistWithEqual(columnStatistic,
                Optional.of(new ConstantOperator(22, Type.BIGINT)));
        Assertions.assertTrue(exist.isPresent());
        Assertions.assertEquals(exist.get().getBuckets().size(), 0);
        Assertions.assertEquals(exist.get().getMCV(), mcv);

        // histogram contains the constant in a bucket
        exist = BinaryPredicateStatisticCalculator.updateHistWithEqual(columnStatistic,
                Optional.of(new ConstantOperator(2, Type.BIGINT)));
        Assertions.assertTrue(exist.isPresent());
        Assertions.assertEquals(exist.get().getBuckets().size(), 0);
        Assertions.assertTrue(exist.get().getMCV().containsKey("2"));
    }

    @Test
    public void testHitBucketInHist() {
        List<Bucket> bucketList = new ArrayList<>();
        bucketList.add(new Bucket(1D, 10D, 100L, 20L));
        bucketList.add(new Bucket(15D, 20D, 200L, 20L));
        bucketList.add(new Bucket(25, 30, 300L, 20L));

        Map<String, Long> mcv = Maps.newHashMap();
        mcv.put("11", 500L);
        Histogram histogram = new Histogram(bucketList, mcv);
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.BIGINT, "v1", true);
        ColumnStatistic columnStatistic = new ColumnStatistic(1, 50, 0, 4, 40,
                histogram, ColumnStatistic.StatisticType.ESTIMATE);
        BinaryPredicateOperator eq10 = new BinaryPredicateOperator(
                BinaryType.EQ,
                columnRefOperator,
                ConstantOperator.createBigint(10));
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(100000);
        builder.addColumnStatistic(columnRefOperator, columnStatistic);
        Statistics statistics = builder.build();

        // hit upper bound
        Statistics estimated = BinaryPredicateStatisticCalculator.estimateColumnToConstantComparison(
                Optional.of(columnRefOperator),
                columnStatistic, eq10, Optional.of(ConstantOperator.createBigint(10)), statistics);
        Assertions.assertEquals(20, estimated.getOutputRowCount(), 0.001);

        // in second bucket
        BinaryPredicateOperator eq15 = new BinaryPredicateOperator(
                BinaryType.EQ,
                columnRefOperator,
                ConstantOperator.createBigint(15));
        estimated = BinaryPredicateStatisticCalculator.estimateColumnToConstantComparison(Optional.of(columnRefOperator),
                columnStatistic, eq10, Optional.of(ConstantOperator.createBigint(15)), statistics);
        Assertions.assertEquals(16, estimated.getOutputRowCount(), 0.001);

        // not in bucket
        BinaryPredicateOperator eq35 = new BinaryPredicateOperator(
                BinaryType.EQ,
                columnRefOperator,
                ConstantOperator.createBigint(35));
        estimated = BinaryPredicateStatisticCalculator.estimateColumnToConstantComparison(Optional.of(columnRefOperator),
                columnStatistic, eq35, Optional.of(ConstantOperator.createBigint(35)), statistics);
        Assertions.assertEquals(961.53846, estimated.getOutputRowCount(), 0.001);
    }

    @Test
    public void testHitMCV() {
        Map<String, Long> mcv = Maps.newHashMap();
        mcv.put("0", 500L);
        Histogram histogram = new Histogram(new ArrayList<>(), mcv);
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.BOOLEAN, "b1", true);
        ColumnStatistic columnStatistic = new ColumnStatistic(0, 1, 0, 4, 2, histogram, ColumnStatistic.StatisticType.ESTIMATE);
        BinaryPredicateOperator eq10 = new BinaryPredicateOperator(
                BinaryType.EQ,
                columnRefOperator,
                ConstantOperator.createBoolean(false));
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(100000);
        builder.addColumnStatistic(columnRefOperator, columnStatistic);
        Statistics statistics = builder.build();

        // hit upper bound
        Statistics estimated = BinaryPredicateStatisticCalculator.estimateColumnToConstantComparison(
                Optional.of(columnRefOperator),
                columnStatistic, eq10, Optional.of(ConstantOperator.createBoolean(false)), statistics);
        Assertions.assertEquals(500L, estimated.getOutputRowCount(), 0.001);


        mcv = Maps.newHashMap();
        mcv.put("0", 500L);
        mcv.put("1", 500L);
        histogram = new Histogram(new ArrayList<>(), mcv);
        columnRefOperator = new ColumnRefOperator(0, Type.BOOLEAN, "b1", true);
        columnStatistic = new ColumnStatistic(0, 1, 0, 4, 2, histogram, ColumnStatistic.StatisticType.ESTIMATE);
        builder = Statistics.builder();
        builder.setOutputRowCount(100000);
        builder.addColumnStatistic(columnRefOperator, columnStatistic);
        statistics = builder.build();

        estimated = PredicateStatisticsCalculator.statisticsCalculate(columnRefOperator, statistics);
        Assertions.assertEquals(500L, estimated.getOutputRowCount(), 0.001);
    }


    @Test
    public void testUpdateHistWithJoin() {
        // no intersection.
        List<Bucket> bucketListLeft = new ArrayList<>();
        bucketListLeft.add(new Bucket(1D, 4D, 100L, 20L));
        bucketListLeft.add(new Bucket(15D, 24D, 200L, 20L));
        Map<String, Long> mcvLeft = new HashMap<>();
        mcvLeft.put("12", 300L);
        mcvLeft.put("22", 100L);
        Histogram histogramLeft = new Histogram(bucketListLeft, mcvLeft);
        ColumnStatistic columnStatisticLeft = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramLeft, ColumnStatistic.StatisticType.ESTIMATE);

        List<Bucket> bucketListRight = new ArrayList<>();
        bucketListRight.add(new Bucket(5D, 11D, 100L, 20L));
        bucketListRight.add(new Bucket(30D, 35D, 200L, 20L));
        Map<String, Long> mcvRight = new HashMap<>();
        mcvRight.put("25", 80L);
        mcvRight.put("9", 50L);
        Histogram histogramRight = new Histogram(bucketListRight, mcvRight);
        ColumnStatistic columnStatisticRight = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramRight, ColumnStatistic.StatisticType.ESTIMATE);

        Optional<Histogram> notExist = BinaryPredicateStatisticCalculator.updateHistWithJoin(columnStatisticLeft, Type.BIGINT,
                columnStatisticRight, Type.BIGINT);
        Assertions.assertTrue(notExist.isEmpty());

        // MCV to MCV intersection.
        mcvLeft = new HashMap<>();
        mcvLeft.put("10", 300L);
        mcvLeft.put("22", 100L);
        histogramLeft = new Histogram(null, mcvLeft);
        columnStatisticLeft = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramLeft, ColumnStatistic.StatisticType.ESTIMATE);

        mcvRight = new HashMap<>();
        mcvRight.put("22", 80L);
        mcvRight.put("9", 50L);
        histogramRight = new Histogram(null, mcvRight);
        columnStatisticRight = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramRight, ColumnStatistic.StatisticType.ESTIMATE);

        Optional<Histogram> exist = BinaryPredicateStatisticCalculator.updateHistWithJoin(columnStatisticLeft, Type.BIGINT,
                columnStatisticRight, Type.BIGINT);
        Assertions.assertTrue(exist.isPresent());
        Assertions.assertNull(exist.get().getBuckets());
        Assertions.assertEquals(exist.get().getMCV().size(), 1);
        Assertions.assertEquals(exist.get().getMCV().get("22").longValue(), 100 * 80);

        // MCV to bucket intersection (upper).
        bucketListLeft = new ArrayList<>();
        bucketListLeft.add(new Bucket(1D, 4D, 100L, 20L));
        bucketListLeft.add(new Bucket(15D, 23D, 200L, 20L));
        mcvLeft = new HashMap<>();
        mcvLeft.put("10", 300L);
        mcvLeft.put("22", 100L);
        histogramLeft = new Histogram(bucketListLeft, mcvLeft);
        columnStatisticLeft = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramLeft, ColumnStatistic.StatisticType.ESTIMATE);

        bucketListRight = new ArrayList<>();
        bucketListRight.add(new Bucket(5D, 10D, 100L, 20L));
        bucketListRight.add(new Bucket(30D, 35D, 200L, 20L));
        mcvRight = new HashMap<>();
        mcvRight.put("23", 80L);
        mcvRight.put("9", 50L);
        histogramRight = new Histogram(bucketListRight, mcvRight);
        columnStatisticRight = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramRight, ColumnStatistic.StatisticType.ESTIMATE);

        exist = BinaryPredicateStatisticCalculator.updateHistWithJoin(columnStatisticLeft, Type.BIGINT,
                columnStatisticRight, Type.BIGINT);
        Assertions.assertTrue(exist.isPresent());
        Assertions.assertTrue(exist.get().getBuckets().isEmpty());
        Assertions.assertEquals(exist.get().getMCV().size(), 2);
        Assertions.assertEquals(exist.get().getMCV().get("10").longValue(), 300 * 20);
        Assertions.assertEquals(exist.get().getMCV().get("23").longValue(), 80 * 20);

        // MCV to bucket intersection (not upper).
        bucketListLeft = new ArrayList<>();
        bucketListLeft.add(new Bucket(1D, 4D, 100L, 20L));
        bucketListLeft.add(new Bucket(15D, 24D, 200L, 20L));
        mcvLeft = new HashMap<>();
        mcvLeft.put("10", 300L);
        mcvLeft.put("22", 100L);
        histogramLeft = new Histogram(bucketListLeft, mcvLeft);
        columnStatisticLeft = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramLeft, ColumnStatistic.StatisticType.ESTIMATE);

        bucketListRight = new ArrayList<>();
        bucketListRight.add(new Bucket(5D, 11D, 100L, 20L));
        bucketListRight.add(new Bucket(30D, 35D, 200L, 20L));
        mcvRight = new HashMap<>();
        mcvRight.put("23", 80L);
        mcvRight.put("9", 50L);
        histogramRight = new Histogram(bucketListRight, mcvRight);
        columnStatisticRight = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramRight, ColumnStatistic.StatisticType.ESTIMATE);

        exist = BinaryPredicateStatisticCalculator.updateHistWithJoin(columnStatisticLeft, Type.BIGINT,
                columnStatisticRight, Type.BIGINT);
        Assertions.assertTrue(exist.isPresent());
        Assertions.assertTrue(exist.get().getBuckets().isEmpty());
        Assertions.assertEquals(exist.get().getMCV().size(), 2);
        Assertions.assertEquals(exist.get().getMCV().get("10").longValue(), 300 * 14);
        Assertions.assertEquals(exist.get().getMCV().get("23").longValue(), 80 * 9);

        // bucket to bucket intersection (upper).
        bucketListLeft = new ArrayList<>();
        bucketListLeft.add(new Bucket(1D, 5D, 100L, 20L));
        bucketListLeft.add(new Bucket(15D, 24D, 200L, 20L));
        histogramLeft = new Histogram(bucketListLeft, new HashMap<>());
        columnStatisticLeft = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramLeft, ColumnStatistic.StatisticType.ESTIMATE);

        bucketListRight = new ArrayList<>();
        bucketListRight.add(new Bucket(5D, 11D, 100L, 20L));
        bucketListRight.add(new Bucket(30D, 35D, 200L, 20L));
        histogramRight = new Histogram(bucketListRight, new HashMap<>());
        columnStatisticRight = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramRight, ColumnStatistic.StatisticType.ESTIMATE);

        exist = BinaryPredicateStatisticCalculator.updateHistWithJoin(columnStatisticLeft, Type.BIGINT,
                columnStatisticRight, Type.BIGINT);
        Assertions.assertTrue(exist.isPresent());
        Assertions.assertTrue(exist.get().getMCV().isEmpty());
        Assertions.assertEquals(exist.get().getBuckets().size(), 1);
        Bucket joinBucket = exist.get().getBuckets().get(0);
        Assertions.assertEquals(joinBucket.getLower(), 5D, 0.001);
        Assertions.assertEquals(joinBucket.getUpper(), 5D, 0.001);
        Assertions.assertEquals(joinBucket.getCount().longValue(), 20L * 14L);
        Assertions.assertEquals(joinBucket.getUpperRepeats().longValue(), 20L * 14L);

        // bucket to bucket intersection (not upper).
        bucketListLeft = new ArrayList<>();
        bucketListLeft.add(new Bucket(1D, 9D, 100L, 20L));
        bucketListLeft.add(new Bucket(15D, 24D, 200L, 20L));
        histogramLeft = new Histogram(bucketListLeft, new HashMap<>());
        columnStatisticLeft = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramLeft, ColumnStatistic.StatisticType.ESTIMATE);

        bucketListRight = new ArrayList<>();
        bucketListRight.add(new Bucket(5D, 11D, 100L, 20L));
        bucketListRight.add(new Bucket(30D, 35D, 200L, 20L));
        histogramRight = new Histogram(bucketListRight, new HashMap<>());
        columnStatisticRight = new ColumnStatistic(1, 50, 0, 4, 500,
                histogramRight, ColumnStatistic.StatisticType.ESTIMATE);

        exist = BinaryPredicateStatisticCalculator.updateHistWithJoin(columnStatisticLeft, Type.BIGINT,
                columnStatisticRight, Type.BIGINT);
        Assertions.assertTrue(exist.isPresent());
        Assertions.assertTrue(exist.get().getMCV().isEmpty());
        Assertions.assertEquals(exist.get().getBuckets().size(), 1);
        joinBucket = exist.get().getBuckets().get(0);
        Assertions.assertEquals(joinBucket.getLower(), 5D, 0.001);
        Assertions.assertEquals(joinBucket.getUpper(), 9D, 0.001);
        Assertions.assertEquals(joinBucket.getCount().longValue(), 833);
        Assertions.assertEquals(joinBucket.getUpperRepeats().longValue(), 20L * 14L);
    }
}
