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
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HistogramStatisticsTest {
    @Test
    public void test() {
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
        check(columnRefOperator, "GT", 100, statistics, 1);
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
        Assert.assertEquals(rowCount, estimated.getOutputRowCount(), 0.1);
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

        Assert.assertEquals(rowCount, estimated.getOutputRowCount(), 0.1);
    }
}
