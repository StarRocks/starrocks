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

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

public class PredicateStatisticsCalculatorTest {
    @Test
    public void testDateBinaryPredicate() throws Exception {
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
        Assert.assertEquals(59.9613, estimatedStatistics.getOutputRowCount(), 0.001);
    }

    @Test
    public void testDateCompoundPredicate() throws Exception {
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
        Assert.assertEquals(58.0270, estimatedStatistics.getOutputRowCount(), 0.001);
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

        Assert.assertEquals(12.49, estimatedStatistics.getOutputRowCount(), 0.1);
        Assert.assertEquals(10, estimatedStatistics.getColumnStatistic(c1).getDistinctValuesCount(), 0.001);
        Assert.assertEquals(0, estimatedStatistics.getColumnStatistic(c1).getNullsFraction(), 0.001);
        Assert.assertEquals(10, estimatedStatistics.getColumnStatistic(c2).getDistinctValuesCount(), 0.001);
        Assert.assertEquals(0, estimatedStatistics.getColumnStatistic(c2).getNullsFraction(), 0.001);
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
        Assert.assertEquals(5000, estimatedStatistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(1, estimatedStatistics.getColumnStatistic(c1).getNullsFraction(), 0.001);
    }
}
