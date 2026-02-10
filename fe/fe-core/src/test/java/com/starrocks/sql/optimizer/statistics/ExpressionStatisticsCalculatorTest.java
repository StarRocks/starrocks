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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class ExpressionStatisticsCalculatorTest {
    @Test
    public void testVariableReference() {
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(100);
        double min = 0.0;
        double max = 100.0;
        double distinctValue = 100;
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.DATE, "id_date", true);
        Statistics statistics = builder.addColumnStatistic(columnRefOperator,
                        ColumnStatistic.builder().setMinValue(min).setMaxValue(max).
                                setDistinctValuesCount(distinctValue).setNullsFraction(0).setAverageRowSize(10).build())
                .build();
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(columnRefOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), max, 0.0001);
        Assertions.assertEquals(columnStatistic.getMinValue(), min, 0.0001);
        Assertions.assertEquals(columnStatistic.getDistinctValuesCount(), distinctValue, 0.001);
    }

    @Test
    public void testConstant() {
        ConstantOperator constantOperator = ConstantOperator.createBigint(100);
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(constantOperator, null);
        Assertions.assertEquals(columnStatistic.getMinValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);

        ConstantOperator constantOperator1 = ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0, 0));
        ColumnStatistic columnStatistic1 = ExpressionStatisticCalculator.calculate(constantOperator1, null);
        Assertions.assertEquals(columnStatistic1.getMaxValue(), getLongFromDateTime(constantOperator1.getDatetime()),
                0.001);

        ConstantOperator constantOperator2 = ConstantOperator.createChar("123");
        ColumnStatistic columnStatistic2 = ExpressionStatisticCalculator.calculate(constantOperator2, null);
        Assertions.assertTrue(columnStatistic2.isInfiniteRange());
        Assertions.assertEquals(columnStatistic2.getDistinctValuesCount(), 1, 0.001);
    }

    @Test
    public void testnullaryFunctionCall() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.INT, "id", true);

        Statistics.Builder builder = Statistics.builder();
        Statistics statistics = builder.addColumnStatistic(columnRefOperator,
                        ColumnStatistic.builder().setMinValue(0).setMaxValue(100).
                                setDistinctValuesCount(100).setNullsFraction(0).setAverageRowSize(10).build())
                .setOutputRowCount(100).build();

        // test rand/random function
        CallOperator callOperator = new CallOperator(FunctionSet.RAND, Type.DOUBLE, Lists.newArrayList());
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0);
        callOperator = new CallOperator(FunctionSet.RANDOM, Type.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0);
        // test e function
        callOperator = new CallOperator(FunctionSet.E, Type.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), Math.E, 0);
        Assertions.assertEquals(columnStatistic.getMinValue(), Math.E, 0);
        // test pi function
        callOperator = new CallOperator(FunctionSet.PI, Type.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), Math.PI, 0);
        Assertions.assertEquals(columnStatistic.getMinValue(), Math.PI, 0);
        // test curdate function
        callOperator = new CallOperator(FunctionSet.CURDATE, Type.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        long epochDay = LocalDate.now().toEpochDay();
        Assertions.assertTrue(columnStatistic.getMaxValue() <
                LocalDate.ofEpochDay(epochDay + 1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond());
        Assertions.assertTrue(columnStatistic.getMinValue() >
                LocalDate.ofEpochDay(epochDay - 1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond());
        // test curtime/current_time function
        callOperator = new CallOperator(FunctionSet.CURTIME, Type.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        LocalDateTime now = LocalDateTime.now();
        long time = now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond();
        Assertions.assertTrue(columnStatistic.getMaxValue() < time + 1);
        Assertions.assertTrue(columnStatistic.getMinValue() > time - 1);
        callOperator = new CallOperator(FunctionSet.CURRENT_TIME, Type.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        now = LocalDateTime.now();
        time = now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond();
        Assertions.assertTrue(columnStatistic.getMaxValue() < time + 1);
        Assertions.assertTrue(columnStatistic.getMinValue() > time - 1);
        // test current_timestamp/unix_timestamp function
        callOperator = new CallOperator(FunctionSet.CURRENT_TIMESTAMP, Type.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        long timestamp = System.currentTimeMillis() / 1000;
        Assertions.assertTrue(columnStatistic.getMaxValue() < timestamp + 1);
        Assertions.assertTrue(columnStatistic.getMinValue() > timestamp - 1);
        callOperator = new CallOperator(FunctionSet.UNIX_TIMESTAMP, Type.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        timestamp = System.currentTimeMillis() / 1000;
        Assertions.assertTrue(columnStatistic.getMaxValue() < timestamp + 1);
        Assertions.assertTrue(columnStatistic.getMinValue() > timestamp - 1);
    }

    @Test
    public void testUnaryFunctionCall() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.INT, "id", true);
        CallOperator callOperator = new CallOperator(FunctionSet.MAX, Type.INT, Lists.newArrayList(columnRefOperator));

        Statistics.Builder builder = Statistics.builder();
        double min = 0.0;
        double max = 100.0;
        double distinctValue = 100;
        Statistics statistics = builder.addColumnStatistic(columnRefOperator,
                        ColumnStatistic.builder().setMinValue(min).setMaxValue(max).
                                setDistinctValuesCount(distinctValue).setNullsFraction(0).setAverageRowSize(10).build())
                .setOutputRowCount(100).build();
        // test max function
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), max, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), min, 0.001);
        // test min function
        callOperator = new CallOperator(FunctionSet.MIN, Type.INT, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), max, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), min, 0.001);
        // test sign function
        callOperator = new CallOperator(FunctionSet.SIGN, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), -1, 0.001);
        Assertions.assertEquals(columnStatistic.getDistinctValuesCount(), 3, 0.001);
        // test greast function
        callOperator = new CallOperator(FunctionSet.GREATEST, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), max, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), min, 0.001);
        // test least function
        callOperator = new CallOperator(FunctionSet.LEAST, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), max, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), min, 0.001);
        // test sum function
        callOperator = new CallOperator(FunctionSet.SUM, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics, 10);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test count/multi_distinct_count function
        callOperator = new CallOperator(FunctionSet.COUNT, Type.INT, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics, 10);
        Assertions.assertEquals(columnStatistic.getMaxValue(), statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0.0, 0.001);
        Assertions.assertEquals(columnStatistic.getDistinctValuesCount(), 10, 0.001);
        callOperator =
                new CallOperator(FunctionSet.MULTI_DISTINCT_COUNT, Type.INT, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics, 10);
        Assertions.assertEquals(columnStatistic.getMaxValue(), statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0.0, 0.001);
        Assertions.assertEquals(columnStatistic.getDistinctValuesCount(), 10, 0.001);
        // test ascii function
        callOperator = new CallOperator(FunctionSet.ASCII, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 127, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        Assertions.assertEquals(columnStatistic.getDistinctValuesCount(), 10, 128);
        // test year function
        callOperator = new CallOperator(FunctionSet.YEAR, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1970, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 1970, 0.001);
        // test quarter function
        callOperator = new CallOperator(FunctionSet.QUARTER, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 4, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 1, 0.001);
        // test month function
        callOperator = new CallOperator(FunctionSet.MONTH, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 12, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 1, 0.001);
        // test weekofyear function
        callOperator = new CallOperator(FunctionSet.WEEKOFYEAR, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 54, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 1, 0.001);
        // test day function
        callOperator = new CallOperator(FunctionSet.DAY, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 31, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 1, 0.001);
        // test dayofmonth function
        callOperator = new CallOperator(FunctionSet.DAY, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 31, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 1, 0.001);
        // test dayofweek function
        callOperator = new CallOperator(FunctionSet.DAYOFWEEK, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 7, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 1, 0.001);
        // test dayofyear function
        callOperator = new CallOperator(FunctionSet.DAYOFYEAR, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 366, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 1, 0.001);
        // test hour function
        callOperator = new CallOperator(FunctionSet.HOUR, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 23, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test minute function
        callOperator = new CallOperator(FunctionSet.MINUTE, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 59, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test second function
        callOperator = new CallOperator(FunctionSet.SECOND, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 59, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test to_date function
        callOperator = new CallOperator(FunctionSet.TO_DATE, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        LocalDate epochDay = LocalDate.of(1970, 1, 1);
        Assertions.assertEquals(columnStatistic.getMaxValue(),
                epochDay.atStartOfDay(ZoneId.systemDefault()).toEpochSecond(), 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(),
                epochDay.atStartOfDay(ZoneId.systemDefault()).toEpochSecond(), 0.001);
        // test to_days function
        callOperator = new CallOperator(FunctionSet.TO_DAYS, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), ExpressionStatisticCalculator.DAYS_FROM_0_TO_1970, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), ExpressionStatisticCalculator.DAYS_FROM_0_TO_1970, 0.001);
        // test from_days function
        callOperator = new CallOperator(FunctionSet.FROM_DAYS, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(),
                epochDay.atStartOfDay(ZoneId.systemDefault()).toEpochSecond(), 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(),
                epochDay.atStartOfDay(ZoneId.systemDefault()).toEpochSecond(), 0.001);
        // test timestamp function
        callOperator = new CallOperator(FunctionSet.TIMESTAMP, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), max, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), min, 0.001);
        // test abs function
        callOperator = new CallOperator(FunctionSet.ABS, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), max, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), min, 0.001);
        // test acos function
        callOperator = new CallOperator(FunctionSet.ACOS, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), Math.PI, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test asin function
        callOperator = new CallOperator(FunctionSet.ASIN, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), Math.PI, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test atan function
        callOperator = new CallOperator(FunctionSet.ATAN, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), Math.PI / 2, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), -Math.PI / 2, 0.001);
        // test atan2 function
        callOperator = new CallOperator(FunctionSet.ATAN2, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), Math.PI / 2, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), -Math.PI / 2, 0.001);
        // test sin function
        callOperator = new CallOperator(FunctionSet.SIN, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), -1, 0.001);
        // test cos function
        callOperator = new CallOperator(FunctionSet.COS, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), -1, 0.001);
        // test sqrt function
        callOperator = new CallOperator(FunctionSet.SQRT, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 10, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test square function
        callOperator = new CallOperator(FunctionSet.SQUARE, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 10000, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test radians function
        callOperator = new CallOperator(FunctionSet.RADIANS, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100 / 57.3, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test rand function
        callOperator = new CallOperator(FunctionSet.RAND, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test rand function
        callOperator = new CallOperator(FunctionSet.RAND, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test random function
        callOperator = new CallOperator(FunctionSet.RANDOM, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test negative function
        callOperator = new CallOperator(FunctionSet.NEGATIVE, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 0, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), -100, 0.001);
        // test positive function
        callOperator = new CallOperator(FunctionSet.POSITIVE, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test floor function
        callOperator = new CallOperator(FunctionSet.FLOOR, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test dfloor function
        callOperator = new CallOperator(FunctionSet.DFLOOR, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test ceil function
        callOperator = new CallOperator(FunctionSet.CEIL, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test ceiling function
        callOperator = new CallOperator(FunctionSet.CEILING, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test round function
        callOperator = new CallOperator(FunctionSet.ROUND, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test dround function
        callOperator = new CallOperator(FunctionSet.DROUND, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test truncate function
        callOperator = new CallOperator(FunctionSet.TRUNCATE, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test upper function
        callOperator = new CallOperator(FunctionSet.UPPER, Type.VARCHAR, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test xx_hash3_64 function
        callOperator = new CallOperator(FunctionSet.XX_HASH3_64, Type.BIGINT, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), Long.MAX_VALUE, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), Long.MIN_VALUE, 0.001);
        // test xx_hash3_128 function
        callOperator = new CallOperator(FunctionSet.XX_HASH3_128, Type.LARGEINT, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), LargeIntLiteral.LARGE_INT_MAX.doubleValue(), 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), LargeIntLiteral.LARGE_INT_MIN.doubleValue(), 0.001);
    }

    @Test
    public void testBinaryFunctionCall() {
        ColumnRefOperator left = new ColumnRefOperator(0, Type.INT, "left", true);
        ColumnRefOperator right = new ColumnRefOperator(1, Type.INT, "right", true);
        Statistics.Builder builder = Statistics.builder();
        ColumnStatistic leftStatistic = new ColumnStatistic(-100, 100, 0, 0, 100);
        ColumnStatistic rightStatistic = new ColumnStatistic(100, 200, 0, 0, 100);
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);

        // test add function
        CallOperator callOperator = new CallOperator(FunctionSet.ADD, Type.BIGINT, Lists.newArrayList(left, right));
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(300, columnStatistic.getMaxValue(), 0.001);
        // test date_add function
        callOperator = new CallOperator(FunctionSet.DATE_ADD, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(300, columnStatistic.getMaxValue(), 0.001);
        // test substract function
        callOperator = new CallOperator(FunctionSet.SUBTRACT, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-300, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test timediff function
        callOperator = new CallOperator(FunctionSet.TIMEDIFF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-300, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test date_sub function
        callOperator = new CallOperator(FunctionSet.DATE_SUB, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-300, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test years_diff function
        callOperator = new CallOperator(FunctionSet.YEARS_DIFF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test months_diff function
        callOperator = new CallOperator(FunctionSet.MONTHS_DIFF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test weeks_diff function
        callOperator = new CallOperator(FunctionSet.WEEKS_DIFF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test days_diff function
        callOperator = new CallOperator(FunctionSet.DAYS_DIFF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.01);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.01);
        // test datediff function
        callOperator = new CallOperator(FunctionSet.DATEDIFF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.01);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.01);
        // test hours_diff function
        callOperator = new CallOperator(FunctionSet.HOURS_DIFF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 1);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 1);
        // test minutes_diff function
        callOperator = new CallOperator(FunctionSet.MINUTES_DIFF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-5, columnStatistic.getMinValue(), 1);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 1);
        // test seconds_diff function
        callOperator = new CallOperator(FunctionSet.SECONDS_DIFF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-300, columnStatistic.getMinValue(), 1);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 1);
        // test mod function
        callOperator = new CallOperator(FunctionSet.MOD, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-200, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);
        // test fmod function
        callOperator = new CallOperator(FunctionSet.FMOD, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-200, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);
        // test pmod function
        callOperator = new CallOperator(FunctionSet.PMOD, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-200, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);
        // test ifnull function
        callOperator = new CallOperator(FunctionSet.IFNULL, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);
        // test nullif function
        callOperator = new CallOperator(FunctionSet.NULLIF, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(100, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.MULTIPLY, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-20000, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(20000, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.DIVIDE, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-1, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1, columnStatistic.getMaxValue(), 0.001);
        
        callOperator = new CallOperator(FunctionSet.LIKE, Type.BOOLEAN, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(2, columnStatistic.getDistinctValuesCount(), 0.001);

        callOperator = new CallOperator(FunctionSet.ILIKE, Type.BOOLEAN, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(2, columnStatistic.getDistinctValuesCount(), 0.001);
        // test multiply/divide column rang is negative
        builder = Statistics.builder();
        leftStatistic = new ColumnStatistic(-100, -10, 0, 0, 20);
        rightStatistic = new ColumnStatistic(-2, 0, 0, 0, 1);
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);
        callOperator = new CallOperator(FunctionSet.MULTIPLY, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.DIVIDE, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(50, columnStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testWeek() {
        ColumnRefOperator left = new ColumnRefOperator(0, Type.DATETIME, "left", true);
        ColumnRefOperator right = new ColumnRefOperator(1, Type.INT, "right", true);
        double min = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2021-09-01", DateUtils.DATE_FORMATTER_UNIX));
        double max = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2022-07-01", DateUtils.DATE_FORMATTER_UNIX));
        ColumnStatistic leftStatistic = new ColumnStatistic(min, max, 0, 0, 100);
        ColumnStatistic rightStatistic = new ColumnStatistic(1, 1, 0, 1, 1);
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);
        CallOperator week = new CallOperator(FunctionSet.WEEK, Type.INT, Lists.newArrayList(left, right));
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(week, builder.build());
        Assertions.assertEquals(45, columnStatistic.getDistinctValuesCount(), 0.1);

        min = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2022-01-20", DateUtils.DATE_FORMATTER_UNIX));
        max = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2022-08-01", DateUtils.DATE_FORMATTER_UNIX));
        leftStatistic = new ColumnStatistic(min, max, 0, 0, 100);
        builder = Statistics.builder();
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);
        columnStatistic = ExpressionStatisticCalculator.calculate(week, builder.build());
        Assertions.assertEquals(29, columnStatistic.getDistinctValuesCount(), 0.1);

        min = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2022-01-20", DateUtils.DATE_FORMATTER_UNIX));
        max = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2023-08-01", DateUtils.DATE_FORMATTER_UNIX));
        leftStatistic = new ColumnStatistic(min, max, 0, 0, 100);
        builder = Statistics.builder();
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);
        columnStatistic = ExpressionStatisticCalculator.calculate(week, builder.build());
        Assertions.assertEquals(54, columnStatistic.getDistinctValuesCount(), 0.1);

        min = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2022-01-20", DateUtils.DATE_FORMATTER_UNIX));
        max = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2023-08-01", DateUtils.DATE_FORMATTER_UNIX));
        leftStatistic = new ColumnStatistic(min, max, 0, 0, 2);
        builder = Statistics.builder();
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);
        columnStatistic = ExpressionStatisticCalculator.calculate(week, builder.build());
        Assertions.assertEquals(2, columnStatistic.getDistinctValuesCount(), 0.1);

    }

    @Test
    public void testCastOperator() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.INT, "id", true);
        CastOperator callOperator = new CastOperator(Type.VARCHAR, columnRefOperator);

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(columnRefOperator, new ColumnStatistic(-100, 100, 0, 0, 100));

        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(100, columnStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testCaseWhenOperator() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, Type.INT, "", true);
        BinaryPredicateOperator whenOperator1 =
                new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(1));
        ConstantOperator constantOperator1 = ConstantOperator.createChar("1");
        BinaryPredicateOperator whenOperator2 =
                new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(2));
        ConstantOperator constantOperator2 = ConstantOperator.createChar("2");

        CaseWhenOperator caseWhenOperator =
                new CaseWhenOperator(Type.VARCHAR, null, ConstantOperator.createChar("others", Type.VARCHAR),
                        ImmutableList.of(whenOperator1, constantOperator1, whenOperator2, constantOperator2));
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator
                .calculate(caseWhenOperator, Statistics.builder().setOutputRowCount(100).build());
        Assertions.assertEquals(columnStatistic.getDistinctValuesCount(), 3, 0.001);
    }

    @Test
    public void testFromDays() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, Type.INT, "", true);
        CallOperator callOperator = new CallOperator(FunctionSet.FROM_DAYS, Type.DOUBLE, Lists.newArrayList(columnRefOperator));

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(columnRefOperator, new ColumnStatistic(Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY, 0, 0, 100));

        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(columnStatistic.getMaxValue(), 2.534021856E11, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), -28800.0, 0.001);
    }

    @Test
    public void testIF() {
        ColumnRefOperator column = new ColumnRefOperator(1, Type.INT, "column", true);
        BinaryPredicateOperator condition = new BinaryPredicateOperator(BinaryType.EQ, column, ConstantOperator.createInt(1));
        ColumnRefOperator left = new ColumnRefOperator(0, Type.INT, "left", true);
        ColumnRefOperator right = new ColumnRefOperator(1, Type.INT, "right", true);

        ColumnStatistic columnStatistic = new ColumnStatistic(-300, 300, 0, 0, 300);
        ColumnStatistic leftStatistic = new ColumnStatistic(-100, 100, 0, 0, 100);
        ColumnStatistic rightStatistic = new ColumnStatistic(100, 200, 0, 0, 100);

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(300);
        builder.addColumnStatistic(column, columnStatistic);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);

        CallOperator callOperator = new CallOperator(FunctionSet.IF, Type.INT, Lists.newArrayList(condition, left, right));
        ColumnStatistic ifStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(ifStatistic.getDistinctValuesCount(), 200, 0.001);
        Assertions.assertEquals(ifStatistic.getMaxValue(), 200, 0.001);
        Assertions.assertEquals(ifStatistic.getMinValue(), -100, 0.001);
    }

    @Test
    public void testMcvPropagationForAddConst() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        // one bucket [1,3) with total 100 rows plus two MCV values
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L)), Map.of("1", 20480L, "2", 10240L));
        ColumnStatistic kStats = ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(1000)
                .setNullsFraction(0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(1000)
                .setHistogram(hist)
                .build();
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100000)
                .addColumnStatistic(k, kStats)
                .build();

        // expr: cast(k as BIGINT) + 10
        CastOperator cast = new CastOperator(Type.BIGINT, k);
        ConstantOperator c10 = ConstantOperator.createBigint(10);
        CallOperator add = new CallOperator(FunctionSet.ADD, Type.BIGINT, Lists.newArrayList(cast, c10));

        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(add, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertNotNull(exprStats.getHistogram().getMCV());
        Assertions.assertEquals(20480L, exprStats.getHistogram().getMCV().get("11"));
        Assertions.assertEquals(10240L, exprStats.getHistogram().getMCV().get("12"));
        Assertions.assertEquals(1, exprStats.getHistogram().getBuckets().size());
        Assertions.assertEquals(11.0, exprStats.getHistogram().getBuckets().get(0).getLower(), 0.0001);
        Assertions.assertEquals(13.0, exprStats.getHistogram().getBuckets().get(0).getUpper(), 0.0001);
    }

    @Test
    public void testMcvPropagationForAddConst_commutativeShiftsBuckets() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        // one bucket [1,3) with total 100 rows plus two MCV values
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L)), Map.of("1", 20480L, "2", 10240L));
        ColumnStatistic kStats = ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(1000)
                .setNullsFraction(0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(1000)
                .setHistogram(hist)
                .build();
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100000)
                .addColumnStatistic(k, kStats)
                .build();

        // expr: 10 + cast(k as BIGINT)
        CastOperator cast = new CastOperator(Type.BIGINT, k);
        ConstantOperator c10 = ConstantOperator.createBigint(10);
        CallOperator add = new CallOperator(FunctionSet.ADD, Type.BIGINT, Lists.newArrayList(c10, cast));

        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(add, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertNotNull(exprStats.getHistogram().getMCV());
        Assertions.assertEquals(20480L, exprStats.getHistogram().getMCV().get("11"));
        Assertions.assertEquals(10240L, exprStats.getHistogram().getMCV().get("12"));
        Assertions.assertEquals(1, exprStats.getHistogram().getBuckets().size());
        Assertions.assertEquals(11.0, exprStats.getHistogram().getBuckets().get(0).getLower(), 0.0001);
        Assertions.assertEquals(13.0, exprStats.getHistogram().getBuckets().get(0).getUpper(), 0.0001);
    }

    @Test
    public void testMcvPropagationForSubtractConst() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(10, 12, 100L, 0L)), Map.of("10", 10L, "11", 5L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(100).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(100)
                        .setHistogram(hist).build())
                .build();

        CastOperator cast = new CastOperator(Type.BIGINT, k);
        ConstantOperator c10 = ConstantOperator.createBigint(10);
        CallOperator sub = new CallOperator(FunctionSet.SUBTRACT, Type.BIGINT, Lists.newArrayList(cast, c10));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(sub, stats);

        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(10L, exprStats.getHistogram().getMCV().get("0"));
        Assertions.assertEquals(5L, exprStats.getHistogram().getMCV().get("1"));
        Assertions.assertEquals(0.0, exprStats.getHistogram().getBuckets().get(0).getLower(), 0.0001);
        Assertions.assertEquals(2.0, exprStats.getHistogram().getBuckets().get(0).getUpper(), 0.0001);
    }

    @Test
    public void testMcvPropagationForSubtractConstMinusX_doesNotShiftBuckets() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L)), Map.of("1", 7L, "2", 3L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(100).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(100)
                        .setHistogram(hist).build())
                .build();

        CastOperator cast = new CastOperator(Type.BIGINT, k);
        ConstantOperator c10 = ConstantOperator.createBigint(10);
        // expr: 10 - cast(k)
        CallOperator sub = new CallOperator(FunctionSet.SUBTRACT, Type.BIGINT, Lists.newArrayList(c10, cast));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(sub, stats);

        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(7L, exprStats.getHistogram().getMCV().get("9"));
        Assertions.assertEquals(3L, exprStats.getHistogram().getMCV().get("8"));
        // Buckets should be transformed for const - x: [l,u) -> [c-u, c-l), reverse order for monotonic decreasing mapping.
        Assertions.assertEquals(7.0, exprStats.getHistogram().getBuckets().get(0).getLower(), 0.0001);
        Assertions.assertEquals(9.0, exprStats.getHistogram().getBuckets().get(0).getUpper(), 0.0001);
    }

    @Test
    public void testMcvPropagationForUnaryNegative() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        Histogram hist = new Histogram(List.of(), Map.of("1", 100L, "2", 50L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(200)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator neg = new CallOperator(FunctionSet.NEGATIVE, Type.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(neg, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(100L, exprStats.getHistogram().getMCV().get("-1"));
        Assertions.assertEquals(50L, exprStats.getHistogram().getMCV().get("-2"));
    }

    @Test
    public void testMcvPropagationForUnaryNegative_transformsBuckets() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L)), Map.of("1", 100L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator neg = new CallOperator(FunctionSet.NEGATIVE, Type.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(neg, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(100L, exprStats.getHistogram().getMCV().get("-1"));
        Assertions.assertEquals(1, exprStats.getHistogram().getBuckets().size());
        Assertions.assertEquals(-3.0, exprStats.getHistogram().getBuckets().get(0).getLower(), 0.0001);
        Assertions.assertEquals(-1.0, exprStats.getHistogram().getBuckets().get(0).getUpper(), 0.0001);
    }

    @Test
    public void testMcvPropagationForUnaryNegative_transformsBuckets_multiBucketCumulativeCounts() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        // two buckets with cumulative counts: [1,3) count=100, [3,5) count=250
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L), new Bucket(3, 5, 250L, 0L)),
                Map.of("1", 10L, "4", 20L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(250)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator neg = new CallOperator(FunctionSet.NEGATIVE, Type.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(neg, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(2, exprStats.getHistogram().getBuckets().size());
        // After negation and reverse:
        // [3,5) -> [-5,-3) should be first and keep per-bucket rows 150 => cumulative 150
        // [1,3) -> [-3,-1) should be second and add 100 => cumulative 250
        Bucket b0 = exprStats.getHistogram().getBuckets().get(0);
        Bucket b1 = exprStats.getHistogram().getBuckets().get(1);
        Assertions.assertEquals(-5.0, b0.getLower(), 0.0001);
        Assertions.assertEquals(-3.0, b0.getUpper(), 0.0001);
        Assertions.assertEquals(150L, b0.getCount().longValue());
        Assertions.assertEquals(-3.0, b1.getLower(), 0.0001);
        Assertions.assertEquals(-1.0, b1.getUpper(), 0.0001);
        Assertions.assertEquals(250L, b1.getCount().longValue());
    }

    @Test
    public void testMcvPropagationForUnaryAbs_collisionMerge() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        Histogram hist = new Histogram(List.of(), Map.of("-1", 100L, "1", 200L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(300)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(-10).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(20)
                        .setHistogram(hist).build())
                .build();

        CallOperator abs = new CallOperator(FunctionSet.ABS, Type.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(abs, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(300L, exprStats.getHistogram().getMCV().get("1"));
    }

    @Test
    public void testMcvPropagationForUnaryAbs_transformsBucketsWhenAllNonPositive() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(-3, -1, 100L, 0L)), Map.of("-2", 10L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(-10).setMaxValue(-1).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator abs = new CallOperator(FunctionSet.ABS, Type.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(abs, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(10L, exprStats.getHistogram().getMCV().get("2"));
        Assertions.assertEquals(1, exprStats.getHistogram().getBuckets().size());
        Assertions.assertEquals(1.0, exprStats.getHistogram().getBuckets().get(0).getLower(), 0.0001);
        Assertions.assertEquals(3.0, exprStats.getHistogram().getBuckets().get(0).getUpper(), 0.0001);
    }

    @Test
    public void testMcvPropagationForUnaryAbs_identityWhenAllNonNegativeBuckets() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L), new Bucket(3, 5, 250L, 0L)),
                Map.of("2", 10L, "4", 20L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(250)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator abs = new CallOperator(FunctionSet.ABS, Type.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(abs, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(2, exprStats.getHistogram().getBuckets().size());
        Bucket b0 = exprStats.getHistogram().getBuckets().get(0);
        Bucket b1 = exprStats.getHistogram().getBuckets().get(1);
        // identity: buckets unchanged
        Assertions.assertEquals(1.0, b0.getLower(), 0.0001);
        Assertions.assertEquals(3.0, b0.getUpper(), 0.0001);
        Assertions.assertEquals(100L, b0.getCount().longValue());
        Assertions.assertEquals(3.0, b1.getLower(), 0.0001);
        Assertions.assertEquals(5.0, b1.getUpper(), 0.0001);
        Assertions.assertEquals(250L, b1.getCount().longValue());
    }

    @Test
    public void testMcvPropagationForUnaryPositive_identity() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        Histogram hist = new Histogram(List.of(), Map.of("1", 10L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(10)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(1).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(1)
                        .setHistogram(hist).build())
                .build();

        CallOperator pos = new CallOperator(FunctionSet.POSITIVE, Type.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(pos, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(10L, exprStats.getHistogram().getMCV().get("1"));
    }

    @Test
    public void testMcvPropagationFailClosedForNonType() {
        ColumnRefOperator k = new ColumnRefOperator(1, Type.INT, "k", true);
        Histogram hist = new Histogram(List.of(), Map.of("1", 10L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(10)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(1).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(1)
                        .setHistogram(hist).build())
                .build();

        // expr type is DOUBLE => should not propagate histogram/MCV
        CallOperator add = new CallOperator(FunctionSet.ADD, Type.DOUBLE,
                Lists.newArrayList(k, ConstantOperator.createInt(1)));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(add, stats);
        Assertions.assertNull(exprStats.getHistogram());
    }

    @Test
    public void testMcvPropagationFailClosedForNoConstSide() {
        ColumnRefOperator k1 = new ColumnRefOperator(1, Type.INT, "k1", true);
        ColumnRefOperator k2 = new ColumnRefOperator(2, Type.INT, "k2", true);
        Histogram hist = new Histogram(List.of(), Map.of("1", 10L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(10)
                .addColumnStatistic(k1, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(1).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(1)
                        .setHistogram(hist).build())
                .addColumnStatistic(k2, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(1).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(1)
                        .build())
                .build();

        CallOperator add = new CallOperator(FunctionSet.ADD, Type.BIGINT, Lists.newArrayList(k1, k2));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(add, stats);
        Assertions.assertNull(exprStats.getHistogram());
    }
}
