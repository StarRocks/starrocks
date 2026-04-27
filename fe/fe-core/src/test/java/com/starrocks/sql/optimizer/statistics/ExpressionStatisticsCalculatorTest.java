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
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
<<<<<<< HEAD
=======
import java.util.Map;
import java.util.function.Function;
>>>>>>> f54576efd8 ([Enhancement] Implement MCV propagation for If and constants (#71000))

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
        Assertions.assertEquals(columnStatistic.getMaxValue(), 53, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 1, 0.001);
        // test week_iso function
        callOperator = new CallOperator(FunctionSet.WEEK_ISO, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 53, 0.001);
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
        // test dayofweek_iso function
        callOperator = new CallOperator(FunctionSet.DAYOFWEEK_ISO, Type.DOUBLE, Lists.newArrayList(columnRefOperator));
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
        // test lower function
        callOperator = new CallOperator(FunctionSet.LOWER, Type.VARCHAR, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test lcase function
        callOperator = new CallOperator(FunctionSet.LCASE, Type.VARCHAR, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test trim function
        callOperator = new CallOperator(FunctionSet.TRIM, Type.VARCHAR, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test ltrim function
        callOperator = new CallOperator(FunctionSet.LTRIM, Type.VARCHAR, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test rtrim function
        callOperator = new CallOperator(FunctionSet.RTRIM, Type.VARCHAR, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0.001);
        // test reverse function
        callOperator = new CallOperator(FunctionSet.REVERSE, Type.VARCHAR, Lists.newArrayList(columnRefOperator));
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
        Assertions.assertEquals(53, columnStatistic.getDistinctValuesCount(), 0.1);

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
    public void testCaseWhenOperatorNullFractionWithoutElse() {
        // GIVEN
        // CASE WHEN col = 1 THEN '1' WHEN col = 2 THEN '2' END  (no ELSE)
        final var columnRefOperator = new ColumnRefOperator(1, Type.INT, "", true);
        final var whenOperator1 = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(1));
        final var constantOperator1 = ConstantOperator.createChar("1");
        final var whenOperator2 = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(2));
        final var constantOperator2 = ConstantOperator.createChar("2");

        // No ELSE clause: elseClause = null
        CaseWhenOperator caseWhenOperator = new CaseWhenOperator(Type.VARCHAR, null, null,
                ImmutableList.of(whenOperator1, constantOperator1, whenOperator2, constantOperator2));

        // WHEN
        final var columnStatistic = ExpressionStatisticCalculator.calculate(caseWhenOperator,
                Statistics.builder().setOutputRowCount(100).build());

        // THEN
        Assertions.assertEquals(2, columnStatistic.getDistinctValuesCount(), 0.001);
        // The implicit ELSE NULL branch has nullsFraction=1.0, the two THEN constant branches have nullsFraction=0.0.
        Assertions.assertEquals(1.0 / 3.0, columnStatistic.getNullsFraction(), 0.001);
    }

    @Test
    public void testCaseWhenOperatorNullFractionWithElse() {
        // GIVEN
        // CASE WHEN col = 1 THEN '1' WHEN col = 2 THEN '2' ELSE 'others' END
        final var columnRefOperator = new ColumnRefOperator(1, Type.INT, "", true);
        final var whenOperator1 = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(1));
        final var constantOperator1 = ConstantOperator.createChar("1");
        final var whenOperator2 = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(2));
        final var constantOperator2 = ConstantOperator.createChar("2");

        final var caseWhenOperator =
                new CaseWhenOperator(Type.VARCHAR, null, ConstantOperator.createChar("others", Type.VARCHAR),
                        ImmutableList.of(whenOperator1, constantOperator1, whenOperator2, constantOperator2));

        // WHEN
        final var columnStatistic = ExpressionStatisticCalculator.calculate(caseWhenOperator,
                Statistics.builder().setOutputRowCount(100).build());

        // THEN
        // All 3 branches (2 THEN + 1 ELSE) are non-null constants => average nullFraction = 0.0
        Assertions.assertEquals(0.0, columnStatistic.getNullsFraction(), 0.001);
        Assertions.assertEquals(3, columnStatistic.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testCaseWhenOperatorExplicitElseNull() {
        // GIVEN
        // CASE WHEN col = 1 THEN 'x' ELSE NULL END
        final var columnRefOperator = new ColumnRefOperator(1, Type.INT, "", true);
        final var whenOperator = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(1));
        final var thenOperator = ConstantOperator.createChar("x");

        final var caseWhenOperator = new CaseWhenOperator(Type.VARCHAR, null,
                ConstantOperator.createNull(Type.VARCHAR), ImmutableList.of(whenOperator, thenOperator));

        // WHEN
        final var columnStatistic = ExpressionStatisticCalculator.calculate(caseWhenOperator,
                Statistics.builder().setOutputRowCount(100).build());

        // THEN
        Assertions.assertEquals(1, columnStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0.5, columnStatistic.getNullsFraction(), 0.001);
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
    public void testIsNullPredicateStatisticsWithNoNulls() {
        // GIVEN
        final var col = new ColumnRefOperator(0, Type.BIGINT, "col", true);
        final var colStat = ColumnStatistic.builder() //
                .setMinValue(0) //
                .setMaxValue(999_999) //
                .setDistinctValuesCount(1_000_237) //
                .setNullsFraction(0) //
                .setAverageRowSize(8) //
                .build();
        final var statistics = Statistics.builder() //
                .setOutputRowCount(1_000_000) //
                .addColumnStatistic(col, colStat) //
                .build();

        final var isNull = new IsNullPredicateOperator(false, col);

        // WHEN
        final var isNullStat = ExpressionStatisticCalculator.calculate(isNull, statistics);

        // THEN
        Assertions.assertFalse(isNullStat.isUnknown());
        Assertions.assertEquals(0, isNullStat.getMinValue(), 0.001);
        Assertions.assertEquals(1, isNullStat.getMaxValue(), 0.001);
        Assertions.assertEquals(0, isNullStat.getNullsFraction(), 0.001);
        // nullsFraction=0 → only the false branch has rows → NDV=1
        Assertions.assertEquals(1, isNullStat.getDistinctValuesCount(), 0.001);
        Assertions.assertNotNull(isNullStat.getHistogram());
        Assertions.assertEquals(1_000_000L, isNullStat.getHistogram().getMCV().get("0"));
        Assertions.assertNull(isNullStat.getHistogram().getMCV().get("1"));
    }

    @Test
    public void testIsNullPredicateStatisticsWithNulls() {
        // GIVEN
        final var col = new ColumnRefOperator(1, Type.BIGINT, "col", true);
        final var colStat = ColumnStatistic.builder() //
                .setMinValue(0) //
                .setMaxValue(999_999) //
                .setDistinctValuesCount(700_000) //
                .setNullsFraction(0.3) //
                .setAverageRowSize(8) //
                .build();
        final var stats = Statistics.builder() //
                .setOutputRowCount(1_000_000) //
                .addColumnStatistic(col, colStat) //
                .build();
        final var isNull = new IsNullPredicateOperator(false, col);

        // WHEN
        final var isNullStat = ExpressionStatisticCalculator.calculate(isNull, stats);

        // THEN
        Assertions.assertFalse(isNullStat.isUnknown());
        Assertions.assertEquals(2, isNullStat.getDistinctValuesCount(), 0.001);
        Assertions.assertNotNull(isNullStat.getHistogram());
        Assertions.assertEquals(300_000L, isNullStat.getHistogram().getMCV().get("1"));
        Assertions.assertEquals(700_000L, isNullStat.getHistogram().getMCV().get("0"));
    }


    @Test
    public void testArrayMapWithDependentLambda() {
        // GIVEN
        final var arrayCol = new ColumnRefOperator(1, Type.ARRAY_INT, "arr", true);
        final var lambdaArg = new ColumnRefOperator(10, Type.INT, "x", true, true);

        final var condition = new BinaryPredicateOperator(BinaryType.EQ, lambdaArg, ConstantOperator.createNull(Type.INT));
        final var nullConst = ConstantOperator.createNull(Type.INT);

        final var ifOp = new CallOperator(FunctionSet.IF, Type.INT, Lists.newArrayList(condition, lambdaArg, nullConst));
        var lambda = new LambdaFunctionOperator(List.of(lambdaArg), ifOp, Type.INT);

        Statistics stats = Statistics.builder()
                .setOutputRowCount(10_000) //
                .addColumnStatistic(arrayCol, ColumnStatistic.builder() //
                        .setMinValue(Double.NEGATIVE_INFINITY) //
                        .setMaxValue(Double.POSITIVE_INFINITY) //
                        .setNullsFraction(0.1) //
                        .setAverageRowSize(16) //
                        .setDistinctValuesCount(50) //
                        .setCollectionSize(5) //
                        .build())
                .build();

        final var arrayMap = new CallOperator(FunctionSet.ARRAY_MAP, Type.ARRAY_INT,
                Lists.newArrayList(lambda, arrayCol));

        // WHEN
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(arrayMap, stats);

        // THEN
        Assertions.assertNotNull(exprStats);
        Assertions.assertFalse(exprStats.isUnknown());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, exprStats.getMinValue(), 0.001);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, exprStats.getMaxValue(), 0.001);
        Assertions.assertEquals(0.1, exprStats.getNullsFraction(), 0.001);
        Assertions.assertEquals(50, exprStats.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(16.0, exprStats.getAverageRowSize(), 0.001);
    }

    @Test
    public void testArrayMapWithIndependentLambda() {
        // GIVEN
        final var arrayCol = new ColumnRefOperator(1, Type.ARRAY_INT, "arr", true);
        final var otherCol = new ColumnRefOperator(2, Type.INT, "other", true);
        // Lambda argument 'x' is a separate ColumnRefOperator with isLambdaArgument=true.
        final var lambdaArg = new ColumnRefOperator(10, Type.INT, "x", true, true);

        var addOp = new CallOperator(FunctionSet.ADD, Type.INT,
                Lists.newArrayList(otherCol, new ConstantOperator(1, Type.INT)));
        var lambda = new LambdaFunctionOperator(List.of(lambdaArg), addOp, Type.INT);

        final var stats = Statistics.builder()
                .setOutputRowCount(10_000) //
                .addColumnStatistic(arrayCol, ColumnStatistic.builder() //
                        .setMinValue(Double.NEGATIVE_INFINITY) //
                        .setMaxValue(Double.POSITIVE_INFINITY) //
                        .setNullsFraction(0.1) //
                        .setAverageRowSize(16) //
                        .setDistinctValuesCount(50) //
                        .setCollectionSize(5) //
                        .build())
                .addColumnStatistic(otherCol, ColumnStatistic.builder() //
                        .setMinValue(2000) //
                        .setMaxValue(3000) //
                        .setNullsFraction(0.5) //
                        .setAverageRowSize(16) //
                        .setDistinctValuesCount(2) //
                        .build())
                .build();

        final var arrayMap = new CallOperator(FunctionSet.ARRAY_MAP, Type.ARRAY_INT,
                Lists.newArrayList(lambda, arrayCol));
        // WHEN
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(arrayMap, stats);

        // THEN
        Assertions.assertNotNull(exprStats);
        Assertions.assertFalse(exprStats.isUnknown());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, exprStats.getMinValue(), 0.001);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, exprStats.getMaxValue(), 0.001);
        Assertions.assertEquals(0.1, exprStats.getNullsFraction(), 0.001);
        // Even though the lambda body has NDV=2, array_map produces arrays, and different input
        // array structures (lengths/NULLs) yield distinct output arrays, so NDV ≥ input array NDV.
        Assertions.assertEquals(50, exprStats.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(16, exprStats.getAverageRowSize(), 0.001);
    }

    @Test
    public void testArrayMapWithCaseWhenAndLambdaArgNotInStats() {
        // GIVEN
        final var idCol = new ColumnRefOperator(3, Type.INT, "ID", true);
        final var arrayTestCol = new ColumnRefOperator(4, Type.ARRAY_INT, "ARRAY_TEST", true);
        final var lambdaArgX = new ColumnRefOperator(5, Type.INT, "x", true, true);

        final var isNotNullPredicate = new IsNullPredicateOperator(true, idCol);
        final var caseWhen = new CaseWhenOperator(Type.INT, null, null,
                Lists.newArrayList(isNotNullPredicate, lambdaArgX));

        var lambda = new LambdaFunctionOperator(List.of(lambdaArgX), caseWhen, Type.INT);

        Statistics stats = Statistics.builder()
                .setOutputRowCount(10_000)
                .addColumnStatistic(idCol, ColumnStatistic.builder()
                        .setMinValue(1)
                        .setMaxValue(1000)
                        .setNullsFraction(0.05)
                        .setAverageRowSize(4)
                        .setDistinctValuesCount(500)
                        .build())
                .addColumnStatistic(arrayTestCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0.1)
                        .setAverageRowSize(16)
                        .setDistinctValuesCount(50)
                        .setCollectionSize(5)
                        .build())
                .build();

        final var arrayMap = new CallOperator(FunctionSet.ARRAY_MAP, Type.ARRAY_INT,
                Lists.newArrayList(lambda, arrayTestCol));

        // WHEN
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(arrayMap, stats);

        // THEN
        Assertions.assertNotNull(exprStats);
        Assertions.assertFalse(exprStats.isUnknown());
        Assertions.assertEquals(0.1, exprStats.getNullsFraction(), 0.001);
        Assertions.assertEquals(16, exprStats.getAverageRowSize(), 0.001);
        Assertions.assertEquals(5, exprStats.getCollectionSize(), 0.001);
    }

    @Test
    public void testIsNotNullPredicateStatisticsWithNulls() {
        // GIVEN
        final var col = new ColumnRefOperator(1, Type.BIGINT, "col", true);
        final var colStat = ColumnStatistic.builder() //
                .setMinValue(0) //
                .setMaxValue(999_999) //
                .setDistinctValuesCount(700_000) //
                .setNullsFraction(0.3) //
                .setAverageRowSize(8) //
                .build();
        final var stats = Statistics.builder() //
                .setOutputRowCount(1_000_000) //
                .addColumnStatistic(col, colStat) //
                .build();
        final var isNotNull = new IsNullPredicateOperator(true, col);

        // WHEN
        final var isNotNullStat = ExpressionStatisticCalculator.calculate(isNotNull, stats);

        // THEN
        Assertions.assertFalse(isNotNullStat.isUnknown());
        Assertions.assertEquals(0, isNotNullStat.getMinValue(), 0.001);
        Assertions.assertEquals(1, isNotNullStat.getMaxValue(), 0.001);
        Assertions.assertEquals(2, isNotNullStat.getDistinctValuesCount(), 0.001);
        Assertions.assertNotNull(isNotNullStat.getHistogram());
        Assertions.assertEquals(700_000L, isNotNullStat.getHistogram().getMCV().get("1"));
        Assertions.assertEquals(300_000L, isNotNullStat.getHistogram().getMCV().get("0"));
    }

    @Test
    public void testIfWithIsNullPredicateHasCorrectNdv() {
        // GIVEN
        // CASE WHEN `NONNULL` IS NULL THEN 1 ELSE 0 END
        final var col = new ColumnRefOperator(0, IntegerType.BIGINT, "NONNULL", true);

        final var colStat = ColumnStatistic.builder() //
                .setDistinctValuesCount(1_000_237) //
                .setNullsFraction(0) //
                .setAverageRowSize(8) //
                .build();

        final var statistics = Statistics.builder() //
                .setOutputRowCount(1_000_000) //
                .addColumnStatistic(col, colStat) //
                .build();

        final var isNull = new IsNullPredicateOperator(false, col);
        final var then = ConstantOperator.createInt(1);
        final var elseClause = ConstantOperator.createInt(0);

        final var ifOp = new CallOperator(FunctionSet.IF, IntegerType.TINYINT, Lists.newArrayList(isNull, then, elseClause));

        // WHEN
        final var ifStat = ExpressionStatisticCalculator.calculate(ifOp, statistics);

        // THEN
        Assertions.assertFalse(ifStat.isUnknown());
        Assertions.assertEquals(1, ifStat.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, ifStat.getMinValue(), 0.001);
        Assertions.assertEquals(0, ifStat.getMaxValue(), 0.001);
    }

    @Test
    public void testIfIsNullMcvPropagation() {
        // GIVEN
        Function<Double, Statistics> makeStats = nullFrac -> {
            final var col = new ColumnRefOperator(0, IntegerType.BIGINT, "COL", true);
            final var colStat = ColumnStatistic.builder() //
                    .setDistinctValuesCount(1_000_000) //
                    .setNullsFraction(nullFrac) //
                    .setAverageRowSize(8) //
                    .build();
            return Statistics.builder() //
                    .setOutputRowCount(1_000_000) //
                    .addColumnStatistic(col, colStat) //
                    .build();
        };

        Function<Statistics, ColumnStatistic> calcIfStat = stats -> {
            final var col = new ColumnRefOperator(0, IntegerType.BIGINT, "COL", true);
            final var isNull = new IsNullPredicateOperator(false, col);
            final var then = ConstantOperator.createTinyInt((byte) 1);
            final var elseConst = ConstantOperator.createTinyInt((byte) 0);
            final var ifOp = new CallOperator(FunctionSet.IF, IntegerType.TINYINT, Lists.newArrayList(isNull, then, elseConst));
            return ExpressionStatisticCalculator.calculate(ifOp, stats);
        };

        // WHEN
        // nullsFraction = 0.0
        var stat = calcIfStat.apply(makeStats.apply(0.0));

        // THEN
        Assertions.assertNotNull(stat.getHistogram());
        var mcv = stat.getHistogram().getMCV();
        Assertions.assertFalse(mcv.containsKey("1"));
        Assertions.assertEquals(1_000_000L, mcv.get("0"));

        // WHEN
        // nullsFraction = 0.3
        stat = calcIfStat.apply(makeStats.apply(0.3));
        // THEN
        Assertions.assertNotNull(stat.getHistogram());
        mcv = stat.getHistogram().getMCV();
        Assertions.assertEquals(300_000L, mcv.get("1"));
        Assertions.assertEquals(700_000L, mcv.get("0"));

        // WHEN
        // nullsFraction = 1.0
        stat = calcIfStat.apply(makeStats.apply(1.0));
        // THEN
        Assertions.assertNotNull(stat.getHistogram());
        mcv = stat.getHistogram().getMCV();
        Assertions.assertEquals(1_000_000L, mcv.get("1"));
        Assertions.assertFalse(mcv.containsKey("0"));
    }

    @Test
    public void testIfNullFractionWeightedByConditionDistribution() {
        // GIVEN
        // IF(col IS NULL, nullable_expr, non_nullable_expr)
        // col has 0% nulls, so IS NULL is always false => only ELSE branch is taken.
        final var col = new ColumnRefOperator(0, IntegerType.BIGINT, "COL", true);

        final var colStat = ColumnStatistic.builder() //
                .setDistinctValuesCount(500) //
                .setNullsFraction(0.0) //
                .setAverageRowSize(8) //
                .build();

        final var statistics = Statistics.builder() //
                .setOutputRowCount(10_000) //
                .addColumnStatistic(col, colStat) //
                .build();

        final var isNull = new IsNullPredicateOperator(false, col);
        final var thenClause = ConstantOperator.createNull(IntegerType.INT);
        final var elseClause = ConstantOperator.createInt(42);

        final var ifOp = new CallOperator(FunctionSet.IF, IntegerType.INT,
                Lists.newArrayList(isNull, thenClause, elseClause));

        // WHEN
        final var ifStat = ExpressionStatisticCalculator.calculate(ifOp, statistics);

        // THEN
        // Condition is always false (0% nulls), so only ELSE branch is reachable.
        // NDV, min/max, and nullsFraction should collapse to the ELSE branch only.
        Assertions.assertFalse(ifStat.isUnknown());
        Assertions.assertEquals(0.0, ifStat.getNullsFraction(), 0.001);
        Assertions.assertEquals(1, ifStat.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(42, ifStat.getMinValue(), 0.001);
        Assertions.assertEquals(42, ifStat.getMaxValue(), 0.001);
        Assertions.assertNotNull(ifStat.getHistogram());
        Assertions.assertNotNull(ifStat.getHistogram().getMCV());
        Assertions.assertTrue(ifStat.getHistogram().getMCV().containsKey("42"));
    }
}

