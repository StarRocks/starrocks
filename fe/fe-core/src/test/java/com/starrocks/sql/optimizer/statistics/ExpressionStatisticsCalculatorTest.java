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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class ExpressionStatisticsCalculatorTest {
    @Test
    public void testVariableReference() {
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(100);
        double min = 0.0;
        double max = 100.0;
        double distinctValue = 100;
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, DateType.DATE, "id_date", true);
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
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, IntegerType.INT, "id", true);

        Statistics.Builder builder = Statistics.builder();
        Statistics statistics = builder.addColumnStatistic(columnRefOperator,
                        ColumnStatistic.builder().setMinValue(0).setMaxValue(100).
                                setDistinctValuesCount(100).setNullsFraction(0).setAverageRowSize(10).build())
                .setOutputRowCount(100).build();

        // test rand/random function
        CallOperator callOperator = new CallOperator(FunctionSet.RAND, FloatType.DOUBLE, Lists.newArrayList());
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0);
        callOperator = new CallOperator(FunctionSet.RANDOM, FloatType.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), 1, 0);
        Assertions.assertEquals(columnStatistic.getMinValue(), 0, 0);
        // test e function
        callOperator = new CallOperator(FunctionSet.E, FloatType.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), Math.E, 0);
        Assertions.assertEquals(columnStatistic.getMinValue(), Math.E, 0);
        // test pi function
        callOperator = new CallOperator(FunctionSet.PI, FloatType.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(columnStatistic.getMaxValue(), Math.PI, 0);
        Assertions.assertEquals(columnStatistic.getMinValue(), Math.PI, 0);
        // test curdate function
        callOperator = new CallOperator(FunctionSet.CURDATE, FloatType.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        long epochDay = LocalDate.now().toEpochDay();
        Assertions.assertTrue(columnStatistic.getMaxValue() <
                LocalDate.ofEpochDay(epochDay + 1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond());
        Assertions.assertTrue(columnStatistic.getMinValue() >
                LocalDate.ofEpochDay(epochDay - 1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond());
        // test curtime/current_time function
        callOperator = new CallOperator(FunctionSet.CURTIME, FloatType.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        LocalDateTime now = LocalDateTime.now();
        long time = now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond();
        Assertions.assertTrue(columnStatistic.getMaxValue() < time + 1);
        Assertions.assertTrue(columnStatistic.getMinValue() > time - 1);
        callOperator = new CallOperator(FunctionSet.CURRENT_TIME, FloatType.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        now = LocalDateTime.now();
        time = now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond();
        Assertions.assertTrue(columnStatistic.getMaxValue() < time + 1);
        Assertions.assertTrue(columnStatistic.getMinValue() > time - 1);
        // test current_timestamp/unix_timestamp function
        callOperator = new CallOperator(FunctionSet.CURRENT_TIMESTAMP, FloatType.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        long timestamp = System.currentTimeMillis() / 1000;
        Assertions.assertTrue(columnStatistic.getMaxValue() < timestamp + 1);
        Assertions.assertTrue(columnStatistic.getMinValue() > timestamp - 1);
        callOperator = new CallOperator(FunctionSet.UNIX_TIMESTAMP, FloatType.DOUBLE, Lists.newArrayList());
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        timestamp = System.currentTimeMillis() / 1000;
        Assertions.assertTrue(columnStatistic.getMaxValue() < timestamp + 1);
        Assertions.assertTrue(columnStatistic.getMinValue() > timestamp - 1);
    }

    private static final double UNARY_INPUT_MIN = 0.0;
    private static final double UNARY_INPUT_MAX = 100.0;
    private static final double UNARY_INPUT_DISTINCT_VALUES = 100.0;
    private static final double UNARY_ROW_COUNT = 100.0;

    private static ColumnRefOperator unaryInputColumn() {
        return new ColumnRefOperator(0, IntegerType.INT, "id", true);
    }

    private static Statistics unaryInputStatistics(ColumnRefOperator inputColumn) {
        return Statistics.builder()
                .setOutputRowCount(UNARY_ROW_COUNT)
                .addColumnStatistic(inputColumn, ColumnStatistic.builder()
                        .setMinValue(UNARY_INPUT_MIN)
                        .setMaxValue(UNARY_INPUT_MAX)
                        .setDistinctValuesCount(UNARY_INPUT_DISTINCT_VALUES)
                        .setNullsFraction(0)
                        .setAverageRowSize(10)
                        .build())
                .build();
    }

    private static Stream<Arguments> rangePreservingUnaryFunctions() {
        return Stream.of(
                Arguments.of(FunctionSet.MAX, IntegerType.INT),
                Arguments.of(FunctionSet.MIN, IntegerType.INT),
                Arguments.of(FunctionSet.GREATEST, FloatType.DOUBLE),
                Arguments.of(FunctionSet.LEAST, FloatType.DOUBLE),
                Arguments.of(FunctionSet.TIMESTAMP, FloatType.DOUBLE),
                Arguments.of(FunctionSet.ABS, FloatType.DOUBLE),
                Arguments.of(FunctionSet.POSITIVE, FloatType.DOUBLE),
                Arguments.of(FunctionSet.FLOOR, FloatType.DOUBLE),
                Arguments.of(FunctionSet.DFLOOR, FloatType.DOUBLE),
                Arguments.of(FunctionSet.CEIL, FloatType.DOUBLE),
                Arguments.of(FunctionSet.CEILING, FloatType.DOUBLE),
                Arguments.of(FunctionSet.ROUND, FloatType.DOUBLE),
                Arguments.of(FunctionSet.DROUND, FloatType.DOUBLE),
                Arguments.of(FunctionSet.TRUNCATE, FloatType.DOUBLE),
                Arguments.of(FunctionSet.UPPER, VarcharType.VARCHAR),
                Arguments.of(FunctionSet.LOWER, VarcharType.VARCHAR),
                Arguments.of(FunctionSet.LCASE, VarcharType.VARCHAR),
                Arguments.of(FunctionSet.TRIM, VarcharType.VARCHAR),
                Arguments.of(FunctionSet.LTRIM, VarcharType.VARCHAR),
                Arguments.of(FunctionSet.RTRIM, VarcharType.VARCHAR),
                Arguments.of(FunctionSet.REVERSE, VarcharType.VARCHAR));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rangePreservingUnaryFunctions")
    public void testUnaryFunctionPreservesInputRange(String functionName, Type returnType) {
        // Given <functionName>(id), where id has min = 0 and max = 100
        // CASE WHEN the function cannot move a value outside the range it was given
        //      THEN the output min/max equal the input min/max END

        final double expectedMin = UNARY_INPUT_MIN;
        final double expectedMax = UNARY_INPUT_MAX;
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator functionCall = new CallOperator(functionName, returnType, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(functionCall, statistics);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
    }

    private static Stream<Arguments> domainClampedUnaryFunctions() {
        return Stream.of(
                Arguments.of(FunctionSet.ACOS, FloatType.DOUBLE, 0.0, Math.PI),
                // asin shares acos's branch in the calculator, so it reports acos's [0, PI] instead of
                // asin's real [-PI/2, PI/2]. Recorded as current behaviour, not as a correct range.
                Arguments.of(FunctionSet.ASIN, FloatType.DOUBLE, 0.0, Math.PI),
                Arguments.of(FunctionSet.ATAN, FloatType.DOUBLE, -Math.PI / 2, Math.PI / 2),
                // atan2 takes two arguments in SQL, so no planner builds this single-argument shape.
                // It is kept because it is the only way to reach the calculator's unary atan2 branch.
                Arguments.of(FunctionSet.ATAN2, FloatType.DOUBLE, -Math.PI / 2, Math.PI / 2),
                Arguments.of(FunctionSet.SIN, FloatType.DOUBLE, -1.0, 1.0),
                Arguments.of(FunctionSet.COS, FloatType.DOUBLE, -1.0, 1.0),
                Arguments.of(FunctionSet.RAND, FloatType.DOUBLE, 0.0, 1.0),
                Arguments.of(FunctionSet.RANDOM, FloatType.DOUBLE, 0.0, 1.0),
                Arguments.of(FunctionSet.QUARTER, FloatType.DOUBLE, 1.0, 4.0),
                Arguments.of(FunctionSet.MONTH, FloatType.DOUBLE, 1.0, 12.0),
                Arguments.of(FunctionSet.WEEKOFYEAR, FloatType.DOUBLE, 1.0, 53.0),
                Arguments.of(FunctionSet.WEEK_ISO, FloatType.DOUBLE, 1.0, 53.0),
                Arguments.of(FunctionSet.DAY, FloatType.DOUBLE, 1.0, 31.0),
                Arguments.of(FunctionSet.DAYOFMONTH, FloatType.DOUBLE, 1.0, 31.0),
                Arguments.of(FunctionSet.DAYOFWEEK, FloatType.DOUBLE, 1.0, 7.0),
                Arguments.of(FunctionSet.DAYOFWEEK_ISO, FloatType.DOUBLE, 1.0, 7.0),
                Arguments.of(FunctionSet.DAYOFYEAR, FloatType.DOUBLE, 1.0, 366.0),
                Arguments.of(FunctionSet.HOUR, FloatType.DOUBLE, 0.0, 23.0),
                Arguments.of(FunctionSet.MINUTE, FloatType.DOUBLE, 0.0, 59.0),
                Arguments.of(FunctionSet.SECOND, FloatType.DOUBLE, 0.0, 59.0));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("domainClampedUnaryFunctions")
    public void testUnaryFunctionClampsToItsOwnDomain(String functionName, Type returnType,
                                                      double expectedMin, double expectedMax) {
        // Given <functionName>(id), where id has min = 0 and max = 100
        // CASE WHEN the calculator fixes the function's output range rather than deriving it from the input
        //      THEN min/max are that fixed range, whatever the input range was END

        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator functionCall = new CallOperator(functionName, returnType, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(functionCall, statistics);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
    }

    private static Stream<Arguments> rangeDerivingUnaryFunctions() {
        return Stream.of(
                // negate and reorder [0, 100]
                Arguments.of(FunctionSet.NEGATIVE, FloatType.DOUBLE, -100.0, 0.0),
                // 10 = sqrt(100)
                Arguments.of(FunctionSet.SQRT, FloatType.DOUBLE, 0.0, 10.0),
                // 10000 = 100 squared
                Arguments.of(FunctionSet.SQUARE, FloatType.DOUBLE, 0.0, 10000.0),
                // 57.3 degrees per radian
                Arguments.of(FunctionSet.RADIANS, FloatType.DOUBLE, 0.0, 100 / 57.3),
                // epoch seconds 0 and 100 both land in 1970
                Arguments.of(FunctionSet.YEAR, FloatType.DOUBLE, 1970.0, 1970.0));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rangeDerivingUnaryFunctions")
    public void testUnaryFunctionDerivesRangeFromInputRange(String functionName, Type returnType,
                                                            double expectedMin, double expectedMax) {
        // Given <functionName>(id), where id has min = 0 and max = 100
        // CASE WHEN the function maps its input bounds onto new bounds
        //      THEN min/max are that mapping applied to the input min/max END

        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator functionCall = new CallOperator(functionName, returnType, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(functionCall, statistics);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
    }

    private static Stream<Arguments> unaryFunctionsWithoutNumericRange() {
        return Stream.of(
                Arguments.of(FunctionSet.MONTHNAME, VarcharType.VARCHAR, 12.0),
                Arguments.of(FunctionSet.DAYNAME, VarcharType.VARCHAR, 7.0),
                Arguments.of(FunctionSet.TIME_TO_SEC, IntegerType.BIGINT, UNARY_INPUT_DISTINCT_VALUES),
                Arguments.of(FunctionSet.FROM_UNIXTIME, VarcharType.VARCHAR, UNARY_INPUT_DISTINCT_VALUES));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unaryFunctionsWithoutNumericRange")
    public void testUnaryFunctionWithoutNumericRangeKeepsInfiniteBounds(String functionName, Type returnType,
                                                                        double expectedDistinctValues) {
        // Given <functionName>(id), where id has min = 0, max = 100 and 100 distinct values
        // CASE WHEN the function's result cannot be ordered numerically
        //      THEN min/max stay [-inf, +inf] and only the distinct value count is estimated END

        final double expectedMin = Double.NEGATIVE_INFINITY;
        final double expectedMax = Double.POSITIVE_INFINITY;
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator functionCall = new CallOperator(functionName, returnType, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(functionCall, statistics);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
    }

    private static Stream<Arguments> hashUnaryFunctions() {
        return Stream.of(
                Arguments.of(FunctionSet.XX_HASH32, IntegerType.INT,
                        (double) Integer.MIN_VALUE, (double) Integer.MAX_VALUE),
                Arguments.of(FunctionSet.XX_HASH64, IntegerType.BIGINT,
                        (double) Long.MIN_VALUE, (double) Long.MAX_VALUE),
                Arguments.of(FunctionSet.XX_HASH3_64, IntegerType.BIGINT,
                        (double) Long.MIN_VALUE, (double) Long.MAX_VALUE),
                Arguments.of(FunctionSet.XX_HASH3_128, IntegerType.LARGEINT,
                        LargeIntLiteral.LARGE_INT_MIN.doubleValue(), LargeIntLiteral.LARGE_INT_MAX.doubleValue()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("hashUnaryFunctions")
    public void testUnaryHashFunctionSpansItsReturnTypeDomain(String functionName, Type returnType,
                                                              double expectedMin, double expectedMax) {
        // Given <functionName>(id), where id has min = 0 and max = 100
        // CASE WHEN hashing discards the input's ordering THEN min/max are the return type's own bounds END

        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator functionCall = new CallOperator(functionName, returnType, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(functionCall, statistics);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
    }

    private static Stream<Arguments> hashUnaryFunctionsReportingRowCountNdv() {
        return Stream.of(
                Arguments.of(FunctionSet.XX_HASH32, IntegerType.INT),
                Arguments.of(FunctionSet.XX_HASH64, IntegerType.BIGINT));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("hashUnaryFunctionsReportingRowCountNdv")
    public void testUnaryHashFunctionReportsRowCountAsDistinctValues(String functionName, Type returnType) {
        // Given <functionName>(id) over statistics whose output row count is 100
        // CASE WHEN a hash is assumed collision-free THEN every row is a distinct value END

        final double expectedDistinctValues = UNARY_ROW_COUNT;
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator functionCall = new CallOperator(functionName, returnType, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(functionCall, statistics);

        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testSignReportsThreeValuedRange() {
        // Given SIGN(id), where id has min = 0 and max = 100
        // CASE WHEN sign can only return -1, 0 or 1 THEN min = -1, max = 1 and there are 3 distinct values END

        final double expectedMin = -1;
        final double expectedMax = 1;
        final double expectedDistinctValues = 3;
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator sign = new CallOperator(FunctionSet.SIGN, FloatType.DOUBLE, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(sign, statistics);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testAsciiReportsSingleByteRange() {
        // Given ASCII(id), where id has min = 0 and max = 100
        // CASE WHEN ascii returns one 7-bit code THEN min = 0, max = 127 and there are 128 distinct values END

        final double expectedMin = 0;
        final double expectedMax = 127;
        final double expectedDistinctValues = 128;
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator ascii = new CallOperator(FunctionSet.ASCII, FloatType.DOUBLE, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(ascii, statistics);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testSumScalesInputRangeByRowsPerDistinctValue() {
        // Given SUM(id) evaluated for 10 rows, where id has min = 0, max = 100 and 100 distinct values
        // CASE WHEN the input min is positive THEN it is kept as it is
        //      WHEN the input max is negative THEN it is kept as it is
        //      ELSE the bound is scaled by rowCount / min(rowCount, distinctValues), which scales both
        //      bounds here: min = 0 * 10 / 10 = 0 and max = 100 * 10 / 10 = 100 END

        final double aggregateRowCount = 10;
        final double expectedMin = 0;
        final double expectedMax = 100;
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator sum = new CallOperator(FunctionSet.SUM, FloatType.DOUBLE, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic =
                ExpressionStatisticCalculator.calculate(sum, statistics, aggregateRowCount);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testCountRangesFromZeroToInputRowCount() {
        // Given COUNT(id) evaluated for 10 rows, over statistics whose output row count is 100
        // CASE WHEN a count cannot be negative or exceed the rows available
        //      THEN min = 0 and max = the input row count, and every counted row is treated as distinct END

        final double aggregateRowCount = 10;
        final double expectedMin = 0;
        final double expectedMax = UNARY_ROW_COUNT;
        final double expectedDistinctValues = aggregateRowCount;
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator count = new CallOperator(FunctionSet.COUNT, IntegerType.INT, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic =
                ExpressionStatisticCalculator.calculate(count, statistics, aggregateRowCount);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testMultiDistinctCountRangesFromZeroToInputDistinctValues() {
        // Given MULTI_DISTINCT_COUNT(id) evaluated for 10 rows, where id has 100 distinct values
        // CASE WHEN a distinct count cannot be negative or exceed the input's distinct values
        //      THEN min = 0 and max = 100, and every counted row is treated as distinct END

        final double aggregateRowCount = 10;
        final double expectedMin = 0;
        final double expectedMax = UNARY_INPUT_DISTINCT_VALUES;
        final double expectedDistinctValues = aggregateRowCount;
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator multiDistinctCount =
                new CallOperator(FunctionSet.MULTI_DISTINCT_COUNT, IntegerType.INT, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic =
                ExpressionStatisticCalculator.calculate(multiDistinctCount, statistics, aggregateRowCount);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
    }

    private static Stream<Arguments> timeStrippingUnaryFunctions() {
        return Stream.of(
                Arguments.of(FunctionSet.TO_DATE,
                        LocalDateTime.of(2021, 1, 10, 8, 30, 0), LocalDateTime.of(2021, 12, 25, 23, 59, 59)),
                Arguments.of(FunctionSet.DATE,
                        LocalDateTime.of(2022, 1, 10, 8, 30, 0), LocalDateTime.of(2022, 12, 25, 23, 59, 59)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("timeStrippingUnaryFunctions")
    public void testUnaryFunctionStripsTimeFromRange(String functionName, LocalDateTime inputMin,
                                                     LocalDateTime inputMax) {
        // Given <functionName>(ts), where ts is a DATETIME column carrying both a date and a time of day
        // CASE WHEN the function drops the time part THEN min/max become the start of day of each input bound,
        //      and the distinct value count is carried over unchanged END

        final double inputDistinctValues = 5;
        final double expectedMin = inputMin.toLocalDate().atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        final double expectedMax = inputMax.toLocalDate().atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        final double expectedDistinctValues = 5;
        final ColumnRefOperator timestampColumn = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(UNARY_ROW_COUNT)
                .addColumnStatistic(timestampColumn, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(inputMin))
                        .setMaxValue(getLongFromDateTime(inputMax))
                        .setDistinctValuesCount(inputDistinctValues)
                        .setNullsFraction(0)
                        .setAverageRowSize(10)
                        .build())
                .build();
        final CallOperator functionCall =
                new CallOperator(functionName, DateType.DATE, Lists.newArrayList(timestampColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(functionCall, statistics);

        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testToDaysCountsDaysFromYearZero() {
        // Given TO_DAYS(id), where id has min = 0 and max = 100 epoch seconds, both on 1970-01-01
        // CASE WHEN both bounds fall on the same day
        //      THEN min = max = the day number of 1970-01-01 counted from year 0 END

        final double expectedDayNumber = ExpressionStatisticCalculator.DAYS_FROM_0_TO_1970;
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator toDays =
                new CallOperator(FunctionSet.TO_DAYS, FloatType.DOUBLE, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(toDays, statistics);

        Assertions.assertEquals(expectedDayNumber, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedDayNumber, actualStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testFromDaysClampsDayNumbersBelowYearZeroOffsetToEpoch() {
        // Given FROM_DAYS(id), where id has min = 0 and max = 100 day numbers, both below the year-0 offset
        // CASE WHEN a day number is smaller than the year-0 offset
        //      THEN it clamps to the start of 1970-01-01, so min = max END

        final LocalDate epochDay = LocalDate.of(1970, 1, 1);
        final double expectedEpochSecond = epochDay.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        final ColumnRefOperator idColumn = unaryInputColumn();
        final Statistics statistics = unaryInputStatistics(idColumn);
        final CallOperator fromDays =
                new CallOperator(FunctionSet.FROM_DAYS, FloatType.DOUBLE, Lists.newArrayList(idColumn));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(fromDays, statistics);

        Assertions.assertEquals(expectedEpochSecond, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedEpochSecond, actualStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testHash32DistinctValuesCap() {
        double uint32Cardinality = 4294967296.0;
        double rowCount = uint32Cardinality + 1024;
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, VarcharType.VARCHAR, "name", true);
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(columnRefOperator, new ColumnStatistic(0, 100, 0, 0, rowCount))
                .build();

        CallOperator callOperator = new CallOperator(FunctionSet.XX_HASH32, IntegerType.INT,
                Lists.newArrayList(columnRefOperator));
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assertions.assertEquals(uint32Cardinality, columnStatistic.getDistinctValuesCount(), 0.001);

    }

    @Test
    public void testBinaryFunctionCall() {
        ColumnRefOperator left = new ColumnRefOperator(0, IntegerType.INT, "left", true);
        ColumnRefOperator right = new ColumnRefOperator(1, IntegerType.INT, "right", true);
        Statistics.Builder builder = Statistics.builder();
        ColumnStatistic leftStatistic = new ColumnStatistic(-100, 100, 0, 0, 100);
        ColumnStatistic rightStatistic = new ColumnStatistic(100, 200, 0, 0, 100);
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);

        // test add function
        CallOperator callOperator = new CallOperator(FunctionSet.ADD, IntegerType.BIGINT, Lists.newArrayList(left, right));
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(300, columnStatistic.getMaxValue(), 0.001);
        // test date_add function
        callOperator = new CallOperator(FunctionSet.DATE_ADD, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(300, columnStatistic.getMaxValue(), 0.001);
        // test substract function
        callOperator = new CallOperator(FunctionSet.SUBTRACT, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-300, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test timediff function
        callOperator = new CallOperator(FunctionSet.TIMEDIFF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-300, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test date_sub function
        callOperator = new CallOperator(FunctionSet.DATE_SUB, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-300, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test from_unix function
        callOperator = new CallOperator(FunctionSet.FROM_UNIXTIME, VarcharType.VARCHAR, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(leftStatistic.getDistinctValuesCount(), columnStatistic.getDistinctValuesCount(), 0.001);
        // test years_diff function
        callOperator = new CallOperator(FunctionSet.YEARS_DIFF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test months_diff function
        callOperator = new CallOperator(FunctionSet.MONTHS_DIFF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test weeks_diff function
        callOperator = new CallOperator(FunctionSet.WEEKS_DIFF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.001);
        // test days_diff function
        callOperator = new CallOperator(FunctionSet.DAYS_DIFF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.01);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.01);
        // test datediff function
        callOperator = new CallOperator(FunctionSet.DATEDIFF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.01);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 0.01);
        // test hours_diff function
        callOperator = new CallOperator(FunctionSet.HOURS_DIFF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 1);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 1);
        // test minutes_diff function
        callOperator = new CallOperator(FunctionSet.MINUTES_DIFF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-5, columnStatistic.getMinValue(), 1);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 1);
        // test seconds_diff function
        callOperator = new CallOperator(FunctionSet.SECONDS_DIFF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-300, columnStatistic.getMinValue(), 1);
        Assertions.assertEquals(0, columnStatistic.getMaxValue(), 1);
        // test mod function
        callOperator = new CallOperator(FunctionSet.MOD, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-200, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);
        // test fmod function
        callOperator = new CallOperator(FunctionSet.FMOD, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-200, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);
        // test pmod function
        callOperator = new CallOperator(FunctionSet.PMOD, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-200, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);
        // test ifnull function
        callOperator = new CallOperator(FunctionSet.IFNULL, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);
        // test nullif function
        callOperator = new CallOperator(FunctionSet.NULLIF, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(100, columnStatistic.getMaxValue(), 0.001);
        // test ltrim function
        callOperator = new CallOperator(FunctionSet.LTRIM, VarcharType.VARCHAR, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(100, columnStatistic.getDistinctValuesCount(), 0.001);
        // test ltrim_string function
        callOperator = new CallOperator(FunctionSet.LTRIM_STRING, VarcharType.VARCHAR, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(100, columnStatistic.getDistinctValuesCount(), 0.001);
        // test rtrim function
        callOperator = new CallOperator(FunctionSet.RTRIM, VarcharType.VARCHAR, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(100, columnStatistic.getDistinctValuesCount(), 0.001);
        // test rtrim_string function
        callOperator = new CallOperator(FunctionSet.RTRIM_STRING, VarcharType.VARCHAR, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(100, columnStatistic.getDistinctValuesCount(), 0.001);


        callOperator = new CallOperator(FunctionSet.MULTIPLY, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-20000, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(20000, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.DIVIDE, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-1, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.LIKE, BooleanType.BOOLEAN, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(2, columnStatistic.getDistinctValuesCount(), 0.001);

        callOperator = new CallOperator(FunctionSet.ILIKE, BooleanType.BOOLEAN, Lists.newArrayList(left, right));
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
        callOperator = new CallOperator(FunctionSet.MULTIPLY, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(200, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.DIVIDE, IntegerType.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(50, columnStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testCoalesceReturnsCombinedStatisticsWhenBothInputsAreKnown() {
        // Given COALESCE(left, right)
        // CASE WHEN both inputs have known stats THEN calculate stats based on inputs END

        final int rowCount = 100;
        final int leftDistinctValues = 70;
        final int rightDistinctValues = 20;
        final double leftNullFraction = 0.2;
        final double rightNullFraction = 0.5;
        final int leftMin = -100;
        final int leftMax = 100;
        final int rightMin = 100;
        final double rightMax = 200.5;

        final double expectedDistinctValues = 90;
        final double expectedNullFraction = 0.1;
        final double expectedMin = -100;                // min(leftMin, rightMin)
        final double expectedMax = 200.5;

        final ColumnRefOperator leftInput = new ColumnRefOperator(2, FloatType.DOUBLE, "left", true);
        final ColumnRefOperator rightInput = new ColumnRefOperator(3, FloatType.DOUBLE, "right", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(leftInput, new ColumnStatistic(leftMin, leftMax, leftNullFraction, 0, leftDistinctValues))
                .addColumnStatistic(rightInput,
                        new ColumnStatistic(rightMin, rightMax, rightNullFraction, 0, rightDistinctValues))
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, FloatType.DOUBLE,
                Lists.newArrayList(leftInput, rightInput));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertFalse(actualStatistic.isUnknown());
        Assertions.assertEquals(expectedNullFraction, actualStatistic.getNullsFraction(), 0.001);
        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testCoalesceReturnsUnknownWhenAnyInputIsUnknown() {
        // Given COALESCE(left, right)
        // CASE WHEN an input has unknown stats THEN output stats are also unknown END

        final int rowCount = 100;
        final ColumnRefOperator leftInput = new ColumnRefOperator(2, IntegerType.INT, "left", true);
        final ColumnRefOperator rightInput = new ColumnRefOperator(3, IntegerType.INT, "right", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(leftInput, new ColumnStatistic(-100, 100, 0.2, 0, 70))
                .addColumnStatistic(rightInput, ColumnStatistic.unknown())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, IntegerType.BIGINT,
                Lists.newArrayList(leftInput, rightInput));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertTrue(actualStatistic.isUnknown());
    }

    @Test
    public void testCoalesceReturnsCombinedStatisticsWhenAllThreeInputsAreKnown() {
        // Given COALESCE(input1, input2, input3)
        // CASE WHEN more than two inputs where all have known stats THEN output stat is known END

        final int rowCount = 100;
        final int input1DistinctValues = 30;
        final int input2DistinctValues = 20;
        final int input3DistinctValues = 10;
        final double input1NullFraction = 0.2;
        final double input2NullFraction = 0.5;
        final double input3NullFraction = 0.4;
        final int input1Min = -100;
        final int input1Max = 100;
        final int input2Min = 100;
        final int input2Max = 200;
        final int input3Min = 0;
        final int input3Max = 50;

        final double expectedDistinctValues = 60;
        final double expectedNullFraction = 0.04;
        final double expectedMin = -100;
        final double expectedMax = 200;

        final ColumnRefOperator input1 = new ColumnRefOperator(0, IntegerType.INT, "input1", true);
        final ColumnRefOperator input2 = new ColumnRefOperator(1, IntegerType.INT, "input2", true);
        final ColumnRefOperator input3 = new ColumnRefOperator(2, IntegerType.INT, "input3", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(input1,
                        new ColumnStatistic(input1Min, input1Max, input1NullFraction, 0, input1DistinctValues))
                .addColumnStatistic(input2,
                        new ColumnStatistic(input2Min, input2Max, input2NullFraction, 0, input2DistinctValues))
                .addColumnStatistic(input3,
                        new ColumnStatistic(input3Min, input3Max, input3NullFraction, 0, input3DistinctValues))
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, IntegerType.BIGINT,
                Lists.newArrayList(input1, input2, input3));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertFalse(actualStatistic.isUnknown());
        Assertions.assertEquals(expectedNullFraction, actualStatistic.getNullsFraction(), 0.001);
        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testCalculateMcvForKnownBinaryInputs() {
        // Given COALESCE(mcvLeft, mcvRight) where both inputs have MCV histograms
        // CASE WHEN both inputs are NOT NULL THEN scale and weight MCV END

        final long rowCount = 1000;
        final double leftNullFraction = 0.3;
        final double rightNullFraction = 0.5;
        final Map<String, Long> leftMcv = Map.of("A", 400L, "B", 200L);
        final Map<String, Long> rightMcv = Map.of("X", 300L, "A", 100L);

        // Left MCVs pass through unscaled; right MCVs are scaled by the left null fraction (0.3) and merged by key.
        final Map<String, Long> expectedMcv = Map.of(
                "A", 430L,
                "B", 200L,
                "X", 90L);

        final ColumnRefOperator mcvLeft = new ColumnRefOperator(4, IntegerType.INT, "mcvLeft", true);
        final ColumnRefOperator mcvRight = new ColumnRefOperator(5, IntegerType.INT, "mcvRight", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(mcvLeft, ColumnStatistic.builder()
                        .setNullsFraction(leftNullFraction)
                        .setDistinctValuesCount(2)
                        .setHistogram(new Histogram(Collections.emptyList(), leftMcv))
                        .build())
                .addColumnStatistic(mcvRight, ColumnStatistic.builder()
                        .setNullsFraction(rightNullFraction)
                        .setDistinctValuesCount(2)
                        .setHistogram(new Histogram(Collections.emptyList(), rightMcv))
                        .build())
                .build();
        final CallOperator coalesce =
                new CallOperator(FunctionSet.COALESCE, IntegerType.BIGINT, Lists.newArrayList(mcvLeft, mcvRight));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertNotNull(actualStatistic.getHistogram());
        Assertions.assertEquals(expectedMcv, actualStatistic.getHistogram().getMCV());
    }

    @Test
    public void testCoalesceMcvCalculationWithMissingMcv() {
        // Given COALESCE(input1, input2, input3)
        // CASE WHEN one input has no mcv THEN mcv calculation does not account for the missing input END

        final long rowCount = 1000;
        final double input1NullFraction = 0.3;
        final double input2NullFraction = 0.5;
        final double input3NullFraction = 0.2;
        final Map<String, Long> input1Mcv = Map.of("A", 400L, "B", 200L);
        final Map<String, Long> input3Mcv = Map.of("Y", 50L);

        // input1 passes through unscaled; input2 has no histogram so it adds nothing, but its null fraction
        // still scales later inputs, so input3 is scaled by 0.3 * 0.5 = 0.15.
        final Map<String, Long> expectedMcv = Map.of(
                "A", 400L,
                "B", 200L,
                "Y", 8L);

        final ColumnRefOperator input1 = new ColumnRefOperator(0, IntegerType.INT, "input1", true);
        final ColumnRefOperator input2 = new ColumnRefOperator(1, IntegerType.INT, "input2", true);
        final ColumnRefOperator input3 = new ColumnRefOperator(2, IntegerType.INT, "input3", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(input1, ColumnStatistic.builder()
                        .setNullsFraction(input1NullFraction)
                        .setDistinctValuesCount(2)
                        .setHistogram(new Histogram(Collections.emptyList(), input1Mcv))
                        .build())
                .addColumnStatistic(input2, ColumnStatistic.builder()
                        .setNullsFraction(input2NullFraction)
                        .setDistinctValuesCount(5)
                        .build())
                .addColumnStatistic(input3, ColumnStatistic.builder()
                        .setNullsFraction(input3NullFraction)
                        .setDistinctValuesCount(1)
                        .setHistogram(new Histogram(Collections.emptyList(), input3Mcv))
                        .build())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, IntegerType.BIGINT,
                Lists.newArrayList(input1, input2, input3));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertNotNull(actualStatistic.getHistogram());
        Assertions.assertEquals(expectedMcv, actualStatistic.getHistogram().getMCV());
    }

    @Test
    public void testCoalesceMcvScalingWhenMaxRowCountIsReached() {
        // Given COALESCE(colA, colB)
        // CASE WHEN accumulated MCV rows reach the row count THEN scale the remaining input's MCVs to fit END

        final int rowCount = 300;
        final double colANullFraction = 0.3;
        final double colBNullFraction = 0.0;
        final Map<String, Long> colAMcv = Map.of("a", 100L, "b", 100L);
        final Map<String, Long> colBMcv = Map.of("c", 1000L, "d", 3000L);

        final Map<String, Long> expectedMcv = Map.of(
                "a", 100L,
                "b", 100L,
                "c", 25L,
                "d", 75L);

        final ColumnRefOperator colA = new ColumnRefOperator(0, IntegerType.INT, "colA", true);
        final ColumnRefOperator colB = new ColumnRefOperator(1, IntegerType.INT, "colB", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(colA, ColumnStatistic.builder()
                        .setNullsFraction(colANullFraction)
                        .setDistinctValuesCount(2)
                        .setHistogram(new Histogram(Collections.emptyList(), colAMcv))
                        .build())
                .addColumnStatistic(colB, ColumnStatistic.builder()
                        .setNullsFraction(colBNullFraction)
                        .setDistinctValuesCount(2)
                        .setHistogram(new Histogram(Collections.emptyList(), colBMcv))
                        .build())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, IntegerType.BIGINT,
                Lists.newArrayList(colA, colB));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertNotNull(actualStatistic.getHistogram());
        Assertions.assertEquals(expectedMcv, actualStatistic.getHistogram().getMCV());
    }

    @Test
    public void testCoalesceMcvScalesRemainingInputsAcrossColumnsWhenRowCountReached() {
        // Given COALESCE(input1, input2, input3)
        // CASE WHEN the budget is reached mid-way THEN scale every remaining input's MCVs across columns END

        final int rowCount = 100;
        final double input1NullFraction = 0.5;
        final double input2NullFraction = 0.5;
        final double input3NullFraction = 0.0;
        final Map<String, Long> input1Mcv = Map.of("P", 40L);
        final Map<String, Long> input2Mcv = Map.of("B", 160L, "C", 240L);
        final Map<String, Long> input3Mcv = Map.of("D", 160L);

        final Map<String, Long> expectedMcv = Map.of(
                "P", 40L,
                "B", 20L,
                "C", 30L,
                "D", 10L);

        final ColumnRefOperator input1 = new ColumnRefOperator(0, IntegerType.INT, "input1", true);
        final ColumnRefOperator input2 = new ColumnRefOperator(1, IntegerType.INT, "input2", true);
        final ColumnRefOperator input3 = new ColumnRefOperator(2, IntegerType.INT, "input3", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(input1, ColumnStatistic.builder()
                        .setNullsFraction(input1NullFraction)
                        .setDistinctValuesCount(1)
                        .setHistogram(new Histogram(Collections.emptyList(), input1Mcv))
                        .build())
                .addColumnStatistic(input2, ColumnStatistic.builder()
                        .setNullsFraction(input2NullFraction)
                        .setDistinctValuesCount(2)
                        .setHistogram(new Histogram(Collections.emptyList(), input2Mcv))
                        .build())
                .addColumnStatistic(input3, ColumnStatistic.builder()
                        .setNullsFraction(input3NullFraction)
                        .setDistinctValuesCount(1)
                        .setHistogram(new Histogram(Collections.emptyList(), input3Mcv))
                        .build())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, IntegerType.BIGINT,
                Lists.newArrayList(input1, input2, input3));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertNotNull(actualStatistic.getHistogram());
        Assertions.assertEquals(expectedMcv, actualStatistic.getHistogram().getMCV());
    }

    @Test
    public void testCoalesceMcvScalesDownFirstColumnWhenItAloneExceedsRowCount() {
        // Given COALESCE(input1, input2) where input1 is never null so input2 is unreachable
        // CASE WHEN the first input's MCVs alone exceed the row count THEN scale them down to fit END

        final int rowCount = 100;
        final double input1NullFraction = 0.0;
        final double input2NullFraction = 0.0;
        final Map<String, Long> input1Mcv = Map.of("A", 300L, "B", 100L);
        final Map<String, Long> input2Mcv = Map.of("Z", 9999L);

        final Map<String, Long> expectedMcv = Map.of(
                "A", 75L,
                "B", 25L);

        final ColumnRefOperator input1 = new ColumnRefOperator(0, IntegerType.INT, "input1", true);
        final ColumnRefOperator input2 = new ColumnRefOperator(1, IntegerType.INT, "input2", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(input1, ColumnStatistic.builder()
                        .setNullsFraction(input1NullFraction)
                        .setDistinctValuesCount(2)
                        .setHistogram(new Histogram(Collections.emptyList(), input1Mcv))
                        .build())
                .addColumnStatistic(input2, ColumnStatistic.builder()
                        .setNullsFraction(input2NullFraction)
                        .setDistinctValuesCount(1)
                        .setHistogram(new Histogram(Collections.emptyList(), input2Mcv))
                        .build())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, IntegerType.BIGINT,
                Lists.newArrayList(input1, input2));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertNotNull(actualStatistic.getHistogram());
        Assertions.assertEquals(expectedMcv, actualStatistic.getHistogram().getMCV());
    }

    @Test
    public void testCoalesceLeavesHistogramUnsetWhenNoInputHasMcv() {
        // Given COALESCE(left, right) where both inputs have known stats but no histogram/MCV
        // CASE WHEN no input contributes any MCV THEN the histogram is left unset (not empty) END

        final int rowCount = 100;
        final ColumnRefOperator left = new ColumnRefOperator(0, IntegerType.INT, "left", true);
        final ColumnRefOperator right = new ColumnRefOperator(1, IntegerType.INT, "right", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(left, ColumnStatistic.builder()
                        .setMinValue(-100).setMaxValue(100).setNullsFraction(0.2)
                        .setAverageRowSize(4).setDistinctValuesCount(70).build())
                .addColumnStatistic(right, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(200).setNullsFraction(0.5)
                        .setAverageRowSize(4).setDistinctValuesCount(20).build())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, IntegerType.BIGINT,
                Lists.newArrayList(left, right));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertFalse(actualStatistic.isUnknown());
        Assertions.assertNull(actualStatistic.getHistogram());
    }

    @Test
    public void testCoalesceIgnoresArgsAfterGuaranteedNonNullColumn() {
        // Given COALESCE(nonNullCol, highNdvCol) where the first argument is guaranteed non-null
        // CASE WHEN an earlier argument can never be null THEN later arguments are unreachable and
        //      contribute nothing to NDV or the min/max range END

        final int rowCount = 10000;
        final double nonNullFraction = 0.0;
        final double highNdvNullFraction = 0.3;

        // The result is exactly nonNullCol, so its NDV and range are the output's; highNdvCol is ignored.
        final double expectedDistinctValues = 10;
        final double expectedMin = 5;
        final double expectedMax = 15;
        final double expectedNullFraction = 0.0;

        final ColumnRefOperator nonNullCol = new ColumnRefOperator(0, IntegerType.INT, "nonNullCol", true);
        final ColumnRefOperator highNdvCol = new ColumnRefOperator(1, IntegerType.INT, "highNdvCol", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(nonNullCol, ColumnStatistic.builder()
                        .setMinValue(5).setMaxValue(15)
                        .setNullsFraction(nonNullFraction).setAverageRowSize(4).setDistinctValuesCount(10).build())
                .addColumnStatistic(highNdvCol, ColumnStatistic.builder()
                        .setMinValue(-100).setMaxValue(100000)
                        .setNullsFraction(highNdvNullFraction).setAverageRowSize(4).setDistinctValuesCount(1000).build())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, IntegerType.INT,
                Lists.newArrayList(nonNullCol, highNdvCol));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertFalse(actualStatistic.isUnknown());
        Assertions.assertEquals(expectedDistinctValues, actualStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(expectedNullFraction, actualStatistic.getNullsFraction(), 0.001);
    }

    @Test
    public void testCoalescePropagatesDateRangeWhenOneInputIsFullyNull() {
        // Given COALESCE(fullyNullDate, dateCol) over DATETIME inputs
        // CASE WHEN one input is fully null THEN min/max come from the reachable (non-null) date input END

        final int rowCount = 100;
        final double fullyNullFraction = 1.0;
        final double dateColNullFraction = 0.2;
        final double dateColMin =
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2021-09-01", DateUtils.DATE_FORMATTER_UNIX));
        final double dateColMax =
                getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2022-07-01", DateUtils.DATE_FORMATTER_UNIX));

        final double expectedMin = dateColMin;
        final double expectedMax = dateColMax;

        final ColumnRefOperator fullyNullDate = new ColumnRefOperator(0, DateType.DATETIME, "fullyNullDate", true);
        final ColumnRefOperator dateCol = new ColumnRefOperator(1, DateType.DATETIME, "dateCol", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(fullyNullDate, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY).setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(fullyNullFraction).setAverageRowSize(8).setDistinctValuesCount(0).build())
                .addColumnStatistic(dateCol, ColumnStatistic.builder()
                        .setMinValue(dateColMin).setMaxValue(dateColMax)
                        .setNullsFraction(dateColNullFraction).setAverageRowSize(8).setDistinctValuesCount(50).build())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, DateType.DATETIME,
                Lists.newArrayList(fullyNullDate, dateCol));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertFalse(actualStatistic.isUnknown());
        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(0.2, actualStatistic.getNullsFraction(), 0.001);
    }

    @Test
    public void testCoalescePropagatesTimeRangeWhenOneInputIsFullyNull() {
        // Given COALESCE(fullyNullTime, timeCol) over TIME inputs (TIME min/max are seconds-of-day)
        // CASE WHEN one input is fully null THEN min/max come from the reachable (non-null) time input END

        final int rowCount = 100;
        final double fullyNullFraction = 1.0;
        final double timeColNullFraction = 0.2;
        final double timeColMin = 3600;   // 01:00:00
        final double timeColMax = 7200;   // 02:00:00

        final double expectedMin = timeColMin;
        final double expectedMax = timeColMax;

        final ColumnRefOperator fullyNullTime = new ColumnRefOperator(0, DateType.TIME, "fullyNullTime", true);
        final ColumnRefOperator timeCol = new ColumnRefOperator(1, DateType.TIME, "timeCol", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(fullyNullTime, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY).setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(fullyNullFraction).setAverageRowSize(8).setDistinctValuesCount(0).build())
                .addColumnStatistic(timeCol, ColumnStatistic.builder()
                        .setMinValue(timeColMin).setMaxValue(timeColMax)
                        .setNullsFraction(timeColNullFraction).setAverageRowSize(8).setDistinctValuesCount(50).build())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, DateType.TIME,
                Lists.newArrayList(fullyNullTime, timeCol));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertFalse(actualStatistic.isUnknown());
        Assertions.assertEquals(expectedMin, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedMax, actualStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(0.2, actualStatistic.getNullsFraction(), 0.001);
    }

    @Test
    public void testCoalesceLeavesRangeInfiniteWhenResultTypeIsNotSupported() {
        // Given COALESCE(left, right) over VARCHAR inputs (result type is cannot be represented numerically)
        // CASE WHEN the result type has no meaningful numeric range THEN min/max stay [-inf, +inf] END

        final int rowCount = 100;
        final ColumnRefOperator left = new ColumnRefOperator(0, VarcharType.VARCHAR, "left", true);
        final ColumnRefOperator right = new ColumnRefOperator(1, VarcharType.VARCHAR, "right", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(left, ColumnStatistic.builder()
                        .setMinValue(10).setMaxValue(20).setNullsFraction(0.2)
                        .setAverageRowSize(16).setDistinctValuesCount(70).build())
                .addColumnStatistic(right, ColumnStatistic.builder()
                        .setMinValue(30).setMaxValue(40).setNullsFraction(0.5)
                        .setAverageRowSize(16).setDistinctValuesCount(20).build())
                .build();
        final CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, VarcharType.VARCHAR,
                Lists.newArrayList(left, right));

        final ColumnStatistic actualStatistic = ExpressionStatisticCalculator.calculate(coalesce, statistics);

        Assertions.assertFalse(actualStatistic.isUnknown());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, actualStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, actualStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testWeek() {
        ColumnRefOperator left = new ColumnRefOperator(0, DateType.DATETIME, "left", true);
        ColumnRefOperator right = new ColumnRefOperator(1, IntegerType.INT, "right", true);
        double min = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2021-09-01", DateUtils.DATE_FORMATTER_UNIX));
        double max = Utils.getLongFromDateTime(DateUtils.parseStringWithDefaultHSM("2022-07-01", DateUtils.DATE_FORMATTER_UNIX));
        ColumnStatistic leftStatistic = new ColumnStatistic(min, max, 0, 0, 100);
        ColumnStatistic rightStatistic = new ColumnStatistic(1, 1, 0, 1, 1);
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);
        CallOperator week = new CallOperator(FunctionSet.WEEK, IntegerType.INT, Lists.newArrayList(left, right));
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
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, IntegerType.INT, "id", true);
        CastOperator callOperator = new CastOperator(VarcharType.VARCHAR, columnRefOperator);

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(100);
        builder.addColumnStatistic(columnRefOperator, new ColumnStatistic(-100, 100, 0, 0, 100));

        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(100, columnStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testCaseWhenOperator() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, IntegerType.INT, "", true);
        BinaryPredicateOperator whenOperator1 =
                new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(1));
        ConstantOperator constantOperator1 = ConstantOperator.createChar("1");
        BinaryPredicateOperator whenOperator2 =
                new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(2));
        ConstantOperator constantOperator2 = ConstantOperator.createChar("2");

        CaseWhenOperator caseWhenOperator =
                new CaseWhenOperator(VarcharType.VARCHAR, null, ConstantOperator.createChar("others", VarcharType.VARCHAR),
                        ImmutableList.of(whenOperator1, constantOperator1, whenOperator2, constantOperator2));
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator
                .calculate(caseWhenOperator, Statistics.builder().setOutputRowCount(100).build());
        Assertions.assertEquals(columnStatistic.getDistinctValuesCount(), 3, 0.001);
    }

    @Test
    public void testCaseWhenOperatorNullFractionWithoutElse() {
        // GIVEN
        // CASE WHEN col = 1 THEN '1' WHEN col = 2 THEN '2' END  (no ELSE)
        final var columnRefOperator = new ColumnRefOperator(1, IntegerType.INT, "", true);
        final var whenOperator1 = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(1));
        final var constantOperator1 = ConstantOperator.createChar("1");
        final var whenOperator2 = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(2));
        final var constantOperator2 = ConstantOperator.createChar("2");

        // No ELSE clause: elseClause = null
        CaseWhenOperator caseWhenOperator = new CaseWhenOperator(VarcharType.VARCHAR, null, null,
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
        final var columnRefOperator = new ColumnRefOperator(1, IntegerType.INT, "", true);
        final var whenOperator1 = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(1));
        final var constantOperator1 = ConstantOperator.createChar("1");
        final var whenOperator2 = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(2));
        final var constantOperator2 = ConstantOperator.createChar("2");

        final var caseWhenOperator =
                new CaseWhenOperator(VarcharType.VARCHAR, null, ConstantOperator.createChar("others", VarcharType.VARCHAR),
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
        final var columnRefOperator = new ColumnRefOperator(1, IntegerType.INT, "", true);
        final var whenOperator = new BinaryPredicateOperator(BinaryType.EQ, columnRefOperator,
                ConstantOperator.createInt(1));
        final var thenOperator = ConstantOperator.createChar("x");

        final var caseWhenOperator = new CaseWhenOperator(VarcharType.VARCHAR, null,
                ConstantOperator.createNull(VarcharType.VARCHAR), ImmutableList.of(whenOperator, thenOperator));

        // WHEN
        final var columnStatistic = ExpressionStatisticCalculator.calculate(caseWhenOperator,
                Statistics.builder().setOutputRowCount(100).build());

        // THEN
        Assertions.assertEquals(1, columnStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0.5, columnStatistic.getNullsFraction(), 0.001);
    }

    @Test
    public void testFromDays() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, IntegerType.INT, "", true);
        CallOperator callOperator = new CallOperator(FunctionSet.FROM_DAYS, FloatType.DOUBLE,
                Lists.newArrayList(columnRefOperator));

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
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "column", true);
        BinaryPredicateOperator condition = new BinaryPredicateOperator(BinaryType.EQ, column, ConstantOperator.createInt(1));
        ColumnRefOperator left = new ColumnRefOperator(0, IntegerType.INT, "left", true);
        ColumnRefOperator right = new ColumnRefOperator(1, IntegerType.INT, "right", true);

        ColumnStatistic columnStatistic = new ColumnStatistic(-300, 300, 0, 0, 300);
        ColumnStatistic leftStatistic = new ColumnStatistic(-100, 100, 0, 0, 100);
        ColumnStatistic rightStatistic = new ColumnStatistic(100, 200, 0, 0, 100);

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(300);
        builder.addColumnStatistic(column, columnStatistic);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);

        CallOperator callOperator = new CallOperator(FunctionSet.IF, IntegerType.INT,
                Lists.newArrayList(condition, left, right));
        ColumnStatistic ifStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assertions.assertEquals(ifStatistic.getDistinctValuesCount(), 200, 0.001);
        Assertions.assertEquals(ifStatistic.getMaxValue(), 200, 0.001);
        Assertions.assertEquals(ifStatistic.getMinValue(), -100, 0.001);
    }

    @Test
    public void testMcvPropagationForAddConst() {
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
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
        CastOperator cast = new CastOperator(IntegerType.BIGINT, k);
        ConstantOperator c10 = ConstantOperator.createBigint(10);
        CallOperator add = new CallOperator(FunctionSet.ADD, IntegerType.BIGINT, Lists.newArrayList(cast, c10));

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
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
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
        CastOperator cast = new CastOperator(IntegerType.BIGINT, k);
        ConstantOperator c10 = ConstantOperator.createBigint(10);
        CallOperator add = new CallOperator(FunctionSet.ADD, IntegerType.BIGINT, Lists.newArrayList(c10, cast));

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
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(10, 12, 100L, 0L)), Map.of("10", 10L, "11", 5L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(100).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(100)
                        .setHistogram(hist).build())
                .build();

        CastOperator cast = new CastOperator(IntegerType.BIGINT, k);
        ConstantOperator c10 = ConstantOperator.createBigint(10);
        CallOperator sub = new CallOperator(FunctionSet.SUBTRACT, IntegerType.BIGINT, Lists.newArrayList(cast, c10));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(sub, stats);

        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(10L, exprStats.getHistogram().getMCV().get("0"));
        Assertions.assertEquals(5L, exprStats.getHistogram().getMCV().get("1"));
        Assertions.assertEquals(0.0, exprStats.getHistogram().getBuckets().get(0).getLower(), 0.0001);
        Assertions.assertEquals(2.0, exprStats.getHistogram().getBuckets().get(0).getUpper(), 0.0001);
    }

    @Test
    public void testMcvPropagationForSubtractConstMinusX_doesNotShiftBuckets() {
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L)), Map.of("1", 7L, "2", 3L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(100).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(100)
                        .setHistogram(hist).build())
                .build();

        CastOperator cast = new CastOperator(IntegerType.BIGINT, k);
        ConstantOperator c10 = ConstantOperator.createBigint(10);
        // expr: 10 - cast(k)
        CallOperator sub = new CallOperator(FunctionSet.SUBTRACT, IntegerType.BIGINT, Lists.newArrayList(c10, cast));
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
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Histogram hist = new Histogram(List.of(), Map.of("1", 100L, "2", 50L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(200)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator neg = new CallOperator(FunctionSet.NEGATIVE, IntegerType.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(neg, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(100L, exprStats.getHistogram().getMCV().get("-1"));
        Assertions.assertEquals(50L, exprStats.getHistogram().getMCV().get("-2"));
    }

    @Test
    public void testMcvPropagationForUnaryNegative_transformsBuckets() {
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L)), Map.of("1", 100L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator neg = new CallOperator(FunctionSet.NEGATIVE, IntegerType.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(neg, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(100L, exprStats.getHistogram().getMCV().get("-1"));
        Assertions.assertEquals(1, exprStats.getHistogram().getBuckets().size());
        Assertions.assertEquals(-3.0, exprStats.getHistogram().getBuckets().get(0).getLower(), 0.0001);
        Assertions.assertEquals(-1.0, exprStats.getHistogram().getBuckets().get(0).getUpper(), 0.0001);
    }

    @Test
    public void testMcvPropagationForUnaryNegative_transformsBuckets_multiBucketCumulativeCounts() {
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        // two buckets with cumulative counts: [1,3) count=100, [3,5) count=250
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L), new Bucket(3, 5, 250L, 0L)),
                Map.of("1", 10L, "4", 20L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(250)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator neg = new CallOperator(FunctionSet.NEGATIVE, IntegerType.INT, Lists.newArrayList(k));
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
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Histogram hist = new Histogram(List.of(), Map.of("-1", 100L, "1", 200L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(300)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(-10).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(20)
                        .setHistogram(hist).build())
                .build();

        CallOperator abs = new CallOperator(FunctionSet.ABS, IntegerType.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(abs, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(300L, exprStats.getHistogram().getMCV().get("1"));
    }

    @Test
    public void testMcvPropagationForUnaryAbs_transformsBucketsWhenAllNonPositive() {
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(-3, -1, 100L, 0L)), Map.of("-2", 10L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(-10).setMaxValue(-1).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator abs = new CallOperator(FunctionSet.ABS, IntegerType.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(abs, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(10L, exprStats.getHistogram().getMCV().get("2"));
        Assertions.assertEquals(1, exprStats.getHistogram().getBuckets().size());
        Assertions.assertEquals(1.0, exprStats.getHistogram().getBuckets().get(0).getLower(), 0.0001);
        Assertions.assertEquals(3.0, exprStats.getHistogram().getBuckets().get(0).getUpper(), 0.0001);
    }

    @Test
    public void testMcvPropagationForUnaryAbs_identityWhenAllNonNegativeBuckets() {
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Histogram hist = new Histogram(List.of(new Bucket(1, 3, 100L, 0L), new Bucket(3, 5, 250L, 0L)),
                Map.of("2", 10L, "4", 20L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(250)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(10).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(10)
                        .setHistogram(hist).build())
                .build();

        CallOperator abs = new CallOperator(FunctionSet.ABS, IntegerType.INT, Lists.newArrayList(k));
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
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Histogram hist = new Histogram(List.of(), Map.of("1", 10L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(10)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(1).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(1)
                        .setHistogram(hist).build())
                .build();

        CallOperator pos = new CallOperator(FunctionSet.POSITIVE, IntegerType.INT, Lists.newArrayList(k));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(pos, stats);
        Assertions.assertNotNull(exprStats.getHistogram());
        Assertions.assertEquals(10L, exprStats.getHistogram().getMCV().get("1"));
    }

    @Test
    public void testMcvPropagationFailClosedForNonIntegerType() {
        ColumnRefOperator k = new ColumnRefOperator(1, IntegerType.INT, "k", true);
        Histogram hist = new Histogram(List.of(), Map.of("1", 10L));
        Statistics stats = Statistics.builder()
                .setOutputRowCount(10)
                .addColumnStatistic(k, ColumnStatistic.builder()
                        .setMinValue(1).setMaxValue(1).setNullsFraction(0).setAverageRowSize(4).setDistinctValuesCount(1)
                        .setHistogram(hist).build())
                .build();

        // expr type is DOUBLE => should not propagate histogram/MCV
        CallOperator add = new CallOperator(FunctionSet.ADD, FloatType.DOUBLE,
                Lists.newArrayList(k, ConstantOperator.createInt(1)));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(add, stats);
        Assertions.assertNull(exprStats.getHistogram());
    }

    @Test
    public void testMcvPropagationFailClosedForNoConstSide() {
        ColumnRefOperator k1 = new ColumnRefOperator(1, IntegerType.INT, "k1", true);
        ColumnRefOperator k2 = new ColumnRefOperator(2, IntegerType.INT, "k2", true);
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

        CallOperator add = new CallOperator(FunctionSet.ADD, IntegerType.BIGINT, Lists.newArrayList(k1, k2));
        ColumnStatistic exprStats = ExpressionStatisticCalculator.calculate(add, stats);
        Assertions.assertNull(exprStats.getHistogram());
    }

    @Test
    public void testIsNullPredicateStatisticsWithNoNulls() {
        // GIVEN
        final var col = new ColumnRefOperator(0, IntegerType.BIGINT, "col", true);
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
        final var col = new ColumnRefOperator(1, IntegerType.BIGINT, "col", true);
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
        final var arrayCol = new ColumnRefOperator(1, ArrayType.ARRAY_INT, "arr", true);
        final var lambdaArg = new ColumnRefOperator(10, IntegerType.INT, "x", true, true);

        final var condition = new BinaryPredicateOperator(BinaryType.EQ, lambdaArg, ConstantOperator.createNull(IntegerType.INT));
        final var nullConst = ConstantOperator.createNull(IntegerType.INT);

        final var ifOp = new CallOperator(FunctionSet.IF, IntegerType.INT, Lists.newArrayList(condition, lambdaArg, nullConst));
        var lambda = new LambdaFunctionOperator(List.of(lambdaArg), ifOp, IntegerType.INT);

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

        final var arrayMap = new CallOperator(FunctionSet.ARRAY_MAP, ArrayType.ARRAY_INT,
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
        final var arrayCol = new ColumnRefOperator(1, ArrayType.ARRAY_INT, "arr", true);
        final var otherCol = new ColumnRefOperator(2, IntegerType.INT, "other", true);
        // Lambda argument 'x' is a separate ColumnRefOperator with isLambdaArgument=true.
        final var lambdaArg = new ColumnRefOperator(10, IntegerType.INT, "x", true, true);

        var addOp = new CallOperator(FunctionSet.ADD, IntegerType.INT,
                Lists.newArrayList(otherCol, new ConstantOperator(1, IntegerType.INT)));
        var lambda = new LambdaFunctionOperator(List.of(lambdaArg), addOp, IntegerType.INT);

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

        final var arrayMap = new CallOperator(FunctionSet.ARRAY_MAP, ArrayType.ARRAY_INT,
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
        final var idCol = new ColumnRefOperator(3, IntegerType.INT, "ID", true);
        final var arrayTestCol = new ColumnRefOperator(4, ArrayType.ARRAY_INT, "ARRAY_TEST", true);
        final var lambdaArgX = new ColumnRefOperator(5, IntegerType.INT, "x", true, true);

        final var isNotNullPredicate = new IsNullPredicateOperator(true, idCol);
        final var caseWhen = new CaseWhenOperator(IntegerType.INT, null, null,
                Lists.newArrayList(isNotNullPredicate, lambdaArgX));

        var lambda = new LambdaFunctionOperator(List.of(lambdaArgX), caseWhen, IntegerType.INT);

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

        final var arrayMap = new CallOperator(FunctionSet.ARRAY_MAP, ArrayType.ARRAY_INT,
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
        final var col = new ColumnRefOperator(1, IntegerType.BIGINT, "col", true);
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
    public void testInPredicateDoesNotLeakOperandHistogram() {
        final var col = new ColumnRefOperator(0, IntegerType.INT, "flag", true);
        final var hist = new Histogram(List.of(), Map.of("0", 300L, "1", 700L));
        final var stats = Statistics.builder()
                .setOutputRowCount(1_000)
                .addColumnStatistic(col, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(1).setNullsFraction(0)
                        .setAverageRowSize(4).setDistinctValuesCount(2)
                        .setHistogram(hist).build())
                .build();

        final var in = new InPredicateOperator(col, new ConstantOperator(5, IntegerType.INT));
        final var notIn = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, in);

        final var resultIn = ExpressionStatisticCalculator.calculate(in, stats);
        final var resultNotIn = ExpressionStatisticCalculator.calculate(notIn, stats);

        Assertions.assertTrue(resultIn.getHistogram() == null || resultIn.getHistogram().getMCV().isEmpty());
        Assertions.assertNotNull(resultNotIn.getHistogram());
        long trueRows = resultNotIn.getHistogram().getMCV().getOrDefault("1", 0L);
        long falseRows = resultNotIn.getHistogram().getMCV().getOrDefault("0", 0L);
        Assertions.assertTrue(trueRows >= 990L);
        Assertions.assertTrue(falseRows <= 10L);
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

    private void assertDateTruncStatistics(Type type, String fmt, LocalDateTime min, LocalDateTime max,
                                           double inputDistinctValues, double nullsFraction,
                                           LocalDateTime expectedMin, LocalDateTime expectedMax,
                                           double expectedDistinctValues) {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, type, "dt", true);
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(2048)
                .addColumnStatistic(columnRefOperator, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(min))
                        .setMaxValue(getLongFromDateTime(max))
                        .setNullsFraction(nullsFraction)
                        .setAverageRowSize(type.getTypeSize())
                        .setDistinctValuesCount(inputDistinctValues)
                        .build())
                .build();

        CallOperator callOperator = new CallOperator(
                FunctionSet.DATE_TRUNC,
                type,
                Lists.newArrayList(ConstantOperator.createVarchar(fmt), columnRefOperator));
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);

        Assertions.assertEquals(getLongFromDateTime(expectedMin), columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(getLongFromDateTime(expectedMax), columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(expectedDistinctValues, columnStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(nullsFraction, columnStatistic.getNullsFraction(), 0.001);
        Assertions.assertEquals(type.getTypeSize(), columnStatistic.getAverageRowSize(), 0.001);
    }

    @Test
    public void testDateTruncStatisticsWithMinMaxStats() {
        // DATETIME month
        assertDateTruncStatistics(
                DateType.DATETIME, "month",
                LocalDateTime.of(2024, 1, 15, 10, 20, 30),
                LocalDateTime.of(2024, 3, 20, 12, 34, 56),
                1000, 0.25,
                LocalDateTime.of(2024, 1, 1, 0, 0),
                LocalDateTime.of(2024, 3, 1, 0, 0),
                3);

        // DATETIME week
        assertDateTruncStatistics(
                DateType.DATETIME, "week",
                LocalDateTime.of(2024, 1, 15, 10, 20, 30),
                LocalDateTime.of(2024, 3, 20, 12, 34, 56),
                1000, 0.25,
                LocalDateTime.of(2024, 1, 15, 0, 0),
                LocalDateTime.of(2024, 3, 18, 0, 0),
                10);

        // DATETIME day
        assertDateTruncStatistics(
                DateType.DATETIME, "day",
                LocalDateTime.of(2024, 1, 15, 10, 20, 30),
                LocalDateTime.of(2024, 3, 20, 12, 34, 56),
                1000, 0.25,
                LocalDateTime.of(2024, 1, 15, 0, 0),
                LocalDateTime.of(2024, 3, 20, 0, 0),
                66);

        // DATETIME hour
        assertDateTruncStatistics(
                DateType.DATETIME, "hour",
                LocalDateTime.of(2024, 1, 15, 10, 20, 30),
                LocalDateTime.of(2024, 1, 15, 12, 22, 32),
                1000, 0.25,
                LocalDateTime.of(2024, 1, 15, 10, 0),
                LocalDateTime.of(2024, 1, 15, 12, 0),
                3);

        // DATETIME minute
        assertDateTruncStatistics(
                DateType.DATETIME, "minute",
                LocalDateTime.of(2024, 1, 15, 10, 20, 30),
                LocalDateTime.of(2024, 1, 15, 10, 22, 32),
                1000, 0.25,
                LocalDateTime.of(2024, 1, 15, 10, 20),
                LocalDateTime.of(2024, 1, 15, 10, 22),
                3);

        // DATETIME second
        assertDateTruncStatistics(
                DateType.DATETIME, "second",
                LocalDateTime.of(2024, 1, 15, 10, 20, 30),
                LocalDateTime.of(2024, 1, 15, 10, 20, 32),
                1000, 0.25,
                LocalDateTime.of(2024, 1, 15, 10, 20, 30),
                LocalDateTime.of(2024, 1, 15, 10, 20, 32),
                3);

        // DATE year
        assertDateTruncStatistics(
                DateType.DATE, "year",
                LocalDateTime.of(2023, 2, 15, 0, 0),
                LocalDateTime.of(2025, 8, 20, 0, 0),
                1000, 0.4,
                LocalDateTime.of(2023, 1, 1, 0, 0),
                LocalDateTime.of(2025, 1, 1, 0, 0),
                3);

        // DATE quarter
        assertDateTruncStatistics(
                DateType.DATE, "quarter",
                LocalDateTime.of(2023, 2, 15, 0, 0),
                LocalDateTime.of(2023, 8, 20, 0, 0),
                1000, 0.4,
                LocalDateTime.of(2023, 1, 1, 0, 0),
                LocalDateTime.of(2023, 7, 1, 0, 0),
                3);

        // DATE month
        assertDateTruncStatistics(
                DateType.DATE, "month",
                LocalDateTime.of(2024, 1, 15, 0, 0),
                LocalDateTime.of(2024, 3, 20, 0, 0),
                1000, 0.4,
                LocalDateTime.of(2024, 1, 1, 0, 0),
                LocalDateTime.of(2024, 3, 1, 0, 0),
                3);

        // DATE week
        assertDateTruncStatistics(
                DateType.DATE, "week",
                LocalDateTime.of(2024, 1, 16, 0, 0),
                LocalDateTime.of(2024, 2, 1, 0, 0),
                1000, 0.4,
                LocalDateTime.of(2024, 1, 15, 0, 0),
                LocalDateTime.of(2024, 1, 29, 0, 0),
                3);

        // DATE day
        assertDateTruncStatistics(
                DateType.DATE, "day",
                LocalDateTime.of(2024, 1, 15, 0, 0),
                LocalDateTime.of(2024, 3, 20, 0, 0),
                5, 0.4,
                LocalDateTime.of(2024, 1, 15, 0, 0),
                LocalDateTime.of(2024, 3, 20, 0, 0),
                5);
    }

    @Test
    public void testDateTruncFallbackStatistics() {
        // Infinite range, NDV capped at truncated(MIN_DATETIME)..truncated(MAX_DATETIME) domain
        assertDateTruncFallbackStatistics("year", 100_000, 10000);
        assertDateTruncFallbackStatistics("quarter", 100_000, 40000);
        assertDateTruncFallbackStatistics("month", 200_000, 120000);
        // week: date_trunc('week', 0000-01-01) underflows (Saturday → previous Monday is year -1),
        // so fallback cannot determine valid bounds and input NDV is preserved
        assertDateTruncFallbackStatistics("week", 1_000_000, 1_000_000);
        assertDateTruncFallbackStatistics("day", 10_000_000, 3652425);
        assertDateTruncFallbackStatistics("hour", 100_000_000, 87658200);

        // Minute/second fallback too precise, NDV preserved from input
        assertDateTruncFallbackStatistics("minute", 500, 500);
        assertDateTruncFallbackStatistics("second", 500, 500);

        // Input NDV smaller than max, stays at input NDV
        assertDateTruncFallbackStatistics("year", 10, 10);
    }

    /**
     * Tests the fallback path of date_trunc NDV estimation when column statistics have
     * infinite min/max (no range information available).
     */
    private void assertDateTruncFallbackStatistics(String fmt, double inputDistinctValues,
                                                   double expectedDistinctValues) {
        // GIVEN
        final var columnRefOperator = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final var statistics = Statistics.builder()
                .setOutputRowCount(10_000_000)
                .addColumnStatistic(columnRefOperator, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0)
                        .setAverageRowSize(DateType.DATETIME.getTypeSize())
                        .setDistinctValuesCount(inputDistinctValues)
                        .build())
                .build();

        // WHEN
        final var callOperator = new CallOperator(
                FunctionSet.DATE_TRUNC,
                DateType.DATETIME,
                Lists.newArrayList(ConstantOperator.createVarchar(fmt), columnRefOperator));
        final var result = ExpressionStatisticCalculator.calculate(callOperator, statistics);

        // THEN
        Assertions.assertEquals(expectedDistinctValues, result.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, result.getMinValue(), 0.001);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, result.getMaxValue(), 0.001);
    }

    @Test
    public void testDateTruncMinMaxResetToInfinityWhenTruncationFails() {
        // date_trunc('week', 0000-01-01) underflows before MIN_DATETIME → truncation fails.
        // min/max must be set to ±INF (unknown), not left as untruncated input values.
        final var col = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final var minDt = LocalDateTime.of(0, 1, 1, 0, 0, 0);   // 0000-01-01, a Monday-ish boundary
        final var maxDt = LocalDateTime.of(0, 1, 10, 0, 0, 0);
        final var statistics = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(col, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(minDt))
                        .setMaxValue(getLongFromDateTime(maxDt))
                        .setDistinctValuesCount(5)
                        .build())
                .build();

        final var call = new CallOperator(FunctionSet.DATE_TRUNC, DateType.DATETIME,
                Lists.newArrayList(ConstantOperator.createVarchar("week"), col));
        final var result = ExpressionStatisticCalculator.calculate(call, statistics);

        Assertions.assertEquals(Double.NEGATIVE_INFINITY, result.getMinValue());
        Assertions.assertEquals(Double.POSITIVE_INFINITY, result.getMaxValue());
    }

    @Test
    public void testDateTruncMcvPropagation() {
        // GIVEN
        final var col = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        Map<String, Long> inputMcv = Map.of(
                "2024-01-15 10:20:30", 100L,
                "2024-01-15 14:45:00", 200L,  // same day as above
                "2024-02-20 08:00:00", 150L
        );
        final var colStat = ColumnStatistic.builder()
                .setDistinctValuesCount(1000)
                .setHistogram(new Histogram(Collections.emptyList(), inputMcv))
                .build();
        final var statistics = Statistics.builder()
                .setOutputRowCount(10000)
                .addColumnStatistic(col, colStat)
                .build();

        // WHEN
        final var dateTruncDay = new CallOperator(FunctionSet.DATE_TRUNC, DateType.DATETIME,
                Lists.newArrayList(ConstantOperator.createVarchar("day"), col));
        final var result = ExpressionStatisticCalculator.calculate(dateTruncDay, statistics);

        // THEN
        Assertions.assertNotNull(result.getHistogram());
        final var mcv = result.getHistogram().getMCV();
        Assertions.assertEquals(2, mcv.size());
        Assertions.assertEquals(100L + 200L, mcv.get("2024-01-15 00:00:00"));
        Assertions.assertEquals(150L, mcv.get("2024-02-20 00:00:00"));
    }

    @Test
    public void testDateTruncMcvPropagationWithMonthTruncation() {
        // GIVEN
        final var col = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final var inputMcv = Map.of(
                "2024-03-10 12:00:00", 50L,
                "2024-03-25 18:30:00", 70L,
                "2024-04-05 09:00:00", 30L
        );
        final var colStat = ColumnStatistic.builder()
                .setDistinctValuesCount(500)
                .setHistogram(new Histogram(Collections.emptyList(), inputMcv))
                .build();
        final var statistics = Statistics.builder()
                .setOutputRowCount(5000)
                .addColumnStatistic(col, colStat)
                .build();

        // WHEN
        final var dateTruncMonth = new CallOperator(FunctionSet.DATE_TRUNC, DateType.DATETIME,
                Lists.newArrayList(ConstantOperator.createVarchar("month"), col));
        final var result = ExpressionStatisticCalculator.calculate(dateTruncMonth, statistics);

        // THEN
        Assertions.assertNotNull(result.getHistogram());
        final var mcv = result.getHistogram().getMCV();
        Assertions.assertEquals(2, mcv.size());
        Assertions.assertEquals(50L + 70L, mcv.get("2024-03-01 00:00:00"));
        Assertions.assertEquals(30L, mcv.get("2024-04-01 00:00:00"));
    }

    @Test
    public void testDateTruncMcvPropagationWithoutHistogram() {
        // GIVEN
        final var col = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final var colStat = ColumnStatistic.builder()
                .setDistinctValuesCount(100)
                .build();
        final var statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col, colStat)
                .build();

        // WHEN
        final var call = new CallOperator(FunctionSet.DATE_TRUNC, DateType.DATETIME,
                Lists.newArrayList(ConstantOperator.createVarchar("month"), col));
        final var result = ExpressionStatisticCalculator.calculate(call, statistics);

        // THEN
        Assertions.assertNull(result.getHistogram());
    }

    @Test
    public void testConvertTzWidensRangeAndUsesProductOfNdvsWhenBelowRowCount() {
        final int rowCount = 100;
        final double dtNdv = 2;
        final double fromTzNdv = 1;
        final double toTzNdv = 2;
        final double nullsFraction = 0.1;
        final LocalDateTime minDt = LocalDateTime.of(2021, 1, 10, 8, 30, 0);
        final LocalDateTime maxDt = LocalDateTime.of(2021, 12, 25, 23, 59, 59);
        final double minEpoch = getLongFromDateTime(minDt);
        final double maxEpoch = getLongFromDateTime(maxDt);

        final ColumnRefOperator dtCol = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        final ColumnRefOperator fromTzCol = new ColumnRefOperator(2, VarcharType.VARCHAR, "from_tz", true);
        final ColumnRefOperator toTzCol = new ColumnRefOperator(3, VarcharType.VARCHAR, "to_tz", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(minEpoch)
                        .setMaxValue(maxEpoch)
                        .setNullsFraction(nullsFraction)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(dtNdv)
                        .build())
                .addColumnStatistic(fromTzCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(fromTzNdv)
                        .build())
                .addColumnStatistic(toTzCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(toTzNdv)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(dtCol, fromTzCol, toTzCol));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        Assertions.assertFalse(actual.isUnknown());
        // Variable zones: estimated range must cover conversions under near-extreme offsets.
        assertConvertTzStatRangeCovers(actual, "UTC", "Asia/Shanghai", minDt, maxDt);
        assertConvertTzStatRangeCovers(actual, "Pacific/Kiritimati", "Etc/GMT+12", minDt, maxDt);
        Assertions.assertEquals(nullsFraction, actual.getNullsFraction(), 0.001);
        Assertions.assertEquals(4, actual.getDistinctValuesCount(), 0.001); // min(100*(1-0.1), 2*1*2)
        Assertions.assertEquals(DateType.DATETIME.getTypeSize(), actual.getAverageRowSize(), 0.001);
        Assertions.assertNull(actual.getHistogram());
    }

    @Test
    public void testConvertTzCapsDistinctValuesAtRowCount() {
        final int rowCount = 5;
        final ColumnRefOperator dtCol = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        final ColumnRefOperator fromTzCol = new ColumnRefOperator(2, VarcharType.VARCHAR, "from_tz", true);
        final ColumnRefOperator toTzCol = new ColumnRefOperator(3, VarcharType.VARCHAR, "to_tz", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(LocalDateTime.of(2021, 1, 1, 0, 0, 0)))
                        .setMaxValue(getLongFromDateTime(LocalDateTime.of(2021, 1, 3, 0, 0, 0)))
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(3)
                        .build())
                .addColumnStatistic(fromTzCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .addColumnStatistic(toTzCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(dtCol, fromTzCol, toTzCol));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        Assertions.assertEquals(5, actual.getDistinctValuesCount(), 0.001); // min(5*(1-0), 3*2*2)
    }

    @Test
    public void testConvertTzCapsDistinctValuesAtNonNullRowCount() {
        final int rowCount = 10;
        final double dtNulls = 0.4;
        final ColumnRefOperator dtCol = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        final ColumnRefOperator fromTzCol = new ColumnRefOperator(2, VarcharType.VARCHAR, "from_tz", true);
        final ColumnRefOperator toTzCol = new ColumnRefOperator(3, VarcharType.VARCHAR, "to_tz", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(rowCount)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(LocalDateTime.of(2021, 1, 1, 0, 0, 0)))
                        .setMaxValue(getLongFromDateTime(LocalDateTime.of(2021, 1, 3, 0, 0, 0)))
                        .setNullsFraction(dtNulls)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(3)
                        .build())
                .addColumnStatistic(fromTzCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .addColumnStatistic(toTzCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(dtCol, fromTzCol, toTzCol));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        // min(10*(1-0.4), 3*2*2) = min(6, 12) = 6
        Assertions.assertEquals(6, actual.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testConvertTzCombinesNullsFractionFromAllArguments() {
        // convert_tz is null if any argument is null:
        // 1 - (1-0.2)*(1-0.5)*(1-0.4) = 1 - 0.8*0.5*0.6 = 0.76
        final double dtNulls = 0.2;
        final double fromTzNulls = 0.5;
        final double toTzNulls = 0.4;
        final double expectedNulls = 1.0 - (1.0 - dtNulls) * (1.0 - fromTzNulls) * (1.0 - toTzNulls);

        final ColumnRefOperator dtCol = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        final ColumnRefOperator fromTzCol = new ColumnRefOperator(2, VarcharType.VARCHAR, "from_tz", true);
        final ColumnRefOperator toTzCol = new ColumnRefOperator(3, VarcharType.VARCHAR, "to_tz", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(100)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(LocalDateTime.of(2021, 1, 1, 0, 0, 0)))
                        .setMaxValue(getLongFromDateTime(LocalDateTime.of(2021, 1, 3, 0, 0, 0)))
                        .setNullsFraction(dtNulls)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(3)
                        .build())
                .addColumnStatistic(fromTzCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(fromTzNulls)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .addColumnStatistic(toTzCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(toTzNulls)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(dtCol, fromTzCol, toTzCol));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        Assertions.assertEquals(expectedNulls, actual.getNullsFraction(), 0.001);
        Assertions.assertEquals(0.76, actual.getNullsFraction(), 0.001);
    }

    @Test
    public void testConvertTzNullsFractionWithConstantTimezonesUsesOnlyDatetimeNulls() {
        // Constant timezones have nullsFraction 0, so result nulls = dt nulls.
        final double dtNulls = 0.3;
        final ColumnRefOperator dtCol = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 10, 20, 30)))
                        .setMaxValue(getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 14, 45, 0)))
                        .setNullsFraction(dtNulls)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(
                        dtCol,
                        ConstantOperator.createVarchar("UTC"),
                        ConstantOperator.createVarchar("Asia/Shanghai")));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        Assertions.assertEquals(dtNulls, actual.getNullsFraction(), 0.001);
    }

    @Test
    public void testConvertTzMcvPropagationWithConstantTimezones() {
        final String fromTz = "UTC";
        final String toTz = "Asia/Shanghai";
        final String dt1 = "2024-01-15 10:20:30";
        final String dt2 = "2024-01-15 14:45:00";
        final Map<String, Long> inputMcv = Map.of(dt1, 100L, dt2, 200L);

        final ColumnRefOperator dtCol = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final ColumnStatistic dtStat = ColumnStatistic.builder()
                .setMinValue(getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 10, 20, 30)))
                .setMaxValue(getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 14, 45, 0)))
                .setNullsFraction(0)
                .setAverageRowSize(8)
                .setDistinctValuesCount(2)
                .setHistogram(new Histogram(Collections.emptyList(), inputMcv))
                .build();
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(dtCol, dtStat)
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(
                        dtCol,
                        ConstantOperator.createVarchar(fromTz),
                        ConstantOperator.createVarchar(toTz)));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        Assertions.assertNotNull(actual.getHistogram());
        Assertions.assertEquals(1, actual.getHistogram().getBuckets().size());
        final Bucket bucket = actual.getHistogram().getBuckets().get(0);
        Assertions.assertEquals(actual.getMinValue(), bucket.getLower(), 0.001);
        Assertions.assertEquals(actual.getMaxValue(), bucket.getUpper(), 0.001);
        Assertions.assertEquals(700L, bucket.getCount()); // 1000 - (100 + 200)
        Assertions.assertEquals(0L, bucket.getUpperRepeats());
        final Map<String, Long> mcv = actual.getHistogram().getMCV();
        Assertions.assertEquals(2, mcv.size());
        Assertions.assertEquals(100L, mcv.get(convertTzMcvKey(dt1, fromTz, toTz)));
        Assertions.assertEquals(200L, mcv.get(convertTzMcvKey(dt2, fromTz, toTz)));
        assertConvertTzStatRangeCovers(actual, fromTz, toTz,
                LocalDateTime.of(2024, 1, 15, 10, 20, 30),
                LocalDateTime.of(2024, 1, 15, 14, 45, 0));
    }

    @Test
    public void testConvertTzRangeCoversSamplesForConstantTimezones() {
        final String fromTz = "UTC";
        final String toTz = "Asia/Shanghai";
        final LocalDateTime minDt = LocalDateTime.of(2024, 1, 15, 10, 20, 30);
        final LocalDateTime maxDt = LocalDateTime.of(2024, 1, 15, 14, 45, 0);

        final ColumnRefOperator dtCol = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(minDt))
                        .setMaxValue(getLongFromDateTime(maxDt))
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(
                        dtCol,
                        ConstantOperator.createVarchar(fromTz),
                        ConstantOperator.createVarchar(toTz)));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        assertConvertTzStatRangeCoversEveryMinute(actual, fromTz, toTz, minDt, maxDt);
        Assertions.assertEquals(2, actual.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testConvertTzRangeCoversSamplesAcrossDstTransition() {
        // Europe/Berlin falls back on 2024-10-27. Endpoint-only conversion under-ranges because
        // UTC 00:59 -> Berlin 02:59 lies outside convert(00:30)/convert(01:30).
        final String fromTz = "UTC";
        final String toTz = "Europe/Berlin";
        final LocalDateTime minDt = LocalDateTime.of(2024, 10, 27, 0, 30, 0);
        final LocalDateTime maxDt = LocalDateTime.of(2024, 10, 27, 1, 30, 0);

        final ColumnRefOperator dtCol = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(minDt))
                        .setMaxValue(getLongFromDateTime(maxDt))
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(3)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(
                        dtCol,
                        ConstantOperator.createVarchar(fromTz),
                        ConstantOperator.createVarchar(toTz)));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        assertConvertTzStatRangeCoversEveryMinute(actual, fromTz, toTz, minDt, maxDt);
    }

    @Test
    public void testConvertTzRangeCoversSamplesWhenOffsetShiftsEarlier() {
        final String fromTz = "Asia/Shanghai";
        final String toTz = "UTC";
        final LocalDateTime minDt = LocalDateTime.of(2024, 1, 15, 10, 20, 30);
        final LocalDateTime maxDt = LocalDateTime.of(2024, 1, 15, 14, 45, 0);

        final ColumnRefOperator dtCol = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(getLongFromDateTime(minDt))
                        .setMaxValue(getLongFromDateTime(maxDt))
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(
                        dtCol,
                        ConstantOperator.createVarchar(fromTz),
                        ConstantOperator.createVarchar(toTz)));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        Assertions.assertTrue(actual.getMinValue() <= actual.getMaxValue());
        assertConvertTzStatRangeCoversEveryMinute(actual, fromTz, toTz, minDt, maxDt);
    }

    @Test
    public void testConvertTzIsAllNullWhenConstantTimezoneInvalid() {
        final double minValue = getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 10, 20, 30));
        final double maxValue = getLongFromDateTime(LocalDateTime.of(2024, 1, 15, 14, 45, 0));

        final ColumnRefOperator dtCol = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(minValue)
                        .setMaxValue(maxValue)
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(
                        dtCol,
                        ConstantOperator.createVarchar("Not/AZone"),
                        ConstantOperator.createVarchar("UTC")));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        Assertions.assertEquals(1.0, actual.getNullsFraction(), 0.001);
        Assertions.assertEquals(0, actual.getDistinctValuesCount(), 0.001);
        Assertions.assertNull(actual.getHistogram());
    }

    @Test
    public void testConvertTzKeepsWidenedRangeWhenChildRangeIsInfinite() {
        final ColumnRefOperator dtCol = new ColumnRefOperator(0, DateType.DATETIME, "dt", true);
        final Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(dtCol, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(2)
                        .build())
                .build();
        final CallOperator convertTz = new CallOperator(FunctionSet.CONVERT_TZ, DateType.DATETIME,
                Lists.newArrayList(
                        dtCol,
                        ConstantOperator.createVarchar("UTC"),
                        ConstantOperator.createVarchar("Asia/Shanghai")));

        final ColumnStatistic actual = ExpressionStatisticCalculator.calculate(convertTz, statistics);

        Assertions.assertTrue(actual.isInfiniteRange());
        Assertions.assertEquals(2, actual.getDistinctValuesCount(), 0.001);
    }

    private static void assertConvertTzStatRangeCovers(ColumnStatistic actual, String fromTz, String toTz,
                                                       LocalDateTime... samples) {
        for (LocalDateTime sample : samples) {
            final double converted = convertTzDateTimeValue(getLongFromDateTime(sample), fromTz, toTz);
            Assertions.assertTrue(
                    converted >= actual.getMinValue() - 0.001 && converted <= actual.getMaxValue() + 0.001,
                    () -> "convert_tz(" + sample + ", " + fromTz + ", " + toTz + ") = " + converted
                            + " outside estimated range [" + actual.getMinValue() + ", " + actual.getMaxValue() + "]");
        }
    }

    private static void assertConvertTzStatRangeCoversEveryMinute(ColumnStatistic actual, String fromTz, String toTz,
                                                                  LocalDateTime start, LocalDateTime end) {
        for (LocalDateTime sample = start; !sample.isAfter(end); sample = sample.plusMinutes(1)) {
            assertConvertTzStatRangeCovers(actual, fromTz, toTz, sample);
        }
    }

    private static String convertTzMcvKey(String datetime, String fromTz, String toTz) {
        final ConstantOperator converted = ScalarOperatorFunctions.convert_tz(
                ConstantOperator.createVarchar(datetime).castTo(DateType.DATETIME).get(),
                ConstantOperator.createVarchar(fromTz),
                ConstantOperator.createVarchar(toTz));
        return converted.castTo(VarcharType.VARCHAR).get().getVarchar();
    }

    private static double convertTzDateTimeValue(double dateTimeValue, String fromTz, String toTz) {
        final ConstantOperator converted = ScalarOperatorFunctions.convert_tz(
                ConstantOperator.createDatetime(Utils.getDatetimeFromLong((long) dateTimeValue)),
                ConstantOperator.createVarchar(fromTz),
                ConstantOperator.createVarchar(toTz));
        return Utils.getLongFromDateTime(converted.getDatetime());
    }

    @Test
    public void testDateTruncMcvPropagationWithDateType() {
        // GIVEN
        final var col = new ColumnRefOperator(0, DateType.DATE, "dt", true);
        final var inputMcv = Map.of(
                "2024-01-15", 100L,
                "2024-01-28", 200L,
                "2024-02-10", 50L
        );
        final var colStat = ColumnStatistic.builder()
                .setDistinctValuesCount(365)
                .setHistogram(new Histogram(Collections.emptyList(), inputMcv))
                .build();
        final var statistics = Statistics.builder()
                .setOutputRowCount(5000)
                .addColumnStatistic(col, colStat)
                .build();

        // WHEN
        final var call = new CallOperator(FunctionSet.DATE_TRUNC, DateType.DATE,
                Lists.newArrayList(ConstantOperator.createVarchar("month"), col));
        final var result = ExpressionStatisticCalculator.calculate(call, statistics);

        // THEN
        Assertions.assertNotNull(result.getHistogram());
        final var mcv = result.getHistogram().getMCV();
        Assertions.assertEquals(2, mcv.size());
        Assertions.assertEquals(100L + 200L, mcv.get("2024-01-01"));
        Assertions.assertEquals(50L, mcv.get("2024-02-01"));
    }

    @Test
    public void testBinaryPredicateExpressionStatisticIsBoolean() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, VarcharType.VARCHAR, "col1", true);
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0.0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(62)
                        .setHistogram(new Histogram(Collections.emptyList(), Map.of("mcv1", 236L)))
                        .build())
                .build();

        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ_FOR_NULL, col1, ConstantOperator.createVarchar("mcv1"));

        ColumnStatistic predicateStatistic = ExpressionStatisticCalculator.calculate(predicate, statistics);

        Assertions.assertEquals(0.0, predicateStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, predicateStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(0.0, predicateStatistic.getNullsFraction(), 0.001);
        Assertions.assertEquals(2.0, predicateStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertNotNull(predicateStatistic.getHistogram());
        Assertions.assertEquals(236L, predicateStatistic.getHistogram().getMCV().get("1"));
        Assertions.assertEquals(764L, predicateStatistic.getHistogram().getMCV().get("0"));
        Assertions.assertFalse(predicateStatistic.getHistogram().getMCV().containsKey("mcv1"));
    }

    @Test
    public void testBinaryPredicateExpressionStatisticForAbsentMcvDoesNotPreserveSourceMcv() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, VarcharType.VARCHAR, "col1", true);
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0.0)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(62)
                        .setHistogram(new Histogram(Collections.emptyList(), Map.of("mcv1", 236L)))
                        .build())
                .build();

        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ_FOR_NULL, col1, ConstantOperator.createVarchar("const1"));

        ColumnStatistic predicateStatistic = ExpressionStatisticCalculator.calculate(predicate, statistics);

        assertBooleanPredicateStatistic(predicateStatistic);
        assertOnlyBooleanMcvs(predicateStatistic, 1000L);
        Assertions.assertTrue(predicateStatistic.getHistogram().getMCV().getOrDefault("0", 0L) >
                predicateStatistic.getHistogram().getMCV().getOrDefault("1", 0L));
        Assertions.assertFalse(predicateStatistic.getHistogram().getMCV().containsKey("mcv1"));
        Assertions.assertFalse(predicateStatistic.getHistogram().getMCV().containsKey("const1"));
    }

    @Test
    public void testConstantBinaryPredicateExpressionStatistic() {
        Statistics statistics = Statistics.builder().setOutputRowCount(1000).build();

        assertConstantBinaryPredicateStatistic(statistics,
                new BinaryPredicateOperator(BinaryType.GT,
                        ConstantOperator.createVarchar("season"), ConstantOperator.createVarchar("a.season")),
                1.0, Map.of("1", 1000L));
        assertConstantBinaryPredicateStatistic(statistics,
                new BinaryPredicateOperator(BinaryType.LT,
                        ConstantOperator.createVarchar("season"), ConstantOperator.createVarchar("a.season")),
                0.0, Map.of("0", 1000L));
        assertConstantBinaryPredicateStatistic(statistics,
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        ConstantOperator.createNull(VarcharType.VARCHAR), ConstantOperator.createNull(VarcharType.VARCHAR)),
                1.0, Map.of("1", 1000L));
        assertConstantBinaryPredicateStatistic(statistics,
                new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                        ConstantOperator.createNull(VarcharType.VARCHAR), ConstantOperator.createVarchar("season")),
                0.0, Map.of("0", 1000L));
    }

    @Test
    public void testConstantBinaryPredicateExpressionStatisticWithRegularNull() {
        Statistics statistics = Statistics.builder().setOutputRowCount(1000).build();
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ, ConstantOperator.createNull(VarcharType.VARCHAR), ConstantOperator.createVarchar("season"));

        ColumnStatistic predicateStatistic = ExpressionStatisticCalculator.calculate(predicate, statistics);

        Assertions.assertEquals(1.0, predicateStatistic.getNullsFraction(), 0.001);
        Assertions.assertEquals(0.0, predicateStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertNull(predicateStatistic.getHistogram());
    }

    @Test
    public void testNullSafeBinaryPredicateExpressionStatisticForNullConstant() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, VarcharType.VARCHAR, "col1", true);
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0.25)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(62)
                        .setHistogram(new Histogram(Collections.emptyList(), Map.of("mcv1", 236L)))
                        .build())
                .build();

        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ_FOR_NULL, col1, ConstantOperator.createNull(VarcharType.VARCHAR));

        ColumnStatistic predicateStatistic = ExpressionStatisticCalculator.calculate(predicate, statistics);

        assertBooleanPredicateStatistic(predicateStatistic, 2.0, Map.of("1", 250L, "0", 750L));
        Assertions.assertFalse(predicateStatistic.getHistogram().getMCV().containsKey("mcv1"));
    }

    @Test
    public void testRegularEqWithNullableColumnHasNullFraction() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, VarcharType.VARCHAR, "col1", true);
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, ColumnStatistic.builder()
                        .setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY)
                        .setNullsFraction(0.2)
                        .setAverageRowSize(8)
                        .setDistinctValuesCount(62)
                        .setHistogram(new Histogram(Collections.emptyList(), Map.of("mcv1", 236L)))
                        .build())
                .build();

        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ, col1, ConstantOperator.createVarchar("mcv1"));

        ColumnStatistic predicateStatistic = ExpressionStatisticCalculator.calculate(predicate, statistics);
        Map<String, Long> mcvs = predicateStatistic.getHistogram().getMCV();

        Assertions.assertEquals(0.0, predicateStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, predicateStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(236, mcvs.getOrDefault("1", 0L));
        Assertions.assertEquals(564, mcvs.getOrDefault("0", 0L));

    }

    @Test
    public void testLessThanWithNullableColumnHasNullFraction() {
        ColumnRefOperator col = new ColumnRefOperator(0, IntegerType.INT, "x", true);
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col, ColumnStatistic.builder()
                        .setMinValue(0)
                        .setMaxValue(100)
                        .setNullsFraction(0.1)
                        .setAverageRowSize(4)
                        .setDistinctValuesCount(100)
                        .build())
                .build();

        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.LT, col, ConstantOperator.createInt(50));

        ColumnStatistic predicateStatistic = ExpressionStatisticCalculator.calculate(predicate, statistics);

        Assertions.assertEquals(0.0, predicateStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, predicateStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(0.1, predicateStatistic.getNullsFraction(), 0.001);

        Map<String, Long> mcvs = predicateStatistic.getHistogram().getMCV();

        assertOnlyBooleanMcvs(predicateStatistic, 900L);
        Assertions.assertEquals(450, mcvs.getOrDefault("1", 0L));
        Assertions.assertEquals(450, mcvs.getOrDefault("0", 0L));
    }

    @Test
    public void testEqForNullWithNullableColumnHasZeroNullFraction() {
        ColumnRefOperator col = new ColumnRefOperator(0, IntegerType.INT, "x", true);
        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col, ColumnStatistic.builder()
                        .setMinValue(0)
                        .setMaxValue(100)
                        .setNullsFraction(0.3)
                        .setAverageRowSize(4)
                        .setDistinctValuesCount(100)
                        .build())
                .build();

        BinaryPredicateOperator predicate = new BinaryPredicateOperator(
                BinaryType.EQ_FOR_NULL, col, ConstantOperator.createInt(50));

        ColumnStatistic predicateStatistic = ExpressionStatisticCalculator.calculate(predicate, statistics);

        Assertions.assertEquals(0.0, predicateStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, predicateStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(0.0, predicateStatistic.getNullsFraction(), 0.001);
        assertOnlyBooleanMcvs(predicateStatistic, 1000L);
    }

    private static void assertBooleanPredicateStatistic(ColumnStatistic predicateStatistic, double ndv,
                                                        Map<String, Long> expectedMcvs) {
        assertBooleanPredicateStatistic(predicateStatistic);
        Assertions.assertEquals(ndv, predicateStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertNotNull(predicateStatistic.getHistogram());
        Assertions.assertEquals(expectedMcvs, predicateStatistic.getHistogram().getMCV());
    }

    private static void assertConstantBinaryPredicateStatistic(Statistics statistics,
                                                               BinaryPredicateOperator predicate,
                                                               double expectedValue,
                                                               Map<String, Long> expectedMcvs) {
        ColumnStatistic predicateStatistic = ExpressionStatisticCalculator.calculate(predicate, statistics);

        Assertions.assertEquals(expectedValue, predicateStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(expectedValue, predicateStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(0.0, predicateStatistic.getNullsFraction(), 0.001);
        Assertions.assertEquals(1.0, predicateStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertNotNull(predicateStatistic.getHistogram());
        Assertions.assertEquals(expectedMcvs, predicateStatistic.getHistogram().getMCV());
    }

    private static void assertBooleanPredicateStatistic(ColumnStatistic predicateStatistic) {
        Assertions.assertEquals(0.0, predicateStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, predicateStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(0.0, predicateStatistic.getNullsFraction(), 0.001);
        Assertions.assertNotNull(predicateStatistic.getHistogram());
        Assertions.assertEquals(predicateStatistic.getHistogram().getMCV().size(),
                predicateStatistic.getDistinctValuesCount(), 0.001);
    }

    private static void assertOnlyBooleanMcvs(ColumnStatistic predicateStatistic, long expectedRows) {
        Map<String, Long> mcvs = predicateStatistic.getHistogram().getMCV();
        Assertions.assertTrue(Set.of("0", "1").containsAll(mcvs.keySet()));
        Assertions.assertEquals(expectedRows, mcvs.values().stream().mapToLong(Long::longValue).sum());
    }

    private static ColumnStatistic booleanColumnStatistic(long trueRows, long falseRows, long nullRows) {
        long totalRows = trueRows + falseRows + nullRows;
        return ColumnStatistic.builder()
                .setMinValue(0)
                .setMaxValue(1)
                .setNullsFraction((double) nullRows / totalRows)
                .setAverageRowSize(BooleanType.BOOLEAN.getTypeSize())
                .setDistinctValuesCount(2)
                .setHistogram(new Histogram(Collections.emptyList(), Map.of("1", trueRows, "0", falseRows)))
                .build();
    }

    // A NON-boolean (INT) column that looks boolean-ish: 0/1 valued with 0/1 MCV keys. It is a "non-suitable"
    // sub-expression for the compound-predicate boolean MCV fast-path, which must be rejected by the type guard.
    private static ColumnStatistic nonBooleanZeroOneColumnStatistic(Map<String, Long> mcv) {
        return ColumnStatistic.builder()
                .setMinValue(0)
                .setMaxValue(1)
                .setNullsFraction(0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(2)
                .setHistogram(new Histogram(Collections.emptyList(), mcv))
                .build();
    }

    private static void assertBooleanDistribution(ColumnStatistic predicateStatistic, long expectedTrueRows,
                                                  long expectedFalseRows, double expectedNullsFraction) {
        Map<String, Long> mcvs = predicateStatistic.getHistogram().getMCV();
        Assertions.assertEquals(expectedTrueRows, mcvs.getOrDefault("1", 0L));
        Assertions.assertEquals(expectedFalseRows, mcvs.getOrDefault("0", 0L));
        Assertions.assertEquals(expectedNullsFraction, predicateStatistic.getNullsFraction(), 0.001);
        Assertions.assertEquals(expectedTrueRows + expectedFalseRows,
                mcvs.values().stream().mapToLong(Long::longValue).sum());
    }

    @Test
    public void testCompoundPredicateAnd() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, IntegerType.INT, "col1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(1, IntegerType.INT, "col2", true);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(100)
                        .setNullsFraction(0.0).setAverageRowSize(4)
                        .setDistinctValuesCount(100).build())
                .addColumnStatistic(col2, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(50)
                        .setNullsFraction(0.0).setAverageRowSize(4)
                        .setDistinctValuesCount(50).build())
                .build();

        // col1 > 50 AND col2 > 25
        BinaryPredicateOperator left = new BinaryPredicateOperator(
                BinaryType.GT, col1, ConstantOperator.createInt(50));
        BinaryPredicateOperator right = new BinaryPredicateOperator(
                BinaryType.GT, col2, ConstantOperator.createInt(25));
        CompoundPredicateOperator andOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, left, right);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(andOp, statistics);

        Assertions.assertEquals(0.0, stat.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, stat.getMaxValue(), 0.001);

        // probability for true should be 0.5 * 0.5 = 0.25
        assertBooleanDistribution(stat, 250L, 750L, 0.0);
    }

    @Test
    public void testCompoundPredicateOr() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, IntegerType.INT, "col1", true);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(100)
                        .setNullsFraction(0.0).setAverageRowSize(4)
                        .setDistinctValuesCount(100).build())
                .build();

        // col1 = 10 OR col1 = 20
        BinaryPredicateOperator left = new BinaryPredicateOperator(
                BinaryType.EQ, col1, ConstantOperator.createInt(10));
        BinaryPredicateOperator right = new BinaryPredicateOperator(
                BinaryType.EQ, col1, ConstantOperator.createInt(20));
        CompoundPredicateOperator orOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, left, right);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(orOp, statistics);

        Assertions.assertEquals(0.0, stat.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, stat.getMaxValue(), 0.001);

        // probability for true should be 2/100, so 0.02 * 1000 = 20 rows.
        assertBooleanDistribution(stat, 20, 980, 0.0);
    }

    @Test
    public void testCompoundPredicateNot() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, IntegerType.INT, "col1", true);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(100)
                        .setNullsFraction(0.0).setAverageRowSize(4)
                        .setDistinctValuesCount(100).build())
                .build();

        // NOT (col1 > 50) should be roughly the complement
        BinaryPredicateOperator inner = new BinaryPredicateOperator(
                BinaryType.GT, col1, ConstantOperator.createInt(50));
        CompoundPredicateOperator notOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, inner);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(notOp, statistics);

        Assertions.assertEquals(0.0, stat.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, stat.getMaxValue(), 0.001);
        Assertions.assertEquals(0.0, stat.getNullsFraction(), 0.001);
        Assertions.assertNotNull(stat.getHistogram());

        assertBooleanDistribution(stat, 500L, 500L, 0.0);
    }

    @Test
    public void testCompoundPredicateAndWithNulls() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, BooleanType.BOOLEAN, "col1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(1, BooleanType.BOOLEAN, "col2", true);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, booleanColumnStatistic(200, 500, 300))
                .addColumnStatistic(col2, booleanColumnStatistic(400, 500, 100))
                .build();

        CompoundPredicateOperator andOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, col1, col2);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(andOp, statistics);

        Assertions.assertEquals(0.0, stat.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, stat.getMaxValue(), 0.001);
        Assertions.assertNotNull(stat.getHistogram());

        assertBooleanDistribution(stat, 80L, 750L, 0.17);
    }

    @Test
    public void testCompoundPredicateOrWithNulls() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, BooleanType.BOOLEAN, "col1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(1, BooleanType.BOOLEAN, "col2", true);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, booleanColumnStatistic(200, 500, 300))
                .addColumnStatistic(col2, booleanColumnStatistic(400, 500, 100))
                .build();

        CompoundPredicateOperator orOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, col1, col2);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(orOp, statistics);

        Assertions.assertEquals(0.0, stat.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, stat.getMaxValue(), 0.001);
        Assertions.assertNotNull(stat.getHistogram());
        // TRUE comes from the existing predicate calculator.
        assertBooleanDistribution(stat, 520L, 250L, 0.23);
    }

    @Test
    public void testCompoundPredicateNotWithNulls() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, BooleanType.BOOLEAN, "col1", true);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, booleanColumnStatistic(200, 500, 300))
                .build();

        CompoundPredicateOperator notOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, col1);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(notOp, statistics);

        Assertions.assertEquals(0.0, stat.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, stat.getMaxValue(), 0.001);
        Assertions.assertNotNull(stat.getHistogram());
        assertBooleanDistribution(stat, 500L, 200L, 0.3);
    }

    @Test
    public void testCompoundPredicateWithNullStatistics() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, IntegerType.INT, "col1", true);

        BinaryPredicateOperator inner = new BinaryPredicateOperator(
                BinaryType.GT, col1, ConstantOperator.createInt(50));
        CompoundPredicateOperator notOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, inner);

        // null input statistics should return unknown
        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(notOp, null);
        Assertions.assertTrue(stat.isUnknown());
    }

    @Test
    public void testCompoundPredicateFiltersOutNonBooleanMcvs() {
        // When a predicate child has non-boolean MCVs (e.g., integer MCVs from a column whose statistics
        // leak through because the visitor is not implemented for that predicate type), the compound predicate
        // calculator should NOT use those MCVs as boolean probabilities. It should fall back to using
        // PredicateStatisticsCalculator selectivity instead.
        ColumnRefOperator col1 = new ColumnRefOperator(0, IntegerType.INT, "col1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(1, IntegerType.INT, "col2", true);

        Map<String, Long> integerMcvs = Map.of("1", 200L, "20", 300L, "30", 500L);
        Histogram intHistogram = new Histogram(Collections.emptyList(), integerMcvs);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(100)
                        .setNullsFraction(0.0).setAverageRowSize(4)
                        .setDistinctValuesCount(100)
                        .setHistogram(intHistogram)
                        .build())
                .addColumnStatistic(col2, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(50)
                        .setNullsFraction(0.0).setAverageRowSize(4)
                        .setDistinctValuesCount(50)
                        .setHistogram(intHistogram)
                        .build())
                .build();

        // col1 > 50 AND col2 > 25 — these predicates fall through to the default visitor which returns
        // col1/col2 stats (with integer MCVs). The booleanOnly guard should reject those MCVs.
        BinaryPredicateOperator left = new BinaryPredicateOperator(
                BinaryType.GT, col1, ConstantOperator.createInt(50));
        BinaryPredicateOperator right = new BinaryPredicateOperator(
                BinaryType.GT, col2, ConstantOperator.createInt(25));
        CompoundPredicateOperator andOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, left, right);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(andOp, statistics);

        Assertions.assertEquals(0.0, stat.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, stat.getMaxValue(), 0.001);
        Assertions.assertNotNull(stat.getHistogram());

        Map<String, Long> resultMcv = stat.getHistogram().getMCV();
        Assertions.assertNotNull(resultMcv);
        Assertions.assertTrue(resultMcv.keySet().stream().allMatch(k -> k.equals("0") || k.equals("1")));

        assertBooleanDistribution(stat, 250L, 750L, 0.0);
    }

    @Test
    public void testNestedCompoundPredicate() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, IntegerType.INT, "col1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(1, IntegerType.INT, "col2", true);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(col1, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(100)
                        .setNullsFraction(0.0).setAverageRowSize(4)
                        .setDistinctValuesCount(100).build())
                .addColumnStatistic(col2, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(50)
                        .setNullsFraction(0.0).setAverageRowSize(4)
                        .setDistinctValuesCount(50).build())
                .build();

        // (col1 > 50) OR (NOT (col2 > 25))
        BinaryPredicateOperator pred1 = new BinaryPredicateOperator(
                BinaryType.GT, col1, ConstantOperator.createInt(50));
        BinaryPredicateOperator pred2 = new BinaryPredicateOperator(
                BinaryType.GT, col2, ConstantOperator.createInt(25));
        CompoundPredicateOperator notPred2 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, pred2);
        CompoundPredicateOperator orOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, pred1, notPred2);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(orOp, statistics);

        Assertions.assertEquals(0.0, stat.getMinValue(), 0.001);
        Assertions.assertEquals(1.0, stat.getMaxValue(), 0.001);
        Assertions.assertNotNull(stat.getHistogram());
        assertBooleanDistribution(stat, 750, 250, 0.0);
    }

    @Test
    public void testNonBooleanZeroOneColumnDoesNotUseBooleanMcvFastPath() {
        final var col = new ColumnRefOperator(0, IntegerType.INT, "flag", true);
        final var hist = new Histogram(List.of(), Map.of("0", 300L, "1", 700L));
        final var stats = Statistics.builder()
                .setOutputRowCount(1_000)
                .addColumnStatistic(col, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(1).setNullsFraction(0)
                        .setAverageRowSize(4).setDistinctValuesCount(2)
                        .setHistogram(hist).build())
                .build();

        final var notCol = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, col);

        final var result = ExpressionStatisticCalculator.calculate(notCol, stats);

        Assertions.assertNotNull(result.getHistogram());
        Assertions.assertEquals(750L, result.getHistogram().getMCV().get("1"));
    }

    @Test
    public void testBooleanColumnStillUsesMcvPath() {
        final var col = new ColumnRefOperator(0, BooleanType.BOOLEAN, "b", true);
        final var hist = new Histogram(List.of(), Map.of("0", 300L, "1", 700L));
        final var stats = Statistics.builder()
                .setOutputRowCount(1_000)
                .addColumnStatistic(col, ColumnStatistic.builder()
                        .setMinValue(0).setMaxValue(1).setNullsFraction(0)
                        .setAverageRowSize(1).setDistinctValuesCount(2)
                        .setHistogram(hist).build())
                .build();

        final var notCol = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, col);

        final var result = ExpressionStatisticCalculator.calculate(notCol, stats);

        Assertions.assertNotNull(result.getHistogram());
        Assertions.assertEquals(300L, result.getHistogram().getMCV().get("1"));
    }

    @Test
    public void testBinaryPredicateWithUnknownOperandReturnsBasicStatsWithoutMcv() {
        final var col = new ColumnRefOperator(0, IntegerType.INT, "c", true);
        final var stats = Statistics.builder()
                .setOutputRowCount(1_000)
                .addColumnStatistic(col, ColumnStatistic.unknown())
                .build();

        final var eq = new BinaryPredicateOperator(BinaryType.EQ, col, ConstantOperator.createInt(5));

        final var result = ExpressionStatisticCalculator.calculate(eq, stats);

        // Unknown operand => the true/false split would only be a default-selectivity guess, so we keep just the
        // basic boolean shape and never materialize an MCV histogram.
        Assertions.assertNull(result.getHistogram());
        Assertions.assertEquals(2, result.getDistinctValuesCount(), 0.0);
        Assertions.assertEquals(0, result.getMinValue(), 0.0);
        Assertions.assertEquals(1, result.getMaxValue(), 0.0);
    }

    @Test
    public void testCompoundPredicateOrIgnoresNonBooleanChildMcvs() {
        ColumnRefOperator col1 = new ColumnRefOperator(0, IntegerType.INT, "flag1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(1, IntegerType.INT, "flag2", true);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1600)
                .addColumnStatistic(col1, nonBooleanZeroOneColumnStatistic(Map.of("1", 1600L)))
                .addColumnStatistic(col2, nonBooleanZeroOneColumnStatistic(Map.of("1", 1600L)))
                .build();

        CompoundPredicateOperator orOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, col1, col2);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(orOp, statistics);

        // pTrue = 0.25 + 0.25 - 0.25*0.25 = 0.4375 (NOT 1.0 from the ignored MCVs) => 700 true / 900 false.
        Assertions.assertNotNull(stat.getHistogram());
        assertOnlyBooleanMcvs(stat, 1600);
        assertBooleanDistribution(stat, 700L, 900L, 0.0);
    }

    @Test
    public void testCompoundPredicateUsesBooleanChildMcvButIgnoresNonBooleanChildMcv() {
        ColumnRefOperator boolCol = new ColumnRefOperator(0, BooleanType.BOOLEAN, "b", true);
        ColumnRefOperator intCol = new ColumnRefOperator(1, IntegerType.INT, "flag", true);

        Statistics statistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(boolCol, booleanColumnStatistic(700, 300, 0))
                .addColumnStatistic(intCol, nonBooleanZeroOneColumnStatistic(Map.of("1", 1000L)))
                .build();

        CompoundPredicateOperator andOp = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, boolCol, intCol);

        ColumnStatistic stat = ExpressionStatisticCalculator.calculate(andOp, statistics);

        Assertions.assertNotNull(stat.getHistogram());
        assertOnlyBooleanMcvs(stat, 1000);
        assertBooleanDistribution(stat, 175L, 825L, 0.0);
    }

}
