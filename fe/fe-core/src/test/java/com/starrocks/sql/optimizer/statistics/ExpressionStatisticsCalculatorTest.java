// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class ExpressionStatisticsCalculatorTest {
    @Test
    public void testVariableReference() {
        Statistics.Builder builder = Statistics.builder();
        double min = 0.0;
        double max = 100.0;
        double distinctValue = 100;
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.DATE, "id_date", true);
        Statistics statistics = builder.addColumnStatistic(columnRefOperator,
                        ColumnStatistic.builder().setMinValue(min).setMaxValue(max).
                                setDistinctValuesCount(distinctValue).setNullsFraction(0).setAverageRowSize(10).build())
                .build();
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(columnRefOperator, statistics);
        Assert.assertEquals(columnStatistic.getMaxValue(), max, 0.0001);
        Assert.assertEquals(columnStatistic.getMinValue(), min, 0.0001);
        Assert.assertEquals(columnStatistic.getDistinctValuesCount(), distinctValue, 0.001);
    }

    @Test
    public void testConstant() {
        ConstantOperator constantOperator = ConstantOperator.createBigint(100);
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(constantOperator, null);
        Assert.assertEquals(columnStatistic.getMinValue(), 100, 0.001);
        Assert.assertEquals(columnStatistic.getMaxValue(), 100, 0.001);

        ConstantOperator constantOperator1 = ConstantOperator.createDate(LocalDateTime.of(2021, 1, 1, 0, 0, 0));
        ColumnStatistic columnStatistic1 = ExpressionStatisticCalculator.calculate(constantOperator1, null);
        Assert.assertEquals(columnStatistic1.getMaxValue(), getLongFromDateTime(constantOperator1.getDatetime()),
                0.001);

        ConstantOperator constantOperator2 = ConstantOperator.createChar("123");
        ColumnStatistic columnStatistic2 = ExpressionStatisticCalculator.calculate(constantOperator2, null);
        Assert.assertTrue(columnStatistic2.isInfiniteRange());
        Assert.assertEquals(columnStatistic2.getDistinctValuesCount(), 1, 0.001);
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
        Assert.assertEquals(columnStatistic.getMaxValue(), max, 0.001);
        Assert.assertEquals(columnStatistic.getMinValue(), min, 0.001);
        // test min function
        callOperator = new CallOperator(FunctionSet.MIN, Type.INT, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics);
        Assert.assertEquals(columnStatistic.getMaxValue(), max, 0.001);
        Assert.assertEquals(columnStatistic.getMinValue(), min, 0.001);
        // test count function
        callOperator = new CallOperator(FunctionSet.COUNT, Type.INT, Lists.newArrayList(columnRefOperator));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, statistics, 10);
        Assert.assertEquals(columnStatistic.getMaxValue(), statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(columnStatistic.getMinValue(), 0.0, 0.001);
        Assert.assertEquals(columnStatistic.getDistinctValuesCount(), 10, 0.001);
    }

    @Test
    public void testBinaryFunctionCall() {
        ColumnRefOperator left = new ColumnRefOperator(0, Type.INT, "left", true);
        ColumnRefOperator right = new ColumnRefOperator(1, Type.INT, "right", true);
        Statistics.Builder builder = Statistics.builder();
        ColumnStatistic leftStatistic = new ColumnStatistic(-100, 100, 0, 0, 100);
        ColumnStatistic rightStatistic = new ColumnStatistic(100, 200, 0, 0, 100);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);

        CallOperator callOperator = new CallOperator(FunctionSet.ADD, Type.BIGINT, Lists.newArrayList(left, right));
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assert.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(300, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.SUBTRACT, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assert.assertEquals(-300, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(0, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.MULTIPLY, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assert.assertEquals(-20000, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(20000, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.DIVIDE, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assert.assertEquals(-1, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(1, columnStatistic.getMaxValue(), 0.001);
        // test multiply/divide column rang is negative
        builder = Statistics.builder();
        leftStatistic = new ColumnStatistic(-100, -10, 0, 0, 20);
        rightStatistic = new ColumnStatistic(-2, 0, 0, 0, 1);
        builder.addColumnStatistic(left, leftStatistic);
        builder.addColumnStatistic(right, rightStatistic);
        callOperator = new CallOperator(FunctionSet.MULTIPLY, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assert.assertEquals(0, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(200, columnStatistic.getMaxValue(), 0.001);

        callOperator = new CallOperator(FunctionSet.DIVIDE, Type.BIGINT, Lists.newArrayList(left, right));
        columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assert.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(50, columnStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testCastOperator() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.INT, "id", true);
        CastOperator callOperator = new CastOperator(Type.VARCHAR, columnRefOperator);

        Statistics.Builder builder = Statistics.builder();
        builder.addColumnStatistic(columnRefOperator, new ColumnStatistic(-100, 100, 0, 0, 100));

        ColumnStatistic columnStatistic = ExpressionStatisticCalculator.calculate(callOperator, builder.build());
        Assert.assertEquals(-100, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(100, columnStatistic.getMaxValue(), 0.001);
    }

    @Test
    public void testCaseWhenOperator() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, Type.INT, "", true);
        BinaryPredicateOperator whenOperator1 =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(1));
        ConstantOperator constantOperator1 = ConstantOperator.createChar("1");
        BinaryPredicateOperator whenOperator2 =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, columnRefOperator,
                        ConstantOperator.createInt(2));
        ConstantOperator constantOperator2 = ConstantOperator.createChar("2");

        CaseWhenOperator caseWhenOperator =
                new CaseWhenOperator(Type.VARCHAR, null, ConstantOperator.createChar("others", Type.VARCHAR),
                        ImmutableList.of(whenOperator1, constantOperator1, whenOperator2, constantOperator2));
        ColumnStatistic columnStatistic = ExpressionStatisticCalculator
                .calculate(caseWhenOperator, Statistics.builder().setOutputRowCount(100).build());
        Assert.assertEquals(columnStatistic.getDistinctValuesCount(), 3, 0.001);
    }
}
