// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.DateTimeException;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.Utils.getDatetimeFromLong;
import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class ExpressionStatisticCalculator {
    private static final Logger LOG = LogManager.getLogger(ExpressionStatisticCalculator.class);

    public static ColumnStatistic calculate(ScalarOperator operator, Statistics input) {
        return operator.accept(new ExpressionStatisticVisitor(input), null);
    }

    private static class ExpressionStatisticVisitor extends ScalarOperatorVisitor<ColumnStatistic, Void> {
        private final Statistics statistics;

        public ExpressionStatisticVisitor(Statistics statistics) {
            this.statistics = statistics;
        }

        @Override
        public ColumnStatistic visit(ScalarOperator operator, Void context) {
            return operator.getChild(0).accept(this, context);
        }

        @Override
        public ColumnStatistic visitVariableReference(ColumnRefOperator operator, Void context) {
            return statistics.getColumnStatistic(operator);
        }

        @Override
        public ColumnStatistic visitConstant(ConstantOperator operator, Void context) {
            OptionalDouble value = doubleValueFromConstant(operator);
            if (value.isPresent()) {
                return new ColumnStatistic(value.getAsDouble(), value.getAsDouble(), 0,
                        operator.getType().getSlotSize(), 1);
            } else {
                return ColumnStatistic.unknown();
            }
        }

        @Override
        public ColumnStatistic visitCall(CallOperator call, Void context) {
            List<ColumnStatistic> childrenColumnStatistics =
                    call.getChildren().stream().map(child -> child.accept(this, context)).collect(Collectors.toList());
            Preconditions.checkState(childrenColumnStatistics.size() == call.getChildren().size());
            if (childrenColumnStatistics.stream().anyMatch(ColumnStatistic::isUnknown)) {
                return ColumnStatistic.unknown();
            }

            if (call.getChildren().size() == 0) {
                return ColumnStatistic.unknown();
            } else if (call.getChildren().size() == 1) {
                return unaryExpressionCalculate(call, childrenColumnStatistics.get(0));
            } else if (call.getChildren().size() == 2) {
                return binaryExpressionCalculate(call, childrenColumnStatistics.get(0),
                        childrenColumnStatistics.get(1));
            } else {
                // TODO: Multiple Arithmetic calculations support later
                return childrenColumnStatistics.get(0);
            }
        }

        private ColumnStatistic unaryExpressionCalculate(CallOperator callOperator, ColumnStatistic columnStatistic) {
            double value;
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.MAX:
                    value = columnStatistic.getMaxValue();
                    return new ColumnStatistic(value, value, 0, callOperator.getType().getSlotSize(), 1);
                case FunctionSet.MIN:
                    value = columnStatistic.getMinValue();
                    return new ColumnStatistic(value, value, 0, callOperator.getType().getSlotSize(), 1);
                case FunctionSet.YEAR:
                    int minValue = 1000;
                    int maxValue = 3000;
                    try {
                        minValue = getDatetimeFromLong((long) columnStatistic.getMinValue()).getYear();
                        maxValue = getDatetimeFromLong((long) columnStatistic.getMaxValue()).getYear();
                    } catch (DateTimeException e) {
                        LOG.warn("get date type column statistics min/max failed. " + e);
                    }
                    return new ColumnStatistic(minValue, maxValue, 0,
                            callOperator.getType().getSlotSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), (maxValue - minValue + 1)));
                case FunctionSet.MONTH:
                    return new ColumnStatistic(1, 12, 0,
                            callOperator.getType().getSlotSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), 12));
                case FunctionSet.DAY:
                    return new ColumnStatistic(1, 31, 0,
                            callOperator.getType().getSlotSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), 31));
                case FunctionSet.HOUR:
                    return new ColumnStatistic(0, 23, 0,
                            callOperator.getType().getSlotSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), 24));
                case FunctionSet.MINUTE:
                case FunctionSet.SECOND:
                    return new ColumnStatistic(0, 59, 0,
                            callOperator.getType().getSlotSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), 60));
                default:
                    // return child column statistic default
                    return columnStatistic;
            }
        }

        private ColumnStatistic binaryExpressionCalculate(CallOperator callOperator, ColumnStatistic left,
                                                          ColumnStatistic right) {
            double distinctValues = Math.max(left.getDistinctValuesCount(), right.getMaxValue());
            double nullsFraction = 1 - ((1 - left.getNullsFraction()) * (1 - right.getNullsFraction()));
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.ADD:
                    return new ColumnStatistic(left.getMinValue() + right.getMinValue(),
                            left.getMaxValue() + right.getMaxValue(), nullsFraction,
                            callOperator.getType().getSlotSize(),
                            distinctValues);
                case FunctionSet.SUBTRACT:
                    return new ColumnStatistic(left.getMinValue() - right.getMaxValue(),
                            left.getMaxValue() - right.getMinValue(), nullsFraction,
                            callOperator.getType().getSlotSize(),
                            distinctValues);
                case FunctionSet.MULTIPLY:
                    double multiplyMinValue = Math.min(Math.min(
                            Math.min(left.getMinValue() * right.getMinValue(),
                                    left.getMinValue() * right.getMaxValue()),
                            left.getMaxValue() * right.getMinValue()), left.getMaxValue() * right.getMaxValue());
                    double multiplyMaxValue = Math.max(Math.max(
                            Math.max(left.getMinValue() * right.getMinValue(),
                                    left.getMinValue() * right.getMaxValue()),
                            left.getMaxValue() * right.getMinValue()), left.getMaxValue() * right.getMaxValue());
                    return new ColumnStatistic(multiplyMinValue, multiplyMaxValue, nullsFraction,
                            callOperator.getType().getSlotSize(), distinctValues);
                case FunctionSet.DIVIDE:
                    double divideMinValue = Math.min(Math.min(
                                    Math.min(left.getMinValue() / divisorNotZero(right.getMinValue()),
                                            left.getMinValue() / divisorNotZero(right.getMaxValue())),
                                    left.getMaxValue() / divisorNotZero(right.getMinValue())),
                            left.getMaxValue() / divisorNotZero(right.getMaxValue()));
                    double divideMaxValue = Math.max(Math.max(
                                    Math.max(left.getMinValue() / divisorNotZero(right.getMinValue()),
                                            left.getMinValue() / divisorNotZero(right.getMaxValue())),
                                    left.getMaxValue() / divisorNotZero(right.getMinValue())),
                            left.getMaxValue() / divisorNotZero(right.getMaxValue()));
                    return new ColumnStatistic(divideMinValue, divideMaxValue, nullsFraction,
                            callOperator.getType().getSlotSize(),
                            distinctValues);
                default:
                    // return child column statistic default
                    return left;
            }
        }

        private double divisorNotZero(double value) {
            return value == 0 ? 1.0 : value;
        }
    }

    private static OptionalDouble doubleValueFromConstant(ConstantOperator constantOperator) {
        if (Type.BOOLEAN.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getBoolean() ? 1.0 : 0.0);
        } else if (Type.TINYINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getTinyInt());
        } else if (Type.SMALLINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getSmallint());
        } else if (Type.INT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getInt());
        } else if (Type.BIGINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getBigint());
        } else if (Type.LARGEINT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getLargeInt().doubleValue());
        } else if (Type.FLOAT.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getFloat());
        } else if (Type.DOUBLE.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getDouble());
        } else if (Type.DATE.equals(constantOperator.getType())) {
            return OptionalDouble.of(getLongFromDateTime(constantOperator.getDate()));
        } else if (Type.DATETIME.equals(constantOperator.getType())) {
            return OptionalDouble.of(getLongFromDateTime(constantOperator.getDatetime()));
        } else if (Type.TIME.equals(constantOperator.getType())) {
            return OptionalDouble.of(constantOperator.getTime());
        } else if (constantOperator.getType().isDecimalOfAnyVersion()) {
            return OptionalDouble.of(constantOperator.getDecimal().doubleValue());
        }
        return OptionalDouble.empty();
    }
}
