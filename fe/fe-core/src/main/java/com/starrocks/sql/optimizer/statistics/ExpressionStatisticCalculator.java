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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;

public class ExpressionStatisticCalculator {
    private static final Logger LOG = LogManager.getLogger(ExpressionStatisticCalculator.class);

    public static final long DAYS_FROM_0_TO_1970 = 719528;
    public static final long DAYS_FROM_0_TO_9999 = 3652424;

    public static ColumnStatistic calculate(ScalarOperator operator, Statistics input) {
        return calculate(operator, input, input != null ? input.getOutputRowCount() : 0);
    }

    public static ColumnStatistic calculate(ScalarOperator operator, Statistics input, double rowCount) {
        return operator.accept(new ExpressionStatisticVisitor(input, rowCount), null);
    }

    private static class ExpressionStatisticVisitor extends ScalarOperatorVisitor<ColumnStatistic, Void> {
        private final Statistics inputStatistics;
        // Some functions estimate need plan node row count, such as COUNT
        private final double rowCount;

        public ExpressionStatisticVisitor(Statistics statistics, double rowCount) {
            this.inputStatistics = statistics;
            this.rowCount = Math.max(1.0, rowCount);
        }

        @Override
        public ColumnStatistic visit(ScalarOperator operator, Void context) {
            if (operator.getChildren().size() > 1) {
                return operator.getChild(0).accept(this, context);
            } else {
                return ColumnStatistic.unknown();
            }
        }

        @Override
        public ColumnStatistic visitVariableReference(ColumnRefOperator operator, Void context) {
            return inputStatistics.getColumnStatistic(operator);
        }

        @Override
        public ColumnStatistic visitConstant(ConstantOperator operator, Void context) {
            if (operator.isNull()) {
                return new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 1, 1);
            }
            OptionalDouble value = ConstantOperatorUtils.doubleValueFromConstant(operator);
            if (value.isPresent()) {
                return new ColumnStatistic(value.getAsDouble(), value.getAsDouble(), 0,
                        operator.getType().getTypeSize(), 1);
            } else if (operator.getType().isStringType()) {
                return new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 1, 1);
            } else {
                return ColumnStatistic.unknown();
            }
        }

        @Override
        public ColumnStatistic visitCaseWhenOperator(CaseWhenOperator caseWhenOperator, Void context) {
            // 1. compute children column statistics
            int whenClauseSize = caseWhenOperator.getWhenClauseSize();
            List<ColumnStatistic> childrenColumnStatistics = Lists.newArrayList();
            for (int i = 0; i < whenClauseSize; ++i) {
                childrenColumnStatistics.add(caseWhenOperator.getThenClause(i).accept(this, context));
            }
            if (caseWhenOperator.hasElse()) {
                childrenColumnStatistics.add(caseWhenOperator.getElseClause().accept(this, context));
            }
            // 2. use sum of then clause and else clause's distinct values as column distinctValues
            double distinctValues = childrenColumnStatistics.stream().mapToDouble(
                    ColumnStatistic::getDistinctValuesCount).sum();
            return ColumnStatistic.builder()
                    .setMinValue(Double.NEGATIVE_INFINITY)
                    .setMaxValue(Double.POSITIVE_INFINITY)
                    .setNullsFraction(0)
                    .setAverageRowSize(caseWhenOperator.getType().getTypeSize())
                    .setDistinctValuesCount(distinctValues)
                    .build();
        }

        @Override
        public ColumnStatistic visitCastOperator(CastOperator cast, Void context) {
            ColumnStatistic childStatistic = cast.getChild(0).accept(this, context);

            if (childStatistic.isUnknown() ||
                    inputStatistics.getColumnStatistics().values().stream().allMatch(ColumnStatistic::isUnknown)) {
                return ColumnStatistic.unknown();
            }

            if (cast.getType().isNumericType()) {
                return ColumnStatistic.buildFrom(childStatistic).setAverageRowSize(cast.getType().getTypeSize())
                        .build();
            } else if (!cast.getType().isDateType()) {
                // If cast destination type is number/string, it's unnecessary to cast, keep self is enough.
                return childStatistic;
            }

            ConstantOperator max = ConstantOperator.createBigint((long) childStatistic.getMaxValue());
            ConstantOperator min = ConstantOperator.createBigint((long) childStatistic.getMinValue());

            try {
                if (cast.getChild(0).getType().isDateType()) {
                    max = ConstantOperator.createDatetime(
                            Utils.getDatetimeFromLong((long) childStatistic.getMaxValue()));
                    min = ConstantOperator.createDatetime(
                            Utils.getDatetimeFromLong((long) childStatistic.getMinValue()));
                } else {
                    max = max.castTo(cast.getType());
                    min = min.castTo(cast.getType());
                }
            } catch (Exception e) {
                LOG.debug("expression statistic compute cast failed: " + max.toString() + ", " + min +
                        ", to type: " + cast.getType());
                return childStatistic;
            }

            double maxValue = Utils.getLongFromDateTime(max.getDatetime());
            double minValue = Utils.getLongFromDateTime(min.getDatetime());

            return ColumnStatistic.builder()
                    .setMinValue(minValue)
                    .setMaxValue(maxValue)
                    .setNullsFraction(childStatistic.getNullsFraction())
                    .setAverageRowSize(childStatistic.getAverageRowSize())
                    .setDistinctValuesCount(childStatistic.getDistinctValuesCount())
                    .build();
        }

        @Override
        public ColumnStatistic visitCall(CallOperator call, Void context) {
            List<ColumnStatistic> childrenColumnStatistics =
                    call.getChildren().stream().map(child -> child.accept(this, context)).collect(Collectors.toList());
            Preconditions.checkState(childrenColumnStatistics.size() == call.getChildren().size(),
                    "column statistics missing for expr: %s. column statistics: %s",
                    call, childrenColumnStatistics);
            if (childrenColumnStatistics.stream().anyMatch(ColumnStatistic::isUnknown) ||
                    inputStatistics.getColumnStatistics().values().stream().allMatch(ColumnStatistic::isUnknown)) {
                return ColumnStatistic.unknown();
            }

            if (call.getChildren().size() == 0) {
                return nullaryExpressionCalculate(call);
            } else if (call.getChildren().size() == 1) {
                return unaryExpressionCalculate(call, childrenColumnStatistics.get(0));
            } else if (call.getChildren().size() == 2) {
                return binaryExpressionCalculate(call, childrenColumnStatistics.get(0),
                        childrenColumnStatistics.get(1));
            } else {
                return multiaryExpressionCalculate(call, childrenColumnStatistics);
            }
        }

        private ColumnStatistic nullaryExpressionCalculate(CallOperator callOperator) {
            double minValue;
            double maxValue;
            double distinctValue = rowCount;
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.COUNT:
                    minValue = 0;
                    maxValue = inputStatistics.getOutputRowCount();
                    break;
                case FunctionSet.RAND:
                case FunctionSet.RANDOM:
                    minValue = 0;
                    maxValue = 1;
                    break;
                case FunctionSet.E:
                    minValue = Math.E;
                    maxValue = Math.E;
                    distinctValue = 1;
                    break;
                case FunctionSet.PI:
                    minValue = Math.PI;
                    maxValue = Math.PI;
                    distinctValue = 1;
                    break;
                case FunctionSet.CURDATE:
                    minValue = LocalDate.now().atStartOfDay().atZone(ZoneId.systemDefault()).toEpochSecond();
                    maxValue = minValue;
                    distinctValue = 1;
                    break;
                case FunctionSet.CURTIME:
                case FunctionSet.CURRENT_TIME: {
                    LocalDateTime now = LocalDateTime.now();
                    minValue = now.getHour() * 3600D + now.getMinute() * 60D + now.getSecond();
                    maxValue = minValue;
                    distinctValue = 1;
                    break;
                }
                case FunctionSet.NOW:
                case FunctionSet.CURRENT_TIMESTAMP:
                case FunctionSet.UNIX_TIMESTAMP:
                    minValue = LocalDateTime.now().atZone(ZoneId.systemDefault()).toEpochSecond();
                    maxValue = minValue;
                    distinctValue = 1;
                    break;
                default:
                    return ColumnStatistic.unknown();
            }
            return ColumnStatistic.builder()
                    .setMinValue(minValue)
                    .setMaxValue(maxValue)
                    .setNullsFraction(0)
                    .setAverageRowSize(callOperator.getType().getTypeSize())
                    .setDistinctValuesCount(distinctValue)
                    .build();
        }

        private ColumnStatistic unaryExpressionCalculate(CallOperator callOperator, ColumnStatistic columnStatistic) {
            double minValue = columnStatistic.getMinValue();
            double maxValue = columnStatistic.getMaxValue();
            double distinctValue = Math.min(rowCount, columnStatistic.getDistinctValuesCount());
            final boolean minMaxValueInfinite = Double.isInfinite(minValue) || Double.isInfinite(maxValue);
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.SIGN:
                    minValue = -1;
                    maxValue = 1;
                    distinctValue = 3;
                    break;
                case FunctionSet.GREATEST:
                case FunctionSet.LEAST:
                case FunctionSet.MAX:
                case FunctionSet.MIN:
                case FunctionSet.ANY_VALUE:
                case FunctionSet.AVG:
                    maxValue = columnStatistic.getMaxValue();
                    minValue = columnStatistic.getMinValue();
                    break;
                case FunctionSet.SUM:
                    minValue = columnStatistic.getMinValue() > 0 ? columnStatistic.getMinValue() :
                            columnStatistic.getMinValue() * rowCount / distinctValue;
                    maxValue = columnStatistic.getMaxValue() < 0 ? columnStatistic.getMaxValue() :
                            columnStatistic.getMaxValue() * rowCount / distinctValue;
                    break;
                case FunctionSet.COUNT:
                    minValue = 0;
                    maxValue = inputStatistics.getOutputRowCount();
                    distinctValue = rowCount;
                    break;
                case FunctionSet.MULTI_DISTINCT_COUNT:
                    minValue = 0;
                    maxValue = columnStatistic.getDistinctValuesCount();
                    distinctValue = rowCount;
                    break;
                case FunctionSet.ASCII:
                    minValue = 0;
                    maxValue = 127;
                    distinctValue = 128;
                    break;
                case FunctionSet.YEAR:
                    minValue = 1700;
                    maxValue = 2100;
                    try {
                        minValue = Utils.getDatetimeFromLong((long) columnStatistic.getMinValue()).getYear();
                        maxValue = Utils.getDatetimeFromLong((long) columnStatistic.getMaxValue()).getYear();
                    } catch (DateTimeException e) {
                        LOG.debug("get date type column statistics min/max failed. " + e);
                    }
                    distinctValue =
                            Math.min(columnStatistic.getDistinctValuesCount(), (maxValue - minValue + 1));
                    break;
                case FunctionSet.QUARTER:
                    minValue = 1;
                    maxValue = 4;
                    distinctValue = 4;
                    break;
                case FunctionSet.MONTH:
                    minValue = 1;
                    maxValue = 12;
                    distinctValue = 12;
                    break;
                case FunctionSet.WEEKOFYEAR:
                    minValue = 1;
                    maxValue = 54;
                    distinctValue = 54;
                    break;
                case FunctionSet.DAY:
                case FunctionSet.DAYOFMONTH:
                    minValue = 1;
                    maxValue = 31;
                    distinctValue = 31;
                    break;
                case FunctionSet.DAYOFWEEK:
                    minValue = 1;
                    maxValue = 7;
                    distinctValue = 7;
                    break;
                case FunctionSet.DAYOFYEAR:
                    minValue = 1;
                    maxValue = 366;
                    distinctValue = 366;
                    break;
                case FunctionSet.HOUR:
                    minValue = 0;
                    maxValue = 23;
                    distinctValue = 24;
                    break;
                case FunctionSet.MINUTE:
                case FunctionSet.SECOND:
                    minValue = 0;
                    maxValue = 59;
                    distinctValue = 60;
                    break;
                case FunctionSet.TO_DATE:
                    if (minMaxValueInfinite) {
                        break;
                    }
                    minValue = Utils.getDatetimeFromLong((long) minValue).toLocalDate()
                            .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                    maxValue = Utils.getDatetimeFromLong((long) maxValue).toLocalDate()
                            .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                    break;
                case FunctionSet.TO_DAYS:
                    if (minMaxValueInfinite) {
                        break;
                    }
                    minValue = Utils.getDatetimeFromLong((long) minValue).toLocalDate().toEpochDay() +
                            (double) DAYS_FROM_0_TO_1970;
                    maxValue = Utils.getDatetimeFromLong((long) maxValue).toLocalDate().toEpochDay() +
                            (double) DAYS_FROM_0_TO_1970;
                    break;
                case FunctionSet.FROM_DAYS:
                    if (minValue < DAYS_FROM_0_TO_1970) {
                        minValue = LocalDate.ofEpochDay(0).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                    } else {
                        if (minValue > DAYS_FROM_0_TO_9999) {
                            minValue = LocalDate.ofEpochDay(DAYS_FROM_0_TO_9999 - DAYS_FROM_0_TO_1970)
                                    .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                        } else {
                            minValue = LocalDate.ofEpochDay((long) (minValue - DAYS_FROM_0_TO_1970))
                                    .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                        }
                    }

                    if (maxValue < DAYS_FROM_0_TO_1970) {
                        maxValue = LocalDate.ofEpochDay(0).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                    } else {
                        if (maxValue > DAYS_FROM_0_TO_9999) {
                            maxValue = LocalDate.ofEpochDay(DAYS_FROM_0_TO_9999 - DAYS_FROM_0_TO_1970)
                                    .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                        } else {
                            maxValue = LocalDate.ofEpochDay((long) (maxValue - DAYS_FROM_0_TO_1970))
                                    .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                        }
                    }
                    break;
                case FunctionSet.TIMESTAMP:
                    break;
                case FunctionSet.ABS:
                    double absMinValue;
                    double absMaxValue = Math.max(Math.abs(minValue), Math.abs(maxValue));
                    if (minValue < 0 && maxValue < 0 || minValue >= 0 && maxValue >= 0) {
                        absMinValue = Math.min(Math.abs(minValue), Math.abs(maxValue));
                    } else {
                        absMinValue = 0;
                    }
                    minValue = absMinValue;
                    maxValue = absMaxValue;
                    distinctValue = maxValue - minValue + 1;
                    break;
                case FunctionSet.ACOS:
                case FunctionSet.ASIN:
                    minValue = 0;
                    maxValue = Math.PI;
                    break;
                case FunctionSet.ATAN:
                case FunctionSet.ATAN2:
                    minValue = -Math.PI / 2;
                    maxValue = Math.PI / 2;
                    break;
                case FunctionSet.SIN:
                case FunctionSet.COS:
                    minValue = -1;
                    maxValue = 1;
                    break;
                case FunctionSet.SQRT:
                    minValue = 0;
                    if (maxValue < 0) {
                        return ColumnStatistic.unknown();
                    }
                    maxValue = Math.sqrt(maxValue);
                    break;
                case FunctionSet.SQUARE:
                    double squareMinValue;
                    double squareMaxValue = Math.max(minValue * minValue, maxValue * maxValue);
                    if (minValue < 0 && maxValue < 0 || minValue >= 0 && maxValue >= 0) {
                        squareMinValue = Math.min(minValue * minValue, maxValue * maxValue);
                    } else {
                        squareMinValue = 0;
                    }
                    minValue = squareMinValue;
                    maxValue = squareMaxValue;
                    break;
                case FunctionSet.RADIANS:
                    // π = 180°, so the ratio is 57.3
                    minValue = minValue / 57.3;
                    maxValue = maxValue / 57.3;
                    break;
                case FunctionSet.RAND:
                case FunctionSet.RANDOM:
                    minValue = 0;
                    maxValue = 1;
                    break;
                case FunctionSet.NEGATIVE:
                    double negativeMinValue = -minValue;
                    double negativeMaxValue = -maxValue;
                    minValue = Math.min(negativeMinValue, negativeMaxValue);
                    maxValue = Math.max(negativeMinValue, negativeMaxValue);
                    break;
                case FunctionSet.MURMUR_HASH3_32:
                    // murmur_hash3_32's range is uint32_t, so 0 ~ 4294967295
                    minValue = 0;
                    maxValue = 4294967295.0;
                    distinctValue = rowCount;
                    break;
                case FunctionSet.POSITIVE:
                case FunctionSet.FLOOR:
                case FunctionSet.DFLOOR:
                case FunctionSet.CEIL:
                case FunctionSet.CEILING:
                case FunctionSet.ROUND:
                case FunctionSet.DROUND:
                case FunctionSet.TRUNCATE:
                    // Just use the input's statistics as output's statistics
                    break;
                case FunctionSet.TO_BITMAP:
                    minValue = Double.NEGATIVE_INFINITY;
                    maxValue = Double.POSITIVE_INFINITY;
                    break;
                default:
                    return ColumnStatistic.unknown();
            }

            final double averageRowSize;
            if (callOperator.getType().isIntegerType() || callOperator.getType().isFloatingPointType()
                    || callOperator.getType().isDateType() || callOperator.getType().isBitmapType()) {
                averageRowSize = callOperator.getType().getTypeSize();
            } else {
                averageRowSize = columnStatistic.getAverageRowSize();
            }

            return ColumnStatistic.builder()
                    .setMinValue(minValue)
                    .setMaxValue(maxValue)
                    .setNullsFraction(columnStatistic.getNullsFraction())
                    .setAverageRowSize(averageRowSize)
                    .setDistinctValuesCount(distinctValue)
                    .build();
        }

        private ColumnStatistic binaryExpressionCalculate(CallOperator callOperator, ColumnStatistic left,
                                                          ColumnStatistic right) {
            final double minValue;
            final double maxValue;
            double nullsFraction = 1 - ((1 - left.getNullsFraction()) * (1 - right.getNullsFraction()));
            double distinctValues = Math.max(left.getDistinctValuesCount(), right.getDistinctValuesCount());
            double averageRowSize = callOperator.getType().getTypeSize();
            long interval;
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.ADD:
                case FunctionSet.DATE_ADD:
                    minValue = left.getMinValue() + right.getMinValue();
                    maxValue = left.getMaxValue() + right.getMaxValue();
                    break;
                case FunctionSet.SUBTRACT:
                case FunctionSet.TIMEDIFF:
                case FunctionSet.DATE_SUB:
                    minValue = left.getMinValue() - right.getMaxValue();
                    maxValue = left.getMaxValue() - right.getMinValue();
                    break;
                case FunctionSet.YEARS_DIFF:
                    interval = 3600L * 24L * 365L;
                    minValue = (left.getMinValue() - right.getMaxValue()) / interval;
                    maxValue = (left.getMaxValue() - right.getMinValue()) / interval;
                    break;
                case FunctionSet.MONTHS_DIFF:
                    interval = 3600L * 24L * 31L;
                    minValue = (left.getMinValue() - right.getMaxValue()) / interval;
                    maxValue = (left.getMaxValue() - right.getMinValue()) / interval;
                    break;
                case FunctionSet.WEEKS_DIFF:
                    interval = 3600L * 24L * 7L;
                    minValue = (left.getMinValue() - right.getMaxValue()) / interval;
                    maxValue = (left.getMaxValue() - right.getMinValue()) / interval;
                    break;
                case FunctionSet.DAYS_DIFF:
                case FunctionSet.DATEDIFF:
                    interval = 3600L * 24L;
                    minValue = (left.getMinValue() - right.getMaxValue()) / interval;
                    maxValue = (left.getMaxValue() - right.getMinValue()) / interval;
                    break;
                case FunctionSet.HOURS_DIFF:
                    interval = 3600;
                    minValue = (left.getMinValue() - right.getMaxValue()) / interval;
                    maxValue = (left.getMaxValue() - right.getMinValue()) / interval;
                    break;
                case FunctionSet.MINUTES_DIFF:
                    interval = 60;
                    minValue = (left.getMinValue() - right.getMaxValue()) / interval;
                    maxValue = (left.getMaxValue() - right.getMinValue()) / interval;
                    break;
                case FunctionSet.SECONDS_DIFF:
                    minValue = left.getMinValue() - right.getMaxValue();
                    maxValue = left.getMaxValue() - right.getMinValue();
                    break;
                case FunctionSet.MULTIPLY:
                    minValue = Math.min(Math.min(
                            Math.min(left.getMinValue() * right.getMinValue(),
                                    left.getMinValue() * right.getMaxValue()),
                            left.getMaxValue() * right.getMinValue()), left.getMaxValue() * right.getMaxValue());
                    maxValue = Math.max(Math.max(
                            Math.max(left.getMinValue() * right.getMinValue(),
                                    left.getMinValue() * right.getMaxValue()),
                            left.getMaxValue() * right.getMinValue()), left.getMaxValue() * right.getMaxValue());
                    break;
                case FunctionSet.DIVIDE:
                    minValue = Math.min(Math.min(
                                    Math.min(left.getMinValue() / divisorNotZero(right.getMinValue()),
                                            left.getMinValue() / divisorNotZero(right.getMaxValue())),
                                    left.getMaxValue() / divisorNotZero(right.getMinValue())),
                            left.getMaxValue() / divisorNotZero(right.getMaxValue()));
                    maxValue = Math.max(Math.max(
                                    Math.max(left.getMinValue() / divisorNotZero(right.getMinValue()),
                                            left.getMinValue() / divisorNotZero(right.getMaxValue())),
                                    left.getMaxValue() / divisorNotZero(right.getMinValue())),
                            left.getMaxValue() / divisorNotZero(right.getMaxValue()));
                    break;
                case FunctionSet.MOD:
                case FunctionSet.FMOD:
                case FunctionSet.PMOD:
                    minValue = -Math.max(Math.abs(right.getMinValue()), Math.abs(right.getMaxValue()));
                    maxValue = -minValue;
                    distinctValues = callOperator.getType().isIntegerType() ? maxValue - minValue : distinctValues;
                    break;
                case FunctionSet.IFNULL:
                    minValue = Math.min(left.getMinValue(), right.getMinValue());
                    maxValue = Math.max(left.getMaxValue(), right.getMaxValue());
                    break;
                case FunctionSet.NULLIF:
                    minValue = left.getMinValue();
                    maxValue = left.getMaxValue();
                    break;
                default:
                    return ColumnStatistic.unknown();
            }
            return ColumnStatistic.builder()
                    .setMinValue(minValue)
                    .setMaxValue(maxValue)
                    .setNullsFraction(nullsFraction)
                    .setAverageRowSize(averageRowSize)
                    .setDistinctValuesCount(distinctValues)
                    .build();
        }

        private ColumnStatistic multiaryExpressionCalculate(CallOperator callOperator,
                                                            List<ColumnStatistic> childColumnStatisticList) {
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.IF:
                    double distinctValues = childColumnStatisticList.get(1).getDistinctValuesCount() +
                            childColumnStatisticList.get(2).getDistinctValuesCount();
                    return new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0,
                            callOperator.getType().getTypeSize(), distinctValues);
                // use child column statistics for now
                case FunctionSet.SUBSTRING:
                    return childColumnStatisticList.get(0);
                default:
                    return ColumnStatistic.unknown();
            }
        }

        private double divisorNotZero(double value) {
            return value == 0 ? 1.0 : value;
        }
    }
}
