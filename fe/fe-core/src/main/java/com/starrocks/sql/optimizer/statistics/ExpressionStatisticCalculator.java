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
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.spm.SPMFunctions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.IsoFields;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        if (Double.isNaN(rowCount)) {
            LOG.debug("found a NaN row count when calculating column statistic for expr: {}", operator);
            return ColumnStatistic.unknown();
        }
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
                return new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0,
                        operator.toString().length(), 1);
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
                    Optional<ConstantOperator> maxRes;
                    Optional<ConstantOperator> minRes;
                    maxRes = max.castTo(cast.getType());
                    minRes = max.castTo(cast.getType());
                    if (maxRes.isPresent() && minRes.isPresent()) {
                        max = maxRes.get();
                        min = minRes.get();
                    } else {
                        throw new IllegalArgumentException();
                    }
                }
            } catch (Exception e) {
                LOG.debug("expression statistic compute cast failed: max value: {}, min value: {}, to type {}",
                        max, min, cast.getType());
                return childStatistic;
            }

            double maxValue = Utils.getLongFromDateTime(max.getDatetime());
            double minValue = Utils.getLongFromDateTime(min.getDatetime());

            return ColumnStatistic.builder()
                    .setMinValue(minValue)
                    .setMaxValue(maxValue)
                    .setNullsFraction(childStatistic.getNullsFraction())
                    .setAverageRowSize(cast.getType().getTypeSize())
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
                return deriveBasicColStats(call);
            }

            if (SPMFunctions.isSPMFunctions(call)) {
                return SPMFunctions.getSPMFunctionStatistics(call, childrenColumnStatistics).get(0);
            } else if (call.getChildren().isEmpty()) {
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
                case FunctionSet.CONCAT:
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
                case FunctionSet.XX_HASH3_64:
                    // xx_hash3_64's range is int64_t
                    minValue = Long.MIN_VALUE;
                    maxValue = Long.MAX_VALUE;
                    distinctValue = rowCount;
                    break;
                case FunctionSet.XX_HASH3_128:
                    // xx_hash3_128's range is LARGE_INT
                    minValue = LargeIntLiteral.LARGE_INT_MIN.doubleValue();
                    maxValue = LargeIntLiteral.LARGE_INT_MAX.doubleValue();
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
                case FunctionSet.UPPER:
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
                averageRowSize = columnStatistic.isUnknown() ? callOperator.getType().getTypeSize() :
                        columnStatistic.getAverageRowSize();
            }

            return ColumnStatistic.builder()
                    .setMinValue(minValue)
                    .setMaxValue(maxValue)
                    .setNullsFraction(columnStatistic.getNullsFraction())
                    .setAverageRowSize(averageRowSize)
                    .setDistinctValuesCount(distinctValue)
                    .setHistogram(transformHistogramForUnary(callOperator, columnStatistic).orElse(null))
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
                case FunctionSet.DAYS_ADD:
                    minValue = left.getMinValue() + right.getMinValue();
                    maxValue = left.getMaxValue() + right.getMaxValue();
                    break;
                case FunctionSet.SUBTRACT:
                case FunctionSet.TIMEDIFF:
                case FunctionSet.DATE_SUB:
                case FunctionSet.DAYS_SUB:
                case FunctionSet.SECONDS_DIFF:
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
                case FunctionSet.WEEK:
                    minValue = 0;
                    maxValue = 53;
                    distinctValues = Math.min(calcDistinctValForWeek(left), distinctValues);
                    break;
                case FunctionSet.CONCAT:
                    minValue = Double.NEGATIVE_INFINITY;
                    maxValue = Double.POSITIVE_INFINITY;
                    distinctValues = Math.min(rowCount, left.getDistinctValuesCount() + right.getDistinctValuesCount());
                    averageRowSize = left.getAverageRowSize() + right.getAverageRowSize();
                    break;
                case FunctionSet.LEFT:
                case FunctionSet.RIGHT:
                    minValue = Double.NEGATIVE_INFINITY;
                    maxValue = Double.POSITIVE_INFINITY;
                    averageRowSize = Math.max(1, left.getAverageRowSize() - right.getAverageRowSize());
                    distinctValues = left.getDistinctValuesCount() * averageRowSize / left.getAverageRowSize();
                    break;
                case FunctionSet.LIKE:
                case FunctionSet.ILIKE:
                    minValue = 0;
                    maxValue = 1;
                    distinctValues = 2;
                    break;
                default:
                    return ColumnStatistic.unknown();
            }
            ColumnStatistic.Builder builder = ColumnStatistic.builder()
                    .setMinValue(minValue)
                    .setMaxValue(maxValue)
                    .setNullsFraction(nullsFraction)
                    .setAverageRowSize(averageRowSize)
                    .setDistinctValuesCount(distinctValues);
            transformHistogramForBinary(callOperator, left, right).ifPresent(builder::setHistogram);
            return builder.build();
        }

        private ColumnStatistic multiaryExpressionCalculate(CallOperator callOperator,
                                                            List<ColumnStatistic> childColumnStatisticList) {
            double distinctValues;
            double averageRowSize;
            double nullsFraction;
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.IF:
                    distinctValues = childColumnStatisticList.get(1).getDistinctValuesCount() +
                            childColumnStatisticList.get(2).getDistinctValuesCount();
                    double minValue = Math.min(childColumnStatisticList.get(1).getMinValue(),
                            childColumnStatisticList.get(2).getMinValue());
                    double maxValue = Math.max(childColumnStatisticList.get(1).getMaxValue(),
                            childColumnStatisticList.get(2).getMaxValue());
                    return new ColumnStatistic(minValue, maxValue, 0,
                            callOperator.getType().getTypeSize(), distinctValues);
                // use child column statistics for now
                case FunctionSet.SUBSTRING:
                case FunctionSet.REGEXP_REPLACE:
                    return childColumnStatisticList.get(0);
                case FunctionSet.CONCAT:
                    distinctValues = Math.min(rowCount,
                            childColumnStatisticList.stream().mapToDouble(ColumnStatistic::getDistinctValuesCount).sum());
                    averageRowSize = childColumnStatisticList.stream().mapToDouble(ColumnStatistic::getAverageRowSize).sum();
                    nullsFraction = 1 - childColumnStatisticList.stream().mapToDouble(ColumnStatistic::getNullsFraction)
                            .reduce(1.0, (accumulator, nullFraction) -> accumulator * (1 - nullFraction));
                    return new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                            nullsFraction, averageRowSize, distinctValues);
                default:
                    return ColumnStatistic.unknown();
            }
        }

        private double divisorNotZero(double value) {
            return value == 0 ? 1.0 : value;
        }

        private ColumnStatistic deriveBasicColStats(CallOperator call) {
            List<ColumnRefOperator> usedCols = call.getColumnRefs();
            if (usedCols.size() == 0) {
                return new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                        0, call.getType().getTypeSize(), 1);
            } else if (usedCols.size() == 1 && !inputStatistics.getColumnStatistic(usedCols.get(0)).isUnknown()) {
                ColumnStatistic usedStats = inputStatistics.getColumnStatistic(usedCols.get(0));
                return new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, usedStats.getNullsFraction(),
                        call.getType().getTypeSize(), usedStats.getDistinctValuesCount());
            } else {
                return ColumnStatistic.unknown();
            }
        }

        private double calcDistinctValForWeek(ColumnStatistic col) {
            if (col.hasNaNValue() || col.isInfiniteRange()) {
                return 54;
            }
            LocalDateTime min = Utils.getDatetimeFromLong((long) col.getMinValue());
            LocalDateTime max = Utils.getDatetimeFromLong((long) col.getMaxValue());

            // the range is more than one year
            if (min.plusYears(1).compareTo(max) <= 0) {
                return 54;
            } else if (min.getYear() < max.getYear()) {
                return (54 - min.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)) + max.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            } else {
                return max.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR) - min.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR) + 1;
            }
        }

        /**
         * Only do histogram/MCV propagation when the transformation is definitely correct for value domain.
         * <p>
         * NOTE: This code path must be semantics-safe because MCV keys may be used as exact constant values
         * (e.g. as IN-list values in skew join elimination v2). Therefore we only support a small whitelist of
         * transformations that we can prove to be correct. If you need to support more functions in the future,
         * add them here with strict guards and "fail closed" (return Optional.empty()) on any ambiguity.
         * <p>
         * Current supported cases:
         * - POSITIVE(x): identity
         * - NEGATIVE(x): exact -x, fixed-point only
         * - ABS(x): exact abs(x), fixed-point only (with collision merge)
         * <p>
         * For unsupported functions we return empty to keep current behavior (no histogram for expression stats).
         */
        private Optional<Histogram> transformHistogramForUnary(CallOperator callOperator, ColumnStatistic childStats) {
            Histogram childHist = childStats == null ? null : childStats.getHistogram();
            if (childHist == null || childHist.getMCV() == null || childHist.getMCV().isEmpty()) {
                return Optional.empty();
            }

            final String fn = callOperator.getFnName().toLowerCase();
            final Type targetType = callOperator.getType();

            if (!isSupportedIntegerMcvType(targetType)) {
                return Optional.empty();
            }

            if (FunctionSet.POSITIVE.equalsIgnoreCase(fn)) {
                return Optional.of(childHist);
            }

            if (!FunctionSet.NEGATIVE.equalsIgnoreCase(fn) && !FunctionSet.ABS.equalsIgnoreCase(fn)) {
                return Optional.empty();
            }

            // Buckets transformation:
            // - NEGATIVE(x): y = -x (monotonic decreasing) => [l,u] -> [-u,-l], reverse order
            // - ABS(x): y = abs(x)
            //     - if buckets are all >= 0: identity
            //     - if buckets are all <= 0: y = -x (monotonic decreasing) => [l,u] -> [-u,-l], reverse order
            //     - if buckets cross 0: non-monotonic => fail closed (Optional.empty()) to avoid inconsistent histogram
            List<Bucket> newBuckets = childHist.getBuckets();
            if (newBuckets != null && !newBuckets.isEmpty()) {
                boolean needNegateBuckets = FunctionSet.NEGATIVE.equalsIgnoreCase(fn);
                if (FunctionSet.ABS.equalsIgnoreCase(fn)) {
                    boolean allNonNegative = newBuckets.stream().allMatch(b -> b.getLower() >= 0);
                    boolean allNonPositive = newBuckets.stream().allMatch(b -> b.getUpper() <= 0);
                    if (allNonNegative) {
                        needNegateBuckets = false;
                    } else if (allNonPositive) {
                        needNegateBuckets = true;
                    } else {
                        return Optional.empty();
                    }
                }

                if (needNegateBuckets) {
                    long prevCum = 0L;
                    List<Long> perBucketTotals = Lists.newArrayListWithCapacity(newBuckets.size());
                    List<double[]> bounds = Lists.newArrayListWithCapacity(newBuckets.size());
                    for (Bucket b : newBuckets) {
                        long perBucketTotal = b.getCount() - prevCum;
                        prevCum = b.getCount();
                        perBucketTotals.add(perBucketTotal);
                        bounds.add(new double[] {-b.getUpper(), -b.getLower()});
                    }

                    long newCum = 0L;
                    List<Bucket> reversed = Lists.newArrayListWithCapacity(newBuckets.size());
                    for (int i = newBuckets.size() - 1; i >= 0; i--) {
                        newCum += perBucketTotals.get(i);
                        double[] bd = bounds.get(i);
                        reversed.add(new Bucket(bd[0], bd[1], newCum, 0L));
                    }
                    newBuckets = reversed;
                }
            }

            Map<String, Long> newMcv = new HashMap<>();
            for (Map.Entry<String, Long> e : childHist.getMCV().entrySet()) {
                Optional<BigInteger> vOpt = parseFixedPointMcvKey(e.getKey(), targetType);
                if (vOpt.isEmpty()) {
                    return Optional.empty();
                }
                BigInteger v = vOpt.get();
                BigInteger out;
                if (FunctionSet.NEGATIVE.equalsIgnoreCase(fn)) {
                    out = v.negate();
                } else {
                    out = v.abs();
                }
                Optional<ConstantOperator> outConst = fixedPointToConstant(out, targetType);
                if (outConst.isEmpty()) {
                    return Optional.empty();
                }
                newMcv.merge(outConst.get().toString(), e.getValue(), Long::sum);
            }

            return Optional.of(new Histogram(newBuckets, newMcv));
        }

        /**
         * Only do histogram/MCV propagation for binary expressions when it is definitely correct.
         * <p>
         * NOTE: This code path must be semantics-safe because MCV keys may be used as exact constant values
         * (e.g. as IN-list values in skew join elimination v2). Therefore we only support a small whitelist of
         * transformations that we can prove to be correct. If you need to support more functions in the future,
         * add them here with strict guards and "fail closed" (return Optional.empty()) on any ambiguity.
         * <p>
         * Current supported cases (fixed-point only):
         * - ADD(x, const) / ADD(const, x)
         * - SUBTRACT(x, const) / SUBTRACT(const, x)
         * <p>
         * For x +/- const we will:
         * - transform MCV keys by applying the exact operation
         * - shift bucket bounds for x +/- const when the mapping is monotonic (x +/- const)
         *   (for const - x we transform buckets by reversing order)
         */
        private Optional<Histogram> transformHistogramForBinary(
                CallOperator callOperator, ColumnStatistic leftStats, ColumnStatistic rightStats) {
            final String fn = callOperator.getFnName().toLowerCase();
            if (!FunctionSet.ADD.equalsIgnoreCase(fn) && !FunctionSet.SUBTRACT.equalsIgnoreCase(fn)) {
                return Optional.empty();
            }

            final Type targetType = callOperator.getType();
            if (!isSupportedIntegerMcvType(targetType)) {
                return Optional.empty();
            }

            ScalarOperator leftOp = callOperator.getChild(0);
            ScalarOperator rightOp = callOperator.getChild(1);
            boolean leftIsConst = leftOp != null && leftOp.isConstant();
            boolean rightIsConst = rightOp != null && rightOp.isConstant();
            if (leftIsConst == rightIsConst) {
                return Optional.empty();
            }

            Optional<ConstantOperator> constOpOpt = toConstantOperator(leftIsConst ? leftOp : rightOp);
            if (constOpOpt.isEmpty()) {
                return Optional.empty();
            }
            ConstantOperator constOp = constOpOpt.get();
            ColumnStatistic baseStats = leftIsConst ? rightStats : leftStats;
            Histogram baseHist = baseStats == null ? null : baseStats.getHistogram();
            if (baseHist == null || baseHist.getMCV() == null || baseHist.getMCV().isEmpty()) {
                return Optional.empty();
            }

            Optional<BigInteger> constValOpt = constantToFixedPoint(constOp, targetType);
            if (constValOpt.isEmpty()) {
                return Optional.empty();
            }
            BigInteger c = constValOpt.get();

            Map<String, Long> newMcv = new HashMap<>();
            for (Map.Entry<String, Long> e : baseHist.getMCV().entrySet()) {
                Optional<BigInteger> vOpt = parseFixedPointMcvKey(e.getKey(), targetType);
                if (vOpt.isEmpty()) {
                    return Optional.empty();
                }
                BigInteger v = vOpt.get();
                BigInteger out;
                if (FunctionSet.ADD.equalsIgnoreCase(callOperator.getFnName())) {
                    out = v.add(c);
                } else {
                    // SUBTRACT
                    if (!leftIsConst) {
                        // x - const
                        out = v.subtract(c);
                    } else {
                        // const - x
                        out = c.subtract(v);
                    }
                }
                Optional<ConstantOperator> outConst = fixedPointToConstant(out, targetType);
                if (outConst.isEmpty()) {
                    return Optional.empty();
                }
                newMcv.merge(outConst.get().toString(), e.getValue(), Long::sum);
            }

            List<Bucket> newBuckets = baseHist.getBuckets();
            boolean shouldShiftBuckets = FunctionSet.ADD.equalsIgnoreCase(callOperator.getFnName()) || !leftIsConst;
            if (shouldShiftBuckets) {
                OptionalDouble deltaOpt = ConstantOperatorUtils.doubleValueFromConstant(constOp);
                if (deltaOpt.isPresent()) {
                    final double bucketShift = FunctionSet.SUBTRACT.equalsIgnoreCase(callOperator.getFnName())
                            ? -deltaOpt.getAsDouble()
                            : deltaOpt.getAsDouble();
                    if (baseHist.getBuckets() != null) {
                        newBuckets = baseHist.getBuckets().stream()
                            .map(b -> new Bucket(b.getLower() + bucketShift, b.getUpper() + bucketShift,
                                    b.getCount(), b.getUpperRepeats()))
                            .collect(Collectors.toList());
                    }
                }
            } else {
                OptionalDouble cOpt = ConstantOperatorUtils.doubleValueFromConstant(constOp);
                if (cOpt.isPresent() && baseHist.getBuckets() != null && !baseHist.getBuckets().isEmpty()) {
                    final double cDouble = cOpt.getAsDouble();
                    final List<Bucket> baseBuckets = baseHist.getBuckets();
                    long prevCum = 0L;
                    List<Long> perBucketTotals = Lists.newArrayListWithCapacity(baseBuckets.size());
                    List<double[]> bounds = Lists.newArrayListWithCapacity(baseBuckets.size());
                    for (Bucket b : baseBuckets) {
                        long perBucketTotal = b.getCount() - prevCum;
                        prevCum = b.getCount();
                        perBucketTotals.add(perBucketTotal);
                        bounds.add(new double[] {cDouble - b.getUpper(), cDouble - b.getLower()});
                    }

                    long newCum = 0L;
                    List<Bucket> reversed = Lists.newArrayListWithCapacity(baseBuckets.size());
                    for (int i = baseBuckets.size() - 1; i >= 0; i--) {
                        newCum += perBucketTotals.get(i);
                        double[] bd = bounds.get(i);
                        reversed.add(new Bucket(bd[0], bd[1], newCum, 0L));
                    }
                    newBuckets = reversed;
                }
            }

            return Optional.of(new Histogram(newBuckets, newMcv));
        }

        private Optional<ConstantOperator> toConstantOperator(ScalarOperator op) {
            if (op == null || !op.isConstant() || op.isConstantNull()) {
                return Optional.empty();
            }
            if (op instanceof ConstantOperator) {
                return Optional.of((ConstantOperator) op);
            }
            return Optional.empty();
        }

        private Optional<BigInteger> parseFixedPointMcvKey(String mcvKey, Type targetType) {
            Optional<ConstantOperator> c = ConstantOperator.createVarchar(mcvKey).castTo(targetType);
            return c.map(constantOperator -> constantToBigInteger(constantOperator, targetType));
        }

        private Optional<BigInteger> constantToFixedPoint(ConstantOperator constant, Type targetType) {
            Optional<ConstantOperator> c = constant.castTo(targetType);
            return c.map(constantOperator -> constantToBigInteger(constantOperator, targetType));
        }

        private BigInteger constantToBigInteger(ConstantOperator c, Type targetType) {
            if (targetType.isTinyint()) {
                return BigInteger.valueOf(c.getTinyInt());
            } else if (targetType.isSmallint()) {
                return BigInteger.valueOf(c.getSmallint());
            } else if (targetType.isInt()) {
                return BigInteger.valueOf(c.getInt());
            } else if (targetType.isBigint()) {
                return BigInteger.valueOf(c.getBigint());
            } else {
                // LARGEINT
                return c.getLargeInt();
            }
        }

        private Optional<ConstantOperator> fixedPointToConstant(BigInteger v, Type targetType) {
            try {
                if (targetType.isTinyint()) {
                    if (v.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0 ||
                            v.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0) {
                        return Optional.empty();
                    }
                    return Optional.of(ConstantOperator.createTinyInt(v.byteValueExact()));
                } else if (targetType.isSmallint()) {
                    if (v.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0 ||
                            v.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0) {
                        return Optional.empty();
                    }
                    return Optional.of(ConstantOperator.createSmallInt(v.shortValueExact()));
                } else if (targetType.isInt()) {
                    if (v.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0 ||
                            v.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
                        return Optional.empty();
                    }
                    return Optional.of(ConstantOperator.createInt(v.intValueExact()));
                } else if (targetType.isBigint()) {
                    if (v.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0 ||
                            v.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                        return Optional.empty();
                    }
                    return Optional.of(ConstantOperator.createBigint(v.longValueExact()));
                } else if (targetType.isLargeint()) {
                    return Optional.of(ConstantOperator.createLargeInt(v));
                } else {
                    return Optional.empty();
                }
            } catch (ArithmeticException ex) {
                return Optional.empty();
            }
        }

        private boolean isSupportedIntegerMcvType(Type t) {
            return t.isTinyint() || t.isSmallint() || t.isInt() || t.isBigint() || t.isLargeint();
        }
    }
}