// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.Utils.getDatetimeFromLong;

public class ExpressionStatisticCalculator {
    private static final Logger LOG = LogManager.getLogger(ExpressionStatisticCalculator.class);

    public static ColumnStatistic calculate(ScalarOperator operator, Statistics input) {
        return calculate(operator, input, input != null ? input.getOutputRowCount() : 0);
    }

    public static ColumnStatistic calculate(ScalarOperator operator, Statistics input, double rowCount) {
        return operator.accept(new ExpressionStatisticVisitor(input, rowCount), null);
    }

    private static class ExpressionStatisticVisitor extends ScalarOperatorVisitor<ColumnStatistic, Void> {
        private final Statistics inputStatistics;
        // Some function estimate need plan node row count, such as COUNT
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
            return new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0,
                    caseWhenOperator.getType().getTypeSize(), distinctValues);
        }

        @Override
        public ColumnStatistic visitCastOperator(CastOperator cast, Void context) {
            ColumnStatistic childStatistic = cast.getChild(0).accept(this, context);

            if (childStatistic.isUnknown() ||
                    inputStatistics.getColumnStatistics().values().stream().allMatch(ColumnStatistic::isUnknown)) {
                return ColumnStatistic.unknown();
            }

            // If cast destination type is number/string, it's unnecessary to cast, keep self is enough.
            if (!cast.getType().isDateType()) {
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
                LOG.debug("expression statistic compute cast failed: " + max.toString() + ", " + min.toString() +
                        ", to type: " + cast.getType());
                return childStatistic;
            }

            double maxValue = Utils.getLongFromDateTime(max.getDatetime());
            double minValue = Utils.getLongFromDateTime(min.getDatetime());

            return new ColumnStatistic(minValue, maxValue, childStatistic.getNullsFraction(),
                    childStatistic.getAverageRowSize(), childStatistic.getDistinctValuesCount());
        }

        @Override
        public ColumnStatistic visitCall(CallOperator call, Void context) {
            List<ColumnStatistic> childrenColumnStatistics =
                    call.getChildren().stream().map(child -> child.accept(this, context)).collect(Collectors.toList());
            Preconditions.checkState(childrenColumnStatistics.size() == call.getChildren().size());
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
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.COUNT:
                    return new ColumnStatistic(0, inputStatistics.getOutputRowCount(), 0,
                            callOperator.getType().getTypeSize(), rowCount);
                default:
                    return ColumnStatistic.unknown();
            }
        }

        private ColumnStatistic unaryExpressionCalculate(CallOperator callOperator, ColumnStatistic columnStatistic) {
            double aggFunDistinctValue = Math.max(1.0, Math.min(rowCount, columnStatistic.getDistinctValuesCount()));
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.MAX:
                case FunctionSet.MIN:
                case FunctionSet.ANY_VALUE:
                case FunctionSet.AVG:
                    double maxValue = columnStatistic.getMaxValue();
                    double minValue = columnStatistic.getMinValue();
                    return new ColumnStatistic(minValue, maxValue, 0, columnStatistic.getAverageRowSize(),
                            aggFunDistinctValue);
                case FunctionSet.SUM:
                    double sumFunMinValue = columnStatistic.getMinValue() > 0 ? columnStatistic.getMinValue() :
                            columnStatistic.getMinValue() * rowCount / aggFunDistinctValue;
                    double sumFunMaxValue = columnStatistic.getMaxValue() < 0 ? columnStatistic.getMaxValue() :
                            columnStatistic.getMaxValue() * rowCount / aggFunDistinctValue;
                    sumFunMaxValue = Math.max(sumFunMaxValue, sumFunMinValue);
                    return new ColumnStatistic(sumFunMinValue, sumFunMaxValue, 0,
                            columnStatistic.getAverageRowSize(), aggFunDistinctValue);
                case FunctionSet.YEAR:
                    int minYearValue = 1700;
                    int maxYearValue = 2100;
                    try {
                        minYearValue = getDatetimeFromLong((long) columnStatistic.getMinValue()).getYear();
                        maxYearValue = getDatetimeFromLong((long) columnStatistic.getMaxValue()).getYear();
                    } catch (DateTimeException e) {
                        LOG.debug("get date type column statistics min/max failed. " + e);
                    }
                    return new ColumnStatistic(minYearValue, maxYearValue, 0,
                            callOperator.getType().getTypeSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), (maxYearValue - minYearValue + 1)));
                case FunctionSet.MONTH:
                    return new ColumnStatistic(1, 12, 0,
                            callOperator.getType().getTypeSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), 12));
                case FunctionSet.DAY:
                    return new ColumnStatistic(1, 31, 0,
                            callOperator.getType().getTypeSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), 31));
                case FunctionSet.HOUR:
                    return new ColumnStatistic(0, 23, 0,
                            callOperator.getType().getTypeSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), 24));
                case FunctionSet.MINUTE:
                case FunctionSet.SECOND:
                    return new ColumnStatistic(0, 59, 0,
                            callOperator.getType().getTypeSize(),
                            Math.min(columnStatistic.getDistinctValuesCount(), 60));
                case FunctionSet.COUNT:
                    return new ColumnStatistic(0, inputStatistics.getOutputRowCount(), 0,
                            callOperator.getType().getTypeSize(), rowCount);
                case FunctionSet.MULTI_DISTINCT_COUNT:
                    // use child column averageRowSize instead call operator type size
                    return new ColumnStatistic(0, columnStatistic.getDistinctValuesCount(), 0,
                            columnStatistic.getAverageRowSize(), rowCount);
                default:
                    return ColumnStatistic.unknown();
            }
        }

        private ColumnStatistic binaryExpressionCalculate(CallOperator callOperator, ColumnStatistic left,
                                                          ColumnStatistic right) {
            double distinctValues = Math.max(left.getDistinctValuesCount(), right.getDistinctValuesCount());
            double nullsFraction = 1 - ((1 - left.getNullsFraction()) * (1 - right.getNullsFraction()));
            switch (callOperator.getFnName().toLowerCase()) {
                case FunctionSet.ADD:
                    return new ColumnStatistic(left.getMinValue() + right.getMinValue(),
                            left.getMaxValue() + right.getMaxValue(), nullsFraction,
                            callOperator.getType().getTypeSize(),
                            distinctValues);
                case FunctionSet.SUBTRACT:
                    return new ColumnStatistic(left.getMinValue() - right.getMaxValue(),
                            left.getMaxValue() - right.getMinValue(), nullsFraction,
                            callOperator.getType().getTypeSize(),
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
                            callOperator.getType().getTypeSize(), distinctValues);
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
                            callOperator.getType().getTypeSize(),
                            distinctValues);
                // use child column statistics for now
                case FunctionSet.SUBSTRING:
                    return left;
                default:
                    return ColumnStatistic.unknown();
            }
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
