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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import org.apache.commons.math3.util.Precision;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PredicateStatisticsCalculator {
    public static Statistics statisticsCalculate(ScalarOperator predicate, Statistics statistics) {
        if (predicate == null) {
            return statistics;
        }

        // The time-complexity of PredicateStatisticsCalculatingVisitor OR row-count is O(2^n), n is OR number
        if (countDisConsecutiveOr(predicate, 0, false) > StatisticsEstimateCoefficient.DEFAULT_OR_OPERATOR_LIMIT) {
            return predicate.accept(new LargeOrCalculatingVisitor(statistics), null);
        } else {
            return predicate.accept(new BaseCalculatingVisitor(statistics), null);
        }
    }

    private static long countDisConsecutiveOr(ScalarOperator root, long count, boolean isConsecutive) {
        boolean isOr = OperatorType.COMPOUND.equals(root.getOpType()) && ((CompoundPredicateOperator) root).isOr();
        if (isOr && !isConsecutive) {
            count = count + 1;
        }

        for (ScalarOperator child : root.getChildren()) {
            count = countDisConsecutiveOr(child, count, isOr);
        }

        return count;
    }

    private static class BaseCalculatingVisitor extends ScalarOperatorVisitor<Statistics, Void> {
        protected final Statistics statistics;

        public BaseCalculatingVisitor(Statistics statistics) {
            this.statistics = statistics;
        }

        protected boolean checkNeedEvalEstimate(ScalarOperator predicate) {
            if (predicate == null) {
                return false;
            }
            // check predicate need to eval
            if (predicate.isNotEvalEstimate()) {
                return false;
            }
            // extract range predicate scalar operator with unknown column statistics will not eval
            if (predicate.isFromPredicateRangeDerive() &&
                    statistics.getColumnStatistics().values().stream().anyMatch(ColumnStatistic::isUnknown)) {
                return false;
            }
            return true;
        }

        @Override
        public Statistics visit(ScalarOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            double outputRowCount =
                    statistics.getOutputRowCount() * StatisticsEstimateCoefficient.PREDICATE_UNKNOWN_FILTER_COEFFICIENT;
            return StatisticsEstimateUtils.adjustStatisticsByRowCount(
                    Statistics.buildFrom(statistics).setOutputRowCount(outputRowCount).build(),
                    outputRowCount);
        }

        @Override
        public Statistics visitInPredicate(InPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            double selectivity;

            ScalarOperator firstChild = getChildForCastOperator(predicate.getChild(0));
            List<ScalarOperator> otherChildrenList =
                    predicate.getChildren().stream().skip(1).map(this::getChildForCastOperator).distinct()
                            .collect(Collectors.toList());
            // 1. compute the inPredicate children column statistics
            ColumnStatistic inColumnStatistic = getExpressionStatistic(firstChild);
            List<ColumnStatistic> otherChildrenColumnStatisticList =
                    otherChildrenList.stream().distinct().map(this::getExpressionStatistic).collect(Collectors.toList());

            // using ndv to estimate string col inPredicate
            if (!predicate.isNotIn() && firstChild.getType().getPrimitiveType().isCharFamily()
                    && firstChild.isColumnRef()
                    && !inColumnStatistic.isUnknown()) {
                selectivity = Math.min(otherChildrenList.size() / inColumnStatistic.getDistinctValuesCount(), 1);

                double rowCount = Math.max(1, statistics.getOutputRowCount() * selectivity);

                // only columnRefOperator could add column statistic to statistics
                Optional<ColumnRefOperator> childOpt =
                        firstChild.isColumnRef() ? Optional.of((ColumnRefOperator) firstChild) : Optional.empty();
                ColumnStatistic newInColumnStatistic =
                        ColumnStatistic.builder()
                                .setDistinctValuesCount(Math.min(inColumnStatistic.getDistinctValuesCount(),
                                        otherChildrenList.size()))
                                .setAverageRowSize(inColumnStatistic.getAverageRowSize())
                                .setNullsFraction(0)
                                .build();

                Statistics inStatistics = childOpt.map(operator ->
                                Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                                        addColumnStatistic(operator, newInColumnStatistic).build()).
                        orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(inStatistics, rowCount);
            }

            double columnMaxVal = inColumnStatistic.getMaxValue();
            double columnMinVal = inColumnStatistic.getMinValue();
            double columnDistinctValues = inColumnStatistic.getDistinctValuesCount();

            double otherChildrenMaxValue =
                    otherChildrenColumnStatisticList.stream().mapToDouble(ColumnStatistic::getMaxValue).max()
                            .orElse(Double.POSITIVE_INFINITY);
            double otherChildrenMinValue =
                    otherChildrenColumnStatisticList.stream().mapToDouble(ColumnStatistic::getMinValue).min()
                            .orElse(Double.NEGATIVE_INFINITY);
            double otherChildrenDistinctValues =
                    otherChildrenColumnStatisticList.stream().mapToDouble(ColumnStatistic::getDistinctValuesCount)
                            .sum();
            boolean hasOverlap =
                    Math.max(columnMinVal, otherChildrenMinValue) <= Math.min(columnMaxVal, otherChildrenMaxValue);

            // 2 .compute the in predicate selectivity
            if (inColumnStatistic.isUnknown() || inColumnStatistic.hasNaNValue() ||
                    otherChildrenColumnStatisticList.stream().anyMatch(
                            columnStatistic -> columnStatistic.hasNaNValue() || columnStatistic.isUnknown()) ||
                    !(firstChild.isColumnRef())) {
                // use default selectivity if column statistic is unknown or has NaN values.
                // can not get accurate column statistics if it is not ColumnRef operator
                selectivity = predicate.isNotIn() ?
                        1 - StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT :
                        StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
            } else {
                // children column statistics are not unknown.
                selectivity = hasOverlap ?
                        Math.min(1.0, otherChildrenDistinctValues / inColumnStatistic.getDistinctValuesCount()) : 0.0;
                selectivity = predicate.isNotIn() ? 1 - selectivity : selectivity;
            }
            // avoid not in predicate too small
            if (predicate.isNotIn() && Precision.equals(selectivity, 0.0, 0.000001d)) {
                selectivity = 1 - StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
            }

            double rowCount = Math.min(statistics.getOutputRowCount() * selectivity, statistics.getOutputRowCount());

            // 3. compute the inPredicate first child column statistics after in predicate
            if (otherChildrenColumnStatisticList.stream()
                    .noneMatch(columnStatistic -> columnStatistic.hasNaNValue() || columnStatistic.isUnknown()) &&
                    !predicate.isNotIn() && hasOverlap) {
                columnMaxVal = Math.min(columnMaxVal, otherChildrenMaxValue);
                columnMinVal = Math.max(columnMinVal, otherChildrenMinValue);
                columnDistinctValues = Math.min(columnDistinctValues, otherChildrenDistinctValues);
            }
            ColumnStatistic newInColumnStatistic =
                    ColumnStatistic.buildFrom(inColumnStatistic).setDistinctValuesCount(columnDistinctValues)
                            .setMinValue(columnMinVal)
                            .setMaxValue(columnMaxVal).build();

            // only columnRefOperator could add column statistic to statistics
            Optional<ColumnRefOperator> childOpt =
                    firstChild.isColumnRef() ? Optional.of((ColumnRefOperator) firstChild) : Optional.empty();

            Statistics inStatistics = childOpt.map(operator ->
                            Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                                    addColumnStatistic(operator, newInColumnStatistic).build()).
                    orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
            return StatisticsEstimateUtils.adjustStatisticsByRowCount(inStatistics, rowCount);
        }

        @Override
        public Statistics visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            double selectivity = 1;
            List<ColumnRefOperator> children = Utils.extractColumnRef(predicate);
            if (children.size() != 1) {
                selectivity = predicate.isNotNull() ?
                        1 - StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT :
                        StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
                double rowCount = statistics.getOutputRowCount() * selectivity;
                return Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build();
            }
            ColumnStatistic isNullColumnStatistic = statistics.getColumnStatistic(children.get(0));
            if (isNullColumnStatistic.isUnknown()) {
                selectivity = predicate.isNotNull() ?
                        1 - StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT :
                        StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
            } else {
                selectivity = predicate.isNotNull() ? 1 - isNullColumnStatistic.getNullsFraction() :
                        isNullColumnStatistic.getNullsFraction();
            }
            // avoid estimate selectivity too small because of the error of null fraction
            selectivity =
                    Math.max(selectivity, StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT);
            double rowCount = statistics.getOutputRowCount() * selectivity;
            Statistics.Builder builder = Statistics.buildFrom(statistics).setOutputRowCount(rowCount);
            builder.addColumnStatistic(children.get(0), ColumnStatistic.buildFrom(isNullColumnStatistic)
                    .setNullsFraction(predicate.isNotNull() ? 0.0 : 1.0)
                    .build());
            return StatisticsEstimateUtils.adjustStatisticsByRowCount(builder.build(), rowCount);
        }

        @Override
        public Statistics visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            ScalarOperator leftChild = predicate.getChild(0);
            ScalarOperator rightChild = predicate.getChild(1);
            Preconditions.checkState(!(leftChild.isConstantRef() && rightChild.isConstantRef()),
                    "ConstantRef-cmp-ConstantRef not supported here, %s should be eliminated earlier",
                    predicate);
            Preconditions.checkState(!(leftChild.isConstant() && rightChild.isVariable()),
                    "Constant-cmp-Column not supported here, %s should be deal earlier", predicate);
            // For CastOperator, we need use child as column statistics
            leftChild = getChildForCastOperator(leftChild);
            rightChild = getChildForCastOperator(rightChild);
            // compute left and right column statistics
            ColumnStatistic leftColumnStatistic = getExpressionStatistic(leftChild);
            ColumnStatistic rightColumnStatistic = getExpressionStatistic(rightChild);
            // do not use NaN to estimate predicate
            if (leftColumnStatistic.hasNaNValue()) {
                leftColumnStatistic =
                        ColumnStatistic.buildFrom(leftColumnStatistic).setMaxValue(Double.POSITIVE_INFINITY)
                                .setMinValue(Double.NEGATIVE_INFINITY).build();
            }
            if (rightColumnStatistic.hasNaNValue()) {
                rightColumnStatistic =
                        ColumnStatistic.buildFrom(rightColumnStatistic).setMaxValue(Double.POSITIVE_INFINITY)
                                .setMinValue(Double.NEGATIVE_INFINITY).build();
            }

            if (leftChild.isVariable()) {
                Optional<ColumnRefOperator> leftChildOpt;
                // only columnRefOperator could add column statistic to statistics
                leftChildOpt = leftChild.isColumnRef() ? Optional.of((ColumnRefOperator) leftChild) : Optional.empty();

                if (rightChild.isConstant()) {
                    Optional<ConstantOperator> constantOperator;
                    if (rightChild.isConstantRef()) {
                        constantOperator = Optional.of((ConstantOperator) rightChild);
                    } else {
                        constantOperator = Optional.empty();
                    }
                    Statistics binaryStats =
                            BinaryPredicateStatisticCalculator.estimateColumnToConstantComparison(leftChildOpt,
                                    leftColumnStatistic, predicate, constantOperator, statistics);
                    return StatisticsEstimateUtils.adjustStatisticsByRowCount(binaryStats,
                            binaryStats.getOutputRowCount());
                } else {
                    Statistics binaryStats = BinaryPredicateStatisticCalculator.estimateColumnToColumnComparison(
                            leftChild, leftColumnStatistic,
                            rightChild, rightColumnStatistic,
                            predicate, statistics);
                    return StatisticsEstimateUtils.adjustStatisticsByRowCount(binaryStats,
                            binaryStats.getOutputRowCount());
                }
            } else {
                // constant compare constant
                double outputRowCount = statistics.getOutputRowCount() *
                        StatisticsEstimateCoefficient.CONSTANT_TO_CONSTANT_PREDICATE_COEFFICIENT;
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(
                        Statistics.buildFrom(statistics).setOutputRowCount(outputRowCount).build(), outputRowCount);
            }
        }

        @Override
        public Statistics visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }

            if (predicate.isAnd()) {
                Statistics leftStatistics = predicate.getChild(0).accept(this, null);
                Statistics andStatistics =
                        predicate.getChild(1).accept(new BaseCalculatingVisitor(leftStatistics), null);
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(andStatistics,
                        andStatistics.getOutputRowCount());
            } else if (predicate.isOr()) {
                List<ScalarOperator> disjunctive = Utils.extractDisjunctive(predicate);
                Statistics cumulativeStatistics = disjunctive.get(0).accept(this, null);
                double rowCount = cumulativeStatistics.getOutputRowCount();

                for (int i = 1; i < disjunctive.size(); ++i) {
                    Statistics orItemStatistics = disjunctive.get(i).accept(this, null);
                    Statistics andStatistics =
                            disjunctive.get(i).accept(new BaseCalculatingVisitor(cumulativeStatistics), null);
                    rowCount = cumulativeStatistics.getOutputRowCount() + orItemStatistics.getOutputRowCount() -
                            andStatistics.getOutputRowCount();
                    rowCount = Math.min(rowCount, statistics.getOutputRowCount());
                    cumulativeStatistics =
                            computeOrPredicateStatistics(cumulativeStatistics, orItemStatistics, rowCount);
                }

                return StatisticsEstimateUtils.adjustStatisticsByRowCount(cumulativeStatistics, rowCount);
            } else {
                Statistics inputStatistics = predicate.getChild(0).accept(this, null);
                double rowCount = Math.max(0, statistics.getOutputRowCount() - inputStatistics.getOutputRowCount());
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(
                        Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build(), rowCount);
            }
        }

        protected Statistics computeOrPredicateStatistics(Statistics cumulativeStatistics, Statistics orItemStatistics,
                                                          double rowCount) {
            Statistics.Builder builder = Statistics.buildFrom(cumulativeStatistics);
            builder.setOutputRowCount(rowCount);

            cumulativeStatistics.getColumnStatistics().forEach((columnRefOperator, columnStatistic) -> {
                ColumnStatistic.Builder columnBuilder = ColumnStatistic.buildFrom(columnStatistic);
                ColumnStatistic rightColumnStatistic = orItemStatistics.getColumnStatistic(columnRefOperator);
                columnBuilder.setMinValue(Math.min(columnStatistic.getMinValue(), rightColumnStatistic.getMinValue()));
                columnBuilder.setMaxValue(Math.max(columnStatistic.getMaxValue(), rightColumnStatistic.getMaxValue()));
                double originalNdv = statistics.getColumnStatistic(columnRefOperator).getDistinctValuesCount();
                double accumulatedNdv = columnStatistic.getDistinctValuesCount() + rightColumnStatistic.getDistinctValuesCount();
                columnBuilder.setDistinctValuesCount(Math.min(originalNdv, accumulatedNdv));
                builder.addColumnStatistic(columnRefOperator, columnBuilder.build());
            });
            return builder.build();
        }

        @Override
        public Statistics visitConstant(ConstantOperator constant, Void context) {
            if (constant.getBoolean()) {
                return statistics;
            } else {
                return Statistics.buildFrom(statistics).setOutputRowCount(0.0).build();
            }
        }

        private ScalarOperator getChildForCastOperator(ScalarOperator operator) {
            if (operator instanceof CastOperator) {
                operator = getChildForCastOperator(operator.getChild(0));
            }
            return operator;
        }

        private ColumnStatistic getExpressionStatistic(ScalarOperator operator) {
            return ExpressionStatisticCalculator.calculate(operator, statistics);
        }
    }

    private static class LargeOrCalculatingVisitor extends BaseCalculatingVisitor {
        public LargeOrCalculatingVisitor(Statistics statistics) {
            super(statistics);
        }

        @Override
        public Statistics visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }

            if (predicate.isAnd()) {
                Statistics leftStatistics = predicate.getChild(0).accept(this, null);
                Statistics andStatistics = predicate.getChild(1)
                        .accept(new LargeOrCalculatingVisitor(leftStatistics), null);
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(andStatistics,
                        andStatistics.getOutputRowCount());
            } else if (predicate.isOr()) {
                List<ScalarOperator> disjunctive = Utils.extractDisjunctive(predicate);
                Statistics baseStatistics = disjunctive.get(0).accept(this, null);
                double rowCount = baseStatistics.getOutputRowCount();

                for (int i = 1; i < disjunctive.size(); ++i) {
                    Statistics orStatistics = disjunctive.get(i).accept(this, null);
                    rowCount = (baseStatistics.getOutputRowCount() + orStatistics.getOutputRowCount()) / 2;
                    rowCount = Math.max(rowCount, baseStatistics.getOutputRowCount());
                    rowCount = Math.max(rowCount, orStatistics.getOutputRowCount());
                    rowCount = Math.min(rowCount, statistics.getOutputRowCount());
                    baseStatistics = computeOrPredicateStatistics(baseStatistics, orStatistics, rowCount);
                }

                return StatisticsEstimateUtils.adjustStatisticsByRowCount(baseStatistics, rowCount);
            } else {
                Statistics inputStatistics = predicate.getChild(0).accept(this, null);
                double rowCount = Math.max(0, statistics.getOutputRowCount() - inputStatistics.getOutputRowCount());
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(
                        Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build(), rowCount);
            }
        }

        @Override
        protected Statistics computeOrPredicateStatistics(Statistics baseStatistics, Statistics orItemStatistics,
                                                          double rowCount) {
            // support simple avg statistics
            Statistics.Builder builder = Statistics.buildFrom(baseStatistics);
            builder.setOutputRowCount(rowCount);

            baseStatistics.getColumnStatistics().forEach((columnRefOperator, columnStatistic) -> {
                ColumnStatistic.Builder columnBuilder = ColumnStatistic.buildFrom(columnStatistic);
                ColumnStatistic rightColumnStatistic = orItemStatistics.getColumnStatistic(columnRefOperator);
                columnBuilder.setMinValue(Math.min(columnStatistic.getMinValue(), rightColumnStatistic.getMinValue()));
                columnBuilder.setMaxValue(Math.max(columnStatistic.getMaxValue(), rightColumnStatistic.getMaxValue()));
                double distinct = Math.max(1,
                        (columnStatistic.getDistinctValuesCount() + rightColumnStatistic.getDistinctValuesCount()) / 2);
                double nulls = Math.max(1,
                        (columnStatistic.getNullsFraction() + rightColumnStatistic.getNullsFraction()) / 2);
                columnBuilder.setDistinctValuesCount(distinct);
                columnBuilder.setNullsFraction(nulls);
                builder.addColumnStatistic(columnRefOperator, columnBuilder.build());
            });
            return builder.build();
        }
    }
}
