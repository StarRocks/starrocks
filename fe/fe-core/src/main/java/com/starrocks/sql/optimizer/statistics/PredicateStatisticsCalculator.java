// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

public class PredicateStatisticsCalculator {
    public static Statistics statisticsCalculate(ScalarOperator predicate, Statistics statistics) {
        if (predicate == null) {
            return statistics;
        }
        return predicate.accept(new PredicateStatisticsCalculatingVisitor(statistics), null);
    }

    private static class PredicateStatisticsCalculatingVisitor extends ScalarOperatorVisitor<Statistics, Void> {
        private final Statistics statistics;

        public PredicateStatisticsCalculatingVisitor(Statistics statistics) {
            this.statistics = statistics;
        }

        private boolean checkNeedEvalEstimate(ScalarOperator predicate) {
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
            return Statistics.buildFrom(statistics).setOutputRowCount(outputRowCount).build();
        }

        @Override
        public Statistics visitInPredicate(InPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            double selectivity = 1;
            int inValueSize = predicate.getChildren().size() - 1;
            ScalarOperator child = predicate.getChild(0);
            if (!(child.isColumnRef())) {
                selectivity = predicate.isNotIn() ?
                        1 - StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT :
                        StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
                double rowCount = statistics.getOutputRowCount() * selectivity;
                return Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build();
            }
            ColumnStatistic inColumnStatistic = statistics.getColumnStatistic((ColumnRefOperator) child);
            if (inColumnStatistic.isUnknown()) {
                selectivity = predicate.isNotIn() ?
                        1 - StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT :
                        StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
            } else {
                selectivity = inValueSize / inColumnStatistic.getDistinctValuesCount();
                selectivity = predicate.isNotIn() ? 1 - selectivity : selectivity;
            }
            double rowCount = Math.min(statistics.getOutputRowCount() * selectivity, statistics.getOutputRowCount());
            return Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                    addColumnStatistic((ColumnRefOperator) child,
                            ColumnStatistic.buildFrom(inColumnStatistic).setDistinctValuesCount(
                                            predicate.isNotIn() ? inColumnStatistic.getDistinctValuesCount() : inValueSize).
                                    build()).build();
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
            return Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                    addColumnStatistics(
                            ImmutableMap.of(children.get(0), ColumnStatistic.buildFrom(isNullColumnStatistic).
                                    setNullsFraction(predicate.isNotNull() ? 0.0 : 1.0).build())).build();

        }

        @Override
        public Statistics visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            Preconditions.checkState(predicate.getChildren().size() == 2);
            ScalarOperator leftChild = predicate.getChild(0);
            ScalarOperator rightChild = predicate.getChild(1);
            Preconditions.checkState(!(leftChild.isConstantRef() && rightChild.isConstantRef()),
                    "ConstantRef-cmp-ConstantRef not supported here, should be eliminated earlier");
            Preconditions.checkState(!(leftChild.isConstant() && rightChild.isVariable()),
                    "Constant-cmp-Column not supported here, should be deal earlier");
            // For CastOperator, we need use child as column statistics
            leftChild = getChildForCastOperator(leftChild);
            rightChild = getChildForCastOperator(rightChild);

            ColumnStatistic leftColumnStatistic = getExpressionStatistic(leftChild);
            ColumnStatistic rightColumnStatistic = getExpressionStatistic(rightChild);

            if (leftChild.isVariable()) {
                Optional<ColumnRefOperator> leftChildOpt;
                // only columnRefOperator could add column statistic to statistics
                leftChildOpt = leftChild.isColumnRef() ? Optional.of((ColumnRefOperator) leftChild) : Optional.empty();

                if (rightChild.isConstant()) {
                    OptionalDouble constant = rightColumnStatistic.isUnknown() ? OptionalDouble.empty() :
                            OptionalDouble.of(rightColumnStatistic.getMaxValue());
                    return BinaryPredicateStatisticCalculator.estimateColumnToConstantComparison(leftChildOpt,
                            leftColumnStatistic, predicate, constant, statistics);
                } else {
                    return BinaryPredicateStatisticCalculator.estimateColumnToColumnComparison(
                            leftChild, leftColumnStatistic,
                            rightChild, rightColumnStatistic,
                            predicate, statistics);
                }
            } else {
                // constant compare constant
                double outputRowCount = statistics.getOutputRowCount() *
                        StatisticsEstimateCoefficient.CONSTANT_TO_CONSTANT_PREDICATE_COEFFICIENT;
                return Statistics.buildFrom(statistics).setOutputRowCount(outputRowCount).build();
            }
        }

        @Override
        public Statistics visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }

            if (predicate.isAnd()) {
                Preconditions.checkState(predicate.getChildren().size() == 2);
                Statistics leftStatistics = predicate.getChild(0).accept(this, null);
                return predicate.getChild(1)
                        .accept(new PredicateStatisticsCalculator.PredicateStatisticsCalculatingVisitor(leftStatistics),
                                null);
            } else if (predicate.isOr()) {
                Preconditions.checkState(predicate.getChildren().size() == 2);
                Statistics leftStatistics = predicate.getChild(0).accept(this, null);
                Statistics rightStatistics = predicate.getChild(1).accept(this, null);
                Statistics andStatistics = predicate.getChild(1)
                        .accept(new PredicateStatisticsCalculator.PredicateStatisticsCalculatingVisitor(leftStatistics),
                                null);
                double rowCount = leftStatistics.getOutputRowCount() + rightStatistics.getOutputRowCount() -
                        andStatistics.getOutputRowCount();
                rowCount = Math.min(rowCount, statistics.getOutputRowCount());
                return computeOrPredicateStatistics(leftStatistics, rightStatistics, statistics, rowCount);
            } else {
                Preconditions.checkState(predicate.getChildren().size() == 1);
                Statistics inputStatistics = predicate.getChild(0).accept(this, null);
                double rowCount = Math.max(0, statistics.getOutputRowCount() - inputStatistics.getOutputRowCount());
                return Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build();
            }
        }

        public Statistics computeOrPredicateStatistics(Statistics leftStatistics, Statistics rightStatistics,
                                                       Statistics inputStatistics, double rowCount) {
            Statistics.Builder builder = Statistics.buildFrom(inputStatistics);
            builder.setOutputRowCount(rowCount);
            leftStatistics.getColumnStatistics().forEach((columnRefOperator, columnStatistic) -> {
                ColumnStatistic.Builder columnBuilder = ColumnStatistic.buildFrom(columnStatistic);
                ColumnStatistic rightColumnStatistic = rightStatistics.getColumnStatistic(columnRefOperator);
                columnBuilder.setMinValue(Math.min(columnStatistic.getMinValue(), rightColumnStatistic.getMinValue()));
                columnBuilder.setMaxValue(Math.max(columnStatistic.getMaxValue(), rightColumnStatistic.getMaxValue()));
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
                Preconditions.checkState(operator.getChildren().size() == 1);
                operator = getChildForCastOperator(operator.getChild(0));
            }
            return operator;
        }

        private ColumnStatistic getExpressionStatistic(ScalarOperator operator) {
            return ExpressionStatisticCalculator.calculate(operator, statistics);
        }
    }
}
