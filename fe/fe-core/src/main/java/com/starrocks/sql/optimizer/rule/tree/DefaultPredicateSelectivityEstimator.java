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


package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.BinaryType;
import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.ExpressionStatisticCalculator;
import com.starrocks.sql.optimizer.statistics.PredicateStatisticsCalculator;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Optional;

/**
 * Estimate Predicate selectivity which value between 0.0 and 1.0
 * mixed PredicateStatisticsCalculator & Hive FilterSelectivityEstimator.java & Spark FilterEstimation.scala
 */
public class DefaultPredicateSelectivityEstimator {

    private static final double SELECTIVITY_MIN = 0.0;

    private static final double SELECTIVITY_MAX = 1.0;
    //"Database Systems, the complete book". Suggest this:
    private static final double SELECTIVITY_EQUALS = 1.0 / 3.0;

    /**
     * Returns a percentage of predicate selectivity
     * If it's a single predicate, we estimate the percentage directly.
     * If it's a compound predicate, it is decomposed into multiple single predicate linked with
     * AND, OR, NOT.
     *
     * @param predicate  the predicate which need to estimate
     * @param statistics the statistics which collected
     * @return an optional double value to get the percentage of predicate selectivity
     * It returns 1.0 if the condition is not supported.
     */
    public double estimate(ScalarOperator predicate, Statistics statistics) {
        // if Optional is null, return SELECTIVITY_MAX by default.
        return predicate.accept(new PredicateSelectivityEstimatorVisitor(statistics), null).orElse(SELECTIVITY_MAX);
    }

    private static class PredicateSelectivityEstimatorVisitor extends ScalarOperatorVisitor<Optional<Double>, Void> {

        private final Statistics statistics;

        public PredicateSelectivityEstimatorVisitor(Statistics statistics) {
            this.statistics = statistics;
        }

        @Override
        public Optional<Double> visit(ScalarOperator scalarOperator, Void context) {
            //follow StatisticsCalculator line 1242 logic default
            Statistics estimatedStatistics =
                    PredicateStatisticsCalculator.statisticsCalculate(scalarOperator, statistics);
            return Optional.of(estimatedStatistics.getOutputRowCount() / statistics.getOutputRowCount());
        }

        @Override
        public Optional<Double> visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            Preconditions.checkState(predicate.getChildren().size() == 2);
            BinaryType binaryType = predicate.getBinaryType();
            ScalarOperator leftChild = predicate.getChild(0);
            ScalarOperator rightChild = predicate.getChild(1);
            ColumnStatistic leftChildStatistic = ExpressionStatisticCalculator.calculate(leftChild, statistics);
            ColumnStatistic rightChildStatistic = ExpressionStatisticCalculator.calculate(rightChild, statistics);
            if (leftChildStatistic.hasNaNValue() || rightChildStatistic.hasNaNValue()) {
                // because is not in range
                return Optional.of(SELECTIVITY_MIN);
            } else if (leftChildStatistic.isUnknown() || rightChildStatistic.isUnknown()) {
                //because can't check
                return Optional.of(SELECTIVITY_MAX);
            }
            if (binaryType.equals(BinaryType.EQ)) {
                return computeEquals(binaryType, leftChild, rightChild, leftChildStatistic, rightChildStatistic);
            } else if (binaryType.equals(BinaryType.NE)) {
                Optional<Double> op = computeEquals(BinaryType.EQ,
                        leftChild, rightChild, leftChildStatistic, rightChildStatistic);
                return op.map(aDouble -> 1 - aDouble);
            } else if (binaryType.equals(BinaryType.LT)) {
                return computeLessOrGreat(binaryType, leftChild, rightChild, leftChildStatistic, rightChildStatistic);
            } else if (binaryType.equals(BinaryType.LE)) {
                return computeLessOrGreat(binaryType, leftChild, rightChild, leftChildStatistic, rightChildStatistic);
            } else if (binaryType.equals(BinaryType.GT)) {
                return computeLessOrGreat(binaryType, leftChild, rightChild, leftChildStatistic, rightChildStatistic);
            } else if (binaryType.equals(BinaryType.GE)) {
                return computeLessOrGreat(binaryType, leftChild, rightChild, leftChildStatistic, rightChildStatistic);
            } else if (binaryType.equals(BinaryType.EQ_FOR_NULL)) {
                return computeEquals(binaryType, leftChild, rightChild, leftChildStatistic, rightChildStatistic);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public Optional<Double> visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (predicate.isAnd()) {
                Optional<Double> leftSelectivity = predicate.getChild(0).accept(this, null);
                Optional<Double> rightSelectivity = predicate.getChild(1).accept(this, null);
                if (leftSelectivity.isPresent() && rightSelectivity.isPresent()) {
                    return Optional.of(leftSelectivity.get() * rightSelectivity.get());
                }
                return Optional.empty();
            } else if (predicate.isOr()) {
                Optional<Double> leftSelectivity = predicate.getChild(0).accept(this, null);
                Optional<Double> rightSelectivity = predicate.getChild(1).accept(this, null);
                if (leftSelectivity.isPresent() && rightSelectivity.isPresent()) {
                    return Optional.of(
                            leftSelectivity.get() + rightSelectivity.get() -
                                    (leftSelectivity.get() * rightSelectivity.get()));
                }
                return Optional.empty();

            } else if (predicate.isNot()) {
                Optional<Double> leftSelectivity = predicate.getChild(0).accept(this, null);
                if (leftSelectivity.isPresent()) {
                    return Optional.of(1.0 - leftSelectivity.get());
                }

            }
            return Optional.empty();
        }

        /**
         * Returns a percentage of  predicate which in EQ, EQ_FOR_NULL
         *
         * @param binaryType          binaryType which is EQ or EQ_FOR_NULL
         * @param leftChild           left ScalarOperator
         * @param rightChild          right ScalarOperator
         * @param leftChildStatistic  column statistic for left ScalarOperator
         * @param rightChildStatistic column statistic for right ScalarOperator
         * @return an optional double value to get the percentage of predicate selectivity
         */
        private Optional<Double> computeEquals(BinaryType binaryType,
                                               ScalarOperator leftChild, ScalarOperator rightChild,
                                               ColumnStatistic leftChildStatistic,
                                               ColumnStatistic rightChildStatistic) {
            if (rightChild.isConstantRef()) {
                // can optimize if we have histogram
                if (leftChild.getType().isNumericType()
                        || leftChild.getType().isDateType()
                        || leftChild.getType().isBoolean()) {
                    double doubleValue = ConstantOperatorUtils.getDoubleValue(((ConstantOperator) rightChild));
                    if (doubleValue < leftChildStatistic.getMinValue() ||
                            doubleValue > leftChildStatistic.getMaxValue()) {
                        return Optional.of(SELECTIVITY_MIN);
                    } else {
                        if (leftChildStatistic.getDistinctValuesCount() == 0) {
                            if (leftChildStatistic.getNullsFraction() > 0) {
                                // column data is null
                                return Optional.of(SELECTIVITY_MIN);
                            } else {
                                // column no data
                                return Optional.empty();
                            }
                        } else {
                            return Optional.of(1.0 / leftChildStatistic.getDistinctValuesCount());
                        }
                    }
                } else {
                    // string、binary no support
                    return Optional.empty();
                }
            } else {
                return evaluateBinaryForExpression(binaryType, leftChildStatistic, rightChildStatistic);
            }
        }

        /**
         * Returns a percentage of predicate which in LT,LE,GT,GE
         *
         * @param binaryType          binaryType which in LT,LE,GT,GE
         * @param leftChild           left ScalarOperator
         * @param rightChild          right ScalarOperator
         * @param leftChildStatistic  column statistic for left ScalarOperator
         * @param rightChildStatistic column statistic for right ScalarOperator
         * @return an optional double value to get the percentage of predicate selectivity
         */
        private Optional<Double> computeLessOrGreat(BinaryType binaryType,
                                                    ScalarOperator leftChild, ScalarOperator rightChild,
                                                    ColumnStatistic leftChildStatistic,
                                                    ColumnStatistic rightChildStatistic) {
            if (rightChild.isConstantRef()) {
                if (leftChild.getType().isNumericType()
                        || leftChild.getType().isDateType()
                        || leftChild.getType().isBoolean()) {
                    double doubleValue = ConstantOperatorUtils.getDoubleValue(((ConstantOperator) rightChild));
                    if (isLessOrGreatValueNoOverlap(doubleValue, leftChildStatistic, binaryType)) {
                        return Optional.of(SELECTIVITY_MIN);
                    } else if (isLessOrGreatValueCompleteOverlap(doubleValue, leftChildStatistic, binaryType)) {
                        return Optional.of(SELECTIVITY_MAX);
                    } else {
                        return evaluateLessOrGreatInRange(binaryType, doubleValue, leftChildStatistic);
                    }
                } else {
                    return Optional.empty();
                }
            } else {
                return evaluateBinaryForExpression(binaryType, leftChildStatistic, rightChildStatistic);
            }
        }

        /**
         * Check right child value is no overlap left child value
         *
         * @param doubleValue     right ScalarOperator value
         * @param columnStatistic column statistic for left ScalarOperator
         * @param binaryType      binaryType which in LT,LE,GT,GE
         * @return if no overlap return true, else return false
         */
        private boolean isLessOrGreatValueNoOverlap(double doubleValue, ColumnStatistic columnStatistic,
                                                    BinaryType binaryType) {
            if (binaryType.equals(BinaryType.LT) &&
                    doubleValue <= columnStatistic.getMinValue()) {
                return true;
            } else if (binaryType.equals(BinaryType.LE) &&
                    doubleValue < columnStatistic.getMinValue()) {
                return true;
            } else if (binaryType.equals(BinaryType.GT) &&
                    doubleValue >= columnStatistic.getMaxValue()) {
                return true;
            } else if (binaryType.equals(BinaryType.GE) &&
                    doubleValue > columnStatistic.getMaxValue()) {
                return true;
            } else {
                return false;
            }
        }

        /**
         * Check right child value is overlap left child value
         *
         * @param doubleValue     right ScalarOperator value
         * @param columnStatistic column statistic for left ScalarOperator
         * @param binaryType      binaryType which in LT,LE,GT,GE
         * @return if overlap return true, else return false.
         */
        private boolean isLessOrGreatValueCompleteOverlap(double doubleValue,
                                                          ColumnStatistic columnStatistic,
                                                          BinaryType binaryType) {
            if (binaryType.equals(BinaryType.LT) &&
                    doubleValue > columnStatistic.getMaxValue()) {
                return true;
            } else if (binaryType.equals(BinaryType.LE) &&
                    doubleValue >= columnStatistic.getMaxValue()) {
                return true;
            } else if (binaryType.equals(BinaryType.GT) &&
                    doubleValue < columnStatistic.getMinValue()) {
                return true;
            } else if (binaryType.equals(BinaryType.GE) &&
                    doubleValue <= columnStatistic.getMinValue()) {
                return true;
            } else {
                return false;
            }
        }

        /**
         * Returns a percentage of predicate which the left and right child values
         * outside the no overlap and overlap states
         *
         * @param binaryType         binaryType which in LT,LE,GT,GE
         * @param doubleValue        right ScalarOperator value
         * @param leftChildStatistic column statistic for left ScalarOperator
         * @return an optional double value to get the percentage of predicate selectivity
         */
        private Optional<Double> evaluateLessOrGreatInRange(BinaryType binaryType,
                                                            double doubleValue, ColumnStatistic leftChildStatistic) {
            //can optimize if we have histogram
            if (binaryType.equals(BinaryType.LT)) {
                if (doubleValue == leftChildStatistic.getMaxValue()) {
                    return Optional.of(1.0 / leftChildStatistic.getDistinctValuesCount());
                } else {
                    return Optional.of(
                            (doubleValue - leftChildStatistic.getMinValue()) /
                                    (leftChildStatistic.getMaxValue() - leftChildStatistic.getMinValue()));
                }
            } else if (binaryType.equals(BinaryType.LE)) {
                if (doubleValue == leftChildStatistic.getMinValue()) {
                    return Optional.of(1.0 / leftChildStatistic.getDistinctValuesCount());
                } else {
                    return Optional.of(
                            (doubleValue - leftChildStatistic.getMinValue()) /
                                    (leftChildStatistic.getMaxValue() - leftChildStatistic.getMinValue()));
                }
            } else if (binaryType.equals(BinaryType.GT)) {
                if (doubleValue == leftChildStatistic.getMinValue()) {
                    return Optional.of(1.0 - 1.0 / leftChildStatistic.getDistinctValuesCount());
                } else {
                    return Optional.of(
                            (leftChildStatistic.getMaxValue() - doubleValue) /
                                    (leftChildStatistic.getMaxValue() - leftChildStatistic.getMinValue()));
                }
            } else if (binaryType.equals(BinaryType.GE)) {
                if (doubleValue == leftChildStatistic.getMaxValue()) {
                    return Optional.of(1.0 / leftChildStatistic.getDistinctValuesCount());
                } else {
                    return Optional.of(
                            (leftChildStatistic.getMaxValue() - doubleValue) /
                                    (leftChildStatistic.getMaxValue() - leftChildStatistic.getMinValue()));
                }
            } else {
                return Optional.empty();
            }
        }

        /**
         * Returns a percentage of  predicate which right child is not constant
         *
         * @param binaryType          binaryType
         * @param leftChildStatistic  column statistic for left ScalarOperator
         * @param rightChildStatistic column statistic for right ScalarOperator
         * @return an optional double value to get the percentage of predicate selectivity. if
         */
        private Optional<Double> evaluateBinaryForExpression(
                BinaryType binaryType,
                ColumnStatistic leftChildStatistic, ColumnStatistic rightChildStatistic) {
            double minLeft = leftChildStatistic.getMinValue();
            double maxLeft = leftChildStatistic.getMaxValue();
            double minRight = rightChildStatistic.getMinValue();
            double maxRight = rightChildStatistic.getMaxValue();
            double distinctCountLeft = leftChildStatistic.getDistinctValuesCount();
            double distinctCountRight = rightChildStatistic.getDistinctValuesCount();
            boolean allNotNull =
                    (leftChildStatistic.getNullsFraction() == 0) && (rightChildStatistic.getNullsFraction() == 0);
            // check overlap
            // if no overlap return SELECTIVITY_MIN
            // else if overlap return SELECTIVITY_MAX
            // else return SELECTIVITY_EQUALS.
            if (binaryType.equals(BinaryType.EQ)) {
                if ((maxLeft < minRight) || (maxRight < minLeft)) {
                    return Optional.of(SELECTIVITY_MIN);
                } else if ((minLeft == minRight) && (maxLeft == maxRight) && allNotNull
                        && (distinctCountLeft == distinctCountRight)) {
                    return Optional.of(SELECTIVITY_MAX);
                } else {
                    return Optional.of(SELECTIVITY_EQUALS);
                }
            } else if (binaryType.equals(BinaryType.LT)) {
                if (minLeft >= maxRight) {
                    return Optional.of(SELECTIVITY_MIN);
                } else if ((maxLeft < minRight) && allNotNull) {
                    return Optional.of(SELECTIVITY_MAX);
                } else {
                    return Optional.of(SELECTIVITY_EQUALS);
                }
            } else if (binaryType.equals(BinaryType.LE)) {
                if (minLeft > maxRight) {
                    return Optional.of(SELECTIVITY_MIN);
                } else if ((maxLeft <= minRight) && allNotNull) {
                    return Optional.of(SELECTIVITY_MAX);
                } else {
                    return Optional.of(SELECTIVITY_EQUALS);
                }
            } else if (binaryType.equals(BinaryType.GT)) {
                if (maxLeft <= minRight) {
                    return Optional.of(SELECTIVITY_MIN);
                } else if ((minLeft > maxRight) && allNotNull) {
                    return Optional.of(SELECTIVITY_MAX);
                } else {
                    return Optional.of(SELECTIVITY_EQUALS);
                }
            } else if (binaryType.equals(BinaryType.GE)) {
                if (maxLeft < minRight) {
                    return Optional.of(SELECTIVITY_MIN);
                } else if ((minLeft >= maxRight) && allNotNull) {
                    return Optional.of(SELECTIVITY_MAX);
                } else {
                    return Optional.of(SELECTIVITY_EQUALS);
                }
            } else if (binaryType.equals(BinaryType.EQ_FOR_NULL)) {
                if (((maxLeft < minRight) || (maxRight < minLeft)) && allNotNull) {
                    return Optional.of(SELECTIVITY_MIN);
                } else if ((minLeft == minRight) && (maxLeft == maxRight) && allNotNull
                        && (distinctCountLeft == distinctCountRight)) {
                    return Optional.of(SELECTIVITY_MAX);
                } else {
                    return Optional.of(SELECTIVITY_EQUALS);
                }
            } else {
                return Optional.of(SELECTIVITY_MAX);
            }
        }

    }

}
