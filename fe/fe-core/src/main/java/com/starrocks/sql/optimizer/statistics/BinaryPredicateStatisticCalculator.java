// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Optional;
import java.util.OptionalDouble;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

public class BinaryPredicateStatisticCalculator {
    public static Statistics estimateColumnToConstantComparison(Optional<ColumnRefOperator> columnRefOperator,
                                                                ColumnStatistic columnStatistic,
                                                                BinaryPredicateOperator predicate,
                                                                OptionalDouble constant,
                                                                Statistics statistics) {
        switch (predicate.getBinaryType()) {
            case EQ:
            case EQ_FOR_NULL:
                return estimateColumnEqualToConstant(columnRefOperator, columnStatistic, constant, statistics);
            case NE:
                return estimateColumnNotEqualToConstant(columnRefOperator, columnStatistic, constant, statistics);
            case LE:
            case LT:
                return estimateColumnLessThanConstant(columnRefOperator, columnStatistic, constant, statistics);
            case GE:
            case GT:
                return estimateColumnGreaterThanConstant(columnRefOperator, columnStatistic, constant, statistics);
            default:
                throw new IllegalArgumentException("unknown binary type: " + predicate.getBinaryType());
        }
    }

    private static Statistics estimateColumnEqualToConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                            ColumnStatistic columnStatistic,
                                                            OptionalDouble constant,
                                                            Statistics statistics) {
        StatisticRangeValues predicateRange;
        if (constant.isPresent()) {
            predicateRange = new StatisticRangeValues(constant.getAsDouble(), constant.getAsDouble(), 1);
        } else {
            predicateRange = new StatisticRangeValues(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }

        return estimatePredicateRange(columnRefOperator, columnStatistic, predicateRange, statistics);
    }

    private static Statistics estimateColumnNotEqualToConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                               ColumnStatistic columnStatistic,
                                                               OptionalDouble constant,
                                                               Statistics statistics) {
        StatisticRangeValues predicateRange;
        if (constant.isPresent()) {
            predicateRange = new StatisticRangeValues(constant.getAsDouble(), constant.getAsDouble(), 1);
        } else {
            predicateRange = new StatisticRangeValues(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }

        StatisticRangeValues columnRange = StatisticRangeValues.from(columnStatistic);
        StatisticRangeValues intersectRange = columnRange.intersect(predicateRange);

        double predicateFactor = 1.0 - columnRange.overlapPercentWith(intersectRange);
        double rowCount = statistics.getOutputRowCount() * (1 - columnStatistic.getNullsFraction()) * predicateFactor;
        // TODO(ywb) use origin column distinct values as new column statistics now, we should re-compute column
        //  distinct values actually.
        ColumnStatistic newEstimateColumnStatistics = ColumnStatistic.builder().
                setAverageRowSize(columnStatistic.getAverageRowSize()).
                setMaxValue(intersectRange.getHigh()).
                setMinValue(intersectRange.getLow()).
                setDistinctValuesCount(columnStatistic.getDistinctValuesCount()).
                setType(columnStatistic.getType()).
                build();
        return columnRefOperator.map(operator -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                        addColumnStatistic(operator, newEstimateColumnStatistics).build()).
                orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
    }

    private static Statistics estimateColumnLessThanConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                             ColumnStatistic columnStatistic,
                                                             OptionalDouble constant,
                                                             Statistics statistics) {
        StatisticRangeValues predicateRange =
                new StatisticRangeValues(NEGATIVE_INFINITY, constant.orElse(POSITIVE_INFINITY), NaN);
        return estimatePredicateRange(columnRefOperator, columnStatistic, predicateRange, statistics);
    }

    private static Statistics estimateColumnGreaterThanConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                                ColumnStatistic columnStatistic,
                                                                OptionalDouble constant,
                                                                Statistics statistics) {
        StatisticRangeValues predicateRange =
                new StatisticRangeValues(constant.orElse(NEGATIVE_INFINITY), POSITIVE_INFINITY, NaN);
        return estimatePredicateRange(columnRefOperator, columnStatistic, predicateRange, statistics);
    }

    public static Statistics estimateColumnToColumnComparison(ScalarOperator leftColumn,
                                                              ColumnStatistic leftColumnStatistic,
                                                              ScalarOperator rightColumn,
                                                              ColumnStatistic rightColumnStatistic,
                                                              BinaryPredicateOperator predicate,
                                                              Statistics statistics) {
        switch (predicate.getBinaryType()) {
            case EQ:
                return estimateColumnEqualToColumn(leftColumn, leftColumnStatistic,
                        rightColumn, rightColumnStatistic, statistics, false);
            case EQ_FOR_NULL:
                return estimateColumnEqualToColumn(leftColumn, leftColumnStatistic,
                        rightColumn, rightColumnStatistic, statistics, true);
            case NE:
                return estimateColumnNotEqualToColumn(leftColumnStatistic, rightColumnStatistic, statistics);
            case LE:
            case GE:
            case LT:
            case GT:
                // 0.5 is unknown filter coefficient
                double rowCount = statistics.getOutputRowCount() * 0.5;
                return Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build();
            default:
                throw new IllegalArgumentException("unknown binary type: " + predicate.getBinaryType());
        }
    }

    public static Statistics estimateColumnEqualToColumn(ScalarOperator leftColumn,
                                                         ColumnStatistic leftColumnStatistic,
                                                         ScalarOperator rightColumn,
                                                         ColumnStatistic rightColumnStatistic,
                                                         Statistics statistics,
                                                         boolean isEqualForNull) {
        double leftDistinctValuesCount = leftColumnStatistic.getDistinctValuesCount();
        double rightDistinctValuesCount = rightColumnStatistic.getDistinctValuesCount();
        double selectivity = 1.0 / Math.max(1, Math.max(leftDistinctValuesCount, rightDistinctValuesCount));
        double rowCount = statistics.getOutputRowCount() * selectivity *
                (isEqualForNull ? 1 :
                        (1 - leftColumnStatistic.getNullsFraction()) * (1 - rightColumnStatistic.getNullsFraction()));

        StatisticRangeValues intersect = StatisticRangeValues.from(leftColumnStatistic)
                .intersect(StatisticRangeValues.from(rightColumnStatistic));
        ColumnStatistic.Builder newEstimateColumnStatistics = ColumnStatistic.builder().
                setMaxValue(intersect.getHigh()).
                setMinValue(intersect.getLow()).
                setDistinctValuesCount(intersect.getDistinctValues());

        ColumnStatistic newLeftStatistic;
        ColumnStatistic newRightStatistic;
        if (!isEqualForNull) {
            newEstimateColumnStatistics.setNullsFraction(0);
            newLeftStatistic = newEstimateColumnStatistics
                    .setAverageRowSize(leftColumnStatistic.getAverageRowSize()).build();
            newRightStatistic = newEstimateColumnStatistics
                    .setAverageRowSize(rightColumnStatistic.getAverageRowSize()).build();
        } else {
            newLeftStatistic = newEstimateColumnStatistics
                    .setAverageRowSize(leftColumnStatistic.getAverageRowSize())
                    .setNullsFraction(leftColumnStatistic.getNullsFraction())
                    .build();
            newRightStatistic = newEstimateColumnStatistics
                    .setAverageRowSize(rightColumnStatistic.getAverageRowSize())
                    .setNullsFraction(rightColumnStatistic.getNullsFraction())
                    .build();
        }

        Statistics.Builder builder = Statistics.buildFrom(statistics);
        if (leftColumn instanceof ColumnRefOperator) {
            builder.addColumnStatistic((ColumnRefOperator) leftColumn, newLeftStatistic);
        }
        if (rightColumn instanceof ColumnRefOperator) {
            builder.addColumnStatistic((ColumnRefOperator) rightColumn, newRightStatistic);
        }
        builder.setOutputRowCount(rowCount);
        return builder.build();
    }

    public static Statistics estimateColumnNotEqualToColumn(
            ColumnStatistic leftColumn,
            ColumnStatistic rightColumn,
            Statistics statistics) {
        double leftDistinctValuesCount = leftColumn.getDistinctValuesCount();
        double rightDistinctValuesCount = rightColumn.getDistinctValuesCount();
        double selectivity = 1.0 / Math.max(1, Math.max(leftDistinctValuesCount, rightDistinctValuesCount));

        double rowCount = statistics.getOutputRowCount();
        // If any ColumnStatistic is default, give a default selectivity
        if (leftColumn.isUnknown() || rightColumn.isUnknown()) {
            rowCount = rowCount * 0.8;
        } else {
            rowCount = rowCount * (1.0 - selectivity)
                    * (1 - leftColumn.getNullsFraction()) * (1 - rightColumn.getNullsFraction());
        }
        return Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build();
    }

    public static Statistics estimatePredicateRange(Optional<ColumnRefOperator> columnRefOperator,
                                                    ColumnStatistic columnStatistic,
                                                    StatisticRangeValues predicateRange,
                                                    Statistics statistics) {
        StatisticRangeValues columnRange = StatisticRangeValues.from(columnStatistic);
        StatisticRangeValues intersectRange = columnRange.intersect(predicateRange);

        double predicateFactor = columnRange.overlapPercentWith(intersectRange);
        double rowCount = statistics.getOutputRowCount() * (1 - columnStatistic.getNullsFraction()) * predicateFactor;
        // TODO(ywb) use origin column distinct values as new column statistics now, we should re-compute column
        //  distinct values actually.
        ColumnStatistic newEstimateColumnStatistics = ColumnStatistic.builder().
                setAverageRowSize(columnStatistic.getAverageRowSize()).
                setMaxValue(intersectRange.getHigh()).
                setMinValue(intersectRange.getLow()).
                setNullsFraction(0).
                setDistinctValuesCount(columnStatistic.getDistinctValuesCount()).
                setType(columnStatistic.getType()).
                build();
        return columnRefOperator.map(operator -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                        addColumnStatistic(operator, newEstimateColumnStatistics).build()).
                orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
    }
}
