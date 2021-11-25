// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

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

        double intersectFactor = columnRange.overlapPercentWith(intersectRange);
        // Column range is infinite if column data type is String or column statistics type is UNKNOWN.
        // If column range is not both infinite, we can calculate the overlap accurately, but if column range is infinite,
        // we assume intersect range is exist in the infinite range，the estimated value of overlap percent will be larger,
        // and the percent of not overlap will be estimated too small.
        if (intersectRange.isBothInfinite()) {
            // If intersect column range is infinite, it need to adjust the intersect factor
            intersectFactor = intersectFactor != 1.0 ? intersectFactor :
                    StatisticsEstimateCoefficient.OVERLAP_INFINITE_RANGE_FILTER_COEFFICIENT;
        }
        double predicateFactor = 1.0 - intersectFactor;

        double rowCount = statistics.getOutputRowCount() * (1 - columnStatistic.getNullsFraction()) * predicateFactor;
        // TODO(ywb) use origin column distinct values as new column statistics now, we should re-compute column
        //  distinct values actually.
        ColumnStatistic newEstimateColumnStatistics =
                ColumnStatistic.buildFrom(columnStatistic).setNullsFraction(0).build();
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

    public static Statistics estimateColumnToColumnComparison(
            ColumnStatistic leftColumn,
            ColumnStatistic rightColumn,
            BinaryPredicateOperator predicate, Statistics statistics) {
        switch (predicate.getBinaryType()) {
            case EQ:
            case EQ_FOR_NULL:
                return estimateColumnEqualToColumn(leftColumn, rightColumn, statistics);
            case NE:
                return estimateColumnNotEqualToColumn(leftColumn, rightColumn, statistics);
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

    public static Statistics estimateColumnEqualToColumn(
            ColumnStatistic leftColumn,
            ColumnStatistic rightColumn,
            Statistics statistics) {
        double leftDistinctValuesCount = leftColumn.getDistinctValuesCount();
        double rightDistinctValuesCount = rightColumn.getDistinctValuesCount();
        double selectivity = 1.0 / Math.max(1, Math.max(leftDistinctValuesCount, rightDistinctValuesCount));
        double rowCount = statistics.getOutputRowCount() * selectivity;
        return Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build();
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
            rowCount = rowCount * (1.0 - selectivity);
        }
        return Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build();
    }

    public static Statistics estimatePredicateRange(Optional<ColumnRefOperator> columnRefOperator,
                                                    ColumnStatistic columnStatistic,
                                                    StatisticRangeValues predicateRange,
                                                    Statistics statistics) {
        StatisticRangeValues columnRange = StatisticRangeValues.from(columnStatistic);
        StatisticRangeValues intersectRange = columnRange.intersect(predicateRange);

        // If column range is not both infinite, we can calculate the overlap accurately, but if column range is infinite,
        // we assume intersect range is exist in the infinite range，the estimated value of overlap percent will be larger.
        // eg.
        //    column_string = 'xxx'
        // column range of column_string is (NEGATIVE_INFINITY, POSITIVE_INFINITY, distinct_val),
        // and column range of 'xxx' is also (NEGATIVE_INFINITY, POSITIVE_INFINITY, 1) because of we can not get min/max
        // value of the string type. we assume that 'xxx' is exist in the column range,
        // so the predicate factor :
        //          pf = 1.0 / distinct_val.
        double predicateFactor = columnRange.overlapPercentWith(intersectRange);
        double rowCount = statistics.getOutputRowCount() * predicateFactor;
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
