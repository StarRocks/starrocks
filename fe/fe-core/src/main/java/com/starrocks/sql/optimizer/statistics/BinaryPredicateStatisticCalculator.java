// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.sum;
import static java.lang.Math.max;

public class BinaryPredicateStatisticCalculator {
    public static Statistics estimateColumnToConstantComparison(Optional<ColumnRefOperator> columnRefOperator,
                                                                ColumnStatistic columnStatistic,
                                                                BinaryPredicateOperator predicate,
                                                                ConstantOperator constant,
                                                                Statistics statistics) {
        switch (predicate.getBinaryType()) {
            case EQ:
            case EQ_FOR_NULL:
                return estimateColumnEqualToConstant(columnRefOperator, columnStatistic, constant, statistics);
            case NE:
                return estimateColumnNotEqualToConstant(columnRefOperator, columnStatistic, constant, statistics);
            case LE:
            case LT:
                return estimateColumnLessThanConstant(columnRefOperator, columnStatistic, constant, statistics,
                        predicate.getBinaryType());
            case GE:
            case GT:
                return estimateColumnGreaterThanConstant(columnRefOperator, columnStatistic, constant, statistics,
                        predicate.getBinaryType());
            default:
                throw new IllegalArgumentException("unknown binary type: " + predicate.getBinaryType());
        }
    }

    private static Statistics estimateColumnEqualToConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                            ColumnStatistic columnStatistic,
                                                            ConstantOperator constant,
                                                            Statistics statistics) {
        double min = NEGATIVE_INFINITY;
        double max = POSITIVE_INFINITY;
        if (!constant.getType().isStringType()) {
            min = constant.getDouble();
            max = constant.getDouble();
        }

        ColumnStatistic estimatedColumnStatistic = ColumnStatistic.builder()
                .setAverageRowSize(columnStatistic.getAverageRowSize())
                .setNullsFraction(0)
                .setMinValue(min)
                .setMaxValue(max)
                .setDistinctValuesCount(1)
                .setType(columnStatistic.getType())
                .build();

        double rowCount;
        if (columnStatistic.getHistogram() != null) {
            Map<String, Long> histogramTopN = columnStatistic.getHistogram().getMCV();
            if (histogramTopN.containsKey(constant.getVarchar())) {
                rowCount = histogramTopN.get(constant.getVarchar());
            } else {
                Long mostCommonValuesCount = histogramTopN.values().stream().reduce(Long::sum).orElse(0L);
                double predicateFactor = 1 / max(columnStatistic.getDistinctValuesCount() - histogramTopN.size(), 1);
                rowCount = (statistics.getOutputRowCount() *
                        (1 - columnStatistic.getNullsFraction()) - mostCommonValuesCount) * predicateFactor;
            }
        } else {
            double predicateFactor = 1 / max(columnStatistic.getDistinctValuesCount(), 1);
            rowCount = statistics.getOutputRowCount() * (1 - columnStatistic.getNullsFraction()) * predicateFactor;
        }

        return columnRefOperator.map(operator -> Statistics.buildFrom(statistics)
                        .setOutputRowCount(rowCount).addColumnStatistic(operator, estimatedColumnStatistic).build())
                .orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
    }

    private static Statistics estimateColumnNotEqualToConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                               ColumnStatistic columnStatistic,
                                                               ConstantOperator constant,
                                                               Statistics statistics) {
        ColumnStatistic estimatedColumnStatistic = ColumnStatistic.buildFrom(columnStatistic).setNullsFraction(0).build();
        double rowCount = statistics.getOutputRowCount() -
                estimateColumnEqualToConstant(columnRefOperator, columnStatistic, constant, statistics).getOutputRowCount();

        return columnRefOperator.map(operator -> Statistics.buildFrom(statistics)
                        .setOutputRowCount(rowCount).addColumnStatistic(operator, estimatedColumnStatistic).build())
                .orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
    }

    private static Statistics estimateColumnLessThanConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                             ColumnStatistic columnStatistic,
                                                             ConstantOperator constant,
                                                             Statistics statistics,
                                                             BinaryPredicateOperator.BinaryType binaryType) {
        StatisticRangeValues predicateRange =
                new StatisticRangeValues(NEGATIVE_INFINITY, constant.orElse(POSITIVE_INFINITY), NaN);
        if (columnStatistic.getHistogram() != null) {
            Histogram histogram = columnStatistic.getHistogram();
            double rowCount;
            if (histogram.getMin() != Double.MIN_VALUE) {
                double r1 = estimatePredicateRangeWithHistogram(columnStatistic, constant, statistics,
                        binaryType.equals(BinaryPredicateOperator.BinaryType.LE));
                double r2 = estimatePredicateRangeWithHistogram(columnStatistic, OptionalDouble.of(histogram.getMin()),
                        statistics, !histogram.isContainMin());
                rowCount = r1 - r2;
            } else {
                rowCount = estimatePredicateRangeWithHistogram(columnStatistic, constant, statistics,
                        binaryType.equals(BinaryPredicateOperator.BinaryType.LE));
            }

            ColumnStatistic newEstimateColumnStatistics =
                    estimateColumnStatisticsWithHistogram(columnStatistic, predicateRange, constant, binaryType);
            Statistics.Builder builder = Statistics.buildFrom(statistics).setOutputRowCount(rowCount);
            columnRefOperator.ifPresent(refOperator -> builder.addColumnStatistic(refOperator, newEstimateColumnStatistics));
            return builder.build();
        } else {
            return estimatePredicateRange(columnRefOperator, columnStatistic, predicateRange, statistics);
        }
    }

    private static Statistics estimateColumnGreaterThanConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                                ColumnStatistic columnStatistic,
                                                                ConstantOperator constant,
                                                                Statistics statistics,
                                                                BinaryPredicateOperator.BinaryType binaryType) {
        StatisticRangeValues predicateRange =
                new StatisticRangeValues(constant.orElse(NEGATIVE_INFINITY), POSITIVE_INFINITY, NaN);

        if (columnStatistic.getHistogram() != null) {
            Histogram histogram = columnStatistic.getHistogram();
            double rowCount;
            if (histogram.getMax() != Double.MAX_VALUE) {
                double r1 = estimatePredicateRangeWithHistogram(
                        columnStatistic, OptionalDouble.of(histogram.getMax()), statistics, !histogram.isContainMax());
                double r2 = estimatePredicateRangeWithHistogram(
                        columnStatistic, constant, statistics, !binaryType.equals(BinaryPredicateOperator.BinaryType.GE));
                rowCount = r2 - r1;
            } else {
                rowCount = statistics.getOutputRowCount() - estimatePredicateRangeWithHistogram(
                        columnStatistic, constant, statistics, !binaryType.equals(BinaryPredicateOperator.BinaryType.GE));
            }

            ColumnStatistic newEstimateColumnStatistics =
                    estimateColumnStatisticsWithHistogram(columnStatistic, predicateRange, constant, binaryType);
            Statistics.Builder builder = Statistics.buildFrom(statistics).setOutputRowCount(rowCount);
            columnRefOperator.ifPresent(refOperator -> builder.addColumnStatistic(refOperator, newEstimateColumnStatistics));
            return builder.build();
        } else {
            return estimatePredicateRange(columnRefOperator, columnStatistic, predicateRange, statistics);
        }
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

    public static Statistics estimatePredicatePoint(Statistics statistics,
                                                    ColumnRefOperator columnRefOperator,
                                                    ColumnStatistic columnStatistic) {
        ColumnStatistic c = ColumnStatistic.builder()
                .setAverageRowSize(columnStatistic.getAverageRowSize())
                .setNullsFraction(0)
                .setMinValue(0)
                .setMaxValue(0)
                .setDistinctValuesCount(1)
                .setType(columnStatistic.getType())
                .build();

        return Statistics.buildFrom(statistics)
                .addColumnStatistic(columnRefOperator, c)
                .setOutputRowCount(0)
                .build();
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

    public static double estimatePredicateRangeWithHistogram(ColumnStatistic columnStatistic, OptionalDouble constant,
                                                             Statistics statistics, boolean containUpper) {
        double constantDouble = constant.getAsDouble();
        Histogram histogram = columnStatistic.getHistogram();

        for (int i = 0; i < histogram.getBuckets().size(); i++) {
            Bucket bucket = histogram.getBuckets().get(i);
            long previousTotalRowCount = 0;
            if (i > 0) {
                previousTotalRowCount = histogram.getBuckets().get(i - 1).getCount();
            }

            if (bucket.getUpper() >= constantDouble && bucket.getLower() <= constantDouble) {
                StatisticRangeValues bucketRange = new StatisticRangeValues(bucket.getLower(), bucket.getUpper(), NaN);
                StatisticRangeValues columnRange = new StatisticRangeValues(bucket.getLower(), constantDouble, NaN);
                double predicateFactor = bucketRange.overlapPercentWith(columnRange);

                double bucketRowCount;
                if (containUpper && constantDouble == bucket.getUpper()) {
                    bucketRowCount = bucket.getCount() - previousTotalRowCount;
                } else {
                    long bucketTotalRows = bucket.getCount() - bucket.getUpperRepeats() - previousTotalRowCount;
                    bucketRowCount = bucketTotalRows * predicateFactor;
                }

                return previousTotalRowCount + (long) bucketRowCount;
            } else if (bucket.getLower() > constantDouble) {
                return previousTotalRowCount;
            }
        }

        return statistics.getOutputRowCount();
    }

    public static ColumnStatistic estimateColumnStatisticsWithHistogram(ColumnStatistic columnStatistic,
                                                                        StatisticRangeValues predicateRange,
                                                                        OptionalDouble constantOptional,
                                                                        BinaryPredicateOperator.BinaryType binaryType) {
        Histogram histogram = columnStatistic.getHistogram();

        StatisticRangeValues columnRange = StatisticRangeValues.from(columnStatistic);
        StatisticRangeValues intersectRange = columnRange.intersect(predicateRange);
        ColumnStatistic.Builder newEstimateColumnStatistics = ColumnStatistic.builder().
                setAverageRowSize(columnStatistic.getAverageRowSize()).
                setMaxValue(intersectRange.getHigh()).
                setMinValue(intersectRange.getLow()).
                setNullsFraction(0).
                setDistinctValuesCount(columnStatistic.getDistinctValuesCount()).
                setType(columnStatistic.getType());

        double constant = constantOptional.getAsDouble();
        Histogram newHistogram = new Histogram(histogram.getBuckets(), histogram.getMin(), histogram.isContainMin(),
                histogram.getMax(), histogram.isContainMax());
        if (binaryType.equals(BinaryPredicateOperator.BinaryType.LE)) {
            newHistogram.setMax(constant, true);
        } else if (binaryType.equals(BinaryPredicateOperator.BinaryType.LT)) {
            newHistogram.setMax(constant, false);
        } else if (binaryType.equals(BinaryPredicateOperator.BinaryType.GE)) {
            newHistogram.setMin(constant, true);
        } else if (binaryType.equals(BinaryPredicateOperator.BinaryType.GT)) {
            newHistogram.setMin(constant, false);
        }
        newEstimateColumnStatistics.setHistogram(newHistogram);

        return newEstimateColumnStatistics.build();
    }
}
