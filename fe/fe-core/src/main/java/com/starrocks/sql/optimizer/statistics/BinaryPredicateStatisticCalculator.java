// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
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
                                                             Statistics statistics,
                                                             BinaryPredicateOperator.BinaryType binaryType) {
        StatisticRangeValues predicateRange =
                new StatisticRangeValues(NEGATIVE_INFINITY, constant.orElse(POSITIVE_INFINITY), NaN);
        if (columnStatistic.getHistogram() != null) {
            double rowCount = estimatePredicateRangeWithHistogram(columnStatistic, constant, statistics,
                    binaryType.equals(BinaryPredicateOperator.BinaryType.LE));

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
                                                                OptionalDouble constant,
                                                                Statistics statistics,
                                                                BinaryPredicateOperator.BinaryType binaryType) {
        StatisticRangeValues predicateRange =
                new StatisticRangeValues(constant.orElse(NEGATIVE_INFINITY), POSITIVE_INFINITY, NaN);

        if (columnStatistic.getHistogram() != null) {
            double rowCount = statistics.getOutputRowCount() - estimatePredicateRangeWithHistogram(
                    columnStatistic, constant, statistics, binaryType.equals(BinaryPredicateOperator.BinaryType.GE));

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
        if (constantDouble < histogram.getBuckets().get(0).getLower()) {
            return 0;
        }

        if (constantDouble > histogram.getBuckets().get(histogram.getBuckets().size() - 1).getUpper()) {
            return statistics.getOutputRowCount();
        }

        for (int i = 0; i < histogram.getBuckets().size(); i++) {
            Bucket bucket = histogram.getBuckets().get(i);

            if (bucket.getUpper() >= constantDouble && bucket.getLower() <= constantDouble) {
                StatisticRangeValues bucketRange = new StatisticRangeValues(bucket.getLower(), bucket.getUpper(), NaN);
                StatisticRangeValues columnRange = new StatisticRangeValues(bucket.getLower(), constantDouble, NaN);
                StatisticRangeValues intersectRange = columnRange.intersect(bucketRange);
                double predicateFactor = columnRange.overlapPercentWith(intersectRange);

                long previousTotalRowCount = i == 0 ? 0 : histogram.getBuckets().get(i - 1).getCount();

                double bucketRowCount;
                if (containUpper && constantDouble == bucket.getUpper()) {
                    bucketRowCount = bucket.getCount() - previousTotalRowCount;
                } else {
                    long bucketTotalRows = bucket.getCount() - bucket.getUpperRepeats() - previousTotalRowCount;
                    bucketRowCount = bucketTotalRows * predicateFactor;
                }

                return previousTotalRowCount + (long) bucketRowCount;
            }
        }

        return statistics.getOutputRowCount();
    }

    public static ColumnStatistic estimateColumnStatisticsWithHistogram(ColumnStatistic columnStatistic,
                                                                        StatisticRangeValues predicateRange,
                                                                        OptionalDouble constantOptional,
                                                                        BinaryPredicateOperator.BinaryType binaryType) {
        Histogram histogram = columnStatistic.getHistogram();
        double constant = constantOptional.getAsDouble();

        List<Bucket> newBucketList = new ArrayList<>();

        if (binaryType.equals(BinaryPredicateOperator.BinaryType.LE)
                || binaryType.equals(BinaryPredicateOperator.BinaryType.LT)) {
            for (int i = 0; i < histogram.getBuckets().size(); ++i) {
                Bucket bucket = histogram.getBuckets().get(i);
                if (bucket.getUpper() < constant
                        || (binaryType.equals(BinaryPredicateOperator.BinaryType.LE) && bucket.getUpper() == constant)) {
                    newBucketList.add(bucket);
                } else if (bucket.getLower() <= constant && bucket.getUpper() >= constant) {
                    StatisticRangeValues bucketRange = new StatisticRangeValues(bucket.getLower(), bucket.getUpper(), NaN);
                    StatisticRangeValues columnRangeInBucket = new StatisticRangeValues(NEGATIVE_INFINITY, constant, NaN);
                    StatisticRangeValues intersectRangeInBucket = columnRangeInBucket.intersect(bucketRange);

                    double bucketRowCount = (bucket.getCount()
                            - (i == 0 ? 0 : histogram.getBuckets().get(i - 1).getCount()) - bucket.getUpperRepeats())
                            * bucketRange.overlapPercentWith(intersectRangeInBucket);
                    if (bucketRowCount == 0 || Double.isNaN(bucketRowCount)) {
                        continue;
                    }

                    Bucket newBucket = new Bucket(bucket.getLower(), constant, (long) bucketRowCount, 1L);
                    newBucketList.add(newBucket);
                }
            }
        }

        if (binaryType.equals(BinaryPredicateOperator.BinaryType.GE)
                || binaryType.equals(BinaryPredicateOperator.BinaryType.GT)) {
            for (int i = 0; i < histogram.getBuckets().size(); ++i) {
                Bucket bucket = histogram.getBuckets().get(i);
                if (bucket.getLower() > constant
                        || (binaryType.equals(BinaryPredicateOperator.BinaryType.GE) && bucket.getLower() == constant)) {
                    newBucketList.add(bucket);
                } else if (bucket.getLower() <= constant && bucket.getUpper() >= constant) {
                    if (bucket.getLower().equals(bucket.getUpper()) && bucket.getUpper() == constant) {
                        continue;
                    }

                    StatisticRangeValues bucketRange = new StatisticRangeValues(bucket.getLower(), bucket.getUpper(), NaN);
                    StatisticRangeValues columnRangeInBucket = new StatisticRangeValues(constant, POSITIVE_INFINITY, NaN);
                    StatisticRangeValues intersectRangeInBucket = columnRangeInBucket.intersect(bucketRange);

                    double bucketRowCount = (bucket.getCount() - (i == 0 ? 0 : histogram.getBuckets().get(i - 1).getCount()))
                            * bucketRange.overlapPercentWith(intersectRangeInBucket);
                    Bucket newBucket = new Bucket(bucket.getLower(), constant, (long) bucketRowCount, bucket.getUpperRepeats());
                    newBucketList.add(newBucket);
                }
            }
        }

        StatisticRangeValues columnRange = StatisticRangeValues.from(columnStatistic);
        StatisticRangeValues intersectRange = columnRange.intersect(predicateRange);
        ColumnStatistic.Builder newEstimateColumnStatistics = ColumnStatistic.builder().
                setAverageRowSize(columnStatistic.getAverageRowSize()).
                setMaxValue(intersectRange.getHigh()).
                setMinValue(intersectRange.getLow()).
                setNullsFraction(0).
                setDistinctValuesCount(columnStatistic.getDistinctValuesCount()).
                setType(columnStatistic.getType());
        Histogram newHistogram = new Histogram(newBucketList);
        newEstimateColumnStatistics.setHistogram(newHistogram);

        return newEstimateColumnStatistics.build();
    }

    public static ColumnStatistic estimateColumnStatisticsWithHistogram(ColumnStatistic columnStatistic,
                                                                        StatisticRangeValues predicateRange,
                                                                        BinaryPredicateOperator.BinaryType binaryType) {

        boolean isLessEqual = binaryType.equals(BinaryPredicateOperator.BinaryType.LE);
        boolean isGreaterEqual = binaryType.equals(BinaryPredicateOperator.BinaryType.GE);

        StatisticRangeValues columnRange = StatisticRangeValues.from(columnStatistic);
        StatisticRangeValues intersectRange = columnRange.intersect(predicateRange);
        ColumnStatistic.Builder newEstimateColumnStatistics = ColumnStatistic.builder().
                setAverageRowSize(columnStatistic.getAverageRowSize()).
                setMaxValue(intersectRange.getHigh()).
                setMinValue(intersectRange.getLow()).
                setNullsFraction(0).
                setDistinctValuesCount(columnStatistic.getDistinctValuesCount()).
                setType(columnStatistic.getType());

        double min = intersectRange.getLow();
        double max = intersectRange.getHigh();

        Histogram histogram = columnStatistic.getHistogram();
        List<Bucket> newBucketList = new ArrayList<>();
        for (Bucket bucket : histogram.getBuckets()) {
            if ((isGreaterEqual && bucket.getLower() <= min && bucket.getUpper() >= min)
                    || (!isGreaterEqual && bucket.getLower() < min && bucket.getUpper() > min)) {
                StatisticRangeValues bucketRange = new StatisticRangeValues(bucket.getLower(), bucket.getUpper(), NaN);
                StatisticRangeValues columnRangeInBucket = new StatisticRangeValues(min, bucket.getUpper(), NaN);
                StatisticRangeValues intersectRangeInBucket = columnRange.intersect(columnRangeInBucket);
                double bucketRowCount = bucket.getCount() * bucketRange.overlapPercentWith(intersectRangeInBucket);
                Bucket newBucket = new Bucket(min, bucket.getUpper(), (long) bucketRowCount, bucket.getUpperRepeats());
                newBucketList.add(newBucket);
            } else if ((isLessEqual && bucket.getLower() <= max && bucket.getUpper() >= max)
                    || (!isLessEqual && bucket.getLower() < max && bucket.getUpper() > max)) {
                StatisticRangeValues bucketRange = new StatisticRangeValues(bucket.getLower(), bucket.getUpper(), NaN);
                StatisticRangeValues columnRangeInBucket = new StatisticRangeValues(bucket.getLower(), max, NaN);
                StatisticRangeValues intersectRangeInBucket = columnRange.intersect(columnRangeInBucket);
                double bucketRowCount = bucket.getCount() * bucketRange.overlapPercentWith(intersectRangeInBucket);
                Bucket newBucket = new Bucket(bucket.getLower(), max, (long) bucketRowCount, 1L);
                newBucketList.add(newBucket);
            } else if ((isLessEqual || isGreaterEqual) && bucket.getLower() >= min && bucket.getUpper() <= max) {
                newBucketList.add(bucket);
            } else if (bucket.getLower() > min && bucket.getUpper() < max) {
                newBucketList.add(bucket);
            }
        }

        Histogram newHistogram = new Histogram(newBucketList);
        newEstimateColumnStatistics.setHistogram(newHistogram);

        return newEstimateColumnStatistics.build();
    }
}
