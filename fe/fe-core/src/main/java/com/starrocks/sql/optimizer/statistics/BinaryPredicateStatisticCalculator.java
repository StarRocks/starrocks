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

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.StatisticUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

public class BinaryPredicateStatisticCalculator {
    public static Statistics estimateColumnToConstantComparison(Optional<ColumnRefOperator> columnRefOperator,
                                                                ColumnStatistic columnStatistic,
                                                                BinaryPredicateOperator predicate,
                                                                Optional<ConstantOperator> constant,
                                                                Statistics statistics) {
        switch (predicate.getBinaryType()) {
            case EQ:
                return estimateColumnEqualToConstant(columnRefOperator, columnStatistic, constant, statistics);
            case EQ_FOR_NULL:
                if (constant.isPresent() && constant.get().isNull()) {

                    ColumnStatistic estimatedColumnStatistic = ColumnStatistic.builder()
                            .setAverageRowSize(columnStatistic.getAverageRowSize())
                            .setNullsFraction(1)
                            .setMinValue(NEGATIVE_INFINITY)
                            .setMaxValue(POSITIVE_INFINITY)
                            .setDistinctValuesCount(1)
                            .setHistogram(null)
                            .setType(columnStatistic.getType())
                            .build();

                    double rowCount = statistics.getOutputRowCount() * columnStatistic.getNullsFraction();
                    return columnRefOperator.map(operator -> Statistics.buildFrom(statistics)
                                    .setOutputRowCount(rowCount).addColumnStatistic(operator, estimatedColumnStatistic).build())
                            .orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
                } else {
                    return estimateColumnEqualToConstant(columnRefOperator, columnStatistic, constant, statistics);
                }
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
                                                            Optional<ConstantOperator> constant,
                                                            Statistics statistics) {
        if (columnStatistic.getHistogram() == null || !constant.isPresent()) {
            StatisticRangeValues predicateRange;

            if (constant.isPresent()) {
                Optional<Double> c = StatisticUtils.convertStatisticsToDouble(
                        constant.get().getType(), constant.get().toString());
                predicateRange = c.map(aDouble -> new StatisticRangeValues(aDouble, aDouble, 1))
                        .orElseGet(() -> new StatisticRangeValues(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1));
            } else {
                predicateRange = new StatisticRangeValues(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
            }

            return estimatePredicateRange(columnRefOperator, columnStatistic, predicateRange, statistics);
        } else {
            ConstantOperator constantOperator = constant.get();

            double min = StatisticUtils.convertStatisticsToDouble(constantOperator.getType(), constantOperator.toString())
                    .orElse(NEGATIVE_INFINITY);
            double max = StatisticUtils.convertStatisticsToDouble(constantOperator.getType(), constantOperator.toString())
                    .orElse(POSITIVE_INFINITY);

            ColumnStatistic estimatedColumnStatistic = ColumnStatistic.buildFrom(columnStatistic)
                    .setNullsFraction(0)
                    .setMinValue(min)
                    .setMaxValue(max)
                    .setDistinctValuesCount(columnStatistic.getDistinctValuesCount())
                    .build();

            double predicateFactor;
            double rows;
            Histogram hist = columnStatistic.getHistogram();
            Map<String, Long> histogramTopN = columnStatistic.getHistogram().getMCV();

            String constantStringValue = constantOperator.toString();
            if (constantOperator.getType() == Type.BOOLEAN) {
                constantStringValue = constantOperator.getBoolean() ? "1" : "0";
            }
            // If there is a constant key in MCV, we use the MCV count to estimate the row count.
            // If it is not in MCV but in a bucket, we use the bucket info to estimate the row count.
            // If it is not in MCV and not in any bucket, we combine hist row count, total row count and bucket number
            // to estimate the row count.
            if (histogramTopN.containsKey(constantStringValue)) {
                double rowCountInHistogram = histogramTopN.get(constantStringValue);
                predicateFactor = rowCountInHistogram / columnStatistic.getHistogram().getTotalRows();
                double estimatedRows = statistics.getOutputRowCount() * (1 - columnStatistic.getNullsFraction())
                        * predicateFactor;
                rows = Math.min(rowCountInHistogram, estimatedRows);
            } else {
                Optional<Long> rowCounts = hist.getRowCountInBucket(constantOperator, columnStatistic.getDistinctValuesCount());
                if (rowCounts.isPresent()) {
                    predicateFactor = rowCounts.get() * 1.0 / columnStatistic.getHistogram().getTotalRows();
                    double estimatedRows = statistics.getOutputRowCount() * (1 - columnStatistic.getNullsFraction())
                            * predicateFactor;
                    rows = Math.min(rowCounts.get(), estimatedRows);
                } else {
                    Long mostCommonValuesCount = histogramTopN.values().stream().reduce(Long::sum).orElse(0L);
                    double f = 1 / Math.max(columnStatistic.getDistinctValuesCount() - histogramTopN.size(),
                            hist.getBuckets().size());
                    predicateFactor = (columnStatistic.getHistogram().getTotalRows() - mostCommonValuesCount)
                            * f / columnStatistic.getHistogram().getTotalRows();
                    rows = statistics.getOutputRowCount() * (1 - columnStatistic.getNullsFraction())
                            * predicateFactor;
                }
            }
            return columnRefOperator.map(operator -> Statistics.buildFrom(statistics)
                            .setOutputRowCount(rows).addColumnStatistic(operator, estimatedColumnStatistic).build())
                    .orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rows).build());
        }
    }

    private static Statistics estimateColumnNotEqualToConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                               ColumnStatistic columnStatistic,
                                                               Optional<ConstantOperator> constant,
                                                               Statistics statistics) {
        if (columnStatistic.getHistogram() == null || !constant.isPresent()) {
            StatisticRangeValues predicateRange;
            if (constant.isPresent()) {
                Optional<Double> c = StatisticUtils.convertStatisticsToDouble(
                        constant.get().getType(), constant.get().toString());
                predicateRange = c.map(aDouble -> new StatisticRangeValues(aDouble, aDouble, 1))
                        .orElseGet(() -> new StatisticRangeValues(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1));
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
        } else {
            ColumnStatistic estimatedColumnStatistic = ColumnStatistic.buildFrom(columnStatistic).setNullsFraction(0).build();
            double rowCount = statistics.getOutputRowCount() -
                    estimateColumnEqualToConstant(columnRefOperator, columnStatistic, constant, statistics).getOutputRowCount();

            return columnRefOperator.map(operator -> Statistics.buildFrom(statistics)
                            .setOutputRowCount(rowCount).addColumnStatistic(operator, estimatedColumnStatistic).build())
                    .orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
        }
    }

    private static Statistics estimateColumnLessThanConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                             ColumnStatistic columnStatistic,
                                                             Optional<ConstantOperator> constant,
                                                             Statistics statistics,
                                                             BinaryType binaryType) {
        Optional<Histogram> hist = updateHistWithLessThan(columnStatistic, constant,
                binaryType.equals(BinaryType.LE));
        if (!hist.isPresent()) {
            StatisticRangeValues predicateRange;
            if (constant.isPresent()) {
                Optional<Double> d = StatisticUtils.convertStatisticsToDouble(
                        constant.get().getType(), constant.get().toString());
                if (d.isPresent()) {
                    predicateRange = new StatisticRangeValues(NEGATIVE_INFINITY, d.get(), NaN);
                } else {
                    predicateRange = new StatisticRangeValues(NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN);
                }
            } else {
                predicateRange = new StatisticRangeValues(NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN);
            }
            return estimatePredicateRange(columnRefOperator, columnStatistic, predicateRange, statistics);
        } else {
            Histogram estimatedHistogram = hist.get();

            long rowCountInHistogram = estimatedHistogram.getTotalRows();
            double rowCount = statistics.getOutputRowCount()
                    * ((double) rowCountInHistogram / (double) columnStatistic.getHistogram().getTotalRows());

            ColumnStatistic newEstimateColumnStatistics =
                    estimateColumnStatisticsWithHistogram(columnStatistic, estimatedHistogram);
            return columnRefOperator.map(operator -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                            addColumnStatistic(operator, newEstimateColumnStatistics).build()).
                    orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
        }
    }

    private static Statistics estimateColumnGreaterThanConstant(Optional<ColumnRefOperator> columnRefOperator,
                                                                ColumnStatistic columnStatistic,
                                                                Optional<ConstantOperator> constant,
                                                                Statistics statistics,
                                                                BinaryType binaryType) {
        Optional<Histogram> hist = updateHistWithGreaterThan(columnStatistic, constant,
                binaryType.equals(BinaryType.GE));
        if (!hist.isPresent()) {
            StatisticRangeValues predicateRange;
            if (constant.isPresent()) {
                Optional<Double> d = StatisticUtils.convertStatisticsToDouble(
                        constant.get().getType(), constant.get().toString());
                if (d.isPresent()) {
                    predicateRange = new StatisticRangeValues(d.get(), POSITIVE_INFINITY, NaN);
                } else {
                    predicateRange = new StatisticRangeValues(NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN);
                }
            } else {
                predicateRange = new StatisticRangeValues(NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN);
            }
            return estimatePredicateRange(columnRefOperator, columnStatistic, predicateRange, statistics);

        } else {
            Histogram estimatedHistogram = hist.get();
            long rowCountInHistogram = estimatedHistogram.getTotalRows();
            double rowCount = statistics.getOutputRowCount()
                    * ((double) rowCountInHistogram / (double) columnStatistic.getHistogram().getTotalRows());

            ColumnStatistic newEstimateColumnStatistics =
                    estimateColumnStatisticsWithHistogram(columnStatistic, estimatedHistogram);

            return columnRefOperator.map(operator -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                            addColumnStatistic(operator, newEstimateColumnStatistics).build()).
                    orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
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

    public static Optional<Histogram> updateHistWithLessThan(ColumnStatistic columnStatistic,
                                                   Optional<ConstantOperator> constant,
                                                   boolean containUpper) {
        if (columnStatistic.getHistogram() == null || !constant.isPresent()) {
            return Optional.empty();
        }

        Optional<Double> optionalDouble = StatisticUtils.convertStatisticsToDouble(constant.get().getType(),
                constant.get().toString());

        if (!optionalDouble.isPresent()) {
            return Optional.empty();
        }

        double constantDouble = optionalDouble.get();
        Histogram histogram = columnStatistic.getHistogram();

        List<Bucket> bucketList = new ArrayList<>();
        for (int i = 0; i < histogram.getBuckets().size(); i++) {
            Bucket bucket = histogram.getBuckets().get(i);
            long previousTotalRowCount = 0;
            if (i > 0) {
                previousTotalRowCount = histogram.getBuckets().get(i - 1).getCount();
            }

            if (bucket.getUpper() >= constantDouble && bucket.getLower() <= constantDouble) {
                StatisticRangeValues bucketRange = new StatisticRangeValues(bucket.getLower(), bucket.getUpper(), NaN);
                StatisticRangeValues columnRange = new StatisticRangeValues(bucket.getLower(), constantDouble, NaN);

                long bucketRowCount;
                long repeat;
                if (containUpper && constantDouble == bucket.getUpper()) {
                    bucketRowCount = bucket.getCount() - previousTotalRowCount;
                    repeat = bucket.getUpperRepeats();
                } else {
                    double predicateFactor = bucketRange.overlapPercentWith(columnRange);
                    long bucketTotalRows = bucket.getCount() - bucket.getUpperRepeats() - previousTotalRowCount;
                    bucketRowCount = (long) (bucketTotalRows * predicateFactor);
                    repeat = 0;
                }

                bucketList.add(new Bucket(bucket.getLower(), constantDouble, previousTotalRowCount + bucketRowCount, repeat));
                break;
            } else if (bucket.getLower() > constantDouble) {
                break;
            }
            bucketList.add(bucket);
        }

        Map<String, Long> mostCommonValues = histogram.getMCV();
        Map<String, Long> estimatedMCV = new HashMap<>();
        for (Map.Entry<String, Long> entry : mostCommonValues.entrySet()) {
            Optional<Double> optionalKey = StatisticUtils.convertStatisticsToDouble(constant.get().getType(), entry.getKey());
            if (!optionalKey.isPresent()) {
                return Optional.empty();
            } else if (optionalKey.get() < constantDouble || (optionalKey.get() == constantDouble && containUpper)) {
                estimatedMCV.put(entry.getKey(), entry.getValue());
            }
        }

        if (bucketList.isEmpty() && estimatedMCV.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new Histogram(bucketList, estimatedMCV));
    }

    public static Optional<Histogram> updateHistWithGreaterThan(ColumnStatistic columnStatistic,
                                                                Optional<ConstantOperator> constant,
                                                                boolean containUpper) {
        if (columnStatistic.getHistogram() == null || !constant.isPresent()) {
            return Optional.empty();
        }

        Optional<Double> optionalDouble = StatisticUtils.convertStatisticsToDouble(constant.get().getType(),
                constant.get().toString());

        if (!optionalDouble.isPresent()) {
            return Optional.empty();
        }
        double constantDouble = optionalDouble.get();
        Histogram histogram = columnStatistic.getHistogram();
        List<Bucket> bucketList = new ArrayList<>();
        int i = 0;
        long previousTotalRowCount = 0;
        for (; i < histogram.getBuckets().size(); i++) {
            Bucket bucket = histogram.getBuckets().get(i);
            if (bucket.getUpper() >= constantDouble && bucket.getLower() <= constantDouble) {
                StatisticRangeValues bucketRange = new StatisticRangeValues(bucket.getLower(), bucket.getUpper(), NaN);
                StatisticRangeValues columnRange = new StatisticRangeValues(constantDouble, bucket.getUpper(), NaN);

                long bucketRowCount;
                if (constantDouble == bucket.getUpper()) {
                    if (containUpper) {
                        previousTotalRowCount = bucket.getCount() - bucket.getUpperRepeats();
                    } else {
                        previousTotalRowCount = bucket.getCount();
                    }
                } else {
                    double predicateFactor = bucketRange.overlapPercentWith(columnRange);
                    long bucketTotalRows = bucket.getCount() - previousTotalRowCount;
                    bucketRowCount = (long) (bucketTotalRows * predicateFactor);
                    previousTotalRowCount = previousTotalRowCount + (bucketTotalRows - bucketRowCount);
                }

                if (bucket.getCount() - previousTotalRowCount != 0) {
                    bucketList.add(new Bucket(constantDouble, bucket.getUpper(),
                            bucket.getCount() - previousTotalRowCount, bucket.getUpperRepeats()));
                }
                i++;
                break;
            } else if (bucket.getLower() > constantDouble) {
                break;
            }
            previousTotalRowCount = histogram.getBuckets().get(i).getCount();
        }

        for (; i < histogram.getBuckets().size(); i++) {
            Bucket bucket = new Bucket(
                    histogram.getBuckets().get(i).getLower(),
                    histogram.getBuckets().get(i).getUpper(),
                    histogram.getBuckets().get(i).getCount() - previousTotalRowCount,
                    histogram.getBuckets().get(i).getUpperRepeats());

            bucketList.add(bucket);
        }

        Map<String, Long> mostCommonValues = histogram.getMCV();
        Map<String, Long> estimatedMCV = new HashMap<>();
        for (Map.Entry<String, Long> entry : mostCommonValues.entrySet()) {
            Optional<Double> optionalKey = StatisticUtils.convertStatisticsToDouble(constant.get().getType(), entry.getKey());
            if (!optionalKey.isPresent()) {
                return Optional.empty();
            } else if (optionalKey.get() > constantDouble || (optionalKey.get() == constantDouble && containUpper)) {
                estimatedMCV.put(entry.getKey(), entry.getValue());
            }
        }

        if (bucketList.isEmpty() && estimatedMCV.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new Histogram(bucketList, estimatedMCV));
    }

    public static ColumnStatistic estimateColumnStatisticsWithHistogram(ColumnStatistic columnStatistic,
                                                                        Histogram histogram) {
        double min;
        double max;
        if (histogram.getBuckets().isEmpty()) {
            min = NaN;
            max = NaN;
        } else {
            min = histogram.getBuckets().get(0).getLower();
            max = histogram.getBuckets().get(histogram.getBuckets().size() - 1).getUpper();
        }

        ColumnStatistic.Builder newEstimateColumnStatistics = ColumnStatistic.builder().
                setAverageRowSize(columnStatistic.getAverageRowSize()).
                setMinValue(min).
                setMaxValue(max).
                setNullsFraction(0).
                setDistinctValuesCount(columnStatistic.getDistinctValuesCount()).
                setType(columnStatistic.getType());

        newEstimateColumnStatistics.setHistogram(histogram);
        return newEstimateColumnStatistics.build();
    }
}
