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

import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
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
import static java.lang.Math.max;
import static java.lang.Math.min;

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
        if (columnStatistic.getHistogram() == null || constant.isEmpty()) {
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

            ColumnStatistic.Builder estimatedColumnStatisticBuilder = ColumnStatistic.buildFrom(columnStatistic)
                    .setNullsFraction(0)
                    .setMinValue(min)
                    .setMaxValue(max)
                    .setDistinctValuesCount(columnStatistic.getDistinctValuesCount());

            double rows;
            Histogram columnHist = columnStatistic.getHistogram();
            Optional<Histogram> hist = updateHistWithEqual(columnStatistic, constant);
            if (hist.isPresent()) {
                estimatedColumnStatisticBuilder.setHistogram(hist.get());
                double rowCountInHistogram = hist.get().getTotalRows();
                double nonNullFraction = 1.0 - columnStatistic.getNullsFraction();
                double outputRows = statistics.getOutputRowCount();

                double factor = rowCountInHistogram <= 1
                        ? 1.0 / Math.max(1.0, columnStatistic.getDistinctValuesCount())
                        : rowCountInHistogram / (double) columnHist.getTotalRows();

                rows = rowCountInHistogram <= 1
                        ? Math.max(1.0, outputRows * nonNullFraction * factor)
                        : Math.min(rowCountInHistogram, outputRows * nonNullFraction * factor);
            } else {
                // The constant was not found in the column histogram.
                Long mostCommonValuesCount = columnHist.getMCV().values().stream().reduce(Long::sum).orElse(0L);
                double f = 1 / Math.max(columnStatistic.getDistinctValuesCount() - columnHist.getMCV().size(),
                        columnHist.getBuckets().size());
                double predicateFactor = (columnHist.getTotalRows() - mostCommonValuesCount) * f / columnHist.getTotalRows();
                rows = statistics.getOutputRowCount() * (1 - columnStatistic.getNullsFraction()) * predicateFactor;
            }

            return columnRefOperator.map(operator -> Statistics.buildFrom(statistics).setOutputRowCount(rows)
                            .addColumnStatistic(operator, estimatedColumnStatisticBuilder.build()).build())
                    .orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rows).build());
        }
    }

    public static Optional<Histogram> updateHistWithEqual(ColumnStatistic columnStatistic,
                                                          Optional<ConstantOperator> constant) {
        if (constant.isEmpty() || columnStatistic.getHistogram() == null) {
            return Optional.empty();
        }

        Map<String, Long> estimatedMcv = new HashMap<>();
        ConstantOperator constantOperator = constant.get();
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
            Long rowCountInHistogram = histogramTopN.get(constantStringValue);
            estimatedMcv.put(constantOperator.toString(), rowCountInHistogram);
        } else {
            Optional<Long> rowCountInHistogram =
                    hist.getRowCountInBucket(constantOperator, columnStatistic.getDistinctValuesCount());
            if (rowCountInHistogram.isEmpty()) {
                return Optional.empty();
            }

            estimatedMcv.put(constantOperator.toString(), rowCountInHistogram.get());
        }
        return Optional.of(new Histogram(new ArrayList<>(), estimatedMcv));
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
        if (hist.isEmpty()) {
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

            ColumnStatistic finalNewEstimateColumnStatistics = adjustColumnStatisticsMinMax(
                    newEstimateColumnStatistics, constant, statistics, columnRefOperator, true);

            return columnRefOperator.map(operator -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                            addColumnStatistic(operator, finalNewEstimateColumnStatistics).build()).
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

            ColumnStatistic finalNewEstimateColumnStatistics = adjustColumnStatisticsMinMax(
                    newEstimateColumnStatistics, constant, statistics, columnRefOperator, false);
            return columnRefOperator.map(operator -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount)
                                    .addColumnStatistic(operator, finalNewEstimateColumnStatistics).build())
                    .orElseGet(() -> Statistics.buildFrom(statistics).setOutputRowCount(rowCount).build());
        }
    }

    private static ColumnStatistic adjustColumnStatisticsMinMax(
            ColumnStatistic newEstimateColumnStatistics,
            Optional<ConstantOperator> constant,
            Statistics statistics,
            Optional<ColumnRefOperator> columnRefOperator,
            boolean isLessThan) {

        ColumnStatistic.Builder builder = ColumnStatistic.buildFrom(newEstimateColumnStatistics);

        if (constant.isPresent()) {
            double constValue = StatisticUtils.convertStatisticsToDouble(
                            constant.get().getType(), constant.get().toString())
                    .orElse(isLessThan ? POSITIVE_INFINITY : NEGATIVE_INFINITY);

            if (isLessThan && Double.isNaN(newEstimateColumnStatistics.getMaxValue())) {
                builder.setMaxValue(constValue);
            } else if (!isLessThan && Double.isNaN(newEstimateColumnStatistics.getMinValue())) {
                builder.setMinValue(constValue);
            }
        }

        if (columnRefOperator.isPresent()) {
            ColumnStatistic stats = statistics.getColumnStatistics().get(columnRefOperator.get());
            if (stats != null) {
                if (isLessThan && Double.isNaN(newEstimateColumnStatistics.getMinValue())) {
                    builder.setMinValue(stats.getMinValue());
                } else if (!isLessThan && Double.isNaN(newEstimateColumnStatistics.getMaxValue())) {
                    builder.setMaxValue(stats.getMaxValue());
                }
            }
        }

        return builder.build();
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
        StatisticRangeValues intersect = StatisticRangeValues.from(leftColumnStatistic)
                .intersect(StatisticRangeValues.from(rightColumnStatistic));
        ColumnStatistic.Builder newEstimateColumnStatistics = ColumnStatistic.builder()
                .setMaxValue(intersect.getHigh())
                .setMinValue(intersect.getLow())
                .setDistinctValuesCount(intersect.getDistinctValues());

        boolean enableJoinHistogram =
                ConnectContext.get() != null &&
                        ConnectContext.get().getSessionVariable() != null &&
                        ConnectContext.get().getSessionVariable().isCboEnableHistogramJoinEstimation();

        double rowCount;
        Optional<Histogram> hist = enableJoinHistogram ?
                updateHistWithJoin(leftColumnStatistic, leftColumn.getType(), rightColumnStatistic, rightColumn.getType()) :
                Optional.empty();
        if (hist.isEmpty()) {
            double selectivity = 1.0 /
                    max(1, max(leftColumnStatistic.getDistinctValuesCount(), rightColumnStatistic.getDistinctValuesCount()));
            rowCount = statistics.getOutputRowCount() * selectivity *
                    (isEqualForNull ? 1 :
                            (1 - leftColumnStatistic.getNullsFraction()) * (1 - rightColumnStatistic.getNullsFraction()));
        } else {
            double selectivity = hist.get().getTotalRows() / (double)
                    (leftColumnStatistic.getHistogram().getTotalRows() * rightColumnStatistic.getHistogram().getTotalRows());
            rowCount = statistics.getOutputRowCount() * selectivity;
        }

        ColumnStatistic.Builder newLeftStatisticBuilder = ColumnStatistic.buildFrom(newEstimateColumnStatistics.build());
        ColumnStatistic.Builder newRightStatisticBuilder = ColumnStatistic.buildFrom(newEstimateColumnStatistics.build());
        if (!isEqualForNull) {
            newLeftStatisticBuilder.setNullsFraction(0).setAverageRowSize(leftColumnStatistic.getAverageRowSize());
            newRightStatisticBuilder.setNullsFraction(0).setAverageRowSize(rightColumnStatistic.getAverageRowSize());
        } else {
            newLeftStatisticBuilder
                    .setAverageRowSize(leftColumnStatistic.getAverageRowSize())
                    .setNullsFraction(leftColumnStatistic.getNullsFraction());
            newRightStatisticBuilder
                    .setAverageRowSize(rightColumnStatistic.getAverageRowSize())
                    .setNullsFraction(rightColumnStatistic.getNullsFraction());
        }

        if (hist.isEmpty()) {
            newLeftStatisticBuilder.setHistogram(leftColumnStatistic.getHistogram());
            newRightStatisticBuilder.setHistogram(rightColumnStatistic.getHistogram());
        } else {
            newLeftStatisticBuilder.setHistogram(hist.get());
            newRightStatisticBuilder.setHistogram(hist.get());
        }

        Statistics.Builder builder = Statistics.buildFrom(statistics);
        if (leftColumn instanceof ColumnRefOperator column) {
            builder.addColumnStatistic(column, newLeftStatisticBuilder.build());
        }
        if (rightColumn instanceof ColumnRefOperator column) {
            builder.addColumnStatistic(column, newRightStatisticBuilder.build());
        }
        builder.setOutputRowCount(rowCount);
        return builder.build();
    }

    public static Optional<Histogram> updateHistWithJoin(ColumnStatistic leftColumnStatistic, Type leftColumnType,
                                                         ColumnStatistic rightColumnStatistic, Type rightColumnType) {
        if (leftColumnStatistic.getHistogram() == null || rightColumnStatistic.getHistogram() == null) {
            return Optional.empty();
        }

        Histogram leftHistogram = leftColumnStatistic.getHistogram();
        Histogram rightHistogram = rightColumnStatistic.getHistogram();
        double leftColumnDistinctCount = min(leftHistogram.getTotalRows(), leftColumnStatistic.getDistinctValuesCount());
        double rightColumnDistinctCount = min(rightHistogram.getTotalRows(), rightColumnStatistic.getDistinctValuesCount());

        Map<String, Long> estimatedMcv = estimateMcvToMcv(leftHistogram.getMCV(), rightHistogram.getMCV());
        estimateMcvToBucket(leftHistogram.getMCV(), estimatedMcv, rightHistogram, rightColumnDistinctCount, leftColumnType);
        estimateMcvToBucket(rightHistogram.getMCV(), estimatedMcv, leftHistogram, leftColumnDistinctCount, rightColumnType);
        List<Bucket> estimatedBuckets =
                estimateBucketToBucket(leftHistogram, leftColumnDistinctCount, leftColumnType, rightHistogram,
                        rightColumnDistinctCount, rightColumnType);

        if (estimatedMcv.isEmpty() && estimatedBuckets.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new Histogram(estimatedBuckets, estimatedMcv));
    }

    private static Map<String, Long> estimateMcvToMcv(Map<String, Long> leftMcv, Map<String, Long> rightMcv) {
        Map<String, Long> mcvIntersection = new HashMap<>();
        leftMcv.forEach((value, leftFreq) -> {
            if (rightMcv.containsKey(value)) {
                mcvIntersection.put(value, leftFreq * rightMcv.get(value));
            }
        });
        return mcvIntersection;
    }

    private static void estimateMcvToBucket(Map<String, Long> leftMcv, Map<String, Long> estimatedMcv,
                                            Histogram rightHistogram, double distinctValuesCount, Type dataType) {
        if (rightHistogram.getBuckets() == null) {
            return;
        }

        for (Map.Entry<String, Long> entry : leftMcv.entrySet()) {
            if (estimatedMcv.containsKey(entry.getKey())) {
                continue;
            }

            Optional<Double> value = StatisticUtils.convertStatisticsToDouble(dataType, entry.getKey());
            if (value.isEmpty()) {
                continue;
            }

            Long leftFreq = entry.getValue();
            Optional<Long> rowCountInBucketOpt = rightHistogram.getRowCountInBucket(value.get(), distinctValuesCount,
                    dataType.isFixedPointType());
            rowCountInBucketOpt.ifPresent(rowCountInBucket -> estimatedMcv.put(entry.getKey(), leftFreq * rowCountInBucket));
        }
    }

    private static List<Bucket> estimateBucketToBucket(Histogram leftHistogram, double leftColumnDistinctValue, Type dataTypeLeft,
                                                       Histogram rightHistogram, double rightColumnDistinctValue,
                                                       Type dataTypeRight) {
        if (leftHistogram == null || rightHistogram == null) {
            return null;
        }

        List<Bucket> leftBuckets = leftHistogram.getBuckets();
        List<Bucket> rightBuckets = rightHistogram.getBuckets();
        if (leftBuckets == null || leftBuckets.isEmpty() || rightBuckets == null || rightBuckets.isEmpty()) {
            return null;
        }

        // Assume the distinct values are uniformly distributed.
        long leftBucketDistinctRowCount = (long) (leftColumnDistinctValue / leftBuckets.size());
        long rightBucketDistinctRowCount = (long) (rightColumnDistinctValue / rightBuckets.size());

        List<Bucket> mergedBuckets = new ArrayList<>();

        long rowCount = 0;
        Long prevLeftBucketRowCount = 0L;
        Long prevRightBucketRowCount = 0L;
        int leftBucketIndex = 0;
        int rightBucketIndex = 0;
        while (leftBucketIndex < leftBuckets.size() && rightBucketIndex < rightBuckets.size()) {
            Bucket leftBucket = leftBuckets.get(leftBucketIndex);
            Bucket rightBucket = rightBuckets.get(rightBucketIndex);

            Optional<StatisticRangeValues> bucketIntersectionRangeOpt = computeBucketIntersection(leftBucket, rightBucket);
            if (bucketIntersectionRangeOpt.isPresent()) {
                StatisticRangeValues bucketIntersectionRange = bucketIntersectionRangeOpt.get();
                long leftBucketRowCount = leftBucket.getCount() - prevLeftBucketRowCount;
                long rightBucketRowCount = rightBucket.getCount() - prevRightBucketRowCount;
                if (dataTypeLeft.isFixedPointType()) {
                    leftBucketDistinctRowCount = (long) (leftBucket.getUpper() - leftBucket.getLower());
                }
                if (dataTypeRight.isFixedPointType()) {
                    rightBucketDistinctRowCount = (long) (rightBucket.getUpper() - rightBucket.getLower());
                }

                // merge the upper repeats.
                long upperRepeats = 0L;
                if (bucketIntersectionRange.getHigh() == leftBucket.getUpper()) {
                    Optional<Long> countInRightBucket = rightBucket.getRowCountInBucket(leftBucket.getUpper(),
                            prevRightBucketRowCount, rightBucketDistinctRowCount, dataTypeRight.isFixedPointType());
                    if (countInRightBucket.isPresent()) {
                        upperRepeats = leftBucket.getUpperRepeats() * countInRightBucket.get();
                    }
                } else {
                    Optional<Long> countInLeftBucket = leftBucket.getRowCountInBucket(rightBucket.getUpper(),
                            prevLeftBucketRowCount, leftBucketDistinctRowCount, dataTypeLeft.isFixedPointType());
                    if (countInLeftBucket.isPresent()) {
                        upperRepeats = countInLeftBucket.get() * rightBucket.getUpperRepeats();
                    }
                }

                // merge the row count.
                long rowCountInBucket = upperRepeats;
                if (bucketIntersectionRange.getLow() < bucketIntersectionRange.getHigh()) {
                    double leftIntersectionFraction = computeBucketIntersectionFraction(leftBucket, bucketIntersectionRange);
                    double rightIntersectionFraction = computeBucketIntersectionFraction(rightBucket, bucketIntersectionRange);

                    // compute the number of matches in the buckets intersection assuming uniform distribution.
                    rowCountInBucket = max(rowCountInBucket, (long) (
                            leftBucketRowCount * leftIntersectionFraction * rightBucketRowCount * rightIntersectionFraction /
                                    max(leftBucketDistinctRowCount * leftIntersectionFraction,
                                            rightBucketDistinctRowCount * rightIntersectionFraction)));
                }

                rowCount += rowCountInBucket;
                mergedBuckets.add(
                        new Bucket(bucketIntersectionRange.getLow(), bucketIntersectionRange.getHigh(), rowCount, upperRepeats));
            }

            if (leftBucket.getUpper() <= rightBucket.getUpper()) {
                ++leftBucketIndex;
                prevLeftBucketRowCount = leftBucket.getCount();
            }
            if (rightBucket.getUpper() <= leftBucket.getUpper()) {
                ++rightBucketIndex;
                prevRightBucketRowCount = rightBucket.getCount();
            }
        }

        return mergedBuckets;
    }

    private static Optional<StatisticRangeValues> computeBucketIntersection(Bucket leftBucket, Bucket rightBucket) {
        if (leftBucket.getUpper() < rightBucket.getLower() || rightBucket.getUpper() < leftBucket.getLower()) {
            return Optional.empty();
        }

        return Optional.of(new StatisticRangeValues(max(leftBucket.getLower(), rightBucket.getLower()),
                min(leftBucket.getUpper(), rightBucket.getUpper()), 0.0));
    }

    private static double computeBucketIntersectionFraction(Bucket bucket, StatisticRangeValues intersectionRange) {
        return (intersectionRange.getHigh() - intersectionRange.getLow()) / (bucket.getUpper() - bucket.getLower());
    }

    public static Statistics estimateColumnNotEqualToColumn(
            ColumnStatistic leftColumn,
            ColumnStatistic rightColumn,
            Statistics statistics) {
        double leftDistinctValuesCount = leftColumn.getDistinctValuesCount();
        double rightDistinctValuesCount = rightColumn.getDistinctValuesCount();
        double selectivity = 1.0 / max(1, max(leftDistinctValuesCount, rightDistinctValuesCount));

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
