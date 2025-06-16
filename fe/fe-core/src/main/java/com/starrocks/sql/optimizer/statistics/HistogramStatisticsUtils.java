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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.math3.util.Precision;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.HISTOGRAM_UNREPRESENTED_VALUE_COEFFICIENT;

public class HistogramStatisticsUtils {

    private static class MatchedConstantsInfo {
        long totalMatchedRows;
        int matchedConstantsCount;
        List<Double> validMatchedConstants;
        Map<String, Long> matchedMcv;
        Map<Integer, List<Double>> matchedBucketValues;
        Map<Integer, Long> matchedBucketRows;

        MatchedConstantsInfo() {
            this.totalMatchedRows = 0;
            this.matchedConstantsCount = 0;
            this.validMatchedConstants = new ArrayList<>();
            this.matchedMcv = new HashMap<>();
            this.matchedBucketValues = new HashMap<>();
            this.matchedBucketRows = new HashMap<>();
        }
    }

    public static Statistics estimateInPredicateWithHistogram(
            ColumnRefOperator columnRefOperator,
            ColumnStatistic columnStatistic,
            List<ConstantOperator> constants,
            boolean isNotIn,
            Statistics statistics) {

        Histogram histogram = columnStatistic.getHistogram();
        boolean isCharFamily = columnRefOperator.getType().getPrimitiveType().isCharFamily();

        MatchedConstantsInfo matchedInfo = collectMatchedConstantsInfo(
                columnRefOperator, columnStatistic, constants, histogram, isCharFamily);

        double selectivity = calculateSelectivity(matchedInfo.totalMatchedRows, histogram.getTotalRows(), isNotIn);
        double nullsFraction = columnStatistic.getNullsFraction();
        double rowCount = statistics.getOutputRowCount() * (1 - nullsFraction) * selectivity;

        if (isNotIn) {
            return estimateNotIn(columnRefOperator, columnStatistic, statistics,
                    matchedInfo, selectivity, nullsFraction, rowCount, histogram);
        } else {
            return estimateIn(columnRefOperator, columnStatistic, statistics,
                    matchedInfo, isCharFamily, rowCount, histogram);
        }
    }

    private static MatchedConstantsInfo collectMatchedConstantsInfo(
            ColumnRefOperator columnRefOperator,
            ColumnStatistic columnStatistic,
            List<ConstantOperator> constants,
            Histogram histogram,
            boolean isCharFamily) {

        MatchedConstantsInfo info = new MatchedConstantsInfo();
        Map<String, Long> mcv = histogram.getMCV();
        List<Bucket> buckets = histogram.getBuckets();

        // 1. First checks if the constant exists in Most Common Values (MCV)
        // 2. If not in MCV, checks if it falls within a histogram bucket
        // 3. For constants not found in either MCV or buckets,
        // we combine histogram and column's statistics to estimates cardinality.
        for (ConstantOperator constant : constants) {
            String constantStr = constant.toString();
            if (constant.getType() == Type.BOOLEAN) {
                constantStr = constant.getBoolean() ? "1" : "0";
            }

            if (mcv.containsKey(constantStr)) {
                long matchedRows = mcv.get(constantStr);
                info.totalMatchedRows += matchedRows;
                info.matchedConstantsCount++;
                info.matchedMcv.put(constantStr, matchedRows);
            } else {
                Optional<Double> valueOpt = StatisticUtils.convertStatisticsToDouble(
                        constant.getType(), constantStr);

                if (valueOpt.isPresent()) {
                    double value = valueOpt.get();
                    Optional<Long> rowCountInBucket = histogram.getRowCountInBucket(
                            value, columnStatistic.getDistinctValuesCount(),
                            constant.getType().isFixedPointType());

                    if (rowCountInBucket.isPresent()) {
                        long matchedRows = rowCountInBucket.get();
                        info.totalMatchedRows += matchedRows;
                        info.matchedConstantsCount++;

                        for (int i = 0; i < buckets.size(); i++) {
                            Bucket bucket = buckets.get(i);
                            if ((bucket.getLower() <= value && value < bucket.getUpper()) ||
                                    (value == bucket.getUpper() && bucket.getUpperRepeats() > 0)) {

                                info.matchedBucketValues
                                        .computeIfAbsent(i, k -> new ArrayList<>())
                                        .add(value);

                                info.matchedBucketRows.merge(i, matchedRows, Long::sum);
                                break;
                            }
                        }
                    } else {
                        long estimatedRows = estimateNonHistogramValueCardinality(
                                columnRefOperator, columnStatistic, constant, histogram);
                        info.totalMatchedRows += estimatedRows;

                        if (estimatedRows > 0) {
                            info.matchedConstantsCount++;
                        }
                    }
                } else {
                    long estimatedRows = estimateNonHistogramValueCardinality(
                            columnRefOperator, columnStatistic, constant, histogram);
                    info.totalMatchedRows += estimatedRows;

                    if (estimatedRows > 0) {
                        info.matchedConstantsCount++;
                    }
                }
            }

            if (!isCharFamily) {
                Optional<Double> constantValueOpt = StatisticUtils.convertStatisticsToDouble(
                        constant.getType(), constantStr);
                constantValueOpt.ifPresent(info.validMatchedConstants::add);
            }
        }

        return info;
    }

    private static double calculateSelectivity(long totalMatchedRows, long histogramTotalRows, boolean isNotIn) {
        double selectivity = (double) totalMatchedRows / histogramTotalRows;
        selectivity = Math.min(1.0, Math.max(0.0, selectivity));

        return isNotIn ? 1.0 - selectivity : selectivity;
    }

    private static Statistics estimateNotIn(
            ColumnRefOperator columnRefOperator,
            ColumnStatistic columnStatistic,
            Statistics statistics,
            MatchedConstantsInfo matchedInfo,
            double selectivity,
            double nullsFraction,
            double rowCount,
            Histogram originalHistogram) {

        if (Precision.equals(selectivity, 0.0, 0.000001d)) {
            selectivity = 1 - StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
            rowCount = statistics.getOutputRowCount() * (1 - nullsFraction) * selectivity;
        }

        Histogram prunedHistogram = createNotInHistogram(originalHistogram, matchedInfo);

        ColumnStatistic estimatedColumnStatistic = createNotInColumnStatistic(
                columnStatistic, matchedInfo, prunedHistogram);

        Statistics result = Statistics.buildFrom(statistics)
                .setOutputRowCount(rowCount)
                .addColumnStatistic(columnRefOperator, estimatedColumnStatistic)
                .build();

        return StatisticsEstimateUtils.adjustStatisticsByRowCount(result, rowCount);
    }

    private static Histogram createNotInHistogram(Histogram originalHistogram, MatchedConstantsInfo matchedInfo) {
        Map<String, Long> originalMcv = originalHistogram.getMCV();
        Map<String, Long> prunedMcv = new HashMap<>(originalMcv);

        for (String key : matchedInfo.matchedMcv.keySet()) {
            prunedMcv.remove(key);
        }

        List<Bucket> originalBuckets = originalHistogram.getBuckets();
        List<Bucket> prunedBuckets = new ArrayList<>();

        if (originalBuckets.isEmpty()) {
            return new Histogram(prunedBuckets, prunedMcv);
        }

        long accumulatedCount = 0;

        for (int i = 0; i < originalBuckets.size(); i++) {
            Bucket bucket = originalBuckets.get(i);
            long previousOriginalCount = (i > 0) ? originalBuckets.get(i - 1).getCount() : 0;

            long bucketNonRepeatingCount = bucket.getCount() - previousOriginalCount - bucket.getUpperRepeats();
            long matchedRowsInBucket = matchedInfo.matchedBucketRows.getOrDefault(i, 0L);
            long adjustedBucketCount = Math.max(0, bucketNonRepeatingCount - matchedRowsInBucket);

            long adjustedUpperRepeats = bucket.getUpperRepeats();
            if (matchedInfo.matchedBucketValues.containsKey(i)) {
                List<Double> matchedValues = matchedInfo.matchedBucketValues.get(i);
                if (matchedValues.contains(bucket.getUpper())) {
                    adjustedUpperRepeats = 0;
                }
            }

            accumulatedCount += adjustedBucketCount + adjustedUpperRepeats;

            prunedBuckets.add(new Bucket(
                    bucket.getLower(),
                    bucket.getUpper(),
                    accumulatedCount,
                    adjustedUpperRepeats
            ));
        }

        return new Histogram(prunedBuckets, prunedMcv);
    }


    private static ColumnStatistic createNotInColumnStatistic(
            ColumnStatistic columnStatistic,
            MatchedConstantsInfo matchedInfo,
            Histogram prunedHistogram) {

        double overlapFactor = Math.min(1.0, matchedInfo.matchedConstantsCount / columnStatistic.getDistinctValuesCount());
        double estimatedDistinctValues = columnStatistic.getDistinctValuesCount() * (1 - overlapFactor);

        return ColumnStatistic.buildFrom(columnStatistic)
                .setNullsFraction(0)
                .setDistinctValuesCount(Math.max(1, estimatedDistinctValues))
                .setHistogram(prunedHistogram)
                .build();
    }

    private static Statistics estimateIn(
            ColumnRefOperator columnRefOperator,
            ColumnStatistic columnStatistic,
            Statistics statistics,
            MatchedConstantsInfo matchedInfo,
            boolean isCharFamily,
            double rowCount,
            Histogram originalHistogram) {

        Histogram prunedHistogram = createInHistogram(originalHistogram, matchedInfo);

        ColumnStatistic estimatedColumnStatistic = createInColumnStatistic(
                columnStatistic, matchedInfo, isCharFamily, prunedHistogram);

        Statistics result = Statistics.buildFrom(statistics)
                .setOutputRowCount(rowCount)
                .addColumnStatistic(columnRefOperator, estimatedColumnStatistic)
                .build();

        return StatisticsEstimateUtils.adjustStatisticsByRowCount(result, rowCount);
    }

    private static Histogram createInHistogram(Histogram originalHistogram, MatchedConstantsInfo matchedInfo) {
        Map<String, Long> prunedMcv = new HashMap<>(matchedInfo.matchedMcv);

        List<Bucket> prunedBuckets = new ArrayList<>();

        if (matchedInfo.matchedBucketValues.isEmpty()) {
            return new Histogram(prunedBuckets, prunedMcv);
        }

        List<Bucket> originalBuckets = originalHistogram.getBuckets();

        long cumulativeCount = 0;

        for (int i = 0; i < originalBuckets.size(); i++) {
            if (!matchedInfo.matchedBucketValues.containsKey(i)) {
                continue;
            }

            Bucket originalBucket = originalBuckets.get(i);
            long matchedRowsInBucket = matchedInfo.matchedBucketRows.getOrDefault(i, 0L);

            List<Double> matchedValues = matchedInfo.matchedBucketValues.get(i);
            long upperRepeats = 0;
            if (matchedValues.contains(originalBucket.getUpper())) {
                upperRepeats = originalBucket.getUpperRepeats();
            }

            cumulativeCount += matchedRowsInBucket;

            prunedBuckets.add(new Bucket(
                    originalBucket.getLower(),
                    originalBucket.getUpper(),
                    cumulativeCount,
                    upperRepeats
            ));
        }

        return new Histogram(prunedBuckets, prunedMcv);
    }

    private static ColumnStatistic createInColumnStatistic(
            ColumnStatistic columnStatistic,
            MatchedConstantsInfo matchedInfo,
            boolean isCharFamily,
            Histogram prunedHistogram) {

        double distinctValues = Math.min(
                matchedInfo.matchedConstantsCount,
                columnStatistic.getDistinctValuesCount());

        if (isCharFamily || matchedInfo.validMatchedConstants.isEmpty()) {
            return ColumnStatistic.buildFrom(columnStatistic)
                    .setNullsFraction(0)
                    .setDistinctValuesCount(distinctValues)
                    .setHistogram(prunedHistogram)
                    .build();
        }

        List<Double> validConstants = new ArrayList<>();
        double columnMin = columnStatistic.getMinValue();
        double columnMax = columnStatistic.getMaxValue();

        for (Double value : matchedInfo.validMatchedConstants) {
            if (value >= columnMin && value <= columnMax) {
                validConstants.add(value);
            }
        }

        if (validConstants.isEmpty()) {
            return ColumnStatistic.buildFrom(columnStatistic)
                    .setNullsFraction(0)
                    .setDistinctValuesCount(distinctValues)
                    .setHistogram(prunedHistogram)
                    .build();
        }

        double newMin = Collections.min(validConstants);
        double newMax = Collections.max(validConstants);

        return ColumnStatistic.buildFrom(columnStatistic)
                .setNullsFraction(0)
                .setMinValue(newMin)
                .setMaxValue(newMax)
                .setDistinctValuesCount(distinctValues)
                .setHistogram(prunedHistogram)
                .build();
    }

    /**
     * Estimates the cardinality of values not explicitly represented in the histogram.
     * This applies to values that are neither in MCV (Most Common Values) nor in regular histogram buckets.
     */
    private static long estimateNonHistogramValueCardinality(
            ColumnRefOperator columnRefOperator,
            ColumnStatistic columnStatistic,
            ConstantOperator constant,
            Histogram histogram) {

        long mcvRowCount = histogram.getMCV().values().stream().mapToLong(Long::longValue).sum();
        long totalRows = histogram.getTotalRows();
        double ndv = columnStatistic.getDistinctValuesCount();
        double mcvCount = histogram.getMCV().size();

        if (columnRefOperator.getType().getPrimitiveType().isCharFamily()) {
            double avgRowsPerValue = totalRows / ndv;
            return Math.max(1, Math.round(avgRowsPerValue * HISTOGRAM_UNREPRESENTED_VALUE_COEFFICIENT));
        } else {
            Optional<Double> constantValueOpt = StatisticUtils.convertStatisticsToDouble(
                    constant.getType(), constant.toString());

            if (constantValueOpt.isPresent()) {
                double constantValue = constantValueOpt.get();
                double minValue = columnStatistic.getMinValue();
                double maxValue = columnStatistic.getMaxValue();

                if (constantValue < minValue || constantValue > maxValue) {
                    return 0;
                }
            }

            long nonMcvRows = totalRows - mcvRowCount;
            double nonMcvNdv = Math.max(1.0, ndv - mcvCount);
            double avgRowsPerNonMcvValue = nonMcvRows / nonMcvNdv;

            return Math.max(1, Math.round(avgRowsPerNonMcvValue * HISTOGRAM_UNREPRESENTED_VALUE_COEFFICIENT));
        }
    }
}
