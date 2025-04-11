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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.HISTOGRAM_UNREPRESENTED_VALUE_COEFFICIENT;

public class HistogramStatisticsUtils {

    private static class MatchedConstantsInfo {
        long totalMatchedRows;
        int matchedConstantsCount;
        List<Double> validMatchedConstants;

        MatchedConstantsInfo() {
            this.totalMatchedRows = 0;
            this.matchedConstantsCount = 0;
            this.validMatchedConstants = new ArrayList<>();
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
                    matchedInfo, selectivity, nullsFraction, rowCount);
        } else {
            return estimateIn(columnRefOperator, columnStatistic, statistics,
                    matchedInfo, isCharFamily, rowCount);
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
                info.totalMatchedRows += mcv.get(constantStr);
                info.matchedConstantsCount++;
            } else {
                Optional<Long> rowCountInBucket = histogram.getRowCountInBucket(
                        constant, columnStatistic.getDistinctValuesCount());

                if (rowCountInBucket.isPresent()) {
                    info.totalMatchedRows += rowCountInBucket.get();
                    info.matchedConstantsCount++;
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
            double rowCount) {

        if (Precision.equals(selectivity, 0.0, 0.000001d)) {
            selectivity = 1 - StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
            rowCount = statistics.getOutputRowCount() * (1 - nullsFraction) * selectivity;
        }

        ColumnStatistic estimatedColumnStatistic = createNotInColumnStatistic(columnStatistic, matchedInfo);

        Statistics result = Statistics.buildFrom(statistics)
                .setOutputRowCount(rowCount)
                .addColumnStatistic(columnRefOperator, estimatedColumnStatistic)
                .build();

        return StatisticsEstimateUtils.adjustStatisticsByRowCount(result, rowCount);
    }

    private static ColumnStatistic createNotInColumnStatistic(
            ColumnStatistic columnStatistic,
            MatchedConstantsInfo matchedInfo) {

        double overlapFactor = Math.min(1.0, matchedInfo.matchedConstantsCount / columnStatistic.getDistinctValuesCount());
        double estimatedDistinctValues = columnStatistic.getDistinctValuesCount() * (1 - overlapFactor);

        return ColumnStatistic.buildFrom(columnStatistic)
                .setNullsFraction(0)
                .setDistinctValuesCount(Math.max(1, estimatedDistinctValues))
                .build();
    }

    private static Statistics estimateIn(
            ColumnRefOperator columnRefOperator,
            ColumnStatistic columnStatistic,
            Statistics statistics,
            MatchedConstantsInfo matchedInfo,
            boolean isCharFamily,
            double rowCount) {

        ColumnStatistic estimatedColumnStatistic = createInColumnStatistic(
                columnStatistic, matchedInfo, isCharFamily);

        Statistics result = Statistics.buildFrom(statistics)
                .setOutputRowCount(rowCount)
                .addColumnStatistic(columnRefOperator, estimatedColumnStatistic)
                .build();

        return StatisticsEstimateUtils.adjustStatisticsByRowCount(result, rowCount);
    }

    private static ColumnStatistic createInColumnStatistic(
            ColumnStatistic columnStatistic,
            MatchedConstantsInfo matchedInfo,
            boolean isCharFamily) {

        double distinctValues = Math.min(
                matchedInfo.matchedConstantsCount,
                columnStatistic.getDistinctValuesCount());

        if (isCharFamily || matchedInfo.validMatchedConstants.isEmpty()) {
            return ColumnStatistic.buildFrom(columnStatistic)
                    .setNullsFraction(0)
                    .setDistinctValuesCount(distinctValues)
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
                    .build();
        }

        double newMin = Collections.min(validConstants);
        double newMax = Collections.max(validConstants);

        return ColumnStatistic.buildFrom(columnStatistic)
                .setNullsFraction(0)
                .setMinValue(newMin)
                .setMaxValue(newMax)
                .setDistinctValuesCount(distinctValues)
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
