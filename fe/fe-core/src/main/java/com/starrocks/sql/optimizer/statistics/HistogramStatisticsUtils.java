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

public class HistogramStatisticsUtils {

    public static Statistics estimateInPredicateWithHistogram(
            ColumnRefOperator columnRefOperator,
            ColumnStatistic columnStatistic,
            List<ConstantOperator> constants,
            boolean isNotIn,
            Statistics statistics) {

        Histogram histogram = columnStatistic.getHistogram();
        Map<String, Long> mcv = histogram.getMCV();
        long histogramTotalRows = histogram.getTotalRows();

        long totalMatchedRows = 0;
        int matchedConstantsCount = 0;
        List<Double> validMatchedConstants = new ArrayList<>();
        boolean isCharFamily = columnRefOperator.getType().getPrimitiveType().isCharFamily();

        for (ConstantOperator constant : constants) {
            String constantStr = constant.toString();
            if (constant.getType() == Type.BOOLEAN) {
                constantStr = constant.getBoolean() ? "1" : "0";
            }

            if (mcv.containsKey(constantStr)) {
                totalMatchedRows += mcv.get(constantStr);
                matchedConstantsCount++;
            } else {
                Optional<Long> rowCountInBucket = histogram.getRowCountInBucket(
                        constant, columnStatistic.getDistinctValuesCount());

                if (rowCountInBucket.isPresent()) {
                    totalMatchedRows += rowCountInBucket.get();
                    matchedConstantsCount++;
                } else {
                    long estimatedRows = estimateOutOfRangeConstant(columnRefOperator, columnStatistic, constant, histogram);
                    totalMatchedRows += estimatedRows;

                    if (estimatedRows > 0) {
                        matchedConstantsCount++;
                    }
                }
            }

            if (!isCharFamily) {
                Optional<Double> constantValueOpt = StatisticUtils.convertStatisticsToDouble(
                        constant.getType(), constantStr);
                constantValueOpt.ifPresent(validMatchedConstants::add);
            }
        }

        double selectivity = (double) totalMatchedRows / histogramTotalRows;
        selectivity = Math.min(1.0, Math.max(0.0, selectivity));

        if (isNotIn) {
            selectivity = 1.0 - selectivity;

            double nullsFraction = columnStatistic.getNullsFraction();
            double rowCount = statistics.getOutputRowCount() * (1 - nullsFraction) * selectivity;

            if (Precision.equals(selectivity, 0.0, 0.000001d)) {
                selectivity = 1 - StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
                rowCount = statistics.getOutputRowCount() * (1 - nullsFraction) * selectivity;
            }

            double overlapFactor = Math.min(1.0, matchedConstantsCount / columnStatistic.getDistinctValuesCount());
            double estimatedDistinctValues = columnStatistic.getDistinctValuesCount() * (1 - overlapFactor);

            ColumnStatistic estimatedColumnStatistic = ColumnStatistic.buildFrom(columnStatistic)
                    .setNullsFraction(0)
                    .setDistinctValuesCount(Math.max(1, estimatedDistinctValues))
                    .build();

            Statistics result = Statistics.buildFrom(statistics)
                    .setOutputRowCount(rowCount)
                    .addColumnStatistic(columnRefOperator, estimatedColumnStatistic)
                    .build();

            return StatisticsEstimateUtils.adjustStatisticsByRowCount(result, rowCount);
        } else {
            double nullsFraction = columnStatistic.getNullsFraction();
            double rowCount = statistics.getOutputRowCount() * (1 - nullsFraction) * selectivity;

            if (isCharFamily) {
                ColumnStatistic estimatedColumnStatistic = ColumnStatistic.buildFrom(columnStatistic)
                        .setNullsFraction(0)
                        .setDistinctValuesCount(Math.min(matchedConstantsCount, columnStatistic.getDistinctValuesCount()))
                        .build();

                Statistics result = Statistics.buildFrom(statistics)
                        .setOutputRowCount(rowCount)
                        .addColumnStatistic(columnRefOperator, estimatedColumnStatistic)
                        .build();

                return StatisticsEstimateUtils.adjustStatisticsByRowCount(result, rowCount);
            } else {
                if (validMatchedConstants.isEmpty()) {
                    ColumnStatistic estimatedColumnStatistic = ColumnStatistic.buildFrom(columnStatistic)
                            .setNullsFraction(0)
                            .setDistinctValuesCount(Math.min(matchedConstantsCount, columnStatistic.getDistinctValuesCount()))
                            .build();

                    Statistics result = Statistics.buildFrom(statistics)
                            .setOutputRowCount(rowCount)
                            .addColumnStatistic(columnRefOperator, estimatedColumnStatistic)
                            .build();

                    return StatisticsEstimateUtils.adjustStatisticsByRowCount(result, rowCount);
                }

                double constantsMin = Collections.min(validMatchedConstants);
                double constantsMax = Collections.max(validMatchedConstants);

                double columnMin = columnStatistic.getMinValue();
                double columnMax = columnStatistic.getMaxValue();

                double newMin = Math.max(columnMin, constantsMin);
                double newMax = Math.min(columnMax, constantsMax);

                if (newMin > newMax) {
                    newMin = constantsMin;
                    newMax = constantsMax;
                }

                ColumnStatistic estimatedColumnStatistic = ColumnStatistic.buildFrom(columnStatistic)
                        .setNullsFraction(0)
                        .setMinValue(newMin)
                        .setMaxValue(newMax)
                        .setDistinctValuesCount(Math.min(matchedConstantsCount, columnStatistic.getDistinctValuesCount()))
                        .build();

                Statistics result = Statistics.buildFrom(statistics)
                        .setOutputRowCount(rowCount)
                        .addColumnStatistic(columnRefOperator, estimatedColumnStatistic)
                        .build();

                return StatisticsEstimateUtils.adjustStatisticsByRowCount(result, rowCount);
            }
        }
    }

    private static long estimateOutOfRangeConstant(
            ColumnRefOperator columnRefOperator, ColumnStatistic columnStatistic,
            ConstantOperator constant, Histogram histogram) {

        long mcvRowCount = histogram.getMCV().values().stream().mapToLong(Long::longValue).sum();
        long totalRows = histogram.getTotalRows();
        double ndv = columnStatistic.getDistinctValuesCount();
        double mcvCount = histogram.getMCV().size();

        if (columnRefOperator.getType().getPrimitiveType().isCharFamily()) {
            double avgRowsPerValue = totalRows / ndv;
            double sparsityFactor = 0.1;
            return Math.max(1, Math.round(avgRowsPerValue * sparsityFactor));
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
            double outOfRangeFactor = 0.1;

            return Math.max(1, Math.round(avgRowsPerNonMcvValue * outOfRangeFactor));
        }
    }

}
