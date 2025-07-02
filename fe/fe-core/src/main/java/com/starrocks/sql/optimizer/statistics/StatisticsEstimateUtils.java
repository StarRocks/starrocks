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
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.StatisticUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isInfinite;

public class StatisticsEstimateUtils {
    public static ColumnStatistic unionColumnStatistic(ColumnStatistic left, double leftRowCount, ColumnStatistic right,
                                                       double rightRowCount) {
        if (left.isUnknown() || right.isUnknown()) {
            return ColumnStatistic.unknown();
        }
        ColumnStatistic.Builder builder = ColumnStatistic.builder();
        builder.setMaxValue(Math.max(left.getMaxValue(), right.getMaxValue()));
        builder.setMinValue(Math.min(left.getMinValue(), right.getMinValue()));
        // compute new statistic range
        StatisticRangeValues leftRange = StatisticRangeValues.from(left);
        StatisticRangeValues rightRange = StatisticRangeValues.from(right);
        StatisticRangeValues newRange = leftRange.union(rightRange);
        // compute new nullsFraction and averageRowSize
        double newRowCount = leftRowCount + rightRowCount;
        double leftNullCount = leftRowCount * left.getNullsFraction();
        double rightNullCount = rightRowCount * right.getNullsFraction();
        double leftSize = (leftRowCount - leftNullCount) * left.getAverageRowSize();
        double rightSize = (rightRowCount - rightNullCount) * right.getAverageRowSize();
        double newNullFraction = (leftNullCount + rightNullCount) / Math.max(1, newRowCount);
        double newNonNullRowCount = newRowCount * (1 - newNullFraction);

        double newAverageRowSize = newNonNullRowCount == 0 ? 0 : (leftSize + rightSize) / newNonNullRowCount;
        builder.setMinValue(newRange.getLow())
                .setMaxValue(newRange.getHigh())
                .setNullsFraction(newNullFraction)
                .setAverageRowSize(newAverageRowSize)
                .setDistinctValuesCount(newRange.getDistinctValues());
        return builder.build();
    }

    public static Statistics adjustStatisticsByRowCount(Statistics statistics, double rowCount) {
        // Do not compute predicate statistics if column statistics is unknown or table row count may inaccurate
        if (statistics.getColumnStatistics().values().stream().anyMatch(ColumnStatistic::isUnknown) ||
                statistics.isTableRowCountMayInaccurate()) {
            return statistics;
        }
        Statistics.Builder builder = Statistics.buildFrom(statistics);
        builder.setOutputRowCount(rowCount);
        // use row count to adjust column statistics distinct values
        double distinctValues = Math.max(1, rowCount);
        statistics.getColumnStatistics().forEach((column, columnStatistic) -> {
            if (columnStatistic.getDistinctValuesCount() > distinctValues) {
                builder.addColumnStatistic(column,
                        ColumnStatistic.buildFrom(columnStatistic).setDistinctValuesCount(distinctValues).build());
            }
        });
        return builder.build();
    }

    public static double getPredicateSelectivity(ScalarOperator predicate, Statistics statistics) {
        Statistics estimatedStatistics = PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics);

        // avoid sample statistics filter all data, save one rows least
        if (statistics.getOutputRowCount() > 0 && estimatedStatistics.getOutputRowCount() == 0) {
            return 1 / statistics.getOutputRowCount();
        } else {
            return estimatedStatistics.getOutputRowCount() / statistics.getOutputRowCount();
        }
    }

    /**
     * Estimates selectivity for conjunctive equality predicates across multiple columns.
     *
     * This method implements a hybrid approach that:
     * 1. Leverages multi-column combined statistics when available to capture column correlations
     * 2. Falls back to a weighted combination model with exponential decay for columns without joint statistics
     * 3. Applies selectivity bounds to avoid both overestimation and underestimation
     *
     * Key formulas:
     * - Multi-column combined statistics based: S_mc = max(min(1/NDV, min_sel), prod_sel)
     *   Where:
     *     - 1/NDV is the selectivity based on multi-columns ndv
     *     - min_sel is the minimum selectivity among correlated columns
     *     - prod_sel is the product of individual column selectivities
     *
     * - Exponential decay for additional columns: S_final = S_base * ∏(S_i^(0.5^i))
     *   Where:
     *     - S_base is the initial selectivity (from multi-column stats or most selective column)
     *     - S_i is the selectivity of the i-th additional column (sorted by ascending selectivity)
     *     - 0.5^i is the exponential decay weight (0.5, 0.25, 0.125, etc.)
     *
     * @param equalityPredicates Map of column references to their equality constant values
     * @param statistics
     * @return Estimated selectivity in range [0,1], or -1 if estimation cannot be performed
     */
    private static double estimateConjunctiveEqualitySelectivity(
            Map<ColumnRefOperator, ConstantOperator> equalityPredicates,
            Statistics statistics) {
        // Require at least two columns for multi-column estimation
        if (equalityPredicates.size() < 2) {
            return -1;
        }

        // Compute individual selectivity factors for each predicate and sort in ascending order
        Map<ColumnRefOperator, Double> columnToSelectivityMap = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, ConstantOperator> entry : equalityPredicates.entrySet()) {
            ColumnRefOperator columnRef = entry.getKey();
            ConstantOperator constantValue = entry.getValue();
            BinaryPredicateOperator equalityPredicate = new BinaryPredicateOperator(BinaryType.EQ, columnRef, constantValue);
            columnToSelectivityMap.put(columnRef, getPredicateSelectivity(equalityPredicate, statistics));
        }

        List<Map.Entry<ColumnRefOperator, Double>> selectivityEntriesSorted =
                new ArrayList<>(columnToSelectivityMap.entrySet());

        // Sort by ascending selectivity (most selective first)
        selectivityEntriesSorted.sort(Map.Entry.comparingByValue());

        // Retrieve available multi-column combined statistics for the target columns
        Set<ColumnRefOperator> targetColumnRefs = equalityPredicates.keySet();
        Pair<Set<ColumnRefOperator>, MultiColumnCombinedStats> multiColumnStatsPair =
                statistics.getLargestSubsetMCStats(targetColumnRefs);

        double estimatedSelectivity;

        // Primary estimation path: utilize multi-column statistics when available
        if (multiColumnStatsPair != null &&
                !multiColumnStatsPair.first.isEmpty() &&
                multiColumnStatsPair.second.getNdv() > 0) {

            Set<ColumnRefOperator> correlatedColumns = multiColumnStatsPair.first;
            double distinctValueCount = Math.max(1.0, multiColumnStatsPair.second.getNdv());

            // Formula: S_corr = 1/NDV
            // NDV-based selectivity estimation for correlated columns
            double correlationBasedSelectivity = 1.0 / distinctValueCount;

            double maxNullFraction = correlatedColumns.stream()
                    .map(statistics::getColumnStatistic)
                    .mapToDouble(ColumnStatistic::getNullsFraction)
                    .max()
                    .orElse(0.0);
            correlationBasedSelectivity = correlationBasedSelectivity * (1.0 - maxNullFraction);

            // Formula: S_ind = ∏(S_i) for all i in correlatedColumns
            // Calculate independence-assumption selectivity product as lower bound
            double independentSelectivityProduct = correlatedColumns.stream()
                    .map(columnToSelectivityMap::get)
                    .reduce(1.0, (a, b) -> a * b);

            // Formula: S_min = min(S_i) for all i in correlatedColumns
            // Identify minimum column selectivity as upper bound
            double minColumnSelectivity = correlatedColumns.stream()
                    .map(columnToSelectivityMap::get)
                    .min(Double::compare)
                    .orElse(1.0);

            // Formula: S_mc = max(min(S_corr, S_min), S_ind)
            // Apply selectivity bounds to balance correlation effects
            // Because a single column may build a histogram or mcv, the selection will be much larger than using only ndv.
            estimatedSelectivity = Math.max(
                    Math.min(correlationBasedSelectivity, minColumnSelectivity),
                    independentSelectivityProduct);

            // Process remaining columns not covered by multi-column combined statistics
            // Formula ordering: S_final = S_mc * ∏(S_i^(0.5^(i+1))) where S_i are sorted by ascending selectivity
            List<Double> uncorrelatedSelectivities = selectivityEntriesSorted.stream()
                    .filter(entry -> !correlatedColumns.contains(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .toList();

            // Apply exponential decay weights to uncorrelated columns (max 3)
            // Multi-column selectivity is used as base, then apply remaining columns in ascending selectivity order
            for (int i = 0; i < Math.min(3, uncorrelatedSelectivities.size()); i++) {
                double decayFactor = 1;
                if (ConnectContext.get().getSessionVariable().isUseCorrelatedPredicateEstimate()) {
                    decayFactor = Math.pow(0.5, i + 1); // Weights: 0.5, 0.25, 0.125
                }
                estimatedSelectivity *= Math.pow(uncorrelatedSelectivities.get(i), decayFactor);
            }
        } else {
            // Fallback estimation path: weighted combination of individual selectivities
            // Formula: S_base = S_0 (most selective predicate)
            // Use most selective predicate as base (first in the sorted list)
            estimatedSelectivity = selectivityEntriesSorted.get(0).getValue();

            // Formula: S_final = S_base * ∏(S_i^(0.5^i)) for i=1,2,3
            // Apply exponential decay weights to additional columns (max 4)
            // Columns are already sorted by ascending selectivity, so most selective is first
            for (int i = 1; i < Math.min(4, selectivityEntriesSorted.size()); i++) {
                double decayFactor = 1;
                if (ConnectContext.get().getSessionVariable().isUseCorrelatedPredicateEstimate()) {
                    decayFactor = Math.pow(0.5, i);
                }
                estimatedSelectivity *= Math.pow(selectivityEntriesSorted.get(i).getValue(), decayFactor);
            }
        }

        // Clamp final selectivity to valid probability range
        return Math.min(1.0, Math.max(0.0, estimatedSelectivity));
    }

    public static Statistics computeCompoundStatsWithMultiColumnOptimize(ScalarOperator predicate, Statistics inputStats) {
        Pair<Map<ColumnRefOperator, ConstantOperator>, List<ScalarOperator>> decomposedPredicates =
                Utils.separateEqualityPredicates(predicate);

        Map<ColumnRefOperator, ConstantOperator> equalityPredicates = decomposedPredicates.first;
        List<ScalarOperator> nonEqualityPredicates = decomposedPredicates.second;

        double conjunctiveSelectivity = estimateConjunctiveEqualitySelectivity(equalityPredicates, inputStats);
        double filteredRowCount = inputStats.getOutputRowCount() * conjunctiveSelectivity;

        Statistics.Builder filteredStatsBuilder = Statistics.buildFrom(inputStats)
                .setOutputRowCount(filteredRowCount);

        for (Map.Entry<ColumnRefOperator, ConstantOperator> entry : equalityPredicates.entrySet()) {
            ColumnRefOperator columnRef = entry.getKey();
            ConstantOperator constantOperator = entry.getValue();
            ColumnStatistic originalColumnStats = inputStats.getColumnStatistic(columnRef);

            double constantValue = StatisticUtils.convertStatisticsToDouble(
                    constantOperator.getType(), constantOperator.toString()).orElse(NEGATIVE_INFINITY);
            ColumnStatistic updatedColumnStats = ColumnStatistic.buildFrom(originalColumnStats)
                    .setDistinctValuesCount(originalColumnStats.getDistinctValuesCount())
                    .setNullsFraction(0.0)
                    .setMinValue(constantValue)
                    .setMaxValue(isInfinite(constantValue) ? POSITIVE_INFINITY : constantValue)
                    .build();

            filteredStatsBuilder.addColumnStatistic(columnRef, updatedColumnStats);
        }

        Statistics equalityFilteredStats = filteredStatsBuilder.build();

        if (nonEqualityPredicates.isEmpty()) {
            return StatisticsEstimateUtils.adjustStatisticsByRowCount(equalityFilteredStats, filteredRowCount);
        }

        // Apply remaining non-equality predicates sequentially
        Statistics combinedFilteredStats = equalityFilteredStats;

        for (ScalarOperator nonEqualityPredicate : nonEqualityPredicates) {
            combinedFilteredStats = PredicateStatisticsCalculator.statisticsCalculate(
                    nonEqualityPredicate, combinedFilteredStats);
        }

        return StatisticsEstimateUtils.adjustStatisticsByRowCount(
                combinedFilteredStats,
                combinedFilteredStats.getOutputRowCount());
    }
}
