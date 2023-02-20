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
}
