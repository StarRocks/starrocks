// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
}
