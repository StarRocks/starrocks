// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

public class ColumnStatistic {
    private enum StatisticType {
        UNKNOWN,
        ESTIMATE
    }

    // Used for the column statistics which we could not get from the statistics storage or
    // can not compute the actual column statistics for now
    private static final ColumnStatistic UNKNOWN =
            new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 1, 1, null, StatisticType.UNKNOWN);

    // For time types, including Date, DateTime, Timestamp. They all represented as timestamp in ColumnStatistic,
    // regardless of their different storage format
    private final double minValue;
    private final double maxValue;
    private final double nullsFraction;
    private final double averageRowSize;
    private final double distinctValuesCount;
    private final Histogram histogram;
    private final StatisticType type;
    private final double rowCount;

    // TODO deal with string max, min
    public ColumnStatistic(
            double minValue,
            double maxValue,
            double nullsFraction,
            double averageRowSize,
            double distinctValuesCount,
            double rowCount,
            Histogram histogram,
            StatisticType type) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.nullsFraction = nullsFraction;
        this.averageRowSize = averageRowSize;
        this.distinctValuesCount = distinctValuesCount;
        this.rowCount = rowCount;
        this.histogram = histogram;
        this.type = type;
    }

    public ColumnStatistic(double minValue,
                           double maxValue,
                           double nullsFraction,
                           double averageRowSize,
                           double distinctValuesCount) {
        this(minValue, maxValue, nullsFraction, averageRowSize, distinctValuesCount, 1, null, StatisticType.ESTIMATE);
    }

    public double getMinValue() {
        return minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public double getNullsFraction() {
        return nullsFraction;
    }

    public double getAverageRowSize() {
        return averageRowSize;
    }

    public double getDistinctValuesCount() {
        return distinctValuesCount;
    }

    public Histogram getHistogram() {
        return histogram;
    }

    public static ColumnStatistic unknown() {
        return UNKNOWN;
    }

    public boolean isUnknown() {
        return this.type == StatisticType.UNKNOWN;
    }

    public boolean isInfiniteRange() {
        return this.minValue == NEGATIVE_INFINITY || this.maxValue == POSITIVE_INFINITY;
    }

    public boolean hasNaNValue() {
        return Double.isNaN(minValue) || Double.isNaN(maxValue);
    }

    // TODO(ywb): remove this after user can dump statistics with type
    public boolean isUnknownValue() {
        return this.minValue == NEGATIVE_INFINITY && this.maxValue == POSITIVE_INFINITY && this.nullsFraction == 0 &&
                this.averageRowSize == 1 && this.distinctValuesCount == 1;
    }

    public double getRowCount() {
        return rowCount;
    }

    public StatisticType getType() {
        return this.type;
    }

    @Override
    public String toString() {
        String separator = ", ";
        return "[" + minValue + separator
                + maxValue + separator
                + nullsFraction + separator
                + averageRowSize + separator
                + distinctValuesCount + "] "
                + type;
    }

    public static Builder buildFrom(ColumnStatistic other) {
        return new Builder(other.minValue, other.maxValue, other.nullsFraction, other.averageRowSize,
                other.distinctValuesCount, other.histogram, other.type);
    }

    public static Builder buildFrom(String columnStatistic) {
        int endIndex = columnStatistic.indexOf(']');
        String valueString = columnStatistic.substring(1, endIndex);
        String typeString = endIndex == columnStatistic.length() - 1 ? "" : columnStatistic.substring(endIndex + 2);

        String[] valueArray = valueString.split(",");
        Preconditions.checkState(valueArray.length == 5,
                "statistic value: %s is illegal", valueString);

        double minValue = Double.parseDouble(valueArray[0]);
        double maxValue = Double.parseDouble(valueArray[1]);
        double distinctValues = Double.parseDouble(valueArray[4]);

        if (minValue > maxValue) {
            minValue = Double.NEGATIVE_INFINITY;
            maxValue = Double.POSITIVE_INFINITY;
        }

        if (distinctValues <= 0) {
            distinctValues = 1;
        }

        Builder builder = new Builder(minValue, maxValue,
                Double.parseDouble(valueArray[2]), Double.parseDouble(valueArray[3]),
                distinctValues);
        if (!typeString.isEmpty()) {
            builder.setType(StatisticType.valueOf(typeString));
        } else if (builder.build().isUnknownValue()) {
            builder.setType(StatisticType.UNKNOWN);
        }
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private double minValue = NEGATIVE_INFINITY;
        private double maxValue = POSITIVE_INFINITY;
        private double nullsFraction = NaN;
        private double averageRowSize = NaN;
        private double distinctValuesCount = NaN;
        private Histogram histogram;
        private StatisticType type = StatisticType.ESTIMATE;
        private double rowCount = 1;

        private Builder() {
        }

        private Builder(double minValue, double maxValue, double nullsFraction, double averageRowSize,
                        double distinctValuesCount, Histogram histogram,
                        StatisticType type) {
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.nullsFraction = nullsFraction;
            this.averageRowSize = averageRowSize;
            this.distinctValuesCount = distinctValuesCount;
            this.histogram = histogram;
            this.type = type;
        }

        private Builder(double minValue, double maxValue, double nullsFraction, double averageRowSize,
                        double distinctValuesCount) {
            this(minValue, maxValue, nullsFraction, averageRowSize, distinctValuesCount, null, StatisticType.ESTIMATE);
        }

        public Builder setMinValue(double minValue) {
            this.minValue = minValue;
            return this;
        }

        public Builder setMaxValue(double maxValue) {
            this.maxValue = maxValue;
            return this;
        }

        public Builder setNullsFraction(double nullsFraction) {
            this.nullsFraction = nullsFraction;
            return this;
        }

        public Builder setAverageRowSize(double averageRowSize) {
            this.averageRowSize = averageRowSize;
            return this;
        }

        public Builder setDistinctValuesCount(double distinctValuesCount) {
            this.distinctValuesCount = distinctValuesCount;
            return this;
        }

        public Builder setHistogram(Histogram histogram) {
            this.histogram = histogram;
            return this;
        }

        public Builder setType(StatisticType type) {
            this.type = type;
            return this;
        }

        public Builder setRowCount(double rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public ColumnStatistic build() {
            return new ColumnStatistic(minValue, maxValue, nullsFraction, averageRowSize, distinctValuesCount, 
                    rowCount, histogram, type);
        }
    }
}
