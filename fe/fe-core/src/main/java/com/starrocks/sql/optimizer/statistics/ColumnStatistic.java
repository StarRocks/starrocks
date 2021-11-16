// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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

    private static final ColumnStatistic
            UNKNOWN = new ColumnStatistic(NEGATIVE_INFINITY, POSITIVE_INFINITY, 0, 1, 1, StatisticType.UNKNOWN);

    private final double minValue;
    private final double maxValue;
    private final double nullsFraction;
    private final double averageRowSize;
    private final double distinctValuesCount;
    private final StatisticType type;

    // TODO deal with string max, min
    public ColumnStatistic(
            double minValue,
            double maxValue,
            double nullsFraction,
            double averageRowSize,
            double distinctValuesCount,
            StatisticType type) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.nullsFraction = nullsFraction;
        this.averageRowSize = averageRowSize;
        this.distinctValuesCount = distinctValuesCount;
        this.type = type;
    }

    public ColumnStatistic(double minValue,
                           double maxValue,
                           double nullsFraction,
                           double averageRowSize,
                           double distinctValuesCount) {
        this(minValue, maxValue, nullsFraction, averageRowSize, distinctValuesCount, StatisticType.ESTIMATE);
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

    public static ColumnStatistic unknown() {
        return UNKNOWN;
    }

    public boolean isUnknown() {
        return this.type == StatisticType.UNKNOWN;
    }

    public boolean isUnknownValue() {
        return this.minValue == NEGATIVE_INFINITY && this.maxValue == POSITIVE_INFINITY && this.nullsFraction == 0 &&
                this.averageRowSize == 1 && this.distinctValuesCount == 1;
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
                other.distinctValuesCount, other.type);
    }

    public static Builder buildFrom(String columnStatistic) {
        int endIndex = columnStatistic.indexOf(']');
        String valueString = columnStatistic.substring(1, endIndex);
        String typeString = endIndex == columnStatistic.length() - 1 ? "" : columnStatistic.substring(endIndex + 2);

        String[] valueArray = valueString.split(",");
        Preconditions.checkState(valueArray.length == 5);

        Builder builder = new Builder(Double.parseDouble(valueArray[0]), Double.parseDouble(valueArray[1]),
                Double.parseDouble(valueArray[2]), Double.parseDouble(valueArray[3]),
                Double.parseDouble(valueArray[4]));
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
        private StatisticType type = StatisticType.ESTIMATE;

        private Builder() {
        }

        private Builder(double minValue, double maxValue, double nullsFraction, double averageRowSize,
                        double distinctValuesCount, StatisticType type) {
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.nullsFraction = nullsFraction;
            this.averageRowSize = averageRowSize;
            this.distinctValuesCount = distinctValuesCount;
            this.type = type;
        }

        private Builder(double minValue, double maxValue, double nullsFraction, double averageRowSize,
                        double distinctValuesCount) {
            this(minValue, maxValue, nullsFraction, averageRowSize, distinctValuesCount, StatisticType.ESTIMATE);
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

        public Builder setType(StatisticType type) {
            this.type = type;
            return this;
        }

        public ColumnStatistic build() {
            return new ColumnStatistic(minValue, maxValue, nullsFraction, averageRowSize, distinctValuesCount, type);
        }
    }
}
