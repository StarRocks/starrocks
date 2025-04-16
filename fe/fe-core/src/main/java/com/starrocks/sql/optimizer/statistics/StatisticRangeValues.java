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

import java.util.Objects;
import javax.validation.constraints.NotNull;

import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.lang.Math.min;

// Calculate the cross range and ratio between column statistics
public class StatisticRangeValues {
    private final double low;
    private final double high;
    private final double distinctValues;

    public StatisticRangeValues(double low, double high, double distinctValues) {
        this.low = low;
        this.high = high;
        this.distinctValues = distinctValues;
    }

    public static StatisticRangeValues empty() {
        return new StatisticRangeValues(NaN, NaN, 0);
    }

    public boolean isEmpty() {
        return isNaN(low) && isNaN(high);
    }

    public boolean isBothInfinite() {
        return isInfinite(low) && isInfinite(high);
    }

    public static StatisticRangeValues from(ColumnStatistic column) {
        return new StatisticRangeValues(column.getMinValue(), column.getMaxValue(), column.getDistinctValuesCount());
    }

    public double getLow() {
        return low;
    }

    public double getHigh() {
        return high;
    }

    public double getDistinctValues() {
        return distinctValues;
    }

    public double length() {
        return this.high - this.low;
    }

    // Calculate the proportion of coverage between column statistic range
    public double overlapPercentWith(@NotNull StatisticRangeValues other) {
        if (this.isEmpty() || other.isEmpty()) {
            return 0.0;
        }
        // If the low and high values is infinite, it represents either string type or unknown of column statistics.
        if (this.equals(other) && !isBothInfinite()) {
            return 1.0;
        }

        double lengthOfIntersect = min(this.high, other.high) - max(this.low, other.low);
        // lengthOfIntersect of char/varchar is infinite
        if (isInfinite(lengthOfIntersect)) {
            if (isFinite(this.distinctValues) && isFinite(other.distinctValues)) {
                return min(other.distinctValues / max(1, this.distinctValues), 1);
            }
            return StatisticsEstimateCoefficient.OVERLAP_INFINITE_RANGE_FILTER_COEFFICIENT;
        }
        if (lengthOfIntersect == 0) {
            // distinctValues equals 1 means the column statistics is unknown,
            // requires special treatment
            if (this.distinctValues == 1 && length() > 1) {
                return 0.5;
            }
            return 1 / max(this.distinctValues, 1);
        }
        if (lengthOfIntersect < 0) {
            return 0;
        }

        double length = length();
        if (isInfinite(length)) {
            return StatisticsEstimateCoefficient.OVERLAP_INFINITE_RANGE_FILTER_COEFFICIENT;
        }

        if (lengthOfIntersect > 0) {
            return lengthOfIntersect / length;
        }
        // length of intersect may be NAN, because min/max may be -infinite/infinite at same time
        return StatisticsEstimateCoefficient.OVERLAP_INFINITE_RANGE_FILTER_COEFFICIENT;
    }

    public StatisticRangeValues intersect(StatisticRangeValues other) {
        double newLow = max(low, other.low);
        double newHigh = min(high, other.high);
        if (newLow <= newHigh) {
            return new StatisticRangeValues(newLow, newHigh, min(this.distinctValues, other.distinctValues));
        }
        return empty();
    }

    public StatisticRangeValues union(StatisticRangeValues other) {
        double overlapPercentThis = this.overlapPercentWith(other);
        double overlapPercentOther = other.overlapPercentWith(this);
        double overlapNDVThis = overlapPercentThis * distinctValues;
        double overlapNDVOther = overlapPercentOther * other.distinctValues;
        double maxOverlapNDV = Math.max(overlapNDVThis, overlapNDVOther);
        double newNDV = maxOverlapNDV + ((1 - overlapPercentThis) * distinctValues) +
                ((1 - overlapPercentOther) * other.distinctValues);
        return new StatisticRangeValues(Math.min(low, other.low), Math.max(high, other.high), newNDV);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StatisticRangeValues that = (StatisticRangeValues) o;

        if (Double.compare(that.low, low) != 0) {
            return false;
        }
        if (Double.compare(that.high, high) != 0) {
            return false;
        }
        return Double.compare(that.distinctValues, distinctValues) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(low, high, distinctValues);
    }
}
