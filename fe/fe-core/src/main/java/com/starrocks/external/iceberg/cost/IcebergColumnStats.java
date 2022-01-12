// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.cost;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IcebergColumnStats {
    private static final Logger LOG = LogManager.getLogger(IcebergColumnStats.class);

    // only valid for string type
    private double avgSize = -1.0f;
    private long numNulls = -1L;
    private long numDistinctValues = -1L;
    private double minValue = Double.NEGATIVE_INFINITY;
    private double maxValue = Double.POSITIVE_INFINITY;

    public IcebergColumnStats() {
    }

    @Override
    public String toString() {
        return String.format("avgSize: %.2f, numNulls: %d, numDistinctValues: %d, minValue: %.2f, maxValue: %.2f",
                avgSize, numNulls, numDistinctValues, minValue, maxValue);
    }

    public double getAvgSize() {
        return avgSize;
    }

    public void setAvgSize(double avgSize) {
        if (avgSize >= 0.0f) {
            this.avgSize = avgSize;
        }
    }

    public long getNumNulls() {
        return numNulls;
    }

    public void setNumNulls(long numNulls) {
        if (numNulls >= 0L) {
            this.numNulls = numNulls;
        }
    }

    public long getNumDistinctValues() {
        return numDistinctValues;
    }

    public void setNumDistinctValues(long numDistinctValues) {
        if (numDistinctValues >= 0L) {
            this.numDistinctValues = numDistinctValues;
        }
    }

    public double getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }
}
