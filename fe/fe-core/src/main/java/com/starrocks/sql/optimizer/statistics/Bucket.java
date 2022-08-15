// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.statistics;

public class Bucket {
    private final double lower;
    private final double upper;
    private final Long count;
    private final Long upperRepeats;

    public Bucket(double lower, double upper, Long count, Long upperRepeats) {
        this.lower = lower;
        this.upper = upper;
        this.count = count;
        this.upperRepeats = upperRepeats;
    }

    public double getLower() {
        return lower;
    }

    public double getUpper() {
        return upper;
    }

    public Long getCount() {
        return count;
    }

    public Long getUpperRepeats() {
        return upperRepeats;
    }
}