// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

public class Bucket {
    private final String lower;
    private final String upper;
    private final Long count;
    private final Long upperRepeats;

    public Bucket(String lower, String upper, Long count, Long upperRepeats) {
        this.lower = lower;
        this.upper = upper;
        this.count = count;
        this.upperRepeats = upperRepeats;
    }

    public String getLower() {
        return lower;
    }

    public String getUpper() {
        return upper;
    }

    public Long getCount() {
        return count;
    }

    public Long getUpperRepeats() {
        return upperRepeats;
    }
}