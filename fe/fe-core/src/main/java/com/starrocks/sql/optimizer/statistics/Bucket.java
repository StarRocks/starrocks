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

public class Bucket {
    // The ordinal of this bucket in the histogram
    // -1 means it's missed, otherwise it would be a positive value
    private final int ordinal;
    private final double lower;
    private final double upper;
    // Accumulated count in the histogram, but not the count of this bucket
    // To calculate the count of this bucket, you can subtract previous bucket
    private final Long accumulatedCount;
    private final Long upperRepeats;

    public Bucket(int ordinal, double lower, double upper, Long count, Long upperRepeats) {
        this.ordinal = ordinal;
        this.lower = lower;
        this.upper = upper;
        this.accumulatedCount = count;
        this.upperRepeats = upperRepeats;
    }

    public Bucket(double lower, double upper, Long count, Long upperRepeats) {
        this.ordinal = -1;
        this.lower = lower;
        this.upper = upper;
        this.accumulatedCount = count;
        this.upperRepeats = upperRepeats;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public double getLower() {
        return lower;
    }

    public double getUpper() {
        return upper;
    }

    public Long getCount() {
        return accumulatedCount;
    }

    public Long getUpperRepeats() {
        return upperRepeats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Bucket bucket = (Bucket) o;
        return ordinal == bucket.ordinal &&
                Double.compare(lower, bucket.lower) == 0 && Double.compare(upper, bucket.upper) == 0 &&
                Objects.equals(accumulatedCount, bucket.accumulatedCount) &&
                Objects.equals(upperRepeats, bucket.upperRepeats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ordinal, lower, upper, accumulatedCount, upperRepeats);
    }

    @Override
    public String toString() {
        return String.format("[%f,%f,%d,%d]", lower, upper, accumulatedCount, upperRepeats);
    }
}