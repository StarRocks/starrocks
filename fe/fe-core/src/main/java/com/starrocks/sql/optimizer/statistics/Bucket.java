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

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class Bucket implements Comparable<Bucket> {
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

    public boolean isOverlapped(Bucket other) {
        return Math.min(upper, other.upper) - Math.max(lower, other.lower) >= 0;
    }

    @Override
    public int compareTo(@NotNull Bucket o) {
        int lowerComparison = Double.compare(this.lower, o.lower);
        if (lowerComparison != 0) {
            return lowerComparison;
        }
        return Double.compare(this.upper, o.upper);
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
        return Double.compare(lower, bucket.lower) == 0 && Double.compare(upper, bucket.upper) == 0 &&
                Objects.equals(count, bucket.count) &&
                Objects.equals(upperRepeats, bucket.upperRepeats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lower, upper, count, upperRepeats);
    }
}