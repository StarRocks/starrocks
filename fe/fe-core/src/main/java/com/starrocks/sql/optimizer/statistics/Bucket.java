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

import java.util.Optional;

public class Bucket {
    private final double lower;
    private final double upper;
    private final Long count;
    private final Long upperRepeats;
    private final Optional<Long> distinctCount;

    public Bucket(double lower, double upper, Long count, Long upperRepeats) {
        this.lower = lower;
        this.upper = upper;
        this.count = count;
        this.upperRepeats = upperRepeats;
        this.distinctCount = Optional.empty();
    }

    public Bucket(double lower, double upper, Long count, Long upperRepeats, Long distinctCount) {
        this.lower = lower;
        this.upper = upper;
        this.count = count;
        this.upperRepeats = upperRepeats;
        this.distinctCount = Optional.of(distinctCount);
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

    public Optional<Long> getRowCountInBucket(double value, Long previousBucketCount, double distinctValuesCount,
                                              boolean useFixedPointEstimation) {
        if (lower <= value && value < upper) {
            long rowCount = count - previousBucketCount - upperRepeats;

            if (distinctCount.isPresent()) {
                distinctValuesCount = distinctCount.get() - 1;
            } else if (useFixedPointEstimation) {
                distinctValuesCount = upper - lower;
            }

            rowCount = (long) Math.ceil(Math.max(1, rowCount / Math.max(1, distinctValuesCount)));
            return Optional.of(rowCount);
        } else if (upper == value) {
            return Optional.of(upperRepeats);
        }

        return Optional.empty();
    }
}