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
<<<<<<< HEAD
=======

    public boolean isInBucket(double value) {
        return lower <= value && value <= upper;
    }

    public Optional<Long> getRowCountInBucket(double value, Long previousBucketCount, double distinctValuesCount,
                                              boolean useFixedPointEstimation) {
        if (lower <= value && value < upper) {
            long rowCount = count - previousBucketCount - upperRepeats;

            if (useFixedPointEstimation) {
                rowCount = (long) Math.ceil(Math.max(1, rowCount / Math.max(1, (upper - lower))));
            } else {
                rowCount = (long) Math.ceil(Math.max(1, rowCount / Math.max(1, distinctValuesCount)));
            }

            return Optional.of(rowCount);
        } else if (upper == value) {
            return Optional.of(upperRepeats);
        }

        return Optional.empty();
    }
>>>>>>> 59303750f6 ([Enhancement]  Remove duplicates during join selectivity estimation with histograms (#58047))
}