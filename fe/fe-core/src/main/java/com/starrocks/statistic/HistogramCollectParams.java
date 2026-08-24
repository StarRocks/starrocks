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

package com.starrocks.statistic;

import java.util.Map;

/**
 * The analyze properties a histogram collection needs, parsed once per collect() call.
 * Deliberately excludes the bucket-NDV mode: that is a native-only concept, so
 * HistogramStatisticsCollectJob derives it itself rather than exposing it to shared code.
 */
final class HistogramCollectParams {
    private final double sampleRatio;
    private final long bucketNum;
    private final long mcvSize;

    HistogramCollectParams(Map<String, String> properties) {
        this.sampleRatio = Double.parseDouble(properties.get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));
        this.bucketNum = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_BUCKET_NUM));
        this.mcvSize = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_MCV_SIZE));
    }

    double sampleRatio() {
        return sampleRatio;
    }

    long bucketNum() {
        return bucketNum;
    }

    long mcvSize() {
        return mcvSize;
    }
}
