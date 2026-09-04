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

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * The analyze properties one histogram collection runs under, parsed once per collect() call.
 *
 * <p>All four are filled in together by AnalyzeStmtAnalyzer while analyzing the ANALYZE statement,
 * so they share a lifetime and are parsed as a unit. Only {@link NativeHistogramTraits} reads the
 * bucket-NDV mode; the external flavour carries it unused.
 */
final class HistogramCollectParams {
    private static final Logger LOG = LogManager.getLogger(HistogramCollectParams.class);

    private final double sampleRatio;
    private final long bucketNum;
    private final long mcvSize;
    private final StatsConstants.HistogramCollectBucketNdvMode bucketNdvMode;

    HistogramCollectParams(Map<String, String> properties) {
        this.sampleRatio = Double.parseDouble(properties.get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));
        this.bucketNum = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_BUCKET_NUM));
        this.mcvSize = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_MCV_SIZE));
        this.bucketNdvMode = parseBucketNdvMode(properties.get(StatsConstants.HISTOGRAM_COLLECT_BUCKET_NDV_MODE));
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

    StatsConstants.HistogramCollectBucketNdvMode bucketNdvMode() {
        return bucketNdvMode;
    }

    /**
     * Resolved once per collection, so an unusable mode is reported once rather than once per
     * column, and never fails the analyze job.
     */
    @VisibleForTesting
    static StatsConstants.HistogramCollectBucketNdvMode parseBucketNdvMode(String mode) {
        if ("none".equalsIgnoreCase(mode)) {
            return StatsConstants.HistogramCollectBucketNdvMode.NONE;
        } else if ("sample".equalsIgnoreCase(mode)) {
            return StatsConstants.HistogramCollectBucketNdvMode.SAMPLE;
        } else if ("hll".equalsIgnoreCase(mode)) {
            return StatsConstants.HistogramCollectBucketNdvMode.HLL;
        } else {
            LOG.warn("Invalid histogram collect bucket ndv mode {}.", mode);
            return StatsConstants.HistogramCollectBucketNdvMode.NONE;
        }
    }
}
