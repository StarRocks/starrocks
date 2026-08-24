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

/**
 * What a histogram collection strategy needs from the job it collects for, regardless of strategy.
 * Implementations wrap the job rather than being it, which is why job() exists: it lets a collector
 * take a single collaborator and still reach the job's inherited execution helpers.
 */
interface HistogramStatsCollectionUtils {
    StatisticsCollectJob job();

    /**
     * SQL for the most-common-values query of one column.
     */
    String buildMcvQuery(HistogramCollectParams params, String columnName);

    /**
     * Ratio the MCV counts were collected under, used to scale them back to full-table counts.
     * Native returns the sample ratio; external's MCV query is unsampled, so it returns 1.0.
     */
    double mcvCountScaleRatio(HistogramCollectParams params);
}
