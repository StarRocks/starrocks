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


package com.starrocks.metric;

import com.starrocks.scheduler.PartitionBasedMvRefreshProcessor;

import java.util.List;

public interface IMaterializedViewMetricsEntity {

    /**
     * Get all metrics
     */
    List<Metric> getMetrics();

    /**
     * Increase the count of query considered which is used as mv candidate
     * @param count: increase count
     */
    void increaseQueryConsideredCount(long count);

    /**
     * Increase the count of query matched which is rewritten by success
     * @param count: increase count
     */
    void increaseQueryMatchedCount(long count);

    /**
     * Increase the count of query text matched which is rewritten by success
     * @param count: increase count
     */
    void increaseQueryTextBasedMatchedCount(long count);

    /**
     * Increase the count of query hit which is rewritten by success and chosen at the end
     * @param count: increase count
     */
    void increaseQueryHitCount(long count);

    /**
     * Increase the count of query materialized view including both rewritten and direct query
     * @param count: increase count
     */
    void increaseQueryMaterializedViewCount(long count);

    /**
     * Increase the refresh job status count
     * @param status: refresh job 's final status: SUCCESS, FAILED, EMPTY
     */
    void increaseRefreshJobStatus(PartitionBasedMvRefreshProcessor.RefreshJobStatus status);

    /**
     * Increase the refresh meta retry count
     * @param retryNum: the retry meta count
     */
    void increaseRefreshRetryMetaCount(Long retryNum);

    /**
     * Increase the refresh duration
     * @param duration: mv refresh duration
     */
    void updateRefreshDuration(long duration);
}

