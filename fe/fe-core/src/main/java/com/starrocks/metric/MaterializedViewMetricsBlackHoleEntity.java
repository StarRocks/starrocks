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

import com.google.api.client.util.Lists;
import com.starrocks.scheduler.PartitionBasedMvRefreshProcessor;

import java.util.List;

public class MaterializedViewMetricsBlackHoleEntity implements IMaterializedViewMetricsEntity {
    @Override
    public List<Metric> getMetrics() {
        return Lists.newArrayList();
    }

    @Override
    public void increaseQueryConsideredCount(long count) {
    }

    @Override
    public void increaseQueryMatchedCount(long count) {

    }

    @Override
    public void increaseQueryTextBasedMatchedCount(long count) {

    }

    @Override
    public void increaseQueryHitCount(long count) {

    }

    @Override
    public void increaseQueryMaterializedViewCount(long count) {

    }

    @Override
    public void increaseRefreshJobStatus(PartitionBasedMvRefreshProcessor.RefreshJobStatus status) {

    }

    @Override
    public void increaseRefreshRetryMetaCount(Long retryNum) {

    }

    @Override
    public void updateRefreshDuration(long duration) {

    }
}

