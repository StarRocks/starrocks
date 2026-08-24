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

import com.starrocks.qe.ConnectContext;
import com.starrocks.type.Type;

import java.util.Map;

/**
 * The job-specific half of the legacy (one INSERT ... SELECT per column) strategy.
 *
 * @see LegacyHistogramCollector
 */
interface LegacyStatsCollectionUtils extends HistogramStatsCollectionUtils {
    /**
     * The complete INSERT statement for one column. Receives the collector's StatisticExecutor
     * because some variants (native HLL mode) run an intermediate query to derive bucket
     * boundaries before the INSERT can be built, and must do so on the same executor.
     */
    String buildSqlCmd(ConnectContext context, StatisticExecutor executor, HistogramCollectParams params,
                       String columnName, Type columnType, Map<String, String> mostCommonValues)
            throws Exception;

    /**
     * Hook run after a column's row is written. No-op unless the flavour needs per-column cleanup.
     */
    void afterColumnInserted(ConnectContext context, StatisticExecutor executor, String columnName);
}
