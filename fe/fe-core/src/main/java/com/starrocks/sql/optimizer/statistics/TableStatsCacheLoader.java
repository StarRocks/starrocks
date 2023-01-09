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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

public class TableStatsCacheLoader implements AsyncCacheLoader<TableStatsCacheKey, Optional<TableStatistic>> {
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull CompletableFuture<Optional<TableStatistic>> asyncLoad(@NonNull TableStatsCacheKey cacheKey, @
            NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                List<TStatisticData> statisticData = queryStatisticsData(connectContext, cacheKey.tableId, cacheKey.partitionId);
                if (statisticData.size() == 0) {
                    return Optional.of(new TableStatistic(cacheKey.getTableId(), cacheKey.getPartitionId(), 0L));
                } else {
                    return Optional.of(new TableStatistic(cacheKey.getTableId(), cacheKey.getPartitionId(),
                            statisticData.get(0).rowCount));
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    @Override
    public @NonNull CompletableFuture<Map<@NonNull TableStatsCacheKey, @NonNull Optional<TableStatistic>>> asyncLoadAll(
            @NonNull Iterable<? extends @NonNull TableStatsCacheKey> cacheKey,
            @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                TableStatsCacheKey tableStatsCacheKey = cacheKey.iterator().next();
                long tableId = tableStatsCacheKey.getTableId();

                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                List<TStatisticData> statisticData = queryStatisticsData(connectContext, tableStatsCacheKey.getTableId());

                Map<TableStatsCacheKey, Optional<TableStatistic>> result = new HashMap<>();
                for (TStatisticData tStatisticData : statisticData) {
                    result.put(new TableStatsCacheKey(tableId, tStatisticData.partitionId),
                            Optional.of(new TableStatistic(tableId, tStatisticData.partitionId, tStatisticData.rowCount)));
                }
                for (TableStatsCacheKey key : cacheKey) {
                    if (!result.containsKey(key)) {
                        result.put(key, Optional.of(new TableStatistic(key.getTableId(), key.getPartitionId(), 0L)));
                    }
                }

                return result;

            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    private List<TStatisticData> queryStatisticsData(ConnectContext context, long tableId) {
        return statisticExecutor.queryTableStats(context, tableId);
    }

    private List<TStatisticData> queryStatisticsData(ConnectContext context, long tableId, long partitionId) {
        return statisticExecutor.queryTableStats(context, tableId, partitionId);
    }
}
