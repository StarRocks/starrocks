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
import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

public class PartitionStatsCacheLoader implements AsyncCacheLoader<ColumnStatsCacheKey, Optional<PartitionStats>> {
    private static final Logger LOG = LogManager.getLogger(PartitionStatsCacheLoader.class);
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull CompletableFuture<Optional<PartitionStats>> asyncLoad(@NonNull ColumnStatsCacheKey cacheKey,
                                                                          @NonNull Executor executor) {
        return asyncLoadAll(Lists.newArrayList(cacheKey), executor).thenApply(x -> x.get(cacheKey));
    }

    @Override
    public @NonNull CompletableFuture<Map<ColumnStatsCacheKey, Optional<PartitionStats>>>
    asyncLoadAll(@NonNull Iterable<? extends @NonNull ColumnStatsCacheKey> cacheKey, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            Map<ColumnStatsCacheKey, Optional<PartitionStats>> result = new HashMap<>();
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();

                long tableId = -1;
                List<String> columns = Lists.newArrayList();
                for (ColumnStatsCacheKey statsCacheKey : cacheKey) {
                    columns.add(statsCacheKey.column);
                    tableId = statsCacheKey.tableId;
                }
                List<TStatisticData> statisticData = statisticExecutor.queryPartitionLevelColumnNDV(connectContext,
                        tableId, Lists.newArrayList(), columns);
                for (TStatisticData data : statisticData) {
                    ColumnStatsCacheKey key = new ColumnStatsCacheKey(tableId, data.columnName);
                    Optional<PartitionStats> stats = result.computeIfAbsent(key, (x) -> Optional.of(new PartitionStats()));
                    stats.get().getDistinctCount().put(data.partitionId, (double) data.countDistinct);
                    stats.get().getNullFraction().put(data.partitionId, data.nullCount * 1.0 / Math.max(data.rowCount, 1));
                }
                for (ColumnStatsCacheKey key : cacheKey) {
                    if (!result.containsKey(key)) {
                        result.put(key, Optional.empty());
                    }
                }
                return result;
            } catch (RuntimeException e) {
                LOG.error(e);
                throw new CompletionException(e);
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }
}
