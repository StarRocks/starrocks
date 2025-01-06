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
import com.starrocks.common.Config;
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

public class TableStatsCacheLoader implements AsyncCacheLoader<TableStatsCacheKey, Optional<Long>> {
    private static final Logger LOG = LogManager.getLogger(TableStatsCacheLoader.class);
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull CompletableFuture<Optional<Long>> asyncLoad(@NonNull TableStatsCacheKey cacheKey, @
            NonNull Executor executor) {

        return asyncLoadAll(Lists.newArrayList(cacheKey), executor)
                .thenApply(x -> x.get(cacheKey));
    }

    @Override
    public @NonNull CompletableFuture<Map<@NonNull TableStatsCacheKey, @NonNull Optional<Long>>> asyncLoadAll(
            @NonNull Iterable<? extends @NonNull TableStatsCacheKey> cacheKey,
            @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            Map<TableStatsCacheKey, Optional<Long>> result = new HashMap<>();
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                List<Long> pids = Lists.newArrayList();
                long tableId = -1;
                for (TableStatsCacheKey statsCacheKey : cacheKey) {
                    pids.add(statsCacheKey.getPartitionId());
                    tableId = statsCacheKey.getTableId();
                    if (pids.size() > Config.expr_children_limit / 2) {
                        List<TStatisticData> statisticData =
                                statisticExecutor.queryTableStats(connectContext, statsCacheKey.getTableId(), pids);

                        statisticData.forEach(tStatisticData -> result.put(
                                new TableStatsCacheKey(statsCacheKey.getTableId(), tStatisticData.partitionId),
                                Optional.of(tStatisticData.rowCount)));
                        pids.clear();
                    }
                }
                List<TStatisticData> statisticData = statisticExecutor.queryTableStats(connectContext, tableId, pids);
                for (TStatisticData data : statisticData) {
                    result.put(new TableStatsCacheKey(tableId, data.partitionId), Optional.of(data.rowCount));
                }
                for (TableStatsCacheKey key : cacheKey) {
                    if (!result.containsKey(key)) {
                        result.put(key, Optional.empty());
                    }
                }
                return result;
            } catch (RuntimeException e) {
                LOG.error(e);
                for (TableStatsCacheKey key : cacheKey) {
                    result.put(key, Optional.empty());
                }
                return result;
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }
}
