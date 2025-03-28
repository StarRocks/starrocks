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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.COLUMN_ID_SEPARATOR;

public class MultiColumnCombinedStatsCacheLoader implements AsyncCacheLoader<Long, Optional<MultiColumnCombinedStatistics>> {
    private static final Logger LOG = LogManager.getLogger(MultiColumnCombinedStatsCacheLoader.class);
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull CompletableFuture<Optional<MultiColumnCombinedStatistics>> asyncLoad(
            @NonNull Long key, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            if (FeConstants.enableUnitStatistics) {
                return Optional.empty();
            }
            Optional<MultiColumnCombinedStatistics> result = Optional.empty();
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                List<TStatisticData> statisticData = queryStatisticsData(connectContext, List.of(key));
                if (!statisticData.isEmpty()) {
                    Map<Long, Optional<MultiColumnCombinedStatistics>> resultMap = convert2MultiColumnStats(statisticData);
                    result = resultMap.getOrDefault(key, Optional.empty());
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

    @Override
    public @NonNull CompletableFuture<Map<@NonNull Long, @NonNull Optional<MultiColumnCombinedStatistics>>> asyncLoadAll(
            @NonNull Iterable<? extends @NonNull Long> keys, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            Map<Long, Optional<MultiColumnCombinedStatistics>> result = new HashMap<>();
            for (Long key : keys) {
                result.put(key, Optional.empty());
            }

            if (FeConstants.enableUnitStatistics) {
                return result;
            }

            try {
                ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
                statsConnectCtx.setThreadLocalInfo();

                List<TStatisticData> statisticData = queryStatisticsData(statsConnectCtx, Lists.newArrayList(keys));
                result = convert2MultiColumnStats(statisticData);
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

    @Override
    public @NonNull CompletableFuture<Optional<MultiColumnCombinedStatistics>> asyncReload(
            @NonNull Long key, @NonNull Optional<MultiColumnCombinedStatistics> oldValue, @NonNull Executor executor) {
        return asyncLoad(key, executor);
    }

    private List<TStatisticData> queryStatisticsData(ConnectContext context, List<Long> tableIds) {
        return statisticExecutor.queryMultiColumnCombinedStats(context, tableIds);
    }

    private Map<Long, Optional<MultiColumnCombinedStatistics>> convert2MultiColumnStats(List<TStatisticData> statisticDatas) {
        Map<Long, Optional<MultiColumnCombinedStatistics>> result = new HashMap<>();
        if (statisticDatas == null || statisticDatas.isEmpty()) {
            return result;
        }

        for (TStatisticData statisticData : statisticDatas) {
            long dbId = statisticData.getDbId();
            long tableId = statisticData.getTableId();
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
            if (!(table instanceof OlapTable)) {
                continue;
            }

            String columnIdsStr = statisticData.getColumnName();
            long ndv = statisticData.getCountDistinct();
            Set<Integer> columnIds = Arrays.stream(columnIdsStr.split(COLUMN_ID_SEPARATOR))
                    .map(Integer::parseInt)
                    .collect(Collectors.toSet());

            result.computeIfAbsent(tableId, k -> Optional.of(new MultiColumnCombinedStatistics(columnIds, ndv)))
                    .ifPresent(statistics -> statistics.update(columnIds, ndv));
        }

        return result;
    }

}
