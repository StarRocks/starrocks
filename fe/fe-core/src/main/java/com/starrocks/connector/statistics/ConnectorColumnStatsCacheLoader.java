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


package com.starrocks.connector.statistics;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.ColumnBasicStatsCacheLoader;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static com.starrocks.connector.statistics.StatisticsUtils.getTableByUUID;

public class ConnectorColumnStatsCacheLoader implements
        AsyncCacheLoader<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> {
    private static final Logger LOG = LogManager.getLogger(ConnectorColumnStatsCacheLoader.class);
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();
    @Override
    public @NonNull CompletableFuture<Optional<ConnectorTableColumnStats>> asyncLoad(@NonNull ConnectorTableColumnKey cacheKey,
                                                                                     @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                List<TStatisticData> statisticData = queryStatisticsData(connectContext, cacheKey.tableUUID, cacheKey.column);
                // check TStatisticData is not empty, There may be no such column Statistics in BE
                if (!statisticData.isEmpty()) {
                    return Optional.of(convert2ColumnStatistics(cacheKey.tableUUID, statisticData.get(0)));
                } else {
                    return Optional.empty();
                }
            } catch (RuntimeException e) {
                LOG.error(e);
                return Optional.empty();
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Map<@NonNull ConnectorTableColumnKey, @NonNull Optional<ConnectorTableColumnStats>>> asyncLoadAll(
            @NonNull Iterable<? extends @NonNull ConnectorTableColumnKey> keys, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> result = Maps.newHashMap();
            // There may be no statistics for the column in BE
            // Complete the list of statistics information, otherwise the columns without statistics may be called repeatedly
            for (ConnectorTableColumnKey cacheKey : keys) {
                result.put(cacheKey, Optional.empty());
            }

            try {
                String tableUUID = null;
                List<String> columns = new ArrayList<>();
                for (ConnectorTableColumnKey key : keys) {
                    tableUUID = key.tableUUID;
                    columns.add(key.column);
                }

                ConnectContext statsConnectCtx = StatisticUtils.buildConnectContext();
                statsConnectCtx.setThreadLocalInfo();
                List<TStatisticData> statisticData = queryStatisticsData(statsConnectCtx, tableUUID, columns);

                for (TStatisticData data : statisticData) {
                    ConnectorTableColumnStats columnStatistic = convert2ColumnStatistics(tableUUID, data);
                    result.put(new ConnectorTableColumnKey(tableUUID, data.columnName),
                            Optional.of(columnStatistic));
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
    public CompletableFuture<Optional<ConnectorTableColumnStats>> asyncReload(
            @NonNull ConnectorTableColumnKey key, @NonNull Optional<ConnectorTableColumnStats> oldValue,
            @NonNull Executor executor) {
        return asyncLoad(key, executor);
    }

    public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID, String column) {
        return queryStatisticsData(context, tableUUID, ImmutableList.of(column));
    }

    public List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID, List<String> columns) {
        Table table = getTableByUUID(tableUUID);
        return statisticExecutor.queryStatisticSync(context, tableUUID, table, columns);
    }

    private ConnectorTableColumnStats convert2ColumnStatistics(String tableUUID, TStatisticData statisticData) {
        Table table = getTableByUUID(tableUUID);
        Type columnType = StatisticUtils.getQueryStatisticsColumnType(table, statisticData.columnName);

        String[] splits = tableUUID.split("\\.");

        ColumnStatistic columnStatistic = ColumnBasicStatsCacheLoader.buildColumnStatistics(statisticData, splits[0],
                splits[1], splits[3], statisticData.columnName, columnType);
        return new ConnectorTableColumnStats(columnStatistic, statisticData.rowCount, statisticData.updateTime);
    }
}
