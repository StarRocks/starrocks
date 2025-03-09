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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.HistogramUtils;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static com.starrocks.connector.statistics.StatisticsUtils.getTableByUUID;

public class ConnectorHistogramColumnStatsCacheLoader implements
        AsyncCacheLoader<ConnectorTableColumnKey, Optional<Histogram>> {
    private static final Logger LOG = LogManager.getLogger(ConnectorHistogramColumnStatsCacheLoader.class);
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull
    CompletableFuture<Optional<Histogram>> asyncLoad(@NonNull ConnectorTableColumnKey cacheKey,
                                                     @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();
                List<TStatisticData> statisticData =
                        queryHistogramStatistics(connectContext, cacheKey.tableUUID, Lists.newArrayList(cacheKey.column));
                // check TStatisticData is not empty, There may be no such column Statistics in BE
                if (!statisticData.isEmpty()) {
                    return Optional.of(convert2Histogram(cacheKey.tableUUID, statisticData.get(0)));
                } else {
                    return Optional.empty();
                }
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
    public CompletableFuture<Map<@NonNull ConnectorTableColumnKey, @NonNull Optional<Histogram>>> asyncLoadAll(
            @NonNull Iterable<? extends @NonNull ConnectorTableColumnKey> keys, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            Map<ConnectorTableColumnKey, Optional<Histogram>> result = new HashMap<>();
            String tableUUID = null;
            List<String> columns = new ArrayList<>();
            if (!keys.iterator().hasNext()) {
                return result;
            }
            for (ConnectorTableColumnKey key : keys) {
                tableUUID = key.tableUUID;
                columns.add(key.column);
                result.put(key, Optional.empty());
            }

            try {
                ConnectContext connectContext = StatisticUtils.buildConnectContext();
                connectContext.setThreadLocalInfo();

                List<TStatisticData> histogramStatsDataList = queryHistogramStatistics(connectContext, tableUUID, columns);
                for (TStatisticData histogramStatsData : histogramStatsDataList) {
                    Histogram histogram = convert2Histogram(tableUUID, histogramStatsData);
                    result.put(new ConnectorTableColumnKey(tableUUID, histogramStatsData.columnName),
                            Optional.of(histogram));
                }

                return result;
            } catch (RuntimeException e) {
                LOG.error(e);
                return result;
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                ConnectContext.remove();
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Optional<Histogram>> asyncReload(
            @NonNull ConnectorTableColumnKey key, @NonNull Optional<Histogram> oldValue,
            @NonNull Executor executor) {
        return asyncLoad(key, executor);
    }

    public List<TStatisticData> queryHistogramStatistics(ConnectContext context, String tableUUID, List<String> column) {
        return statisticExecutor.queryHistogram(context, tableUUID, column);
    }

    private Histogram convert2Histogram(String tableUUID, TStatisticData statisticData) throws AnalysisException {
        Table table = getTableByUUID(tableUUID);
        Type columnType = StatisticUtils.getQueryStatisticsColumnType(table, statisticData.columnName);

        List<Bucket> buckets = HistogramUtils.convertBuckets(statisticData.histogram, columnType);
        Map<String, Long> mcv = HistogramUtils.convertMCV(statisticData.histogram);
        return new Histogram(buckets, mcv);
    }
}
