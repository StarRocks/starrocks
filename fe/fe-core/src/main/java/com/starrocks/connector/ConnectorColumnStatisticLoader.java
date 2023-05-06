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


package com.starrocks.connector;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.statistics.ColumnBasicStatsCacheLoader;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
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

public class ConnectorColumnStatisticLoader implements
        AsyncCacheLoader<ConnectorTableColumnKey, Optional<ConnectorTableColumnStat>> {
    private static final Logger LOG = LogManager.getLogger(ConnectorColumnStatisticLoader.class);
    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    @Override
    public @NonNull CompletableFuture<Optional<ConnectorTableColumnStat>> asyncLoad(@NonNull ConnectorTableColumnKey cacheKey,
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
                throw e;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Map<@NonNull ConnectorTableColumnKey, @NonNull Optional<ConnectorTableColumnStat>>> asyncLoadAll(
            @NonNull Iterable<? extends @NonNull ConnectorTableColumnKey> keys, @NonNull Executor executor) {
        return CompletableFuture.supplyAsync(() -> {

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
                Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStat>> result = new HashMap<>();
                // There may be no statistics for the column in BE
                // Complete the list of statistics information, otherwise the columns without statistics may be called repeatedly
                for (ConnectorTableColumnKey cacheKey : keys) {
                    result.put(cacheKey, Optional.empty());
                }

                for (TStatisticData data : statisticData) {
                    ConnectorTableColumnStat columnStatistic = convert2ColumnStatistics(tableUUID, data);
                    result.put(new ConnectorTableColumnKey(tableUUID, data.columnName),
                            Optional.of(columnStatistic));
                }
                return result;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Optional<ConnectorTableColumnStat>> asyncReload(
            @NonNull ConnectorTableColumnKey key, @NonNull Optional<ConnectorTableColumnStat> oldValue,
            @NonNull Executor executor) {
        return asyncLoad(key, executor);
    }

    private List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID, String column) {
        return queryStatisticsData(context, tableUUID, ImmutableList.of(column));
    }

    private List<TStatisticData> queryStatisticsData(ConnectContext context, String tableUUID, List<String> columns) {
        return statisticExecutor.queryStatisticSync(context, tableUUID, columns);
    }

    private ConnectorTableColumnStat convert2ColumnStatistics(String tableUUID, TStatisticData statisticData)
            throws AnalysisException {
        Table table = MetaUtils.getTableByUUID(tableUUID);
        Column column = table.getColumn(statisticData.columnName);
        if (column == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, statisticData.columnName);
        }

        ColumnStatistic columnStatistic = ColumnBasicStatsCacheLoader.buildColumnStatistics(statisticData, column);
        return new ConnectorTableColumnStat(columnStatistic, statisticData.rowCount);
    }
}
