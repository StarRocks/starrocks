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
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.statistics.ConnectorColumnStatsCacheLoader;
import com.starrocks.connector.statistics.ConnectorHistogramColumnStatsCacheLoader;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CachedStatisticStorage implements StatisticStorage, MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(CachedStatisticStorage.class);

    private final Executor statsCacheRefresherExecutor = Executors.newFixedThreadPool(Config.statistic_cache_thread_pool_size,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("stats-cache-refresher-%d").build());

    AsyncLoadingCache<TableStatsCacheKey, Optional<Long>> tableStatsCache =
            createAsyncLoadingCache(new TableStatsCacheLoader());

    AsyncLoadingCache<ColumnStatsCacheKey, Optional<ColumnStatistic>> columnStatistics =
            createAsyncLoadingCache(new ColumnBasicStatsCacheLoader());

    AsyncLoadingCache<ColumnStatsCacheKey, Optional<PartitionStats>> partitionStatistics =
            createAsyncLoadingCache(new PartitionStatsCacheLoader());

    AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> connectorTableCachedStatistics =
            createAsyncLoadingCache(new ConnectorColumnStatsCacheLoader());

    AsyncLoadingCache<ColumnStatsCacheKey, Optional<Histogram>> histogramCache =
            createAsyncLoadingCache(new ColumnHistogramStatsCacheLoader());

    AsyncLoadingCache<ConnectorTableColumnKey, Optional<Histogram>> connectorHistogramCache =
            createAsyncLoadingCache(new ConnectorHistogramColumnStatsCacheLoader());

    AsyncLoadingCache<Long, Optional<MultiColumnCombinedStatistics>> multiColumnStats =
            createAsyncLoadingCache(new MultiColumnCombinedStatsCacheLoader());

    @Override
    public Map<Long, Optional<Long>> getTableStatistics(Long tableId, Collection<Partition> partitions) {
        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(tableId)) {
            return partitions.stream().collect(Collectors.toMap(Partition::getId, p -> Optional.empty()));
        }

        List<TableStatsCacheKey> keys = partitions.stream().map(p -> new TableStatsCacheKey(tableId, p.getId()))
                .collect(Collectors.toList());

        try {
            CompletableFuture<Map<TableStatsCacheKey, Optional<Long>>> result = tableStatsCache.getAll(keys);
            if (result.isDone()) {
                Map<TableStatsCacheKey, Optional<Long>> data = result.get();
                return keys.stream().collect(Collectors.toMap(TableStatsCacheKey::getPartitionId,
                        k -> data.getOrDefault(k, Optional.empty())));
            }
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute tableStatsCache.getAll", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Faied to execute tableStatsCache.getAll", e);
        }
        return partitions.stream().collect(Collectors.toMap(Partition::getId, p -> Optional.empty()));
    }

    @Override
    public void refreshTableStatistic(Table table, boolean isSync) {
        List<TableStatsCacheKey> statsCacheKeyList = new ArrayList<>();
        for (Partition partition : table.getPartitions()) {
            statsCacheKeyList.add(new TableStatsCacheKey(table.getId(), partition.getId()));
        }

        try {
            TableStatsCacheLoader loader = new TableStatsCacheLoader();
            CompletableFuture<Map<TableStatsCacheKey, Optional<Long>>> future = loader.asyncLoadAll(statsCacheKeyList,
                    statsCacheRefresherExecutor);
            if (isSync) {
                Map<TableStatsCacheKey, Optional<Long>> result = future.get();
                tableStatsCache.synchronous().putAll(result);
            } else {
                future.whenComplete((result, e) -> tableStatsCache.synchronous().putAll(result));
            }
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute refreshTableStatistic", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Failed to execute refreshTableStatistic", e);
        }
    }

    @Override
    public void refreshColumnStatistics(Table table, List<String> columns, boolean isSync) {
        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId()) ||
                !StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }

        List<ColumnStatsCacheKey> cacheKeys = new ArrayList<>();
        long tableId = table.getId();
        for (String column : columns) {
            cacheKeys.add(new ColumnStatsCacheKey(tableId, column));
        }

        try {
            ColumnBasicStatsCacheLoader loader = new ColumnBasicStatsCacheLoader();
            CompletableFuture<Map<ColumnStatsCacheKey, Optional<ColumnStatistic>>> future =
                    loader.asyncLoadAll(cacheKeys, statsCacheRefresherExecutor);
            if (isSync) {
                Map<ColumnStatsCacheKey, Optional<ColumnStatistic>> result = future.get();
                columnStatistics.synchronous().putAll(result);
            } else {
                future.whenComplete((res, e) -> columnStatistics.synchronous().putAll(res));
            }
        } catch (Exception e) {
            LOG.warn("Failed to refresh getColumnStatistics", e);
        }
    }

    @Override
    public List<ConnectorTableColumnStats> getConnectorTableStatistics(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return getDefaultConnectorTableStatistics(columns);
        }

        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return getDefaultConnectorTableStatistics(columns);
        }

        List<ConnectorTableColumnKey> cacheKeys = new ArrayList<>();
        for (String column : columns) {
            cacheKeys.add(new ConnectorTableColumnKey(table.getUUID(), column));
        }

        try {
            CompletableFuture<Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>>> result =
                    connectorTableCachedStatistics.getAll(cacheKeys);

            SessionVariable sessionVariable = ConnectContext.get() == null ?
                    GlobalStateMgr.getCurrentState().getVariableMgr().newSessionVariable() :
                    ConnectContext.get().getSessionVariable();
            result.whenCompleteAsync((res, e) -> {
                if (e != null) {
                    LOG.warn("Get connector table column statistics filed, exception: ", e);
                    return;
                }
                if (sessionVariable.isEnableQueryTriggerAnalyze() && GlobalStateMgr.getCurrentState().isLeader()) {
                    GlobalStateMgr.getCurrentState().getConnectorTableTriggerAnalyzeMgr().checkAndUpdateTableStats(res);
                }
            }, statsCacheRefresherExecutor);
            if (result.isDone()) {
                List<ConnectorTableColumnStats> columnStatistics = new ArrayList<>();
                Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> realResult;
                realResult = result.get();
                for (String column : columns) {
                    Optional<ConnectorTableColumnStats> columnStatistic =
                            realResult.getOrDefault(new ConnectorTableColumnKey(table.getUUID(), column),
                                    Optional.empty());
                    if (columnStatistic.isPresent()) {
                        columnStatistics.add(
                                StatisticsUtils.estimateColumnStatistics(table, column, columnStatistic.get()));
                    } else {
                        columnStatistics.add(ConnectorTableColumnStats.unknown());
                    }
                }
                return columnStatistics;
            } else {
                return getDefaultConnectorTableStatistics(columns);
            }
        } catch (Exception e) {
            LOG.warn("Failed to execute connectorTableCachedStatistics.getAll", e);
            return getDefaultConnectorTableStatistics(columns);
        }
    }

    @Override
    public List<ConnectorTableColumnStats> getConnectorTableStatisticsSync(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return getDefaultConnectorTableStatistics(columns);
        }

        List<ConnectorTableColumnKey> cacheKeys = new ArrayList<>();
        for (String column : columns) {
            cacheKeys.add(new ConnectorTableColumnKey(table.getUUID(), column));
        }

        try {
            Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> result =
                    connectorTableCachedStatistics.synchronous().getAll(cacheKeys);
            List<ConnectorTableColumnStats> columnStatistics = new ArrayList<>();

            for (String column : columns) {
                Optional<ConnectorTableColumnStats> columnStatistic =
                        result.getOrDefault(new ConnectorTableColumnKey(table.getUUID(), column), Optional.empty());
                if (columnStatistic.isPresent()) {
                    columnStatistics.add(columnStatistic.get());
                } else {
                    columnStatistics.add(ConnectorTableColumnStats.unknown());
                }
            }
            return columnStatistics;
        } catch (Exception e) {
            LOG.warn("Failed to execute getConnectorTableStatisticsSync", e);
            return getDefaultConnectorTableStatistics(columns);
        }
    }

    @Override
    public void expireConnectorTableColumnStatistics(Table table, List<String> columns) {
        if (table == null || columns == null) {
            return;
        }
        List<ConnectorTableColumnKey> allKeys = Lists.newArrayList();
        for (String column : columns) {
            ConnectorTableColumnKey key = new ConnectorTableColumnKey(table.getUUID(), column);
            allKeys.add(key);
        }
        connectorTableCachedStatistics.synchronous().invalidateAll(allKeys);
    }

    @Override
    public void refreshConnectorTableColumnStatistics(Table table, List<String> columns, boolean isSync) {
        Preconditions.checkState(table != null);
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return;
        }

        List<ConnectorTableColumnKey> cacheKeys = new ArrayList<>();
        for (String column : columns) {
            cacheKeys.add(new ConnectorTableColumnKey(table.getUUID(), column));
        }

        try {
            ConnectorColumnStatsCacheLoader loader = new ConnectorColumnStatsCacheLoader();
            CompletableFuture<Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>>> future =
                    loader.asyncLoadAll(cacheKeys, statsCacheRefresherExecutor);
            if (isSync) {
                Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStats>> result = future.get();
                connectorTableCachedStatistics.synchronous().putAll(result);
            } else {
                future.whenComplete((res, e) -> connectorTableCachedStatistics.synchronous().putAll(res));
            }
        } catch (Exception e) {
            LOG.warn("Failed to refresh getConnectorTableStatistics", e);
        }
    }

    @Override
    public ColumnStatistic getColumnStatistic(Table table, String column) {
        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return ColumnStatistic.unknown();
        }

        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return ColumnStatistic.unknown();
        }
        try {
            CompletableFuture<Optional<ColumnStatistic>> result =
                        columnStatistics.get(new ColumnStatsCacheKey(table.getId(), column));
            if (result.isDone()) {
                Optional<ColumnStatistic> realResult;
                realResult = result.get();
                return realResult.orElseGet(ColumnStatistic::unknown);
            } else {
                return ColumnStatistic.unknown();
            }
        } catch (Exception e) {
            LOG.warn("Failed to execute getColumnStatistic", e);
            return ColumnStatistic.unknown();
        }
    }

    // ColumnStatistic List sequence is guaranteed to be consistent with Columns
    @Override
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return getDefaultColumnStatisticList(columns);
        }

        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return getDefaultColumnStatisticList(columns);
        }

        List<ColumnStatsCacheKey> cacheKeys = new ArrayList<>();
        long tableId = table.getId();
        for (String column : columns) {
            cacheKeys.add(new ColumnStatsCacheKey(tableId, column));
        }

        try {
            CompletableFuture<Map<ColumnStatsCacheKey, Optional<ColumnStatistic>>> result =
                    columnStatistics.getAll(cacheKeys);
            if (result.isDone()) {
                List<ColumnStatistic> columnStatistics = new ArrayList<>();
                Map<ColumnStatsCacheKey, Optional<ColumnStatistic>> realResult;
                realResult = result.get();
                for (String column : columns) {
                    Optional<ColumnStatistic> columnStatistic =
                            realResult.getOrDefault(new ColumnStatsCacheKey(tableId, column), Optional.empty());
                    if (columnStatistic.isPresent()) {
                        columnStatistics.add(columnStatistic.get());
                    } else {
                        columnStatistics.add(ColumnStatistic.unknown());
                    }
                }
                return columnStatistics;
            } else {
                return getDefaultColumnStatisticList(columns);
            }
        } catch (Exception e) {
            LOG.warn("Failed to execute getColumnStatistics", e);
            return getDefaultColumnStatisticList(columns);
        }
    }

    /**
     *
     */
    private Map<String, PartitionStats> getColumnNDVForPartitions(Table table, List<Long> partitions,
                                                                  List<String> columns) {

        List<ColumnStatsCacheKey> cacheKeys = new ArrayList<>();
        long tableId = table.getId();
        for (String column : columns) {
            cacheKeys.add(new ColumnStatsCacheKey(tableId, column));
        }

        try {
            Map<ColumnStatsCacheKey, Optional<PartitionStats>> result =
                    partitionStatistics.synchronous().getAll(cacheKeys);

            Map<String, PartitionStats> columnStatistics = Maps.newHashMap();
            for (String column : columns) {
                Optional<PartitionStats> columnStatistic = result.get(new ColumnStatsCacheKey(tableId, column));
                columnStatistics.put(column, columnStatistic.orElse(null));
            }
            return columnStatistics;
        } catch (Exception e) {
            LOG.warn("Get partition NDV fail", e);
            return null;
        }
    }

    /**
     * We don't really maintain all statistics for partition, as most of them are not necessary.
     * Currently, the only partition-level statistics is DistinctCount, which may differs a lot among partitions
     */
    @Override
    public Map<Long, List<ColumnStatistic>> getColumnStatisticsOfPartitionLevel(Table table, List<Long> partitions,
                                                                                List<String> columns) {

        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return null;
        }
        if (!StatisticUtils.checkStatisticTableStateNormal()) {
            return null;
        }

        long tableId = table.getId();
        List<ColumnStatsCacheKey> cacheKeys = columns.stream()
                .map(x -> new ColumnStatsCacheKey(tableId, x))
                .collect(Collectors.toList());
        List<ColumnStatistic> columnStatistics = getColumnStatistics(table, columns);
        Map<String, PartitionStats> columnNDVForPartitions = getColumnNDVForPartitions(table, partitions, columns);
        if (MapUtils.isEmpty(columnNDVForPartitions)) {
            return null;
        }

        Map<Long, List<ColumnStatistic>> result = Maps.newHashMap();
        for (long partition : partitions) {
            List<ColumnStatistic> newStatistics = Lists.newArrayList();
            for (int i = 0; i < columns.size(); i++) {
                ColumnStatistic columnStatistic = columnStatistics.get(i);
                PartitionStats partitionStats = columnNDVForPartitions.get(columns.get(i));
                if (partitionStats == null) {
                    // some of the columns miss statistics
                    return null;
                }
                if (!partitionStats.getDistinctCount().containsKey(partition)) {
                    // some of the partitions miss statistics
                    return null;
                }
                double distinctCount = partitionStats.getDistinctCount().get(partition);
                ColumnStatistic newStats = ColumnStatistic.buildFrom(columnStatistic)
                                .setDistinctValuesCount(distinctCount).build();
                newStatistics.add(newStats);
            }
            result.put(partition, newStatistics);
        }
        return result;
    }

    @Override
    public void expireTableAndColumnStatistics(Table table, List<String> columns) {
        List<TableStatsCacheKey> tableStatsCacheKeys = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            tableStatsCacheKeys.add(new TableStatsCacheKey(table.getId(), partition.getId()));
        }
        tableStatsCache.synchronous().invalidateAll(tableStatsCacheKeys);

        if (columns == null) {
            return;
        }
        List<ColumnStatsCacheKey> allKeys = Lists.newArrayList();
        for (String column : columns) {
            ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), column);
            allKeys.add(key);
        }
        columnStatistics.synchronous().invalidateAll(allKeys);
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
        this.columnStatistics.synchronous()
                .put(new ColumnStatsCacheKey(table.getId(), column), Optional.of(columnStatistic));
    }

    @Override
    public Map<String, Histogram> getHistogramStatistics(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        List<String> columnHasHistogram = new ArrayList<>();
        for (String columnName : columns) {
            if (GlobalStateMgr.getCurrentState().getAnalyzeMgr().getHistogramStatsMetaMap()
                    .get(new Pair<>(table.getId(), columnName)) != null) {
                columnHasHistogram.add(columnName);
            }
        }

        List<ColumnStatsCacheKey> cacheKeys = new ArrayList<>();
        long tableId = table.getId();
        for (String columnName : columnHasHistogram) {
            cacheKeys.add(new ColumnStatsCacheKey(tableId, columnName));
        }

        try {
            CompletableFuture<Map<ColumnStatsCacheKey, Optional<Histogram>>> result = histogramCache.getAll(cacheKeys);
            if (result.isDone()) {
                Map<ColumnStatsCacheKey, Optional<Histogram>> realResult;
                realResult = result.get();

                Map<String, Histogram> histogramStats = new HashMap<>();
                for (String columnName : columns) {
                    Optional<Histogram> histogramStatistics =
                            realResult.getOrDefault(new ColumnStatsCacheKey(tableId, columnName), Optional.empty());
                    histogramStatistics.ifPresent(histogram -> histogramStats.put(columnName, histogram));
                }
                return histogramStats;
            } else {
                return Maps.newHashMap();
            }
        } catch (Exception e) {
            LOG.warn("Failed to execute getHistogramStatistics", e);
            return Maps.newHashMap();
        }
    }

    @Override
    public Map<String, Histogram> getConnectorHistogramStatistics(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        List<ConnectorTableColumnKey> cacheKeys = new ArrayList<>();
        for (String columnName : columns) {
            cacheKeys.add(new ConnectorTableColumnKey(table.getUUID(), columnName));
        }

        try {
            CompletableFuture<Map<ConnectorTableColumnKey, Optional<Histogram>>> result =
                    connectorHistogramCache.getAll(cacheKeys);
            if (result.isDone()) {
                Map<ConnectorTableColumnKey, Optional<Histogram>> realResult = result.get();

                Map<String, Histogram> histogramStats = Maps.newHashMap();
                for (String columnName : columns) {
                    Optional<Histogram> histogramStatistics =
                            realResult.getOrDefault(new ConnectorTableColumnKey(table.getUUID(), columnName), Optional.empty());
                    histogramStatistics.ifPresent(histogram -> histogramStats.put(columnName, histogram));
                }
                return histogramStats;
            } else {
                return Maps.newHashMap();
            }
        } catch (Exception e) {
            LOG.warn("Failed to execute getConnectorHistogramStatistics", e);
            return Maps.newHashMap();
        }
    }

    @Override
    public void expireHistogramStatistics(Long tableId, List<String> columns) {
        Preconditions.checkNotNull(columns);

        List<ColumnStatsCacheKey> allKeys = Lists.newArrayList();
        for (String column : columns) {
            ColumnStatsCacheKey key = new ColumnStatsCacheKey(tableId, column);
            allKeys.add(key);
        }
        histogramCache.synchronous().invalidateAll(allKeys);
    }


    @Override
    public void expireConnectorHistogramStatistics(Table table, List<String> columns) {
        if (table == null || columns == null) {
            return;
        }
        List<ConnectorTableColumnKey> allKeys = Lists.newArrayList();
        for (String column : columns) {
            ConnectorTableColumnKey key = new ConnectorTableColumnKey(table.getUUID(), column);
            allKeys.add(key);
        }
        connectorHistogramCache.synchronous().invalidateAll(allKeys);
    }

    private List<ColumnStatistic> getDefaultColumnStatisticList(List<String> columns) {
        List<ColumnStatistic> columnStatisticList = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            columnStatisticList.add(ColumnStatistic.unknown());
        }
        return columnStatisticList;
    }

    private List<ConnectorTableColumnStats> getDefaultConnectorTableStatistics(List<String> columns) {
        List<ConnectorTableColumnStats> connectorTableColumnStatsList = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            connectorTableColumnStatsList.add(ConnectorTableColumnStats.unknown());
        }
        return connectorTableColumnStatsList;
    }

    public MultiColumnCombinedStatistics getMultiColumnCombinedStatistics(Long tableId) {
        if (StatisticUtils.statisticTableBlackListCheck(tableId)) {
            return MultiColumnCombinedStatistics.EMPTY;
        }

        try {
            CompletableFuture<Optional<MultiColumnCombinedStatistics>> result = multiColumnStats.get(tableId);
            if (result.isDone()) {
                Optional<MultiColumnCombinedStatistics> data = result.get();
                return data.orElse(MultiColumnCombinedStatistics.EMPTY);
            }
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute tableStatsCache.getAll", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Faied to execute tableStatsCache.getAll", e);
        }
        return MultiColumnCombinedStatistics.EMPTY;
    }

    public void refreshMultiColumnStatistics(Long tableId) {
        try {
            MultiColumnCombinedStatsCacheLoader loader = new MultiColumnCombinedStatsCacheLoader();
            CompletableFuture<Optional<MultiColumnCombinedStatistics>> future =
                    loader.asyncLoad(tableId, statsCacheRefresherExecutor);
            Optional<MultiColumnCombinedStatistics> result = future.get();
            multiColumnStats.synchronous().put(tableId, result);
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute refresh multi-column combined statistics", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Failed to execute refresh multi-column combined statistics", e);
        }
    }

    public void expireMultiColumnStatistics(Long tableId) {
        Preconditions.checkNotNull(tableId);
        multiColumnStats.synchronous().invalidate(tableId);
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.<String, Long>builder()
                .put("TableStats", tableStatsCache.synchronous().estimatedSize())
                .put("ColumnStats", columnStatistics.synchronous().estimatedSize())
                .put("PartitionStats", partitionStatistics.synchronous().estimatedSize())
                .put("HistogramStats", histogramCache.synchronous().estimatedSize())
                .put("ConnectorTableStats", connectorTableCachedStatistics.synchronous().estimatedSize())
                .put("ConnectorHistogramStats", connectorHistogramCache.synchronous().estimatedSize())
                .build();
    }

    private <K, V> Pair<List<Object>, Long> sampleFromCache(AsyncLoadingCache<K, V> cache) {
        Map<K, CompletableFuture<V>> map = cache.asMap();
        if (map.isEmpty()) {
            return Pair.create(List.of(), 0L);
        }
        Map.Entry<K, CompletableFuture<V>> next = map.entrySet().iterator().next();
        V value = null;
        try {
            value = next.getValue().getNow(null);
        } catch (Exception e) {
            LOG.warn("sample load statistic cache failed", e);
        }
        if (value == null) {
            return Pair.create(List.of(next.getKey()), cache.synchronous().estimatedSize());
        }
        return Pair.create(List.of(next.getKey(), value), cache.synchronous().estimatedSize());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        return List.of(
                sampleFromCache(tableStatsCache),
                sampleFromCache(columnStatistics),
                sampleFromCache(partitionStatistics),
                sampleFromCache(histogramCache),
                sampleFromCache(connectorHistogramCache),
                sampleFromCache(connectorTableCachedStatistics)
        );
    }

    private <K, V> AsyncLoadingCache<K, V> createAsyncLoadingCache(AsyncCacheLoader<K, V> cacheLoader) {
        return Caffeine.newBuilder()
                .expireAfterWrite(Config.statistic_update_interval_sec * 2, TimeUnit.SECONDS)
                .refreshAfterWrite(Config.statistic_update_interval_sec, TimeUnit.SECONDS)
                .maximumSize(Config.statistic_cache_columns)
                .executor(statsCacheRefresherExecutor)
                .buildAsync(cacheLoader);
    }

}
