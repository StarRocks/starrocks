// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CachedStatisticStorage implements StatisticStorage {
    private static final Logger LOG = LogManager.getLogger(CachedStatisticStorage.class);

    private final Executor statsCacheRefresherExecutor = Executors.newFixedThreadPool(Config.statistic_cache_thread_pool_size,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("stats-cache-refresher-%d").build());

    AsyncLoadingCache<ColumnStatsCacheKey, Optional<ColumnStatistic>> cachedStatistics = Caffeine.newBuilder()
            .expireAfterWrite(Config.statistic_update_interval_sec * 2, TimeUnit.SECONDS)
            .refreshAfterWrite(Config.statistic_update_interval_sec, TimeUnit.SECONDS)
            .maximumSize(Config.statistic_cache_columns)
            .executor(statsCacheRefresherExecutor)
            .buildAsync(new ColumnBasicStatsCacheLoader());

    AsyncLoadingCache<ColumnStatsCacheKey, Optional<Histogram>> histogramCache = Caffeine.newBuilder()
            .expireAfterWrite(Config.statistic_update_interval_sec * 2, TimeUnit.SECONDS)
            .refreshAfterWrite(Config.statistic_update_interval_sec, TimeUnit.SECONDS)
            .maximumSize(Config.statistic_cache_columns)
            .executor(statsCacheRefresherExecutor)
            .buildAsync(new ColumnHistogramStatsCacheLoader());

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
                    cachedStatistics.get(new ColumnStatsCacheKey(table.getId(), column));
            if (result.isDone()) {
                Optional<ColumnStatistic> realResult;
                realResult = result.get();
                return realResult.orElseGet(ColumnStatistic::unknown);
            } else {
                return ColumnStatistic.unknown();
            }
        } catch (Exception e) {
            LOG.warn(e);
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
            CompletableFuture<Map<ColumnStatsCacheKey, Optional<ColumnStatistic>>> result = cachedStatistics.getAll(cacheKeys);
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
            LOG.warn(e);
            return getDefaultColumnStatisticList(columns);
        }
    }

    @Override
    public List<ColumnStatistic> getColumnStatisticsSync(Table table, List<String> columns) {
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
            Map<ColumnStatsCacheKey, Optional<ColumnStatistic>> result = cachedStatistics.synchronous().getAll(cacheKeys);
            List<ColumnStatistic> columnStatistics = new ArrayList<>();

            for (String column : columns) {
                Optional<ColumnStatistic> columnStatistic =
                        result.getOrDefault(new ColumnStatsCacheKey(tableId, column), Optional.empty());
                if (columnStatistic.isPresent()) {
                    columnStatistics.add(columnStatistic.get());
                } else {
                    columnStatistics.add(ColumnStatistic.unknown());
                }
            }
            return columnStatistics;
        } catch (Exception e) {
            LOG.warn("Get column statistic fail, message : " + e.getMessage());
            return getDefaultColumnStatisticList(columns);
        }
    }

    @Override
    public void expireColumnStatistics(Table table, List<String> columns) {
        if (columns == null) {
            return;
        }
        List<ColumnStatsCacheKey> allKeys = Lists.newArrayList();
        for (String column : columns) {
            ColumnStatsCacheKey key = new ColumnStatsCacheKey(table.getId(), column);
            allKeys.add(key);
        }
        cachedStatistics.synchronous().invalidateAll(allKeys);
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
        this.cachedStatistics.synchronous().put(new ColumnStatsCacheKey(table.getId(), column), Optional.of(columnStatistic));
    }

    @Override
    public Map<String, Histogram> getHistogramStatistics(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        List<String> columnHasHistogram = new ArrayList<>();
        for (String columnName : columns) {
            if (GlobalStateMgr.getCurrentAnalyzeMgr().getHistogramStatsMetaMap()
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
            LOG.warn(e);
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

    private List<ColumnStatistic> getDefaultColumnStatisticList(List<String> columns) {
        List<ColumnStatistic> columnStatisticList = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            columnStatisticList.add(ColumnStatistic.unknown());
        }
        return columnStatisticList;
    }
}
