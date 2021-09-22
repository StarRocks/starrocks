// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.clearspring.analytics.util.Lists;
import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class CachedStatisticStorage implements StatisticStorage {
    private static final Logger LOG = LogManager.getLogger(CachedStatisticStorage.class);

    private final StatisticExecutor statisticExecutor = new StatisticExecutor();

    private final AsyncCacheLoader<ColumnIdentifier, Optional<ColumnStatistic>> statisticLoader =
            new AsyncCacheLoader<ColumnIdentifier, Optional<ColumnStatistic>>() {
                @Override
                public @NonNull CompletableFuture<Optional<ColumnStatistic>> asyncLoad(@NonNull ColumnIdentifier columnIdentifier,
                                                                                       @NonNull Executor executor) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            List<TStatisticData> statisticData = queryStatisticsData(columnIdentifier.getTableId(),
                                    columnIdentifier.getColumnName());
                            // check TStatisticData is not empty, There may be no such column Statistics in BE
                            if (!statisticData.isEmpty()) {
                                return Optional.of(convert2ColumnStatistics(statisticData.get(0)));
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
                public CompletableFuture<Map<@NonNull ColumnIdentifier, @NonNull Optional<ColumnStatistic>>> asyncLoadAll(
                        @NonNull Iterable<? extends @NonNull ColumnIdentifier> keys, @NonNull Executor executor) {
                    return CompletableFuture.supplyAsync(() -> {
                        Map<ColumnIdentifier, Optional<ColumnStatistic>> result = new HashMap<>();
                        try {
                            long tableId = -1;
                            List<String> columns = new ArrayList<>();
                            for (ColumnIdentifier key : keys) {
                                tableId = key.getTableId();
                                columns.add(key.getColumnName());
                            }
                            List<TStatisticData> statisticData = queryStatisticsData(tableId, columns);
                            // check TStatisticData is not empty, There may be no such column Statistics in BE
                            if (!statisticData.isEmpty()) {
                                for (TStatisticData data : statisticData) {
                                    ColumnStatistic columnStatistic = convert2ColumnStatistics(data);
                                    result.put(new ColumnIdentifier(data.tableId, data.columnName),
                                            Optional.of(columnStatistic));
                                }
                            } else {
                                // put null for cache key which can't get TStatisticData from BE
                                for (ColumnIdentifier columnIdentifier : keys) {
                                    result.put(columnIdentifier, Optional.empty());
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

                @Override
                public CompletableFuture<Optional<ColumnStatistic>> asyncReload(
                        @NonNull ColumnIdentifier key, @NonNull Optional<ColumnStatistic> oldValue,
                        @NonNull Executor executor) {
                    return asyncLoad(key, executor);
                }
            };

    AsyncLoadingCache<ColumnIdentifier, Optional<ColumnStatistic>> cachedStatistics = Caffeine.newBuilder()
            .expireAfterWrite(Config.statistic_collect_interval_sec * 2, TimeUnit.SECONDS)
            .refreshAfterWrite(Config.statistic_collect_interval_sec, TimeUnit.SECONDS)
            .maximumSize(Config.statistic_cache_columns)
            .buildAsync(statisticLoader);

    @Override
    public void expireColumnStatistics(Table table, List<String> columns) {
        List<ColumnIdentifier> allKeys = Lists.newArrayList();
        for (String column : columns) {
            ColumnIdentifier key = new ColumnIdentifier(table.getId(), column);
            allKeys.add(key);
        }
        cachedStatistics.synchronous().invalidateAll(allKeys);
    }

    private List<TStatisticData> queryStatisticsData(long tableId, String column) {
        return queryStatisticsData(tableId, ImmutableList.of(column));
    }

    private List<TStatisticData> queryStatisticsData(long tableId, List<String> columns) {
        return statisticExecutor.queryStatisticSync(null, tableId, columns);
    }

    private ColumnStatistic convert2ColumnStatistics(TStatisticData statisticData) throws AnalysisException {
        Database db = Catalog.getCurrentCatalog().getDb(statisticData.dbId);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, statisticData.dbId);
        }
        Table table = db.getTable(statisticData.tableId);
        if (!(table instanceof OlapTable)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, statisticData.tableId);
        }
        Column column = table.getColumn(statisticData.columnName);
        if (column == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, statisticData.columnName);
        }

        ColumnStatistic.Builder builder = ColumnStatistic.builder();
        double minValue = Double.NEGATIVE_INFINITY;
        double maxValue = Double.POSITIVE_INFINITY;
        try {
            if (column.getPrimitiveType().isCharFamily()) {
                // do nothing
            } else if (column.getPrimitiveType().equals(PrimitiveType.DATE)) {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                if (statisticData.isSetMin() && !statisticData.getMin().isEmpty()) {
                    minValue = getLongFromDateTime(LocalDate.parse(statisticData.min, dtf).atStartOfDay());
                }
                if (statisticData.isSetMax() && !statisticData.getMax().isEmpty()) {
                    maxValue = getLongFromDateTime(LocalDate.parse(statisticData.max, dtf).atStartOfDay());
                }
            } else if (column.getPrimitiveType().equals(PrimitiveType.DATETIME)) {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                if (statisticData.isSetMin() && !statisticData.getMin().isEmpty()) {
                    minValue = getLongFromDateTime(LocalDateTime.parse(statisticData.min, dtf));
                }
                if (statisticData.isSetMax() && !statisticData.getMax().isEmpty()) {
                    maxValue = getLongFromDateTime(LocalDateTime.parse(statisticData.max, dtf));
                }
            } else {
                if (statisticData.isSetMin() && !statisticData.getMin().isEmpty()) {
                    minValue = Double.parseDouble(statisticData.min);
                }
                if (statisticData.isSetMax() && !statisticData.getMax().isEmpty()) {
                    maxValue = Double.parseDouble(statisticData.max);
                }
            }
        } catch (Exception e) {
            LOG.warn("convert TStatisticData to ColumnStatistics failed, db : {}, table : {}, column : {}, errMsg : {}",
                    db.getFullName(), table.getName(), column.getName(), e.getMessage());
        }

        return builder.setMinValue(minValue).
                setMaxValue(maxValue).
                setDistinctValuesCount(statisticData.countDistinct).
                setAverageRowSize(statisticData.dataSize / Math.max(statisticData.rowCount, 1)).
                setNullsFraction(statisticData.nullCount * 1.0 / Math.max(statisticData.rowCount, 1)).build();
    }

    @Override
    public ColumnStatistic getColumnStatistic(Table table, String column) {
        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return ColumnStatistic.unknown();
        }

        Table statisticsTable = StatisticUtils.getStatisticsTable();
        if (statisticsTable == null) {
            return ColumnStatistic.unknown();
        }

        CompletableFuture<Optional<ColumnStatistic>> result = cachedStatistics.get(new ColumnIdentifier(table.getId(), column));
        if (result.isDone()) {
            Optional<ColumnStatistic> realResult;
            try {
                realResult = result.get();
            } catch (Exception e) {
                LOG.warn(e);
                return ColumnStatistic.unknown();
            }
            return realResult.orElseGet(ColumnStatistic::unknown);
        } else {
            return ColumnStatistic.unknown();
        }
    }

    private List<ColumnStatistic> getDefaultColumnStatisticList(List<String> columns) {
        List<ColumnStatistic> columnStatisticList = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            columnStatisticList.add(ColumnStatistic.unknown());
        }
        return columnStatisticList;
    }

    // ColumnStatistic List sequence is guaranteed to be consistent with Columns
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
        Preconditions.checkState(table != null);

        // get Statistics Table column info, just return default column statistics
        if (StatisticUtils.statisticTableBlackListCheck(table.getId())) {
            return getDefaultColumnStatisticList(columns);
        }

        Table statisticsTable = StatisticUtils.getStatisticsTable();
        if (statisticsTable == null) {
            return getDefaultColumnStatisticList(columns);
        }

        List<ColumnIdentifier> columnIdentifiers = new ArrayList<>();
        long tableId = table.getId();
        for (String column : columns) {
            columnIdentifiers.add(new ColumnIdentifier(tableId, column));
        }

        CompletableFuture<Map<ColumnIdentifier, Optional<ColumnStatistic>>> result = cachedStatistics.getAll(
                columnIdentifiers);
        if (result.isDone()) {
            List<ColumnStatistic> columnStatistics = new ArrayList<>();
            Map<ColumnIdentifier, Optional<ColumnStatistic>> realResult;
            try {
                realResult = result.get();
            } catch (Exception e) {
                LOG.warn(e);
                return getDefaultColumnStatisticList(columns);
            }
            for (String column : columns) {
                Optional<ColumnStatistic> columnStatistic =
                        realResult.getOrDefault(new ColumnIdentifier(tableId, column), Optional.empty());
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
    }

    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
        this.cachedStatistics.synchronous().put(new ColumnIdentifier(table.getId(), column), Optional.of(columnStatistic));
    }

}
