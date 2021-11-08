// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.thrift.TGlobalDict;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static com.starrocks.statistic.StatisticExecutor.queryDictSync;

public class CacheDictManager implements IDictManager {
    private static final Logger LOG = LogManager.getLogger(CacheDictManager.class);
    private static final Set<ColumnIdentifier> noDictStringColumns = Sets.newHashSet();
    private static final Set<Long> forbiddenDictTableIds = Sets.newHashSet();

    public static final Integer LOW_CARDINALITY_THRESHOLD = 255;

    private CacheDictManager() {
    }

    private static final CacheDictManager instance = new CacheDictManager();

    protected static CacheDictManager getInstance() {
        return instance;
    }

    private final AsyncCacheLoader<ColumnIdentifier, Optional<ColumnDict>> dictLoader =
            new AsyncCacheLoader<ColumnIdentifier, Optional<ColumnDict>>() {
                @Override
                public @NonNull CompletableFuture<Optional<ColumnDict>> asyncLoad(
                        @NonNull ColumnIdentifier columnIdentifier,
                        @NonNull Executor executor) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            List<TStatisticData> statisticData = queryDictSync(columnIdentifier.getDbId(),
                                    columnIdentifier.getTableId(),
                                    columnIdentifier.getColumnName());
                            // check TStatisticData is not empty, There may be no such column Statistics in BE
                            if (!statisticData.isEmpty()) {
                                return Optional.of(deserializeColumnDict(statisticData.get(0)));
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
                public CompletableFuture<Map<@NonNull ColumnIdentifier, @NonNull Optional<ColumnDict>>> asyncLoadAll(
                        @NonNull Iterable<? extends @NonNull ColumnIdentifier> keys, @NonNull Executor executor) {
                    return CompletableFuture.supplyAsync(() -> {
                        Map<ColumnIdentifier, Optional<ColumnDict>> result = new HashMap<>();
                        try {
                            long tableId = -1;
                            long dbId = -1;
                            List<String> columns = new ArrayList<>();
                            for (ColumnIdentifier key : keys) {
                                dbId = key.getDbId();
                                tableId = key.getTableId();
                                columns.add(key.getColumnName());
                            }
                            List<TStatisticData> statisticData = queryDictSync(dbId, tableId, columns);
                            // check TStatisticData is not empty, There may be no such column Statistics in BE
                            if (!statisticData.isEmpty()) {
                                for (TStatisticData data : statisticData) {
                                    ColumnDict columnDict = deserializeColumnDict(data);
                                    result.put(new ColumnIdentifier(data.tableId, data.columnName),
                                            Optional.of(columnDict));
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
                public CompletableFuture<Optional<ColumnDict>> asyncReload(
                        @NonNull ColumnIdentifier key, @NonNull Optional<ColumnDict> oldValue,
                        @NonNull Executor executor) {
                    return asyncLoad(key, executor);
                }
            };

    private final AsyncLoadingCache<ColumnIdentifier, Optional<ColumnDict>> dictStatistics = Caffeine.newBuilder()
            .maximumSize(Config.statistic_cache_columns)
            .buildAsync(dictLoader);

    private ColumnDict deserializeColumnDict(TStatisticData statisticData) throws AnalysisException {
        TGlobalDict tGlobalDict = statisticData.dict;
        ImmutableMap.Builder<String, Integer> dicts = ImmutableMap.builder();
        if (tGlobalDict.isSetIds()) {
            int dictSize = tGlobalDict.getIdsSize();
            for (int i = 0; i < dictSize; ++i) {
                dicts.put(tGlobalDict.strings.get(i), tGlobalDict.ids.get(i));
            }
        }
        return new ColumnDict(dicts.build(), statisticData.meta_version);
    }

    @Override
    public boolean hasGlobalDict(long tableId, String columnName, long versionTime) {
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);
        if (noDictStringColumns.contains(columnIdentifier)) {
            LOG.debug("{} isn't low cardinality string column", columnName);
            return false;
        }

        if (forbiddenDictTableIds.contains(tableId)) {
            LOG.debug("table {} forbit low cardinality global dict", tableId);
            return false;
        }

        Set<Long> dbIds = ConnectContext.get().getCurrentSqlDbIds();
        for (Long id : dbIds) {
            Database db = Catalog.getCurrentCatalog().getDb(id);
            if (db != null && db.getTable(tableId) != null) {
                columnIdentifier.setDbId(db.getId());
                break;
            }
        }

        if (columnIdentifier.getDbId() == -1) {
            LOG.debug("{} couldn't find db id", columnName);
            return false;
        }

        CompletableFuture<Optional<ColumnDict>> result = dictStatistics.get(columnIdentifier);
        if (result.isDone()) {
            Optional<ColumnDict> realResult;
            try {
                realResult = result.get();
            } catch (Exception e) {
                LOG.warn(e);
                return false;
            }
            if (!realResult.isPresent()) {
                LOG.debug("Invalidate column {} dict cache because don't present", columnName);
                dictStatistics.synchronous().invalidate(columnIdentifier);
            } else if (realResult.get().getVersionTime() < versionTime) {
                LOG.debug("Invalidate column {} dict cache because out of date", columnName);
                dictStatistics.synchronous().invalidate(columnIdentifier);
            } else {
                return true;
            }
        }
        LOG.debug("{} first get column dict", columnName);
        return false;
    }

    @Override
    public boolean hasGlobalDict(long tableId, String columnName) {
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);
        return dictStatistics.asMap().containsKey(columnIdentifier);
    }

    @Override
    public void removeGlobalDict(long tableId, String columnName) {
        LOG.debug("remove dict for column {}", columnName);
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);
        dictStatistics.synchronous().invalidate(columnIdentifier);
    }

    @Override
    public void forbitGlobalDict(long tableId) {
        LOG.debug("remove dict for table {}", tableId);
        forbiddenDictTableIds.add(tableId);
    }

    @Override
    public void updateGlobalDict(long tableId, String columnName, long versionTime) {
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);
        if (!dictStatistics.synchronous().asMap().containsKey(columnIdentifier)) {
            return;
        }

        Optional<ColumnDict> columnDictOptional = dictStatistics.synchronous().get(columnIdentifier);
        Preconditions.checkState(columnDictOptional != null && columnDictOptional.isPresent());
        ColumnDict columnDict = columnDictOptional.get();
        ColumnDict newColumnDict = new ColumnDict(columnDict.getDict(), versionTime);
        dictStatistics.synchronous().put(columnIdentifier, Optional.of(newColumnDict));
        LOG.debug("update dict for column {}, version {}", columnName, versionTime);
    }

    @Override
    public ColumnDict getGlobalDict(long tableId, String columnName) {
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);
        CompletableFuture<Optional<ColumnDict>> result = dictStatistics.get(columnIdentifier);
        Preconditions.checkArgument(result.isDone());
        try {
            Optional<ColumnDict> dict = result.get();
            Preconditions.checkArgument(dict.isPresent());
            return dict.get();
        } catch (Exception e) {
            LOG.warn(e);
            Preconditions.checkArgument(false, "Shouldn't run here");
        }
        Preconditions.checkArgument(false, "Shouldn't run here");
        return null;
    }
}
