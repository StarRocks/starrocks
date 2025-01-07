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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.thrift.TGlobalDict;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.starrocks.statistic.StatisticExecutor.queryDictSync;

public class CacheRelaxDictManager implements IRelaxDictManager, MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(CacheRelaxDictManager.class);
    private static final Set<ConnectorTableColumnKey> NO_DICT_STRING_COLUMNS = Sets.newConcurrentHashSet();
    private static final Set<String> FORBIDDEN_DICT_TABLE_UUIDS = Sets.newConcurrentHashSet();
    public static final Integer LOW_CARDINALITY_THRESHOLD = 255;
    private static final long DICT_EXPIRATION_SECONDS = 7 * 24 * 3600;
    private static final long DICT_REFRESH_INTERVAL_SECONDS = 7 * 24 * 3600;
    public static final long PASSIVE_VERSION_THRESHOLD = 3;
    private static final CacheRelaxDictManager INSTANCE = new CacheRelaxDictManager();


    private final AsyncCacheLoader<ConnectorTableColumnKey, Optional<ColumnDict>> dictLoader =
            new AsyncCacheLoader<ConnectorTableColumnKey, Optional<ColumnDict>>() {

                @Override
                public CompletableFuture<Optional<ColumnDict>> asyncReload(
                        @NonNull ConnectorTableColumnKey key, @NonNull Optional<ColumnDict> oldValue,
                        @NonNull Executor executor) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            if (NO_DICT_STRING_COLUMNS.contains(key)) {
                                return Optional.empty();
                            }
                            Pair<List<TStatisticData>, Status> result = queryDictSync(key.tableUUID, key.column);
                            if (!result.second.ok()) {
                                LOG.warn("{}-{} error while collecting global dict", key.tableUUID,
                                        key.column);
                                return Optional.empty();
                            } else {
                                // check TStatisticData is not empty
                                if (!result.first.isEmpty()) {
                                    Optional<ColumnDict> newDict = deserializeColumnDict(result.first.get(0));
                                    if (newDict.isEmpty()) {
                                        FORBIDDEN_DICT_TABLE_UUIDS.add(key.tableUUID);
                                        return Optional.empty();
                                    }
                                    // merge old value
                                    if (oldValue.isPresent()) {
                                        ColumnDict oldDict = oldValue.get();
                                        int oldSize = oldDict.getDictSize();
                                        oldDict.merge(newDict.get());
                                        if (oldDict.getDictSize() > oldSize) {
                                            oldDict.updateVersion(0);
                                        }
                                        newDict = Optional.of(oldDict);
                                    }
                                    return checkDictSize(key, newDict);
                                } else {
                                    return oldValue;
                                }
                            }
                        } catch (RuntimeException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }, executor);
                }

                @Override
                @NonNull
                public CompletableFuture<Optional<ColumnDict>> asyncLoad(
                        @NonNull ConnectorTableColumnKey key,
                        @NonNull Executor executor) {
                    return asyncReload(key, Optional.empty(), executor);
                }
            };

    private final AsyncLoadingCache<ConnectorTableColumnKey, Optional<ColumnDict>> dictStatistics = Caffeine.newBuilder()
            .maximumSize(Config.statistic_dict_columns)
            .expireAfterAccess(DICT_EXPIRATION_SECONDS, TimeUnit.SECONDS)
            .refreshAfterWrite(DICT_REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS)
            .buildAsync(dictLoader);

    private CacheRelaxDictManager() {
    }

    protected static CacheRelaxDictManager getInstance() {
        return INSTANCE;
    }

    private Optional<ColumnDict> checkDictSize(ConnectorTableColumnKey key, Optional<ColumnDict> input) {
        if (input.isPresent()) {
            if (input.get().getDictSize() > LOW_CARDINALITY_THRESHOLD) {
                NO_DICT_STRING_COLUMNS.add(key);
                return Optional.empty();
            }
        }
        return input;
    }

    private Optional<ColumnDict> deserializeColumnDict(TStatisticData statisticData) {
        if (statisticData.dict == null) {
            return Optional.empty();
        }
        TGlobalDict tGlobalDict = statisticData.dict;
        ImmutableMap.Builder<ByteBuffer, Integer> dicts = ImmutableMap.builder();
        int dictSize = tGlobalDict.getIdsSize();
        for (int i = 0; i < dictSize; ++i) {
            dicts.put(tGlobalDict.strings.get(i), tGlobalDict.ids.get(i));
        }
        return Optional.of(new ColumnDict(dicts.build(), 0));
    }

    @Override
    public boolean hasGlobalDict(String tableUUID, String columnName) {
        if (FORBIDDEN_DICT_TABLE_UUIDS.contains(tableUUID)) {
            return false;
        }
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, columnName);
        if (NO_DICT_STRING_COLUMNS.contains(key)) {
            LOG.debug("{} isn't low cardinality string column", columnName);
            return false;
        }

        return true;
    }

    @Override
    public Optional<ColumnDict> getGlobalDict(String tableUUID, String columnName) {
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, columnName);
        CompletableFuture<Optional<ColumnDict>> columnFuture = dictStatistics.get(key);
        if (columnFuture.isDone()) {
            try {
                return columnFuture.get();
            } catch (Exception e) {
                LOG.warn(String.format("get dict cache for %s: %s failed", tableUUID, columnName), e);
            }
        }
        return Optional.empty();
    }

    @Override
    public void updateGlobalDict(String tableUUID, String columnName, Optional<TStatisticData> stat) {
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, columnName);
        if (NO_DICT_STRING_COLUMNS.contains(key)) {
            return;
        }
        if (stat.isEmpty()) {
            NO_DICT_STRING_COLUMNS.add(key);
            return;
        }

        Optional<ColumnDict> columnDict = deserializeColumnDict(stat.get());
        if (columnDict.isEmpty()) {
            return;
        }

        CompletableFuture<Optional<ColumnDict>> future = dictStatistics.getIfPresent(key);
        if (future != null && future.isDone()) {
            try {
                Optional<ColumnDict> columnOptional = future.get();
                if (columnOptional.isPresent()) {
                    columnOptional.get().merge(columnDict.get());
                    columnOptional.get().updateVersion(columnOptional.get().getVersion() + 1);
                    dictStatistics.put(key, CompletableFuture.completedFuture(checkDictSize(key, columnOptional)));
                }
            } catch (Exception e) {
                LOG.warn(String.format("update dict cache for %s: %s failed", tableUUID, columnName), e);
            }
        }
    }

    @Override
    public void removeGlobalDict(String tableUUID, String columnName) {
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, columnName);

        if (dictStatistics.getIfPresent(key) == null) {
            return;
        }

        LOG.info("remove dict for table:{} column:{}", tableUUID, columnName);
        dictStatistics.synchronous().invalidate(key);
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("ColumnDict", (long) dictStatistics.asMap().size());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        List<Object> samples = new ArrayList<>();
        dictStatistics.asMap().values().stream().findAny().ifPresent(future -> {
            if (future.isDone()) {
                try {
                    future.get().ifPresent(samples::add);
                } catch (Exception e) {
                    LOG.warn("get samples failed", e);
                }
            }
        });

        return Lists.newArrayList(Pair.create(samples, (long) dictStatistics.asMap().size()));
    }

}
