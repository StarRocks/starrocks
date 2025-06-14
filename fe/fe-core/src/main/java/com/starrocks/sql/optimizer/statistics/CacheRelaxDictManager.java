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
import com.starrocks.common.ThreadPoolManager;
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
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.starrocks.statistic.StatisticExecutor.queryDictSync;

public class CacheRelaxDictManager implements IRelaxDictManager, MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(CacheRelaxDictManager.class);
    private static final Set<ConnectorTableColumnKey> NO_DICT_STRING_COLUMNS = Sets.newConcurrentHashSet();
    private static final Set<ConnectorTableColumnKey> TEMP_NOT_MATCHED_KEY = Sets.newConcurrentHashSet();
    private static final Set<String> FORBIDDEN_DICT_TABLE_UUIDS = Sets.newConcurrentHashSet();
    private static final long DICT_EXPIRATION_SECONDS = 7 * 24 * 3600;
    private static final long DICT_REFRESH_INTERVAL_SECONDS = 7 * 24 * 3600;
    public static final long HISTORICAL_VERSION_THRESHOLD = 30;
    public static final long PERIOD_VERSION_THRESHOLD = 5;
    private static final CacheRelaxDictManager INSTANCE = new CacheRelaxDictManager();

    private static Optional<ColumnDict> checkDictSize(ConnectorTableColumnKey key, Optional<ColumnDict> input) {
        if (input.isPresent() && input.get().getDictSize() > CacheDictManager.LOW_CARDINALITY_THRESHOLD) {
            NO_DICT_STRING_COLUMNS.add(key);
            return Optional.empty();
        }

        return input;
    }

    private static Optional<ColumnDict> mergeStatToColumnDict(TGlobalDict statisticData,
                                                              Optional<ColumnDict> oldValue,
                                                              boolean mergeVersion) {
        ImmutableMap.Builder<ByteBuffer, Integer> dict = ImmutableMap.builder();
        if (oldValue.isEmpty()) {
            int dictSize = statisticData.getIdsSize();
            for (int i = 0; i < dictSize; ++i) {
                dict.put(statisticData.strings.get(i), statisticData.ids.get(i));
            }
            return Optional.of(new ColumnDict(dict.build(), 0, 0));
        } else {
            TreeSet<ByteBuffer> orderSet = new TreeSet<>(oldValue.get().getDict().keySet());
            int dictSize = statisticData.getIdsSize();
            for (int i = 0; i < dictSize; ++i) {
                orderSet.add(statisticData.strings.get(i));
            }

            int index = 1;
            for (ByteBuffer v : orderSet) {
                dict.put(v, index);
                index++;
            }

            long oldCollectedVersion = oldValue.get().getCollectedVersion();
            long oldVersion = oldValue.get().getVersion();
            if (mergeVersion) {
                return Optional.of(new ColumnDict(dict.build(), oldCollectedVersion + oldVersion, 0));
            } else {
                long version = orderSet.size() > oldValue.get().getDictSize() ? oldVersion + 1 : oldVersion;
                return Optional.of(new ColumnDict(dict.build(), oldCollectedVersion, version));
            }
        }
    }

    public static class DictLoader implements AsyncCacheLoader<ConnectorTableColumnKey, Optional<ColumnDict>> {
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
                        return oldValue;
                    } else {
                        // check TStatisticData is not empty
                        if (!result.first.isEmpty() && result.first.get(0).dict != null) {
                            Optional<ColumnDict> newDict =
                                    mergeStatToColumnDict(result.first.get(0).dict, oldValue, true);
                            if (newDict.isEmpty()) {
                                return oldValue;
                            }
                            if (newDict.get().getCollectedVersion() > HISTORICAL_VERSION_THRESHOLD) {
                                NO_DICT_STRING_COLUMNS.add(key);
                                return Optional.empty();
                            }

                            return checkDictSize(key, newDict);
                        } else {
                            FORBIDDEN_DICT_TABLE_UUIDS.add(key.tableUUID);
                            return Optional.empty();
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
    }

    private final AsyncLoadingCache<ConnectorTableColumnKey, Optional<ColumnDict>> dictStatistics = Caffeine.newBuilder()
            .maximumSize(Config.statistic_dict_columns)
            .expireAfterAccess(DICT_EXPIRATION_SECONDS, TimeUnit.SECONDS)
            .refreshAfterWrite(DICT_REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS)
            .executor(ThreadPoolManager.getDictCacheThreadPoolForLake())
            .buildAsync(new DictLoader());

    private CacheRelaxDictManager() {
    }

    protected static CacheRelaxDictManager getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean hasGlobalDict(String tableUUID, String columnName) {
        if (FORBIDDEN_DICT_TABLE_UUIDS.contains(tableUUID)) {
            return false;
        }
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, columnName);
        if (NO_DICT_STRING_COLUMNS.contains(key) || TEMP_NOT_MATCHED_KEY.contains(key)) {
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
        if (NO_DICT_STRING_COLUMNS.contains(key) || stat.isEmpty() || stat.get().dict == null) {
            return;
        }

        CompletableFuture<Optional<ColumnDict>> future = dictStatistics.getIfPresent(key);
        if (future != null && future.isDone()) {
            try {
                Optional<ColumnDict> columnOptional = future.get();
                if (columnOptional.isPresent()) {
                    Optional<ColumnDict> columnDict = mergeStatToColumnDict(stat.get().dict, columnOptional, false);
                    dictStatistics.put(key, CompletableFuture.completedFuture(checkDictSize(key, columnDict)));
                }
            } catch (Exception e) {
                LOG.warn(String.format("update dict cache for %s: %s failed", tableUUID, columnName), e);
            }
        }
    }

    @Override
    public void removeGlobalDict(String tableUUID, String columnName) {
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, columnName);
        FORBIDDEN_DICT_TABLE_UUIDS.remove(tableUUID);
        NO_DICT_STRING_COLUMNS.remove(key);
        TEMP_NOT_MATCHED_KEY.remove(key);

        if (dictStatistics.getIfPresent(key) == null) {
            return;
        }

        LOG.info("remove dict for table:{} column:{}", tableUUID, columnName);
        dictStatistics.synchronous().invalidate(key);
    }

    @Override
    public void invalidTemporarily(String tableUUID, String columnName) {
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, columnName);
        TEMP_NOT_MATCHED_KEY.add(key);
    }

    @Override
    public void removeTemporaryInvalid(String tableUUID, String columnName) {
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, columnName);
        TEMP_NOT_MATCHED_KEY.remove(key);
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("ExternalTableColumnDict", (long) dictStatistics.asMap().size());
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