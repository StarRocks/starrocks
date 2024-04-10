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
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.thrift.TGlobalDict;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static com.starrocks.statistic.StatisticExecutor.queryDictSync;

public class CacheDictManager implements IDictManager, MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(CacheDictManager.class);
    private static final Set<ColumnIdentifier> NO_DICT_STRING_COLUMNS = Sets.newConcurrentHashSet();
    private static final Set<Long> FORBIDDEN_DICT_TABLE_IDS = Sets.newConcurrentHashSet();

    public static final Integer LOW_CARDINALITY_THRESHOLD = 255;

    private CacheDictManager() {
    }

    private static final CacheDictManager INSTANCE = new CacheDictManager();

    protected static CacheDictManager getInstance() {
        return INSTANCE;
    }

    private final AsyncCacheLoader<ColumnIdentifier, Optional<ColumnDict>> dictLoader =
            new AsyncCacheLoader<ColumnIdentifier, Optional<ColumnDict>>() {
                @Override
                public @NonNull
                CompletableFuture<Optional<ColumnDict>> asyncLoad(
                        @NonNull ColumnIdentifier columnIdentifier,
                        @NonNull Executor executor) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            long tableId = columnIdentifier.getTableId();
                            String columnName = columnIdentifier.getColumnName();
                            Pair<List<TStatisticData>, Status> result = queryDictSync(columnIdentifier.getDbId(),
                                    tableId, columnName);
                            if (result.second.isGlobalDictError()) {
                                LOG.debug("{}-{} isn't low cardinality string column", tableId, columnName);
                                NO_DICT_STRING_COLUMNS.add(columnIdentifier);
                                return Optional.empty();
                            } else {
                                // check TStatisticData is not empty, There may be no such column Statistics in BE
                                if (!result.first.isEmpty()) {
                                    return deserializeColumnDict(tableId, columnName, result.first.get(0));
                                } else {
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
                public CompletableFuture<Optional<ColumnDict>> asyncReload(
                        @NonNull ColumnIdentifier key, @NonNull Optional<ColumnDict> oldValue,
                        @NonNull Executor executor) {
                    return asyncLoad(key, executor);
                }
            };

    private final AsyncLoadingCache<ColumnIdentifier, Optional<ColumnDict>> dictStatistics = Caffeine.newBuilder()
            .maximumSize(Config.statistic_dict_columns)
            .buildAsync(dictLoader);

    private Optional<ColumnDict> deserializeColumnDict(long tableId, String columnName, TStatisticData statisticData) {
        if (statisticData.dict == null) {
            throw new RuntimeException("Collect dict error in BE");
        }
        TGlobalDict tGlobalDict = statisticData.dict;
        ImmutableMap.Builder<ByteBuffer, Integer> dicts = ImmutableMap.builder();
        if (!tGlobalDict.isSetIds()) {
            return Optional.empty();
        }
        int dictSize = tGlobalDict.getIdsSize();
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);
        if (dictSize > LOW_CARDINALITY_THRESHOLD) {
            NO_DICT_STRING_COLUMNS.add(columnIdentifier);
            return Optional.empty();
        } else {
            int dictDataSize = 0;
            for (int i = 0; i < dictSize; i++) {
                // a UTF-8 code may take up to 3 bytes
                dictDataSize += tGlobalDict.strings.get(i).limit();
                // string offsets
                dictDataSize += 4;
            }
            // 1M
            final int DICT_PAGE_MAX_SIZE = 1024 * 1024;
            // If the dictionary data size exceeds 1M,
            // we won't use the global dictionary optimization.
            // In this case BE cannot guarantee that the dictionary page
            // will be generated after the compaction.
            // Additional 32 bytes reserved for security.
            if (dictDataSize > DICT_PAGE_MAX_SIZE - 32) {
                NO_DICT_STRING_COLUMNS.add(columnIdentifier);
                return Optional.empty();
            }
        }
        for (int i = 0; i < dictSize; ++i) {
            dicts.put(tGlobalDict.strings.get(i), tGlobalDict.ids.get(i));
        }
        LOG.info("collected dictionary table:{} column:{}, version:{} size:{}",
                tableId, columnName, statisticData.meta_version, dictSize);
        return Optional.of(new ColumnDict(dicts.build(), statisticData.meta_version));
    }

    @Override
    public boolean hasGlobalDict(long tableId, String columnName, long versionTime) {
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);
        if (NO_DICT_STRING_COLUMNS.contains(columnIdentifier)) {
            LOG.debug("{}-{} isn't low cardinality string column", tableId, columnName);
            return false;
        }

        if (FORBIDDEN_DICT_TABLE_IDS.contains(tableId)) {
            LOG.debug("table {} forbid low cardinality global dict", tableId);
            return false;
        }

        Set<Long> dbIds = ConnectContext.get().getCurrentSqlDbIds();
        for (Long id : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(id);
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
                LOG.warn(String.format("get dict cache for %d: %s failed", tableId, columnName), e);
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
        if (NO_DICT_STRING_COLUMNS.contains(columnIdentifier)) {
            LOG.debug("{} isn't low cardinality string column", columnName);
            return false;
        }

        if (FORBIDDEN_DICT_TABLE_IDS.contains(tableId)) {
            LOG.debug("table {} forbid low cardinality global dict", tableId);
            return false;
        }

        return dictStatistics.asMap().containsKey(columnIdentifier);
    }

    @Override
    public void removeGlobalDict(long tableId, String columnName) {
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);

        // skip dictionary operator in checkpoint thread
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }

        if (!dictStatistics.asMap().containsKey(columnIdentifier)) {
            return;
        }

        LOG.info("remove dict for table:{} column:{}", tableId, columnName);
        dictStatistics.synchronous().invalidate(columnIdentifier);
    }

    @Override
    public void disableGlobalDict(long tableId) {
        LOG.debug("disable dict optimize for table {}", tableId);
        FORBIDDEN_DICT_TABLE_IDS.add(tableId);
    }

    @Override
    public void enableGlobalDict(long tableId) {
        FORBIDDEN_DICT_TABLE_IDS.remove(tableId);
    }

    @Override
    public void updateGlobalDict(long tableId, String columnName, long collectVersion, long versionTime) {
        // skip dictionary operator in checkpoint thread
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }

        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);
        if (!dictStatistics.asMap().containsKey(columnIdentifier)) {
            return;
        }

        CompletableFuture<Optional<ColumnDict>> future = dictStatistics.getIfPresent(columnIdentifier);

        if (future != null && future.isDone()) {
            try {
                Optional<ColumnDict> columnOptional = future.get();
                if (columnOptional.isPresent()) {
                    ColumnDict columnDict = columnOptional.get();
                    long lastVersion = columnDict.getVersionTime();
                    long dictCollectVersion = columnDict.getCollectedVersionTime();
                    if (collectVersion != dictCollectVersion) {
                        LOG.info("remove dict by unmatched version {}:{}", collectVersion, dictCollectVersion);
                        removeGlobalDict(tableId, columnName);
                        return;
                    }
                    columnDict.updateVersionTime(versionTime);
                    LOG.info("update dict for table {} column {} from version {} to {}", tableId, columnName,
                            lastVersion, versionTime);
                }
            } catch (Exception e) {
                LOG.warn(String.format("update dict cache for %d: %s failed", tableId, columnName), e);
            }
        }
    }

    @Override
    public Optional<ColumnDict> getGlobalDict(long tableId, String columnName) {
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);
        CompletableFuture<Optional<ColumnDict>> columnFuture = dictStatistics.get(columnIdentifier);
        if (columnFuture.isDone()) {
            try {
                return columnFuture.get();
            } catch (Exception e) {
                LOG.warn(String.format("get dict cache for %d: %s failed", tableId, columnName), e);
            }
        }
        return Optional.empty();
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("ColumnDict", (long) dictStatistics.asMap().size());
    }
}
