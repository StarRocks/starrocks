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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.SubfieldAccessPathNormalizer;
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

import static com.starrocks.statistic.StatisticExecutor.queryDictSync;

/**
 * CacheDictManager manages global dictionary caching for low cardinality string columns.
 * <p>
 * Global Dictionary Maintenance:
 * <p>
 * 1. Dictionary Collection:
 * - Dictionaries are collected from BE (Backend) nodes via queryDictSync()
 * - Only columns with cardinality <= LOW_CARDINALITY_THRESHOLD are considered
 * - Dictionary data size must be <= 1MB to ensure BE can generate dictionary pages after compaction
 * <p>
 * 2. Cache Management:
 * - Uses Caffeine AsyncLoadingCache with maximum size from Config.statistic_dict_columns
 * - Cache entries are keyed by ColumnIdentifier (tableId + columnName)
 * - Cache automatically loads dictionaries asynchronously when accessed
 * <p>
 * 3. Version Control:
 * - Each dictionary has a version timestamp for consistency checking
 * - Dictionaries are invalidated when versions become outdated
 * - Version mismatches trigger dictionary removal and re-collection
 * <p>
 * 4. Data Update Handling:
 * - When data is updated (INSERT/UPDATE/DELETE), the dictionary version becomes stale
 * - hasGlobalDict() checks version timestamps and invalidates outdated dictionaries
 * - updateGlobalDict() updates version timestamps when new data is collected
 * - Version mismatches between collectVersion and dictCollectVersion trigger removal
 * - During data updates, queries may fall back to non-dictionary optimization temporarily
 * - New dictionaries are automatically collected when cache is accessed after invalidation
 */
public class CacheDictManager implements IDictManager, MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(CacheDictManager.class);
    private static final Set<ColumnIdentifier> NO_DICT_STRING_COLUMNS = Sets.newConcurrentHashSet();
    private static final Set<Long> FORBIDDEN_DICT_TABLE_IDS = Sets.newConcurrentHashSet();

<<<<<<< HEAD
=======
    // Global dicts seeded from a query dump during ReplayFromDump. On replay there is no BE to load a dict
    // from, so hasGlobalDict/getGlobalDict below short-circuit to a seeded dict when present -- reproducing the
    // dict-encoding optimization the dump captured. Keyed by the REPLAYED (newly assigned) tableId + column.
    // Empty outside replay, so it is inert in production.
    @VisibleForTesting
    private static final Map<ColumnIdentifier, ColumnDict> REPLAY_DICTS = Maps.newConcurrentMap();

    @VisibleForTesting
    public static void replayPut(long tableId, ColumnId columnName, ColumnDict dict) {
        REPLAY_DICTS.put(new ColumnIdentifier(tableId, columnName), dict);
    }

    @VisibleForTesting
    public static void clearReplayDicts() {
        REPLAY_DICTS.clear();
    }
    // Thrash guard: per-column history of recent dictionary invalidation timestamps (millis). Used to
    // detect "rolling low-cardinality" columns whose dictionary keeps being invalidated and re-collected;
    // such columns are forbidden (added to NO_DICT_STRING_COLUMNS) so they stop wasting MetaScans.
    // Bounded so idle / rarely-thrashing columns cannot accumulate forever. maximumSize caps the entry
    // count and Caffeine LRU-evicts the least-recently-touched columns; a column that keeps thrashing is
    // touched on every invalidation and is therefore never evicted while it still matters.
    // Fixed-window invalidation counter per column -- far lighter than a Deque of boxed timestamps: one
    // small object (window start + count), O(1) per invalidation, no boxing, no queue scanning. A fixed
    // window (reset every window_sec) is enough to detect a persistently thrashing column, which
    // invalidates far more often than the threshold; the only imprecision is that a burst straddling a
    // window boundary may take one extra window to trip -- irrelevant for sustained thrash.
    private static final class InvalidationWindow {
        long windowStartMs;
        int count;
    }

    private static final Cache<ColumnIdentifier, InvalidationWindow> DICT_INVALIDATION_HISTORY =
            Caffeine.newBuilder()
                    .maximumSize(Config.statistic_dict_columns)
                    .build();

>>>>>>> 0d5f656 ([BugFix] Add global-dict thrash guard with persisted per-column forbid (#78486))
    public static final Integer LOW_CARDINALITY_THRESHOLD = Config.low_cardinality_threshold;

    public CacheDictManager() {
    }

    private static final CacheDictManager INSTANCE = new CacheDictManager();

    public static CacheDictManager getInstance() {
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
                            ColumnId columnName = columnIdentifier.getColumnName();
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
            .executor(ThreadPoolManager.getStatsCacheThread())
            .buildAsync(dictLoader);

    private Optional<ColumnDict> deserializeColumnDict(long tableId, ColumnId columnName, TStatisticData statisticData) {
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
                ByteBuffer buf = tGlobalDict.strings.get(i);
                dictDataSize += buf.limit() - buf.position();
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
    public boolean hasGlobalDict(long tableId, ColumnId columnName, long versionTime) {
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
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(id);
            if (db != null && GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId) != null) {
                columnIdentifier.setDbId(db.getId());
                break;
            }
        }

        if (columnIdentifier.getDbId() == -1) {
            LOG.debug("{} couldn't find db id", columnName);
            return false;
        }

        // Column-level persisted forbid (survives FE restart / leader failover): the dictionary thrash
        // guard records such columns as a table property, checked here so they stop being collected.
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(columnIdentifier.getDbId(), tableId);
        if (table instanceof OlapTable olapTable && olapTable.isNoDictColumn(columnName.getId())) {
            LOG.debug("table {} column {} is a persisted no-dict column", tableId, columnName);
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
            } else if (realResult.get().getVersion() < versionTime) {
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
    public boolean hasGlobalDict(long tableId, ColumnId columnName) {
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
    public void removeGlobalDict(OlapTable table, ColumnId columnName) {
        long tableId = table.getId();
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

        // A real present->invalidated transition just happened (the containsKey guard above skips the
        // no-op case where the dictionary is already absent). Record it and, if this column keeps
        // thrashing, forbid it so it stops being collected.
        recordInvalidationAndMaybeForbid(table, columnName, columnIdentifier);

        // Remove all subfields' dicts if the column is a JSON type
        try {
            List<String> parts = SubfieldAccessPathNormalizer.parseSimpleJsonPath(columnName.getId());
            if (parts.size() > 1) {
                String rootColumn = parts.get(0);
                columnIdentifier = new ColumnIdentifier(tableId, ColumnId.create(rootColumn));

                // Set dbId for the columnIdentifier to enable MetaUtils.getColumnByColumnId lookup
                long dbId = MetaUtils.lookupDbIdByTable(table);
                if (dbId != -1) {
                    columnIdentifier.setDbId(dbId);
                    Column column = MetaUtils.getColumnByColumnId(columnIdentifier);
                    if (column.getType().isJsonType()) {
                        removeGlobalDictForJson(tableId, column.getColumnId());
                    }
                } else {
                    LOG.debug("Could not find db id for table {}, skipping JSON subfield cleanup", tableId);
                }
            }
        } catch (Exception ignored) {
        }
    }

    // Track one dictionary invalidation for a column and, when a column is invalidated too frequently,
    // forbid collecting its dictionary. Frequent invalidation signals a column unsuited to the global
    // dictionary: a rolling low-cardinality column whose values keep turning over, and/or the
    // high-concurrency version race it triggers (each re-collection bumps the collected version, so
    // in-flight loads that carry the older version invalidate it again on commit). Both funnel through
    // removeGlobalDict and are counted here; either way the remedy is the same -- stop collecting it.
    // Forbidding only disables an optimization, so it is always correctness-safe. The in-memory entry
    // takes effect immediately; the forbid is also persisted (asynchronously) as a table property so it
    // survives FE restart / leader failover.
    private void recordInvalidationAndMaybeForbid(OlapTable table, ColumnId columnName,
                                                  ColumnIdentifier columnIdentifier) {
        if (recordInvalidationAndCheckThreshold(columnIdentifier)) {
            NO_DICT_STRING_COLUMNS.add(columnIdentifier);
            DICT_INVALIDATION_HISTORY.invalidate(columnIdentifier);
            LOG.info("forbid global dict for table:{} column:{} due to dictionary thrashing " +
                            "(>= {} invalidations within {}s); it will stop being collected",
                    columnIdentifier.getTableId(), columnName,
                    Config.dict_thrash_guard_threshold, Config.dict_thrash_guard_window_sec);
            persistNoDictColumnAsync(table, columnName);
        }
    }

    // Record one invalidation of this column and return true once it has been invalidated at least
    // dict_thrash_guard_threshold times within dict_thrash_guard_window_sec (the thrash condition).
    private boolean recordInvalidationAndCheckThreshold(ColumnIdentifier columnIdentifier) {
        if (!Config.enable_dict_thrash_guard || Config.dict_thrash_guard_threshold <= 0) {
            return false;
        }
        long now = System.currentTimeMillis();
        long windowMs = Math.max(1, Config.dict_thrash_guard_window_sec) * 1000L;
        InvalidationWindow w = DICT_INVALIDATION_HISTORY.get(columnIdentifier, k -> new InvalidationWindow());
        int count;
        synchronized (w) {
            if (w.count == 0 || now - w.windowStartMs > windowMs) {
                w.windowStartMs = now;
                w.count = 1;
            } else {
                w.count++;
            }
            count = w.count;
        }
        return count >= Config.dict_thrash_guard_threshold;
    }

    // Persist the column-level forbid so it survives restart. MUST run off the caller's thread:
    // removeGlobalDict is invoked from the transaction-apply path, which may hold db/txn locks, while
    // the persist takes a table WRITE lock and writes an edit log -- doing that inline risks a deadlock
    // or lock-order violation. Running it on a separate thread (with no other locks held) is safe, and
    // leader-only because only the leader writes edit logs (followers get it via replay).
    private void persistNoDictColumnAsync(OlapTable table, ColumnId columnName) {
        if (table == null || !GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        long dbId = MetaUtils.lookupDbIdByTable(table);
        if (dbId == -1) {
            return;
        }
        long tableId = table.getId();
        String col = columnName.getId();
        try {
            ThreadPoolManager.getStatsCacheThread().execute(() -> {
                try {
                    GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .disableGlobalDictForColumn(dbId, tableId, col);
                } catch (Exception e) {
                    LOG.warn("failed to persist no-dict column table:{} column:{}", tableId, col, e);
                }
            });
        } catch (Exception e) {
            LOG.warn("failed to submit no-dict persist task table:{} column:{}", tableId, col, e);
        }
    }

    @Override
    public void disableGlobalDict(long tableId) {
        LOG.debug("disable dict optimize for table {}", tableId);
        FORBIDDEN_DICT_TABLE_IDS.add(tableId);
    }

    @Override
    public void enableGlobalDict(long tableId) {
        FORBIDDEN_DICT_TABLE_IDS.remove(tableId);
        // Give this table a clean slate: drop any of its columns that the thrash guard (or the
        // cardinality check) previously forbade, so collection can be retried after an explicit enable.
        NO_DICT_STRING_COLUMNS.removeIf(columnIdentifier -> columnIdentifier.getTableId() == tableId);
        DICT_INVALIDATION_HISTORY.asMap().keySet().removeIf(columnIdentifier -> columnIdentifier.getTableId() == tableId);
    }

    // Clear the in-memory forbid state for specific columns. Called from ALTER TABLE ... ENABLE DICTIONARY
    // (and its edit-log replay) so an explicit ENABLE takes effect on every FE immediately, without waiting
    // for a restart. hasGlobalDict() checks NO_DICT_STRING_COLUMNS before the persisted noDictColumns, so
    // clearing the persisted property alone is not enough. Only the named columns are cleared, so a
    // cardinality-based forbid on some other column is untouched; if one of these columns was actually
    // high-cardinality it is simply re-derived on the next collection (correctness-safe).
    @Override
    public void clearForbiddenColumns(long tableId, Set<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            return;
        }
        NO_DICT_STRING_COLUMNS.removeIf(ci -> ci.getTableId() == tableId
                && columnNames.contains(ci.getColumnName().getId()));
        DICT_INVALIDATION_HISTORY.asMap().keySet().removeIf(ci -> ci.getTableId() == tableId
                && columnNames.contains(ci.getColumnName().getId()));
    }

    @Override
    public boolean isColumnForbidden(long tableId, String columnName) {
        return NO_DICT_STRING_COLUMNS.stream()
                .anyMatch(ci -> ci.getTableId() == tableId && ci.getColumnName().getId().equals(columnName));
    }

    @Override
    public void updateGlobalDict(OlapTable table, ColumnId columnName, long collectVersion, long versionTime) {
        // skip dictionary operator in checkpoint thread
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }

        long tableId = table.getId();
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
                    long lastVersion = columnDict.getVersion();
                    long dictCollectVersion = columnDict.getCollectedVersion();
                    if (collectVersion != dictCollectVersion) {
                        LOG.info("remove dict by unmatched version {}:{}", collectVersion, dictCollectVersion);
                        removeGlobalDict(table, columnName);
                        return;
                    }
                    columnDict.updateVersion(versionTime);
                    LOG.info("update dict for table {} column {} from version {} to {}", tableId, columnName,
                            lastVersion, versionTime);
                }
            } catch (Exception e) {
                LOG.warn(String.format("update dict cache for %d: %s failed", tableId, columnName), e);
            }
        }
    }

    @Override
    public Optional<ColumnDict> getGlobalDict(long tableId, ColumnId columnName) {
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

    public Optional<ColumnDict> getGlobalDictSync(OlapTable table, ColumnId columnName) {
        long tableId = table.getId();
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(tableId, columnName);

        // NOTE: it's used to patch the dbId, because asyncLoad requires the dbId
        long dbId = MetaUtils.lookupDbIdByTable(table);
        if (dbId == -1) {
            throw new RuntimeException("table not found " + tableId);
        }
        columnIdentifier.setDbId(dbId);

        CompletableFuture<Optional<ColumnDict>> columnFuture = dictStatistics.get(columnIdentifier);
        try {
            return columnFuture.get();
        } catch (Exception e) {
            LOG.warn(String.format("get dict cache for %d: %s failed", tableId, columnName), e);
        }
        return Optional.empty();
    }

    // TODO(murphy) support an efficient dict cache for JSON subfields
    @Override
    public boolean hasGlobalDictForJson(long tableId, ColumnId columnName) {
        // TODO(murphy) optimizer performance
        return dictStatistics.asMap().keySet().stream().anyMatch(column -> {
            if (column.getTableId() != tableId) {
                return false;
            }
            List<String> parts = SubfieldAccessPathNormalizer.parseSimpleJsonPath(column.getColumnName().getId());
            if (parts.size() > 1 && parts.get(0).equalsIgnoreCase(columnName.getId())) {
                return true;
            }
            return false;
        });
    }

    // TODO(murphy) support an efficient dict cache for JSON subfields
    @Override
    public List<ColumnDict> getGlobalDictForJson(long tableId, ColumnId columnName) {
        // TODO(murphy) optimizer performance
        return dictStatistics.synchronous().asMap().entrySet().stream().filter(kv -> {
            ColumnIdentifier column = kv.getKey();
            if (column.getTableId() != tableId) {
                return false;
            }
            List<String> parts = SubfieldAccessPathNormalizer.parseSimpleJsonPath(column.getColumnName().getId());
            if (parts.size() > 1 && parts.get(0).equalsIgnoreCase(columnName.getId())) {
                return true;
            }
            return false;
        }).flatMap(x -> x.getValue().stream()).toList();
    }

    // TODO(murphy) support an efficient dict cache for JSON subfields
    @Override
    public void removeGlobalDictForJson(long tableId, ColumnId columnName) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        // TODO(murphy) optimizer performance
        // Remove all global dict entries for JSON subfields of the given column in the given table.
        // This is used to invalidate all dicts for subfields of a JSON column (e.g., c2.f1, c2.f2, etc.)
        List<ColumnIdentifier> toRemove = new ArrayList<>();
        for (ColumnIdentifier column : dictStatistics.asMap().keySet()) {
            if (column.getTableId() != tableId) {
                continue;
            }
            List<String> parts = SubfieldAccessPathNormalizer.parseSimpleJsonPath(column.getColumnName().getId());
            if (parts.size() > 1 && parts.get(0).equalsIgnoreCase(columnName.getId())) {
                toRemove.add(column);
            }
        }
        for (ColumnIdentifier column : toRemove) {
            dictStatistics.synchronous().invalidate(column);
        }
    }


    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("ColumnDict", (long) dictStatistics.asMap().size());
    }

    @Override
    public long estimateSize() {
        return Estimator.estimate(dictStatistics.asMap(), 20);
    }
}
