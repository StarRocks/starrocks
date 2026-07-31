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

package com.starrocks.connector.starrocks;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.DatabaseTableName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Caches for the StarRocks catalog, all fronted by {@link StarRocksFeClient}.
 *
 * <p>Metadata caches (db names / table names / table info): plain TTL caches.
 *
 * <p>Statistics caches (time-driven async refresh only — queries never trigger
 * remote calls once warm):
 * <ul>
 * <li>Cache 1 — table-level stats snapshot per (db, table): partition vector +
 * row counts + table-level column stats. Caffeine {@code refreshAfterWrite}
 * (access-triggered) with an epoch-gated conditional fetch: the reload sends the
 * cached epochs and the remote re-ships only the fields whose epoch moved
 * ({@code list}/{@code data} gate the partition metas + row counts,
 * {@code analyze} gates the collected column stats).</li>
 * <li>Cache 2 — per-(table, partition) column stats: loaded lazily in batch for
 * the partitions a query actually selects; no timer of its own — when Cache 1's
 * reload observes a changed {@code analyzeEpoch} (or cannot compare, after its
 * entry was evicted) it re-pulls this table's cached entries in one batch call
 * and drops entries for removed partitions.</li>
 * </ul>
 */
public class StarRocksMetadataCache {
    private static final Logger LOG = LogManager.getLogger(StarRocksMetadataCache.class);
    private static final String ALL_DATABASES_CACHE_KEY = "__all_databases__";

    private final StarRocksFeClient feClient;
    private final Options options;
    private final SnapshotLoader snapshotLoader;
    private final PartitionStatsLoader partitionStatsLoader;
    private final LoadingCache<String, List<String>> dbNamesCache;
    private final LoadingCache<String, List<String>> tableNamesCache;
    private final LoadingCache<DatabaseTableName, Optional<StarRocksRemoteScanWire.Table>> tableCache;
    private final LoadingCache<DatabaseTableName, Optional<StarRocksRemoteTableStats.Snapshot>> statsSnapshotCache;
    private final Cache<PartitionStatsKey, PartitionStatsEntry> partitionStatsCache;

    public StarRocksMetadataCache(StarRocksFeClient feClient) {
        this(feClient, Options.defaults(), null, null, null);
    }

    public StarRocksMetadataCache(StarRocksFeClient feClient, Options options,
                                  SnapshotLoader snapshotLoader, PartitionStatsLoader partitionStatsLoader) {
        this(feClient, options, snapshotLoader, partitionStatsLoader, null);
    }

    public StarRocksMetadataCache(StarRocksFeClient feClient, Options options,
                                  SnapshotLoader snapshotLoader, PartitionStatsLoader partitionStatsLoader,
                                  Executor refreshExecutor) {
        this.feClient = feClient;
        this.options = options == null ? Options.defaults() : options;
        this.snapshotLoader = snapshotLoader != null ? snapshotLoader :
                (dbName, tableName, cachedEpochs) ->
                        feClient.fetchTableStatsSnapshot(dbName, tableName, cachedEpochs);
        this.partitionStatsLoader = partitionStatsLoader != null ? partitionStatsLoader :
                (dbName, tableName, partitionIds, columns) ->
                        feClient.fetchPartitionColumnStats(dbName, tableName, partitionIds, columns);
        // The name caches hold one entry per database — no size bound needed.
        this.dbNamesCache = newNamesCache(ignored -> loadDbNames());
        this.tableNamesCache = newNamesCache(this::loadTableNames);
        this.tableCache = Caffeine.newBuilder()
                .expireAfterWrite(this.options.ttlSec(), TimeUnit.SECONDS)
                .maximumSize(this.options.tableCacheMaxNum())
                .build(this::loadTable);
        // The async refresh (and the cascade it may trigger) does blocking HTTP; run it on the
        // connector-owned pool rather than ForkJoinPool.commonPool(), matching the other
        // connector caches (Hive/Iceberg/JDBC all pass an explicit executor).
        Caffeine<Object, Object> snapshotCacheBuilder = Caffeine.newBuilder()
                .refreshAfterWrite(this.options.refreshSec(), TimeUnit.SECONDS)
                .expireAfterWrite(this.options.ttlSec(), TimeUnit.SECONDS)
                .maximumSize(this.options.tableCacheMaxNum());
        if (refreshExecutor != null) {
            snapshotCacheBuilder.executor(refreshExecutor);
        }
        this.statsSnapshotCache = snapshotCacheBuilder
                .build(new CacheLoader<DatabaseTableName, Optional<StarRocksRemoteTableStats.Snapshot>>() {
                    @Override
                    public Optional<StarRocksRemoteTableStats.Snapshot> load(DatabaseTableName key) {
                        return Optional.ofNullable(loadSnapshot(key, null));
                    }

                    @Override
                    public Optional<StarRocksRemoteTableStats.Snapshot> reload(
                            DatabaseTableName key, Optional<StarRocksRemoteTableStats.Snapshot> oldValue) {
                        StarRocksRemoteTableStats.Snapshot fresh =
                                loadSnapshot(key, oldValue.orElse(null));
                        if (fresh == null) {
                            // Transient failure or endpoint unavailable: serve stale.
                            return oldValue;
                        }
                        return Optional.of(fresh);
                    }
                });
        this.partitionStatsCache = Caffeine.newBuilder()
                .expireAfterWrite(this.options.ttlSec(), TimeUnit.SECONDS)
                .maximumSize(this.options.partitionCacheMaxNum())
                .build();
    }

    public List<String> getDbNames() {
        return dbNamesCache.get(ALL_DATABASES_CACHE_KEY);
    }

    public List<String> getTableNames(String dbName) {
        return tableNamesCache.get(dbName);
    }

    public StarRocksRemoteScanWire.Table getTable(String dbName, String tableName) {
        return getNullableValue(tableCache, DatabaseTableName.of(dbName, tableName));
    }

    public void refreshTable(String dbName, String tableName) {
        DatabaseTableName cacheKey = DatabaseTableName.of(dbName, tableName);
        invalidateTable(dbName, tableName);
        getNullableValue(tableCache, cacheKey);
    }

    public void invalidateTable(String dbName, String tableName) {
        DatabaseTableName cacheKey = DatabaseTableName.of(dbName, tableName);
        tableCache.invalidate(cacheKey);
        tableNamesCache.invalidate(dbName);
        statsSnapshotCache.invalidate(cacheKey);
        invalidatePartitionStats(dbName, tableName, null);
    }

    public void invalidateAll() {
        dbNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        tableCache.invalidateAll();
        statsSnapshotCache.invalidateAll();
        partitionStatsCache.invalidateAll();
    }

    /**
     * Current table statistics snapshot, possibly stale (refreshAfterWrite
     * reloads in the background on access). Null when the remote does not
     * serve the statistics endpoints.
     */
    public StarRocksRemoteTableStats.Snapshot getTableStatsSnapshot(String dbName, String tableName) {
        Optional<StarRocksRemoteTableStats.Snapshot> snapshot =
                statsSnapshotCache.get(DatabaseTableName.of(dbName, tableName));
        return snapshot == null ? null : snapshot.orElse(null);
    }

    /**
     * Per-(partition, column) collected statistics for the given partitions,
     * batch-loading the missing entries in a single remote call. Partitions the
     * remote has no collected stats for are simply absent from the result.
     */
    public Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> getPartitionColumnStats(
            String dbName, String tableName, List<Long> partitionIds, List<String> columns) {
        Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> result = new HashMap<>();
        if (partitionIds == null || partitionIds.isEmpty() || columns == null || columns.isEmpty()) {
            return result;
        }
        // Column names are case-insensitive in the engine; the cache keys are lowercase
        // (see groupPartitionStats) so the hit check must compare lowercase too — the
        // remote may report a different case than the local schema objects use.
        Set<String> wantedColumns = new HashSet<>();
        for (String column : columns) {
            wantedColumns.add(column.toLowerCase(Locale.ROOT));
        }
        List<Long> missing = new ArrayList<>();
        for (Long partitionId : partitionIds) {
            PartitionStatsEntry entry =
                    partitionStatsCache.getIfPresent(new PartitionStatsKey(dbName, tableName, partitionId));
            if (entry != null && entry.columns().containsAll(wantedColumns)) {
                result.put(partitionId, entry.columnStats);
            } else {
                missing.add(partitionId);
            }
        }
        if (missing.isEmpty()) {
            return result;
        }
        StarRocksRemoteTableStats.PartitionStatsResponse response =
                partitionStatsLoader.load(dbName, tableName, missing, columns);
        if (response == null || response.partitionStats == null) {
            return result;
        }
        Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> loaded =
                groupPartitionStats(response.partitionStats);
        for (Map.Entry<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> entry : loaded.entrySet()) {
            PartitionStatsKey key = new PartitionStatsKey(dbName, tableName, entry.getKey());
            PartitionStatsEntry previous = partitionStatsCache.getIfPresent(key);
            Map<String, StarRocksRemoteTableStats.PartitionColumnStats> merged = new HashMap<>();
            if (previous != null) {
                merged.putAll(previous.columnStats);
            }
            merged.putAll(entry.getValue());
            partitionStatsCache.put(key, new PartitionStatsEntry(ImmutableMap.copyOf(merged)));
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    // Also used by the cache-disabled direct path in StarRocksMetadata. Keys are the
    // lowercase column names; the original remote-reported name stays in the value's
    // {@code column} field (the wire needs it back in the remote's own case).
    static Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> groupPartitionStats(
            List<StarRocksRemoteTableStats.PartitionColumnStats> stats) {
        Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> grouped = new HashMap<>();
        for (StarRocksRemoteTableStats.PartitionColumnStats stat : stats) {
            if (stat == null || stat.column == null) {
                continue;
            }
            grouped.computeIfAbsent(stat.partitionId, ignored -> new HashMap<>())
                    .put(stat.column.toLowerCase(Locale.ROOT), stat);
        }
        return grouped;
    }

    private StarRocksRemoteTableStats.Snapshot loadSnapshot(DatabaseTableName key,
                                                            StarRocksRemoteTableStats.Snapshot oldSnapshot) {
        StarRocksRemoteTableStats.Epochs cachedEpochs = oldSnapshot == null ? null : oldSnapshot.epochs;
        StarRocksRemoteTableStats.Snapshot fresh;
        try {
            fresh = snapshotLoader.load(key.getDatabaseName(), key.getTableName(), cachedEpochs);
        } catch (Exception e) {
            LOG.warn("failed to load stats snapshot for {}.{}", key.getDatabaseName(), key.getTableName(), e);
            return null;
        }
        if (fresh == null) {
            return null;
        }
        StarRocksRemoteTableStats.Snapshot merged = mergeSnapshot(oldSnapshot, fresh);
        // Cascade whenever we cannot prove the analyze epoch is unchanged. The
        // oldSnapshot == null case matters: after this snapshot entry was evicted
        // (TTL/size) partition-stats entries may survive on their own younger clocks,
        // and a full re-load is the only chance to bring them back in sync — on a
        // truly first touch the partition cache is empty and the cascade is a no-op.
        boolean analyzeMayHaveChanged = oldSnapshot == null
                || oldSnapshot.epochs == null || fresh.epochs == null
                || !Objects.equals(oldSnapshot.epochs.analyze, fresh.epochs.analyze);
        if (analyzeMayHaveChanged) {
            cascadePartitionStatsRefresh(key, merged);
        }
        return merged;
    }

    /**
     * Epoch-gated conditional merge: fields whose epoch did not move keep the
     * previous generation's data under the new epochs.
     *
     * <p>Which fields the response carries is derived by comparing the fresh epochs
     * against the previous snapshot's. The server re-ships each field exactly when
     * its gating epoch moved against the cached epochs the client sent — and those
     * are precisely {@code oldSnapshot.epochs} — so re-running the same comparison
     * here reproduces the server's shipping decision without extra wire flags. The
     * immutable partitioning shape (partitionType/partitionColumns) is not gated:
     * the server always ships it and the merge always takes it from the response.
     */
    // Package-private for direct matrix testing.
    static StarRocksRemoteTableStats.Snapshot mergeSnapshot(
            StarRocksRemoteTableStats.Snapshot oldSnapshot, StarRocksRemoteTableStats.Snapshot fresh) {
        if (oldSnapshot == null || oldSnapshot.epochs == null || fresh.epochs == null) {
            // Nothing usable to compare against: the server could not have seen our
            // cached epochs either, so it shipped everything.
            return fresh;
        }
        boolean listChanged = !Objects.equals(fresh.epochs.list, oldSnapshot.epochs.list);
        boolean dataChanged = !Objects.equals(fresh.epochs.data, oldSnapshot.epochs.data);
        boolean analyzeChanged = !Objects.equals(fresh.epochs.analyze, oldSnapshot.epochs.analyze);
        if ((listChanged || dataChanged) && analyzeChanged) {
            return fresh;
        }
        StarRocksRemoteTableStats.Snapshot merged = new StarRocksRemoteTableStats.Snapshot();
        merged.status = fresh.status;
        merged.epochs = fresh.epochs;
        merged.partitionType = fresh.partitionType;
        merged.partitionColumns = fresh.partitionColumns;
        // partitions embed per-partition row counts, so a moved partition set (list)
        // OR moved data versions (data) re-ship them together with tableRowCount.
        if (listChanged || dataChanged) {
            merged.tableRowCount = fresh.tableRowCount;
            merged.partitions = fresh.partitions;
        } else {
            merged.tableRowCount = oldSnapshot.tableRowCount;
            merged.partitions = oldSnapshot.partitions;
        }
        if (analyzeChanged) {
            merged.analyzeType = fresh.analyzeType;
            merged.columnStats = fresh.columnStats;
        } else {
            merged.analyzeType = oldSnapshot.analyzeType;
            merged.columnStats = oldSnapshot.columnStats;
        }
        return merged;
    }

    /**
     * Cache 1 observed a changed (or unprovable, after eviction) analyze epoch:
     * re-pull this table's cached Cache 2 entries in one batch call (runs on the
     * refresh thread, never on the query path) and drop entries for partitions
     * that no longer exist.
     */
    private void cascadePartitionStatsRefresh(DatabaseTableName key, StarRocksRemoteTableStats.Snapshot snapshot) {
        String dbName = key.getDatabaseName();
        String tableName = key.getTableName();
        Set<Long> livePartitionIds = new HashSet<>();
        if (snapshot.partitions != null) {
            snapshot.partitions.forEach(partition -> livePartitionIds.add(partition.id));
        }
        List<Long> cachedIds = new ArrayList<>();
        // Re-request columns under the remote's own case (kept in the value's column
        // field) — the cache keys are lowercase which the remote may not recognize.
        Set<String> cachedColumns = new LinkedHashSet<>();
        for (Map.Entry<PartitionStatsKey, PartitionStatsEntry> entry : partitionStatsCache.asMap().entrySet()) {
            PartitionStatsKey statsKey = entry.getKey();
            if (!statsKey.dbName.equals(dbName) || !statsKey.tableName.equals(tableName)) {
                continue;
            }
            if (!livePartitionIds.contains(statsKey.partitionId)) {
                partitionStatsCache.invalidate(statsKey);
                continue;
            }
            cachedIds.add(statsKey.partitionId);
            for (StarRocksRemoteTableStats.PartitionColumnStats stat : entry.getValue().columnStats.values()) {
                cachedColumns.add(stat.column);
            }
        }
        if (cachedIds.isEmpty()) {
            return;
        }
        try {
            StarRocksRemoteTableStats.PartitionStatsResponse response = partitionStatsLoader.load(
                    dbName, tableName, cachedIds, new ArrayList<>(cachedColumns));
            if (response == null || response.partitionStats == null) {
                return;
            }
            Map<Long, Map<String, StarRocksRemoteTableStats.PartitionColumnStats>> loaded =
                    groupPartitionStats(response.partitionStats);
            for (Long partitionId : cachedIds) {
                PartitionStatsKey statsKey = new PartitionStatsKey(dbName, tableName, partitionId);
                Map<String, StarRocksRemoteTableStats.PartitionColumnStats> stats = loaded.get(partitionId);
                if (stats == null) {
                    // The new ANALYZE generation no longer covers this partition.
                    partitionStatsCache.invalidate(statsKey);
                } else {
                    partitionStatsCache.put(statsKey, new PartitionStatsEntry(ImmutableMap.copyOf(stats)));
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to cascade partition stats refresh for {}.{}", dbName, tableName, e);
        }
    }

    private void invalidatePartitionStats(String dbName, String tableName, Set<Long> partitionIds) {
        for (PartitionStatsKey key : partitionStatsCache.asMap().keySet()) {
            if (key.dbName.equals(dbName) && key.tableName.equals(tableName)
                    && (partitionIds == null || partitionIds.contains(key.partitionId))) {
                partitionStatsCache.invalidate(key);
            }
        }
    }

    private List<String> loadDbNames() {
        return ImmutableList.copyOf(feClient.listDbNames());
    }

    private List<String> loadTableNames(String dbName) {
        return ImmutableList.copyOf(feClient.listTableNames(dbName));
    }

    private <K, V> LoadingCache<K, V> newNamesCache(java.util.function.Function<K, V> loader) {
        return Caffeine.newBuilder()
                .expireAfterWrite(options.ttlSec(), TimeUnit.SECONDS)
                .build(loader::apply);
    }

    private <T> T getNullableValue(LoadingCache<DatabaseTableName, Optional<T>> cache, DatabaseTableName cacheKey) {
        Optional<T> value = cache.get(cacheKey);
        if (value.isPresent()) {
            return value.get();
        }
        cache.invalidate(cacheKey);
        return null;
    }

    public static final class Options {
        // Hard expiry of every cache.
        private final long ttlSec;
        // Background refresh interval of the statistics snapshot cache (the epoch-gated
        // conditional fetch), which is the only cache with refreshAfterWrite.
        private final long refreshSec;
        // Entry limit of the per-table caches (table schema, stats snapshot). The
        // db/table NAME caches are deliberately unbounded: one entry per database.
        private final long tableCacheMaxNum;
        // Entry limit of the per-(table, partition) statistics cache.
        private final long partitionCacheMaxNum;

        public Options(long ttlSec, long refreshSec, long tableCacheMaxNum, long partitionCacheMaxNum) {
            this.ttlSec = Math.max(1L, ttlSec);
            this.refreshSec = Math.max(1L, refreshSec);
            this.tableCacheMaxNum = Math.max(1L, tableCacheMaxNum);
            this.partitionCacheMaxNum = Math.max(1L, partitionCacheMaxNum);
        }

        public static Options defaults() {
            return new Options(3600, 300, 10000, 100000);
        }

        public long ttlSec() {
            return ttlSec;
        }

        public long refreshSec() {
            return refreshSec;
        }

        public long tableCacheMaxNum() {
            return tableCacheMaxNum;
        }

        public long partitionCacheMaxNum() {
            return partitionCacheMaxNum;
        }
    }

    /** Loader for Cache 1; injectable for tests and call counting. */
    @FunctionalInterface
    public interface SnapshotLoader {
        StarRocksRemoteTableStats.Snapshot load(String dbName, String tableName,
                                                StarRocksRemoteTableStats.Epochs cachedEpochs);
    }

    /** Loader for Cache 2; injectable for tests and call counting. */
    @FunctionalInterface
    public interface PartitionStatsLoader {
        StarRocksRemoteTableStats.PartitionStatsResponse load(String dbName, String tableName,
                                                              List<Long> partitionIds, List<String> columns);
    }

    private static final class PartitionStatsKey {
        private final String dbName;
        private final String tableName;
        private final long partitionId;

        private PartitionStatsKey(String dbName, String tableName, long partitionId) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionId = partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PartitionStatsKey)) {
                return false;
            }
            PartitionStatsKey that = (PartitionStatsKey) o;
            return partitionId == that.partitionId && dbName.equals(that.dbName) && tableName.equals(that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, partitionId);
        }
    }

    private static final class PartitionStatsEntry {
        // Keyed by lowercase column name; values keep the remote-reported case.
        private final Map<String, StarRocksRemoteTableStats.PartitionColumnStats> columnStats;

        private PartitionStatsEntry(Map<String, StarRocksRemoteTableStats.PartitionColumnStats> columnStats) {
            this.columnStats = columnStats;
        }

        private Set<String> columns() {
            return columnStats.keySet();
        }
    }

    private Optional<StarRocksRemoteScanWire.Table> loadTable(DatabaseTableName cacheKey) {
        return Optional.ofNullable(feClient.getTable(cacheKey.getDatabaseName(), cacheKey.getTableName()));
    }
}
