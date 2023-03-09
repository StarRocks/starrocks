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


package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.events.MetastoreNotificationFetchException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingHiveMetastore implements IHiveMetastore {
    private static final Logger LOG = LogManager.getLogger(CachingHiveMetastore.class);

    public static final long NEVER_CACHE = 0;
    public static final long NEVER_EVICT = -1;
    public static final long NEVER_REFRESH = -1;
    private final boolean enableListNameCache;
    protected final IHiveMetastore metastore;

    protected LoadingCache<String, List<String>> databaseNamesCache;
    protected LoadingCache<String, List<String>> tableNamesCache;

    // eg: HiveTableName -> List("year=2022/month=10", "year=2022/month=11")
    protected LoadingCache<HiveTableName, List<String>> partitionKeysCache;

    protected LoadingCache<String, Database> databaseCache;
    protected LoadingCache<HiveTableName, Table> tableCache;

    // eg: "year=2022/month=10" -> Partition
    protected LoadingCache<HivePartitionName, Partition> partitionCache;
    protected LoadingCache<HiveTableName, HivePartitionStats> tableStatsCache;
    protected LoadingCache<HivePartitionName, HivePartitionStats> partitionStatsCache;

    public static CachingHiveMetastore createQueryLevelInstance(IHiveMetastore metastore, long perQueryCacheMaxSize) {
        return new CachingHiveMetastore(
                metastore,
                newDirectExecutorService(),
                NEVER_EVICT,
                NEVER_REFRESH,
                perQueryCacheMaxSize,
                true);
    }

    public static CachingHiveMetastore createCatalogLevelInstance(IHiveMetastore metastore, Executor executor,
                                                                  long expireAfterWrite, long refreshInterval,
                                                                  long maxSize, boolean enableListNamesCache) {
        return new CachingHiveMetastore(metastore, executor, expireAfterWrite, refreshInterval, maxSize, enableListNamesCache);
    }

    protected CachingHiveMetastore(IHiveMetastore metastore, Executor executor, long expireAfterWriteSec,
                                   long refreshIntervalSec, long maxSize, boolean enableListNamesCache) {
        this.metastore = metastore;
        this.enableListNameCache = enableListNamesCache;

        databaseNamesCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE, NEVER_CACHE)
                .build(asyncReloading(CacheLoader.from(this::loadAllDatabaseNames), executor));
        tableNamesCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE, NEVER_CACHE)
                .build(asyncReloading(CacheLoader.from(this::loadAllTableNames), executor));

        // The list names interface of hive metastore latency is very low, so we default to pull the latest every time.
        if (enableListNamesCache) {
            partitionKeysCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                    .build(asyncReloading(CacheLoader.from(this::loadPartitionKeys), executor));
        } else {
            partitionKeysCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE, NEVER_CACHE)
                    .build(asyncReloading(CacheLoader.from(this::loadPartitionKeys), executor));
        }

        databaseCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadDb), executor));

        tableCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadTable), executor));

        partitionCache = newCacheBuilder(expireAfterWriteSec, NEVER_REFRESH, maxSize)
                .build(asyncReloading(new CacheLoader<HivePartitionName, Partition>() {
                    @Override
                    public Partition load(@NotNull HivePartitionName key) {
                        return loadPartition(key);
                    }

                    @Override
                    public Map<HivePartitionName, Partition> loadAll(
                            @NotNull Iterable<? extends HivePartitionName> partitionKeys) {
                        return loadPartitionsByNames(partitionKeys);
                    }
                }, executor));

        tableStatsCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadTableStatistics), executor));

        partitionStatsCache = newCacheBuilder(expireAfterWriteSec, NEVER_REFRESH, maxSize)
                .build(asyncReloading(new CacheLoader<HivePartitionName, HivePartitionStats>() {
                    @Override
                    public HivePartitionStats load(@NotNull HivePartitionName key) {
                        return loadPartitionStatistics(key);
                    }

                    @Override
                    public Map<HivePartitionName, HivePartitionStats> loadAll(
                            @NotNull Iterable<? extends HivePartitionName> partitionKeys) {
                        return loadPartitionsStatistics(partitionKeys);
                    }
                }, executor));
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long refreshSec, long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, SECONDS);
        }

        if (refreshSec > 0 && expiresAfterWriteSec > refreshSec) {
            cacheBuilder.refreshAfterWrite(refreshSec, SECONDS);
        }

        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    public List<String> getAllDatabaseNames() {
        return get(databaseNamesCache, "");
    }

    private List<String> loadAllDatabaseNames() {
        return metastore.getAllDatabaseNames();
    }

    public List<String> getAllTableNames(String dbName) {
        return get(tableNamesCache, dbName);
    }

    public Set<HiveTableName> getCachedTableNames() {
        return partitionKeysCache.asMap().keySet();
    }

    private List<String> loadAllTableNames(String dbName) {
        return metastore.getAllTableNames(dbName);
    }

    public List<String> getPartitionKeys(String dbName, String tableName) {
        return get(partitionKeysCache, HiveTableName.of(dbName, tableName));
    }

    private List<String> loadPartitionKeys(HiveTableName hiveTableName) {
        return metastore.getPartitionKeys(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    public Database getDb(String dbName) {
        return get(databaseCache, dbName);
    }

    private Database loadDb(String dbName) {
        return metastore.getDb(dbName);
    }

    public Table getTable(String dbName, String tableName) {
        return get(tableCache, HiveTableName.of(dbName, tableName));
    }

    private Table loadTable(HiveTableName hiveTableName) {
        return metastore.getTable(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        return get(partitionCache, HivePartitionName.of(dbName, tblName, partitionValues));
    }

    public Partition loadPartition(HivePartitionName key) {
        return metastore.getPartition(key.getDatabaseName(), key.getTableName(), key.getPartitionValues());
    }

    public Map<String, Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames) {
        List<HivePartitionName> hivePartitionNames = partitionNames.stream()
                .map(partitionName -> HivePartitionName.of(dbName, tblName, partitionName))
                .peek(hivePartitionName -> checkState(hivePartitionName.getPartitionNames().isPresent(),
                        "partition name is missing"))
                .collect(Collectors.toList());

        Map<HivePartitionName, Partition> all = getAll(partitionCache, hivePartitionNames);
        ImmutableMap.Builder<String, Partition> partitionsByName = ImmutableMap.builder();
        for (Map.Entry<HivePartitionName, Partition> entry : all.entrySet()) {
            Optional<String> optPartitionNames = entry.getKey().getPartitionNames();
            Preconditions.checkState(optPartitionNames.isPresent());
            partitionsByName.put(optPartitionNames.get(), entry.getValue());
        }
        return partitionsByName.build();
    }

    private Map<HivePartitionName, Partition> loadPartitionsByNames(Iterable<? extends HivePartitionName> partitionNames) {
        HivePartitionName hivePartitionName = Iterables.get(partitionNames, 0);
        Map<String, Partition> partitionsByNames =  metastore.getPartitionsByNames(
                hivePartitionName.getDatabaseName(),
                hivePartitionName.getTableName(),
                Streams.stream(partitionNames).map(partitionName -> partitionName.getPartitionNames().get())
                        .collect(Collectors.toList()));

        ImmutableMap.Builder<HivePartitionName, Partition> partitions = ImmutableMap.builder();
        for (HivePartitionName partitionName : partitionNames) {
            Optional<Partition> optPartition = partitionName.getPartitionNames().map(partitionsByNames::get);
            Preconditions.checkState(optPartition.isPresent());
            partitions.put(partitionName, optPartition.get());
        }
        return partitions.build();
    }

    public HivePartitionStats getTableStatistics(String dbName, String tblName) {
        return get(tableStatsCache, HiveTableName.of(dbName, tblName));
    }

    private HivePartitionStats loadTableStatistics(HiveTableName hiveTableName) {
        return metastore.getTableStatistics(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    @Override
    public Map<String, HivePartitionStats> getPartitionStatistics(Table table, List<String> partitionNames) {
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();

        List<HivePartitionName> hivePartitionNames = partitionNames.stream()
                .map(partitionName -> HivePartitionName.of(dbName, tblName, partitionName))
                .collect(Collectors.toList());

        Map<HivePartitionName, HivePartitionStats> statistics = getAll(partitionStatsCache, hivePartitionNames);

        return statistics.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> entry.getKey().getPartitionNames().get(), Map.Entry::getValue));
    }

    public Map<String, HivePartitionStats> getPresentPartitionsStatistics(List<HivePartitionName> partitions) {
        if (metastore instanceof CachingHiveMetastore) {
            return ((CachingHiveMetastore) metastore).getPresentPartitionsStatistics(partitions);
        } else {
            return partitionStatsCache.getAllPresent(partitions).entrySet().stream()
                    .collect(Collectors.toMap(entry -> entry.getKey().getPartitionNames().get(), Map.Entry::getValue));
        }
    }

    private HivePartitionStats loadPartitionStatistics(HivePartitionName hivePartitionName) {
        Table table = getTable(hivePartitionName.getDatabaseName(), hivePartitionName.getTableName());
        Optional<String> optPartitionNames = hivePartitionName.getPartitionNames();
        Preconditions.checkState(optPartitionNames.isPresent(), "hive partition name is missing");
        Map<String, HivePartitionStats> partitionsStatistics = metastore
                .getPartitionStatistics(table, Lists.newArrayList(optPartitionNames.get()));

        return partitionsStatistics.get(optPartitionNames.get());
    }

    private Map<HivePartitionName, HivePartitionStats> loadPartitionsStatistics(
            Iterable<? extends HivePartitionName> partitionNames) {
        HivePartitionName hivePartitionName = Iterables.get(partitionNames, 0);
        Table table = getTable(hivePartitionName.getDatabaseName(), hivePartitionName.getTableName());

        Map<String, HivePartitionStats> partitionsStatistics =  metastore.getPartitionStatistics(table,
                Streams.stream(partitionNames).map(partitionName -> partitionName.getPartitionNames().get())
                        .collect(Collectors.toList()));

        return partitionsStatistics.entrySet().stream().collect(Collectors.toMap(
                entry -> HivePartitionName.of(
                        hivePartitionName.getDatabaseName(), hivePartitionName.getTableName(), entry.getKey()),
                Map.Entry::getValue
        ));
    }

    public synchronized void refreshTable(String hiveDbName, String hiveTblName, boolean onlyCachedPartitions) {
        HiveTableName hiveTableName = HiveTableName.of(hiveDbName, hiveTblName);
        Table updatedTable = loadTable(hiveTableName);
        tableCache.put(hiveTableName, updatedTable);
        if (enableListNameCache) {
            partitionKeysCache.put(hiveTableName, loadPartitionKeys(hiveTableName));
        }

        HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) updatedTable;
        if (hmsTable.isUnPartitioned()) {
            HivePartitionName hivePartitionName = HivePartitionName.of(hiveDbName, hiveTblName, Lists.newArrayList());
            Partition updatedPartition = loadPartition(hivePartitionName);
            partitionCache.put(hivePartitionName, updatedPartition);
            tableStatsCache.put(hiveTableName, loadTableStatistics(hiveTableName));
        } else {
            List<String> existNames = loadPartitionKeys(hiveTableName);
            List<HivePartitionName> presentPartitionNames;
            List<HivePartitionName> presentPartitionStatistics;

            if (onlyCachedPartitions) {
                presentPartitionNames = getPresentPartitionNames(partitionCache, hiveDbName, hiveTblName);
                presentPartitionStatistics = getPresentPartitionNames(partitionStatsCache, hiveDbName, hiveTblName);
            } else {
                presentPartitionNames = presentPartitionStatistics = existNames.stream()
                        .map(partitionKey -> HivePartitionName.of(hiveDbName, hiveTblName, partitionKey))
                        .collect(Collectors.toList());
            }

            refreshPartitions(presentPartitionNames, existNames, this::loadPartitionsByNames, partitionCache);
            if (Config.enable_refresh_hive_partitions_statistics) {
                refreshPartitions(presentPartitionStatistics, existNames, this::loadPartitionsStatistics, partitionStatsCache);
            }
        }
    }

    private <T> void refreshPartitions(List<HivePartitionName> presentInCache,
                                       List<String> partitionNamesInHMS,
                                       Function<List<HivePartitionName>, Map<HivePartitionName, T>> reload,
                                       LoadingCache<HivePartitionName, T> cache) {
        List<HivePartitionName> needToRefresh = Lists.newArrayList();
        List<HivePartitionName> needToInvalidate = Lists.newArrayList();
        for (HivePartitionName name : presentInCache) {
            Optional<String> optPartitionNames = name.getPartitionNames();
            if (optPartitionNames.isPresent() && partitionNamesInHMS.contains(optPartitionNames.get())) {
                needToRefresh.add(name);
            } else {
                needToInvalidate.add(name);
            }
        }

        if (needToRefresh.size() > 0) {
            for (int i = 0; i < needToRefresh.size(); i += Config.max_hive_partitions_per_rpc) {
                List<HivePartitionName> partsToFetch = needToRefresh.subList(
                        i, Math.min(i + Config.max_hive_partitions_per_rpc, needToRefresh.size()));
                Map<HivePartitionName, T> updatedPartitions = reload.apply(partsToFetch);
                cache.putAll(updatedPartitions);
            }
        }
        cache.invalidateAll(needToInvalidate);
    }

    public synchronized void refreshPartition(List<HivePartitionName> partitionNames) {
        Map<HivePartitionName, Partition> updatedPartitions = loadPartitionsByNames(partitionNames);
        partitionCache.putAll(updatedPartitions);

        Map<HivePartitionName, HivePartitionStats> updatePartitionStats = loadPartitionsStatistics(partitionNames);
        partitionStatsCache.putAll(updatePartitionStats);

        if (enableListNameCache && !partitionNames.isEmpty()) {
            HivePartitionName firstName = partitionNames.get(0);
            HiveTableName hiveTableName = HiveTableName.of(firstName.getDatabaseName(), firstName.getTableName());
            partitionKeysCache.put(hiveTableName, loadPartitionKeys(hiveTableName));
        }
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key) {
        try {
            return cache.getUnchecked(key);
        } catch (UncheckedExecutionException e) {
            LOG.error("Error occurred when loading cache", e);
            throwIfInstanceOf(e.getCause(), StarRocksConnectorException.class);
            throw e;
        }
    }

    private static <K, V> Map<K, V> getAll(LoadingCache<K, V> cache, Iterable<K> keys) {
        try {
            return cache.getAll(keys);
        } catch (ExecutionException | UncheckedExecutionException e) {
            LOG.error("Error occurred when loading cache", e);
            throwIfInstanceOf(e.getCause(), StarRocksConnectorException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    private List<HivePartitionName> getPresentPartitionNames(LoadingCache<HivePartitionName, ?> cache,
                                                             String dbName, String tableName) {
        return cache.asMap().keySet().stream()
                .filter(partitionName -> partitionName.approximateMatchTable(dbName, tableName))
                .collect(Collectors.toList());
    }

    public synchronized void invalidateAll() {
        databaseNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        partitionKeysCache.invalidateAll();
        databaseCache.invalidateAll();
        tableCache.invalidateAll();
        partitionCache.invalidateAll();
        tableStatsCache.invalidateAll();
        partitionStatsCache.invalidateAll();
    }

    public synchronized void invalidateTable(String dbName, String tableName) {
        HiveTableName hiveTableName = HiveTableName.of(dbName, tableName);
        tableCache.invalidate(hiveTableName);
        tableStatsCache.invalidate(hiveTableName);
        partitionKeysCache.invalidate(hiveTableName);
        List<HivePartitionName> presentPartitions = getPresentPartitionNames(partitionCache, dbName, tableName);
        presentPartitions.forEach(p -> partitionCache.invalidate(p));
        List<HivePartitionName> presentPartitionStats = getPresentPartitionNames(partitionStatsCache, dbName, tableName);
        presentPartitionStats.forEach(p -> partitionStatsCache.invalidate(p));
    }

    public synchronized void invalidatePartition(HivePartitionName partitionName) {
        HiveTableName hiveTableName = HiveTableName.of(partitionName.getDatabaseName(), partitionName.getTableName());
        partitionKeysCache.invalidate(hiveTableName);
        partitionCache.invalidate(partitionName);
        partitionStatsCache.invalidate(partitionName);
    }

    public boolean isTablePresent(HiveTableName tableName) {
        return tableCache.getIfPresent(tableName) != null;
    }

    public boolean isPartitionPresent(HivePartitionName hivePartitionName) {
        return partitionCache.getIfPresent(hivePartitionName) != null;
    }

    public synchronized void refreshTableByEvent(HiveTable updatedHiveTable, HiveCommonStats commonStats, Partition partition) {
        String dbName = updatedHiveTable.getDbName();
        String tableName = updatedHiveTable.getTableName();
        HiveTableName hiveTableName = HiveTableName.of(dbName, tableName);
        tableCache.put(hiveTableName, updatedHiveTable);
        if (updatedHiveTable.isUnPartitioned()) {
            Map<String, HiveColumnStats> columnStats = get(tableStatsCache, hiveTableName).getColumnStats();
            HivePartitionStats updatedPartitionStats = createPartitionStats(commonStats, columnStats);
            tableStatsCache.put(hiveTableName, updatedPartitionStats);
            partitionCache.put(HivePartitionName.of(dbName, tableName, Lists.newArrayList()), partition);
        } else {
            partitionKeysCache.invalidate(hiveTableName);
            List<HivePartitionName> presentPartitions = getPresentPartitionNames(partitionCache, dbName, tableName);
            presentPartitions.forEach(p -> partitionCache.invalidate(p));
            List<HivePartitionName> presentPartitionStats = getPresentPartitionNames(partitionStatsCache, dbName, tableName);
            presentPartitionStats.forEach(p -> partitionStatsCache.invalidate(p));
        }
    }

    public synchronized void refreshPartitionByEvent(HivePartitionName hivePartitionName,
                                                     HiveCommonStats commonStats,
                                                     Partition partition) {
        Map<String, HiveColumnStats> columnStats = get(partitionStatsCache, hivePartitionName).getColumnStats();
        HivePartitionStats updatedPartitionStats = createPartitionStats(commonStats, columnStats);
        HiveTableName hiveTableName = HiveTableName.of(hivePartitionName.getDatabaseName(), hivePartitionName.getTableName());
        partitionKeysCache.invalidate(hiveTableName);
        partitionCache.put(hivePartitionName, partition);
        partitionStatsCache.put(hivePartitionName, updatedPartitionStats);
    }

    private HivePartitionStats createPartitionStats(HiveCommonStats commonStats, Map<String, HiveColumnStats> columnStats) {
        long totalRowNums = commonStats.getRowNums();
        if (totalRowNums == -1) {
            return HivePartitionStats.empty();
        }
        return new HivePartitionStats(commonStats, columnStats);
    }

    public long getCurrentEventId() {
        return metastore.getCurrentEventId();
    }

    public NotificationEventResponse getNextEventResponse(
            long lastSyncedEventId,
            String catalogName,
            final boolean getAllEvents) throws MetastoreNotificationFetchException {
        return ((HiveMetastore) metastore).getNextEventResponse(lastSyncedEventId, catalogName, getAllEvents);
    }
}