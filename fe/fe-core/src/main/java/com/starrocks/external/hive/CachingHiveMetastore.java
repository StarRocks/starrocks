// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

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
import com.starrocks.catalog.Table;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingHiveMetastore implements IHiveMetastore {
    public static final long NEVER_CACHE = 0;
    public static final long NEVER_EVICT = -1;
    public static final long NEVER_REFRESH = -1;
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

        // The list names interface of hive metastore latency is very low, so we default to pull the latest every time.
        if (enableListNamesCache) {
            databaseNamesCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                    .build(asyncReloading(CacheLoader.from(this::loadAllDatabaseNames), executor));
            tableNamesCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                    .build(asyncReloading(CacheLoader.from(this::loadAllTableNames), executor));
            partitionKeysCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                    .build(asyncReloading(CacheLoader.from(this::loadPartitionKeys), executor));
        } else {
            databaseNamesCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE, NEVER_CACHE)
                    .build(asyncReloading(CacheLoader.from(this::loadAllDatabaseNames), executor));
            tableNamesCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE, NEVER_CACHE)
                    .build(asyncReloading(CacheLoader.from(this::loadAllTableNames), executor));
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
            partitionsByName.put(entry.getKey().getPartitionNames().get(), entry.getValue());
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
            partitions.put(partitionName, partitionsByNames.get(partitionName.getPartitionNames().get()));
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
        Preconditions.checkState(hivePartitionName.getPartitionNames().isPresent(), "hive partition name is missing");
        Map<String, HivePartitionStats> partitionsStatistics = metastore
                .getPartitionStatistics(table, Lists.newArrayList(hivePartitionName.getPartitionNames().get()));

        return partitionsStatistics.get(hivePartitionName.getPartitionNames().get());
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

    public void refreshTable(String hiveDbName, String hiveTblName) {
        databaseCache.put(hiveDbName, loadDb(hiveDbName));

        HiveTableName hiveTableName = HiveTableName.of(hiveDbName, hiveTblName);
        Table updatedTable = loadTable(hiveTableName);
        tableCache.put(hiveTableName, updatedTable);

        HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) updatedTable;
        if (hmsTable.isUnPartitioned()) {
            HivePartitionName hivePartitionName = HivePartitionName.of(hiveDbName, hiveTblName, Lists.newArrayList());
            Partition updatedPartition = loadPartition(hivePartitionName);
            partitionCache.put(hivePartitionName, updatedPartition);
        } else {
            List<HivePartitionName> presentPartitionNames = partitionCache.asMap().keySet().stream()
                    .filter(partitionName -> partitionName.approximateMatchTable(hiveDbName, hiveTblName))
                    .collect(Collectors.toList());
            if (presentPartitionNames.size() > 0) {
                Map<HivePartitionName, Partition> updatedPartitions = loadPartitionsByNames(presentPartitionNames);
                partitionCache.putAll(updatedPartitions);
            }
        }

        if (hmsTable.isUnPartitioned()) {
            tableStatsCache.put(hiveTableName, loadTableStatistics(hiveTableName));
        } else {
            List<HivePartitionName> presentPartitionStatistics = partitionStatsCache.asMap().keySet().stream()
                    .filter(partitionName -> partitionName.approximateMatchTable(hiveDbName, hiveTblName))
                    .collect(Collectors.toList());
            if (presentPartitionStatistics.size() > 0) {
                Map<HivePartitionName, HivePartitionStats> updatePartitionStats =
                        loadPartitionsStatistics(presentPartitionStatistics);
                partitionStatsCache.putAll(updatePartitionStats);
            }
        }
    }

    public void refreshPartition(List<HivePartitionName> partitionNames) {
        Map<HivePartitionName, Partition> updatedPartitions = loadPartitionsByNames(partitionNames);
        partitionCache.putAll(updatedPartitions);

        Map<HivePartitionName, HivePartitionStats> updatePartitionStats = loadPartitionsStatistics(partitionNames);
        partitionStatsCache.putAll(updatePartitionStats);
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key) {
        try {
            return cache.getUnchecked(key);
        } catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), StarRocksConnectorException.class);
            throw e;
        }
    }

    private static <K, V> Map<K, V> getAll(LoadingCache<K, V> cache, Iterable<K> keys) {
        try {
            return cache.getAll(keys);
        } catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), StarRocksConnectorException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    public void invalidateAll() {
        databaseNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        partitionKeysCache.invalidateAll();
        databaseCache.invalidateAll();
        tableCache.invalidateAll();
        partitionCache.invalidateAll();
        tableStatsCache.invalidateAll();
        partitionStatsCache.invalidateAll();
    }
}