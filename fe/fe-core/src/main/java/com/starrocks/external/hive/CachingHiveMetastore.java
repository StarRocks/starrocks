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
    protected LoadingCache<NewHivePartitionName, Partition> partitionCache;
    protected LoadingCache<HiveTableName, HivePartitionStatistics> tableStatsCache;
    protected LoadingCache<NewHivePartitionName, HivePartitionStatistics> partitionStatsCache;

    public static CachingHiveMetastore reuseMetastore(IHiveMetastore metastore, long perQueryCacheMaxSize) {
        return new CachingHiveMetastore(
                metastore,
                newDirectExecutorService(),
                NEVER_EVICT,
                NEVER_REFRESH,
                perQueryCacheMaxSize,
                true);
    }

    public CachingHiveMetastore(IHiveMetastore metastore, Executor executor, long expireAfterWriteSec,
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
                .build(asyncReloading(new CacheLoader<NewHivePartitionName, Partition>() {
                    @Override
                    public Partition load(@NotNull NewHivePartitionName key) {
                        return loadPartition(key);
                    }

                    @Override
                    public Map<NewHivePartitionName, Partition> loadAll(
                            @NotNull Iterable<? extends NewHivePartitionName> partitionKeys) {
                        return loadPartitionsByNames(partitionKeys);
                    }
                }, executor));

        tableStatsCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadTableStatistics), executor));

        partitionStatsCache = newCacheBuilder(expireAfterWriteSec, NEVER_REFRESH, maxSize)
                .build(asyncReloading(new CacheLoader<NewHivePartitionName, HivePartitionStatistics>() {
                    @Override
                    public HivePartitionStatistics load(@NotNull NewHivePartitionName key) {
                        return loadPartitionStatistics(key);
                    }

                    @Override
                    public Map<NewHivePartitionName, HivePartitionStatistics> loadAll(
                            @NotNull Iterable<? extends NewHivePartitionName> partitionKeys) {
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
        return get(partitionCache, NewHivePartitionName.of(dbName, tblName, partitionValues));
    }

    public Partition loadPartition(NewHivePartitionName key) {
        return metastore.getPartition(key.getDatabaseName(), key.getTableName(), key.getPartitionValues());
    }

    public Map<String, Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames) {
        List<NewHivePartitionName> hivePartitionNames = partitionNames.stream()
                .map(partitionName -> NewHivePartitionName.of(dbName, tblName, partitionName))
                .peek(hivePartitionName -> checkState(hivePartitionName.getPartitionNames().isPresent(),
                        "partition name is missing"))
                .collect(Collectors.toList());

        Map<NewHivePartitionName, Partition> all = getAll(partitionCache, hivePartitionNames);
        ImmutableMap.Builder<String, Partition> partitionsByName = ImmutableMap.builder();
        for (Map.Entry<NewHivePartitionName, Partition> entry : all.entrySet()) {
            partitionsByName.put(entry.getKey().getPartitionNames().get(), entry.getValue());
        }
        return partitionsByName.build();
    }

    private Map<NewHivePartitionName, Partition> loadPartitionsByNames(Iterable<? extends NewHivePartitionName> partitionNames) {
        NewHivePartitionName hivePartitionName = Iterables.get(partitionNames, 0);
        Map<String, Partition> partitionsByNames =  metastore.getPartitionsByNames(
                hivePartitionName.getDatabaseName(),
                hivePartitionName.getTableName(),
                Streams.stream(partitionNames).map(partitionName -> partitionName.getPartitionNames().get())
                        .collect(Collectors.toList()));

        ImmutableMap.Builder<NewHivePartitionName, Partition> partitions = ImmutableMap.builder();
        for (NewHivePartitionName partitionName : partitionNames) {
            partitions.put(partitionName, partitionsByNames.get(partitionName.getPartitionNames().get()));
        }
        return partitions.build();
    }

    public HivePartitionStatistics getTableStatistics(String dbName, String tblName) {
        return get(tableStatsCache, HiveTableName.of(dbName, tblName));
    }

    private HivePartitionStatistics loadTableStatistics(HiveTableName hiveTableName) {
        return metastore.getTableStatistics(hiveTableName.getDatabaseName(), hiveTableName.getTableName());
    }

    @Override
    public Map<String, HivePartitionStatistics> getPartitionStatistics(Table table, List<String> partitionNames) {
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();

        List<NewHivePartitionName> hivePartitionNames = partitionNames.stream()
                .map(partitionName -> NewHivePartitionName.of(dbName, tblName, partitionName))
                .collect(Collectors.toList());

        Map<NewHivePartitionName, HivePartitionStatistics> statistics = getAll(partitionStatsCache, hivePartitionNames);

        return statistics.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> entry.getKey().getPartitionNames().get(), Map.Entry::getValue));
    }

    public Map<String, HivePartitionStatistics> getPresentPartitionsStatistics(List<NewHivePartitionName> partitions) {
        return partitionStatsCache.getAllPresent(partitions).entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getPartitionNames().get(), Map.Entry::getValue));

    }

    private HivePartitionStatistics loadPartitionStatistics(NewHivePartitionName hivePartitionName) {
        Table table = getTable(hivePartitionName.getDatabaseName(), hivePartitionName.getTableName());
        Preconditions.checkState(hivePartitionName.getPartitionNames().isPresent(), "hive partition name is missing");
        Map<String, HivePartitionStatistics> partitionsStatistics = metastore
                .getPartitionStatistics(table, Lists.newArrayList(hivePartitionName.getPartitionNames().get()));

        return partitionsStatistics.get(hivePartitionName.getPartitionNames().get());
    }

    private Map<NewHivePartitionName, HivePartitionStatistics> loadPartitionsStatistics(
            Iterable<? extends NewHivePartitionName> partitionNames) {
        NewHivePartitionName hivePartitionName = Iterables.get(partitionNames, 0);
        Table table = getTable(hivePartitionName.getDatabaseName(), hivePartitionName.getTableName());

        Map<String, HivePartitionStatistics> partitionsStatistics =  metastore.getPartitionStatistics(table,
                Streams.stream(partitionNames).map(partitionName -> partitionName.getPartitionNames().get())
                        .collect(Collectors.toList()));

        return partitionsStatistics.entrySet().stream().collect(Collectors.toMap(
                entry -> NewHivePartitionName.of(
                        hivePartitionName.getDatabaseName(), hivePartitionName.getTableName(), entry.getKey()),
                Map.Entry::getValue
        ));
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

}