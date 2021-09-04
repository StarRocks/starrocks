// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HiveMetaCache {
    private static final Logger LOG = LogManager.getLogger(HiveMetaCache.class);
    private static final long MAX_TABLE_CACHE_SIZE = 1000L;
    private static final long MAX_PARTITION_CACHE_SIZE = MAX_TABLE_CACHE_SIZE * 1000L;

    private final HiveMetaClient client;

    // HivePartitionKeysKey => ImmutableMap<PartitionKey -> PartitionId>
    // for unPartitioned table, partition map is: ImmutableMap<>.of(new PartitionKey(), PartitionId)
    LoadingCache<HivePartitionKeysKey, ImmutableMap<PartitionKey, Long>> partitionKeysCache;
    // HivePartitionKey => Partitions
    LoadingCache<HivePartitionKey, HivePartition> partitionsCache;

    // statistic cache
    // HiveTableKey => HiveTableStatistic
    LoadingCache<HiveTableKey, HiveTableStats> tableStatsCache;
    // HivePartitionKey => PartitionStatistic
    LoadingCache<HivePartitionKey, HivePartitionStats> partitionStatsCache;

    // HiveTableColumnsKey => ImmutableMap<ColumnName -> HiveColumnStats>
    LoadingCache<HiveTableColumnsKey, ImmutableMap<String, HiveColumnStats>> tableColumnStatsCache;

    public HiveMetaCache(HiveMetaClient hiveMetaClient, Executor executor) {
        this.client = hiveMetaClient;

        init(executor);
    }

    private void init(Executor executor) {
        partitionKeysCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HivePartitionKeysKey, ImmutableMap<PartitionKey, Long>>() {
                    @Override
                    public ImmutableMap<PartitionKey, Long> load(HivePartitionKeysKey key) throws Exception {
                        return loadPartitionKeys(key);
                    }
                }, executor));

        partitionsCache = newCacheBuilder(MAX_PARTITION_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HivePartitionKey, HivePartition>() {
                    @Override
                    public HivePartition load(HivePartitionKey key) throws Exception {
                        return loadPartition(key);
                    }
                }, executor));

        tableStatsCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HiveTableKey, HiveTableStats>() {
                    @Override
                    public HiveTableStats load(HiveTableKey key) throws Exception {
                        return loadTableStats(key);
                    }
                }, executor));

        partitionStatsCache = newCacheBuilder(MAX_PARTITION_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HivePartitionKey, HivePartitionStats>() {
                    @Override
                    public HivePartitionStats load(HivePartitionKey key) throws Exception {
                        return loadPartitionStats(key);
                    }
                }, executor));

        tableColumnStatsCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HiveTableColumnsKey, ImmutableMap<String, HiveColumnStats>>() {
                    @Override
                    public ImmutableMap<String, HiveColumnStats> load(HiveTableColumnsKey key) throws Exception {
                        return loadTableColumnStats(key);
                    }
                }, executor));
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder.expireAfterWrite(Config.hive_meta_cache_ttl_s, SECONDS);
        if (Config.hive_meta_cache_ttl_s > Config.hive_meta_cache_refresh_interval_s) {
            cacheBuilder.refreshAfterWrite(Config.hive_meta_cache_refresh_interval_s, SECONDS);
        }
        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    private ImmutableMap<PartitionKey, Long> loadPartitionKeys(HivePartitionKeysKey key) throws DdlException {
        Map<PartitionKey, Long> partitionKeys = client.getPartitionKeys(key.getDatabaseName(),
                key.getTableName(),
                key.getPartitionColumns());
        return ImmutableMap.copyOf(partitionKeys);
    }

    private HivePartition loadPartition(HivePartitionKey key) throws DdlException {
        return client.getPartition(key.getDatabaseName(), key.getTableName(), key.getPartitionValues());
    }

    private HiveTableStats loadTableStats(HiveTableKey key) throws DdlException {
        return client.getTableStats(key.getDatabaseName(), key.getTableName());
    }

    private HivePartitionStats loadPartitionStats(HivePartitionKey key) throws Exception {
        HivePartitionStats partitionStats =
                client.getPartitionStats(key.getDatabaseName(), key.getTableName(), key.getPartitionValues());
        HivePartition partition = partitionsCache.get(key);
        long totalFileBytes = 0;
        for (HdfsFileDesc fileDesc : partition.getFiles()) {
            totalFileBytes += fileDesc.getLength();
        }
        partitionStats.setTotalFileBytes(totalFileBytes);
        return partitionStats;
    }

    private ImmutableMap<String, HiveColumnStats> loadTableColumnStats(HiveTableColumnsKey key) throws Exception {
        if (key.getPartitionColumns().size() > 0) {
            List<PartitionKey> partitionKeys = new ArrayList<>(partitionKeysCache
                    .get(HivePartitionKeysKey.gen(key.getDatabaseName(),
                            key.getTableName(),
                            key.getPartitionColumns())).keySet());
            return ImmutableMap.copyOf(client.getTableLevelColumnStatsForPartTable(key.getDatabaseName(),
                    key.getTableName(),
                    partitionKeys,
                    key.getPartitionColumns(),
                    key.getColumnNames()));
        } else {
            return ImmutableMap.copyOf(client.getTableLevelColumnStatsForUnpartTable(key.getDatabaseName(),
                    key.getTableName(),
                    key.getColumnNames()));
        }
    }

    public ImmutableMap<PartitionKey, Long> getPartitionKeys(String dbName, String tableName,
                                                             List<Column> partColumns) throws DdlException {
        try {
            return partitionKeysCache.get(HivePartitionKeysKey.gen(dbName, tableName, partColumns));
        } catch (ExecutionException e) {
            LOG.warn("get partition keys failed", e);
            throw new DdlException("get partition keys failed: " + e.getMessage());
        }
    }

    public HivePartition getPartition(String dbName, String tableName,
                                      PartitionKey partitionKey) throws DdlException {
        List<String> partitionValues = Utils.getPartitionValues(partitionKey);
        try {
            return partitionsCache.get(new HivePartitionKey(dbName, tableName, partitionValues));
        } catch (ExecutionException e) {
            throw new DdlException("get partition detail failed: " + e.getMessage());
        }
    }

    public HiveTableStats getTableStats(String dbName, String tableName) throws DdlException {
        try {
            return tableStatsCache.get(new HiveTableKey(dbName, tableName));
        } catch (ExecutionException e) {
            throw new DdlException("get table stats failed: " + e.getMessage());
        }
    }

    public HivePartitionStats getPartitionStats(String dbName, String tableName,
                                                PartitionKey partitionKey) throws DdlException {
        List<String> partValues = Utils.getPartitionValues(partitionKey);
        HivePartitionKey key = HivePartitionKey.gen(dbName, tableName, partValues);
        try {
            return partitionStatsCache.get(key);
        } catch (ExecutionException e) {
            throw new DdlException("get table partition stats failed: " + e.getMessage());
        }
    }

    // NOTE: always using all column names in HiveTable as request param, this will get the best cache effect.
    // set all partitions keys to partitionKeys param, if table is partition table
    public ImmutableMap<String, HiveColumnStats> getTableLevelColumnStats(String dbName, String tableName,
                                                                          List<Column> partitionColumns,
                                                                          List<String> columnNames)
            throws DdlException {
        HiveTableColumnsKey key = HiveTableColumnsKey.gen(dbName, tableName, partitionColumns, columnNames);
        try {
            return tableColumnStatsCache.get(key);
        } catch (ExecutionException e) {
            throw new DdlException("get table level column stats failed: " + e.getMessage());
        }
    }

    public void refreshTable(String dbName, String tableName, List<Column> partColumns, List<String> columnNames)
            throws DdlException {
        HivePartitionKeysKey hivePartitionKeysKey = HivePartitionKeysKey.gen(dbName, tableName, partColumns);
        HiveTableKey hiveTableKey = HiveTableKey.gen(dbName, tableName);
        HiveTableColumnsKey hiveTableColumnsKey = HiveTableColumnsKey.gen(dbName, tableName, partColumns, columnNames);
        try {
            ImmutableMap<PartitionKey, Long> partitionKeys = loadPartitionKeys(hivePartitionKeysKey);
            partitionKeysCache.put(hivePartitionKeysKey, partitionKeys);
            tableStatsCache.put(hiveTableKey, loadTableStats(hiveTableKey));
            tableColumnStatsCache.put(hiveTableColumnsKey, loadTableColumnStats(hiveTableColumnsKey));

            // for unpartition table, refresh the partition info, because there is only one partition
            if (partColumns.size() <= 0) {
                HivePartitionKey hivePartitionKey = HivePartitionKey.gen(dbName, tableName, new ArrayList<>());
                partitionsCache.put(hivePartitionKey, loadPartition(hivePartitionKey));
                partitionStatsCache.put(hivePartitionKey, loadPartitionStats(hivePartitionKey));
            }
        } catch (Exception e) {
            LOG.warn("refresh table cache failed", e);
            throw new DdlException("refresh table cache failed: " + e.getMessage());
        }
    }

    public void refreshPartition(String dbName, String tableName, List<String> partNames) throws DdlException {
        try {
            for (String partName : partNames) {
                List<String> partValues = client.partitionNameToVals(partName);
                HivePartitionKey key = HivePartitionKey.gen(dbName, tableName, partValues);
                partitionsCache.put(key, loadPartition(key));
                partitionStatsCache.put(key, loadPartitionStats(key));
            }
        } catch (Exception e) {
            LOG.warn("refresh partition cache failed", e);
            throw new DdlException("refresh partition cached failed: " + e.getMessage());
        }
    }

    public void clearCache(String dbName, String tableName) {
        HivePartitionKeysKey hivePartitionKeysKey = HivePartitionKeysKey.gen(dbName, tableName, null);
        ImmutableMap<PartitionKey, Long> partitionKeys = partitionKeysCache.getIfPresent(hivePartitionKeysKey);
        partitionKeysCache.invalidate(hivePartitionKeysKey);
        tableStatsCache.invalidate(HiveTableKey.gen(dbName, tableName));
        tableColumnStatsCache.invalidate(HiveTableColumnsKey.gen(dbName, tableName, null, null));
        if (partitionKeys != null) {
            for (Map.Entry<PartitionKey, Long> entry : partitionKeys.entrySet()) {
                HivePartitionKey pKey =
                        HivePartitionKey.gen(dbName, tableName, Utils.getPartitionValues(entry.getKey()));
                partitionsCache.invalidate(pKey);
                partitionStatsCache.invalidate(pKey);
            }
        }
    }
}