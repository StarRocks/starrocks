// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveResource;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Resource.ResourceType;
import com.starrocks.common.DdlException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HiveRepository {
    // hiveResourceName => HiveMetaClient
    Map<String, HiveMetaClient> metaClients = Maps.newHashMap();
    ReadWriteLock metaClientsLock = new ReentrantReadWriteLock();

    // hiveResourceName => HiveMetaCache
    Map<String, HiveMetaCache> metaCaches = Maps.newHashMap();
    ReadWriteLock metaCachesLock = new ReentrantReadWriteLock();

    Executor executor = Executors.newFixedThreadPool(100);

    public HiveMetaClient getClient(String resourceName) throws DdlException {
        HiveMetaClient client;
        metaClientsLock.readLock().lock();
        try {
            client = metaClients.get(resourceName);
        } finally {
            metaClientsLock.readLock().unlock();
        }

        if (client != null) {
            return client;
        }

        metaClientsLock.writeLock().lock();
        try {
            client = metaClients.get(resourceName);
            if (client != null) {
                return client;
            }
            Resource resource = Catalog.getCurrentCatalog().getResourceMgr().getResource(resourceName);
            if (resource == null) {
                throw new DdlException("get hive client failed, resource[" + resourceName + "] not exists");
            }
            if (resource.getType() != ResourceType.HIVE) {
                throw new DdlException("resource [" + resourceName + "] is not hive resource");
            }

            client = new HiveMetaClient(((HiveResource) resource).getHiveMetastoreURIs());
            metaClients.put(resourceName, client);
        } finally {
            metaClientsLock.writeLock().unlock();
        }

        return client;
    }

    public HiveMetaCache getMetaCache(String resourceName) throws DdlException {
        HiveMetaCache hiveMetaCache;
        metaCachesLock.readLock().lock();
        try {
            hiveMetaCache = metaCaches.get(resourceName);
        } finally {
            metaCachesLock.readLock().unlock();
        }
        if (hiveMetaCache != null) {
            return hiveMetaCache;
        }

        HiveMetaClient metaClient = getClient(resourceName);
        metaCachesLock.writeLock().lock();
        try {
            hiveMetaCache = metaCaches.get(resourceName);
            if (hiveMetaCache != null) {
                return hiveMetaCache;
            }

            hiveMetaCache = new HiveMetaCache(metaClient, executor);
            metaCaches.put(resourceName, hiveMetaCache);
            return hiveMetaCache;
        } finally {
            metaCachesLock.writeLock().unlock();
        }
    }

    public Table getTable(String resourceName, String dbName, String tableName) throws DdlException {
        HiveMetaClient client = getClient(resourceName);
        return client.getTable(dbName, tableName);
    }

    public ImmutableMap<PartitionKey, Long> getPartitionKeys(String resourceName, String dbName, String tableName,
                                                             List<Column> partColumns) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName);
        return metaCache.getPartitionKeys(dbName, tableName, partColumns);
    }

    public HivePartition getPartition(String resourceName, String dbName, String tableName,
                                      PartitionKey partitionKey) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName);
        return metaCache.getPartition(dbName, tableName, partitionKey);
    }

    public HiveTableStats getTableStats(String resourceName, String dbName, String tableName) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName);
        return metaCache.getTableStats(dbName, tableName);
    }

    public HivePartitionStats getPartitionStats(String resourceName, String dbName,
                                                String tableName, PartitionKey partitionKey) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName);
        return metaCache.getPartitionStats(dbName, tableName, partitionKey);
    }

    public ImmutableMap<String, HiveColumnStats> getTableLevelColumnStats(String resourceName, String dbName,
                                                                          String tableName,
                                                                          List<Column> partitionColumns,
                                                                          List<String> columnNames)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName);
        return metaCache.getTableLevelColumnStats(dbName, tableName, partitionColumns, columnNames);
    }

    public void refreshTableCache(String resourceName, String dbName, String tableName, List<Column> partColumns,
                                  List<String> columnNames) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName);
        metaCache.refreshTable(dbName, tableName, partColumns, columnNames);
    }

    public void refreshPartitionCache(String resourceName, String dbName, String tableName, List<String> partNames)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName);
        metaCache.refreshPartition(dbName, tableName, partNames);
    }

    public void clearCache(String resourceName, String dbName, String tableName) {
        try {
            HiveMetaCache metaCache = getMetaCache(resourceName);
            metaCache.clearCache(dbName, tableName);
        } catch (DdlException e) {

        }
    }

    public void clearCache(String resourceName) {
        metaCachesLock.writeLock().lock();
        try {
            metaCaches.remove(resourceName);
        } finally {
            metaCachesLock.writeLock().unlock();
        }

        metaClientsLock.writeLock().lock();
        try {
            metaClients.remove(resourceName);
        } finally {
            metaClientsLock.writeLock().unlock();
        }
    }
}
