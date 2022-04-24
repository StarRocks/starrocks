// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveResource;
import com.starrocks.catalog.HudiResource;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Resource.ResourceType;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    private static final Logger LOG = LogManager.getLogger(HiveRepository.class);
    private final ExecutorService partitionDaemonExecutor =
            ThreadPoolManager.newDaemonFixedThreadPool(Config.hive_meta_load_concurrency,
                    Integer.MAX_VALUE, "hive-meta-concurrency-pool", true);

    public HiveMetaClient getClient(String resourceName, boolean isInternalCatalog) throws DdlException {
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
            Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
            if (resource == null && isInternalCatalog) {
                throw new DdlException("get hive client failed, resource[" + resourceName + "] not exists");
            }

            if (isInternalCatalog && resource.getType() != ResourceType.HIVE && resource.getType() != ResourceType.HUDI) {
                throw new DdlException("resource [" + resourceName + "] is not hive/hudi resource");
            }

            if (!isInternalCatalog) {
                client = new HiveMetaClient(resourceName);
            } else {
                if (resource.getType() == ResourceType.HIVE) {
                    client = new HiveMetaClient(((HiveResource) resource).getHiveMetastoreURIs());
                } else if (resource.getType() == ResourceType.HUDI) {
                    client = new HiveMetaClient(((HudiResource) resource).getHiveMetastoreURIs());
                }
            }

            metaClients.put(resourceName, client);
        } finally {
            metaClientsLock.writeLock().unlock();
        }

        return client;
    }

    public HiveMetaCache getMetaCache(String resourceName, boolean isDefaultCatalog) throws DdlException {
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

        HiveMetaClient metaClient = getClient(resourceName, isDefaultCatalog);
        metaCachesLock.writeLock().lock();
        try {
            hiveMetaCache = metaCaches.get(resourceName);
            if (hiveMetaCache != null) {
                return hiveMetaCache;
            }

            hiveMetaCache = new HiveMetaCache(metaClient, executor, resourceName);
            metaCaches.put(resourceName, hiveMetaCache);
            return hiveMetaCache;
        } finally {
            metaCachesLock.writeLock().unlock();
        }
    }

    public Table getTable(String resourceName, String dbName, String tableName) throws DdlException {
        HiveMetaClient client = getClient(resourceName, true);
        return client.getTable(dbName, tableName);
    }

    public ImmutableMap<PartitionKey, Long> getPartitionKeys(String resourceName, String dbName, String tableName,
                                                             List<Column> partColumns) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        return metaCache.getPartitionKeys(dbName, tableName, partColumns);
    }

    public HivePartition getPartition(String resourceName, String dbName, String tableName,
                                      PartitionKey partitionKey) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        return metaCache.getPartition(dbName, tableName, partitionKey);
    }

    public List<HivePartition> getPartitions(String resourceName, String dbName, String tableName,
                                             List<PartitionKey> partitionKeys)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        List<Future<HivePartition>> futures = Lists.newArrayList();
        for (PartitionKey partitionKey : partitionKeys) {
            Future<HivePartition> future = partitionDaemonExecutor
                    .submit(() -> metaCache.getPartition(dbName, tableName, partitionKey));
            futures.add(future);
        }
        List<HivePartition> result = Lists.newArrayList();
        for (Future<HivePartition> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("get table {}.{} partition meta info failed.", dbName, tableName,  e);
                throw new DdlException(e.getMessage());
            }
        }
        return result;
    }

    public List<HivePartition> getHudiPartitions(String resourceName, String dbName, String tableName,
                                                 List<PartitionKey> partitionKeys)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        List<Future<HivePartition>> futures = Lists.newArrayList();
        for (PartitionKey partitionKey : partitionKeys) {
            Future<HivePartition> future = partitionDaemonExecutor
                    .submit(() -> metaCache.getHudiPartition(dbName, tableName, partitionKey));
            futures.add(future);
        }
        List<HivePartition> result = Lists.newArrayList();
        for (Future<HivePartition> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("Get table {}.{} partition meta info failed.", dbName, tableName,  e);
                throw new DdlException(e.getMessage());
            }
        }
        return result;
    }

    public HiveTableStats getTableStats(String resourceName, String dbName, String tableName) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        return metaCache.getTableStats(dbName, tableName);
    }

    public HivePartitionStats getPartitionStats(String resourceName, String dbName,
                                                String tableName, PartitionKey partitionKey) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        return metaCache.getPartitionStats(dbName, tableName, partitionKey);
    }

    public List<HivePartitionStats> getPartitionsStats(String resourceName, String dbName,
                                                String tableName, List<PartitionKey> partitionKeys) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        List<Future<HivePartitionStats>> futures = Lists.newArrayList();
        for (PartitionKey partitionKey : partitionKeys) {
            Future<HivePartitionStats> future = partitionDaemonExecutor.
                    submit(() -> metaCache.getPartitionStats(dbName, tableName, partitionKey));
            futures.add(future);
        }
        List<HivePartitionStats> result = Lists.newArrayList();
        for (Future<HivePartitionStats> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("get table {}.{} partition stats meta info failed.", dbName, tableName,  e);
                throw new DdlException(e.getMessage());
            }
        }
        return result;
    }

    public ImmutableMap<String, HiveColumnStats> getTableLevelColumnStats(String resourceName, String dbName,
                                                                          String tableName,
                                                                          List<Column> partitionColumns,
                                                                          List<String> columnNames)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        return metaCache.getTableLevelColumnStats(dbName, tableName, partitionColumns, columnNames);
    }

    public void refreshTableCache(String resourceName, String dbName, String tableName, List<Column> partColumns,
                                  List<String> columnNames) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        metaCache.refreshTable(dbName, tableName, partColumns, columnNames);
    }

    public void refreshPartitionCache(String resourceName, String dbName, String tableName, List<String> partNames)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        metaCache.refreshPartition(dbName, tableName, partNames);
    }

    public void refreshTableColumnStats(String resourceName, String dbName, String tableName, List<Column> partColumns,
                                        List<String> columnNames)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName, true);
        metaCache.refreshColumnStats(dbName, tableName, partColumns, columnNames);
    }

    public void clearCache(String resourceName, String dbName, String tableName) {
        try {
            HiveMetaCache metaCache = getMetaCache(resourceName, true);
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
