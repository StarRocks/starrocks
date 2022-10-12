// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.HiveMetaStoreTableInfo;
import com.starrocks.catalog.HiveResource;
import com.starrocks.catalog.HudiResource;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Resource.ResourceType;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.starrocks.external.HiveMetaStoreTableUtils.isInternalCatalog;

public class HiveRepository {
    // hiveResourceName => HiveMetaClient
    Map<String, HiveMetaClient> metaClients = Maps.newHashMap();
    ReadWriteLock metaClientsLock = new ReentrantReadWriteLock();

    // hiveResourceName => HiveMetaCache
    Map<String, HiveMetaCache> metaCaches = Maps.newHashMap();
    ReadWriteLock metaCachesLock = new ReentrantReadWriteLock();

    HiveExternalTableCounter counter = new HiveExternalTableCounter();

    Executor executor = Executors.newFixedThreadPool(100,
            new ThreadFactoryBuilder().setNameFormat("hive-metastore-refresh-%d").build());

    private static final Logger LOG = LogManager.getLogger(HiveRepository.class);
    private final ExecutorService partitionDaemonExecutor =
            ThreadPoolManager.newDaemonFixedThreadPool(Config.hive_meta_load_concurrency,
                    Integer.MAX_VALUE, "hive-meta-concurrency-pool", true);

    public HiveMetaClient getClient(String resourceName) throws DdlException {
        boolean isInternalCatalog = isInternalCatalog(resourceName);
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
            if (isInternalCatalog && resource.getType() != ResourceType.HIVE &&
                    resource.getType() != ResourceType.HUDI) {
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

            hiveMetaCache = new HiveMetaCache(metaClient, executor, resourceName);
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

    public ImmutableMap<PartitionKey, Long> getPartitionKeys(HiveMetaStoreTableInfo hmsTable) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(hmsTable.getResourceName());
        return metaCache.getPartitionKeys(hmsTable);
    }

    public List<HivePartition> getPartitions(HiveMetaStoreTableInfo hmsTable, List<PartitionKey> partitionKeys)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(hmsTable.getResourceName());
        List<Future<HivePartition>> futures = Lists.newArrayList();
        for (PartitionKey partitionKey : partitionKeys) {
            Future<HivePartition> future = partitionDaemonExecutor
                    .submit(() -> metaCache.getPartition(hmsTable, partitionKey));
            futures.add(future);
        }
        List<HivePartition> result = Lists.newArrayList();
        for (Future<HivePartition> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("get table {}.{} partition meta info failed.", hmsTable.getDb(), hmsTable.getTable(), e);
                throw new DdlException(e.getMessage());
            }
        }
        return result;
    }

    public HiveTableStats getTableStats(String resourceName, String dbName, String tableName) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(resourceName);
        return metaCache.getTableStats(dbName, tableName);
    }

    public List<HivePartitionStats> getPartitionsStats(HiveMetaStoreTableInfo hmsTable,
                                                       List<PartitionKey> partitionKeys) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(hmsTable.getResourceName());
        List<Future<HivePartitionStats>> futures = Lists.newArrayList();
        for (PartitionKey partitionKey : partitionKeys) {
            Future<HivePartitionStats> future = partitionDaemonExecutor.
                    submit(() -> metaCache.getPartitionStats(hmsTable, partitionKey));
            futures.add(future);
        }
        List<HivePartitionStats> result = Lists.newArrayList();
        for (Future<HivePartitionStats> future : futures) {
            try {
                result.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("get table {}.{} partition stats meta info failed.", hmsTable.getDb(), hmsTable.getTable(), e);
                throw new DdlException(e.getMessage());
            }
        }

        Map<PartitionKey, HivePartitionStats> partitionStatsMaps = Maps.newHashMap();
        for (int index = 0; index < partitionKeys.size(); ++index) {
            partitionStatsMaps.put(partitionKeys.get(index), result.get(index));
        }
        ConnectContext.get().getDumpInfo().getHMSTable(hmsTable.getResourceName(), hmsTable.getDb(),
                hmsTable.getTable()).addPartitionsStats(partitionStatsMaps);
        return result;
    }

    public ImmutableMap<String, HiveColumnStats> getTableLevelColumnStats(HiveMetaStoreTableInfo hmsTable)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(hmsTable.getResourceName());
        return metaCache.getTableLevelColumnStats(hmsTable);
    }

    public void refreshConnectorTable(String resource, String db, String table)
            throws DdlException, TException, ExecutionException {
        HiveMetaCache metaCache = getMetaCache(resource);
        metaCache.refreshConnectorTable(db, table);
    }

    public void refreshTableCache(HiveMetaStoreTableInfo hmsTable) throws DdlException {
        HiveMetaCache metaCache = getMetaCache(hmsTable.getResourceName());
        metaCache.refreshTable(hmsTable);
    }

    public void refreshPartitionCache(HiveMetaStoreTableInfo hmsTable, List<String> partNames)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(hmsTable.getResourceName());
        metaCache.refreshPartition(hmsTable, partNames);
    }

    public void refreshTableColumnStats(HiveMetaStoreTableInfo hmsTable)
            throws DdlException {
        HiveMetaCache metaCache = getMetaCache(hmsTable.getResourceName());
        metaCache.refreshColumnStats(hmsTable);
    }

    public void clearCache(HiveMetaStoreTableInfo hmsTable) {
        try {
            HiveMetaCache metaCache = getMetaCache(hmsTable.getResourceName());
            metaCache.clearCache(hmsTable);
        } catch (DdlException e) {
            LOG.warn("clean table {}.{} cache failed.", hmsTable.getDb(), hmsTable.getTable(), e);
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

    public HiveExternalTableCounter getCounter() {
        return counter;
    }

    public static class HiveExternalTableCounter {
        // Each hive table corresponds to the number counter of the StarRocks hive external table
        private final ConcurrentHashMap<HiveExternalTableIdentifier, Integer> hiveTblCounter = new ConcurrentHashMap<>();

        public int add(String resource, String database, String table) {
            HiveExternalTableIdentifier identifier = HiveExternalTableIdentifier.of(resource, database, table);
            return hiveTblCounter.compute(identifier, (k, v) -> v == null ? 1 : v + 1);
        }

        public int reduce(String resource, String database, String table) {
            HiveExternalTableIdentifier identifier = HiveExternalTableIdentifier.of(resource, database, table);
            return hiveTblCounter.compute(identifier, (k, v) -> v == null || v == 0 ? 0 : v - 1);
        }

        public int get(String resource, String database, String table) {
            HiveExternalTableIdentifier identifier = HiveExternalTableIdentifier.of(resource, database, table);
            return hiveTblCounter.getOrDefault(identifier, 0);
        }

        private static class HiveExternalTableIdentifier {
            String resourceName;
            String databaseName;
            String tableName;

            private HiveExternalTableIdentifier(String resourceName, String databaseName, String tableName) {
                this.resourceName = resourceName;
                this.databaseName = databaseName;
                this.tableName = tableName;
            }

            public static HiveExternalTableIdentifier of(String resourceName, String databaseName, String tableName) {
                return new HiveExternalTableIdentifier(resourceName, databaseName, tableName);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                HiveExternalTableIdentifier that = (HiveExternalTableIdentifier) o;

                return Objects.equals(resourceName, that.resourceName) &&
                        Objects.equals(databaseName, that.databaseName) &&
                        Objects.equals(tableName, that.tableName);
            }

            @Override
            public int hashCode() {
                return Objects.hash(resourceName, databaseName, tableName);
            }
        }
    }

}
