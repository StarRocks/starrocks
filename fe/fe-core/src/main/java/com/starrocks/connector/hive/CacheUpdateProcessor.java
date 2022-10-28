// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
<<<<<<< HEAD
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemotePathKey;
=======
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events)
import com.starrocks.connector.exception.StarRocksConnectorException;
<<<<<<< HEAD
import com.starrocks.connector.hive.events.MetastoreEventType;
import com.starrocks.connector.hive.events.MetastoreNotificationFetchException;
import com.starrocks.server.GlobalStateMgr;
=======
import com.starrocks.external.CachingRemoteFileIO;
import com.starrocks.external.RemoteFileIO;
import com.starrocks.external.RemotePathKey;
import com.starrocks.external.hive.CachingHiveMetastore;
import com.starrocks.external.hive.HivePartitionName;
import com.starrocks.external.hive.HiveTableName;
import com.starrocks.external.hive.IHiveMetastore;
import com.starrocks.external.hive.Partition;
import com.starrocks.external.hive.events.MetastoreEventType;
import com.starrocks.external.hive.events.MetastoreNotificationFetchException;
<<<<<<< HEAD
>>>>>>> bc43ed9ba (refactor hive meta incremental sync by events)
=======
import com.starrocks.server.GlobalStateMgr;
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events)
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class CacheUpdateProcessor {
    private static final Logger LOG = LogManager.getLogger(CacheUpdateProcessor.class);

    private final String catalogName;
    private final IHiveMetastore metastore;
    private final Optional<CachingRemoteFileIO> remoteFileIO;
    private final ExecutorService executor;
    private final boolean isRecursive;

    // catalog => syncedEventId
    // Record the latest catalog event id when processed hive events
    private long lastSyncedEventId = -1;

    public CacheUpdateProcessor(String catalogName,
                                IHiveMetastore metastore,
                                RemoteFileIO remoteFileIO,
                                ExecutorService executor,
                                boolean isRecursive) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.remoteFileIO = remoteFileIO instanceof CachingRemoteFileIO
                ? Optional.of((CachingRemoteFileIO) remoteFileIO) : Optional.empty();
        this.executor = executor;
        this.isRecursive = isRecursive;
        if (Config.enable_hms_events_incremental_sync) {
            setLastSyncedEventId(metastore.getCurrentEventId());
        }
    }

    public void setLastSyncedEventId(long lastSyncedEventId) {
        this.lastSyncedEventId = lastSyncedEventId;
    }

    public void refreshTable(String dbName, String tableName, Table table) {
        GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().getEventProcessorLock().writeLock().lock();
        try {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            metastore.refreshTable(hmsTable.getDbName(), hmsTable.getTableName());

<<<<<<< HEAD
<<<<<<< HEAD
            updateTableRemoteFileIO(hmsTable, "REFRESH");
=======
            if (remoteFileIO.isPresent()) {
                String tableLocation = hmsTable.getTableLocation();
                List<RemotePathKey> presentPathKey = remoteFileIO.get()
                        .getPresentPathKeyInCache(tableLocation, isRecursive);
                List<Future<?>> futures = Lists.newArrayList();
                presentPathKey.forEach(pathKey -> {
                    futures.add(executor.submit(() -> remoteFileIO.get().updateRemoteFiles(pathKey)));
                });

                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.error("Failed to update remote files on [{}.{}]", dbName, tableName);
                        throw new StarRocksConnectorException("Failed to update remote files", e);
                    }
                }
            }
>>>>>>> bc43ed9ba (refactor hive meta incremental sync by events)
=======
            updateTableRemoteFileIO(hmsTable, "REFRESH");
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events)

            boolean isSchemaChange = false;
            if (isResourceMappingCatalog(catalogName) && table.isHiveTable()) {
                HiveTable resourceMappingCatalogTable = (HiveTable) metastore.getTable(
                        hmsTable.getDbName(), hmsTable.getTableName());
                for (Column column : resourceMappingCatalogTable.getColumns()) {
                    Column baseColumn = table.getColumn(column.getName());
                    if (baseColumn == null) {
                        isSchemaChange = true;
                        break;
                    }
                    if (!baseColumn.equals(column)) {
                        isSchemaChange = true;
                        break;
                    }
                }

                if (isSchemaChange) {
                    ((HiveTable) table).modifyTableSchema(dbName, tableName, resourceMappingCatalogTable);
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().getEventProcessorLock().writeLock().unlock();
        }
    }

    public void refreshPartition(Table table, List<String> hivePartitionNames) {
        GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().getEventProcessorLock().writeLock().lock();
        try {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            String hiveDbName = hmsTable.getDbName();
            String hiveTableName = hmsTable.getTableName();
            List<HivePartitionName> partitionNames = hivePartitionNames.stream()
                    .map(partitionName -> HivePartitionName.of(hiveDbName, hiveTableName, partitionName))
                    .collect(Collectors.toList());

            metastore.refreshPartition(partitionNames);

            updatePartitionRemoteFileIO(table, hivePartitionNames, hmsTable, "REFRESH");
        } finally {
            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().getEventProcessorLock().writeLock().unlock();
        }
    }

    private void updateTableRemoteFileIO(HiveMetaStoreTable hmsTable, String type) {
        if (remoteFileIO.isPresent()) {
            String tableLocation = hmsTable.getTableLocation();
            List<RemotePathKey> presentPathKey = remoteFileIO.get()
                    .getPresentPathKeyInCache(tableLocation, isRecursive);
            List<Future<?>> futures = Lists.newArrayList();
            if ("drop".equalsIgnoreCase(type)) {
                presentPathKey.forEach(pathKey -> {
                    futures.add(executor.submit(() -> remoteFileIO.get().removeRemoteFile(pathKey)));
                });
            } else {
                presentPathKey.forEach(pathKey -> {
                    futures.add(executor.submit(() -> remoteFileIO.get().updateRemoteFiles(pathKey)));
                });
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("Failed to update remote files on [{}.{}]", hmsTable.getDbName(), hmsTable.getTableName());
                    throw new StarRocksConnectorException("Failed to update remote files", e);
                }
            }
        }
    }

    private void updatePartitionRemoteFileIO(Table table, List<String> hivePartitionNames,
                                             HiveMetaStoreTable hmsTable, String type) {
        if (remoteFileIO.isPresent()) {
            Map<String, Partition> partitions = metastore.getPartitionsByNames(
                    hmsTable.getDbName(), hmsTable.getTableName(), hivePartitionNames);
            Optional<String> hudiBasePath = table.isHiveTable() ? Optional.empty() : Optional.of(hmsTable.getTableLocation());
            List<RemotePathKey> remotePathKeys = partitions.values().stream()
                    .map(partition -> RemotePathKey.of(partition.getFullPath(), isRecursive, hudiBasePath))
                    .collect(Collectors.toList());

            List<Future<?>> futures = Lists.newArrayList();
            if ("drop".equalsIgnoreCase(type)) {
                remotePathKeys.forEach(pathKey -> {
                    futures.add(executor.submit(() -> remoteFileIO.get().removeRemoteFile(pathKey)));
                });
            } else {
                remotePathKeys.forEach(pathKey -> {
                    futures.add(executor.submit(() -> remoteFileIO.get().updateRemoteFiles(pathKey)));
                });
            }


            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("Failed to refresh remote files on [{}.{}], partitionNames: {}",
                            hmsTable.getDbName(), hmsTable.getTableName(), hivePartitionNames);
                    throw new StarRocksConnectorException("Failed to refresh remote files", e);
                }
            }
        }
    }

    public NotificationEventResponse getNextEventResponse(String catalogName, final boolean getAllEvents)
            throws MetastoreNotificationFetchException {
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events)
        if (lastSyncedEventId == -1) {
            lastSyncedEventId = metastore.getCurrentEventId();
            LOG.info("Last synced event id is null when pulling events on catalog [{}]", catalogName);
            return null;
        }

        long currentEventId = metastore.getCurrentEventId();
        if (currentEventId == lastSyncedEventId) {
            LOG.info("Event id not updated when pulling events on catalog [{}]", catalogName);
            return null;
        }
<<<<<<< HEAD
=======
>>>>>>> bc43ed9ba (refactor hive meta incremental sync by events)
=======
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events)
        return ((CachingHiveMetastore) metastore).getNextEventResponse(lastSyncedEventId, catalogName, getAllEvents);
    }

    public boolean existIncache(MetastoreEventType eventType, Object key) {
        return ((CachingHiveMetastore) metastore).existInCache(eventType, key);
    }

<<<<<<< HEAD
    public void refreshCacheByEvent(MetastoreEventType eventType, HiveTableName hiveTableName,
                                    HivePartitionName hivePartitionName,
                                    HiveCommonStats commonStats, Partition partion, HiveTable hiveTable) {
        ((CachingHiveMetastore) metastore).refreshCacheByEvent(eventType, hiveTableName,
                hivePartitionName, commonStats, partion, hiveTable);
        updateRemoteFilebyEvent(eventType, hiveTableName, hivePartitionName, hiveTable);
=======
    public void alterCacheByEvent(MetastoreEventType eventType, HiveTableName hiveTableName, HivePartitionName hivePartitionName,
                                  Map<String, String> params, StorageDescriptor sd, HiveTable hiveTable) {
<<<<<<< HEAD
        try {
            refreshMetaCacheLock.writeLock().lock();
            ((CachingHiveMetastore) metastore).alterCacheByEvent(
                    eventType, hiveTableName, hivePartitionName, params, sd, hiveTable);
            updateRemoteFilebyEvent(eventType, hiveTableName, hivePartitionName, hiveTable);
        } finally {
            refreshMetaCacheLock.writeLock().unlock();
        }
>>>>>>> bc43ed9ba (refactor hive meta incremental sync by events)
=======
        ((CachingHiveMetastore) metastore).alterCacheByEvent(eventType, hiveTableName,
                    hivePartitionName, params, sd, hiveTable);
        updateRemoteFilebyEvent(eventType, hiveTableName, hivePartitionName, hiveTable);
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events)
    }

    public void updateRemoteFilebyEvent(MetastoreEventType eventType, HiveTableName hiveTableName,
                                        HivePartitionName hivePartitionName, HiveTable hiveTable) {
        switch (eventType) {
            case ALTER_TABLE:
<<<<<<< HEAD
<<<<<<< HEAD
                updateTableRemoteFileIO(hiveTable, "ALTER");
                break;
            case ALTER_PARTITION:
                updatePartitionRemoteFileIO(hiveTable, hivePartitionName.getPartitionValues(), hiveTable, "ALTER");
                break;
            case DROP_PARTITION:
                updatePartitionRemoteFileIO(hiveTable, hivePartitionName.getPartitionValues(), hiveTable, "DROP");
                break;
            case DROP_TABLE:
                updateTableRemoteFileIO(hiveTable, "DROP");
                break;
            case INSERT:
                if (hiveTable.isUnPartitioned()) {
                    updateTableRemoteFileIO(hiveTable, "ALTER");
                } else {
                    updatePartitionRemoteFileIO(hiveTable, hivePartitionName.getPartitionValues(), hiveTable, "ALTER");
=======
                if (remoteFileIO.isPresent()) {
                    String tableLocation = hiveTable.getTableLocation();
                    List<RemotePathKey> presentPathKey = remoteFileIO.get()
                            .getPresentPathKeyInCache(tableLocation, isRecursive);
                    List<Future<?>> futures = Lists.newArrayList();
                    presentPathKey.forEach(pathKey -> {
                        futures.add(executor.submit(() -> remoteFileIO.get().updateRemoteFiles(pathKey)));
                    });

                    for (Future<?> future : futures) {
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            LOG.error("Failed to update remote files on [{}.{}]",
                                    hiveTableName.getDatabaseName(), hiveTableName.getTableName());
                            throw new StarRocksConnectorException("Failed to update remote files", e);
                        }
                    }
                }
                break;
            case ALTER_PARTITION:
                if (remoteFileIO.isPresent()) {
                    Map<String, Partition> partitions = metastore.getPartitionsByNames(
                            hiveTableName.getDatabaseName(), hiveTableName.getTableName(),
                            hivePartitionName.getPartitionValues());
                    Optional<String> hudiBasePath = hiveTable.isHiveTable() ?
                            Optional.empty() : Optional.of(hiveTable.getTableLocation());
                    List<RemotePathKey> remotePathKeys = partitions.values().stream()
                            .map(partition -> RemotePathKey.of(partition.getFullPath(), isRecursive, hudiBasePath))
                            .collect(Collectors.toList());

                    List<Future<?>> futures = Lists.newArrayList();
                    remotePathKeys.forEach(pathKey -> {
                        futures.add(executor.submit(() -> remoteFileIO.get().updateRemoteFiles(pathKey)));
                    });

                    for (Future<?> future : futures) {
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            LOG.error("Failed to update remote files by event on [{}.{}], partitionNames: {}",
                                    hiveTableName.getDatabaseName(), hiveTableName, hivePartitionName);
                            throw new StarRocksConnectorException("Failed to update remote files by event", e);
                        }
                    }
>>>>>>> bc43ed9ba (refactor hive meta incremental sync by events)
                }
                break;
            default:
                return;
=======
                updateTableRemoteFileIO(hiveTable, "ALTER");
                break;
            case ALTER_PARTITION:
                updatePartitionRemoteFileIO(hiveTable, hivePartitionName.getPartitionValues(), hiveTable, "ALTER");
                break;
            case DROP_PARTITION:
                updatePartitionRemoteFileIO(hiveTable, hivePartitionName.getPartitionValues(), hiveTable, "DROP");
            case DROP_TABLE:
                updateTableRemoteFileIO(hiveTable, "DROP");
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events)
        }
    }

    public void invalidateAll() {
        metastore.invalidateAll();
        remoteFileIO.ifPresent(CachingRemoteFileIO::invalidateAll);
    }
}
