// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.events.MetastoreEventType;
import com.starrocks.connector.hive.events.MetastoreNotificationFetchException;
import com.starrocks.server.GlobalStateMgr;
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
                                boolean isRecursive,
                                boolean enableHmsEventsIncrementalSync) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.remoteFileIO = remoteFileIO instanceof CachingRemoteFileIO
                ? Optional.of((CachingRemoteFileIO) remoteFileIO) : Optional.empty();
        this.executor = executor;
        this.isRecursive = isRecursive;
        if (enableHmsEventsIncrementalSync) {
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
            updateTableRemoteFileIO(hmsTable, "REFRESH");
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
                if (remotePathKeys.size() > 1) {
                    remotePathKeys.forEach(pathKey -> {
                        futures.add(executor.submit(() -> remoteFileIO.get().removeRemoteFile(pathKey)));
                    });
                    for (Future<?> future : futures) {
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            LOG.error("Failed to refresh remote files on [{}.{}], partitionNames: {}",
                                    hmsTable.getDbName(), hmsTable.getTableName(), hivePartitionNames);
                            throw new StarRocksConnectorException("Failed to refresh remote files", e);
                        }
                    }
                } else {
                    remotePathKeys.forEach(pathKey -> {
                        remoteFileIO.get().removeRemoteFile(pathKey);
                    });
                }
            } else {
                if (remotePathKeys.size() > 1) {
                    remotePathKeys.forEach(pathKey -> {
                        futures.add(executor.submit(() -> remoteFileIO.get().updateRemoteFiles(pathKey)));
                    });
                    for (Future<?> future : futures) {
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            LOG.error("Failed to refresh remote files on [{}.{}], partitionNames: {}",
                                    hmsTable.getDbName(), hmsTable.getTableName(), hivePartitionNames);
                            throw new StarRocksConnectorException("Failed to refresh remote files", e);
                        }
                    }
                } else {
                    remotePathKeys.forEach(pathKey -> {
                        remoteFileIO.get().updateRemoteFiles(pathKey);
                    });
                }
            }
        }
    }

    public NotificationEventResponse getNextEventResponse(String catalogName, final boolean getAllEvents)
            throws MetastoreNotificationFetchException {
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
        return ((CachingHiveMetastore) metastore).getNextEventResponse(lastSyncedEventId, catalogName, getAllEvents);
    }

    public boolean existIncache(MetastoreEventType eventType, Object key) {
        return ((CachingHiveMetastore) metastore).existInCache(eventType, key);
    }

    public void refreshCacheByEvent(MetastoreEventType eventType, HiveTableName hiveTableName,
                                    HivePartitionName hivePartitionName,
                                    HiveCommonStats commonStats, Partition partion, HiveTable hiveTable) {
        ((CachingHiveMetastore) metastore).refreshCacheByEvent(eventType, hiveTableName,
                hivePartitionName, commonStats, partion, hiveTable);
        updateRemoteFilebyEvent(eventType, hiveTableName, hivePartitionName, hiveTable);
    }

    public void updateRemoteFilebyEvent(MetastoreEventType eventType, HiveTableName hiveTableName,
                                        HivePartitionName hivePartitionName, HiveTable hiveTable) {
        switch (eventType) {
            case ALTER_TABLE:
                updateTableRemoteFileIO(hiveTable, "ALTER");
                break;
            case ALTER_PARTITION:
                updatePartitionRemoteFileIO(hiveTable,
                        Lists.newArrayList(hivePartitionName.getPartitionNames().get()), hiveTable, "ALTER");
                break;
            case DROP_PARTITION:
                updatePartitionRemoteFileIO(hiveTable,
                        Lists.newArrayList(hivePartitionName.getPartitionNames().get()), hiveTable, "DROP");
                break;
            case DROP_TABLE:
                updateTableRemoteFileIO(hiveTable, "DROP");
                break;
            case INSERT:
                if (hiveTable.isUnPartitioned()) {
                    updateTableRemoteFileIO(hiveTable, "ALTER");
                } else {
                    updatePartitionRemoteFileIO(hiveTable, hivePartitionName.getPartitionValues(), hiveTable, "ALTER");
                }
                break;
            default:
                return;
        }
    }

    public void invalidateAll() {
        metastore.invalidateAll();
        remoteFileIO.ifPresent(CachingRemoteFileIO::invalidateAll);
    }
}
