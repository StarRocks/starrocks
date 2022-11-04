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
import com.starrocks.connector.hive.events.MetastoreNotificationFetchException;
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

    private enum Operator {
        UPDATE,
        DROP
    }

    private final String catalogName;
    private final IHiveMetastore metastore;
    private final Optional<CachingRemoteFileIO> remoteFileIO;
    private final ExecutorService executor;
    private final boolean isRecursive;

    // Record the latest synced event id when processing hive events
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

    public void refreshTable(String dbName, Table table) {
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        metastore.refreshTable(hmsTbl.getDbName(), hmsTbl.getTableName());
        refreshRemoteFiles(hmsTbl.getTableLocation(), Operator.UPDATE);
        if (isResourceMappingCatalog(catalogName) && table.isHiveTable()) {
            processSchemaChange(dbName, (HiveTable) table);
        }
    }

    public void refreshPartition(Table table, List<String> hivePartitionNames) {
        HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
        String hiveDbName = hmsTable.getDbName();
        String hiveTableName = hmsTable.getTableName();
        List<HivePartitionName> partitionNames = hivePartitionNames.stream()
                .map(partitionName -> HivePartitionName.of(hiveDbName, hiveTableName, partitionName))
                .collect(Collectors.toList());
        metastore.refreshPartition(partitionNames);

        if (remoteFileIO.isPresent()) {
            Map<String, Partition> partitions = metastore.getPartitionsByNames(hiveDbName, hiveTableName, hivePartitionNames);
            Optional<String> hudiBasePath = table.isHiveTable() ? Optional.empty() : Optional.of(hmsTable.getTableLocation());
            List<RemotePathKey> remotePathKeys = partitions.values().stream()
                    .map(partition -> RemotePathKey.of(partition.getFullPath(), isRecursive, hudiBasePath))
                    .collect(Collectors.toList());
            remotePathKeys.forEach(path -> remoteFileIO.get().updateRemoteFiles(path));
        }
    }

    private void processSchemaChange(String srDbName, HiveTable hiveTable) {
        boolean isSchemaChange = false;
        HiveTable resourceMappingCatalogTable = (HiveTable) metastore.getTable(
                hiveTable.getDbName(), hiveTable.getTableName());
        for (Column column : resourceMappingCatalogTable.getColumns()) {
            Column baseColumn = hiveTable.getColumn(column.getName());
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
            hiveTable.modifyTableSchema(srDbName, hiveTable.getName(), resourceMappingCatalogTable);
        }
    }

    private void refreshRemoteFiles(String tableLocation, Operator operator) {
        if (remoteFileIO.isPresent()) {
            List<RemotePathKey> presentPathKey = remoteFileIO.get().getPresentPathKeyInCache(tableLocation, isRecursive);
            List<Future<?>> futures = Lists.newArrayList();
            presentPathKey.forEach(pathKey -> {
                if (operator == Operator.UPDATE) {
                    futures.add(executor.submit(() -> remoteFileIO.get().updateRemoteFiles(pathKey)));
                } else {
                    futures.add(executor.submit(() -> remoteFileIO.get().invalidatePartition(pathKey)));
                }
            });

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("Failed to update remote files on [{}]", tableLocation, e);
                    throw new StarRocksConnectorException("Failed to update remote files", e);
                }
            }
        }
    }

    public boolean isTablePresent(HiveTableName tableName) {
        return ((CachingHiveMetastore) metastore).isTablePresent(tableName);
    }

    public boolean isPartitionPresent(HivePartitionName partitionName) {
        return ((CachingHiveMetastore) metastore).isPartitionPresent(partitionName);
    }

    public void refreshTableByEvent(HiveTable updatedHiveTable, HiveCommonStats commonStats, Partition partition) {
        ((CachingHiveMetastore) metastore).refreshTableByEvent(updatedHiveTable, commonStats, partition);
        refreshRemoteFiles(updatedHiveTable.getTableLocation(), Operator.UPDATE);
    }

    public void refreshPartitionByEvent(HivePartitionName hivePartitionName, HiveCommonStats commonStats, Partition partion) {
        ((CachingHiveMetastore) metastore).refreshPartitionByEvent(hivePartitionName, commonStats, partion);
        if (remoteFileIO.isPresent()) {
            RemotePathKey pathKey = RemotePathKey.of(partion.getFullPath(), isRecursive);
            remoteFileIO.get().updateRemoteFiles(pathKey);
        }
    }

    public void invalidateAll() {
        metastore.invalidateAll();
        remoteFileIO.ifPresent(CachingRemoteFileIO::invalidateAll);
    }

    public void invalidateTable(String dbName, String tableName) {
        String tableLocation = ((HiveMetaStoreTable) metastore.getTable(dbName, tableName)).getTableLocation();
        metastore.invalidateTable(dbName, tableName);
        remoteFileIO.ifPresent(ignore -> refreshRemoteFiles(tableLocation, Operator.DROP));
    }

    public void invalidatePartition(HivePartitionName partitionName) {
        Partition partition = metastore.getPartition(
                partitionName.getDatabaseName(), partitionName.getTableName(), partitionName.getPartitionValues());
        metastore.invalidatePartition(partitionName);
        if (remoteFileIO.isPresent()) {
            RemotePathKey pathKey = RemotePathKey.of(partition.getFullPath(), isRecursive);
            remoteFileIO.get().invalidatePartition(pathKey);
        }
    }

    public void setLastSyncedEventId(long lastSyncedEventId) {
        this.lastSyncedEventId = lastSyncedEventId;
    }

    public NotificationEventResponse getNextEventResponse(String catalogName, final boolean getAllEvents)
            throws MetastoreNotificationFetchException {
        if (lastSyncedEventId == -1) {
            lastSyncedEventId = metastore.getCurrentEventId();
            LOG.error("Last synced event id is null when pulling events on catalog [{}]", catalogName);
            return null;
        }

        long currentEventId = metastore.getCurrentEventId();
        if (currentEventId == lastSyncedEventId) {
            LOG.info("Event id not updated when pulling events on catalog [{}]", catalogName);
            return null;
        }
        return ((CachingHiveMetastore) metastore).getNextEventResponse(lastSyncedEventId, catalogName, getAllEvents);
    }
}
