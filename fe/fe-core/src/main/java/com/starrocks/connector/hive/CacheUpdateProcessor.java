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

import com.google.common.base.Strings;
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

import static com.starrocks.connector.ColumnTypeConverter.columnEquals;
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
        refreshRemoteFiles(hmsTbl.getTableLocation(), Operator.UPDATE, getExistPaths(hmsTbl));
        if (isResourceMappingCatalog(catalogName) && table.isHiveTable()) {
            processSchemaChange(dbName, (HiveTable) table);
        }
    }

    private List<String> getExistPaths(HiveMetaStoreTable table) {
        List<String> existPaths;
        String dbName = table.getDbName();
        String tblName = table.getTableName();

        if (table.isUnPartitioned()) {
            String path = metastore.getPartition(dbName, tblName, Lists.newArrayList()).getFullPath();
            existPaths = Lists.newArrayList(path.endsWith("/") ? path : path + "/");
        } else {
            List<String> partitionNames = metastore.getPartitionKeys(dbName, tblName);
            existPaths = metastore.getPartitionsByNames(dbName, tblName, partitionNames)
                    .values().stream()
                    .map(Partition::getFullPath)
                    .map(path -> path.endsWith("/") ? path : path + "/")
                    .collect(Collectors.toList());
        }
        return existPaths;
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
            if (!columnEquals(baseColumn, column)) {
                isSchemaChange = true;
                break;
            }
        }

        if (isSchemaChange) {
            hiveTable.modifyTableSchema(srDbName, hiveTable.getName(), resourceMappingCatalogTable);
        }
    }

    private void refreshRemoteFiles(String tableLocation, Operator operator, List<String> existPaths) {
        if (remoteFileIO.isPresent()) {
            List<RemotePathKey> presentPathKey = remoteFileIO.get().getPresentPathKeyInCache(tableLocation, isRecursive);
            List<Future<?>> futures = Lists.newArrayList();
            presentPathKey.forEach(pathKey -> {
                String pathWithSlash = pathKey.getPath().endsWith("/") ? pathKey.getPath() : pathKey.getPath() + "/";
                if (operator == Operator.UPDATE && existPaths.contains(pathWithSlash)) {
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
        refreshRemoteFiles(updatedHiveTable.getTableLocation(), Operator.UPDATE, getExistPaths(updatedHiveTable));
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

    public void invalidateTable(String dbName, String tableName, String originLocation) {
        String tableLocation;
        if (!Strings.isNullOrEmpty(originLocation)) {
            tableLocation = originLocation;
        } else {
            LOG.warn("table [{}.{}] origin location is null", dbName, tableName);
            try {
                tableLocation = ((HiveMetaStoreTable) metastore.getTable(dbName, tableName)).getTableLocation();
            } catch (Exception e) {
                LOG.error("Can't get table location from cache or hive metastore. ignore it");
                return;
            }
        }

        metastore.invalidateTable(dbName, tableName);

        if (remoteFileIO.isPresent()) {
            refreshRemoteFiles(tableLocation, Operator.DROP, Lists.newArrayList());
        }
    }

    public void invalidatePartition(HivePartitionName partitionName) {
        Partition partition;
        try {
            partition = metastore.getPartition(
                    partitionName.getDatabaseName(), partitionName.getTableName(), partitionName.getPartitionValues());
        } catch (Exception e) {
            LOG.warn("Failed to get partition {}. ignore it", partitionName);
            return;
        }

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
