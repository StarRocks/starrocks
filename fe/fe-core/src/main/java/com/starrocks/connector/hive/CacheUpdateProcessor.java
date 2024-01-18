// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.Set;
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

    private final Map<BasePartitionInfo, Long> partitionUpdatedTimes;

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
        this.partitionUpdatedTimes = Maps.newHashMap();
        if (enableHmsEventsIncrementalSync) {
            trySyncEventId();
        }
    }

    private void trySyncEventId() {
        try {
            setLastSyncedEventId(metastore.getCurrentEventId());
        } catch (MetastoreNotificationFetchException e) {
            LOG.error("Sync event id on init get exception when pulling events on catalog [{}]", catalogName);
        }
    }

    public void refreshTable(String dbName, Table table, boolean onlyCachedPartitions) {
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        metastore.refreshTable(hmsTbl.getDbName(), hmsTbl.getTableName(), onlyCachedPartitions);
        refreshRemoteFiles(hmsTbl.getTableLocation(), Operator.UPDATE, getExistPaths(hmsTbl), onlyCachedPartitions);
        if (isResourceMappingCatalog(catalogName) && table.isHiveTable()) {
            processSchemaChange(dbName, (HiveTable) table);
        }
    }

    public void refreshTableBackground(Table table, boolean onlyCachedPartitions, ExecutorService executor) {
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        List<HivePartitionName> refreshPartitionNames = metastore.refreshTableBackground(
                hmsTbl.getDbName(), hmsTbl.getTableName(), onlyCachedPartitions);

        if (refreshPartitionNames != null) {
            Map<BasePartitionInfo, Partition> updatedPartitions = getUpdatedPartitions(hmsTbl, refreshPartitionNames);
            if (!updatedPartitions.isEmpty()) {
                // update partition remote files cache
                List<String> updatedPaths = updatedPartitions.values().stream().map(Partition::getFullPath)
                        .map(path -> path.endsWith("/") ? path : path + "/")
                        .collect(Collectors.toList());
                refreshRemoteFilesBackground(hmsTbl.getTableLocation(), updatedPaths, onlyCachedPartitions, executor);

                LOG.info("{}.{}.{} partitions has updated, updated partition size is {}, " +
                                "refresh partition and file success", hmsTbl.getCatalogName(), hmsTbl.getDbName(),
                        hmsTbl.getTableName(), updatedPartitions.size());
            }

            // update partitionUpdatedTimes
            updatedPartitions.entrySet().stream().filter(entry -> entry.getValue().getModifiedTime() != 0).
                    forEach(entry -> partitionUpdatedTimes.put(entry.getKey(), entry.getValue().getModifiedTime()));
            Map<HivePartitionName, Partition> cachedPartitions = metastore.getAllCachedPartitions();
            partitionUpdatedTimes.keySet().removeIf(basePartitionInfo -> !cachedPartitions.containsKey(
                    HivePartitionName.of(basePartitionInfo.dbName, basePartitionInfo.tableName,
                            basePartitionInfo.partitionName)));
        }
    }

    public Set<HiveTableName> getCachedTableNames() {
        return ((CachingHiveMetastore) metastore).getCachedTableNames();
    }

    private Map<BasePartitionInfo, Partition> getUpdatedPartitions(HiveMetaStoreTable table,
                                                                   List<HivePartitionName> refreshPartitionNames) {
        String dbName = table.getDbName();
        String tblName = table.getTableName();

        Map<BasePartitionInfo, Partition> toCheckUpdatedPartitionInfoMap = Maps.newHashMap();
        if (table.isUnPartitioned()) {
            Partition partition = metastore.getPartition(dbName, tblName, Lists.newArrayList());
            BasePartitionInfo partitionInfo = new BasePartitionInfo(dbName, tblName, tblName);
            toCheckUpdatedPartitionInfoMap.put(partitionInfo, partition);
        } else {
            Map<HivePartitionName, Partition> partitions = metastore.getCachedPartitions(refreshPartitionNames);
            for (Map.Entry<HivePartitionName, Partition> partitionEntry : partitions.entrySet()) {
                Optional<String> partitionName = partitionEntry.getKey().getPartitionNames();
                partitionName.ifPresent(s -> toCheckUpdatedPartitionInfoMap.put(new BasePartitionInfo(dbName, tblName, s),
                                partitionEntry.getValue()));
            }
        }

        Map<BasePartitionInfo, Partition> updatedPartitions = Maps.newHashMap();
        for (Map.Entry<BasePartitionInfo, Partition> checkPartition : toCheckUpdatedPartitionInfoMap.entrySet()) {
            BasePartitionInfo checkPartitionKey = checkPartition.getKey();
            Partition partition = checkPartition.getValue();
            if (!partitionUpdatedTimes.containsKey(checkPartitionKey)) {
                updatedPartitions.put(checkPartitionKey, partition);
            } else {
                if (partitionUpdatedTimes.get(checkPartitionKey) != partition.getModifiedTime()) {
                    updatedPartitions.put(checkPartitionKey, partition);
                }
            }
        }

        return updatedPartitions;
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

    private void refreshRemoteFilesBackground(String tableLocation, List<String> updatePaths,
                                              boolean onlyCachedPartitions, ExecutorService refreshExecutor) {
        if (remoteFileIO.isPresent()) {
            List<RemotePathKey> presentPathKey = updatePaths.stream().map(path -> RemotePathKey.of(path, isRecursive))
                    .collect(Collectors.toList());
            if (onlyCachedPartitions) {
                List<RemotePathKey> cachedPathKey = remoteFileIO.get().getPresentPathKeyInCache(tableLocation,
                        isRecursive);
                presentPathKey = cachedPathKey.stream().filter(pathKey -> {
                    String pathWithSlash = pathKey.getPath().endsWith("/") ? pathKey.getPath() : pathKey.getPath() + "/";
                    return updatePaths.contains(pathWithSlash);
                }).collect(Collectors.toList());
            }

            refreshRemoteFilesImpl(tableLocation, presentPathKey, Lists.newArrayList(), refreshExecutor);
        }
    }

    private void refreshRemoteFiles(String tableLocation, Operator operator, List<String> existPaths,
                                    boolean onlyCachedPartitions) {
        if (remoteFileIO.isPresent()) {
            List<RemotePathKey> presentPathKey;
            if (onlyCachedPartitions) {
                presentPathKey = remoteFileIO.get().getPresentPathKeyInCache(tableLocation, isRecursive);
            } else {
                presentPathKey = existPaths.stream()
                        .map(path -> RemotePathKey.of(path, isRecursive))
                        .collect(Collectors.toList());
            }
            List<RemotePathKey> updateKeys = Lists.newArrayList();
            List<RemotePathKey> invalidateKeys = Lists.newArrayList();
            presentPathKey.forEach(pathKey -> {
                String pathWithSlash = pathKey.getPath().endsWith("/") ? pathKey.getPath() : pathKey.getPath() + "/";
                if (operator == Operator.UPDATE && existPaths.contains(pathWithSlash)) {
                    updateKeys.add(pathKey);
                } else {
                    invalidateKeys.add(pathKey);
                }
            });

            refreshRemoteFilesImpl(tableLocation, updateKeys, invalidateKeys, executor);
        }
    }

    private void refreshRemoteFilesImpl(String tableLocation, List<RemotePathKey> updateKeys,
                                        List<RemotePathKey> invalidateKeys,
                                        ExecutorService refreshExecutor) {
        List<Future<?>> futures = Lists.newArrayList();
        updateKeys.forEach(pathKey -> futures.add(refreshExecutor.submit(() ->
                remoteFileIO.get().updateRemoteFiles(pathKey))));
        invalidateKeys.forEach(pathKey -> futures.add(refreshExecutor.submit(() ->
                remoteFileIO.get().invalidatePartition(pathKey))));

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Failed to update remote files on [{}]", tableLocation, e);
                throw new StarRocksConnectorException("Failed to update remote files", e);
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
        refreshRemoteFiles(updatedHiveTable.getTableLocation(), Operator.UPDATE, getExistPaths(updatedHiveTable), true);
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
            refreshRemoteFiles(tableLocation, Operator.DROP, Lists.newArrayList(), true);
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

    private static class BasePartitionInfo {
        private String dbName;
        private String tableName;
        private String partitionName;

        public BasePartitionInfo(String dbName, String tableName, String partitionName) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionName = partitionName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BasePartitionInfo)) {
                return false;
            }
            BasePartitionInfo that = (BasePartitionInfo) o;
            return Objects.equal(dbName, that.dbName) &&
                    Objects.equal(tableName, that.tableName) &&
                    Objects.equal(partitionName, that.partitionName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(dbName, tableName, partitionName);
        }
    }
    
}
