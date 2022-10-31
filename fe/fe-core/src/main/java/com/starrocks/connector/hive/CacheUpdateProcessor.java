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
    }

    public void refreshTable(String dbName, String tableName, Table table) {
        HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
        metastore.refreshTable(hmsTable.getDbName(), hmsTable.getTableName());

        if (remoteFileIO.isPresent()) {
            String tableLocation = hmsTable.getTableLocation();
            List<RemotePathKey> presentPathKey = remoteFileIO.get().getPresentPathKeyInCache(tableLocation, isRecursive);
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

        boolean isSchemaChange = false;
        if (isResourceMappingCatalog(catalogName) && table.isHiveTable()) {
            HiveTable resourceMappingCatalogTable = (HiveTable) metastore.getTable(hmsTable.getDbName(), hmsTable.getTableName());
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

            List<Future<?>> futures = Lists.newArrayList();
            remotePathKeys.forEach(pathKey -> {
                futures.add(executor.submit(() -> remoteFileIO.get().updateRemoteFiles(pathKey)));
            });

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("Failed to refresh remote files on [{}.{}], partitionNames: {}",
                            hiveDbName, hiveTableName, hivePartitionNames);
                    throw new StarRocksConnectorException("Failed to refresh remote files", e);
                }
            }
        }
    }

    public void invalidateAll() {
        metastore.invalidateAll();
        remoteFileIO.ifPresent(CachingRemoteFileIO::invalidateAll);
    }
}
