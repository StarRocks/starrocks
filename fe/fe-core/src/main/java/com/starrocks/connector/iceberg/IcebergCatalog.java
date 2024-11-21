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

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.memory.MemoryTrackable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.iceberg.StarRocksIcebergTableScan.newTableScanContext;

public interface IcebergCatalog extends MemoryTrackable {
    Logger DEFAULT_LOGGER = LogManager.getLogger(IcebergCatalog.class);
    String EMPTY_PARTITION_NAME = "";

    default Logger getLogger() {
        return DEFAULT_LOGGER;
    }

    IcebergCatalogType getIcebergCatalogType();

    List<String> listAllDatabases();

    default void createDb(String dbName, Map<String, String> properties) {
    }

    default void dropDb(String dbName) throws MetaNotFoundException {
    }

    Database getDB(String dbName);

    List<String> listTables(String dbName);

    default boolean createTable(String dbName,
                                String tableName,
                                Schema schema,
                                PartitionSpec partitionSpec,
                                String location,
                                Map<String, String> properties) {
        return false;
    }

    default boolean dropTable(String dbName, String tableName, boolean purge) {
        throw new StarRocksConnectorException("This catalog doesn't support dropping tables");
    }

    void renameTable(String dbName, String tblName, String newTblName) throws StarRocksConnectorException;

    Table getTable(String dbName, String tableName) throws StarRocksConnectorException;

    default boolean tableExists(String dbName, String tableName) throws StarRocksConnectorException {
        try {
            getTable(dbName, tableName);
            return true;
        } catch (NoSuchTableException e) {
            return false;
        }
    }

    default boolean createView(ConnectorViewDefinition connectorViewDefinition, boolean replace) {
        throw new StarRocksConnectorException("This catalog doesn't support creating views");
    }

    default boolean dropView(String dbName, String viewName) {
        throw new StarRocksConnectorException("This catalog doesn't support dropping views");
    }

    default View getView(String dbName, String viewName) {
        throw new StarRocksConnectorException("This catalog doesn't loading iceberg view");
    }

    default void deleteUncommittedDataFiles(List<String> fileLocations) {
    }

    default void refreshTable(String dbName, String tableName, ExecutorService refreshExecutor) {
    }

    default void invalidateCacheWithoutTable(CachingIcebergCatalog.IcebergTableName icebergTableName) {
    }

    default void invalidateCache(CachingIcebergCatalog.IcebergTableName icebergTableName) {
    }

    default StarRocksIcebergTableScan getTableScan(Table table, StarRocksIcebergTableScanContext srScanContext) {
        return new StarRocksIcebergTableScan(
                table,
                table.schema(),
                newTableScanContext(table),
                srScanContext);
    }

    default String defaultTableLocation(String dbName, String tableName) {
        return "";
    }

    default Map<String, Object> loadNamespaceMetadata(String dbName) {
        return new HashMap<>();
    }

    default Map<String, Long> estimateCount() {
        return new HashMap<>();
    }

    default List<Pair<List<Object>, Long>> getSamples() {
        return new ArrayList<>();
    }

    // --------------- partition APIs ---------------
    private Map<String, Partition> getPartitions(Table icebergTable, long snapshotId, ExecutorService executorService) {
        Map<String, Partition> partitionMap = Maps.newHashMap();
        PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils.
                createMetadataTableInstance(icebergTable, org.apache.iceberg.MetadataTableType.PARTITIONS);
        TableScan tableScan = partitionsTable.newScan();
        if (snapshotId != -1) {
            tableScan = tableScan.useSnapshot(snapshotId);
        }
        if (executorService != null) {
            tableScan = tableScan.planWith(executorService);
        }
        Logger logger = getLogger();

        // TODO: ideally we should know if table is partitioned under a snapshotId.
        // but currently we just did it in a very wild way.
        if (icebergTable.spec().isUnpartitioned()) {
            Partition partition = null;
            try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
                for (FileScanTask task : tasks) {
                    // partitionsTable Table schema :
                    // record_count,
                    // file_count,
                    // total_data_file_size_in_bytes,
                    // position_delete_record_count,
                    // position_delete_file_count,
                    // equality_delete_record_count,
                    // equality_delete_file_count,
                    // last_updated_at,
                    // last_updated_snapshot_id
                    CloseableIterable<StructLike> rows = task.asDataTask().rows();
                    for (StructLike row : rows) {
                        // Get the last updated time of the table according to the table schema
                        long lastUpdated = -1;
                        try {
                            lastUpdated = row.get(7, Long.class);
                        } catch (NullPointerException e) {
                            logger.error("The table [{}] snapshot [{}] has been expired", icebergTable.name(), snapshotId, e);
                        }
                        partition = new Partition(lastUpdated);
                        break;
                    }
                }
                if (partition == null) {
                    partition = new Partition(-1);
                }
                partitionMap.put(EMPTY_PARTITION_NAME, partition);
            } catch (IOException e) {
                throw new StarRocksConnectorException("Failed to get partitions for table: " + icebergTable.name(), e);
            }
        } else {
            // For partition table, we need to get all partitions from PartitionsTable.
            try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
                for (FileScanTask task : tasks) {
                    // partitionsTable Table schema :
                    // partition,
                    // spec_id,
                    // record_count,
                    // file_count,
                    // total_data_file_size_in_bytes,
                    // position_delete_record_count,
                    // position_delete_file_count,
                    // equality_delete_record_count,
                    // equality_delete_file_count,
                    // last_updated_at,
                    // last_updated_snapshot_id
                    CloseableIterable<StructLike> rows = task.asDataTask().rows();
                    for (StructLike row : rows) {
                        // Get the partition data/spec id/last updated time according to the table schema
                        StructProjection partitionData = row.get(0, StructProjection.class);
                        int specId = row.get(1, Integer.class);
                        PartitionSpec spec = icebergTable.specs().get(specId);

                        String partitionName =
                                PartitionUtil.convertIcebergPartitionToPartitionName(spec, partitionData);

                        long lastUpdated = -1;
                        try {
                            lastUpdated = row.get(9, Long.class);
                        } catch (NullPointerException e) {
                            logger.error("The table [{}.{}] snapshot [{}] has been expired", icebergTable.name(), partitionName,
                                    snapshotId, e);
                        }
                        Partition partition = new Partition(lastUpdated);
                        partitionMap.put(partitionName, partition);
                    }
                }
            } catch (IOException e) {
                throw new StarRocksConnectorException("Failed to get partitions for table: " + icebergTable.name(), e);
            }
        }
        return partitionMap;
    }

    default Map<String, Partition> getPartitions(String dbName, String tableName, long snapshotId,
                                                 ExecutorService executorService) {
        Table icebergTable = getTable(dbName, tableName);
        return getPartitions(icebergTable, snapshotId, executorService);
    }

    default List<String> listPartitionNames(String dbName, String tableName, long snapshotId, ExecutorService executorService) {
        Table icebergTable = getTable(dbName, tableName);
        Map<String, Partition> partitionMap = getPartitions(icebergTable, snapshotId, executorService);
        if (icebergTable.spec().isUnpartitioned()) {
            return List.of();
        } else {
            return new ArrayList<>(partitionMap.keySet());
        }
    }

    default List<Partition> getPartitionsByNames(String dbName, String tableName, long snapshotId,
                                                 ExecutorService executorService,
                                                 List<String> partitionNames) {
        Table icebergTable = getTable(dbName, tableName);
        Map<String, Partition> partitionMap = getPartitions(icebergTable, snapshotId, executorService);
        if (icebergTable.spec().isUnpartitioned()) {
            return List.of(partitionMap.get(EMPTY_PARTITION_NAME));
        } else {
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
            partitionNames.forEach(partitionName -> partitions.add(partitionMap.get(partitionName)));
            return partitions.build();
        }
    }
}
