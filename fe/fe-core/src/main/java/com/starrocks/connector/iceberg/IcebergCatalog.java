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

import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.memory.MemoryTrackable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.view.View;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.starrocks.connector.PartitionUtil.convertIcebergPartitionToPartitionName;
import static org.apache.iceberg.StarRocksIcebergTableScan.newTableScanContext;

public interface IcebergCatalog extends MemoryTrackable {

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

    default List<String> listPartitionNames(String dbName, String tableName,  long snapshotId, ExecutorService executorService) {
        org.apache.iceberg.Table icebergTable = getTable(dbName, tableName);
        Set<String> partitionNames = Sets.newHashSet();

        if (icebergTable.specs().values().stream().allMatch(PartitionSpec::isUnpartitioned)) {
            return new ArrayList<>();
        }

        TableScan tableScan = icebergTable.newScan().planWith(executorService);
        try (CloseableIterable<FileScanTask> fileScanTaskIterable = tableScan.planFiles();
                CloseableIterator<FileScanTask> fileScanTaskIterator = fileScanTaskIterable.iterator()) {

            while (fileScanTaskIterator.hasNext()) {
                FileScanTask scanTask = fileScanTaskIterator.next();
                StructLike partition = scanTask.file().partition();
                String partitionName = convertIcebergPartitionToPartitionName(scanTask.spec(), partition);
                partitionNames.add(partitionName);
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException(String.format("Failed to list iceberg partition names %s.%s",
                    dbName, tableName), e);
        }

        return new ArrayList<>(partitionNames);
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
<<<<<<< HEAD
=======

    // --------------- partition APIs ---------------
    default Map<String, Partition> getPartitions(IcebergTable icebergTable, long snapshotId, ExecutorService executorService) {
        Table nativeTable = icebergTable.getNativeTable();
        Map<String, Partition> partitionMap = Maps.newHashMap();
        PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils.
                createMetadataTableInstance(nativeTable, MetadataTableType.PARTITIONS);
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
        if (nativeTable.spec().isUnpartitioned()) {
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
                            // It is a known issue but we do not hanle it right now. If the refresh frequency of 
                            // the materialized view is very high, an excessive number of error logs will be printed. 
                            // Therefore, only brief logs are printed now.
                            logger.error("The table [{}] snapshot [{}] has been expired", nativeTable.name(), snapshotId);
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
                throw new StarRocksConnectorException("Failed to get partitions for table: " + nativeTable.name(), e);
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
                        PartitionSpec spec = nativeTable.specs().get(specId);

                        String partitionName =
                                PartitionUtil.convertIcebergPartitionToPartitionName(spec, partitionData);

                        long lastUpdated = -1;
                        try {
                            lastUpdated = row.get(9, Long.class);
                        } catch (NullPointerException e) {
                            logger.error("The table [{}.{}] snapshot [{}] has been expired", nativeTable.name(), partitionName,
                                    snapshotId, e);
                        }
                        Partition partition = new Partition(lastUpdated);
                        partitionMap.put(partitionName, partition);
                    }
                }
            } catch (IOException e) {
                throw new StarRocksConnectorException("Failed to get partitions for table: " + nativeTable.name(), e);
            }
        }
        return partitionMap;
    }

    default List<String> listPartitionNames(IcebergTable icebergTable,
                                            ConnectorMetadatRequestContext requestContext,
                                            ExecutorService executorService) {
        Table nativeTable = icebergTable.getNativeTable();

        // Call public method so subclasses can override and optimize this method.
        Map<String, Partition> partitionMap = getPartitions(icebergTable, requestContext.getSnapshotId(), executorService);
        if (nativeTable.spec().isUnpartitioned()) {
            return List.of();
        } else {
            return new ArrayList<>(partitionMap.keySet());
        }
    }

    default List<Partition> getPartitionsByNames(IcebergTable icebergTable,
                                                 ExecutorService executorService,
                                                 List<String> partitionNames) {
        Table nativeTable = icebergTable.getNativeTable();
        long snapshotId = -1;
        if (nativeTable.currentSnapshot() != null) {
            snapshotId = nativeTable.currentSnapshot().snapshotId();
        }

        // Call public method so subclasses can override and optimize this method.
        Map<String, Partition> partitionMap = getPartitions(icebergTable, snapshotId, executorService);
        if (nativeTable.spec().isUnpartitioned()) {
            return List.of(partitionMap.get(EMPTY_PARTITION_NAME));
        } else {
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
            partitionNames.forEach(partitionName -> partitions.add(partitionMap.get(partitionName)));
            return partitions.build();
        }
    }
>>>>>>> 16e78bfba7 ([Enhancement] Simplify the exception log (#55862))
}
