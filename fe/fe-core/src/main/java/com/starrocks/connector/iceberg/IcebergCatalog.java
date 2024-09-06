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
}
