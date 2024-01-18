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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.exception.MetaNotFoundException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.starrocks.connector.PartitionUtil.convertIcebergPartitionToPartitionName;

public interface IcebergCatalog {

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

    default List<String> listPartitionNames(String dbName, String tableName, ExecutorService executorService) {
        org.apache.iceberg.Table icebergTable = getTable(dbName, tableName);
        List<String> partitionNames = Lists.newArrayList();

        // all partitions specs are unpartitioned
        if (icebergTable.specs().values().stream().allMatch(PartitionSpec::isUnpartitioned)) {
            return partitionNames;
        }

        TableScan tableScan = icebergTable.newScan().planWith(executorService);
        List<FileScanTask> tasks = Lists.newArrayList(tableScan.planFiles());

        for (FileScanTask fileScanTask : tasks) {
            StructLike partition = fileScanTask.file().partition();
            partitionNames.add(convertIcebergPartitionToPartitionName(fileScanTask.spec(), partition));
        }
        return partitionNames;
    }

    default void deleteUncommittedDataFiles(List<String> fileLocations) {
    }

    default void refreshTable(String dbName, String tableName, ExecutorService refreshExecutor) {
    }

    default void invalidateCacheWithoutTable(CachingIcebergCatalog.IcebergTableName icebergTableName) {
    }

    default void invalidateCache(CachingIcebergCatalog.IcebergTableName icebergTableName) {
    }
}
