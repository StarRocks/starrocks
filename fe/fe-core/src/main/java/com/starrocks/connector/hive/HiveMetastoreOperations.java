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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.PartitionUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.starrocks.connector.PartitionUtil.executeInNewThread;

public class HiveMetastoreOperations {
    public static String BACKGROUND_THREAD_NAME_PREFIX = "background-get-partitions-statistics-";
    private final CachingHiveMetastore metastore;
    private final boolean enableCatalogLevelCache;

    public HiveMetastoreOperations(CachingHiveMetastore cachingHiveMetastore, boolean enableCatalogLevelCache) {
        this.metastore = cachingHiveMetastore;
        this.enableCatalogLevelCache = enableCatalogLevelCache;
    }

    public List<String> getAllDatabaseNames() {
        return metastore.getAllDatabaseNames();
    }

<<<<<<< HEAD
=======
    public void createDb(String dbName, Map<String, String> properties) {
        properties = properties == null ? new HashMap<>() : properties;
        String dbLocation = null;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equalsIgnoreCase(LOCATION_PROPERTY)) {
                try {
                    dbLocation = value;
                    URI uri = new Path(value).toUri();
                    FileSystem fileSystem = FileSystem.get(uri, hadoopConf);
                    fileSystem.exists(new Path(value));
                } catch (Exception e) {
                    LOG.error("Invalid location URI: {}", value, e);
                    throw new StarRocksConnectorException("Invalid location URI: %s. msg: %s", value, e.getMessage());
                }
            } else {
                throw new IllegalArgumentException("Unrecognized property: " + key);
            }
        }

        if (dbLocation == null && metastoreType == MetastoreType.GLUE) {
            throw new StarRocksConnectorException("The database location must be set when using glue. " +
                    "you could execute command like " +
                    "'CREATE DATABASE <db_name> properties('location'='s3://<bucket>/<your_db_path>')'");
        }

        metastore.createDb(dbName, properties);
    }

    public void dropDb(String dbName, boolean force) throws MetaNotFoundException {
        Database database;
        try {
            database = getDb(dbName);
        } catch (Exception e) {
            LOG.error("Failed to access database {}", dbName, e);
            throw new MetaNotFoundException("Failed to access database " + dbName);
        }

        if (database == null) {
            throw new MetaNotFoundException("Not found database " + dbName);
        }

        String dbLocation = database.getLocation();
        if (Strings.isNullOrEmpty(dbLocation)) {
            throw new MetaNotFoundException("Database location is empty");
        }
        boolean deleteData = false;
        try {
            deleteData = !FileSystem.get(URI.create(dbLocation), hadoopConf)
                    .listLocatedStatus(new Path(dbLocation)).hasNext();
        } catch (Exception e) {
            LOG.error("Failed to check database directory", e);
        }

        metastore.dropDb(dbName, deleteData);
    }

    public Database getDb(String dbName) {
        return metastore.getDb(dbName);
    }

>>>>>>> fc74a4dd60 ([Enhancement] Fix the checkstyle of semicolons (#33130))
    public List<String> getAllTableNames(String dbName) {
        return metastore.getAllTableNames(dbName);
    }

    public List<String> getPartitionKeys(String dbName, String tableName) {
        return metastore.getPartitionKeysByValue(dbName, tableName, HivePartitionValue.ALL_PARTITION_VALUES);
    }

    public List<String> getPartitionKeysByValue(String dbName, String tableName, List<Optional<String>> partitionValues) {
        return metastore.getPartitionKeysByValue(dbName, tableName, partitionValues);
    }

    public Database getDb(String dbName) {
        return metastore.getDb(dbName);
    }

    public Table getTable(String dbName, String tableName) {
        return metastore.getTable(dbName, tableName);
    }

    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        return metastore.getPartition(dbName, tableName, partitionValues);
    }

    public Map<String, Partition> getPartitionByPartitionKeys(Table table, List<PartitionKey> partitionKeys) {
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();
        List<String> partitionColumnNames = ((HiveMetaStoreTable) table).getPartitionColumnNames();
        List<String> partitionNames = partitionKeys.stream()
                .map(partitionKey -> PartitionUtil.toHivePartitionName(partitionColumnNames, partitionKey))
                .collect(Collectors.toList());

        return metastore.getPartitionsByNames(dbName, tblName, partitionNames);
    }

    public Map<String, Partition> getPartitionByNames(Table table, List<String> partitionNames) {
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();
        return metastore.getPartitionsByNames(dbName, tblName, partitionNames);
    }

    public HivePartitionStats getTableStatistics(String dbName, String tblName) {
        return metastore.getTableStatistics(dbName, tblName);
    }

    public Map<String, HivePartitionStats> getPartitionStatistics(Table table, List<String> partitionNames) {
        String catalogName = ((HiveMetaStoreTable) table).getCatalogName();
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();
        List<HivePartitionName> hivePartitionNames = partitionNames.stream()
                .map(partitionName -> HivePartitionName.of(dbName, tblName, partitionName))
                .peek(hivePartitionName -> checkState(hivePartitionName.getPartitionNames().isPresent(),
                        "partition name is missing"))
                .collect(Collectors.toList());

        Map<String, HivePartitionStats> partitionStats;
        if (enableCatalogLevelCache) {
            partitionStats = metastore.getPresentPartitionsStatistics(hivePartitionNames);
            if (partitionStats.size() == partitionNames.size()) {
                return partitionStats;
            }

            String backgroundThreadName = String.format(BACKGROUND_THREAD_NAME_PREFIX + "%s-%s-%s",
                    catalogName, dbName, tblName);
            executeInNewThread(backgroundThreadName, () -> metastore.getPartitionStatistics(table, partitionNames));
        } else {
            partitionStats = metastore.getPartitionStatistics(table, partitionNames);
        }

        return partitionStats;
    }

    public void invalidateAll() {
        metastore.invalidateAll();
    }
}
