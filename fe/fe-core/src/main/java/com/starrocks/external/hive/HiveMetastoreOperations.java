// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.external.Utils;
import org.apache.hadoop.hive.common.FileUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class HiveMetastoreOperations {
    private final CachingHiveMetastore metastore;

    public HiveMetastoreOperations(CachingHiveMetastore cachingHiveMetastore) {
        this.metastore = cachingHiveMetastore;
    }

    public List<String> getAllDatabaseNames() {
        return metastore.getAllDatabaseNames();
    }

    public List<String> getAllTableNames(String dbName) {
        return metastore.getAllTableNames(dbName);
    }

    public List<String> getPartitionKeys(String dbName, String tableName) {
        return metastore.getPartitionKeys(dbName, tableName);
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

    public Map<String, Partition> getPartitionByNames(Table table, List<PartitionKey> partitionKeys) {
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();
        List<String> partitionColumnNames = ((HiveMetaStoreTable) table).getPartitionColumnNames();
        List<String> partitionNames = partitionKeys.stream()
                .map(partitionKey ->
                        FileUtils.makePartName(partitionColumnNames, Utils.getPartitionValues(partitionKey)))
                .collect(Collectors.toList());

        return metastore.getPartitionsByNames(dbName, tblName, partitionNames);
    }

    public HivePartitionStatistics getTableStatistics(String dbName, String tblName) {
        return metastore.getTableStatistics(dbName, tblName);
    }

    public Map<String, HivePartitionStatistics> getPartitionStatistics(Table table, List<String> partitionNames) {
        String catalogName = ((HiveMetaStoreTable) table).getCatalogName();
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();
        List<NewHivePartitionName> hivePartitionNames = partitionNames.stream()
                .map(partitionName -> NewHivePartitionName.of(dbName, tblName, partitionName))
                .peek(hivePartitionName -> checkState(hivePartitionName.getPartitionNames().isPresent(),
                        "partition name is missing"))
                .collect(Collectors.toList());

        Map<String, HivePartitionStatistics> presentPartitionStatsInCache =
                metastore.getPresentPartitionsStatistics(hivePartitionNames);

        if (presentPartitionStatsInCache.size() == partitionNames.size()) {
            return presentPartitionStatsInCache;
        }

        String backgroundThreadName = String.format("background-get-partitions-statistics-%s-%s-%s",
                catalogName, dbName, tblName);
        Utils.executeInNewThread(backgroundThreadName, () -> metastore.getPartitionStatistics(table, partitionNames));

        return presentPartitionStatsInCache;
    }
}
