// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;

import java.util.List;
import java.util.Map;

public interface IHiveMetastore {

    List<String> getAllDatabaseNames();

    List<String> getAllTableNames(String dbName);

    Database getDb(String dbName);

    Table getTable(String dbName, String tableName);

    List<String> getPartitionKeys(String dbName, String tableName);

    default Map<HivePartitionName, Partition> getCachedPartitions(List<HivePartitionName> hivePartitionNames) {
        return Maps.newHashMap();
    }

    default Map<HivePartitionName, Partition> getAllCachedPartitions() {
        return Maps.newHashMap();
    }

    Partition getPartition(String dbName, String tableName, List<String> partitionValues);

    Map<String, Partition> getPartitionsByNames(String dbName, String tableName, List<String> partitionNames);

    HivePartitionStats getTableStatistics(String dbName, String tableName);

    Map<String, HivePartitionStats> getPartitionStatistics(Table table, List<String> partitions);

    // return refreshed partitions in cache for partitioned table, return empty list for unpartitioned table
    default List<HivePartitionName> refreshTable(String hiveDbName, String hiveTblName, boolean onlyCachedPartitions) {
        return Lists.newArrayList();
    }

    default List<HivePartitionName> refreshTableBackground(String hiveDbName, String hiveTblName, boolean onlyCachedPartitions) {
        return Lists.newArrayList();
    }

    default void refreshPartition(List<HivePartitionName> partitionNames) {
    }

    default void invalidateAll() {
    }

    default void invalidateTable(String dbName, String tableName) {
    }

    default void invalidatePartition(HivePartitionName partitionName) {
    }

    default long getCurrentEventId() {
        return -1;
    }
}
