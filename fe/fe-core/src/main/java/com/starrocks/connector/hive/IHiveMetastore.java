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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface IHiveMetastore {

    List<String> getAllDatabaseNames();

    void createDb(String dbName, Map<String, String> properties);

    void dropDb(String dbName, boolean deleteData);

    Database getDb(String dbName);

    List<String> getAllTableNames(String dbName);

    void createTable(String dbName, Table table);

    void dropTable(String dbName, String tableName);

    Table getTable(String dbName, String tableName);

    List<String> getPartitionKeysByValue(String dbName, String tableName, List<Optional<String>> partitionValues);

    default Map<HivePartitionName, Partition> getCachedPartitions(List<HivePartitionName> hivePartitionNames) {
        return Maps.newHashMap();
    }

    default Map<HivePartitionName, Partition> getAllCachedPartitions() {
        return Maps.newHashMap();
    }

    Partition getPartition(String dbName, String tableName, List<String> partitionValues);

    void addPartitions(String dbName, String tableName, List<HivePartitionWithStats> partitions);

    void dropPartition(String dbName, String tableName, List<String> partValues, boolean deleteData);

    boolean partitionExists(Table table, List<String> partitionValues);

    Map<String, Partition> getPartitionsByNames(String dbName, String tableName, List<String> partitionNames);

    HivePartitionStats getTableStatistics(String dbName, String tableName);

    Map<String, HivePartitionStats> getPartitionStatistics(Table table, List<String> partitions);

    void updateTableStatistics(String dbName, String tableName, Function<HivePartitionStats, HivePartitionStats> update);

    void updatePartitionStatistics(String dbName, String tableName, String partitionName,
                                   Function<HivePartitionStats, HivePartitionStats> update);

    // return refreshed partitions in cache for partitioned table, return empty list for unpartitioned table
    default List<HivePartitionName> refreshTable(String hiveDbName, String hiveTblName, boolean onlyCachedPartitions) {
        return Lists.newArrayList();
    }

    default boolean refreshView(String hiveDbName, String hiveTblName) {
        return true;
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

    default void invalidatePartitionKeys(HivePartitionValue partitionValue) {
    }

    default long getCurrentEventId() {
        return -1;
    }
}