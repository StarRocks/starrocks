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
import com.starrocks.catalog.Table;

import java.util.List;
import java.util.Map;

public interface IHiveMetastore {

    List<String> getAllDatabaseNames();

    List<String> getAllTableNames(String dbName);

    Database getDb(String dbName);

    Table getTable(String dbName, String tableName);

    List<String> getPartitionKeys(String dbName, String tableName);

    Partition getPartition(String dbName, String tableName, List<String> partitionValues);

    Map<String, Partition> getPartitionsByNames(String dbName, String tableName, List<String> partitionNames);

    HivePartitionStats getTableStatistics(String dbName, String tableName);

    Map<String, HivePartitionStats> getPartitionStatistics(Table table, List<String> partitions);

    default void refreshTable(String hiveDbName, String hiveTblName, boolean onlyCachedPartitions) {
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
