// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.dump;

import com.starrocks.catalog.PartitionKey;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HivePartitionStats;
import com.starrocks.external.hive.HiveTableStats;

import java.util.List;
import java.util.Map;

public interface HiveMetaStoreTableDumpInfo {
    default String getType() {
        return "";
    }

    default void setPartitionKeys(Map<PartitionKey, Long> partitionKeys) {
    }

    default Map<PartitionKey, Long> getPartitionKeys() {
        return null;
    }

    default void setHiveTableStats(HiveTableStats hiveTableStats) {
    }

    default HiveTableStats getHiveTableStats() {
        return null;
    }

    default void addPartitionsStats(Map<PartitionKey, HivePartitionStats> hivePartitionStats) {
    }

    default Map<PartitionKey, HivePartitionStats> getPartitionsStats() {
        return null;
    }

    default void addTableLevelColumnStats(Map<String, HiveColumnStats> tableLevelColumnStats) {
    }

    default Map<String, HiveColumnStats> getTableLevelColumnStats() {
        return null;
    }

    default void addPartitions(Map<PartitionKey, HivePartition> partitions) {
    }

    default Map<PartitionKey, HivePartition> getPartitions() {
        return null;
    }

    default void setPartColumnNames(List<String> partColumnNames) {
    }

    default List<String> getPartColumnNames() {
        return null;
    }

    default void setDataColumnNames(List<String> dataColumnNames) {
    }

    default List<String> getDataColumnNames() {
        return null;
    }
}
