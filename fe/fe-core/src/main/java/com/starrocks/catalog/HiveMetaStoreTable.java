// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.starrocks.common.DdlException;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HiveTableStats;

import java.util.List;
import java.util.Map;

public interface HiveMetaStoreTable {
    // TODO(stephen): remove the dependencies on resource
    String getResourceName();

    String getCatalogName();

    String getDbName();

    String getTableName();

    List<String> getDataColumnNames();

    boolean isUnPartitioned();

    List<String> getPartitionColumnNames();

    HiveTableStats getTableStats() throws DdlException;

    Map<String, HiveColumnStats> getTableLevelColumnStats(List<String> columnNames) throws DdlException;

    List<HivePartition> getPartitions(List<PartitionKey> partitionKeys) throws DdlException;

    long getPartitionStatsRowCount(List<PartitionKey> partitions);

    List<Column> getPartitionColumns();

    Map<PartitionKey, Long> getPartitionKeys() throws DdlException;

    void refreshTableCache(String dbName, String tableName) throws DdlException;

    void refreshPartCache(List<String> partNames) throws DdlException;

    void refreshTableColumnStats() throws DdlException;
}
