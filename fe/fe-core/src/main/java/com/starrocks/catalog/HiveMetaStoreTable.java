// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.starrocks.common.DdlException;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HiveTableStats;

import java.util.List;
import java.util.Map;

public interface HiveMetaStoreTable {

    HiveTableStats getTableStats() throws DdlException;

    Map<String, HiveColumnStats> getTableLevelColumnStats(List<String> columnNames) throws DdlException;

    List<HivePartition> getPartitions(List<PartitionKey> partitionKeys) throws DdlException;

    long getPartitionStatsRowCount(List<PartitionKey> partitions);

    List<Column> getPartitionColumns();

    Map<PartitionKey, Long> getPartitionKeys() throws DdlException;

    void refreshTableCache() throws DdlException;

    void refreshPartCache(List<String> partNames) throws DdlException;

    void refreshTableColumnStats() throws DdlException;
}
