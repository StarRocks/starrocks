// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.DdlException;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HivePartitionStats;
import com.starrocks.external.hive.HiveTableStats;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveMetaStoreTableUtils {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreTableUtils.class);

    public static Map<String, HiveColumnStats> getTableLevelColumnStats(String resourceName,
                                                                        String db,
                                                                        String table,
                                                                        Map<String, Column> nameToColumn,
                                                                        List<String> columnNames,
                                                                        List<Column> partColumns) throws DdlException {
        // NOTE: Using allColumns as param to get column stats, we will get the best cache effect.
        List<String> allColumnNames = new ArrayList<>(nameToColumn.keySet());
        Map<String, HiveColumnStats> allColumnStats = GlobalStateMgr.getCurrentState().getHiveRepository()
                .getTableLevelColumnStats(resourceName, db, table, partColumns, allColumnNames);
        Map<String, HiveColumnStats> result = Maps.newHashMapWithExpectedSize(columnNames.size());
        for (String columnName : columnNames) {
            result.put(columnName, allColumnStats.get(columnName));
        }
        return result;
    }

    public static List<Column> getPartitionColumns(Map<String, Column> nameToColumn, List<String> partColumnNames) {
        List<Column> partColumns = Lists.newArrayList();
        for (String columnName : partColumnNames) {
            partColumns.add(nameToColumn.get(columnName));
        }
        return partColumns;
    }

    public static List<HivePartitionStats> getPartitionsStats(String resourceName,
                                                       String db,
                                                       String table,
                                                       List<PartitionKey> partitionKeys) throws DdlException {
        return GlobalStateMgr.getCurrentState().getHiveRepository()
                .getPartitionsStats(resourceName, db, table, partitionKeys);
    }

    public static HiveTableStats getTableStats(String resourceName,
                                               String db,
                                               String table) throws DdlException {
        return GlobalStateMgr.getCurrentState().getHiveRepository().getTableStats(resourceName, db, table);
    }

    public static List<HivePartition> getPartitions(String resourceName,
                                             String db,
                                             String table,
                                             List<PartitionKey> partitionKeys)
            throws DdlException {
        return GlobalStateMgr.getCurrentState().getHiveRepository()
                .getPartitions(resourceName, db, table, partitionKeys);
    }

    public static Map<PartitionKey, Long> getPartitionKeys(String resourceName,
                                                    String db,
                                                    String table,
                                                    List<Column> partColumns) throws DdlException {
        return GlobalStateMgr.getCurrentState().getHiveRepository()
                .getPartitionKeys(resourceName, db, table, partColumns);
    }

    public static long getPartitionStatsRowCount(String resourceName,
                                                 String db,
                                                 String table,
                                                 List<PartitionKey> partitions,
                                                 List<Column> partColumns) {
        if (partitions == null) {
            try {
                partitions = Lists.newArrayList(getPartitionKeys(resourceName, db, table, partColumns).keySet());
            } catch (DdlException e) {
                LOG.warn("Failed to get table {} partitions.", table, e);
                return -1;
            }
        }
        if (partitions.isEmpty()) {
            return 0;
        }

        long numRows = -1;

        List<HivePartitionStats> partitionsStats = Lists.newArrayList();
        try {
            partitionsStats = getPartitionsStats(resourceName, db, table, partitions);
        } catch (DdlException e) {
            LOG.warn("Failed to get table {} partitions stats.", table, e);
        }

        for (int i = 0; i < partitionsStats.size(); i++) {
            long partNumRows = partitionsStats.get(i).getNumRows();
            long partTotalFileBytes = partitionsStats.get(i).getTotalFileBytes();
            // -1: missing stats
            if (partNumRows > -1) {
                if (numRows == -1) {
                    numRows = 0;
                }
                numRows += partNumRows;
            } else {
                LOG.debug("Table {} partition {} stats is invalid. num rows: {}, total file bytes: {}",
                        table, partitions.get(i), partNumRows, partTotalFileBytes);
            }
        }
        return numRows;
    }
}
