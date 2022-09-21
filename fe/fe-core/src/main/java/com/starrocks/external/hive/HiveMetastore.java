// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.Table;
import com.starrocks.external.Utils;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starrocks.external.hive.HiveMetastoreApiConverter.toHiveCommonStats;

public class HiveMetastore implements IHiveMetastore {
    private final HiveMetaClient client;
    private final String catalogName;

    public HiveMetastore(HiveMetaClient client, String catalogName) {
        this.client = client;
        this.catalogName = catalogName;
    }

    @Override
    public List<String> getAllDatabaseNames() {
        return client.getAllDatabaseNames();
    }

    public List<String> getAllTableNames(String dbName) {
        return client.getAllTableNames(dbName);
    }

    @Override
    public Database getDb(String dbName) {
        org.apache.hadoop.hive.metastore.api.Database db = client.getDb(dbName);
        return HiveMetastoreApiConverter.toDatabase(db);
    }

    public Table getTable(String dbName, String tableName) {
        org.apache.hadoop.hive.metastore.api.Table table = client.getTable(dbName, tableName);
        return HiveMetastoreApiConverter.toHiveTable(table, catalogName);
    }

    public List<String> getPartitionKeys(String dbName, String tableName) {
        return client.getPartitionKeys(dbName, tableName);
    }

    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        StorageDescriptor sd;
        Map<String, String> params;
        if (partitionValues.size() > 0) {
            org.apache.hadoop.hive.metastore.api.Partition partition =
                    client.getPartition(HiveTableName.of(dbName, tblName), partitionValues);
            sd = partition.getSd();
            params = partition.getParameters();
        } else {
            org.apache.hadoop.hive.metastore.api.Table table = client.getTable(dbName, tblName);
            sd = table.getSd();
            params = table.getParameters();
        }

        return HiveMetastoreApiConverter.toPartition(sd, params);
    }

    public Map<String, Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames) {
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions =
                client.getPartitionsByNames(dbName, tblName, partitionNames);

        Map<String, List<String>> partitionNameToPartitionValues = partitionNames.stream()
                .collect(Collectors.toMap(Function.identity(), Utils::toPartitionValues));

        Map<List<String>, Partition> partitionValuesToPartition = partitions.stream()
                .collect(Collectors.toMap(
                        org.apache.hadoop.hive.metastore.api.Partition::getValues,
                        partition -> HiveMetastoreApiConverter.toPartition(partition.getSd(), partition.getParameters())));

        ImmutableMap.Builder<String, Partition> resultBuilder = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValues.entrySet()) {
            Partition partition = partitionValuesToPartition.get(entry.getValue());
            resultBuilder.put(entry.getKey(), partition);
        }
        return resultBuilder.build();
    }

    public HivePartitionStatistics getTableStatistics(String dbName, String tblName) {
        org.apache.hadoop.hive.metastore.api.Table table = client.getTable(dbName, tblName);
        HiveCommonStats commonStats = toHiveCommonStats(table.getParameters());
        long totalRowNums = commonStats.getRowNums();
        if (totalRowNums == -1) {
            return HivePartitionStatistics.empty();
        }

        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        List<ColumnStatisticsObj> statisticsObjs = client.getTableColumnStats(dbName, tblName, dataColumns);
        Map<String, HiveColumnStatistics> columnStatistics =
                HiveMetastoreApiConverter.toSinglePartitionColumnStats(statisticsObjs, totalRowNums);
        return new HivePartitionStatistics(commonStats, columnStatistics);
    }

    public Map<String, HivePartitionStatistics> getPartitionsStatistics(Table table, List<String> partitionNames) {
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        String dbName = hmsTbl.getDbName();
        String tblName = hmsTbl.getTableName();
        List<String> dataColumns = hmsTbl.getDataColumnNames();
        List<String> partitionColumnNames = hmsTbl.getPartitionColumnNames();
        Map<String, Partition> partitions = getPartitionsByNames(hmsTbl.getDbName(), hmsTbl.getTableName(), partitionNames);

        Map<String, HiveCommonStats> partitionCommonStats = partitions.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> toHiveCommonStats(entry.getValue().getParameters())));

        Map<String, Long> partitionRowNums = partitionCommonStats.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRowNums()));

        ImmutableMap.Builder<String, HivePartitionStatistics> resultBuilder = ImmutableMap.builder();
        Map<String, List<ColumnStatisticsObj>> partitionNameToColumnStatsObj =
                client.getPartitionColumnStats(dbName, tblName, partitionColumnNames, dataColumns);

        Map<String, Map<String, HiveColumnStatistics>> partitionColumnStats = HiveMetastoreApiConverter
                .toPartitionColumnStatistics(partitionNameToColumnStatsObj, partitionRowNums);

        for (String partitionName : partitionCommonStats.keySet()) {
            HiveCommonStats commonStats = partitionCommonStats.get(partitionName);
            Map<String, HiveColumnStatistics> columnStatistics = partitionColumnStats
                    .getOrDefault(partitionName, ImmutableMap.of());
            resultBuilder.put(partitionName, new HivePartitionStatistics(commonStats, columnStatistics));
        }

        return resultBuilder.build();
    }
}
