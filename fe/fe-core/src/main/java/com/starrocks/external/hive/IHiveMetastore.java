package com.starrocks.external.hive;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.external.elasticsearch.HivePartitionStatistics;

import java.util.List;
import java.util.Map;

public interface IHiveMetastore {

    List<String> getAllDatabaseNames();

    List<String> getAllTableNames(String dbName);

    List<String> getPartitionKeys(String dbName, String tableName);

    Database getDb(String dbName);

    Table getTable(String dbName, String tableName);

    Partition getPartition(String dbName, String tblName, List<String> partitionValues);

    Map<String, Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames);

    HivePartitionStatistics getTableStatistics(String dbName, String tblName);

    Map<String, HivePartitionStatistics> getPartitionsStatistics(Table table, List<String> partitions);
}
