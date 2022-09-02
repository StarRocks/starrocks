// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.HiveMetaStoreTableInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.DdlException;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HivePartitionStats;
import com.starrocks.external.hive.HiveRepository;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.sql.optimizer.dump.HiveMetaStoreTableDumpInfo;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ReplayHiveRepository extends HiveRepository  {
    // repository -> db -> tableName -> table
    private Map<String, Map<String, Map<String, HiveTableInfo>>> replayTableMap;

    public ReplayHiveRepository(Map<String, Map<String, Map<String, HiveMetaStoreTableDumpInfo>>> externalTableMap) {
        init(externalTableMap);
    }

    private void init(Map<String, Map<String, Map<String, HiveMetaStoreTableDumpInfo>>> externalTableInfoMap) {
        replayTableMap = Maps.newHashMap();
        for (Map.Entry<String, Map<String, Map<String, HiveMetaStoreTableDumpInfo>>> resourceEntry :
                externalTableInfoMap.entrySet()) {
            String resourceName = resourceEntry.getKey();
            replayTableMap.putIfAbsent(resourceName, Maps.newHashMap());
            Map<String, Map<String, HiveTableInfo>> replayDbMap = replayTableMap.get(resourceName);
            Map<String, Map<String, HiveMetaStoreTableDumpInfo>> externalDbMap = resourceEntry.getValue();

            for (Map.Entry<String, Map<String, HiveMetaStoreTableDumpInfo>> dbEntry : externalDbMap.entrySet()) {
                String dbName = dbEntry.getKey();
                replayDbMap.putIfAbsent(dbName, Maps.newHashMap());
                Map<String, HiveTableInfo> replayTableMap = replayDbMap.get(dbName);

                Map<String, HiveMetaStoreTableDumpInfo> externalTableMap = dbEntry.getValue();
                for (Map.Entry<String, HiveMetaStoreTableDumpInfo> tableEntry : externalTableMap.entrySet()) {
                    String tableName = tableEntry.getKey();
                    HiveMetaStoreTableDumpInfo hiveMetaStoreTableDumpInfo = tableEntry.getValue();

                    replayTableMap.putIfAbsent(tableName, new HiveTableInfo(dbName, tableName,
                            hiveMetaStoreTableDumpInfo.getPartitionKeys(), hiveMetaStoreTableDumpInfo.getPartitions(),
                            hiveMetaStoreTableDumpInfo.getPartColumnNames(), hiveMetaStoreTableDumpInfo.getDataColumnNames(),
                            hiveMetaStoreTableDumpInfo.getHiveTableStats(), hiveMetaStoreTableDumpInfo.getPartitionsStats(),
                            hiveMetaStoreTableDumpInfo.getTableLevelColumnStats()));
                }
            }
        }
    }

    private static class HiveTableInfo {
        public final String dbName;
        public final String tableName;
        public final Map<PartitionKey, Long> partitionKeyMap;
        public final Map<PartitionKey, HivePartition> partitionMap;
        public final List<String> partColumnNames;
        public final List<String> dataColumnNames;
        public final HiveTableStats tableStats;
        public final Map<PartitionKey, HivePartitionStats> partitionStats;
        public final Map<String, HiveColumnStats> columnStatsMap;
        public final Table table;

        public HiveTableInfo(String dbName, String tableName, Map<PartitionKey, Long> partitionKeyMap,
                             Map<PartitionKey, HivePartition> partitionMap, List<String> partColumnNames,
                             List<String> dataColumnNames, HiveTableStats tableStats,
                             Map<PartitionKey, HivePartitionStats> partitionStats) {
            this(dbName, tableName, partitionKeyMap, partitionMap, partColumnNames, dataColumnNames, tableStats,
                    partitionStats, Maps.newHashMap());
        }

        public HiveTableInfo(String dbName, String tableName, Map<PartitionKey, Long> partitionKeyMap,
                             Map<PartitionKey, HivePartition> partitionMap,
                             List<String> partColumnNames, List<String> dataColumnNames,
                             HiveTableStats tableStats, Map<PartitionKey, HivePartitionStats> partitionStats,
                             Map<String, HiveColumnStats> columnStatsMap) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionKeyMap = partitionKeyMap;
            this.partitionMap = partitionMap;
            this.partColumnNames = partColumnNames;
            this.dataColumnNames = dataColumnNames;
            this.tableStats = tableStats;
            this.partitionStats = partitionStats;

            List<FieldSchema> dataCols = Lists.newArrayList();
            List<FieldSchema> partCols = Lists.newArrayList();
            dataColumnNames.stream().map(col -> new FieldSchema(col, "string", null)).forEach(dataCols::add);
            partColumnNames.stream().map(col -> new FieldSchema(col, "string", null)).forEach(partCols::add);

            StorageDescriptor sd = new StorageDescriptor(dataCols, "", "",  "", false,
                    -1, null, Lists.newArrayList(), Lists.newArrayList(), Maps.newHashMap());
            Table table = new Table(tableName, dbName, null, 0, 0, 0,  sd,
                    partCols, Maps.newHashMap(), null, null, "EXTERNAL_TABLE");
            this.table = table;

            if (columnStatsMap.isEmpty()) {
                List<String> colNames = table.getSd().getCols().stream().map(FieldSchema::getName).
                        collect(Collectors.toList());
                columnStatsMap = colNames.stream().collect(Collectors.toMap(Function.identity(),
                        col -> HiveColumnStats.UNKNOWN));
            }
            this.columnStatsMap = columnStatsMap;
        }
    }

    @Override
    public Table getTable(String resourceName, String dbName, String tableName) throws DdlException {
        return replayTableMap.get(resourceName).get(dbName).get(tableName).table;
    }

    @Override
    public ImmutableMap<PartitionKey, Long> getPartitionKeys(HiveMetaStoreTableInfo hmsTable) throws DdlException {
        return ImmutableMap.copyOf(replayTableMap.get(hmsTable.getResourceName()).get(hmsTable.getDb()).
                get(hmsTable.getTable()).partitionKeyMap);

    }

    @Override
    public HiveTableStats getTableStats(String resourceName, String dbName, String tableName) throws DdlException {
        return replayTableMap.get(resourceName).get(dbName).get(tableName).tableStats;
    }

    @Override
    public List<HivePartitionStats> getPartitionsStats(HiveMetaStoreTableInfo hmsTable,
                                                       List<PartitionKey> partitionKeys) throws DdlException {
        List<HivePartitionStats> partitionStatsList = Lists.newArrayList();
        for (PartitionKey partitionKey : partitionKeys) {
            partitionStatsList.add(replayTableMap.get(hmsTable.getResourceName()).get(hmsTable.getDb()).
                    get(hmsTable.getTable()).partitionStats.get(partitionKey));
        }
        return partitionStatsList;
    }

    @Override
    public List<HivePartition> getPartitions(HiveMetaStoreTableInfo hmsTable, List<PartitionKey> partitionKeys) {
        List<HivePartition> partitionList = Lists.newArrayList();
        for (PartitionKey partitionKey : partitionKeys) {
            partitionList.add(replayTableMap.get(hmsTable.getResourceName()).get(hmsTable.getDb()).
                    get(hmsTable.getTable()).partitionMap.get(partitionKey));
        }
        return partitionList;
    }

    @Override
    public ImmutableMap<String, HiveColumnStats> getTableLevelColumnStats(HiveMetaStoreTableInfo hmsTable)
            throws DdlException {
        return ImmutableMap.copyOf(replayTableMap.get(hmsTable.getResourceName()).get(hmsTable.getDb()).
                get(hmsTable.getTable()).columnStatsMap);
    }
}
