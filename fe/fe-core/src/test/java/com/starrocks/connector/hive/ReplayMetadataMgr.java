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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.dump.HiveMetaStoreTableDumpInfo;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class ReplayMetadataMgr extends MetadataMgr {
    private static final List<RemoteFileInfo> MOCKED_FILES = ImmutableList.of(
            new RemoteFileInfo(null, ImmutableList.of(), null));
    private Map<String, Map<String, Map<String, HiveTableInfo>>> replayTableMap;
    private long idGen = 0;

    public ReplayMetadataMgr(LocalMetastore localMetastore,
                             ConnectorMgr connectorMgr,
                             Map<String, Map<String, Map<String, HiveMetaStoreTableDumpInfo>>> externalTableInfoMap,
                             Map<String, Map<String, ColumnStatistic>> identifyToColumnStats) {
        super(localMetastore, connectorMgr, new ConnectorTblMetaInfoMgr());
        init(externalTableInfoMap, identifyToColumnStats);
    }

    private void init(Map<String, Map<String, Map<String, HiveMetaStoreTableDumpInfo>>> externalTableInfoMap,
                      Map<String, Map<String, ColumnStatistic>> identifyToColumnStats) {
        replayTableMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Map<String, HiveMetaStoreTableDumpInfo>>> resourceEntry :
                externalTableInfoMap.entrySet()) {
            String resourceName = resourceEntry.getKey();
            String resourceMappingCatalogName = CatalogMgr.ResourceMappingCatalog
                    .getResourceMappingCatalogName(resourceName, "hive");
            replayTableMap.putIfAbsent(resourceMappingCatalogName, Maps.newHashMap());
            Map<String, Map<String, HiveTableInfo>> replayDbMap = replayTableMap.get(resourceMappingCatalogName);
            Map<String, Map<String, HiveMetaStoreTableDumpInfo>> externalDbMap = resourceEntry.getValue();

            for (Map.Entry<String, Map<String, HiveMetaStoreTableDumpInfo>> dbEntry : externalDbMap.entrySet()) {
                String dbName = dbEntry.getKey();
                replayDbMap.putIfAbsent(dbName, Maps.newHashMap());
                Map<String, HiveTableInfo> replayTableMap = replayDbMap.get(dbName);

                Map<String, HiveMetaStoreTableDumpInfo> externalTableMap = dbEntry.getValue();
                for (Map.Entry<String, HiveMetaStoreTableDumpInfo> tableEntry : externalTableMap.entrySet()) {
                    String tableName = tableEntry.getKey();
                    HiveMetaStoreTableDumpInfo hiveMetaStoreTableDumpInfo = tableEntry.getValue();
                    List<String> dataColNames = hiveMetaStoreTableDumpInfo.getDataColumnNames();
                    List<String> partColNames = hiveMetaStoreTableDumpInfo.getPartColumnNames();
                    HiveTable.Builder tableBuilder = HiveTable.builder()
                            .setId(idGen++)
                            .setTableName(tableName)
                            .setCatalogName(resourceMappingCatalogName)
                            .setResourceName(resourceName)
                            .setHiveDbName(dbName)
                            .setHiveTableName(tableName)
                            .setPartitionColumnNames(partColNames)
                            .setDataColumnNames(dataColNames)
                            .setFullSchema(mockColumns(partColNames, dataColNames))
                            .setTableLocation("")
                            .setCreateTime(System.currentTimeMillis());
                    List<String> partitionNames = hiveMetaStoreTableDumpInfo.getPartitionNames();

                    Map<String, ColumnStatistic> columnStatistics = identifyToColumnStats.get(dbName + "." + tableName);
                    Map<ColumnRefOperator, ColumnStatistic> columnStatisticMap = columnStatistics.entrySet().stream().collect(
                            toImmutableMap(entry -> new ColumnRefOperator((int) idGen++, Type.INT, entry.getKey(), false),
                                    Map.Entry::getValue));
                    double rowCount = hiveMetaStoreTableDumpInfo.getScanRowCount();
                    Statistics statistics = Statistics.builder()
                            .addColumnStatistics(columnStatisticMap)
                            .setOutputRowCount(rowCount)
                            .build();
                    HiveTableInfo hiveTableInfo = new HiveTableInfo(tableBuilder.build(), partitionNames,
                            statistics, Lists.newArrayList(MOCKED_FILES));
                    replayTableMap.putIfAbsent(tableName, hiveTableInfo);
                }
            }
        }

    }

    private List<Column> mockColumns(List<String> partitionColumns, List<String> dataColumns) {
        List<Column> res = dataColumns.stream().map(x -> new Column(x, Type.STRING)).collect(Collectors.toList());
        res.addAll(partitionColumns.stream().map(x -> new Column(x, Type.STRING)).collect(Collectors.toList()));
        return res;
    }

    public List<String> listPartitionNames(String catalogName, String dbName, String tableName) {
        return replayTableMap.get(catalogName).get(dbName).get(tableName).partitionNames;
    }

    public Database getDb(String catalogName, String dbName) {
        return new Database(idGen++, dbName);
    }

    public Table getTable(String catalogName, String dbName, String tblName) {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return super.getTable(catalogName, dbName, tblName);
        }

        if (!CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(catalogName)) {
            catalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(catalogName, "hive");
        }
        return replayTableMap.get(catalogName).get(dbName).get(tblName).table;
    }

    public Statistics getTableStatistics(OptimizerContext session,
                                         String catalogName,
                                         Table table,
                                         List<ColumnRefOperator> columns,
                                         List<PartitionKey> partitionKeys) {
        Statistics.Builder resStatistics = Statistics.builder();
        Map<ColumnRefOperator, ColumnStatistic> res = new HashMap<>();
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();
        Statistics statistics =  replayTableMap.get(catalogName).get(dbName).get(tblName).statistics;
        Map<ColumnRefOperator, ColumnStatistic> columnStatisticMap = statistics.getColumnStatistics();
        for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : columnStatisticMap.entrySet()) {
            for (ColumnRefOperator columnRefOperator : columns) {
                if (columnRefOperator.getName().equalsIgnoreCase(entry.getKey().getName())) {
                    res.put(columnRefOperator, entry.getValue());
                }
            }
        }
        resStatistics.addColumnStatistics(res);
        resStatistics.setOutputRowCount(statistics.getOutputRowCount());
        return resStatistics.build();
    }

    public List<RemoteFileInfo> getRemoteFileInfos(String catalogName, Table table, List<PartitionKey> partitionKeys) {
        return Lists.newArrayList(MOCKED_FILES);
    }

    private static class HiveTableInfo {
        public final com.starrocks.catalog.Table table;
        public final List<String> partitionNames;
        public final Statistics statistics;
        private final List<RemoteFileInfo> partitions;

        public HiveTableInfo(Table table,
                             List<String> partitionNames,
                             Statistics statistics,
                             List<RemoteFileInfo> partitions) {
            this.table = table;
            this.partitionNames = partitionNames;
            this.statistics = statistics;
            this.partitions = partitions;
        }
    }
}
