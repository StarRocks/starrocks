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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.events.MetastoreNotificationFetchException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.starrocks.connector.hive.HiveMetastoreApiConverter.toHiveCommonStats;
import static com.starrocks.connector.hive.HiveMetastoreApiConverter.validateHiveTableType;

public class HiveMetastore implements IHiveMetastore {

    private static final Logger LOG = LogManager.getLogger(CachingHiveMetastore.class);
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
        StorageDescriptor sd = table.getSd();
        if (sd == null) {
            throw new StarRocksConnectorException("Table is missing storage descriptor");
        }

        if (!HiveMetastoreApiConverter.isHudiTable(table.getSd().getInputFormat())) {
            validateHiveTableType(table.getTableType());
            if (table.getTableType().equalsIgnoreCase("VIRTUAL_VIEW")) {
                return HiveMetastoreApiConverter.toHiveView(table, catalogName);
            } else {
                return HiveMetastoreApiConverter.toHiveTable(table, catalogName);
            }
        } else {
            return HiveMetastoreApiConverter.toHudiTable(table, catalogName);
        }
    }

    @Override
    public List<String> getPartitionKeysByValue(String dbName, String tableName, List<Optional<String>> partitionValues) {
        if (partitionValues.isEmpty()) {
            return client.getPartitionKeys(dbName, tableName);
        } else {
            List<String> partitionValuesStr = partitionValues.stream()
                    .map(v -> v.orElse("")).collect(Collectors.toList());
            return client.getPartitionKeysByValue(dbName, tableName, partitionValuesStr);
        }
    }

    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        StorageDescriptor sd;
        Map<String, String> params;
        if (partitionValues.size() > 0) {
            org.apache.hadoop.hive.metastore.api.Partition partition =
                    client.getPartition(dbName, tblName, partitionValues);
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
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions = new ArrayList<>();
        // fetch partitions by batch per RPC
        for (int start = 0; start < partitionNames.size(); start += Config.max_hive_partitions_per_rpc) {
            int end = Math.min(start + Config.max_hive_partitions_per_rpc, partitionNames.size());
            List<String> namesPerRPC = partitionNames.subList(start, end);
            List<org.apache.hadoop.hive.metastore.api.Partition> partsPerRPC =
                    client.getPartitionsByNames(dbName, tblName, namesPerRPC);
            partitions.addAll(partsPerRPC);
        }

        Map<String, List<String>> partitionNameToPartitionValues = partitionNames.stream()
                .collect(Collectors.toMap(Function.identity(), PartitionUtil::toPartitionValues));

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

    public HivePartitionStats getTableStatistics(String dbName, String tblName) {
        org.apache.hadoop.hive.metastore.api.Table table = client.getTable(dbName, tblName);
        HiveCommonStats commonStats = toHiveCommonStats(table.getParameters());
        long totalRowNums = commonStats.getRowNums();
        if (totalRowNums == -1) {
            return HivePartitionStats.empty();
        }

        List<String> dataColumns = table.getSd().getCols().stream()
                .map(FieldSchema::getName)
                .collect(toImmutableList());
        List<ColumnStatisticsObj> statisticsObjs = client.getTableColumnStats(dbName, tblName, dataColumns);
        Map<String, HiveColumnStats> columnStatistics =
                HiveMetastoreApiConverter.toSinglePartitionColumnStats(statisticsObjs, totalRowNums);
        return new HivePartitionStats(commonStats, columnStatistics);
    }

    public Map<String, HivePartitionStats> getPartitionStatistics(Table table, List<String> partitionNames) {
        HiveMetaStoreTable hmsTbl = (HiveMetaStoreTable) table;
        String dbName = hmsTbl.getDbName();
        String tblName = hmsTbl.getTableName();
        List<String> dataColumns = hmsTbl.getDataColumnNames();
        Map<String, Partition> partitions = getPartitionsByNames(hmsTbl.getDbName(), hmsTbl.getTableName(), partitionNames);

        Map<String, HiveCommonStats> partitionCommonStats = partitions.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> toHiveCommonStats(entry.getValue().getParameters())));

        Map<String, Long> partitionRowNums = partitionCommonStats.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getRowNums()));

        ImmutableMap.Builder<String, HivePartitionStats> resultBuilder = ImmutableMap.builder();
        Map<String, List<ColumnStatisticsObj>> partitionNameToColumnStatsObj =
                client.getPartitionColumnStats(dbName, tblName, partitionNames, dataColumns);

        Map<String, Map<String, HiveColumnStats>> partitionColumnStats = HiveMetastoreApiConverter
                .toPartitionColumnStatistics(partitionNameToColumnStatsObj, partitionRowNums);

        for (String partitionName : partitionCommonStats.keySet()) {
            HiveCommonStats commonStats = partitionCommonStats.get(partitionName);
            Map<String, HiveColumnStats> columnStatistics = partitionColumnStats
                    .getOrDefault(partitionName, ImmutableMap.of());
            resultBuilder.put(partitionName, new HivePartitionStats(commonStats, columnStatistics));
        }

        return resultBuilder.build();
    }

    public long getCurrentEventId() {
        return client.getCurrentNotificationEventId().getEventId();
    }

    public NotificationEventResponse getNextEventResponse(long lastSyncedEventId, String catalogName,
                                                          final boolean getAllEvents)
            throws MetastoreNotificationFetchException {
        try {
            int batchSize = getAllEvents ? -1 : Config.hms_events_batch_size_per_rpc;
            NotificationEventResponse response = client.getNextNotification(lastSyncedEventId, batchSize, null);
            if (response.getEvents().size() == 0) {
                LOG.info("Event size is 0 when pulling events on catalog [{}]", catalogName);
                return null;
            }
            LOG.info(String.format("Received %d events. Start event id : %d. Last synced id : %d on catalog : %s",
                    response.getEvents().size(), response.getEvents().get(0).getEventId(),
                    lastSyncedEventId, catalogName));

            return response;
        } catch (MetastoreNotificationFetchException e) {
            LOG.error("Unable to fetch notifications from metastore. Last synced event id is {}", lastSyncedEventId, e);
            throw new MetastoreNotificationFetchException("Unable to fetch notifications from metastore. " +
                    "Last synced event id is " + lastSyncedEventId, e);
        }
    }
}
