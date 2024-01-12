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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.hive.RemoteFileInputFormat.ORC;
import static org.apache.hadoop.hive.common.StatsSetupConst.ROW_COUNT;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class HiveMetastoreTest {
    @Test
    public void testGetAllDatabaseNames() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "xxx", MetastoreType.HMS);
        List<String> databaseNames = metastore.getAllDatabaseNames();
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
    }

    @Test
    public void testGetAllTableNames() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "xxx", MetastoreType.HMS);
        List<String> databaseNames = metastore.getAllTableNames("xxx");
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetDb() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "xxx", MetastoreType.HMS);
        Database database = metastore.getDb("db1");
        Assert.assertEquals("db1", database.getFullName());

        try {
            metastore.getDb("db2");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksConnectorException);
        }
    }

    @Test
    public void testGetTable() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        com.starrocks.catalog.Table table = metastore.getTable("db1", "tbl1");
        HiveTable hiveTable = (HiveTable) table;
        Assert.assertEquals("db1", hiveTable.getDbName());
        Assert.assertEquals("tbl1", hiveTable.getTableName());
        Assert.assertEquals(Lists.newArrayList("col1"), hiveTable.getPartitionColumnNames());
        Assert.assertEquals(Lists.newArrayList("col2"), hiveTable.getDataColumnNames());
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive", hiveTable.getTableLocation());
        Assert.assertEquals(ScalarType.INT, hiveTable.getPartitionColumns().get(0).getType());
        Assert.assertEquals(ScalarType.INT, hiveTable.getBaseSchema().get(0).getType());
        Assert.assertEquals("hive_catalog", hiveTable.getCatalogName());
    }

    @Test
    public void testTableExists() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        Assert.assertTrue(metastore.tableExists("db1", "tbl1"));
    }

    @Test
    public void testGetPartitionKeys() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        Assert.assertEquals(Lists.newArrayList("col1"), metastore.getPartitionKeysByValue("db1", "tbl1",
                HivePartitionValue.ALL_PARTITION_VALUES));
    }

    @Test
    public void testGetPartition() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        com.starrocks.connector.hive.Partition partition = metastore.getPartition("db1", "tbl1", Lists.newArrayList("par1"));
        Assert.assertEquals(ORC, partition.getInputFormat());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));

        partition = metastore.getPartition("db1", "tbl1", Lists.newArrayList());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));
    }

    @Test
    public void testPartitionExists() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        Assert.assertTrue(metastore.partitionExists(metastore.getTable("db1", "tbl1"), new ArrayList<>()));
    }

    @Test
    public void testAddPartitions() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        HivePartition hivePartition = HivePartition.builder()
                .setColumns(Lists.newArrayList(new Column("c1", Type.INT)))
                .setStorageFormat(HiveStorageFormat.PARQUET)
                .setDatabaseName("db")
                .setTableName("table")
                .setLocation("location")
                .setValues(Lists.newArrayList("p1=1"))
                .setParameters(new HashMap<>()).build();

        HivePartitionStats hivePartitionStats = HivePartitionStats.empty();
        HivePartitionWithStats hivePartitionWithStats = new HivePartitionWithStats("p1=1", hivePartition, hivePartitionStats);
        metastore.addPartitions("db", "table", Lists.newArrayList(hivePartitionWithStats));
    }

    @Test
    public void testDropPartition() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        metastore.dropPartition("db", "table", Lists.newArrayList("k1=1"), false);
    }

    @Test
    public void testGetPartitionByNames() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        List<String> partitionNames = Lists.newArrayList("part1=1/part2=2", "part1=3/part2=4");
        Map<String, com.starrocks.connector.hive.Partition> partitions =
                metastore.getPartitionsByNames("db1", "table1", partitionNames);

        com.starrocks.connector.hive.Partition partition1 = partitions.get("part1=1/part2=2");
        Assert.assertEquals(ORC, partition1.getInputFormat());
        Assert.assertEquals("100", partition1.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/part1=1/part2=2", partition1.getFullPath());

        com.starrocks.connector.hive.Partition partition2 = partitions.get("part1=3/part2=4");
        Assert.assertEquals(ORC, partition2.getInputFormat());
        Assert.assertEquals("100", partition2.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/part1=3/part2=4", partition2.getFullPath());
    }

    @Test
    public void testGetTableStatistics() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        HivePartitionStats statistics = metastore.getTableStatistics("db1", "table1");
        HiveCommonStats commonStats = statistics.getCommonStats();
        Assert.assertEquals(50, commonStats.getRowNums());
        Assert.assertEquals(100, commonStats.getTotalFileBytes());
        HiveColumnStats columnStatistics = statistics.getColumnStats().get("col1");
        Assert.assertEquals(0, columnStatistics.getTotalSizeBytes());
        Assert.assertEquals(1, columnStatistics.getNumNulls());
        Assert.assertEquals(2, columnStatistics.getNdv());
    }

    @Test
    public void testGetPartitionStatistics() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        com.starrocks.catalog.Table hiveTable = metastore.getTable("db1", "table1");
        Map<String, HivePartitionStats> statistics = metastore.getPartitionStatistics(
                hiveTable, Lists.newArrayList("col1=1", "col1=2"));

        HivePartitionStats stats1 = statistics.get("col1=1");
        HiveCommonStats commonStats1 = stats1.getCommonStats();
        Assert.assertEquals(50, commonStats1.getRowNums());
        Assert.assertEquals(100, commonStats1.getTotalFileBytes());
        HiveColumnStats columnStatistics1 = stats1.getColumnStats().get("col2");
        Assert.assertEquals(0, columnStatistics1.getTotalSizeBytes());
        Assert.assertEquals(1, columnStatistics1.getNumNulls());
        Assert.assertEquals(2, columnStatistics1.getNdv());

        HivePartitionStats stats2 = statistics.get("col1=2");
        HiveCommonStats commonStats2 = stats2.getCommonStats();
        Assert.assertEquals(50, commonStats2.getRowNums());
        Assert.assertEquals(100, commonStats2.getTotalFileBytes());
        HiveColumnStats columnStatistics2 = stats2.getColumnStats().get("col2");
        Assert.assertEquals(0, columnStatistics2.getTotalSizeBytes());
        Assert.assertEquals(2, columnStatistics2.getNumNulls());
        Assert.assertEquals(5, columnStatistics2.getNdv());
    }

    @Test
    public void testUpdateTableStatistics() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        HivePartitionStats partitionStats = HivePartitionStats.empty();
        metastore.updateTableStatistics("db", "table", ignore -> partitionStats);
    }

    @Test
    public void testUpdatePartitionStatistics() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        HivePartitionStats partitionStats = HivePartitionStats.empty();
        metastore.updatePartitionStatistics("db", "table", "p1=1", ignore -> partitionStats);
    }

    public static class MockedHiveMetaClient extends HiveMetaClient {

        public MockedHiveMetaClient() {
            super(new HiveConf());
        }

        public CurrentNotificationEventId getCurrentNotificationEventId() {
            return new CurrentNotificationEventId(1L);
        }

        public List<String> getAllDatabaseNames() {
            return Lists.newArrayList("db1", "db2");
        }

        public void createDatabase(org.apache.hadoop.hive.metastore.api.Database database) {

        }

        public void dropDatabase(String dbName, boolean deleteData) {
        }

        public void createTable(Table table) {

        }

        public void dropTable(String dbName, String tableName) {

        }

        public List<String> getAllTableNames(String dbName) {
            if (dbName.equals("empty_db")) {
                return Lists.newArrayList();
            } else {
                return Lists.newArrayList("table1", "table2");
            }
        }

        public org.apache.hadoop.hive.metastore.api.Database getDb(String dbName) {
            if (dbName.equals("db1")) {
                return new org.apache.hadoop.hive.metastore.api.Database("db1", "", "", ImmutableMap.of());
            } else {
                return super.getDb(dbName);
            }
        }

        public Table getTable(String dbName, String tblName) {
            List<FieldSchema> partKeys = Lists.newArrayList(new FieldSchema("col1", "INT", ""));
            List<FieldSchema> unPartKeys = Lists.newArrayList(new FieldSchema("col2", "INT", ""));
            String hdfsPath = "hdfs://127.0.0.1:10000/hive";
            StorageDescriptor sd = new StorageDescriptor();
            sd.setCols(unPartKeys);
            sd.setLocation(hdfsPath);
            sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
            SerDeInfo serDeInfo = new SerDeInfo();
            serDeInfo.setParameters(ImmutableMap.of());
            sd.setSerdeInfo(serDeInfo);
            Table msTable1 = new Table();
            msTable1.setDbName(dbName);
            msTable1.setTableName(tblName);

            msTable1.setSd(sd);
            msTable1.setTableType("MANAGED_TABLE");
            msTable1.setParameters(ImmutableMap.of(ROW_COUNT, "50", TOTAL_SIZE, "100"));

            if (!tblName.equals("unpartitioned_table")) {
                msTable1.setPartitionKeys(partKeys);
            } else {
                msTable1.setPartitionKeys(new ArrayList<>());
            }

            return msTable1;
        }

        public boolean tableExists(String dbName, String tblName) {
            return getTable(dbName, tblName) != null;
        }

        public void alterTable(String dbName, String tableName, Table newTable) {

        }

        public List<String> getPartitionKeys(String dbName, String tableName) {
            return Lists.newArrayList("col1");
        }

        public List<String> getPartitionKeysByValue(String dbName, String tableName, List<String> partitionValues) {
            return Lists.newArrayList("col1");
        }

        public void addPartitions(String dbName, String tableName, List<Partition> partitions) {
        }

        public Partition getPartition(HiveTableName name, List<String> partitionValues) {
            StorageDescriptor sd = new StorageDescriptor();
            String hdfsPath = "hdfs://127.0.0.1:10000/hive";
            sd.setLocation(hdfsPath);
            sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
            SerDeInfo serDeInfo = new SerDeInfo();
            serDeInfo.setParameters(ImmutableMap.of());
            sd.setSerdeInfo(serDeInfo);
            Partition partition = new Partition();
            partition.setSd(sd);
            partition.setParameters(ImmutableMap.of(TOTAL_SIZE, "100"));
            return partition;
        }

        public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
            StorageDescriptor sd = new StorageDescriptor();
            sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
            SerDeInfo serDeInfo = new SerDeInfo();
            serDeInfo.setParameters(ImmutableMap.of());
            sd.setSerdeInfo(serDeInfo);

            Partition partition = new Partition();
            partition.setSd(sd);
            partition.setParameters(ImmutableMap.of(TOTAL_SIZE, "100", ROW_COUNT, "50"));
            partition.setValues(partitionValues);
            return partition;
        }

        public void dropPartition(String dbName, String tableName, List<String> partValues, boolean deleteData) {
        }

        public void alterPartition(String dbName, String tableName, Partition newPartition) {
        }

        public List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames) {
            String hdfsPath = "hdfs://127.0.0.1:10000/hive.db/hive_tbl/";
            List<Partition> res = Lists.newArrayList();
            for (String partitionName : partitionNames) {
                StorageDescriptor sd = new StorageDescriptor();
                sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
                SerDeInfo serDeInfo = new SerDeInfo();
                serDeInfo.setParameters(ImmutableMap.of());
                sd.setSerdeInfo(serDeInfo);
                sd.setLocation(hdfsPath + partitionName);

                Partition partition = new Partition();
                partition.setSd(sd);
                partition.setParameters(ImmutableMap.of(TOTAL_SIZE, "100", ROW_COUNT, "50"));
                partition.setValues(Lists.newArrayList(PartitionUtil.toPartitionValues(partitionName)));
                res.add(partition);
            }
            return res;
        }

        public List<ColumnStatisticsObj> getTableColumnStats(String dbName, String tableName, List<String> columns) {
            ColumnStatisticsObj stats = new ColumnStatisticsObj();
            ColumnStatisticsData data = new ColumnStatisticsData();
            LongColumnStatsData longColumnStatsData = new LongColumnStatsData();
            longColumnStatsData.setLowValue(111);
            longColumnStatsData.setHighValue(222222222);
            longColumnStatsData.setNumDVs(3);
            longColumnStatsData.setNumNulls(1);
            data.setLongStats(longColumnStatsData);
            stats.setStatsData(data);
            stats.setColName("col1");
            return Lists.newArrayList(stats);
        }

        public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStats(String dbName,
                                                                              String tableName,
                                                                              List<String> columns,
                                                                              List<String> partitionNames) {
            Map<String, List<ColumnStatisticsObj>> res = Maps.newHashMap();
            ColumnStatisticsObj stats1 = new ColumnStatisticsObj();
            ColumnStatisticsData data1 = new ColumnStatisticsData();
            LongColumnStatsData longColumnStatsData1 = new LongColumnStatsData();
            longColumnStatsData1.setLowValue(0);
            longColumnStatsData1.setHighValue(222222222);
            longColumnStatsData1.setNumDVs(3);
            longColumnStatsData1.setNumNulls(1);
            data1.setLongStats(longColumnStatsData1);
            stats1.setStatsData(data1);
            stats1.setColName("col2");
            res.put("col1=1", Lists.newArrayList(stats1));

            ColumnStatisticsObj stats2 = new ColumnStatisticsObj();
            ColumnStatisticsData data2 = new ColumnStatisticsData();
            LongColumnStatsData longColumnStatsData2 = new LongColumnStatsData();
            longColumnStatsData2.setLowValue(1);
            longColumnStatsData2.setHighValue(222222222);
            longColumnStatsData2.setNumDVs(6);
            longColumnStatsData2.setNumNulls(2);
            data2.setLongStats(longColumnStatsData2);
            stats2.setStatsData(data2);
            stats2.setColName("col2");
            res.put("col1=2", Lists.newArrayList(stats2));

            return res;
        }
    }
}
