// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HiveColumnStats;
import com.starrocks.connector.hive.HiveCommonStats;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HivePartitionStats;
import com.starrocks.connector.hive.HiveTableName;
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
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.connector.hive.RemoteFileInputFormat.ORC;
import static org.apache.hadoop.hive.common.StatsSetupConst.ROW_COUNT;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class HiveMetastoreTest {
    @Test
    public void testGetAllDatabaseNames() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "xxx");
        List<String> databaseNames = metastore.getAllDatabaseNames();
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
    }

    @Test
    public void testGetAllTableNames() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "xxx");
        List<String> databaseNames = metastore.getAllTableNames("xxx");
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetDb() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "xxx");
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
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog");
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
    public void testGetPartitionKeys() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog");
        Assert.assertEquals(Lists.newArrayList("col1"), metastore.getPartitionKeys("db1", "tbl1"));
    }

    @Test
    public void testGetPartition() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog");
        com.starrocks.connector.hive.Partition partition = metastore.getPartition("db1", "tbl1", Lists.newArrayList("par1"));
        Assert.assertEquals(ORC, partition.getInputFormat());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));

        partition = metastore.getPartition("db1", "tbl1", Lists.newArrayList());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));
    }

    @Test
    public void testGetPartitionByNames() {
        HiveMetaClient client = new MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog");
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
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog");
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
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog");
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

        public List<String> getAllTableNames(String dbName) {
            return Lists.newArrayList("table1", "table2");
        }

        public org.apache.hadoop.hive.metastore.api.Database getDb(String dbName) {
            if (dbName.equals("db1")) {
                return new org.apache.hadoop.hive.metastore.api.Database("db1", "", "", ImmutableMap.of());
            } else {
                return super.getDb(dbName);
            }
        }

        public Table getTable(String dbName, String tblName) {
            if (dbName.equalsIgnoreCase("transactional_db")) {
                if (tblName.equalsIgnoreCase("insert_only")) {
                    return getTransactionalTable(dbName, tblName, true);
                } else if (tblName.equalsIgnoreCase("full_acid")) {
                    return getTransactionalTable(dbName, tblName, false);
                }
            }
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
            msTable1.setPartitionKeys(partKeys);
            msTable1.setSd(sd);
            msTable1.setTableType("MANAGED_TABLE");
            msTable1.setParameters(ImmutableMap.of(ROW_COUNT, "50", TOTAL_SIZE, "100"));
            return msTable1;
        }

<<<<<<< HEAD
=======
        private Table getTransactionalTable(String dbName, String tblName, boolean insertOnly) {
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
            if (insertOnly) {
                msTable1.setParameters(ImmutableMap.of(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true",
                        hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "insert_only"));
            } else {
                msTable1.setParameters(ImmutableMap.of(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true",
                        hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "default"));
            }

            msTable1.setPartitionKeys(new ArrayList<>());

            return msTable1;
        }

        public boolean tableExists(String dbName, String tblName) {
            return getTable(dbName, tblName) != null;
        }

        public void alterTable(String dbName, String tableName, Table newTable) {

        }

>>>>>>> a685c5fc68 ([BugFix] Banned hive full acid table (#39264))
        public List<String> getPartitionKeys(String dbName, String tableName) {
            return Lists.newArrayList("col1");
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
