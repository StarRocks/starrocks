// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.external.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.external.hive.RemoteFileInputFormat.PARQUET;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class HiveMetastoreOperationsTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private CachingHiveMetastore cachingHiveMetastore;
    private HiveMetastoreOperations hmsOps;
    private ExecutorService executor;
    private long expireAfterWriteSec = 10;
    private long refreshAfterWriteSec = -1;

    @Before
    public void setUp() throws Exception {
        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog");
        executor = Executors.newFixedThreadPool(5);
        cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        hmsOps = new HiveMetastoreOperations(cachingHiveMetastore);
    }

    @After
    public void tearDown() {
        executor.shutdown();
    }

    @Test
    public void testGetAllDatabaseNames() {
        List<String> databaseNames = hmsOps.getAllDatabaseNames();
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
        CachingHiveMetastore queryLevelCache = CachingHiveMetastore.createQueryLevelInstance(cachingHiveMetastore, 100);
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), queryLevelCache.getAllDatabaseNames());
    }

    @Test
    public void testGetAllTableNames() {
        List<String> databaseNames = hmsOps.getAllTableNames("xxx");
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetDb() {
        Database database = hmsOps.getDb("db1");
        Assert.assertEquals("db1", database.getFullName());

        try {
            hmsOps.getDb("db2");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksConnectorException);
        }
    }

    @Test
    public void testGetTable() {
        com.starrocks.catalog.Table table = hmsOps.getTable("db1", "tbl1");
        HiveTable hiveTable = (HiveTable) table;
        Assert.assertEquals("db1", hiveTable.getDbName());
        Assert.assertEquals("tbl1", hiveTable.getTableName());
        Assert.assertEquals(Lists.newArrayList("col1"), hiveTable.getPartitionColumnNames());
        Assert.assertEquals(Lists.newArrayList("col2"), hiveTable.getDataColumnNames());
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive", hiveTable.getHdfsPath());
        Assert.assertEquals(ScalarType.INT, hiveTable.getPartitionColumns().get(0).getType());
        Assert.assertEquals(ScalarType.INT, hiveTable.getBaseSchema().get(0).getType());
        Assert.assertEquals("hive_catalog", hiveTable.getCatalogName());
    }

    @Test
    public void testGetPartitionKeys() {
        Assert.assertEquals(Lists.newArrayList("col1"), hmsOps.getPartitionKeys("db1", "tbl1"));
    }

    @Test
    public void testGetPartition() {
        com.starrocks.external.hive.Partition partition = hmsOps.getPartition(
                "db1", "tbl1", Lists.newArrayList("par1"));
        Assert.assertEquals(PARQUET, partition.getInputFormat());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive", partition.getFullPath());

        partition = hmsOps.getPartition("db1", "tbl1", Lists.newArrayList());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive", partition.getFullPath());
    }

    @Test
    public void testGetPartitionByNames() throws AnalysisException {
        com.starrocks.catalog.Table table = hmsOps.getTable("db1", "table1");
        HiveTable hiveTable = (HiveTable) table;
        PartitionKey hivePartitionKey1 = Utils.createPartitionKey(Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = Utils.createPartitionKey(Lists.newArrayList("2"), hiveTable.getPartitionColumns());
        Map<String, Partition> partitions =
                hmsOps.getPartitionByNames(hiveTable, Lists.newArrayList(hivePartitionKey1, hivePartitionKey2));

        com.starrocks.external.hive.Partition partition1 = partitions.get("col1=1");
        Assert.assertEquals(PARQUET, partition1.getInputFormat());
        Assert.assertEquals("100", partition1.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=1", partition1.getFullPath());

        com.starrocks.external.hive.Partition partition2 = partitions.get("col1=2");
        Assert.assertEquals(PARQUET, partition2.getInputFormat());
        Assert.assertEquals("100", partition2.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=2", partition2.getFullPath());
    }

    @Test
    public void testGetTableStatistics() {
        HivePartitionStatistics statistics = hmsOps.getTableStatistics("db1", "table1");
        HiveCommonStats commonStats = statistics.getCommonStats();
        Assert.assertEquals(50, commonStats.getRowNums());
        Assert.assertEquals(100, commonStats.getTotalFileBytes());
        HiveColumnStatistics columnStatistics = statistics.getColumnStats().get("col1");
        Assert.assertEquals(0, columnStatistics.getTotalSizeBytes());
        Assert.assertEquals(1, columnStatistics.getNumNulls());
        Assert.assertEquals(2, columnStatistics.getNdv());
    }

    @Test
    public void testGetPartitionStatistics() {
        com.starrocks.catalog.Table hiveTable = hmsOps.getTable("db1", "table1");
        Map<String, HivePartitionStatistics> statistics = hmsOps.getPartitionStatistics(
                hiveTable, Lists.newArrayList("col1=1", "col1=2"));
        Assert.assertEquals(0, statistics.size());

        cachingHiveMetastore.getPartitionStatistics(hiveTable, Lists.newArrayList("col1=1", "col1=2"));
        statistics = hmsOps.getPartitionStatistics(
                hiveTable, Lists.newArrayList("col1=1", "col1=2"));
        HivePartitionStatistics stats1 = statistics.get("col1=1");
        HiveCommonStats commonStats1 = stats1.getCommonStats();
        Assert.assertEquals(50, commonStats1.getRowNums());
        Assert.assertEquals(100, commonStats1.getTotalFileBytes());
        HiveColumnStatistics columnStatistics1 = stats1.getColumnStats().get("col2");
        Assert.assertEquals(0, columnStatistics1.getTotalSizeBytes());
        Assert.assertEquals(1, columnStatistics1.getNumNulls());
        Assert.assertEquals(2, columnStatistics1.getNdv());

        HivePartitionStatistics stats2 = statistics.get("col1=2");
        HiveCommonStats commonStats2 = stats2.getCommonStats();
        Assert.assertEquals(50, commonStats2.getRowNums());
        Assert.assertEquals(100, commonStats2.getTotalFileBytes());
        HiveColumnStatistics columnStatistics2 = stats2.getColumnStats().get("col2");
        Assert.assertEquals(0, columnStatistics2.getTotalSizeBytes());
        Assert.assertEquals(2, columnStatistics2.getNumNulls());
        Assert.assertEquals(5, columnStatistics2.getNdv());
    }
}