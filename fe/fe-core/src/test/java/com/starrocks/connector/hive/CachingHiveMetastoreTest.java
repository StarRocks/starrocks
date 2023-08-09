// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Expectations;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.RemoteFileInputFormat.ORC;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class CachingHiveMetastoreTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private ExecutorService executor;
    private long expireAfterWriteSec = 10;
    private long refreshAfterWriteSec = -1;

    @Before
    public void setUp() throws Exception {
        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog");
        executor = Executors.newFixedThreadPool(5);
    }

    @After
    public void tearDown() {
        executor.shutdown();
    }

    @Test
    public void testGetAllDatabaseNames() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        List<String> databaseNames = cachingHiveMetastore.getAllDatabaseNames();
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
        CachingHiveMetastore queryLevelCache = CachingHiveMetastore.createQueryLevelInstance(cachingHiveMetastore, 100);
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), queryLevelCache.getAllDatabaseNames());
    }

    @Test
    public void testGetAllTableNames() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        List<String> databaseNames = cachingHiveMetastore.getAllTableNames("xxx");
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetDb() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Database database = cachingHiveMetastore.getDb("db1");
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
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        com.starrocks.catalog.Table table = cachingHiveMetastore.getTable("db1", "tbl1");
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
    public void testRefreshTable() {
        new Expectations(metastore) {
            {
                metastore.getTable(anyString, "notExistTbl");
                minTimes = 0;
                Throwable targetException = new NoSuchObjectException("no such obj");
                Throwable e = new InvocationTargetException(targetException);
                result = new StarRocksConnectorException("table not exist", e);
            }
        };
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        try {
            cachingHiveMetastore.refreshTable("db1", "notExistTbl", true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksConnectorException);
            Assert.assertTrue(e.getMessage().contains("invalidated cache"));
        }

        try {
            cachingHiveMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetPartitionKeys() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assert.assertEquals(Lists.newArrayList("col1"), cachingHiveMetastore.getPartitionKeys("db1", "tbl1"));
    }

    @Test
    public void testGetPartition() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Partition partition = cachingHiveMetastore.getPartition(
                "db1", "tbl1", Lists.newArrayList("par1"));
        Assert.assertEquals(ORC, partition.getInputFormat());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));

        partition = metastore.getPartition("db1", "tbl1", Lists.newArrayList());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));
    }

    @Test
    public void testGetPartitionByNames() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        List<String> partitionNames = Lists.newArrayList("part1=1/part2=2", "part1=3/part2=4");
        Map<String, Partition> partitions =
                cachingHiveMetastore.getPartitionsByNames("db1", "table1", partitionNames);

        Partition partition1 = partitions.get("part1=1/part2=2");
        Assert.assertEquals(ORC, partition1.getInputFormat());
        Assert.assertEquals("100", partition1.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/part1=1/part2=2", partition1.getFullPath());

        Partition partition2 = partitions.get("part1=3/part2=4");
        Assert.assertEquals(ORC, partition2.getInputFormat());
        Assert.assertEquals("100", partition2.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/part1=3/part2=4", partition2.getFullPath());
    }

    @Test
    public void testGetTableStatistics() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HivePartitionStats statistics = cachingHiveMetastore.getTableStatistics("db1", "table1");
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
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        com.starrocks.catalog.Table hiveTable = cachingHiveMetastore.getTable("db1", "table1");
        Map<String, HivePartitionStats> statistics = cachingHiveMetastore.getPartitionStatistics(
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

        List<HivePartitionName> partitionNames = Lists.newArrayList(
                HivePartitionName.of("db1", "table1", "col1=1"),
                HivePartitionName.of("db1", "table1", "col1=2"));

        Assert.assertEquals(2, cachingHiveMetastore.getPresentPartitionsStatistics(partitionNames).size());
    }

    @Test
    public void testPartitionNames() {
        HivePartitionKey hivePartitionKey = new HivePartitionKey();
        hivePartitionKey.pushColumn(new StringLiteral(HiveMetaClient.PARTITION_NULL_VALUE), PrimitiveType.NULL_TYPE);
        List<String> value = PartitionUtil.fromPartitionKey(hivePartitionKey);
        Assert.assertEquals(HiveMetaClient.PARTITION_NULL_VALUE, value.get(0));
    }
}
