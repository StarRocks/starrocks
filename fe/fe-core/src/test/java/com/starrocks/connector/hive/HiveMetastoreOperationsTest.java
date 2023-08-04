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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.RemoteFileInputFormat.ORC;
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
        hmsOps = new HiveMetastoreOperations(cachingHiveMetastore, true, new Configuration(), MetastoreType.HMS, "hive_catalog");
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
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive", hiveTable.getTableLocation());
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
        Partition partition = hmsOps.getPartition(
                "db1", "tbl1", Lists.newArrayList("par1"));
        Assert.assertEquals(ORC, partition.getInputFormat());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));

        partition = hmsOps.getPartition("db1", "tbl1", Lists.newArrayList());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));
    }

    @Test
    public void testGetPartitionByNames() throws AnalysisException {
        com.starrocks.catalog.Table table = hmsOps.getTable("db1", "table1");
        HiveTable hiveTable = (HiveTable) table;
        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());
        Map<String, Partition> partitions =
                hmsOps.getPartitionByPartitionKeys(hiveTable, Lists.newArrayList(hivePartitionKey1, hivePartitionKey2));

        Partition partition1 = partitions.get("col1=1");
        Assert.assertEquals(ORC, partition1.getInputFormat());
        Assert.assertEquals("100", partition1.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=1", partition1.getFullPath());

        Partition partition2 = partitions.get("col1=2");
        Assert.assertEquals(ORC, partition2.getInputFormat());
        Assert.assertEquals("100", partition2.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=2", partition2.getFullPath());
    }

    @Test
    public void testGetTableStatistics() {
        HivePartitionStats statistics = hmsOps.getTableStatistics("db1", "table1");
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
        com.starrocks.catalog.Table hiveTable = hmsOps.getTable("db1", "table1");
        Map<String, HivePartitionStats> statistics = hmsOps.getPartitionStatistics(
                hiveTable, Lists.newArrayList("col1=1", "col1=2"));
        Assert.assertEquals(0, statistics.size());

        cachingHiveMetastore.getPartitionStatistics(hiveTable, Lists.newArrayList("col1=1", "col1=2"));
        statistics = hmsOps.getPartitionStatistics(
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
    public void testDropDb() throws MetaNotFoundException {
        class MockedTestMetaClient extends HiveMetastoreTest.MockedHiveMetaClient {
            public org.apache.hadoop.hive.metastore.api.Database getDb(String dbName) throws RuntimeException {
                if (dbName.equals("not_exist_db")) {
                    throw new RuntimeException("db not_exist_db not found");
                }
                return null;
            }
        }

        HiveMetaClient client = new MockedTestMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog");
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HiveMetastoreOperations hmsOps = new HiveMetastoreOperations(cachingHiveMetastore, true,
                new Configuration(), MetastoreType.HMS, "hive_catalog");

        HiveMetastoreOperations finalHmsOps = hmsOps;
        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "Failed to access database not_exist_db",
                () -> finalHmsOps.dropDb("not_exist_db", true));

        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "Database location is empty",
                () -> this.hmsOps.dropDb("db1", true));

        class MockedTestMetaClient1 extends HiveMetastoreTest.MockedHiveMetaClient {

            public org.apache.hadoop.hive.metastore.api.Database getDb(String dbName) throws RuntimeException {
                if (dbName.equals("db1")) {
                    org.apache.hadoop.hive.metastore.api.Database database = new org.apache.hadoop.hive.metastore.api.Database();
                    database.setName("db1");
                    database.setLocationUri("locationXXX");
                    return database;
                }
                return null;
            }
        }

        metastore = new HiveMetastore(new MockedTestMetaClient1(), "hive_catalog");
        executor = Executors.newFixedThreadPool(5);
        cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        hmsOps = new HiveMetastoreOperations(cachingHiveMetastore, true, new Configuration(), MetastoreType.HMS, "hive_catalog");

        hmsOps.dropDb("db1", false);
    }
}