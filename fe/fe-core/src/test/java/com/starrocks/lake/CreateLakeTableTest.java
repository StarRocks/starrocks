// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;


import com.google.common.collect.Lists;
import com.staros.proto.ObjectStorageInfo;
import com.staros.proto.ShardStorageInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateLakeTableTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database lake_test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());

        Config.use_staros = true;
    }

    @AfterClass
    public static void afterClass() {
        Config.use_staros = false;
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
    }

    private void checkLakeTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(tableName);
        Assert.assertTrue(table.isLakeTable());
    }

    private LakeTable getLakeTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(tableName);
        Assert.assertTrue(table.isLakeTable());
        return (LakeTable) table;
    }

    @Test
    public void testCreateLakeTable(@Mocked StarOSAgent agent) throws UserException {
        ObjectStorageInfo objectStorageInfo = ObjectStorageInfo.newBuilder().setObjectUri("s3://bucket/1/").build();
        ShardStorageInfo shardStorageInfo =
                ShardStorageInfo.newBuilder().setObjectStorageInfo(objectStorageInfo).build();

        new Expectations() {
            {
                agent.getServiceShardStorageInfo();
                result = shardStorageInfo;
                agent.createShards(anyInt, (ShardStorageInfo) any, anyLong);
                returns(Lists.newArrayList(20001L, 20002L, 20003L),
                        Lists.newArrayList(20004L, 20005L), Lists.newArrayList(20006L, 20007L),
                        Lists.newArrayList(20008L), Lists.newArrayList(20009L));
                agent.getPrimaryBackendIdByShard(anyLong);
                result = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0);
            }
        };

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "starOSAgent", agent);

        // normal
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.single_partition_duplicate_key (key1 int, key2 varchar(10))\n" +
                        "engine = starrocks distributed by hash(key1) buckets 3\n" +
                        "properties('replication_num' = '1');"));
        checkLakeTable("lake_test", "single_partition_duplicate_key");

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.multi_partition_aggregate_key (key1 date, key2 varchar(10), v bigint sum)\n" +
                        "engine = starrocks partition by range(key1)\n" +
                        "(partition p1 values less than (\"2022-03-01\"),\n" +
                        " partition p2 values less than (\"2022-04-01\"))\n" +
                        "distributed by hash(key2) buckets 2\n" +
                        "properties('replication_num' = '1');"));
        checkLakeTable("lake_test", "multi_partition_aggregate_key");

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.multi_partition_unique_key (key1 int, key2 varchar(10), v bigint)\n" +
                        "engine = starrocks unique key (key1, key2)\n" +
                        "partition by range(key1)\n" +
                        "(partition p1 values less than (\"10\"),\n" +
                        " partition p2 values less than (\"20\"))\n" +
                        "distributed by hash(key2) buckets 1\n" +
                        "properties('replication_num' = '1');"));
        checkLakeTable("lake_test", "multi_partition_unique_key");
    }

    @Test
    public void testCreateLakeTableWithStorageCache(@Mocked StarOSAgent agent) throws UserException {
        ObjectStorageInfo objectStorageInfo = ObjectStorageInfo.newBuilder().setObjectUri("s3://bucket/1/").build();
        ShardStorageInfo shardStorageInfo =
                ShardStorageInfo.newBuilder().setObjectStorageInfo(objectStorageInfo).build();

        new Expectations() {
            {
                agent.getServiceShardStorageInfo();
                result = shardStorageInfo;
                agent.createShards(anyInt, (ShardStorageInfo) any, anyLong);
                returns(Lists.newArrayList(20001L, 20002L, 20003L),
                        Lists.newArrayList(20004L, 20005L), Lists.newArrayList(20006L, 20007L),
                        Lists.newArrayList(20008L), Lists.newArrayList(20009L));
                agent.getPrimaryBackendIdByShard(anyLong);
                result = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0);
            }
        };

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "starOSAgent", agent);

        // normal
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.single_partition_duplicate_key_cache (key1 int, key2 varchar(10))\n" +
                        "engine = starrocks distributed by hash(key1) buckets 3\n" +
                        "properties('enable_storage_cache' = 'true', 'storage_cache_ttl' = '3600');"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "single_partition_duplicate_key_cache");
            // check table property
            StorageInfo storageInfo = lakeTable.getTableProperty().getStorageInfo();
            Assert.assertTrue(storageInfo.isEnableStorageCache());
            Assert.assertEquals(3600, storageInfo.getStorageCacheTtlS());
            // check partition property
            long partitionId = lakeTable.getPartition("single_partition_duplicate_key_cache").getId();
            StorageCacheInfo partitionStorageCacheInfo = lakeTable.getPartitionInfo().getStorageCacheInfo(partitionId);
            Assert.assertTrue(partitionStorageCacheInfo.isEnableStorageCache());
            Assert.assertEquals(3600, partitionStorageCacheInfo.getStorageCacheTtlS());
            Assert.assertEquals(false, partitionStorageCacheInfo.isAllowAsyncWriteBack());
        }

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.multi_partition_aggregate_key_cache \n" +
                        "(key1 date, key2 varchar(10), v bigint sum)\n" +
                        "engine = starrocks partition by range(key1)\n" +
                        "(partition p1 values less than (\"2022-03-01\"),\n" +
                        " partition p2 values less than (\"2022-04-01\"))\n" +
                        "distributed by hash(key2) buckets 2\n" +
                        "properties('enable_storage_cache' = 'true', 'storage_cache_ttl' = '7200'," +
                        "'allow_async_write_back' = 'true');"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "multi_partition_aggregate_key_cache");
            // check table property
            StorageInfo storageInfo = lakeTable.getTableProperty().getStorageInfo();
            Assert.assertTrue(storageInfo.isEnableStorageCache());
            Assert.assertEquals(7200, storageInfo.getStorageCacheTtlS());
            // check partition property
            long partition1Id = lakeTable.getPartition("p1").getId();
            StorageCacheInfo partition1StorageCacheInfo = lakeTable.getPartitionInfo().getStorageCacheInfo(partition1Id);
            Assert.assertTrue(partition1StorageCacheInfo.isEnableStorageCache());
            Assert.assertEquals(7200, partition1StorageCacheInfo.getStorageCacheTtlS());
            long partition2Id = lakeTable.getPartition("p2").getId();
            StorageCacheInfo partition2StorageCacheInfo = lakeTable.getPartitionInfo().getStorageCacheInfo(partition2Id);
            Assert.assertTrue(partition2StorageCacheInfo.isEnableStorageCache());
            Assert.assertEquals(7200, partition2StorageCacheInfo.getStorageCacheTtlS());
            Assert.assertEquals(true, partition2StorageCacheInfo.isAllowAsyncWriteBack());
        }

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.multi_partition_unique_key_cache (key1 int, key2 varchar(10), v bigint)\n" +
                        "engine = starrocks unique key (key1, key2)\n" +
                        "partition by range(key1)\n" +
                        "(partition p1 values less than (\"10\"),\n" +
                        " partition p2 values less than (\"20\") ('enable_storage_cache' = 'true'))\n" +
                        "distributed by hash(key2) buckets 1\n" +
                        "properties('replication_num' = '1');"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "multi_partition_unique_key_cache");
            // check table property
            StorageInfo storageInfo = lakeTable.getTableProperty().getStorageInfo();
            Assert.assertFalse(storageInfo.isEnableStorageCache());
            Assert.assertEquals(0, storageInfo.getStorageCacheTtlS());
            // check partition property
            long partition1Id = lakeTable.getPartition("p1").getId();
            StorageCacheInfo partition1StorageCacheInfo = lakeTable.getPartitionInfo().getStorageCacheInfo(partition1Id);
            Assert.assertFalse(partition1StorageCacheInfo.isEnableStorageCache());
            Assert.assertEquals(0, partition1StorageCacheInfo.getStorageCacheTtlS());
            long partition2Id = lakeTable.getPartition("p2").getId();
            StorageCacheInfo partition2StorageCacheInfo = lakeTable.getPartitionInfo().getStorageCacheInfo(partition2Id);
            Assert.assertTrue(partition2StorageCacheInfo.isEnableStorageCache());
            Assert.assertEquals(Config.tablet_sched_storage_cooldown_second,
                    partition2StorageCacheInfo.getStorageCacheTtlS());
        }
    }

    @Test
    public void testCreateLakeTableException() {
        // primary key type is not supported
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Lake table does not support primary key type",
                () -> createTable(
                        "create table lake_test.single_partition_duplicate_key (key1 int, key2 varchar(10))\n" +
                                "engine = starrocks primary key (key1) distributed by hash(key1) buckets 3"));

        // storage_cache disabled but allow_async_write_back = true
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "storage allow_async_write_back can't be enabled when cache is disabled",
                () -> createTable(
                        "create table lake_test.single_partition_invalid_cache_property (key1 int, key2 varchar(10))\n" +
                        "engine = starrocks distributed by hash(key1) buckets 3\n" +
                        " properties('enable_storage_cache' = 'false', 'storage_cache_ttl' = '0'," +
                        "'allow_async_write_back' = 'true');"));
    }
}
