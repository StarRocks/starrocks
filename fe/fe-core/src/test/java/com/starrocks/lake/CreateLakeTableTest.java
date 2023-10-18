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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.SharedDataStorageVolumeMgr;
import com.starrocks.server.SharedNothingStorageVolumeMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Objects;

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

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new MockUp<SharedNothingStorageVolumeMgr>() {
            S3FileStoreInfo s3FileStoreInfo = S3FileStoreInfo.newBuilder().setBucket("default-bucket")
                    .setRegion(Config.aws_s3_region).setEndpoint(Config.aws_s3_endpoint)
                    .setCredential(AwsCredentialInfo.newBuilder()
                            .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()).build()).build();
            FileStoreInfo fsInfo = FileStoreInfo.newBuilder().setFsName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME)
                    .setFsKey("1").setFsType(FileStoreType.S3)
                    .setS3FsInfo(s3FileStoreInfo).build();

            @Mock
            public StorageVolume getStorageVolumeByName(String svName) throws AnalysisException {
                return StorageVolume.fromFileStoreInfo(fsInfo);
            }

            @Mock
            public String getStorageVolumeIdOfTable(long tableId) {
                return fsInfo.getFsKey();
            }

            @Mock
            public StorageVolume getStorageVolume(String svId) {
                return StorageVolume.fromFileStoreInfo(fsInfo);
            }
        };
    }

    @AfterClass
    public static void afterClass() {
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
    }

    private void checkLakeTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(tableName);
        Assert.assertTrue(table.isCloudNativeTable());
    }

    private LakeTable getLakeTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(tableName);
        Assert.assertTrue(table.isCloudNativeTable());
        return (LakeTable) table;
    }

    private FilePathInfo getPathInfo() {
        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();

        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("test-bucket");
        s3FsBuilder.setRegion("test-region");
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();

        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();

        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://test-bucket/1/");
        return builder.build();
    }

    private FileStoreInfo getFileStoreInfo() {
        S3FileStoreInfo s3FileStoreInfo = S3FileStoreInfo.newBuilder().setBucket("default-bucket")
                .setRegion(Config.aws_s3_region).setEndpoint(Config.aws_s3_endpoint)
                .setCredential(AwsCredentialInfo.newBuilder()
                        .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()).build()).build();
        return FileStoreInfo.newBuilder().setFsName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME)
                .setFsKey("1").setFsType(FileStoreType.S3)
                .setS3FsInfo(s3FileStoreInfo).build();
    }

    @Test
    public void testCreateLakeTable(@Mocked StarOSAgent agent) throws UserException {
        new Expectations(agent) {
            {
                agent.allocateFilePath(anyString, anyLong);
                result = getPathInfo();
                agent.createShardGroup(anyLong, anyLong, anyLong);
                result = GlobalStateMgr.getCurrentState().getNextId();
                agent.createShards(anyInt, (FilePathInfo) any, (FileCacheInfo) any, anyLong, (Map<String, String>) any);
                returns(Lists.newArrayList(20001L, 20002L, 20003L),
                        Lists.newArrayList(20004L, 20005L), Lists.newArrayList(20006L, 20007L),
                        Lists.newArrayList(20008L), Lists.newArrayList(20009L));
                agent.getPrimaryComputeNodeIdByShard(anyLong, anyLong);
                result = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0);
            }
        };

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "starOSAgent", agent);

        // normal
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.single_partition_duplicate_key (key1 int, key2 varchar(10))\n" +
                        "distributed by hash(key1) buckets 3\n" +
                        "properties('replication_num' = '1');"));
        checkLakeTable("lake_test", "single_partition_duplicate_key");

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.multi_partition_aggregate_key (key1 date, key2 varchar(10), v bigint sum)\n" +
                        "partition by range(key1)\n" +
                        "(partition p1 values less than (\"2022-03-01\"),\n" +
                        " partition p2 values less than (\"2022-04-01\"))\n" +
                        "distributed by hash(key2) buckets 2\n" +
                        "properties('replication_num' = '1');"));
        checkLakeTable("lake_test", "multi_partition_aggregate_key");

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.multi_partition_unique_key (key1 int, key2 varchar(10), v bigint)\n" +
                        "unique key (key1, key2)\n" +
                        "partition by range(key1)\n" +
                        "(partition p1 values less than (\"10\"),\n" +
                        " partition p2 values less than (\"20\"))\n" +
                        "distributed by hash(key2) buckets 1\n" +
                        "properties('replication_num' = '1');"));
        checkLakeTable("lake_test", "multi_partition_unique_key");

        LakeTable table = getLakeTable("lake_test", "multi_partition_unique_key");
        Assert.assertEquals(getPathInfo(), table.getDefaultFilePathInfo());
        Assert.assertEquals(getPathInfo().getFullPath() + "/100",
                Objects.requireNonNull(table.getPartitionFilePathInfo(100)).getFullPath());
    }

    @Test
    public void testCreateLakeTableWithStorageCache(@Mocked StarOSAgent agent) throws UserException {
        new Expectations() {
            {
                agent.allocateFilePath(anyString, anyLong);
                result = getPathInfo();
                agent.createShardGroup(anyLong, anyLong, anyLong);
                result = GlobalStateMgr.getCurrentState().getNextId();
                agent.createShards(anyInt, (FilePathInfo) any, (FileCacheInfo) any, anyLong, (Map<String, String>) any);
                returns(Lists.newArrayList(20001L, 20002L, 20003L),
                        Lists.newArrayList(20004L, 20005L), Lists.newArrayList(20006L, 20007L),
                        Lists.newArrayList(20008L), Lists.newArrayList(20009L));
                agent.getPrimaryComputeNodeIdByShard(anyLong, anyLong);
                result = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0);
            }
        };

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "starOSAgent", agent);

        // normal
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.single_partition_duplicate_key_cache (key1 int, key2 varchar(10))\n" +
                        "distributed by hash(key1) buckets 3\n" +
                        "properties('datacache.enable' = 'true');"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "single_partition_duplicate_key_cache");
            // check table property
            StorageInfo storageInfo = lakeTable.getTableProperty().getStorageInfo();
            Assert.assertTrue(storageInfo.isEnableDataCache());
            // check partition property
            long partitionId = lakeTable.getPartition("single_partition_duplicate_key_cache").getId();
            DataCacheInfo partitionDataCacheInfo = lakeTable.getPartitionInfo().getDataCacheInfo(partitionId);
            Assert.assertTrue(partitionDataCacheInfo.isEnabled());
            Assert.assertEquals(false, partitionDataCacheInfo.isAsyncWriteBack());
        }

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.multi_partition_aggregate_key_cache \n" +
                        "(key1 date, key2 varchar(10), v bigint sum)\n" +
                        "partition by range(key1)\n" +
                        "(partition p1 values less than (\"2022-03-01\"),\n" +
                        " partition p2 values less than (\"2022-04-01\"))\n" +
                        "distributed by hash(key2) buckets 2\n" +
                        "properties('datacache.enable' = 'true','enable_async_write_back' = 'false');"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "multi_partition_aggregate_key_cache");
            // check table property
            StorageInfo storageInfo = lakeTable.getTableProperty().getStorageInfo();
            Assert.assertTrue(storageInfo.isEnableDataCache());
            // check partition property
            long partition1Id = lakeTable.getPartition("p1").getId();
            DataCacheInfo partition1DataCacheInfo =
                    lakeTable.getPartitionInfo().getDataCacheInfo(partition1Id);
            Assert.assertTrue(partition1DataCacheInfo.isEnabled());
            long partition2Id = lakeTable.getPartition("p2").getId();
            DataCacheInfo partition2DataCacheInfo =
                    lakeTable.getPartitionInfo().getDataCacheInfo(partition2Id);
            Assert.assertTrue(partition2DataCacheInfo.isEnabled());
            Assert.assertEquals(false, partition2DataCacheInfo.isAsyncWriteBack());
        }

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.multi_partition_unique_key_cache (key1 int, key2 varchar(10), v bigint)\n" +
                        "unique key (key1, key2)\n" +
                        "partition by range(key1)\n" +
                        "(partition p1 values less than (\"10\"),\n" +
                        " partition p2 values less than (\"20\") ('datacache.enable' = 'false'))\n" +
                        "distributed by hash(key2) buckets 1\n" +
                        "properties('replication_num' = '1');"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "multi_partition_unique_key_cache");
            // check table property
            StorageInfo storageInfo = lakeTable.getTableProperty().getStorageInfo();
            // enabled by default if property key `datacache.enable` is absent
            Assert.assertTrue(storageInfo.isEnableDataCache());
            // check partition property
            long partition1Id = lakeTable.getPartition("p1").getId();
            DataCacheInfo partition1DataCacheInfo =
                    lakeTable.getPartitionInfo().getDataCacheInfo(partition1Id);
            Assert.assertTrue(partition1DataCacheInfo.isEnabled());
            long partition2Id = lakeTable.getPartition("p2").getId();
            DataCacheInfo partition2DataCacheInfo =
                    lakeTable.getPartitionInfo().getDataCacheInfo(partition2Id);
            Assert.assertFalse(partition2DataCacheInfo.isEnabled());
        }

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.auto_partition (key1 date, key2 varchar(10), key3 int)\n" +
                        "partition by date_trunc(\"day\", key1) distributed by hash(key2) buckets 3;"));

        // `day` function is not supported
        ExceptionChecker.expectThrows(AnalysisException.class, () -> createTable(
                "create table lake_test.auto_partition (key1 date, key2 varchar(10), key3 int)\n" +
                        "partition by day(key1) distributed by hash(key2) buckets 3;"));
    }

    @Test
    public void testCreateLakeTableEnablePersistentIndex(@Mocked StarOSAgent agent) throws Exception {
        new Expectations() {
            {
                agent.allocateFilePath(anyString, anyLong);
                result = getPathInfo();
                agent.createShardGroup(anyLong, anyLong, anyLong);
                result = GlobalStateMgr.getCurrentState().getNextId();
                agent.createShards(anyInt, (FilePathInfo) any, (FileCacheInfo) any, anyLong, (Map<String, String>) any);
                returns(Lists.newArrayList(20001L, 20002L, 20003L),
                        Lists.newArrayList(20004L, 20005L), Lists.newArrayList(20006L, 20007L),
                        Lists.newArrayList(20008L), Lists.newArrayList(20009L));
                agent.getPrimaryComputeNodeIdByShard(anyLong, anyLong);
                result = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0);
            }
        };

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "starOSAgent", agent);

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.table_with_persistent_index\n" +
                        "(c0 int, c1 string, c2 int, c3 bigint)\n" +
                        "PRIMARY KEY(c0)\n" +
                        "distributed by hash(c0) buckets 2\n" +
                        "properties('enable_persistent_index' = 'true');"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "table_with_persistent_index");
            // check table persistentIndex
            boolean enablePersistentIndex = lakeTable.enablePersistentIndex();
            Assert.assertTrue(enablePersistentIndex);
            // check table persistentIndexType
            String indexType = lakeTable.getPersistentIndexTypeString();
            Assert.assertEquals(indexType, "LOCAL");

            String sql = "show create table lake_test.table_with_persistent_index";
            ShowCreateTableStmt showCreateTableStmt =
                    (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ShowExecutor executor = new ShowExecutor(connectContext, showCreateTableStmt);
            ShowResultSet resultSet = executor.execute();

            Assert.assertTrue(resultSet.getResultRows().size() != 0);
        }
    }

    @Test
    public void testCreateLakeTableException() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return new StarOSAgent();
            }
        };
        new MockUp<StarOSAgent>() {
            @Mock
            public FilePathInfo allocateFilePath(String storageVolumeId, long tableId) throws DdlException {
                return FilePathInfo.newBuilder().build();
            }
        };

        // storage_cache disabled but enable_async_write_back = true
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "enable_async_write_back is disabled since version 3.1.4",
                () -> createTable(
                        "create table lake_test.single_partition_invalid_cache_property (key1 int, key2 varchar(10))\n" +
                                "distributed by hash(key1) buckets 3\n" +
                                " properties('datacache.enable' = 'false', 'enable_async_write_back' = 'true');"));

        // enable_async_write_back disabled
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "enable_async_write_back is disabled since version 3.1.4",
                () -> createTable(
                        "create table lake_test.single_partition_invalid_cache_property (key1 int, key2 varchar(10))\n" +
                                "distributed by hash(key1) buckets 3\n" +
                                " properties('datacache.enable' = 'true', 'enable_async_write_back' = 'true');"));
    }

    @Test
    public void testExplainRowCount(@Mocked StarOSAgent agent) throws Exception {
        new Expectations(agent) {
            {
                agent.allocateFilePath(anyString, anyLong);
                result = getPathInfo();
                agent.createShardGroup(anyLong, anyLong, anyLong);
                result = GlobalStateMgr.getCurrentState().getNextId();
                agent.createShards(anyInt, (FilePathInfo) any, (FileCacheInfo) any, anyLong, (Map<String, String>) any);
                result = Lists.newArrayList(20001L, 20002L, 20003L);
                agent.getPrimaryComputeNodeIdByShard(anyLong, anyLong);
                result = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0);
                agent.getBackendIdsByShard(anyLong, 0);
                result = Sets.newHashSet(GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0));
            }
        };

        new MockUp<Partition>() {
            @Mock
            public boolean hasData() {
                return true;
            }
        };

        new MockUp<LakeTablet>() {
            @Mock
            public long getRowCount(long version) {
                return 2L;
            }
        };

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "starOSAgent", agent);

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.duplicate_key_rowcount (key1 int, key2 varchar(10))\n" +
                        "distributed by hash(key1) buckets 3 properties('replication_num' = '1');"));
        checkLakeTable("lake_test", "duplicate_key_rowcount");

        // check explain result
        String sql = "select * from lake_test.duplicate_key_rowcount";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("actualRows=6"));
    }

    @Test
    public void testCreateLakeTableListPartition(@Mocked StarOSAgent agent) throws UserException {
        new Expectations() {
            {
                agent.allocateFilePath(anyString, anyLong);
                result = getPathInfo();
                agent.createShardGroup(anyLong, anyLong, anyLong);
                result = GlobalStateMgr.getCurrentState().getNextId();
                agent.createShards(anyInt, (FilePathInfo) any, (FileCacheInfo) any, anyLong, (Map<String, String>) any);
                returns(Lists.newArrayList(20001L, 20002L, 20003L),
                        Lists.newArrayList(20004L, 20005L), Lists.newArrayList(20006L, 20007L),
                        Lists.newArrayList(20008L), Lists.newArrayList(20009L));
                agent.getPrimaryComputeNodeIdByShard(anyLong, anyLong);
                result = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0);
            }
        };

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "starOSAgent", agent);

        // list partition
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.list_partition (dt date not null, key2 varchar(10))\n" +
                        "PARTITION BY LIST (dt) (PARTITION p1 VALUES IN ((\"2022-04-01\")),\n" +
                        "PARTITION p2 VALUES IN ((\"2022-04-02\")),\n" +
                        "PARTITION p3 VALUES IN ((\"2022-04-03\")))\n" +
                        "distributed by hash(dt) buckets 3;"));
        checkLakeTable("lake_test", "list_partition");

        // auto list partition
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.auto_list_partition (dt date not null, key2 varchar(10))\n" +
                        "PARTITION BY (dt) \n" +
                        "distributed by hash(dt) buckets 3;"));
        checkLakeTable("lake_test", "auto_list_partition");
    }
}
