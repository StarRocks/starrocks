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

import com.staros.proto.FileStoreInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.reflect.FieldUtils;
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
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database lake_test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
    }

    @AfterClass
    public static void afterClass() {
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
    }

    private void checkLakeTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        Assert.assertTrue(table.isCloudNativeTable());
    }

    private LakeTable getLakeTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        Assert.assertTrue(table.isCloudNativeTable());
        return (LakeTable) table;
    }

    private String getDefaultStorageVolumeFullPath() {
        StorageVolume sv = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getDefaultStorageVolume();
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        FileStoreInfo fsInfo = sv.toFileStoreInfo();
        String serviceId = "";
        try {
            serviceId = (String) FieldUtils.readField(starOSAgent, "serviceId", true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Fail to access StarOSAgent.serviceId");
        }
        return String.format("%s/%s", fsInfo.getLocations(0), serviceId);
    }

    @Test
    public void testCreateLakeTable() throws StarRocksException {
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

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("lake_test");
        LakeTable table = getLakeTable("lake_test", "multi_partition_unique_key");
        String defaultFullPath = getDefaultStorageVolumeFullPath();
        String defaultTableFullPath = String.format("%s/db%d/%d", defaultFullPath, db.getId(), table.getId());
        Assert.assertEquals(defaultTableFullPath, Objects.requireNonNull(table.getDefaultFilePathInfo()).getFullPath());
        Assert.assertEquals(defaultTableFullPath + "/100",
                Objects.requireNonNull(table.getPartitionFilePathInfo(100)).getFullPath());
        Assert.assertEquals(2, table.getMaxColUniqueId());
        Assert.assertEquals(0, table.getColumn("key1").getUniqueId());
        Assert.assertEquals(1, table.getColumn("key2").getUniqueId());
        Assert.assertEquals(2, table.getColumn("v").getUniqueId());
    }

    @Test
    public void testCreateLakeTableWithStorageCache() throws StarRocksException {
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
            Assert.assertFalse(partitionDataCacheInfo.isAsyncWriteBack());
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
            Assert.assertFalse(partition2DataCacheInfo.isAsyncWriteBack());
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
    public void testCreateLakeTableEnablePersistentIndex() throws Exception {
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
            Assert.assertEquals(indexType, "CLOUD_NATIVE");

            String sql = "show create table lake_test.table_with_persistent_index";
            ShowCreateTableStmt showCreateTableStmt =
                    (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ShowResultSet resultSet = ShowExecutor.execute(showCreateTableStmt, connectContext);

            Assert.assertFalse(resultSet.getResultRows().isEmpty());
        }

        UtFrameUtils.addMockComputeNode(50001);
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Cannot create cloud native table with local persistent index",
                () -> createTable(
                "create table lake_test.table_with_persistent_index2\n" +
                        "(c0 int, c1 string, c2 int, c3 bigint)\n" +
                        "PRIMARY KEY(c0)\n" +
                        "distributed by hash(c0) buckets 2\n" +
                        "properties('enable_persistent_index' = 'true', 'persistent_index_type' = 'LOCAL');"));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.table_in_be_and_cn\n" +
                        "(c0 int, c1 string, c2 int, c3 bigint)\n" +
                        "PRIMARY KEY(c0)\n" +
                        "distributed by hash(c0) buckets 2"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "table_in_be_and_cn");
            // check table persistentIndex
            boolean enablePersistentIndex = lakeTable.enablePersistentIndex();
            Assert.assertTrue(enablePersistentIndex);

            String sql = "show create table lake_test.table_in_be_and_cn";
            ShowCreateTableStmt showCreateTableStmt =
                    (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ShowResultSet resultSet = ShowExecutor.execute(showCreateTableStmt, connectContext);

            Assert.assertNotEquals(0, resultSet.getResultRows().size());
        }
    }

    @Test
    public void testCreateLakeTableException() {
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
    public void testExplainRowCount() throws Exception {
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

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.duplicate_key_rowcount (key1 int, key2 varchar(10))\n" +
                        "distributed by hash(key1) buckets 3 properties('replication_num' = '1');"));
        checkLakeTable("lake_test", "duplicate_key_rowcount");

        // check explain result
        String sql = "select * from lake_test.duplicate_key_rowcount";
        String plan = UtFrameUtils.getVerboseFragmentPlan(connectContext, sql);
        Assert.assertTrue(plan.contains("actualRows=6"));
    }

    @Test
    public void testCreateLakeTableListPartition() throws StarRocksException {
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

    @Test
    public void testCreateLakeTableEnableCloudNativePersistentIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.table_with_cloud_native_persistent_index\n" +
                        "(c0 int, c1 string, c2 int, c3 bigint)\n" +
                        "PRIMARY KEY(c0)\n" +
                        "distributed by hash(c0) buckets 2\n" +
                        "properties('enable_persistent_index' = 'true', 'persistent_index_type' = 'cloud_native');"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "table_with_cloud_native_persistent_index");
            // check table persistentIndex
            boolean enablePersistentIndex = lakeTable.enablePersistentIndex();
            Assert.assertTrue(enablePersistentIndex);
            // check table persistentIndexType
            String indexType = lakeTable.getPersistentIndexTypeString();
            Assert.assertEquals("CLOUD_NATIVE", indexType);

            String sql = "show create table lake_test.table_with_cloud_native_persistent_index";
            ShowCreateTableStmt showCreateTableStmt =
                    (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            ShowResultSet resultSet = ShowExecutor.execute(showCreateTableStmt, connectContext);

            Assert.assertNotEquals(0, resultSet.getResultRows().size());
        }
    }

    @Test
    public void testCreateTableWithRollUp() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.table_with_rollup\n" +
                        "(c0 int, c1 string, c2 int, c3 bigint)\n" +
                        "DUPLICATE KEY(c0)\n" +
                        "distributed by hash(c0) buckets 2\n" +
                        "ROLLUP (mv1 (c0, c1));"));
        {
            LakeTable lakeTable = getLakeTable("lake_test", "table_with_rollup");
            Assert.assertEquals(2, lakeTable.getShardGroupIds().size());

            Assert.assertEquals(2, lakeTable.getAllPartitions().stream().findAny().
                    get().getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).size());

        }
    }
    @Test
    public void testRestoreColumnUniqueId() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table lake_test.test_unique_id\n" +
                        "(c0 int, c1 string, c2 int, c3 bigint)\n" +
                        "PRIMARY KEY(c0)\n" +
                        "distributed by hash(c0) buckets 2\n" +
                        "properties('enable_persistent_index' = 'true', 'persistent_index_type' = 'cloud_native');"));
        LakeTable lakeTable = getLakeTable("lake_test", "test_unique_id");
        // Clear unique id first
        lakeTable.setMaxColUniqueId(0);
        for (Column column : lakeTable.getColumns()) {
            column.setUniqueId(0);
        }
        lakeTable.gsonPostProcess();
        Assert.assertEquals(3, lakeTable.getMaxColUniqueId());
        Assert.assertEquals(0, lakeTable.getColumn("c0").getUniqueId());
        Assert.assertEquals(1, lakeTable.getColumn("c1").getUniqueId());
        Assert.assertEquals(2, lakeTable.getColumn("c2").getUniqueId());
        Assert.assertEquals(3, lakeTable.getColumn("c3").getUniqueId());
    }

    @Test
    public void testCreateTableWithUKFK() {
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE lake_test.region (\n" +
                        "  r_regionkey  INT NOT NULL,\n" +
                        "  r_name       VARCHAR(25) NOT NULL,\n" +
                        "  r_comment    VARCHAR(152)\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`r_regionkey`)\n" +
                        "DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        " 'replication_num' = '1',\n " +
                        " 'unique_constraints' = 'r_regionkey'\n" +
                        ");"));
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE lake_test.nation (\n" +
                        "  n_nationkey INT(11) NOT NULL,\n" +
                        "  n_name      VARCHAR(25) NOT NULL,\n" +
                        "  n_regionkey INT(11) NOT NULL,\n" +
                        "  n_comment   VARCHAR(152) NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`N_NATIONKEY`)\n" +
                        "DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        " 'replication_num' = '1',\n" +
                        " 'unique_constraints' = 'n_nationkey',\n" +
                        " 'foreign_key_constraints' = '(n_regionkey) references region(r_regionkey)'\n" +
                        ");"));
        LakeTable region = getLakeTable("lake_test", "region");
        LakeTable nation = getLakeTable("lake_test", "nation");
        Map<String, String> regionProps = region.getProperties();
        Assert.assertTrue(regionProps.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT));
        Map<String, String> nationProps = nation.getProperties();
        Assert.assertTrue(nationProps.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT));
        Assert.assertTrue(nationProps.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT));
    }
}
