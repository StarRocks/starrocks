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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModifyDatacaheEnableTest {
    private static final Logger LOG = LogManager.getLogger(ModifyDatacaheEnableTest.class);
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database lake_test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE IF NOT EXISTS `lake_test`.`lake_table` (\n" +
                        "    `recruit_date`  DATE           NOT NULL COMMENT 'YYYY-MM-DD',\n" +
                        "    `region_num`    TINYINT        COMMENT 'range [-128, 127]',\n" +
                        "    `num_plate`     SMALLINT       COMMENT 'range [-32768, 32767]',\n" +
                        "    `tel`           INT            COMMENT 'range [-2147483648, 2147483647]',\n" +
                        "    `id`            BIGINT         COMMENT 'range [-2^63 + 1 ~ 2^63 - 1]',\n" +
                        "    `password`      LARGEINT       COMMENT 'range [-2^127 + 1 ~ 2^127 - 1]',\n" +
                        "    `name`          CHAR(20)       NOT NULL COMMENT 'range char(m),m in (1-255)',\n" +
                        "    `profile`       VARCHAR(500)   NOT NULL COMMENT 'upper limit value 1048576 bytes',\n" +
                        "    `hobby`         STRING         NOT NULL COMMENT 'upper limit value 65533 bytes',\n" +
                        "    `leave_time`    DATETIME       COMMENT 'YYYY-MM-DD HH:MM:SS'\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`recruit_date`, `region_num`)\n" +
                        "PARTITION BY RANGE(`recruit_date`)\n" +
                        "(\n" +
                        "    PARTITION p1 VALUES [('2022-03-11'), ('2022-03-12')),\n" +
                        "    PARTITION p2 VALUES [('2022-03-12'), ('2022-03-13')),\n" +
                        "    PARTITION p3 VALUES [('2022-03-13'), ('2022-03-14')),\n" +
                        "    PARTITION p4 VALUES [('2022-03-14'), ('2022-03-15')),\n" +
                        "    PARTITION p5 VALUES [('2022-03-15'), ('2022-03-16'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`recruit_date`, `region_num`) BUCKETS 100\n" +
                        "PROPERTIES (\n" +
                        "    'datacache.partition_duration' = '1 days',\n" +
                        "    'datacache.enable' = 'true',\n" +
                        "    'storage_volume' = 'builtin_storage_volume',\n" +
                        "    'enable_async_write_back' = 'false',\n" +
                        "    'enable_persistent_index' = 'false'\n" +
                        ");"
        ));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE IF NOT EXISTS `lake_test`.`base` (\n" +
                        "    `k1` date NULL,\n" +
                        "    `k2` int(11) NULL,\n" +
                        "    `v1` int(11) SUM NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`k1`, `k2`)\n" +
                        "COMMENT 'OLAP'\n" +
                        "PARTITION BY RANGE(`k1`)\n" +
                        "(\n" +
                        "    PARTITION p1 VALUES [('0000-01-01'), ('2020-02-01')),\n" +
                        "    PARTITION p2 VALUES [('2020-02-01'), ('2020-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "    'replication_num' = '1',\n" +
                        "    'datacache.enable' = 'true'\n" +
                        ");"
        ));
        checkLakeTable("lake_test", "base");
        ExceptionChecker.expectThrowsNoException(() -> createMaterializedView(
                "CREATE MATERIALIZED VIEW `lake_test`.`lake_mv`\n" +
                        "DISTRIBUTED BY HASH(`k1`)\n" +
                        "PROPERTIES (\n" +
                        "    'replication_num' = '1',\n" +
                        "    'datacache.enable' = 'true',\n" +
                        "    'datacache.partition_duration' = '25 hour'\n" +
                        ")\n" +
                        "AS SELECT `k1` FROM `lake_test`.`base`;"
        ));
        checkMaterializedView("lake_test", "lake_mv");

        new MockUp<Preconditions>() {
            @Mock
            public void checkArgument(boolean param) {

            }
        };
    }

    @AfterClass
    public static void afterClass() {
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
    }

    private static void createMaterializedView(String sql) throws Exception {
        CreateMaterializedViewStatement createMVStmt =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMVStmt);
    }

    private static void checkLakeTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(tableName);
        Assert.assertTrue(table.isCloudNativeTable());
    }

    private static void checkMaterializedView(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        List<MaterializedView> mvsList = db.getMaterializedViews();
        for (MaterializedView lakeMv : mvsList) {
            Assert.assertTrue(lakeMv.isCloudNativeMaterializedView());
        }
    }

    private LakeTable getLakeTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(tableName);
        Assert.assertTrue(table.isCloudNativeTable());
        return (LakeTable) table;
    }

    private LakeMaterializedView getMaterializedView(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        List<MaterializedView> mvsList = db.getMaterializedViews();
        for (MaterializedView lakeMv : mvsList) {
            if (lakeMv.getName().equals(mvName)) {
                return (LakeMaterializedView) lakeMv;
            }
        }
        return null;
    }

    private static void alterTableProperties(String dbName, String tableName, boolean isEnable) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        OlapTable table = (OlapTable) db.getTable(tableName);
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, String.valueOf(isEnable));
        ExceptionChecker.expectThrowsNoException(() ->
                GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, table, properties));
    }

    private static void alterPartitionProperties(String dbName, String tableName,
            List<String> partitionNames, boolean isEnable) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        OlapTable table = (OlapTable) db.getTable(tableName);
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_DATACACHE_ENABLE, String.valueOf(isEnable));
        ExceptionChecker.expectThrowsNoException(() ->
                GlobalStateMgr.getCurrentState().getAlterJobMgr().modifyPartitionsProperty(
                        db, table, partitionNames, properties));
    }

    @Test
    public void testModifyTableDatacacheEnable() throws UserException {
        LakeTable table = getLakeTable("lake_test", "lake_table");
        { // check the original state of datacache.enable
            StorageInfo storageInfo = table.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertTrue(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = table.getPartitionInfo();
            Collection<Partition> partitions = table.getPartitions();
            for (Partition partition : partitions) {
                DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partition.getId());
                Assert.assertTrue(dataCacheInfo.isEnabled());
            }
        }
        alterTableProperties("lake_test", "lake_table", false);
        { // check the state of datacache.enable after alter table properties
            StorageInfo storageInfo = table.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertFalse(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = table.getPartitionInfo();
            Collection<Partition> partitions = table.getPartitions();
            for (Partition partition : partitions) {
                DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partition.getId());
                Assert.assertFalse(dataCacheInfo.isEnabled());
            }
        }
        alterPartitionProperties("lake_test", "lake_table", List.of("p1", "p2"), true);
        { // check the state of datacache.enable after alter partition properties
            StorageInfo storageInfo = table.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertFalse(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = table.getPartitionInfo();
            Collection<Partition> partitions = table.getPartitions();
            for (Partition partition : partitions) {
                DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partition.getId());
                if (partition.getName().equals("p1") || partition.getName().equals("p2")) {
                    Assert.assertTrue(dataCacheInfo.isEnabled());
                } else {
                    Assert.assertFalse(dataCacheInfo.isEnabled());
                }
            }
        }
        alterTableProperties("lake_test", "lake_table", true);
        { // check the state of datacache.enable after alter table properties
            StorageInfo storageInfo = table.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertTrue(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = table.getPartitionInfo();
            Collection<Partition> partitions = table.getPartitions();
            for (Partition partition : partitions) {
                DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partition.getId());
                Assert.assertTrue(dataCacheInfo.isEnabled());
            }
        }
        alterPartitionProperties("lake_test", "lake_table", List.of("p4", "p5"), false);
        { // check the state of datacache.enable after alter partition properties
            StorageInfo storageInfo = table.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertTrue(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = table.getPartitionInfo();
            Collection<Partition> partitions = table.getPartitions();
            for (Partition partition : partitions) {
                DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partition.getId());
                if (partition.getName().equals("p4") || partition.getName().equals("p5")) {
                    Assert.assertFalse(dataCacheInfo.isEnabled());
                } else {
                    Assert.assertTrue(dataCacheInfo.isEnabled());
                }
            }
        }
        alterTableProperties("lake_test", "lake_table", false);
        { // check the state of datacache.enable after alter table properties
            StorageInfo storageInfo = table.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertFalse(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = table.getPartitionInfo();
            Collection<Partition> partitions = table.getPartitions();
            for (Partition partition : partitions) {
                DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partition.getId());
                Assert.assertFalse(dataCacheInfo.isEnabled());
            }
        }
    }

    @Test
    public void testModifyPartitionDatacacheEnable() throws UserException {
        LakeMaterializedView lakeMv = getMaterializedView("lake_test", "lake_mv");
        { // check the original state of datacache.enable
            StorageInfo storageInfo = lakeMv.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertTrue(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = lakeMv.getPartitionInfo();
            Collection<Partition> partitions = lakeMv.getPartitions();
            List<Partition> partitionsList = new ArrayList<>(partitions);
            DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partitionsList.get(0).getId());
            Assert.assertTrue(dataCacheInfo.isEnabled());
        }
        alterTableProperties("lake_test", "lake_mv", false);
        { // check the state of datacache.enable after alter table properties
            StorageInfo storageInfo = lakeMv.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertFalse(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = lakeMv.getPartitionInfo();
            Collection<Partition> partitions = lakeMv.getPartitions();
            List<Partition> partitionsList = new ArrayList<>(partitions);
            DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partitionsList.get(0).getId());
            Assert.assertFalse(dataCacheInfo.isEnabled());
        }
        alterPartitionProperties("lake_test", "lake_mv", List.of("lake_mv"), true);
        { // check the state of datacache.enable after alter partition properties
            StorageInfo storageInfo = lakeMv.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertFalse(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = lakeMv.getPartitionInfo();
            Collection<Partition> partitions = lakeMv.getPartitions();
            List<Partition> partitionsList = new ArrayList<>(partitions);
            DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partitionsList.get(0).getId());
            Assert.assertTrue(dataCacheInfo.isEnabled());
        }
        alterTableProperties("lake_test", "lake_mv", true);
        { // check the state of datacache.enable after alter table properties
            StorageInfo storageInfo = lakeMv.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertTrue(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = lakeMv.getPartitionInfo();
            Collection<Partition> partitions = lakeMv.getPartitions();
            List<Partition> partitionsList = new ArrayList<>(partitions);
            DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partitionsList.get(0).getId());
            Assert.assertTrue(dataCacheInfo.isEnabled());
        }
        alterPartitionProperties("lake_test", "lake_mv", List.of("lake_mv"), false);
        { // check the state of datacache.enable after alter partition properties
            StorageInfo storageInfo = lakeMv.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertTrue(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = lakeMv.getPartitionInfo();
            Collection<Partition> partitions = lakeMv.getPartitions();
            List<Partition> partitionsList = new ArrayList<>(partitions);
            DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partitionsList.get(0).getId());
            Assert.assertFalse(dataCacheInfo.isEnabled());
        }
        alterTableProperties("lake_test", "lake_mv", false);
        { // check the state of datacache.enable after alter table properties
            StorageInfo storageInfo = lakeMv.getTableProperty().getStorageInfo();
            Assert.assertNotNull(storageInfo);
            Assert.assertFalse(storageInfo.getDataCacheInfo().isEnabled());
            PartitionInfo partitionInfo = lakeMv.getPartitionInfo();
            Collection<Partition> partitions = lakeMv.getPartitions();
            List<Partition> partitionsList = new ArrayList<>(partitions);
            DataCacheInfo dataCacheInfo = partitionInfo.getDataCacheInfo(partitionsList.get(0).getId());
            Assert.assertFalse(dataCacheInfo.isEnabled());
        }
    }
}
