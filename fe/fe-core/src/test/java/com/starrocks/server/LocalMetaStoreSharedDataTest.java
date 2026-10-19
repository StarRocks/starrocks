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

package com.starrocks.server;

import com.google.gson.JsonObject;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LocalMetaStoreSharedDataTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster(true, RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable(
                        "CREATE TABLE test.t1(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
    }

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void teardown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAlterDatabaseSetProperty() {
        String dbName = "db_for_alter_db_set_property";
        Assertions.assertDoesNotThrow(() -> starRocksAssert.withDatabase(dbName));

        DdlException ddlException = Assertions.assertThrows(DdlException.class, () -> starRocksAssert.ddl(
                String.format("ALTER DATABASE %s SET ('prop1' = 'value1')", dbName)));
        Assertions.assertEquals("Unknown properties: prop1", ddlException.getMessage());

        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        StorageVolumeMgr svMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        StorageVolume defaultSv = svMgr.getDefaultStorageVolume();

        Database db = metastore.getDb(dbName);
        String dbSvId = svMgr.getStorageVolumeIdOfDb(db.getId());
        Assertions.assertEquals(defaultSv.getId(), dbSvId);

        // Nothing changed
        Assertions.assertDoesNotThrow(() -> {
            starRocksAssert.ddl(
                    String.format("ALTER DATABASE %s SET ('storage_volume' = '%s')", dbName, defaultSv.getName()));
        });

        // Bind to a non-exist storage volume
        DdlException exception1 = Assertions.assertThrows(DdlException.class, () -> starRocksAssert.ddl(
                String.format("ALTER DATABASE %s SET ('storage_volume' = 'non-exist-storage-volume')", dbName)));
        Assertions.assertEquals("Storage volume non-exist-storage-volume does not exist", exception1.getMessage());

        Assertions.assertDoesNotThrow(() -> {
            starRocksAssert.ddl(
                    "CREATE STORAGE VOLUME hdfsvolDisabled type = HDFS locations = ('hdfs://localhost:8020/user/sv') PROPERTIES ('enabled'='false');");
        });
        StorageVolume hdfsSvDisabled = svMgr.getStorageVolumeByName("hdfsvolDisabled");
        Assertions.assertFalse(hdfsSvDisabled.getEnabled());

        // Bind to a disabled storage volume
        DdlException exception2 = Assertions.assertThrows(DdlException.class, () -> starRocksAssert.ddl(
                String.format("ALTER DATABASE %s SET ('storage_volume' = 'hdfsvolDisabled')", dbName)));
        Assertions.assertEquals("Storage volume hdfsvolDisabled is disabled", exception2.getMessage());

        Assertions.assertDoesNotThrow(() -> {
            starRocksAssert.ddl(
                    "CREATE STORAGE VOLUME hdfsvol type = HDFS locations = ('hdfs://localhost:8020/user/sv');");
        });
        StorageVolume hdfsSv = svMgr.getStorageVolumeByName("hdfsvol");
        Assertions.assertTrue(hdfsSv.getEnabled());

        // Bind to an enabled storage volume
        Assertions.assertDoesNotThrow(() -> {
            starRocksAssert.ddl(
                    String.format("ALTER DATABASE %s SET ('storage_volume' = '%s')", dbName, hdfsSv.getName()));
        });
        String dbSvId2 = svMgr.getStorageVolumeIdOfDb(db.getId());
        Assertions.assertEquals(hdfsSv.getId(), dbSvId2);
    }

    @Test
    public void testInvalidStatisticAutoCollectLeavesNoStorageVolumeBinding() throws Exception {
        StorageVolumeMgr svMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        int boundTables = svMgr.storageVolumeToTables.values().stream().mapToInt(Set::size).sum();

        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "CREATE TABLE test.t_invalid_auto_collect(k1 int) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 " +
                        "PROPERTIES ('enable_statistic_auto_collect' = 'off')", connectContext);
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(stmt));
        Assertions.assertTrue(exception.getMessage().contains("enable_statistic_auto_collect"), exception.getMessage());
        Assertions.assertEquals(boundTables, svMgr.storageVolumeToTables.values().stream().mapToInt(Set::size).sum());
    }

    @Test
    public void testAlterTablePropertiesEnableStatisticAutoCollect() throws Exception {
        starRocksAssert.useDatabase("test").withTable("CREATE TABLE test.t_auto_collect(k1 int)"
                + " distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = metastore.getDb("test");
        OlapTable table = (OlapTable) metastore.getTable(db.getFullName(), "t_auto_collect");
        Assertions.assertTrue(table.enableStatisticAutoCollect());

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_STATISTIC_AUTO_COLLECT, "false");
        metastore.alterTableProperties(db, table, properties);

        Assertions.assertFalse(table.enableStatisticAutoCollect());
        Assertions.assertEquals("false", table.getProperties().get(PropertyAnalyzer.PROPERTIES_ENABLE_STATISTIC_AUTO_COLLECT));
        Assertions.assertFalse(table.getTableProperty().copy().enableStatisticAutoCollect());
        TableProperty fromImage = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table.getTableProperty()),
                TableProperty.class);
        Assertions.assertFalse(fromImage.enableStatisticAutoCollect());
        JsonObject olderFeImage = GsonUtils.GSON.toJsonTree(table.getTableProperty()).getAsJsonObject();
        olderFeImage.remove("enableStatisticAutoCollect");
        TableProperty fromOlderFeImage = GsonUtils.GSON.fromJson(olderFeImage, TableProperty.class);
        Assertions.assertFalse(fromOlderFeImage.enableStatisticAutoCollect());

        ModifyTablePropertyOperationLog replayInfo = (ModifyTablePropertyOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);
        table.setEnableStatisticAutoCollect(true);
        metastore.replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, replayInfo);
        Assertions.assertFalse(table.enableStatisticAutoCollect());
    }
}
