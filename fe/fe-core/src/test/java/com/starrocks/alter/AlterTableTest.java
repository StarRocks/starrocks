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

package com.starrocks.alter;

import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threeten.extra.PeriodDuration;

import java.util.List;
import java.util.Set;

public class AlterTableTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        Config.enable_experimental_rowstore = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");

        UtFrameUtils.setUpForPersistTest();
    }

    @Test(expected = AnalysisException.class)
    public void testAlterTableBucketSize() throws Exception {
        starRocksAssert.useDatabase("test").withTable("CREATE TABLE test_alter_bucket_size (\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN MAXVALUE\n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "PROPERTIES(\n" +
                "\t\"replication_num\" = \"1\",\n" +
                "    \"storage_medium\" = \"SSD\",\n" +
                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                ");");
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "ALTER TABLE test_alter_bucket_size SET (\"bucket_size\" = \"-1\");";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);
    }

    @Test
    public void testAlterTableStorageCoolDownTTL() throws Exception {
        starRocksAssert.useDatabase("test").withTable("CREATE TABLE test_alter_cool_down_ttl (\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN MAXVALUE\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "\t\"replication_num\" = \"1\",\n" +
                "    \"storage_medium\" = \"SSD\",\n" +
                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                ");");
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "ALTER TABLE test_alter_cool_down_ttl SET (\"storage_cooldown_ttl\" = \"3 day\");";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);

        Table table = GlobalStateMgr.getCurrentState().getDb("test").getTable("test_alter_cool_down_ttl");
        OlapTable olapTable = (OlapTable) table;
        String storageCooldownTtl = olapTable.getTableProperty().getProperties().get("storage_cooldown_ttl");
        Assert.assertEquals("3 day", storageCooldownTtl);
        PeriodDuration storageCoolDownTTL = olapTable.getTableProperty().getStorageCoolDownTTL();
        Assert.assertEquals("P3D", storageCoolDownTTL.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testAlterTableStorageCoolDownTTLExcept() throws Exception {
        starRocksAssert.useDatabase("test").withTable("CREATE TABLE test_alter_cool_down_ttl_2 (\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN MAXVALUE\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "\t\"replication_num\" = \"1\",\n" +
                "    \"storage_medium\" = \"SSD\",\n" +
                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                ");");
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "ALTER TABLE test_alter_cool_down_ttl_2 SET (\"storage_cooldown_ttl\" = \"abc\");";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);
    }

    @Test
    public void testAlterTableStorageCoolDownTTLPartition() throws Exception {
        starRocksAssert.useDatabase("test").withTable("CREATE TABLE test_alter_cool_down_ttl_partition (\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN MAXVALUE\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "\t\"replication_num\" = \"1\",\n" +
                "    \"storage_medium\" = \"SSD\",\n" +
                "    \"storage_cooldown_ttl\" = \"1 day\"\n" +
                ");");
        ConnectContext ctx = starRocksAssert.getCtx();
        Table table = GlobalStateMgr.getCurrentState().getDb("test").getTable("test_alter_cool_down_ttl_partition");
        OlapTable olapTable = (OlapTable) table;
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        DataProperty p20200321 = rangePartitionInfo.getDataProperty(olapTable.getPartition("p20200321").getId());
        DataProperty p20200322 = rangePartitionInfo.getDataProperty(olapTable.getPartition("p20200322").getId());
        DataProperty p20200323 = rangePartitionInfo.getDataProperty(olapTable.getPartition("p20200323").getId());
        DataProperty p20200324 = rangePartitionInfo.getDataProperty(olapTable.getPartition("p20200324").getId());
        Assert.assertEquals("2020-03-23 00:00:00", TimeUtils.longToTimeString(p20200321.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-24 00:00:00", TimeUtils.longToTimeString(p20200322.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-25 00:00:00", TimeUtils.longToTimeString(p20200323.getCooldownTimeMs()));
        Assert.assertEquals("9999-12-31 23:59:59", TimeUtils.longToTimeString(p20200324.getCooldownTimeMs()));

        String sql = "ALTER TABLE test_alter_cool_down_ttl_partition\n" +
                "MODIFY PARTITION (*) SET(\"storage_cooldown_ttl\" = \"2 day\", \"storage_medium\" = \"SSD\");";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);

        p20200321 = rangePartitionInfo.getDataProperty(olapTable.getPartition("p20200321").getId());
        p20200322 = rangePartitionInfo.getDataProperty(olapTable.getPartition("p20200322").getId());
        p20200323 = rangePartitionInfo.getDataProperty(olapTable.getPartition("p20200323").getId());
        p20200324 = rangePartitionInfo.getDataProperty(olapTable.getPartition("p20200324").getId());
        Assert.assertEquals("2020-03-24 00:00:00", TimeUtils.longToTimeString(p20200321.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-25 00:00:00", TimeUtils.longToTimeString(p20200322.getCooldownTimeMs()));
        Assert.assertEquals("2020-03-26 00:00:00", TimeUtils.longToTimeString(p20200323.getCooldownTimeMs()));
        Assert.assertEquals("9999-12-31 23:59:59", TimeUtils.longToTimeString(p20200324.getCooldownTimeMs()));

    }

    @Test
    public void testAlterTablePartitionTTLInvalid() throws Exception {
        starRocksAssert.useDatabase("test").withTable("CREATE TABLE test_partition_live_number (\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN MAXVALUE\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "\t\"replication_num\" = \"1\",\n" +
                "    \"storage_medium\" = \"SSD\",\n" +
                "    \"partition_live_number\" = \"2\"\n" +
                ");");
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "ALTER TABLE test_partition_live_number SET(\"partition_live_number\" = \"-1\");";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);
        Set<Pair<Long, Long>> ttlPartitionInfo = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler().getTtlPartitionInfo();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("test_partition_live_number");
        Assert.assertFalse(ttlPartitionInfo.contains(new Pair<>(db.getId(), table.getId())));
        sql = "ALTER TABLE test_partition_live_number SET(\"partition_live_number\" = \"1\");";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);
        Assert.assertTrue(ttlPartitionInfo.contains(new Pair<>(db.getId(), table.getId())));
    }

    @Test
    public void testAlterTablePartitionStorageMedium() throws Exception {
        starRocksAssert.useDatabase("test").withTable("CREATE TABLE test_partition_storage_medium (\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN MAXVALUE\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "ALTER TABLE test_partition_storage_medium SET(\"default.storage_medium\" = \"SSD\");";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable olapTable = (OlapTable) db.getTable("test_partition_storage_medium");
        Assert.assertTrue(olapTable.getStorageMedium().equals("SSD"));
    }

    @Test
    public void testAlterTableStorageType() throws Exception {
        starRocksAssert.useDatabase("test").withTable("CREATE TABLE test_storage_type (\n" +
                "event_day DATE,\n" +
                "site_id INT DEFAULT '10',\n" +
                "city_code VARCHAR(100),\n" +
                "user_name VARCHAR(32) DEFAULT '',\n" +
                "pv BIGINT DEFAULT '2'\n" +
                ")\n" +
                "PRIMARY KEY(event_day, site_id, city_code, user_name)\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_type\" = \"column_with_row\"\n" +
                ");");
        ConnectContext ctx = starRocksAssert.getCtx();

        String sql1 = "ALTER TABLE test_storage_type SET(\"storage_type\" = \"column\");";
        AnalysisException e1 =
                Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql1, ctx));
        Assert.assertTrue(e1.getMessage().contains("Can't change storage type"));
        String sql2 = "ALTER TABLE test_storage_type SET(\"storage_type\" = \"column_with_row\");";
        AnalysisException e2 =
                Assert.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql2, ctx));
        Assert.assertTrue(e2.getMessage().contains("Can't change storage type"));

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable olapTable = (OlapTable) db.getTable("test_storage_type");
        Assert.assertTrue(olapTable.getStorageType().equals(TStorageType.COLUMN_WITH_ROW));
    }

    public void testAlterTableLocationProp() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

        // add label to backend
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        System.out.println(systemInfoService.getBackends());
        List<Long> backendIds = systemInfoService.getBackendIds();
        Backend backend = systemInfoService.getBackend(backendIds.get(0));
        String modifyBackendPropSqlStr = "alter system modify backend '" + backend.getHost() +
                ":" + backend.getHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = 'rack:rack1')";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(modifyBackendPropSqlStr, connectContext),
                connectContext);

        String sql = "CREATE TABLE test.`test_location_alter` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        starRocksAssert.withTable(sql);

        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage initialImage = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(initialImage.getDataOutputStream());

        // ** test alter table location to rack:*
        sql = "ALTER TABLE test.`test_location_alter` SET ('" +
                PropertyAnalyzer.PROPERTIES_LABELS_LOCATION + "' = 'rack:*');";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext),
                connectContext);
        Assert.assertEquals("rack", ((OlapTable) testDb.getTable("test_location_alter"))
                .getLocation().keySet().stream().findFirst().get());
        Assert.assertEquals("*", ((OlapTable) testDb.getTable("test_location_alter"))
                .getLocation().get("rack").stream().findFirst().get());

        // ** test alter table location to nil
        sql = "ALTER TABLE test.`test_location_alter` SET ('" +
                PropertyAnalyzer.PROPERTIES_LABELS_LOCATION + "' = '');";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext),
                connectContext);
        Assert.assertNull(((OlapTable) testDb.getTable("test_location_alter"))
                .getLocation());

        // ** test replay from edit log: alter to rack:*
        LocalMetastore localMetastoreFollower = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        localMetastoreFollower.load(new SRMetaBlockReader(initialImage.getDataInputStream()));
        ModifyTablePropertyOperationLog info = (ModifyTablePropertyOperationLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);
        localMetastoreFollower.replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, info);
        OlapTable olapTable = (OlapTable) localMetastoreFollower.getDb("test")
                .getTable("test_location_alter");
        System.out.println(olapTable.getLocation());
        Assert.assertEquals(1, olapTable.getLocation().size());
        Assert.assertTrue(olapTable.getLocation().containsKey("rack"));


        // ** test replay from edit log: alter to nil
        info = (ModifyTablePropertyOperationLog)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_TABLE_PROPERTIES);
        localMetastoreFollower.replayModifyTableProperty(OperationType.OP_ALTER_TABLE_PROPERTIES, info);
        olapTable = (OlapTable) localMetastoreFollower.getDb("test")
                .getTable("test_location_alter");
        System.out.println(olapTable.getLocation());
        Assert.assertNull(olapTable.getLocation());
    }

    @Test
    public void testAlterColocateTableLocationProp() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

        // add label to backend
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        System.out.println(systemInfoService.getBackends());
        List<Long> backendIds = systemInfoService.getBackendIds();
        Backend backend = systemInfoService.getBackend(backendIds.get(0));
        String modifyBackendPropSqlStr = "alter system modify backend '" + backend.getHost() +
                ":" + backend.getHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = 'rack:rack1')";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(modifyBackendPropSqlStr, connectContext),
                connectContext);

        String sql = "CREATE TABLE test.`test_location_colocate_alter1` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"cg1\"\n" +
                ");";
        starRocksAssert.withTable(sql);

        OlapTable olapTable = (OlapTable) testDb.getTable("test_location_colocate_alter1");
        Assert.assertNull(olapTable.getLocation());

        sql = "ALTER TABLE test.`test_location_colocate_alter1` SET ('" +
                PropertyAnalyzer.PROPERTIES_LABELS_LOCATION + "' = 'rack:*');";
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot set location for colocate table"));
        }
    }

    @Test
    public void testAlterLocationPropTableToColocate() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");

        // add label to backend
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        System.out.println(systemInfoService.getBackends());
        List<Long> backendIds = systemInfoService.getBackendIds();
        Backend backend = systemInfoService.getBackend(backendIds.get(0));
        String modifyBackendPropSqlStr = "alter system modify backend '" + backend.getHost() +
                ":" + backend.getHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = 'rack:rack1')";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(modifyBackendPropSqlStr, connectContext),
                connectContext);

        String sql = "CREATE TABLE test.`test_location_colocate_alter2` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(sql);

        OlapTable olapTable = (OlapTable) testDb.getTable("test_location_colocate_alter2");
        Assert.assertTrue(olapTable.getLocation().containsKey("*"));

        sql = "ALTER TABLE test.`test_location_colocate_alter1` SET ('" +
                PropertyAnalyzer.PROPERTIES_COLOCATE_WITH + "' = 'cg1');";
        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
        } catch (DdlException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("table has location property and cannot be colocated"));
        }
    }
}
