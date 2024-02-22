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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.thrift.TStorageType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.BaseTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threeten.extra.PeriodDuration;

import java.util.Map;
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

    @Test
    public void testAlterTableExternalCoolDownConfig() throws Exception {
        new MockUp<IcebergMetadata>() {
            @Mock
            public Table getTable(String dbName, String tblName) {
                return new IcebergTable(1, "table1", "iceberg_catalog",
                        "iceberg_catalog", "iceberg_db",
                        "table1", Lists.newArrayList(), new BaseTable(null, ""), Maps.newHashMap());
            }
        };
        new MockUp<IcebergTable>() {
            @Mock
            public String getTableIdentifier() {
                return "iceberg_catalog.iceberg_db.table1";
            }
        };

        ConnectorMgr connectorMgr = GlobalStateMgr.getCurrentState().getConnectorMgr();
        Map<String, String> properties = Maps.newHashMap();

        properties.put("type", "iceberg");
        properties.put("iceberg.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        connectorMgr.createConnector(new ConnectorContext("iceberg_catalog", "iceberg", properties));

        starRocksAssert.useDatabase("test").withTable("CREATE TABLE test_external_cooldown (\n" +
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
                "\"replication_num\" = \"1\",\n" +
                "\"external_cooldown_target\" = \"iceberg_catalog.db.external_iceberg_tbl\",\n" +
                "\"external_cooldown_schedule\" = \"START 01:00 END 07:59 EVERY INTERVAL 1m\"\n," +
                "\"external_cooldown_wait_second\" = \"3600\"\n" +
                ");");

        ConnectContext ctx = starRocksAssert.getCtx();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable olapTable = (OlapTable) db.getTable("test_external_cooldown");

        Assert.assertNotNull(olapTable.getExternalCoolDownTarget());
        Assert.assertTrue(olapTable.getExternalCoolDownTarget().equals("iceberg_catalog.db.external_iceberg_tbl"));
        Assert.assertTrue(olapTable.getExternalCoolDownSchedule().equals("START 01:00 END 07:59 EVERY INTERVAL 1m"));
        Assert.assertTrue(olapTable.getExternalCoolDownWaitSecond().equals(3600L));
        String sql = "ALTER TABLE test_external_cooldown SET(\"external_cooldown_target\" = \"iceberg_catalog.db.tbl\");";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt);
        Assert.assertTrue(olapTable.getExternalCoolDownTarget().equals("iceberg_catalog.db.tbl"));

        String sql2 = "ALTER TABLE test_external_cooldown " +
                "SET(\"external_cooldown_schedule\" = \"START 00:00 END 07:59 EVERY INTERVAL 2m\");";
        AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt2);
        Assert.assertTrue(olapTable.getExternalCoolDownSchedule().equals(
                "START 00:00 END 07:59 EVERY INTERVAL 2m"));

        String sql3 = "ALTER TABLE test_external_cooldown SET(\"external_cooldown_wait_second\" = \"7200\");";
        AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql3, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(alterTableStmt3);
        Assert.assertTrue(olapTable.getExternalCoolDownWaitSecond().equals(7200L));
    }
}
