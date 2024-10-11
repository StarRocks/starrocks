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

package com.starrocks.externalcooldown;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class ExternalCooldownPartitionSelectorTest {
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

        Config.default_replication_num = 1;

        starRocksAssert.withDatabase("test");
        starRocksAssert.useDatabase("test");

        new MockUp<IcebergMetadata>() {
            @Mock
            public Table getTable(String dbName, String tblName) {
                return new IcebergTable(1, "iceberg_tbl", "iceberg_catalog",
                        "iceberg_catalog", "iceberg_db",
                        "table1", "", Lists.newArrayList(), new BaseTable(null, ""), Maps.newHashMap());
            }
        };
        new MockUp<IcebergTable>() {
            @Mock
            public String getTableIdentifier() {
                return "iceberg_catalog.iceberg_db.iceberg_tbl";
            }
        };
        new MockUp<BaseTable>() {
            @Mock
            public PartitionSpec spec() {
                return PartitionSpec.unpartitioned();
            }
        };

        ConnectorMgr connectorMgr = GlobalStateMgr.getCurrentState().getConnectorMgr();
        Map<String, String> properties = Maps.newHashMap();

        properties.put("type", "iceberg");
        properties.put("iceberg.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        connectorMgr.createConnector(new ConnectorContext("iceberg_catalog", "iceberg", properties), false);

        starRocksAssert.withTable("CREATE TABLE test.tbl1\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2024-03-01 00:00:00'),('2024-03-02 00:00:00')),\n" +
                "    PARTITION p2 values [('2024-03-02 00:00:00'),('2024-03-03 00:00:00')),\n" +
                "    PARTITION p3 values [('2024-03-03 00:00:00'),('2024-03-04 00:00:00')),\n" +
                "    PARTITION p4 values [('2024-03-04 00:00:00'),('2024-03-05 00:00:00')),\n" +
                "    PARTITION p5 values [('2024-03-05 00:00:00'),('2024-03-06 00:00:00'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 1\n" +
                "PROPERTIES(" +
                "'replication_num' = '1',\n" +
                "'external_cooldown_target' = 'iceberg_catalog.iceberg_db.iceberg_tbl',\n" +
                "'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "'external_cooldown_wait_second' = '1'\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE test.tbl2\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2024-03-01 00:00:00'),('2024-03-02 00:00:00')),\n" +
                "    PARTITION p2 values [('2024-03-02 00:00:00'),('2024-03-03 00:00:00')),\n" +
                "    PARTITION p3 values [('2024-03-03 00:00:00'),('2024-03-04 00:00:00')),\n" +
                "    PARTITION p4 values [('2024-03-04 00:00:00'),('2024-03-05 00:00:00')),\n" +
                "    PARTITION p5 values [('2024-03-05 00:00:00'),('2024-03-06 00:00:00'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 1\n" +
                "PROPERTIES(" +
                "'replication_num' = '1',\n" +
                "'external_cooldown_target' = 'iceberg_catalog.iceberg_db.iceberg_tbl',\n" +
                "'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "'external_cooldown_wait_second' = '1'\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE test.tbl3\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2024-03-01 00:00:00'),('2024-03-02 00:00:00')),\n" +
                "    PARTITION p2 values [('2024-03-02 00:00:00'),('2024-03-03 00:00:00')),\n" +
                "    PARTITION p3 values [('2024-03-03 00:00:00'),('2024-03-04 00:00:00')),\n" +
                "    PARTITION p4 values [('2024-03-04 00:00:00'),('2024-03-05 00:00:00')),\n" +
                "    PARTITION p5 values [('2024-03-05 00:00:00'),('2024-03-06 00:00:00'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 1\n" +
                "PROPERTIES(" +
                "'replication_num' = '1',\n" +
                "'external_cooldown_target' = 'iceberg_catalog.iceberg_db.iceberg_tbl',\n" +
                "'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "'external_cooldown_wait_second' = '1'\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE test.tbl4\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 1\n" +
                "PROPERTIES(" +
                "'replication_num' = '1',\n" +
                "'external_cooldown_target' = 'iceberg_catalog.iceberg_db.iceberg_tbl',\n" +
                "'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 10 SECOND'\n," +
                "'external_cooldown_wait_second' = '1'\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE test.tbl5 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN ('beijing','chongqing') ,\n" +
                "     PARTITION p2 VALUES IN ('shenzhen','hainan') ,\n" +
                "     PARTITION p3 VALUES IN ('guangdong') \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    'replication_num' = '1',\n" +
                "    'external_cooldown_target' = 'iceberg_catalog.iceberg_db.iceberg_tbl',\n" +
                "    'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "    'external_cooldown_wait_second' = '1'\n" +
                ")");
    }

    @Test
    public void testCooldownWaitSecond() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl1");

        long updateTime = System.currentTimeMillis() - 2000;
        Partition p1 = table.getPartition("p1");
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);
        Partition p2 = table.getPartition("p2");
        p2.updateVisibleVersion(p2.getVisibleVersion() + 1, updateTime);
        Partition p3 = table.getPartition("p3");
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, updateTime);
        Partition p5 = table.getPartition("p5");
        p5.updateVisibleVersion(p5.getVisibleVersion() + 1, updateTime);

        ConnectContext ctx = starRocksAssert.getCtx();
        String sql3 = "ALTER TABLE test.tbl1 SET(\"external_cooldown_wait_second\" = \"1\");";
        AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql3, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt3);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(testDb, (OlapTable) table);
        Assert.assertTrue(selector.isTableSatisfied());

        List<Partition> partitions = selector.getSatisfiedPartitions(-1);
        Assert.assertEquals(4, partitions.size());
        Set<String> satisfiedPartitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertTrue("p1 satisfied cooldown condition", satisfiedPartitionNames.contains("p1"));
        Assert.assertTrue("p2 satisfied cooldown condition", satisfiedPartitionNames.contains("p2"));
        Assert.assertTrue("p3 satisfied cooldown condition", satisfiedPartitionNames.contains("p3"));
        Assert.assertFalse("p4 not satisfied cooldown condition", satisfiedPartitionNames.contains("p4"));
        Assert.assertTrue("p5 satisfied cooldown condition", satisfiedPartitionNames.contains("p5"));
    }

    @Test
    public void testPartitionStartEnd() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl2");

        long updateTime = System.currentTimeMillis() - 2000;
        Partition p1 = table.getPartition("p1");
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);
        Partition p2 = table.getPartition("p2");
        p2.updateVisibleVersion(p2.getVisibleVersion() + 1, updateTime);
        Partition p3 = table.getPartition("p3");
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, updateTime);
        Partition p5 = table.getPartition("p5");
        p5.updateVisibleVersion(p5.getVisibleVersion() + 1, updateTime);

        ConnectContext ctx = starRocksAssert.getCtx();
        String sql3 = "ALTER TABLE test.tbl2 SET(\"external_cooldown_wait_second\" = \"1\");";
        AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql3, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt3);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(
                testDb, (OlapTable) table, "2024-03-02 00:00:00", "2024-03-05 00:00:00", false);
        Assert.assertTrue(selector.isTableSatisfied());

        List<Partition> partitions = selector.getSatisfiedPartitions(-1);
        Assert.assertEquals(2, partitions.size());
        Set<String> satisfiedPartitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertFalse("p1 not in partition range", satisfiedPartitionNames.contains("p1"));
        Assert.assertTrue("p2 in partition range", satisfiedPartitionNames.contains("p2"));
        Assert.assertTrue("p3 in partition range", satisfiedPartitionNames.contains("p3"));
        Assert.assertFalse("p4 not in partition range", satisfiedPartitionNames.contains("p4"));
        Assert.assertFalse("p5 not in partition range", satisfiedPartitionNames.contains("p5"));
    }

    @Test
    public void testForce() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl3");

        Partition p1 = table.getPartition("p1");
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1);
        Partition p2 = table.getPartition("p2");
        p2.updateVisibleVersion(p2.getVisibleVersion() + 1);
        Partition p4 = table.getPartition("p4");
        p4.updateVisibleVersion(p4.getVisibleVersion() + 1);
        Partition p5 = table.getPartition("p5");
        p5.updateVisibleVersion(p5.getVisibleVersion() + 1);

        ConnectContext ctx = starRocksAssert.getCtx();
        String sql3 = "ALTER TABLE test.tbl3 SET(\"external_cooldown_wait_second\" = \"7200\");";
        AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql3, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt3);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(
                testDb, (OlapTable) table, "2024-03-02 00:00:00", "2024-03-05 00:00:00", true);
        Assert.assertTrue(selector.isTableSatisfied());

        List<Partition> partitions = selector.getSatisfiedPartitions(-1);
        Assert.assertEquals(2, partitions.size());
        Set<String> satisfiedPartitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertFalse("p1 not in partition range", satisfiedPartitionNames.contains("p1"));
        Assert.assertTrue("p2 in partition range", satisfiedPartitionNames.contains("p2"));
        Assert.assertFalse("p3 not satisfied condition", satisfiedPartitionNames.contains("p3"));
        Assert.assertTrue("p4 in partition range", satisfiedPartitionNames.contains("p4"));
        Assert.assertFalse("p5 not in partition range", satisfiedPartitionNames.contains("p5"));
    }

    @Test
    public void testSinglePartitionTable() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl4");

        long updateTime = System.currentTimeMillis() - 2000;
        Partition p1 = table.getPartition("tbl4");
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(testDb, (OlapTable) table);
        Assert.assertTrue(selector.isTableSatisfied());

        List<Partition> partitions = selector.getSatisfiedPartitions(-1);
        Assert.assertEquals(1, partitions.size());
        Set<String> satisfiedPartitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertTrue("tbl4 in partition range", satisfiedPartitionNames.contains("tbl4"));
    }

    @Test
    public void testListPartitionTable() {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl5");

        long updateTime = System.currentTimeMillis() - 2000;
        Partition p1 = table.getPartition("p1");
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);
        Partition p3 = table.getPartition("p3");
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, updateTime);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(testDb, (OlapTable) table);
        Assert.assertTrue(selector.isTableSatisfied());

        List<Partition> partitions = selector.getSatisfiedPartitions(-1);
        Assert.assertEquals(2, partitions.size());
        Set<String> satisfiedPartitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertTrue("p1 satisfied condition", satisfiedPartitionNames.contains("p1"));
        Assert.assertFalse("p2 not satisfied condition", satisfiedPartitionNames.contains("p2"));
        Assert.assertTrue("p3 satisfied condition", satisfiedPartitionNames.contains("p3"));
    }
}