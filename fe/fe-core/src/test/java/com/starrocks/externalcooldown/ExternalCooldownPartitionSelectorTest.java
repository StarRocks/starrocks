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

import com.google.common.collect.Range;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mockStatic;


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

        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);

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
                "'external_cooldown_target' = 'iceberg0.partitioned_transforms_db.t0_day_dt',\n" +
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
                "'external_cooldown_target' = 'iceberg0.partitioned_transforms_db.t0_day_dt',\n" +
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
                "'external_cooldown_target' = 'iceberg0.partitioned_transforms_db.t0_day_dt',\n" +
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
                "'external_cooldown_target' = 'iceberg0.unpartitioned_db.t0',\n" +
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
                "    'external_cooldown_target' = 'iceberg0.partitioned_transforms_db.t0_day_dt',\n" +
                "    'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "    'external_cooldown_wait_second' = '1'\n" +
                ")");

        starRocksAssert.withTable("CREATE TABLE test.tbl6 (\n" +
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
                "    'external_cooldown_target' = 'iceberg0.partitioned_transforms_db.t0_day_dt',\n" +
                "    'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "    'external_cooldown_wait_second' = '1'\n" +
                ")");

        starRocksAssert.withTable("CREATE TABLE test.tbl7 (\n" +
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
                "    'external_cooldown_target' = 'iceberg0.partitioned_transforms_db.t0_day_dt',\n" +
                "    'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "    'external_cooldown_wait_second' = '1'\n" +
                ")");

        starRocksAssert.withTable("CREATE TABLE test.tbl8 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    'replication_num' = '1',\n" +
                "    'external_cooldown_target' = 'iceberg0.partitioned_transforms_db.t0_day_dt',\n" +
                "    'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "    'external_cooldown_wait_second' = '1'\n" +
                ")");

        starRocksAssert.withTable("CREATE TABLE test.tbl9 (\n" +
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
                "     PARTITION p3 VALUES IN ('shanghai','wuxi') ,\n" +
                "     PARTITION p4 VALUES IN ('wuhan') \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    'replication_num' = '1',\n" +
                "    'external_cooldown_target' = 'iceberg0.partitioned_db.part_tbl1',\n" +
                "    'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "    'external_cooldown_wait_second' = '1'\n" +
                ")");

        starRocksAssert.withTable("CREATE TABLE test.tbl10 (\n" +
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
                "     PARTITION p3 VALUES IN ('shanghai','wuxi') ,\n" +
                "     PARTITION p4 VALUES IN ('wuhan') \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    'replication_num' = '1',\n" +
                "    'external_cooldown_target' = 'iceberg0.partitioned_db.part_tbl1',\n" +
                "    'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "    'external_cooldown_wait_second' = '1'\n" +
                ")");
        starRocksAssert.withTable("CREATE TABLE test.tbl11\n" +
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
                "'external_cooldown_target' = 'iceberg0.partitioned_transforms_db.t0_day_dt',\n" +
                "'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "'external_cooldown_wait_second' = '1'\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE test.tbl12\n" +
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
                "'external_cooldown_target' = 'iceberg0.partitioned_transforms_db.t0_day_dt',\n" +
                "'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE'\n," +
                "'external_cooldown_wait_second' = '1'\n" +
                ");");
    }

    @Test
    public void testCooldownWaitSecond() throws Exception {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl1");

        long updateTime = System.currentTimeMillis() - 2000;
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p2 = table.getPartition("p2").getDefaultPhysicalPartition();
        p2.updateVisibleVersion(p2.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p5 = table.getPartition("p5").getDefaultPhysicalPartition();
        p5.updateVisibleVersion(p5.getVisibleVersion() + 1, System.currentTimeMillis());

        ConnectContext ctx = starRocksAssert.getCtx();
        String sql3 = "ALTER TABLE test.tbl1 SET(\"external_cooldown_wait_second\" = \"1\");";
        AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql3, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt3);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector((OlapTable) table);
        Assert.assertTrue(selector.isTableSatisfied());

        List<Partition> partitions = selector.computeSatisfiedPartitions(-1);
        Assert.assertEquals(3, partitions.size());
        Set<String> satisfiedPartitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertTrue("p1 satisfied cooldown condition", satisfiedPartitionNames.contains("p1"));
        Assert.assertTrue("p2 satisfied cooldown condition", satisfiedPartitionNames.contains("p2"));
        Assert.assertTrue("p3 satisfied cooldown condition", satisfiedPartitionNames.contains("p3"));
        Assert.assertFalse("p4 not satisfied cooldown condition", satisfiedPartitionNames.contains("p4"));
        Assert.assertFalse("p5 not satisfied cooldown condition", satisfiedPartitionNames.contains("p5"));
    }

    @Test
    public void testPartitionStartEnd() throws Exception {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl2");

        long updateTime = System.currentTimeMillis() - 2000;
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p2 = table.getPartition("p2").getDefaultPhysicalPartition();
        p2.updateVisibleVersion(p2.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p5 = table.getPartition("p5").getDefaultPhysicalPartition();
        p5.updateVisibleVersion(p5.getVisibleVersion() + 1, updateTime);

        ConnectContext ctx = starRocksAssert.getCtx();
        String sql3 = "ALTER TABLE test.tbl2 SET(\"external_cooldown_wait_second\" = \"1\");";
        AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql3, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt3);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(
                (OlapTable) table, "2024-03-02 00:00:00", "2024-03-05 00:00:00", false);
        Assert.assertTrue(selector.isTableSatisfied());

        List<Partition> partitions = selector.computeSatisfiedPartitions(-1);
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
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl3");

        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1);
        PhysicalPartition p2 = table.getPartition("p2").getDefaultPhysicalPartition();
        p2.updateVisibleVersion(p2.getVisibleVersion() + 1);
        PhysicalPartition p4 = table.getPartition("p4").getDefaultPhysicalPartition();
        p4.updateVisibleVersion(p4.getVisibleVersion() + 1);
        PhysicalPartition p5 = table.getPartition("p5").getDefaultPhysicalPartition();
        p5.updateVisibleVersion(p5.getVisibleVersion() + 1);

        ConnectContext ctx = starRocksAssert.getCtx();
        String sql3 = "ALTER TABLE test.tbl3 SET(\"external_cooldown_wait_second\" = \"7200\");";
        AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql3, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt3);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(
                (OlapTable) table, "2024-03-02 00:00:00", "2024-03-05 00:00:00", true);
        Assert.assertTrue(selector.isTableSatisfied());

        List<Partition> partitions = selector.computeSatisfiedPartitions(-1);
        Assert.assertEquals(3, partitions.size());
        Set<String> satisfiedPartitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertFalse("p1 not in partition range", satisfiedPartitionNames.contains("p1"));
        Assert.assertTrue("p2 in partition range", satisfiedPartitionNames.contains("p2"));
        Assert.assertTrue("p3 satisfied condition", satisfiedPartitionNames.contains("p3"));
        Assert.assertTrue("p4 in partition range", satisfiedPartitionNames.contains("p4"));
        Assert.assertFalse("p5 not in partition range", satisfiedPartitionNames.contains("p5"));
    }

    @Test
    public void testSinglePartitionTable() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl4");
        OlapTable olapTable = (OlapTable) table;

        long updateTime = System.currentTimeMillis() - 2000;
        PhysicalPartition p1 = table.getPartition("tbl4").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(olapTable);
        Assert.assertTrue(selector.isTableSatisfied());
        List<Partition> partitions = selector.computeSatisfiedPartitions(-1);
        Assert.assertEquals(1, partitions.size());
        Set<String> satisfiedPartitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertTrue("tbl4 in partition range", satisfiedPartitionNames.contains("tbl4"));

        ExternalCooldownPartitionSelector selector1 = new ExternalCooldownPartitionSelector(olapTable, null, null, true);
        Assert.assertTrue(selector1.isTableSatisfied());
        List<Partition> partitions1 = selector1.computeSatisfiedPartitions(-1);
        Assert.assertEquals(1, partitions1.size());
        Set<String> satisfiedPartitionNames1 = partitions1.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertTrue("tbl4 in partition range", satisfiedPartitionNames1.contains("tbl4"));

        ExternalCooldownPartitionSelector selector2 = new ExternalCooldownPartitionSelector(olapTable, "tbl4", "tbl4", false);
        Assert.assertTrue(selector2.isTableSatisfied());
        List<Partition> partitions2 = selector2.computeSatisfiedPartitions(-1);
        Assert.assertEquals(1, partitions2.size());
        Set<String> satisfiedPartitionNames2 = partitions2.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertTrue("tbl4 in partition range", satisfiedPartitionNames2.contains("tbl4"));
    }

    @Test
    public void testListPartitionTable() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl5");

        long updateTime = System.currentTimeMillis() - 2000;
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, updateTime);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector((OlapTable) table);
        Assert.assertTrue(selector.isTableSatisfied());

        List<Partition> partitions = selector.computeSatisfiedPartitions(-1);
        Assert.assertEquals(2, partitions.size());
        Set<String> satisfiedPartitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toSet());
        Assert.assertTrue("p1 satisfied condition", satisfiedPartitionNames.contains("p1"));
        Assert.assertFalse("p2 not satisfied condition", satisfiedPartitionNames.contains("p2"));
        Assert.assertTrue("p3 satisfied condition", satisfiedPartitionNames.contains("p3"));
    }

    @Test
    public void testReloadSatisfiedPartitions() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl6");

        long updateTime = System.currentTimeMillis() - 2000;
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, updateTime);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector((OlapTable) table);
        Assert.assertTrue(selector.isTableSatisfied());

        // wait seconds not satisfied
        String sql = "ALTER TABLE tbl6 SET(\"external_cooldown_wait_second\" = \"0\");";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
        selector.reloadSatisfiedPartitions();
        Assert.assertFalse(selector.isTableSatisfied());

        // recover wait seconds not satisfied
        String sql1 = "ALTER TABLE tbl6 SET(\"external_cooldown_wait_second\" = \"1\");";
        AlterTableStmt alterTableStmt1 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt1);
        selector.reloadSatisfiedPartitions();
        Assert.assertTrue(selector.isTableSatisfied());

        // target table not satisfied (null)
        String sql2 = "ALTER TABLE tbl6 SET(\"external_cooldown_target\" = \"\");";
        AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt2);
        selector.reloadSatisfiedPartitions();
        Assert.assertFalse(selector.isTableSatisfied());

        // recover target table not satisfied
        String sql3 = "ALTER TABLE tbl6 SET(\"external_cooldown_target\" = \"iceberg0.partitioned_transforms_db.t0_day_dt\");";
        AlterTableStmt alterTableStmt3 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql3, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt3);
        selector.reloadSatisfiedPartitions();
        Assert.assertTrue(selector.isTableSatisfied());

        // target table not satisfied (null)
        ExternalCooldownConfig config = new ExternalCooldownConfig(
                "default_catalog.test.tbl5", "START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE", 1L);
        ((OlapTable) table).setCurExternalCoolDownConfig(config);
        selector.reloadSatisfiedPartitions();
        Assert.assertFalse(selector.isTableSatisfied());

        // recover target table not satisfied
        String sql5 = "ALTER TABLE tbl6 SET(\"external_cooldown_target\" = \"iceberg0.partitioned_transforms_db.t0_day_dt\");";
        AlterTableStmt alterTableStmt5 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql5, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt5);
        selector.reloadSatisfiedPartitions();
        Assert.assertTrue(selector.isTableSatisfied());
        List<Partition> satisfiedPartitions = selector.computeSatisfiedPartitions(-1);
        Assert.assertEquals(2, satisfiedPartitions.size());
        List<Partition> satisfiedPartitionsLimited = selector.computeSatisfiedPartitions(1);
        Assert.assertEquals(1, satisfiedPartitionsLimited.size());
    }

    @Test
    public void testGetNextSatisfiedPartition() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl7");

        long updateTime = System.currentTimeMillis() - 2000;
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, updateTime);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector((OlapTable) table);
        Partition partition = selector.getOneSatisfiedPartition();
        Assert.assertEquals("p1", partition.getName());
        Partition partition2 = selector.getNextSatisfiedPartition(partition);
        Assert.assertEquals("p3", partition2.getName());
        Partition partition3 = selector.getNextSatisfiedPartition(partition2);
        Assert.assertNull(partition3);
    }

    @Test
    public void testNonPartitionTableWithIcebergPartitioned() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl8");
        long updateTime = System.currentTimeMillis() - 2000;
        PhysicalPartition p1 = table.getPartition("tbl8").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector((OlapTable) table);
        List<Partition> satisfiedPartitions = selector.computeSatisfiedPartitions(-1);
        Assert.assertEquals(0, satisfiedPartitions.size());
    }

    @Test
    public void testGetPartitionsInRange() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl9");
        long updateTime = System.currentTimeMillis() - 2000;
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, updateTime);
        PhysicalPartition p4 = table.getPartition("p4").getDefaultPhysicalPartition();
        p4.updateVisibleVersion(p4.getVisibleVersion() + 1, updateTime);

        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(
                (OlapTable) table, "p1", "p3", false);
        List<Partition> satisfiedPartitions = selector.computeSatisfiedPartitions(-1);
        Assert.assertEquals(2, satisfiedPartitions.size());

        ExternalCooldownPartitionSelector selector1 = new ExternalCooldownPartitionSelector(
                (OlapTable) table, "p1", "p3", true);
        List<Partition> satisfiedPartitions1 = selector1.computeSatisfiedPartitions(-1);
        Assert.assertEquals(3, satisfiedPartitions1.size());

        ExternalCooldownPartitionSelector selector2 = new ExternalCooldownPartitionSelector((OlapTable) table);
        List<Partition> satisfiedPartitions2 = selector2.computeSatisfiedPartitions(-1);
        Assert.assertEquals(3, satisfiedPartitions2.size());

        new MockUp<ExternalCooldownPartitionSelector>() {
            @Mock
            public Set<String> getPartitionNamesByRangeWithPartitionLimit() throws SemanticException  {
                throw new SemanticException("test");
            }
        };
        Table table3 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl3");
        ExternalCooldownPartitionSelector selector3 = new ExternalCooldownPartitionSelector(
                (OlapTable) table3, "2024-03-02 00:00:00", "2024-03-05 00:00:00", true);
        List<Partition> satisfiedPartitions3 = selector3.computeSatisfiedPartitions(-1);
        Assert.assertEquals(0, satisfiedPartitions3.size());
    }

    @Test
    public void testGetPartitionsNull() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl9");
        long updateTime = System.currentTimeMillis() - 2000;
        PhysicalPartition p1 = table.getPartition("p1").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);

        new MockUp<OlapTable>() {
            @Mock
            public Partition getPartition(String partitionName)  {
                return null;
            }
        };
        ExternalCooldownPartitionSelector selector2 = new ExternalCooldownPartitionSelector((OlapTable) table);
        List<Partition> satisfiedPartitions2 = selector2.computeSatisfiedPartitions(-1);
        Assert.assertEquals(0, satisfiedPartitions2.size());
    }

    @Test
    public void testGetNativeTableNull() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl10");
        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector((OlapTable) table);

        new MockUp<IcebergTable>() {
            @Mock
            public org.apache.iceberg.Table getNativeTable() {
                return null;
            }
        };
        selector.reloadSatisfiedPartitions();
        Assert.assertFalse(selector.isTableSatisfied());
    }

    @Test
    public void testWaitConsistencyCheck() {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(
                "test", "tbl11");

        PartitionInfo partitionInfo = table.getPartitionInfo();
        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(table);
        long updateTime = System.currentTimeMillis() - 2000;
        Partition p1 = table.getPartition("p1");
        p1.getDefaultPhysicalPartition().updateVisibleVersion(
                p1.getDefaultPhysicalPartition().getVisibleVersion() + 1, updateTime);
        partitionInfo.setExternalCoolDownSyncedTimeMs(p1.getId(), updateTime);
        partitionInfo.setExternalCoolDownConsistencyCheckTimeMs(p1.getId(), updateTime + 1000);
        partitionInfo.setCoolDownConsistencyCheckDifference(p1.getId(), 0);
        Assert.assertFalse(selector.isPartitionSatisfied(p1));

        Partition p2 = table.getPartition("p2");
        p2.getDefaultPhysicalPartition().updateVisibleVersion(
                p2.getDefaultPhysicalPartition().getVisibleVersion() + 1, updateTime);
        partitionInfo.setExternalCoolDownSyncedTimeMs(p2.getId(), updateTime);
        partitionInfo.setExternalCoolDownConsistencyCheckTimeMs(p2.getId(), updateTime - 1000);
        partitionInfo.setCoolDownConsistencyCheckDifference(p2.getId(), 0);
        Assert.assertFalse(selector.isPartitionSatisfied(p2));

        Partition p3 = table.getPartition("p3");
        p3.getDefaultPhysicalPartition().updateVisibleVersion(
                p3.getDefaultPhysicalPartition().getVisibleVersion() + 1, updateTime);
        partitionInfo.setExternalCoolDownSyncedTimeMs(p3.getId(), updateTime);
        partitionInfo.setExternalCoolDownConsistencyCheckTimeMs(p3.getId(), updateTime + 1000);
        partitionInfo.setCoolDownConsistencyCheckDifference(p3.getId(), 1);
        Assert.assertTrue(selector.isPartitionSatisfied(p3));

        Partition p4 = table.getPartition("p4");
        p4.getDefaultPhysicalPartition().updateVisibleVersion(
                p4.getDefaultPhysicalPartition().getVisibleVersion() + 1, updateTime);
        partitionInfo.setExternalCoolDownSyncedTimeMs(p4.getId(), updateTime);
        partitionInfo.setExternalCoolDownConsistencyCheckTimeMs(p4.getId(), updateTime - 1000);
        partitionInfo.setCoolDownConsistencyCheckDifference(p4.getId(), -1);
        Assert.assertFalse(selector.isPartitionSatisfied(p4));

        new MockUp<ExternalCooldownPartitionSelector>() {
            @Mock
            private boolean checkPartitionSatisfied(Partition partition) {
                throw new RuntimeException("test");
            }
        };
        Assert.assertFalse(selector.isPartitionSatisfied(p4));
    }

    @Test
    public void testGetValidRangePartitionMapException() {
        new MockUp<OlapTable>() {
            @Mock
            public Map<String, Range<PartitionKey>> getValidRangePartitionMap(int lastPartitionNum) throws AnalysisException {
                throw new AnalysisException("test");
            }
        };
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl12");
        PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, p3.getVisibleVersionTime());
        ExternalCooldownPartitionSelector selector3 = new ExternalCooldownPartitionSelector(table);
        List<Partition> satisfiedPartitions3 = selector3.computeSatisfiedPartitions(-1);
        Assert.assertEquals(0, satisfiedPartitions3.size());
    }

    @Test
    public void testGetValidRangePartitionMap() {
        new MockUp<OlapTable>() {
            @Mock
            public Map<String, Range<PartitionKey>> getValidRangePartitionMap(int lastPartitionNum) throws AnalysisException {
                throw new AnalysisException("test");
            }
        };
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl12");
        PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
        p3.updateVisibleVersion(p3.getVisibleVersion() + 1, p3.getVisibleVersionTime());
        ExternalCooldownPartitionSelector selector3 = new ExternalCooldownPartitionSelector(
                table, "2024-03-02 00:00:00", "2024-03-05 00:00:00", true);
        List<Partition> satisfiedPartitions3 = selector3.computeSatisfiedPartitions(-1);
        Assert.assertEquals(0, satisfiedPartitions3.size());
    }

    @Test
    public void testGetPartitionNamesByRangeWithPartitionLimit() {
        try (MockedStatic<SyncPartitionUtils> mocked = mockStatic(SyncPartitionUtils.class)) {
            mocked.when(() -> SyncPartitionUtils.createRange(Mockito.any(), Mockito.any(), Mockito.any())).thenThrow(
                    new AnalysisException("test"));

            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl12");
            PhysicalPartition p3 = table.getPartition("p3").getDefaultPhysicalPartition();
            p3.updateVisibleVersion(p3.getVisibleVersion() + 1, p3.getVisibleVersionTime());
            ExternalCooldownPartitionSelector selector3 = new ExternalCooldownPartitionSelector(
                    table, "2024-03-02 00:00:00", "2024-03-05 00:00:00", true);
            List<Partition> satisfiedPartitions3 = selector3.computeSatisfiedPartitions(-1);
            Assert.assertEquals(0, satisfiedPartitions3.size());
        }
    }
}