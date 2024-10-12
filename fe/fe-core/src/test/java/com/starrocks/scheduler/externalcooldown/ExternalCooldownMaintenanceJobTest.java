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


package com.starrocks.scheduler.externalcooldown;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.externalcooldown.ExternalCooldownConfig;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class ExternalCooldownMaintenanceJobTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    protected static PseudoCluster cluster;

    protected static final String TEST_DB_NAME = "test";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        starRocksAssert.withDatabase(TEST_DB_NAME);
        starRocksAssert.useDatabase(TEST_DB_NAME);

        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);


        starRocksAssert.withDatabase(TEST_DB_NAME).useDatabase(TEST_DB_NAME)
                .withTable("CREATE TABLE tbl1\n" +
                        "(\n" +
                        "    id int,\n" +
                        "    ts datetime,\n" +
                        "    data string\n" +
                        ")\n" +
                        "DUPLICATE KEY(`id`, `ts`)\n" +
                        "PARTITION BY RANGE(`ts`)\n" +
                        "(\n" +
                        "    PARTITION p20200101 VALUES [('2020-01-01 00:00:00'),('2020-01-02 00:00:00')),\n" +
                        "    PARTITION p20200102 VALUES [('2020-01-02 00:00:00'),('2020-01-03 00:00:00')),\n" +
                        "    PARTITION p20200103 VALUES [('2020-01-03 00:00:00'),('2020-01-04 00:00:00')),\n" +
                        "    PARTITION p20200104 VALUES [('2020-01-04 00:00:00'),('2020-01-05 00:00:00')),\n" +
                        "    PARTITION p20200105 VALUES [('2020-01-05 00:00:00'),('2020-01-06 00:00:00'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                        "PROPERTIES(\n" +
                        "'external_cooldown_target'='iceberg0.partitioned_transforms_db.t0_day',\n" +
                        "'external_cooldown_schedule'='START 01:00 END 07:59 EVERY INTERVAL 1 MINUTE',\n" +
                        "'external_cooldown_wait_second'='3600',\n" +
                        "'replication_num' = '1'\n" +
                        ");")
                .withTable("CREATE TABLE tbl2\n" +
                        "(\n" +
                        "    id int,\n" +
                        "    ts datetime,\n" +
                        "    data string\n" +
                        ")\n" +
                        "DUPLICATE KEY(`id`, `ts`)\n" +
                        "PARTITION BY RANGE(`ts`)\n" +
                        "(\n" +
                        "    PARTITION p20200101 VALUES [('2020-01-01 00:00:00'),('2020-01-02 00:00:00')),\n" +
                        "    PARTITION p20200102 VALUES [('2020-01-02 00:00:00'),('2020-01-03 00:00:00')),\n" +
                        "    PARTITION p20200103 VALUES [('2020-01-03 00:00:00'),('2020-01-04 00:00:00')),\n" +
                        "    PARTITION p20200104 VALUES [('2020-01-04 00:00:00'),('2020-01-05 00:00:00')),\n" +
                        "    PARTITION p20200105 VALUES [('2020-01-05 00:00:00'),('2020-01-06 00:00:00'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                        "PROPERTIES(\n" +
                        "'external_cooldown_target'='iceberg0.partitioned_transforms_db.t0_day',\n" +
                        "'external_cooldown_schedule'='START 22:00 END 08:00 EVERY INTERVAL 1 MINUTE',\n" +
                        "'external_cooldown_wait_second'='3600',\n" +
                        "'replication_num' = '1'\n" +
                        ");")
                .withTable("CREATE TABLE tbl3\n" +
                "(\n" +
                "    id int,\n" +
                "    ts datetime,\n" +
                "    data string\n" +
                ")\n" +
                "DUPLICATE KEY(`id`, `ts`)\n" +
                "PARTITION BY RANGE(`ts`)\n" +
                "(\n" +
                "    PARTITION p20200101 VALUES [('2020-01-01 00:00:00'),('2020-01-02 00:00:00')),\n" +
                "    PARTITION p20200102 VALUES [('2020-01-02 00:00:00'),('2020-01-03 00:00:00')),\n" +
                "    PARTITION p20200103 VALUES [('2020-01-03 00:00:00'),('2020-01-04 00:00:00')),\n" +
                "    PARTITION p20200104 VALUES [('2020-01-04 00:00:00'),('2020-01-05 00:00:00')),\n" +
                "    PARTITION p20200105 VALUES [('2020-01-05 00:00:00'),('2020-01-06 00:00:00'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "'replication_num' = '1'\n" +
                ");");
    }

    @Test
    public void testNormal() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl1");
        ExternalCooldownMaintenanceJob job = new ExternalCooldownMaintenanceJob(table, testDb.getId());
        Assert.assertEquals(table.getId(), job.getJobId());
        String str = String.format("ExternalCooldownJob id=%s,dbId=%s,tableId=%d",
                job.getJobId(), job.getDbId(), job.getTableId());
        Assert.assertEquals(str, job.toString());
        ExternalCooldownMaintenanceJob job2 = new ExternalCooldownMaintenanceJob(table, testDb.getId());
        Assert.assertEquals(job, job2);
        Assert.assertEquals(job.hashCode(), job2.hashCode());
        Assert.assertTrue(job.equals(job));
    }

    @Test
    public void testRestore() throws IOException {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tbl1");
        long updateTime = System.currentTimeMillis() - 3600 * 1000;
        Partition p1 = table.getPartition("p20200101");
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);

        // 1. Write job to file
        String fileName = "./ExternalCooldownMaintenanceJobRestoreTest";
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        ExternalCooldownMaintenanceJob job = new ExternalCooldownMaintenanceJob(table, testDb.getId());
        job.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        ExternalCooldownMaintenanceJob jobRestore = ExternalCooldownMaintenanceJob.read(in);
        in.close();
        jobRestore.restore();
        Assert.assertNotNull(jobRestore.getSchedule());
        Assert.assertNotNull(jobRestore.getOlapTable());
        Assert.assertNotNull(jobRestore.getPartitionSelector());
        Assert.assertEquals(1, jobRestore.getPartitionSelector().computeSatisfiedPartitions(-1).size());

        // 3. Check reload when config changed
        ExternalCooldownConfig config = table.getCurExternalCoolDownConfig();
        config.setWaitSecond(0L);
        table.setCurExternalCoolDownConfig(config);
        jobRestore.restore();
        Assert.assertEquals(0, jobRestore.getPartitionSelector().computeSatisfiedPartitions(-1).size());
    }

    @Test
    public void testOnSchedule() throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date parsedDate = dateFormat.parse("2024-10-13 23:50:00.000");
        ExternalCooldownMgr mgr = new ExternalCooldownMgr();
        mgr.doInitializeIfNeed();

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(TEST_DB_NAME, "tbl2");
        mgr.prepareMaintenanceWork(testDb.getId(), table);
        Assert.assertEquals(1, mgr.getRunnableJobs().size());
        ExternalCooldownMaintenanceJob job = mgr.getRunnableJobs().get(0);
        job.onSchedule(parsedDate.getTime());

        String sql = "ALTER TABLE tbl1 SET(\"external_cooldown_target\" = \"\");";
        ConnectContext ctx = starRocksAssert.getCtx();
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
        job.onSchedule(parsedDate.getTime());
        ExternalCooldownConfig config = table.getCurExternalCoolDownConfig();
        Assert.assertNotNull(config);
        config.setTarget("");
        table.setCurExternalCoolDownConfig(new ExternalCooldownConfig());
        mgr.reloadJobs();
        Assert.assertEquals(0, mgr.getRunnableJobs().size());

        ExternalCooldownMaintenanceJob job2 = new ExternalCooldownMaintenanceJob(1L, 1L);
        job2.onSchedule(parsedDate.getTime());
        Assert.assertFalse(job2.isRunnable());

        OlapTable table3 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(TEST_DB_NAME, "tbl3");
        ExternalCooldownMaintenanceJob job3 = new ExternalCooldownMaintenanceJob(table3, testDb.getId());
        job3.onSchedule(parsedDate.getTime());
        Assert.assertFalse(job3.isRunnable());

        long updateTime = System.currentTimeMillis() - 3600 * 1000;
        Partition p1 = table.getPartition("p20200101");
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);

        String sql1 = "ALTER TABLE tbl2 SET(\"external_cooldown_target\" = \"iceberg0.partitioned_transforms_db.t0_day\");";
        AlterTableStmt alterTableStmt1 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt1);
        table.setCurExternalCoolDownConfig(new ExternalCooldownConfig(
                "iceberg0.partitioned_transforms_db.t0_day",
                "START 22:00 END 08:00 EVERY INTERVAL 1 MINUTE", 3600L));

        mgr.prepareMaintenanceWork(testDb.getId(), table);
        ExternalCooldownMaintenanceJob job1 = mgr.getRunnableJobs().get(0);
        job1.onSchedule(parsedDate.getTime() + 60 * 1000L);
        job1.onSchedule(parsedDate.getTime() + 2 * 60 * 1000L);
        job1.onSchedule(parsedDate.getTime() + 3 * 60 * 1000L);
        job1.onSchedule(parsedDate.getTime() + 4 * 60 * 1000L);

        long now = System.currentTimeMillis();
        new MockUp<TaskManager>() {
            @Mock
            public Task getTask(String taskName) {
                Task task = new Task("test");
                task.setCreateTime(now);
                return task;
            }
        };
        job1.onSchedule(now);
    }
}