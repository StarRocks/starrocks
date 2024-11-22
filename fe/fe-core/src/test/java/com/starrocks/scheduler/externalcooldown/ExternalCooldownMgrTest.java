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
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
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

import java.text.SimpleDateFormat;
import java.util.Date;


public class ExternalCooldownMgrTest {
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
                        "'external_cooldown_schedule'='START 22:00 END 08:00 EVERY INTERVAL 1 MINUTE',\n" +
                        "'external_cooldown_wait_second'='3600',\n" +
                        "'replication_num' = '1'\n" +
                        ");");
    }

    @Test
    public void testReload() throws Exception {
        ExternalCooldownMgr mgr = new ExternalCooldownMgr();
        mgr.doInitializeIfNeed();

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(TEST_DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(TEST_DB_NAME, "tbl1");
        mgr.prepareMaintenanceWork(testDb.getId(), table);
        Assert.assertEquals(1, mgr.getRunnableJobs().size());

        String sql = "ALTER TABLE tbl1 SET(\"external_cooldown_target\" = \"\");";
        ConnectContext ctx = starRocksAssert.getCtx();
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);

        mgr.reloadJobs();
        Assert.assertEquals(0, mgr.getRunnableJobs().size());

        long updateTime = System.currentTimeMillis() - 3600 * 1000;
        PhysicalPartition p1 = table.getPartition("p20200101").getDefaultPhysicalPartition();
        p1.updateVisibleVersion(p1.getVisibleVersion() + 1, updateTime);

        String sql1 = "ALTER TABLE tbl1 SET(\"external_cooldown_target\" = \"iceberg0.partitioned_transforms_db.t0_day\");";
        AlterTableStmt alterTableStmt1 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt1);

        mgr.prepareMaintenanceWork(testDb.getId(), table);
        ExternalCooldownMaintenanceJob job = mgr.getRunnableJobs().get(0);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date parsedDate = dateFormat.parse("2024-10-13 23:50:00.000");
        job.onSchedule(parsedDate.getTime());
        mgr.stopMaintainExternalCooldown(table);
        String lastRunTaskName = job.getLastRunTaskName();
        Assert.assertNotNull(lastRunTaskName);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(lastRunTaskName);
        boolean isRunning = taskManager.getTaskRunScheduler().isTaskRunning(task.getId());
        Assert.assertFalse(isRunning);

        mgr.prepareMaintenanceWork(testDb.getId(), table);
        Assert.assertNotEquals(0, mgr.getRunnableJobs().size());
        new MockUp<ExternalCooldownMaintenanceJob>() {
            @Mock
            public void restore() {
                throw new IllegalStateException("test");
            }
        };
        mgr.reloadJobs();
        Assert.assertEquals(0, mgr.getRunnableJobs().size());
    }
}