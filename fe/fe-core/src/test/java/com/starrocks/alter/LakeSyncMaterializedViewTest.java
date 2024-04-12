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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class LakeSyncMaterializedViewTest {
    private static final String DB = "db_for_lake_mv";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    private static LakeRollupJob lakeRollupJob;

    private static Database db;
    private static Table table;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();

        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);

        starRocksAssert.withTable("CREATE TABLE base_table\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3");

        String sql = "create materialized view mv1 as\n" +
                "select k2, k1 from base_table order by k2;";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assert.assertTrue(stmt instanceof CreateMaterializedViewStmt);
        CreateMaterializedViewStmt createMaterializedViewStmt = (CreateMaterializedViewStmt) stmt;
        GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMaterializedViewStmt);

        db = GlobalStateMgr.getServingState().getDb(DB);
        table = db.getTable("base_table");

        List<AlterJobV2> alterJobV2List = GlobalStateMgr.getCurrentState().getRollupHandler().
                getUnfinishedAlterJobV2ByTableId(table.getId());
        GlobalStateMgr.getCurrentState().getRollupHandler().clearJobs();
        lakeRollupJob = (LakeRollupJob) alterJobV2List.get(0);
    }



    @AfterClass
    public static void tearDown() {

    }

    @Test
    public void testCreateSyncMv() throws Exception {
        new MockUp<LakeRollupJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        lakeRollupJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, lakeRollupJob.getJobState());

        lakeRollupJob.runWaitingTxnJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, lakeRollupJob.getJobState());

        lakeRollupJob.runRunningJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, lakeRollupJob.getJobState());

        lakeRollupJob.runFinishedRewritingJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, lakeRollupJob.getJobState());
    }

}
