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

import com.staros.proto.ShardGroupInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.proc.RollupProcDir;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LakeRollupJobTest {
    private static final String DB = "db_for_lake_mv";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    private static LakeRollupJob lakeRollupJob;
    private static LakeRollupJob lakeRollupJob2;
    private static LakeRollupJob lakeRollupJob3;
    private static LakeRollupJob lakeRollupJob4;

    private static Database db;
    private static Table table;

    @BeforeAll
    public static void setUp() throws Exception {
        new MockUp<MaterializedViewHandler>() {
            @Mock protected void runAfterCatalogReady() {
                System.out.println("Mocked MaterializedViewHandler.runAfterCatalogReady() called");
            }
        };

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
                "DISTRIBUTED BY HASH(k2) BUCKETS 3")
                .withTable("CREATE TABLE base_table2\n" +
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
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3")
                .withTable("CREATE TABLE base_table3\n" +
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
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3")
                .withTable("CREATE TABLE base_table4\n" +
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
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES(\n" +
                        "    \"file_bundling\" = \"true\"\n" +
                        ");");

        String sql = "create materialized view mv1 as\n" +
                "select k2, k1 from base_table order by k2;";
        lakeRollupJob = createJob(sql);

        String sql2 = "create materialized view mv2 as\n" +
                "select k2, k1 from base_table2 order by k2;";
        lakeRollupJob2 = createJob(sql2);

        String sql3 = "create materialized view mv3 as\n" +
                "select k2, k1 from base_table3 order by k2;";
        lakeRollupJob3 = createJob(sql3);

        String sql4 = "create materialized view mv4 as\n" +
                "select k2, k1 from base_table4 order by k2;";
        lakeRollupJob4 = createJob(sql4);

        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        table = db.getTable("base_table");
    }

    private static LakeRollupJob createJob(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertTrue(stmt instanceof CreateMaterializedViewStmt);
        CreateMaterializedViewStmt createMaterializedViewStmt = (CreateMaterializedViewStmt) stmt;
        GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMaterializedViewStmt);
        Map<Long, AlterJobV2> alterJobV2Map = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        List<AlterJobV2> alterJobV2List = new ArrayList<>(alterJobV2Map.values());
        // Disable the execution of job in background thread
        GlobalStateMgr.getCurrentState().getRollupHandler().clearJobs();
        Assertions.assertEquals(1, alterJobV2List.size());
        return (LakeRollupJob) alterJobV2List.get(0);
    }

    @AfterAll
    public static void tearDown() {
        GlobalStateMgr.getCurrentState().getRollupHandler().clearJobs();
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
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, lakeRollupJob.getJobState());

        lakeRollupJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, lakeRollupJob.getJobState());

        List<List<Comparable>> infos = new ArrayList<>();
        lakeRollupJob.getInfo(infos);
        Assertions.assertEquals(1, infos.size());
        Assertions.assertTrue(!infos.get(0).get(10).equals(FeConstants.NULL_STRING));

        Assertions.assertEquals(1, infos.size());
        lakeRollupJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, lakeRollupJob.getJobState());

        while (lakeRollupJob.getJobState() != AlterJobV2.JobState.FINISHED) {
            lakeRollupJob.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, lakeRollupJob.getJobState());
    }

    @Test
    public void testCreateSyncMvWithEnableFileBundling() throws Exception {
        new MockUp<LakeRollupJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        lakeRollupJob4.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, lakeRollupJob4.getJobState());

        lakeRollupJob4.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, lakeRollupJob4.getJobState());

        List<List<Comparable>> infos = new ArrayList<>();
        lakeRollupJob4.getInfo(infos);
        Assertions.assertEquals(1, infos.size());
        Assertions.assertTrue(!infos.get(0).get(10).equals(FeConstants.NULL_STRING));

        Assertions.assertEquals(1, infos.size());
        lakeRollupJob4.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, lakeRollupJob4.getJobState());

        while (lakeRollupJob4.getJobState() != AlterJobV2.JobState.FINISHED) {
            lakeRollupJob4.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, lakeRollupJob4.getJobState());

        for (Partition partition : table.getPartitions()) {
            long partitionId = partition.getId();
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                List<ShardGroupInfo> shardGroupInfos = null;
                shardGroupInfos = GlobalStateMgr.getCurrentState().getStarOSAgent().listShardGroup();
                Assertions.assertTrue(shardGroupInfos != null && !shardGroupInfos.isEmpty());
                Optional<ShardGroupInfo> targetGroup = shardGroupInfos.stream()
                        .filter(group -> group.getGroupId() == physicalPartition.getShardGroupId())
                        .findFirst();
                Assertions.assertTrue(targetGroup.isPresent());
                Map<String, String> labels = targetGroup.get().getLabelsMap();
                if (!labels.containsKey("partitionId")) {
                    Assertions.assertTrue(false);
                }
                long targetPartitionId = Long.parseLong(labels.get("partitionId"));
                Assertions.assertEquals(targetPartitionId, partitionId);
            }
        }
    }

    @Test
    public void testGetInfo() {
        List<List<Comparable>> infos = new ArrayList<>();
        lakeRollupJob.getInfo(infos);
        Assertions.assertEquals(1, infos.size());
        Assertions.assertEquals(RollupProcDir.TITLE_NAMES.size(), infos.get(0).size());
        Assertions.assertTrue(infos.get(0).get(10).equals(FeConstants.NULL_STRING));
    }

    @Test
    public void testCancelImpl() {
        String errorMsg = "test cancel";
        lakeRollupJob2.cancelImpl(errorMsg);
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, lakeRollupJob2.jobState);
        Assertions.assertEquals(errorMsg, lakeRollupJob2.errMsg);
    }

    @Test
    public void testPendingJobNoAliveBackend() {
        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };

        mockedWarehouseManager.setComputeNodesAssignedToTablet(null);
        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> lakeRollupJob3.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains("No alive backend"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, lakeRollupJob3.getJobState());
    }
}