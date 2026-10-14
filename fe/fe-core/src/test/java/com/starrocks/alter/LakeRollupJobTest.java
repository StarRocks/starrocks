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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.proc.RollupProcDir;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.lake.Utils;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CreateSyncMVStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.thrift.TAlterTabletReqV2;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.utframe.LockProbe;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

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
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        stopBackgroundRollupHandler(60000);

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
        UtFrameUtils.setUpForPersistTest();
    }

    /**
     * The rollup handler is a leader daemon with no runningUnitTest guard: it ticks every
     * alter_scheduler_interval_millisecond and runs every non-final job in alterJobsV2 -- the very state
     * machine these cases drive by hand. clearJobs() does not close that race, because the daemon can
     * already have advanced the job in the window between createMaterializedView() and clearJobs() (the
     * same race fixed in RollupJobV2Test, #74346); the test then drives a job that is no longer PENDING and
     * hits "index meta id ... already exists". Stopping the daemon is what makes every transition here an
     * explicit call. A MockUp of runAfterLeaseValid() is not equivalent: it is name-coupled to whichever
     * method the daemon ticks (it already had to be renamed once, in #73043), and it leaves the daemon
     * thread calling into JMockit-instrumented code for the whole class while the cases install and tear
     * down other MockUps -- JMockit's shared state is not thread-safe under that, which is the flake fixed
     * in InfoSchemaDbTest (#74312).
     */
    private static void stopBackgroundRollupHandler(long timeoutMs) throws Exception {
        MaterializedViewHandler rollupHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        Assertions.assertTrue(rollupHandler.isRunning(),
                "the rollup daemon must be up before it is stopped; otherwise a later start() would clear "
                        + "the stop request and resurrect the racing tick");
        rollupHandler.setStop();
        LeaderDaemon.awaitQuiesced(List.<LeaderDaemon>of(rollupHandler), timeoutMs);
        // setStop() drives the LeaderDaemon worker through onStopped(), which shuts the handler executor
        // down as part of the demotion cleanup. Rebuild it so finished AlterReplicaTask reports can still
        // be submitted while the cases drive jobs manually (mirrors stopBackgroundSchemaChangeHandler).
        rollupHandler.rebuildExecutorForTest();
    }

    private static LakeRollupJob createJob(String sql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertTrue(stmt instanceof CreateSyncMVStmt);
        CreateSyncMVStmt createMaterializedViewStmt = (CreateSyncMVStmt) stmt;
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
        // Capture AgentBatchTask objects
        List<AgentBatchTask> capturedBatchTasks = new ArrayList<>();
        new MockUp<LakeRollupJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                capturedBatchTasks.add(batchTask);
                // Mark tasks as finished to allow job to proceed
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        // Use existing lakeRollupJob which is based on base_table with 2 partitions
        lakeRollupJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, lakeRollupJob.getJobState());

        lakeRollupJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, lakeRollupJob.getJobState());

        lakeRollupJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, lakeRollupJob.getJobState());

        // Verify that we captured at least one batch task
        Assertions.assertFalse(capturedBatchTasks.isEmpty(), "Should have captured at least one AgentBatchTask");

        // Extract all AlterReplicaTask objects
        List<AlterReplicaTask> alterReplicaTasks = new ArrayList<>();
        for (AgentBatchTask batchTask : capturedBatchTasks) {
            for (com.starrocks.task.AgentTask agentTask : batchTask.getAllTasks()) {
                if (agentTask instanceof AlterReplicaTask) {
                    alterReplicaTasks.add((AlterReplicaTask) agentTask);
                }
            }
        }

        Assertions.assertFalse(alterReplicaTasks.isEmpty(), "Should have at least one AlterReplicaTask");

        // Get table and TabletInvertedIndex for verification
        OlapTable tbl = (OlapTable) table;
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        // Group tasks by baseTabletIndexId
        Map<Long, List<AlterReplicaTask>> tasksByBaseIndexId = new HashMap<>();
        Map<Long, TTabletSchema> expectedSchemasByIndexId = new HashMap<>();

        for (AlterReplicaTask task : alterReplicaTasks) {
            // Verify baseTabletReadSchema is not null
            TAlterTabletReqV2 request = task.toThrift();
            Assertions.assertTrue(request.isSetBase_tablet_read_schema(),
                    "base_tablet_read_schema should be set for tablet " + task.getBaseTabletId());
            Assertions.assertNotNull(request.getBase_tablet_read_schema(),
                    "base_tablet_read_schema should not be null for tablet " + task.getBaseTabletId());

            // Get baseTabletIndexId from TabletInvertedIndex
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(task.getBaseTabletId());
            Assertions.assertNotNull(tabletMeta, "TabletMeta should exist for tablet " + task.getBaseTabletId());
            long baseTabletIndexId = tabletMeta.getIndexId();

            // Group tasks by baseTabletIndexId
            tasksByBaseIndexId.computeIfAbsent(baseTabletIndexId, k -> new ArrayList<>()).add(task);

            // Create expected schema if not already created
            if (!expectedSchemasByIndexId.containsKey(baseTabletIndexId)) {
                TTabletSchema expectedSchema = SchemaInfo.fromMaterializedIndex(
                        tbl, baseTabletIndexId, tbl.getIndexMetaByMetaId(baseTabletIndexId)).toTabletSchema();
                expectedSchemasByIndexId.put(baseTabletIndexId, expectedSchema);
            }
        }

        // Verify that tasks with the same baseTabletIndexId use the same baseTabletReadSchema
        for (Map.Entry<Long, List<AlterReplicaTask>> entry : tasksByBaseIndexId.entrySet()) {
            long baseIndexId = entry.getKey();
            List<AlterReplicaTask> tasks = entry.getValue();
            TTabletSchema expectedSchema = expectedSchemasByIndexId.get(baseIndexId);

            Assertions.assertNotNull(expectedSchema, "Expected schema should exist for index " + baseIndexId);

            // Verify all tasks for this index use the same schema
            for (AlterReplicaTask task : tasks) {
                TAlterTabletReqV2 request = task.toThrift();
                TTabletSchema actualSchema = request.getBase_tablet_read_schema();

                // Verify schema ID matches
                Assertions.assertEquals(expectedSchema.getId(), actualSchema.getId(),
                        "Schema ID should match for index " + baseIndexId);
                Assertions.assertEquals(expectedSchema.getKeys_type(), actualSchema.getKeys_type(),
                        "Keys type should match for index " + baseIndexId);
                Assertions.assertEquals(expectedSchema.getShort_key_column_count(),
                        actualSchema.getShort_key_column_count(),
                        "Short key column count should match for index " + baseIndexId);
                Assertions.assertEquals(expectedSchema.getColumns().size(), actualSchema.getColumns().size(),
                        "Column count should match for index " + baseIndexId);
            }
        }

        // Verify that different baseTabletIndexId use different schemas (if there are multiple indices)
        if (tasksByBaseIndexId.size() > 1) {
            List<Long> indexIds = new ArrayList<>(tasksByBaseIndexId.keySet());
            TTabletSchema schema1 = expectedSchemasByIndexId.get(indexIds.get(0));
            TTabletSchema schema2 = expectedSchemasByIndexId.get(indexIds.get(1));
            // They might have the same schema if they're from the same base index, but structure should be correct
            Assertions.assertNotNull(schema1);
            Assertions.assertNotNull(schema2);
        }
    }

    /**
     * Whether {@link LakeRollupJob#lakePublishVersion} was holding a metadata lock when it published,
     * sampled inside the faked transport. Static because the faked transport is a static method.
     */
    private static LockProbe publishProbe;

    @Test
    public void testCreateSyncMvWithEnableFileBundling() throws Exception {
        publishProbe = LockProbe.onAnyThread();
        new MockUp<LakeRollupJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        // Mock Utils.sendAggregatePublishVersionRequest to avoid RPC calls in test
        // environment
        new MockUp<Utils>() {
            @Mock
            public static void sendAggregatePublishVersionRequest(AggregatePublishVersionRequest request,
                    long baseVersion, ComputeResource computeResource,
                    Map<Long, Double> compactionScores,
                    Map<Long, com.starrocks.proto.TabletStatPB> tabletStats,
                    java.util.List<com.starrocks.proto.VectorIndexBuildInfoPB> vectorIndexBuildInfos)
                    throws NoAliveBackendException, RpcException {
                // Publishing is a BE RPC. Sample the lock depth where the real transport would be
                // contacted: the job reads the partition's tablets under the table's READ lock, but the
                // publish itself has to happen after that lock is dropped, or every waiter on the table
                // -- transaction publish included -- pays the round trip.
                publishProbe.record("publish");
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

        publishProbe.assertReachedOutsideTheLock("publish",
                "lakePublishVersion published while holding an FE metadata lock");

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

    @Test
    public void testLightWeightTabletCreationSkipsSendTask() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_table_lw\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2022-02-01'),('2022-02-16'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('light_weight_tablet_creation' = 'true');");

        LakeRollupJob job = createJob(
                "create materialized view mv_lw as select k2, k1 from base_table_lw order by k2;");
        OlapTable lwTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(DB).getTable("base_table_lw");
        Assertions.assertTrue(lwTable.isLightWeightTabletCreation());

        AtomicBoolean sendCalled = new AtomicBoolean(false);
        new MockUp<LakeRollupJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask,
                                             com.starrocks.common.util.concurrent.MarkedCountDownLatch<Long, Long> latch,
                                             long timeoutSeconds) {
                sendCalled.set(true);
            }
        };

        job.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, job.getJobState());
        Assertions.assertFalse(sendCalled.get(),
                "sendAgentTaskAndWait must not be invoked when light_weight_tablet_creation is enabled");
    }

    @Test
    public void testCancelWithoutJobIdsReportsJobInFinishedRewriting() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_table_cancel\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2022-02-01'),('2022-02-16'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3");
        LakeRollupJob job = createJob(
                "create materialized view mv_cancel as select k2, k1 from base_table_cancel order by k2;");

        new MockUp<LakeRollupJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };
        job.runPendingJob();
        job.runWaitingTxnJob();
        job.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());

        // A lake rollup job in FINISHED_REWRITING refuses to be cancelled while its table exists.
        // CANCEL ALTER TABLE ROLLUP without job ids picks it up and must say so instead of succeeding.
        MaterializedViewHandler rollupHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        rollupHandler.addAlterJobV2(job);
        try {
            CancelAlterTableStmt cancelStmt = (CancelAlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                    "cancel alter table rollup from base_table_cancel", connectContext);
            DdlException e = Assertions.assertThrows(DdlException.class,
                    () -> GlobalStateMgr.getCurrentState().getLocalMetastore().cancelAlter(cancelStmt));
            Assertions.assertTrue(e.getMessage().contains("Job can not be cancelled. JobId: " + job.getJobId()
                    + ", State: FINISHED_REWRITING"), e.getMessage());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, job.getJobState());
        } finally {
            rollupHandler.clearJobs();
        }
    }

    @Test
    public void testCancelAcceptsJobCancelledConcurrently() throws Exception {
        starRocksAssert.withTable("CREATE TABLE base_table_cancel2\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values [('2022-02-01'),('2022-02-16'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3");
        LakeRollupJob job = createJob(
                "create materialized view mv_cancel2 as select k2, k1 from base_table_cancel2 order by k2;");

        // The scheduler cancels the job (e.g. on timeout) after CANCEL ALTER TABLE ROLLUP has picked it
        // up, so the statement's own cancel() finds it CANCELLED already and returns false.
        new MockUp<LakeRollupJob>() {
            @Mock
            public boolean cancelImpl(Invocation invocation, String errMsg) {
                invocation.proceed(errMsg);
                return false;
            }
        };
        MaterializedViewHandler rollupHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        rollupHandler.addAlterJobV2(job);
        try {
            CancelAlterTableStmt cancelStmt = (CancelAlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                    "cancel alter table rollup from base_table_cancel2", connectContext);
            GlobalStateMgr.getCurrentState().getLocalMetastore().cancelAlter(cancelStmt);
            Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        } finally {
            rollupHandler.clearJobs();
        }
    }

    @Test
    public void testLeaderPrunesFinishedJob() throws Exception {
        completeTasksAndPublishOnSend();
        createBaseTable("base_table_prune_finished");
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        LakeRollupJob job = createJob(
                "create materialized view mv_prune_finished as select k2, k1 from base_table_prune_finished order by k2;");
        Assertions.assertFalse(job.physicalPartitionIdToRollupIndex.isEmpty());

        runToFinished(job);

        assertPruned(job);
        List<LakeRollupJob> entries = readJournaledEntries(job.getJobId());
        for (LakeRollupJob entry : entries) {
            Assertions.assertFalse(entry.physicalPartitionIdToRollupIndex.isEmpty());
        }
        LakeRollupJob finishedEntry = entries.get(entries.size() - 1);
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, finishedEntry.getJobState());
        List<List<Comparable>> rowBefore = new ArrayList<>();
        finishedEntry.getInfo(rowBefore);
        finishedEntry.pruneMeta();
        assertPruned(finishedEntry);
        List<List<Comparable>> rowAfter = new ArrayList<>();
        finishedEntry.getInfo(rowAfter);
        Assertions.assertEquals(rowBefore, rowAfter);
    }

    @Test
    public void testLeaderPrunesCancelledJob() throws Exception {
        new MockUp<LakeRollupJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
            }
        };
        OlapTable baseTable = createBaseTable("base_table_prune_cancelled");
        LakeRollupJob job = createJob(
                "create materialized view mv_prune_cancelled as select k2, k1 from base_table_prune_cancelled order by k2;");
        job.runPendingJob();
        job.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        Assertions.assertTrue(job.rollupBatchTask.getTaskNum() > 0);

        Assertions.assertTrue(job.cancel("cancel"));

        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertPruned(job);
        Assertions.assertFalse(baseTable.hasMaterializedIndex("mv_prune_cancelled"));
    }

    @Test
    public void testFollowerReplayPrunesFinishedJob() throws Exception {
        completeTasksAndPublishOnSend();
        OlapTable baseTable = createBaseTable("base_table_prune_follower");
        // The follower starts from the catalog as it was before the rollup.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage initialImage = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(initialImage.getImageWriter());

        LakeRollupJob job = createJob(
                "create materialized view mv_prune_follower as select k2, k1 from base_table_prune_follower order by k2;");
        runToFinished(job);
        List<LakeRollupJob> entries = readJournaledEntries(job.getJobId());

        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        SRMetaBlockReader blockReader = new SRMetaBlockReaderV2(initialImage.getJsonReader());
        followerMetastore.load(blockReader);
        blockReader.close();
        MaterializedViewHandler followerHandler = new MaterializedViewHandler();
        LocalMetastore leaderMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        try {
            GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
            for (LakeRollupJob entry : entries) {
                followerHandler.replayAlterJobV2(entry);
            }
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(leaderMetastore);
        }

        // The handler keeps the first replayed entry and copies the data of each later one into it.
        LakeRollupJob retained = entries.get(0);
        Assertions.assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, retained.getJobState());
        assertPruned(retained);
        for (LakeRollupJob entry : entries.subList(1, entries.size())) {
            Assertions.assertFalse(entry.physicalPartitionIdToRollupIndex.isEmpty());
        }
        OlapTable followerTable = (OlapTable) followerMetastore.getTable(DB, baseTable.getName());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, followerTable.getState());
        long rollupIndexMetaId = followerTable.getIndexMetaIdByName("mv_prune_follower");
        for (PhysicalPartition physicalPartition : followerTable.getAllPhysicalPartitions()) {
            MaterializedIndex rollupIndex = physicalPartition.getLatestIndex(rollupIndexMetaId);
            Assertions.assertNotNull(rollupIndex);
            Assertions.assertEquals(MaterializedIndex.IndexState.NORMAL, rollupIndex.getState());
        }
    }

    @Test
    public void testFailedAddRollupRemovesTabletsOfBuiltClauses() throws Exception {
        OlapTable baseTable = createBaseTable("base_table_add_rollup_fails");
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        long tabletCountBefore = invertedIndex.getTabletCount();

        // r1 is built (its shadow tablets are registered in the inverted index) before r2 fails.
        Exception e = Assertions.assertThrows(Exception.class, () -> starRocksAssert.alterTable(
                "ALTER TABLE base_table_add_rollup_fails ADD ROLLUP r1(k2, k1), r2(k2, no_such_col)"));
        Assertions.assertTrue(e.getMessage().contains("no_such_col"), e.getMessage());

        Assertions.assertEquals(tabletCountBefore, invertedIndex.getTabletCount());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, baseTable.getState());
    }

    private static OlapTable createBaseTable(String tableName) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + tableName + "\n" +
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
        return (OlapTable) db.getTable(tableName);
    }

    /**
     * Marks every rollup task finished as soon as it is sent, and lets the publish succeed without reaching a node.
     */
    private static void completeTasksAndPublishOnSend() {
        new MockUp<LakeRollupJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };
        new MockUp<Utils>() {
            @Mock
            public static void sendAggregatePublishVersionRequest(AggregatePublishVersionRequest request,
                    long baseVersion, ComputeResource computeResource,
                    Map<Long, Double> compactionScores,
                    Map<Long, com.starrocks.proto.TabletStatPB> tabletStats,
                    java.util.List<com.starrocks.proto.VectorIndexBuildInfoPB> vectorIndexBuildInfos) {
            }
        };
    }

    private static void runToFinished(AlterJobV2 job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (!job.isDone()) {
            Assertions.assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState(), job.errMsg);
    }

    /**
     * Reads the journal entries of the job, from the one that submitted it to the one of its final state.
     */
    private static List<LakeRollupJob> readJournaledEntries(long jobId) throws Exception {
        List<LakeRollupJob> entries = new ArrayList<>();
        AlterJobV2 entry;
        do {
            entry = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_JOB_V2);
            Assertions.assertEquals(jobId, entry.getJobId());
            entries.add((LakeRollupJob) entry);
        } while (!entry.isDone());
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, entries.get(0).getJobState());
        return entries;
    }

    private static void assertPruned(LakeRollupJob job) {
        Assertions.assertTrue(job.physicalPartitionIdToRollupIndex.isEmpty());
        Assertions.assertTrue(job.physicalPartitionIdToBaseRollupTabletIdMap.isEmpty());
        Assertions.assertEquals(0, job.rollupBatchTask.getTaskNum());
    }
}