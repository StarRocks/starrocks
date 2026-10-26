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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/RollupJobV2Test.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RollupJobV2Test extends DDLTestBase {
    private static AddRollupClause clause;
    private static AddRollupClause clause2;

    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2Test.class);

    @BeforeAll
    public static void beforeAll() {
        new MockUp<MaterializedViewHandler>() {
            @Mock
            protected void runAfterLeaseValid() {
                // do nothing
            }
        };

        DDLTestBase.beforeAll();
        UtFrameUtils.setUpForPersistTest();
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        clause = new AddRollupClause(GlobalStateMgrTestUtil.testRollupIndex2, Lists.newArrayList("v1"), null,
                    GlobalStateMgrTestUtil.testTable1, null);
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);

        clause2 = new AddRollupClause(GlobalStateMgrTestUtil.testRollupIndex3, Lists.newArrayList("v1", "v2"), null,
                    GlobalStateMgrTestUtil.testTable1, null);
        analyzer.analyze(null, clause2);

        AgentTaskQueue.clearAllTasks();
    }

    @AfterEach
    public void tearDown() {
        GlobalStateMgr.getCurrentState().getRollupHandler().clearJobs();
    }

    @Test
    public void testRunAlterJobV2CleansUpCompletedJobAfterRunThrows() {
        long dbId = Long.MAX_VALUE;
        long tableId = Long.MAX_VALUE - 1;
        long jobId = 1L;
        AtomicBoolean done = new AtomicBoolean(false);
        AlterJobV2 job = mock(AlterJobV2.class);
        when(job.getDbId()).thenReturn(dbId);
        when(job.getTableId()).thenReturn(tableId);
        when(job.getJobId()).thenReturn(jobId);
        when(job.getType()).thenReturn(AlterJobV2.JobType.ROLLUP);
        when(job.getJobState()).thenReturn(AlterJobV2.JobState.RUNNING);
        when(job.isDone()).thenAnswer(invocation -> done.get());
        when(job.isTimeout()).thenReturn(false);
        doAnswer(invocation -> {
            done.set(true);
            throw new RuntimeException("NORMAL");
        }).when(job).run();

        MaterializedViewHandler handler = new MaterializedViewHandler();
        handler.getAlterJobsV2().put(jobId, job);
        handler.getTableRunningJobMap().computeIfAbsent(tableId, key -> new HashSet<>()).add(jobId);
        Map<Long, Set<Long>> tableNotFinalStateJobMap =
                Deencapsulation.getField(handler, "tableNotFinalStateJobMap");
        tableNotFinalStateJobMap.computeIfAbsent(tableId, key -> new HashSet<>()).add(jobId);

        assertDoesNotThrow(() -> Deencapsulation.invoke(handler, "runAlterJobV2"));
        assertFalse(handler.getTableRunningJobMap().getOrDefault(tableId, Set.of()).contains(jobId));
        assertFalse(tableNotFinalStateJobMap.getOrDefault(tableId, Set.of()).contains(jobId));
    }

    @Test
    public void testRunRollupJobConcurrentLimit() throws StarRocksException {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        alterClauses.add(clause2);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();

        Deencapsulation.invoke(materializedViewHandler, "runAlterJobV2");

        assertEquals(Config.max_running_rollup_job_num_per_table,
                    materializedViewHandler.getTableRunningJobMap().get(olapTable.getId()).size());
        assertEquals(2, alterJobsV2.size());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    @Test
    public void testAddSchemaChange() throws StarRocksException {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();
        materializedViewHandler.clearJobs(); // Disable the execution of job in background thread

        // runPendingJob
        rollupJob.runPendingJob();
        assertEquals(AlterJobV2.JobState.WAITING_TXN, rollupJob.getJobState());
        assertEquals(2, testPartition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL).size());
        assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        rollupJob.runWaitingTxnJob();
        assertEquals(AlterJobV2.JobState.RUNNING, rollupJob.getJobState());

        int retryCount = 0;
        while (!rollupJob.getJobState().isFinalState()) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            rollupJob.runRunningJob();
            if (rollupJob.getJobState() == AlterJobV2.JobState.FINISHED) {
                break;
            }
            retryCount++;
            LOG.info("rollupJob is waiting for JobState retryCount:" + retryCount);
        }

        // finish alter tasks
        assertEquals(AlterJobV2.JobState.FINISHED, rollupJob.getJobState());
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getDefaultPhysicalPartition().getLatestBaseIndex();
        assertEquals(MaterializedIndex.IndexState.NORMAL, baseIndex.getState());
        assertEquals(Partition.PartitionState.NORMAL, testPartition.getState());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());

        LocalTablet baseTablet = (LocalTablet) baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getImmutableReplicas();
        Replica replica1 = replicas.get(0);

        assertEquals(1, replica1.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(1, replica1.getLastSuccessVersion());

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        rollupJob.runPendingJob();
        assertEquals(AlterJobV2.JobState.PENDING, rollupJob.getJobState());

        // table is stable, runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        rollupJob.runPendingJob();
        assertEquals(AlterJobV2.JobState.WAITING_TXN, rollupJob.getJobState());
        assertEquals(2, testPartition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL).size());
        assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        rollupJob.runWaitingTxnJob();
        assertEquals(AlterJobV2.JobState.RUNNING, rollupJob.getJobState());

        int retryCount = 0;
        int maxRetry = 5;
        while (retryCount < maxRetry) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            if (rollupJob.getJobState() == AlterJobV2.JobState.FINISHED) {
                break;
            }
            rollupJob.runRunningJob();
            retryCount++;
            LOG.info("rollupJob is waiting for JobState retryCount:" + retryCount);
        }
    }

    @Test
    public void testReplayPendingRollupJob() throws Exception {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());

        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();
        rollupJob.replay(rollupJob);
    }

    @Test
    public void testCancelPendingJobWithFlag() throws Exception {
        // Use a standalone handler whose daemon thread is not started, so the job can never
        // be picked up by the background alter scheduler. Using the global rollup handler is
        // racy: its daemon may run the job to WAITING_TXN between process() and the explicit
        // runPendingJob() call below, even if clearJobs() is called right after process().
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        rollupJob.setIsCancelling(true);
        rollupJob.runPendingJob();
        rollupJob.setIsCancelling(false);

        rollupJob.setWaitingCreatingReplica(true);
        rollupJob.cancel("");
        rollupJob.setWaitingCreatingReplica(false);
    }

    @Test
    public void testCancelSkipsReplicaLatchAlreadyDropped() throws Exception {
        OlapTable table = createTable("t_rollup_cancel_no_latch");
        RollupJobV2 job = addRollup(new MaterializedViewHandler(), table, "r_cancel_no_latch");
        // A cancel can see the replica wait in progress and then find no latch: the wait has ended since.
        job.setWaitingCreatingReplica(true);

        assertTrue(job.cancel("cancel"));

        assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
    }

    @Test
    public void testLeaderPrunesFinishedJob() throws Exception {
        OlapTable table = createTable("t_rollup_prune_finished");
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        RollupJobV2 job = addRollup(new MaterializedViewHandler(), table, "r_prune_finished");
        assertFalse(job.getPartitionIdToRollupIndex().isEmpty());

        // The handler is never started, so only run() can prune the job.
        runToFinished(job);

        assertPruned(job);
        assertRollupIndexVisible(table, "r_prune_finished");
        List<RollupJobV2> entries = readJournaledEntries(job.getJobId());
        for (RollupJobV2 entry : entries) {
            assertFalse(entry.getPartitionIdToRollupIndex().isEmpty());
        }
        RollupJobV2 finishedEntry = entries.get(entries.size() - 1);
        assertEquals(AlterJobV2.JobState.FINISHED, finishedEntry.getJobState());
        List<List<Comparable>> rowBefore = new ArrayList<>();
        finishedEntry.getInfo(rowBefore);
        finishedEntry.pruneMeta();
        assertPruned(finishedEntry);
        List<List<Comparable>> rowAfter = new ArrayList<>();
        finishedEntry.getInfo(rowAfter);
        assertEquals(rowBefore, rowAfter);
    }

    @Test
    public void testLeaderPrunesCancelledJob() throws Exception {
        OlapTable table = createTable("t_rollup_prune_cancelled");
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        RollupJobV2 job = addRollup(new MaterializedViewHandler(), table, "r_prune_cancelled");
        job.runPendingJob();
        job.runWaitingTxnJob();
        assertEquals(AlterJobV2.JobState.RUNNING, job.getJobState());
        assertTrue(job.rollupBatchTask.getTaskNum() > 0);
        assertNotNull(Deencapsulation.getField(job, "createReplicaLatch"));

        assertTrue(job.cancel("cancel"));

        assertEquals(AlterJobV2.JobState.CANCELLED, job.getJobState());
        assertPruned(job);
        assertFalse(table.hasMaterializedIndex("r_prune_cancelled"));
        List<RollupJobV2> entries = readJournaledEntries(job.getJobId());
        assertEquals(AlterJobV2.JobState.CANCELLED, entries.get(entries.size() - 1).getJobState());
        for (RollupJobV2 entry : entries) {
            assertFalse(entry.getPartitionIdToRollupIndex().isEmpty());
        }
    }

    @Test
    public void testFollowerReplayPrunesFinishedJob() throws Exception {
        OlapTable table = createTable("t_rollup_prune_follower");
        // The follower starts from the catalog as it was before the rollup.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage initialImage = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(initialImage.getImageWriter());

        RollupJobV2 job = addRollup(new MaterializedViewHandler(), table, "r_prune_follower");
        runToFinished(job);
        List<RollupJobV2> entries = readJournaledEntries(job.getJobId());

        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        SRMetaBlockReader blockReader = new SRMetaBlockReaderV2(initialImage.getJsonReader());
        followerMetastore.load(blockReader);
        blockReader.close();
        MaterializedViewHandler followerHandler = new MaterializedViewHandler();
        LocalMetastore leaderMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        try {
            GlobalStateMgr.getCurrentState().setLocalMetastore(followerMetastore);
            for (RollupJobV2 entry : entries) {
                followerHandler.replayAlterJobV2(entry);
            }
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(leaderMetastore);
        }

        // The handler keeps the first replayed entry and applies the later ones to it.
        RollupJobV2 retained = entries.get(0);
        assertSame(retained, followerHandler.getAlterJobsV2().get(job.getJobId()));
        assertEquals(AlterJobV2.JobState.FINISHED, retained.getJobState());
        assertPruned(retained);
        for (RollupJobV2 entry : entries.subList(1, entries.size())) {
            assertFalse(entry.getPartitionIdToRollupIndex().isEmpty());
        }
        Database followerDb = followerMetastore.getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable followerTable = (OlapTable) followerMetastore.getTable(followerDb.getFullName(), table.getName());
        assertEquals(OlapTableState.NORMAL, followerTable.getState());
        assertRollupIndexVisible(followerTable, "r_prune_follower");
    }

    private static OlapTable createTable(String tableName) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + tableName + "(k1 INT, v1 INT, v2 INT) DUPLICATE KEY(k1) " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        return (OlapTable) starRocksAssert.getTable(GlobalStateMgrTestUtil.testDb1, tableName);
    }

    /**
     * Submits ADD ROLLUP through a handler whose daemon is never started, so only the test runs the job.
     */
    private static RollupJobV2 addRollup(MaterializedViewHandler handler, OlapTable table, String rollupName)
            throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "ALTER TABLE " + table.getName() + " ADD ROLLUP " + rollupName + "(k1, v1)", starRocksAssert.getCtx());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        handler.process(stmt.getAlterClauseList(), db, table);
        assertEquals(1, handler.getAlterJobsV2().size());
        return (RollupJobV2) handler.getAlterJobsV2().values().iterator().next();
    }

    private static void runToFinished(AlterJobV2 job) throws Exception {
        long deadlineMs = System.currentTimeMillis() + 60_000L;
        while (!job.isDone()) {
            assertTrue(System.currentTimeMillis() < deadlineMs, "job is still " + job.getJobState());
            job.run();
            Thread.sleep(100);
        }
        assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState(), job.errMsg);
    }

    /**
     * Reads the journal entries of the job, from the one that submitted it to the one of its final state.
     */
    private static List<RollupJobV2> readJournaledEntries(long jobId) throws Exception {
        List<RollupJobV2> entries = new ArrayList<>();
        BatchAlterJobPersistInfo submitted = (BatchAlterJobPersistInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_BATCH_ADD_ROLLUP_V2);
        assertEquals(1, submitted.getAlterJobV2List().size());
        AlterJobV2 entry = submitted.getAlterJobV2List().get(0);
        while (true) {
            assertEquals(jobId, entry.getJobId());
            entries.add((RollupJobV2) entry);
            if (entry.isDone()) {
                return entries;
            }
            entry = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_JOB_V2);
        }
    }

    private static void assertPruned(RollupJobV2 job) {
        assertTrue(job.getPartitionIdToRollupIndex().isEmpty());
        Map<Long, Map<Long, Long>> baseTabletIds =
                Deencapsulation.getField(job, "physicalPartitionIdToBaseRollupTabletIdMap");
        assertTrue(baseTabletIds.isEmpty());
        assertEquals(0, job.rollupBatchTask.getTaskNum());
        assertNull(Deencapsulation.getField(job, "createReplicaLatch"));
    }

    private static void assertRollupIndexVisible(OlapTable table, String rollupName) {
        long rollupIndexMetaId = table.getIndexMetaIdByName(rollupName);
        for (PhysicalPartition physicalPartition : table.getAllPhysicalPartitions()) {
            MaterializedIndex rollupIndex = physicalPartition.getLatestIndex(rollupIndexMetaId);
            assertNotNull(rollupIndex);
            assertEquals(MaterializedIndex.IndexState.NORMAL, rollupIndex.getState());
        }
    }
}
