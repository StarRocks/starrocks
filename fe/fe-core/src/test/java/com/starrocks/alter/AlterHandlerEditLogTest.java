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

import com.starrocks.common.Config;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RemoveAlterJobV2OperationLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class AlterHandlerEditLogTest {
    private SchemaChangeHandler schemaChangeHandler;
    private static final long JOB_ID_1 = 20001L;
    private static final long JOB_ID_2 = 20002L;
    private static final long DB_ID = 20003L;
    private static final long TABLE_ID = 20004L;
    private final int savedHistoryJobKeepMaxNum = Config.history_job_keep_max_num;
    private final int savedMaxCapRemovalsPerRound = AlterHandler.maxCapRemovalsPerRound;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        schemaChangeHandler.clearJobs();
    }

    @AfterEach
    public void tearDown() {
        Config.history_job_keep_max_num = savedHistoryJobKeepMaxNum;
        AlterHandler.maxCapRemovalsPerRound = savedMaxCapRemovalsPerRound;
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testClearExpireFinishedOrCancelledAlterJobsV2NormalCase(@Mocked ClusterSnapshotMgr clusterSnapshotMgr)
            throws Exception {
        mockSharedNothingMode();
        mockClusterSnapshotMgr(clusterSnapshotMgr);
        new Expectations() {
            {
                clusterSnapshotMgr.isDeletionSafeToExecute(anyLong);
                times = 0;
            }
        };

        // 1. Create expired finished alter job
        long oldFinishedTime = System.currentTimeMillis() - (Config.history_job_keep_max_second + 100) * 1000L;
        MockAlterJobV2 expiredJob = new MockAlterJobV2(JOB_ID_1, AlterJobV2.JobType.SCHEMA_CHANGE,
                DB_ID, TABLE_ID, "test_table", 10000L);
        expiredJob.setJobState(AlterJobV2.JobState.FINISHED);
        expiredJob.setFinishedTimeMs(oldFinishedTime);
        schemaChangeHandler.addAlterJobV2(expiredJob);

        // 2. Create non-expired finished alter job
        MockAlterJobV2 nonExpiredJob = new MockAlterJobV2(JOB_ID_2, AlterJobV2.JobType.SCHEMA_CHANGE,
                DB_ID, TABLE_ID, "test_table2", 10000L);
        nonExpiredJob.setJobState(AlterJobV2.JobState.FINISHED);
        nonExpiredJob.setFinishedTimeMs(System.currentTimeMillis());
        schemaChangeHandler.addAlterJobV2(nonExpiredJob);

        // 3. Verify initial state
        Assertions.assertEquals(2, schemaChangeHandler.getAlterJobsV2().size());
        Assertions.assertTrue(schemaChangeHandler.getAlterJobsV2().containsKey(JOB_ID_1));
        Assertions.assertTrue(schemaChangeHandler.getAlterJobsV2().containsKey(JOB_ID_2));

        // 4. Execute clearExpireFinishedOrCancelledAlterJobsV2
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();

        // 5. Verify master state - expired job should be removed
        Assertions.assertEquals(1, schemaChangeHandler.getAlterJobsV2().size());
        Assertions.assertFalse(schemaChangeHandler.getAlterJobsV2().containsKey(JOB_ID_1));
        Assertions.assertTrue(schemaChangeHandler.getAlterJobsV2().containsKey(JOB_ID_2));

        // 6. Test follower replay
        RemoveAlterJobV2OperationLog replayLog = (RemoveAlterJobV2OperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_REMOVE_ALTER_JOB_V2);

        // Verify replay log
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(JOB_ID_1, replayLog.getJobId());
        Assertions.assertEquals(AlterJobV2.JobType.SCHEMA_CHANGE, replayLog.getType());

        // Create follower handler and the same job, then replay
        SchemaChangeHandler followerHandler = new SchemaChangeHandler();
        MockAlterJobV2 followerExpiredJob = new MockAlterJobV2(JOB_ID_1, AlterJobV2.JobType.SCHEMA_CHANGE,
                DB_ID, TABLE_ID, "test_table", 10000L);
        followerExpiredJob.setJobState(AlterJobV2.JobState.FINISHED);
        followerExpiredJob.setFinishedTimeMs(oldFinishedTime);
        followerHandler.addAlterJobV2(followerExpiredJob);

        MockAlterJobV2 followerNonExpiredJob = new MockAlterJobV2(JOB_ID_2, AlterJobV2.JobType.SCHEMA_CHANGE,
                DB_ID, TABLE_ID, "test_table2", 10000L);
        followerNonExpiredJob.setJobState(AlterJobV2.JobState.FINISHED);
        followerNonExpiredJob.setFinishedTimeMs(System.currentTimeMillis());
        followerHandler.addAlterJobV2(followerNonExpiredJob);

        Assertions.assertEquals(2, followerHandler.getAlterJobsV2().size());

        followerHandler.replayRemoveAlterJobV2(replayLog);

        // 7. Verify follower state
        Assertions.assertEquals(1, followerHandler.getAlterJobsV2().size());
        Assertions.assertFalse(followerHandler.getAlterJobsV2().containsKey(JOB_ID_1));
        Assertions.assertTrue(followerHandler.getAlterJobsV2().containsKey(JOB_ID_2));
    }

    @Test
    public void testClearExpireFinishedOrCancelledAlterJobsV2SharedDataModeWaitsForSafeDeletion()
            throws Exception {
        ClusterSnapshotMgr clusterSnapshotMgr = mockClusterSnapshotMgr(false);
        mockSharedDataMode();

        long oldFinishedTime = System.currentTimeMillis() - (Config.history_job_keep_max_second + 100) * 1000L;
        MockAlterJobV2 expiredJob = new MockAlterJobV2(JOB_ID_1, AlterJobV2.JobType.SCHEMA_CHANGE,
                DB_ID, TABLE_ID, "test_table", 10000L);
        expiredJob.setJobState(AlterJobV2.JobState.FINISHED);
        expiredJob.setFinishedTimeMs(oldFinishedTime);
        schemaChangeHandler.addAlterJobV2(expiredJob);

        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();

        Assertions.assertEquals(1, schemaChangeHandler.getAlterJobsV2().size());
        Assertions.assertTrue(schemaChangeHandler.getAlterJobsV2().containsKey(JOB_ID_1));
        org.mockito.Mockito.verify(clusterSnapshotMgr).isDeletionSafeToExecute(oldFinishedTime);
    }

    @Test
    public void testClearExpireFinishedOrCancelledAlterJobsV2EditLogException() throws Exception {
        mockSharedNothingMode();

        // 1. Create expired finished alter job
        long oldFinishedTime = System.currentTimeMillis() - (Config.history_job_keep_max_second + 100) * 1000L;
        MockAlterJobV2 expiredJob = new MockAlterJobV2(JOB_ID_1, AlterJobV2.JobType.SCHEMA_CHANGE,
                DB_ID, TABLE_ID, "test_table", 10000L);
        expiredJob.setJobState(AlterJobV2.JobState.FINISHED);
        expiredJob.setFinishedTimeMs(oldFinishedTime);
        schemaChangeHandler.addAlterJobV2(expiredJob);

        // 2. Mock EditLog.logRemoveExpiredAlterJobV2 to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logRemoveExpiredAlterJobV2(any(RemoveAlterJobV2OperationLog.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute clearExpireFinishedOrCancelledAlterJobsV2 and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 4. Verify job is still present (not removed due to exception)
        Assertions.assertEquals(1, schemaChangeHandler.getAlterJobsV2().size());
        Assertions.assertTrue(schemaChangeHandler.getAlterJobsV2().containsKey(JOB_ID_1));
    }

    private MockAlterJobV2 addDoneJob(SchemaChangeHandler handler, long jobId, AlterJobV2.JobState state,
                                      long finishedTimeMs) {
        MockAlterJobV2 job = new MockAlterJobV2(jobId, AlterJobV2.JobType.SCHEMA_CHANGE,
                DB_ID, TABLE_ID, "test_table", 10000L);
        job.setJobState(state);
        job.setFinishedTimeMs(finishedTimeMs);
        handler.addAlterJobV2(job);
        return job;
    }

    private static Set<Long> removedJobIds(int count) throws Exception {
        Set<Long> ids = new LinkedHashSet<>();
        for (int i = 0; i < count; i++) {
            RemoveAlterJobV2OperationLog log = (RemoveAlterJobV2OperationLog) UtFrameUtils
                    .PseudoJournalReplayer.replayNextJournal(OperationType.OP_REMOVE_ALTER_JOB_V2);
            Assertions.assertNotNull(log);
            ids.add(log.getJobId());
        }
        return ids;
    }

    @Test
    public void testCapRemovesOldestFinishedJobsBeyondLimit() throws Exception {
        mockSharedNothingMode();
        Config.history_job_keep_max_num = 2;
        long now = System.currentTimeMillis();

        // the oldest finished job has the largest job id, so ordering by id alone would be wrong
        addDoneJob(schemaChangeHandler, 30004L, AlterJobV2.JobState.FINISHED, now - 4000);
        addDoneJob(schemaChangeHandler, 30003L, AlterJobV2.JobState.CANCELLED, now - 3000);
        addDoneJob(schemaChangeHandler, 30002L, AlterJobV2.JobState.FINISHED, now - 2000);
        addDoneJob(schemaChangeHandler, 30001L, AlterJobV2.JobState.CANCELLED, now - 1000);
        MockAlterJobV2 running = new MockAlterJobV2(10001L, AlterJobV2.JobType.SCHEMA_CHANGE,
                DB_ID, TABLE_ID, "test_table", 10000L);
        running.setJobState(AlterJobV2.JobState.RUNNING);
        schemaChangeHandler.addAlterJobV2(running);
        MockAlterJobV2 rewriting = new MockAlterJobV2(10002L, AlterJobV2.JobType.SCHEMA_CHANGE,
                DB_ID, TABLE_ID, "test_table", 10000L);
        rewriting.setJobState(AlterJobV2.JobState.FINISHED_REWRITING);
        schemaChangeHandler.addAlterJobV2(rewriting);

        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();

        Assertions.assertEquals(Set.of(30002L, 30001L, 10001L, 10002L), schemaChangeHandler.getAlterJobsV2().keySet());

        // removals are journaled oldest first and replay to the same set on a follower
        Assertions.assertEquals(List.of(30004L, 30003L), new ArrayList<>(removedJobIds(2)));
        SchemaChangeHandler follower = new SchemaChangeHandler();
        addDoneJob(follower, 30004L, AlterJobV2.JobState.FINISHED, now - 4000);
        addDoneJob(follower, 30003L, AlterJobV2.JobState.CANCELLED, now - 3000);
        addDoneJob(follower, 30002L, AlterJobV2.JobState.FINISHED, now - 2000);
        addDoneJob(follower, 30001L, AlterJobV2.JobState.CANCELLED, now - 1000);
        follower.replayRemoveAlterJobV2(new RemoveAlterJobV2OperationLog(30004L, AlterJobV2.JobType.SCHEMA_CHANGE));
        follower.replayRemoveAlterJobV2(new RemoveAlterJobV2OperationLog(30003L, AlterJobV2.JobType.SCHEMA_CHANGE));
        Assertions.assertEquals(Set.of(30002L, 30001L), follower.getAlterJobsV2().keySet());
    }

    @Test
    public void testCapBreaksFinishTimeTiesByJobId() {
        mockSharedNothingMode();
        Config.history_job_keep_max_num = 1;
        long finishedTime = System.currentTimeMillis() - 1000;
        addDoneJob(schemaChangeHandler, 30002L, AlterJobV2.JobState.FINISHED, finishedTime);
        addDoneJob(schemaChangeHandler, 30001L, AlterJobV2.JobState.FINISHED, finishedTime);

        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();

        Assertions.assertEquals(Set.of(30002L), schemaChangeHandler.getAlterJobsV2().keySet());
    }

    @Test
    public void testCapKeepsProtectedOlderJob() {
        mockSharedNothingMode();
        Config.history_job_keep_max_num = 1;
        long now = System.currentTimeMillis();
        ProtectableMockAlterJobV2 older = new ProtectableMockAlterJobV2(30001L);
        older.setJobState(AlterJobV2.JobState.FINISHED);
        older.setFinishedTimeMs(now - 2000);
        older.historySchemaInUse = true;
        schemaChangeHandler.addAlterJobV2(older);
        addDoneJob(schemaChangeHandler, 30002L, AlterJobV2.JobState.FINISHED, now - 1000);

        // the older job is protected and the newer one is inside the newest-1 window: nothing goes
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertEquals(Set.of(30001L, 30002L), schemaChangeHandler.getAlterJobsV2().keySet());

        older.historySchemaInUse = false;
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertEquals(Set.of(30002L), schemaChangeHandler.getAlterJobsV2().keySet());
    }

    @Test
    public void testCapSkipsProtectedJobAndRemovesNewerCandidates() throws Exception {
        mockSharedNothingMode();
        Config.history_job_keep_max_num = 1;
        long now = System.currentTimeMillis();
        ProtectableMockAlterJobV2 protectedJob = new ProtectableMockAlterJobV2(30001L);
        protectedJob.setJobState(AlterJobV2.JobState.FINISHED);
        protectedJob.setFinishedTimeMs(now - 3000);
        protectedJob.historySchemaInUse = true;
        schemaChangeHandler.addAlterJobV2(protectedJob);
        addDoneJob(schemaChangeHandler, 30002L, AlterJobV2.JobState.FINISHED, now - 2000);
        addDoneJob(schemaChangeHandler, 30003L, AlterJobV2.JobState.FINISHED, now - 1000);

        // the oldest candidate is protected, yet the next older one must still be removed
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();

        Assertions.assertEquals(Set.of(30001L, 30003L), schemaChangeHandler.getAlterJobsV2().keySet());
        Assertions.assertEquals(List.of(30002L), new ArrayList<>(removedJobIds(1)));
    }

    @Test
    public void testCapRespectsSnapshotDeletionGateInSharedData() {
        mockSharedDataMode();
        Config.history_job_keep_max_num = 1;
        long now = System.currentTimeMillis();
        addDoneJob(schemaChangeHandler, 30001L, AlterJobV2.JobState.FINISHED, now - 2000);
        addDoneJob(schemaChangeHandler, 30002L, AlterJobV2.JobState.FINISHED, now - 1000);

        mockClusterSnapshotMgr(false);
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertEquals(Set.of(30001L, 30002L), schemaChangeHandler.getAlterJobsV2().keySet());

        mockClusterSnapshotMgr(true);
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertEquals(Set.of(30002L), schemaChangeHandler.getAlterJobsV2().keySet());
    }

    @Test
    public void testCapDisabledWhenNonPositive() {
        mockSharedNothingMode();
        long now = System.currentTimeMillis();
        addDoneJob(schemaChangeHandler, 30001L, AlterJobV2.JobState.FINISHED, now - 3000);
        addDoneJob(schemaChangeHandler, 30002L, AlterJobV2.JobState.FINISHED, now - 2000);
        addDoneJob(schemaChangeHandler, 30003L, AlterJobV2.JobState.FINISHED, now - 1000);

        for (int limit : new int[] {0, -1}) {
            Config.history_job_keep_max_num = limit;
            schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
            Assertions.assertEquals(3, schemaChangeHandler.getAlterJobsV2().size(), "limit " + limit);
        }
    }

    @Test
    public void testCapAndAgeTogether() {
        mockSharedNothingMode();
        Config.history_job_keep_max_num = 2;
        long now = System.currentTimeMillis();
        addDoneJob(schemaChangeHandler, 30000L, AlterJobV2.JobState.FINISHED,
                now - (Config.history_job_keep_max_second + 100) * 1000L);
        addDoneJob(schemaChangeHandler, 30001L, AlterJobV2.JobState.FINISHED, now - 3000);
        addDoneJob(schemaChangeHandler, 30002L, AlterJobV2.JobState.FINISHED, now - 2000);
        addDoneJob(schemaChangeHandler, 30003L, AlterJobV2.JobState.FINISHED, now - 1000);

        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();

        Assertions.assertEquals(Set.of(30002L, 30003L), schemaChangeHandler.getAlterJobsV2().keySet());
    }

    @Test
    public void testCapRemovesAtMostBudgetPerRound() throws Exception {
        mockSharedNothingMode();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        AlterHandler.maxCapRemovalsPerRound = 3;
        Config.history_job_keep_max_num = 1;
        long now = System.currentTimeMillis();
        for (int i = 1; i <= 8; i++) {
            addDoneJob(schemaChangeHandler, 30000L + i, AlterJobV2.JobState.FINISHED, now - 10000 + i * 100L);
        }

        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertEquals(5, schemaChangeHandler.getAlterJobsV2().size());
        Assertions.assertEquals(Set.of(30001L, 30002L, 30003L), removedJobIds(3));

        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertEquals(2, schemaChangeHandler.getAlterJobsV2().size());
        Assertions.assertEquals(Set.of(30004L, 30005L, 30006L), removedJobIds(3));

        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertEquals(Set.of(30008L), schemaChangeHandler.getAlterJobsV2().keySet());
        Assertions.assertEquals(Set.of(30007L), removedJobIds(1));
    }

    @Test
    public void testCapEditLogExceptionKeepsJob() {
        mockSharedNothingMode();
        Config.history_job_keep_max_num = 1;
        long now = System.currentTimeMillis();
        addDoneJob(schemaChangeHandler, 30001L, AlterJobV2.JobState.FINISHED, now - 2000);
        addDoneJob(schemaChangeHandler, 30002L, AlterJobV2.JobState.FINISHED, now - 1000);

        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRemoveExpiredAlterJobV2(any(RemoveAlterJobV2OperationLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2());
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));
        Assertions.assertEquals(Set.of(30001L, 30002L), schemaChangeHandler.getAlterJobsV2().keySet());
    }

    private static void mockSharedNothingMode() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return false;
            }

            @Mock
            public boolean isSharedNothingMode() {
                return true;
            }
        };
    }

    private static void mockSharedDataMode() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }

            @Mock
            public boolean isSharedNothingMode() {
                return false;
            }
        };
    }

    private static void mockClusterSnapshotMgr(ClusterSnapshotMgr clusterSnapshotMgr) {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public ClusterSnapshotMgr getClusterSnapshotMgr() {
                return clusterSnapshotMgr;
            }
        };
    }

    private static ClusterSnapshotMgr mockClusterSnapshotMgr(boolean deletionSafe) {
        ClusterSnapshotMgr clusterSnapshotMgr = spy(new ClusterSnapshotMgr());
        org.mockito.Mockito.doReturn(deletionSafe).when(clusterSnapshotMgr)
                .isDeletionSafeToExecute(org.mockito.ArgumentMatchers.anyLong());
        mockClusterSnapshotMgr(clusterSnapshotMgr);
        return clusterSnapshotMgr;
    }

    // A mock job whose history schema can be switched between "still bound by a running load" and released.
    private static class ProtectableMockAlterJobV2 extends MockAlterJobV2 {
        private volatile boolean historySchemaInUse;

        ProtectableMockAlterJobV2(long jobId) {
            super(jobId, JobType.SCHEMA_CHANGE, DB_ID, TABLE_ID, "test_table", 10000L);
        }

        @Override
        protected boolean expireHistorySchema(OlapTableHistorySchema historySchema) {
            return !historySchemaInUse;
        }
    }

    // Mock AlterJobV2 for testing
    private static class MockAlterJobV2 extends AlterJobV2 {
        public MockAlterJobV2(long jobId, JobType jobType, long dbId, long tableId, String tableName, long timeoutMs) {
            super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
        }

        @Override
        protected void resetTransientState() {
            // Mock: nothing transient to reset.
        }

        @Override
        protected void runPendingJob() throws AlterCancelException {
            // Mock implementation
        }

        @Override
        protected void runWaitingTxnJob() throws AlterCancelException {
            // Mock implementation
        }

        @Override
        protected void runRunningJob() throws AlterCancelException {
            // Mock implementation
        }

        @Override
        protected void runFinishedRewritingJob() throws AlterCancelException {
            // Mock implementation
        }

        @Override
        protected boolean cancelImpl(String errMsg) {
            return true;
        }

        @Override
        protected void getInfo(List<List<Comparable>> infos) {
            // Mock implementation
        }

        @Override
        public void replay(AlterJobV2 replayedJob) {
            // Mock implementation
        }

        @Override
        public java.util.Optional<Long> getTransactionId() {
            return java.util.Optional.empty();
        }

        @Override
        public void write(java.io.DataOutput out) throws java.io.IOException {
            // Mock implementation
        }

        @Override
        public AlterJobV2 copyForPersist() {
            return new MockAlterJobV2(getJobId(), getType(), getDbId(), getTableId(), getTableName(), getTimeoutMs());
        }
    }
}
