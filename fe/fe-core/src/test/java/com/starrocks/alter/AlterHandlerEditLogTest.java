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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalTask;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.EditLogException;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.ReleaseAlterJobHistorySchemaLog;
import com.starrocks.persist.RemoveAlterJobV2OperationLog;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
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
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class AlterHandlerEditLogTest {
    private SchemaChangeHandler schemaChangeHandler;
    private static final long JOB_ID_1 = 20001L;
    private static final long JOB_ID_2 = 20002L;
    private static final long DB_ID = 20003L;
    private static final long TABLE_ID = 20004L;
    private static final long INDEX_META_ID = 20005L;
    private static final long PRE_ALTER_SCHEMA_ID = 20006L;
    private static final long POST_ALTER_SCHEMA_ID = 20007L;
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
    public void testDefaultCapKeepsTenThousandJobs() throws Exception {
        mockSharedNothingMode();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        long now = System.currentTimeMillis();
        for (int i = 1; i <= 10001; i++) {
            addDoneJob(schemaChangeHandler, 30000L + i, AlterJobV2.JobState.FINISHED, now - 20000 + i);
        }

        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();

        Assertions.assertEquals(10000, schemaChangeHandler.getAlterJobsV2().size());
        Assertions.assertFalse(schemaChangeHandler.getAlterJobsV2().containsKey(30001L));
        Assertions.assertEquals(Set.of(30001L), removedJobIds(1));
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

    @Test
    public void testReleaseHistorySchemaIsJournaledAndReplayedOnFollower() throws Exception {
        LakeTableAsyncFastSchemaChangeJob job = prepareJobWithHistorySchema(JOB_ID_1, System.currentTimeMillis());
        AlterJobV2.persistStateChange(job, AlterJobV2.JobState.FINISHED);
        SchemaChangeHandler follower = replayJobOnNewFollower(JOB_ID_1);
        Assertions.assertTrue(follower.getHistorySchema(DB_ID, TABLE_ID, PRE_ALTER_SCHEMA_ID).isPresent());

        // leader: nothing below the threshold is running, so the payload is released and the release journaled
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertTrue(job.getHistorySchema().orElseThrow().isExpired());
        Assertions.assertSame(job, schemaChangeHandler.getAlterJobsV2().get(JOB_ID_1));
        ReleaseAlterJobHistorySchemaLog log = (ReleaseAlterJobHistorySchemaLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA);
        Assertions.assertEquals(JOB_ID_1, log.getJobId());
        Assertions.assertEquals(DB_ID, log.getDbId());
        Assertions.assertEquals(TABLE_ID, log.getTableId());
        // a released history schema is journaled only once
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        assertNotJournaled(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA);

        // follower: the replay drops the follower's copy, and replaying it again changes nothing
        replayReleaseOnFollower(follower, log);
        Assertions.assertTrue(follower.getAlterJobsV2().get(JOB_ID_1).getHistorySchema().orElseThrow().isExpired());
        Assertions.assertTrue(follower.getHistorySchema(DB_ID, TABLE_ID, PRE_ALTER_SCHEMA_ID).isEmpty());
        replayReleaseOnFollower(follower, log);
        Assertions.assertTrue(follower.getAlterJobsV2().get(JOB_ID_1).getHistorySchema().orElseThrow().isExpired());

        // a job that is gone, or that has no history schema, is skipped
        replayReleaseOnFollower(follower, new ReleaseAlterJobHistorySchemaLog(JOB_ID_2, DB_ID, TABLE_ID));
        MockAlterJobV2 jobWithoutHistorySchema =
                addDoneJob(follower, JOB_ID_2, AlterJobV2.JobState.FINISHED, System.currentTimeMillis());
        replayReleaseOnFollower(follower, new ReleaseAlterJobHistorySchemaLog(JOB_ID_2, DB_ID, TABLE_ID));
        Assertions.assertSame(jobWithoutHistorySchema, follower.getAlterJobsV2().get(JOB_ID_2));
        Assertions.assertEquals(Set.of(JOB_ID_1, JOB_ID_2), follower.getAlterJobsV2().keySet());
    }

    @Test
    public void testReleasedHistorySchemaStaysReleasedInFollowerImage() throws Exception {
        LakeTableAsyncFastSchemaChangeJob job = prepareJobWithHistorySchema(JOB_ID_1, System.currentTimeMillis());
        AlterJobV2.persistStateChange(job, AlterJobV2.JobState.FINISHED);
        SchemaChangeHandler follower = replayJobOnNewFollower(JOB_ID_1);
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        replayReleaseOnFollower(follower, (ReleaseAlterJobHistorySchemaLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA));
        List<List<Comparable>> expectedRows = new ArrayList<>();
        follower.getAlterJobsV2().get(JOB_ID_1).getInfo(expectedRows);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        new AlterJobMgr(follower, new MaterializedViewHandler(), new SystemHandler()).save(image.getImageWriter());
        AlterJobMgr restoredMgr =
                new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(), new SystemHandler());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());
        restoredMgr.load(reader);
        reader.close();

        SchemaChangeHandler restoredHandler = restoredMgr.getSchemaChangeHandler();
        AlterJobV2 restoredJob = restoredHandler.getAlterJobsV2().get(JOB_ID_1);
        Assertions.assertTrue(restoredJob.getHistorySchema().orElseThrow().isExpired());
        Assertions.assertTrue(restoredHandler.getHistorySchema(DB_ID, TABLE_ID, PRE_ALTER_SCHEMA_ID).isEmpty());
        List<List<Comparable>> restoredRows = new ArrayList<>();
        restoredJob.getInfo(restoredRows);
        Assertions.assertEquals(expectedRows, restoredRows);
    }

    @Test
    public void testHistorySchemaOfJobNotDoneIsKept() throws Exception {
        // the job prepared its history schema, but its FINISHED record is not written
        LakeTableAsyncFastSchemaChangeJob job = prepareJobWithHistorySchema(JOB_ID_1, System.currentTimeMillis());

        Assertions.assertFalse(job.isHistorySchemaReleased());
        Assertions.assertFalse(job.isExpire());
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertFalse(job.getHistorySchema().orElseThrow().isExpired());
        assertNotJournaled(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA);

        AlterJobV2.persistStateChange(job, AlterJobV2.JobState.FINISHED);
        // the count-cap check releases a done job's history schema and journals the release on its own
        Assertions.assertTrue(job.isHistorySchemaReleased());
        Assertions.assertTrue(job.getHistorySchema().orElseThrow().isExpired());
        ReleaseAlterJobHistorySchemaLog log = (ReleaseAlterJobHistorySchemaLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA);
        Assertions.assertEquals(JOB_ID_1, log.getJobId());
        Assertions.assertEquals(DB_ID, log.getDbId());
        Assertions.assertEquals(TABLE_ID, log.getTableId());

        // the release is journaled once, whichever path asks again
        Assertions.assertTrue(job.isHistorySchemaReleased());
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        assertNotJournaled(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA);
    }

    @Test
    public void testFailedReleaseKeepsHistorySchemaAndJob() throws Exception {
        mockSharedNothingMode();
        long oldFinishedTime = System.currentTimeMillis() - (Config.history_job_keep_max_second + 100) * 1000L;
        LakeTableAsyncFastSchemaChangeJob job = prepareJobWithHistorySchema(JOB_ID_1, oldFinishedTime);
        AlterJobV2.persistStateChange(job, AlterJobV2.JobState.FINISHED);
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        // the WAL gate is closed, as on an FE that is no longer the active leader
        EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
        editLog.closeWalGate();
        try {
            Assertions.assertThrows(EditLogException.class,
                    () -> schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2());
        } finally {
            editLog.openWalGate();
        }
        Assertions.assertFalse(job.getHistorySchema().orElseThrow().isExpired());
        Assertions.assertSame(job, schemaChangeHandler.getAlterJobsV2().get(JOB_ID_1));

        // the journal writer aborts the write
        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new LinkedBlockingQueue<JournalTask>() {
            @Override
            public void put(JournalTask task) {
                task.markAbort();
            }
        }));
        try {
            Assertions.assertThrows(EditLogException.class,
                    () -> schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(editLog);
        }
        Assertions.assertFalse(job.getHistorySchema().orElseThrow().isExpired());
        Assertions.assertSame(job, schemaChangeHandler.getAlterJobsV2().get(JOB_ID_1));

        // only the release write fails: the error propagates, so the round does not go on to remove the job
        EditLog releaseFails = spy(editLog);
        doThrow(new RuntimeException("release write failed")).when(releaseFails)
                .logReleaseAlterJobHistorySchema(any(ReleaseAlterJobHistorySchemaLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(releaseFails);
        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2());
            Assertions.assertEquals("release write failed", exception.getMessage());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(editLog);
        }
        Assertions.assertFalse(job.getHistorySchema().orElseThrow().isExpired());
        Assertions.assertSame(job, schemaChangeHandler.getAlterJobsV2().get(JOB_ID_1));
        assertNotJournaled(OperationType.OP_REMOVE_ALTER_JOB_V2);

        // the next round releases the payload first, then removes the expired job
        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();
        Assertions.assertTrue(job.getHistorySchema().orElseThrow().isExpired());
        Assertions.assertFalse(schemaChangeHandler.getAlterJobsV2().containsKey(JOB_ID_1));
        ReleaseAlterJobHistorySchemaLog releaseLog = (ReleaseAlterJobHistorySchemaLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA);
        Assertions.assertEquals(JOB_ID_1, releaseLog.getJobId());
        RemoveAlterJobV2OperationLog removeLog = (RemoveAlterJobV2OperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_REMOVE_ALTER_JOB_V2);
        Assertions.assertEquals(JOB_ID_1, removeLog.getJobId());
    }

    @Test
    public void testCapJournalsReleaseBeforeRemovingJob() throws Exception {
        mockSharedNothingMode();
        Config.history_job_keep_max_num = 1;
        long now = System.currentTimeMillis();
        LakeTableAsyncFastSchemaChangeJob older = prepareJobWithHistorySchema(JOB_ID_1, now - 2000);
        AlterJobV2.persistStateChange(older, AlterJobV2.JobState.FINISHED);
        addDoneJob(schemaChangeHandler, JOB_ID_2, AlterJobV2.JobState.FINISHED, now - 1000);

        schemaChangeHandler.clearExpireFinishedOrCancelledAlterJobsV2();

        Assertions.assertEquals(Set.of(JOB_ID_2), schemaChangeHandler.getAlterJobsV2().keySet());
        ReleaseAlterJobHistorySchemaLog releaseLog = (ReleaseAlterJobHistorySchemaLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA);
        Assertions.assertEquals(JOB_ID_1, releaseLog.getJobId());
        RemoveAlterJobV2OperationLog removeLog = (RemoveAlterJobV2OperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_REMOVE_ALTER_JOB_V2);
        Assertions.assertEquals(JOB_ID_1, removeLog.getJobId());
    }

    /**
     * Journals a lake fast schema change job through PENDING, WAITING_TXN and FINISHED_REWRITING, then prepares its
     * history schema the way the job does right before its FINISHED record, which carries it to followers. The
     * caller journals FINISHED. No transaction runs in the database, so the history schema can be released as soon
     * as the job is done.
     */
    private LakeTableAsyncFastSchemaChangeJob prepareJobWithHistorySchema(long jobId, long finishedTimeMs) {
        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_ID) == null) {
            GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(new Database(DB_ID, "test_db"));
        }
        LakeTableAsyncFastSchemaChangeJob job =
                new LakeTableAsyncFastSchemaChangeJob(jobId, DB_ID, TABLE_ID, "test_table", 10000L);
        job.setIndexTabletSchema(INDEX_META_ID, "test_table", newSchemaInfo(POST_ALTER_SCHEMA_ID));
        AlterJobV2.persistStateChange(job, AlterJobV2.JobState.PENDING, () -> schemaChangeHandler.addAlterJobV2(job));
        AlterJobV2.persistStateChange(job, AlterJobV2.JobState.WAITING_TXN);
        AlterJobV2.persistStateChange(job, AlterJobV2.JobState.FINISHED_REWRITING);
        Deencapsulation.setField(job, "historySchema", OlapTableHistorySchema.newBuilder()
                .setHistoryTxnIdThreshold(100L)
                .addIndexSchema(new IndexSchemaInfo(INDEX_META_ID, "test_table", newSchemaInfo(PRE_ALTER_SCHEMA_ID)))
                .build());
        job.setFinishedTimeMs(finishedTimeMs);
        return job;
    }

    private static SchemaInfo newSchemaInfo(long schemaId) {
        return SchemaInfo.newBuilder()
                .setId(schemaId)
                .setKeysType(KeysType.DUP_KEYS)
                .setShortKeyColumnCount((short) 1)
                .setStorageType(TStorageType.COLUMN)
                .addColumn(new Column("c0", IntegerType.INT))
                .build();
    }

    // A follower that replayed the job's journaled states, so its history schema is the copy the FINISHED record carried.
    private static SchemaChangeHandler replayJobOnNewFollower(long jobId) throws Exception {
        SchemaChangeHandler follower = new SchemaChangeHandler();
        for (AlterJobV2.JobState state : List.of(AlterJobV2.JobState.PENDING, AlterJobV2.JobState.WAITING_TXN,
                AlterJobV2.JobState.FINISHED_REWRITING, AlterJobV2.JobState.FINISHED)) {
            AlterJobV2 record = (AlterJobV2) UtFrameUtils.PseudoJournalReplayer
                    .replayNextJournal(OperationType.OP_ALTER_JOB_V2);
            Assertions.assertEquals(jobId, record.getJobId());
            Assertions.assertEquals(state, record.getJobState());
            follower.replayAlterJobV2(record);
        }
        return follower;
    }

    // Replays through the journal dispatch with the WAL gate closed, as on a follower, so a journal write would throw.
    private static void replayReleaseOnFollower(SchemaChangeHandler follower, ReleaseAlterJobHistorySchemaLog log)
            throws Exception {
        GlobalStateMgr followerState = mock(GlobalStateMgr.class);
        when(followerState.getSchemaChangeHandler()).thenReturn(follower);
        EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
        editLog.closeWalGate();
        try {
            editLog.loadJournal(followerState,
                    new JournalEntity(OperationType.OP_RELEASE_ALTER_JOB_HISTORY_SCHEMA, log));
        } finally {
            editLog.openWalGate();
        }
    }

    // Consumes the rest of the journal and fails if it holds an entry of the given type.
    private static void assertNotJournaled(short opCode) {
        Exception exception = Assertions.assertThrows(Exception.class,
                () -> UtFrameUtils.PseudoJournalReplayer.replayNextJournal(opCode));
        Assertions.assertTrue(exception.getMessage().contains("queue is empty"), exception.getMessage());
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
