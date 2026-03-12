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
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.RemoveAlterJobV2OperationLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class AlterHandlerEditLogTest {
    private SchemaChangeHandler schemaChangeHandler;
    private static final long JOB_ID_1 = 20001L;
    private static final long JOB_ID_2 = 20002L;
    private static final long DB_ID = 20003L;
    private static final long TABLE_ID = 20004L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        schemaChangeHandler.clearJobs();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testClearExpireFinishedOrCancelledAlterJobsV2NormalCase() throws Exception {
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
    public void testClearExpireFinishedOrCancelledAlterJobsV2EditLogException() throws Exception {
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

    // Mock AlterJobV2 for testing
    private static class MockAlterJobV2 extends AlterJobV2 {
        public MockAlterJobV2(long jobId, JobType jobType, long dbId, long tableId, String tableName, long timeoutMs) {
            super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
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

