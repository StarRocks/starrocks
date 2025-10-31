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

package com.starrocks.scheduler.history;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.persist.ArchiveTaskRunsLog;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * Test TaskRunHistory.archiveHistory() EditLog functionality
 * This test uses a test subclass to expose the protected archiveHistory() method
 * and to enable archive history functionality in unit test environment
 */
public class TaskRunHistoryEditLogTest {
    private TestableTaskRunHistory masterHistory;
    private boolean originalRunningUnitTest;

    /**
     * Testable subclass that exposes archiveHistory() and allows enabling archive in tests
     */
    private static class TestableTaskRunHistory extends TaskRunHistory {
        private boolean enableArchive = false;

        public void setEnableArchive(boolean enable) {
            this.enableArchive = enable;
        }

        // Override to allow enabling archive in test environment
        @Override
        protected void archiveHistory() {
            // Temporarily set FeConstants.runningUnitTest to false to enable archive
            boolean originalFlag = FeConstants.runningUnitTest;
            try {
                FeConstants.runningUnitTest = !enableArchive;
                super.archiveHistory();
            } finally {
                FeConstants.runningUnitTest = originalFlag;
            }
        }

        public void testArchiveHistory() {
            archiveHistory();
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Save original runningUnitTest flag
        originalRunningUnitTest = FeConstants.runningUnitTest;
        
        // Mock TableKeeper to make it ready
        new MockUp<TableKeeper>() {
            @Mock
            public boolean isReady() {
                return true;
            }
        };
        
        // Mock SimpleExecutor to avoid database operations
        // Use MockUp to replace getRepoExecutor() and executeDML() behavior
        new MockUp<SimpleExecutor>() {
            @Mock
            public static SimpleExecutor getRepoExecutor() {
                // Return a mock instance that doesn't execute SQL
                return new SimpleExecutor("MockExecutor", 
                    com.starrocks.thrift.TResultSinkType.HTTP_PROTOCAL) {
                    @Override
                    public void executeDML(String sql) {
                        // Do nothing - just allow the method to succeed
                    }
                };
            }
        };
        
        // Create testable history instance
        masterHistory = new TestableTaskRunHistory();
        masterHistory.setEnableArchive(true);
    }

    @AfterEach
    public void tearDown() {
        // Restore original runningUnitTest flag
        FeConstants.runningUnitTest = originalRunningUnitTest;
        Config.enable_task_history_archive = true; // Restore default
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testArchiveHistoryNormalCase() throws Exception {
        // 1. Prepare test data - create finished task runs
        String queryId1 = "test_query_id_1";
        String queryId2 = "test_query_id_2";
        String queryId3 = "test_query_id_3";
        
        TaskRunStatus status1 = new TaskRunStatus();
        status1.setQueryId(queryId1);
        status1.setTaskName("test_task_1");
        status1.setState(Constants.TaskRunState.SUCCESS);
        status1.setExpireTime(System.currentTimeMillis() + 100000);
        
        TaskRunStatus status2 = new TaskRunStatus();
        status2.setQueryId(queryId2);
        status2.setTaskName("test_task_2");
        status2.setState(Constants.TaskRunState.FAILED);
        status2.setExpireTime(System.currentTimeMillis() + 100000);
        
        TaskRunStatus status3 = new TaskRunStatus();
        status3.setQueryId(queryId3);
        status3.setTaskName("test_task_3");
        status3.setState(Constants.TaskRunState.PENDING); // Not finished, should not be archived
        status3.setExpireTime(System.currentTimeMillis() + 100000);
        
        // 2. Add history to master
        masterHistory.addHistory(status1);
        masterHistory.addHistory(status2);
        masterHistory.addHistory(status3);
        
        // Verify initial state
        Assertions.assertNotNull(masterHistory.getTask(queryId1));
        Assertions.assertNotNull(masterHistory.getTask(queryId2));
        Assertions.assertNotNull(masterHistory.getTask(queryId3));
        Assertions.assertEquals(3, masterHistory.getTaskRunCount());

        // 3. Enable archive and set Config
        Config.enable_task_history_archive = true;
        
        // 4. Execute archiveHistory operation (master side)
        masterHistory.testArchiveHistory();

        // 5. Verify master state - only finished task runs should be archived and removed
        Assertions.assertNull(masterHistory.getTask(queryId1)); // SUCCESS - archived
        Assertions.assertNull(masterHistory.getTask(queryId2)); // FAILED - archived
        Assertions.assertNotNull(masterHistory.getTask(queryId3)); // PENDING - not archived
        Assertions.assertEquals(1, masterHistory.getTaskRunCount());

        // 6. Test follower replay functionality
        TestableTaskRunHistory followerHistory = new TestableTaskRunHistory();
        
        // First add the same task runs to follower
        TaskRunStatus followerStatus1 = new TaskRunStatus();
        followerStatus1.setQueryId(queryId1);
        followerStatus1.setTaskName("test_task_1");
        followerStatus1.setState(Constants.TaskRunState.SUCCESS);
        followerStatus1.setExpireTime(status1.getExpireTime());
        
        TaskRunStatus followerStatus2 = new TaskRunStatus();
        followerStatus2.setQueryId(queryId2);
        followerStatus2.setTaskName("test_task_2");
        followerStatus2.setState(Constants.TaskRunState.FAILED);
        followerStatus2.setExpireTime(status2.getExpireTime());
        
        followerHistory.addHistory(followerStatus1);
        followerHistory.addHistory(followerStatus2);
        
        Assertions.assertEquals(2, followerHistory.getTaskRunCount());

        // Replay the archive operation
        ArchiveTaskRunsLog archiveLog = (ArchiveTaskRunsLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ARCHIVE_TASK_RUNS);
        followerHistory.replay(archiveLog);

        // 7. Verify follower state is consistent with master
        Assertions.assertNull(followerHistory.getTask(queryId1));
        Assertions.assertNull(followerHistory.getTask(queryId2));
        Assertions.assertEquals(0, followerHistory.getTaskRunCount());
        
        // Verify the archived query IDs match
        List<String> archivedQueryIds = archiveLog.getTaskRuns();
        Assertions.assertNotNull(archivedQueryIds);
        Assertions.assertEquals(2, archivedQueryIds.size());
        Assertions.assertTrue(archivedQueryIds.contains(queryId1));
        Assertions.assertTrue(archivedQueryIds.contains(queryId2));
    }

    @Test
    public void testArchiveHistoryEditLogException() throws Exception {
        // 1. Prepare test data - create finished task runs
        String queryId1 = "exception_query_id_1";
        String queryId2 = "exception_query_id_2";
        
        TaskRunStatus status1 = new TaskRunStatus();
        status1.setQueryId(queryId1);
        status1.setTaskName("exception_task_1");
        status1.setState(Constants.TaskRunState.SUCCESS);
        status1.setExpireTime(System.currentTimeMillis() + 100000);
        
        TaskRunStatus status2 = new TaskRunStatus();
        status2.setQueryId(queryId2);
        status2.setTaskName("exception_task_2");
        status2.setState(Constants.TaskRunState.FAILED);
        status2.setExpireTime(System.currentTimeMillis() + 100000);
        
        // 2. Create a separate history for exception testing
        TestableTaskRunHistory exceptionHistory = new TestableTaskRunHistory();
        exceptionHistory.setEnableArchive(true);
        exceptionHistory.addHistory(status1);
        exceptionHistory.addHistory(status2);
        
        Assertions.assertEquals(2, exceptionHistory.getTaskRunCount());

        // 3. Mock EditLog.logArchiveTaskRuns to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logArchiveTaskRuns(any(ArchiveTaskRunsLog.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Enable archive and set Config
        Config.enable_task_history_archive = true;

        // Save initial state snapshot
        long initialTaskRunCount = exceptionHistory.getTaskRunCount();
        Assertions.assertNotNull(exceptionHistory.getTask(queryId1));
        Assertions.assertNotNull(exceptionHistory.getTask(queryId2));

        // 4. Execute archiveHistory operation
        // Note: archiveHistory() catches Throwable and logs warning, so exception won't propagate
        // However, the editlog write will fail, causing the replay() callback not to be invoked
        // This means addHistories() may have executed (persisting to table), but memory state
        // should remain because replay() removes from memory and it won't be called if editlog fails
        exceptionHistory.testArchiveHistory();

        // 5. Verify leader memory state after exception
        // Since editlog failed, the replay() callback was not invoked, so task runs should
        // remain in memory even though they may have been persisted to the table
        // The key point is that without successful editlog, followers won't see the archive,
        // so memory state should be preserved for consistency
        Assertions.assertEquals(initialTaskRunCount, exceptionHistory.getTaskRunCount());
        Assertions.assertNotNull(exceptionHistory.getTask(queryId1));
        Assertions.assertNotNull(exceptionHistory.getTask(queryId2));
    }

    @Test
    public void testArchiveHistoryEmptyList() throws Exception {
        // 1. Prepare test data - create task runs that are not finished
        String queryId1 = "running_query_id_1";
        String queryId2 = "pending_query_id_2";
        
        TaskRunStatus status1 = new TaskRunStatus();
        status1.setQueryId(queryId1);
        status1.setTaskName("running_task_1");
        status1.setState(Constants.TaskRunState.RUNNING); // Not finished
        status1.setExpireTime(System.currentTimeMillis() + 100000);
        
        TaskRunStatus status2 = new TaskRunStatus();
        status2.setQueryId(queryId2);
        status2.setTaskName("pending_task_2");
        status2.setState(Constants.TaskRunState.PENDING); // Not finished
        status2.setExpireTime(System.currentTimeMillis() + 100000);
        
        // 2. Add history to master
        masterHistory.addHistory(status1);
        masterHistory.addHistory(status2);
        
        Assertions.assertEquals(2, masterHistory.getTaskRunCount());

        // 3. Enable archive and set Config
        Config.enable_task_history_archive = true;
        
        // 4. Execute archiveHistory operation
        masterHistory.testArchiveHistory();

        // 5. Verify no task runs were archived (all are still in memory)
        Assertions.assertNotNull(masterHistory.getTask(queryId1));
        Assertions.assertNotNull(masterHistory.getTask(queryId2));
        Assertions.assertEquals(2, masterHistory.getTaskRunCount());
        
        // Verify no editlog was written (since there are no finished task runs)
        // We can't directly check this, but the behavior shows no archive occurred
    }

    @Test
    public void testArchiveHistoryMixedStates() throws Exception {
        // 1. Prepare test data - mix of finished and unfinished task runs
        List<String> finishedQueryIds = new ArrayList<>();
        List<String> unfinishedQueryIds = new ArrayList<>();
        
        // Create finished task runs
        for (int i = 1; i <= 3; i++) {
            String queryId = "finished_query_" + i;
            finishedQueryIds.add(queryId);
            TaskRunStatus status = new TaskRunStatus();
            status.setQueryId(queryId);
            status.setTaskName("finished_task_" + i);
            status.setState(i % 2 == 0 ? Constants.TaskRunState.SUCCESS : Constants.TaskRunState.FAILED);
            status.setExpireTime(System.currentTimeMillis() + 100000);
            masterHistory.addHistory(status);
        }
        
        // Create unfinished task runs
        for (int i = 1; i <= 2; i++) {
            String queryId = "unfinished_query_" + i;
            unfinishedQueryIds.add(queryId);
            TaskRunStatus status = new TaskRunStatus();
            status.setQueryId(queryId);
            status.setTaskName("unfinished_task_" + i);
            status.setState(Constants.TaskRunState.RUNNING);
            status.setExpireTime(System.currentTimeMillis() + 100000);
            masterHistory.addHistory(status);
        }
        
        Assertions.assertEquals(5, masterHistory.getTaskRunCount());

        // 2. Enable archive and set Config
        Config.enable_task_history_archive = true;
        
        // 3. Execute archiveHistory operation
        masterHistory.testArchiveHistory();

        // 4. Verify only finished task runs were archived
        for (String queryId : finishedQueryIds) {
            Assertions.assertNull(masterHistory.getTask(queryId),
                    "Finished task run " + queryId + " should be archived");
        }
        
        for (String queryId : unfinishedQueryIds) {
            Assertions.assertNotNull(masterHistory.getTask(queryId),
                    "Unfinished task run " + queryId + " should not be archived");
        }
        
        Assertions.assertEquals(2, masterHistory.getTaskRunCount());

        // 5. Test follower replay functionality
        TestableTaskRunHistory followerHistory = new TestableTaskRunHistory();
        
        // Add finished task runs to follower
        for (int i = 1; i <= 3; i++) {
            String queryId = "finished_query_" + i;
            TaskRunStatus status = new TaskRunStatus();
            status.setQueryId(queryId);
            status.setTaskName("finished_task_" + i);
            status.setState(i % 2 == 0 ? Constants.TaskRunState.SUCCESS : Constants.TaskRunState.FAILED);
            status.setExpireTime(System.currentTimeMillis() + 100000);
            followerHistory.addHistory(status);
        }
        
        Assertions.assertEquals(3, followerHistory.getTaskRunCount());

        // Replay the archive operation
        ArchiveTaskRunsLog archiveLog = (ArchiveTaskRunsLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ARCHIVE_TASK_RUNS);
        followerHistory.replay(archiveLog);

        // 6. Verify follower state is consistent with master
        Assertions.assertEquals(0, followerHistory.getTaskRunCount());
        for (String queryId : finishedQueryIds) {
            Assertions.assertNull(followerHistory.getTask(queryId));
        }
        
        // Verify the archived query IDs match
        List<String> archivedQueryIds = archiveLog.getTaskRuns();
        Assertions.assertNotNull(archivedQueryIds);
        Assertions.assertEquals(3, archivedQueryIds.size());
        for (String queryId : finishedQueryIds) {
            Assertions.assertTrue(archivedQueryIds.contains(queryId));
        }
    }

    @Test
    public void testArchiveHistoryFollowerReplayNormalCase() throws Exception {
        // 1. Prepare test data - create finished task runs in master
        String queryId1 = "replay_query_id_1";
        String queryId2 = "replay_query_id_2";
        
        TaskRunStatus status1 = new TaskRunStatus();
        status1.setQueryId(queryId1);
        status1.setTaskName("replay_task_1");
        status1.setState(Constants.TaskRunState.SUCCESS);
        status1.setExpireTime(System.currentTimeMillis() + 100000);
        
        TaskRunStatus status2 = new TaskRunStatus();
        status2.setQueryId(queryId2);
        status2.setTaskName("replay_task_2");
        status2.setState(Constants.TaskRunState.FAILED);
        status2.setExpireTime(System.currentTimeMillis() + 100000);
        
        masterHistory.addHistory(status1);
        masterHistory.addHistory(status2);
        Assertions.assertEquals(2, masterHistory.getTaskRunCount());

        // 2. Enable archive and archive in master
        Config.enable_task_history_archive = true;
        masterHistory.testArchiveHistory();
        
        Assertions.assertNull(masterHistory.getTask(queryId1));
        Assertions.assertNull(masterHistory.getTask(queryId2));

        // 3. Test follower replay
        TestableTaskRunHistory followerHistory = new TestableTaskRunHistory();
        
        // Add same task runs to follower (simulating follower state before archive)
        TaskRunStatus followerStatus1 = new TaskRunStatus();
        followerStatus1.setQueryId(queryId1);
        followerStatus1.setTaskName("replay_task_1");
        followerStatus1.setState(Constants.TaskRunState.SUCCESS);
        followerStatus1.setExpireTime(status1.getExpireTime());
        
        TaskRunStatus followerStatus2 = new TaskRunStatus();
        followerStatus2.setQueryId(queryId2);
        followerStatus2.setTaskName("replay_task_2");
        followerStatus2.setState(Constants.TaskRunState.FAILED);
        followerStatus2.setExpireTime(status2.getExpireTime());
        
        followerHistory.addHistory(followerStatus1);
        followerHistory.addHistory(followerStatus2);
        Assertions.assertEquals(2, followerHistory.getTaskRunCount());

        // Replay the archive operation
        ArchiveTaskRunsLog archiveLog = (ArchiveTaskRunsLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ARCHIVE_TASK_RUNS);
        
        Assertions.assertNotNull(archiveLog);
        Assertions.assertNotNull(archiveLog.getTaskRuns());
        Assertions.assertEquals(2, archiveLog.getTaskRuns().size());
        
        followerHistory.replay(archiveLog);

        // 4. Verify follower state matches master
        Assertions.assertEquals(0, followerHistory.getTaskRunCount());
        Assertions.assertNull(followerHistory.getTask(queryId1));
        Assertions.assertNull(followerHistory.getTask(queryId2));
    }
}

