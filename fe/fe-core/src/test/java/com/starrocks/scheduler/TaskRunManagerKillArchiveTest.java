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

package com.starrocks.scheduler;

import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TaskRunManagerKillArchiveTest {
    private TaskManager masterTaskManager;
    private TaskRunManager masterTaskRunManager;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        // The pseudo follower-journal queue is static and shared across tests in this class; clear it
        // so each test's replayNextJournal reads only its own OP_UPDATE_TASK_RUN entry.
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        masterTaskManager = new TaskManager();
        masterTaskRunManager = masterTaskManager.getTaskRunManager();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private TaskRun addRunningTaskRun(TaskManager taskManager, String taskName, long taskId,
                                      String queryId, Constants.TaskRunState state) {
        Task task = new Task(taskName);
        task.setId(taskId);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        taskManager.replayCreateTask(task);

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.setTaskId(taskId);
        taskRun.initStatus(queryId, System.currentTimeMillis());
        taskRun.getStatus().setState(state);
        taskManager.getTaskRunScheduler().addRunningTaskRun(taskRun);
        return taskRun;
    }

    // A-1: on the leader, force kill of a RUNNING task run archives it as FAILED + writes edit log.
    @Test
    public void testForceKillArchivesAsFailedOnLeader() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        long taskId = 1001L;
        String queryId = "kill_archive_q1";
        TaskRun taskRun = addRunningTaskRun(masterTaskManager, "kill_archive_leader", taskId, queryId,
                Constants.TaskRunState.RUNNING);
        Assertions.assertEquals(1, masterTaskRunManager.getTaskRunScheduler().getRunningTaskCount());

        masterTaskRunManager.killRunningTaskRun(taskRun, true, "killed by TaskCleaner due to timeout");

        // master: removed from running, archived as FAILED
        Assertions.assertEquals(0, masterTaskRunManager.getTaskRunScheduler().getRunningTaskCount());
        TaskRunStatus archived = masterTaskRunManager.getTaskRunHistory().getTask(queryId);
        Assertions.assertNotNull(archived);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, archived.getState());
        Assertions.assertEquals(-1, archived.getErrorCode());
        Assertions.assertEquals("killed by TaskCleaner due to timeout", archived.getErrorMessage());

        // edit log emitted as RUNNING -> FAILED
        TaskRunStatusChange change = (TaskRunStatusChange) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_TASK_RUN);
        Assertions.assertNotNull(change);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, change.getToStatus());
        Assertions.assertEquals(queryId, change.getQueryId());
        Assertions.assertEquals(-1, change.getErrorCode());

        // follower replays the edit log -> FAILED appears in follower history
        TaskManager followerTaskManager = new TaskManager();
        addRunningTaskRun(followerTaskManager, "kill_archive_leader", taskId, queryId,
                Constants.TaskRunState.RUNNING);
        followerTaskManager.replayUpdateTaskRun(change);
        Assertions.assertEquals(0, followerTaskManager.getTaskRunScheduler().getRunningTaskCount());
        TaskRunStatus followerArchived = followerTaskManager.getTaskRunHistory().getTask(queryId);
        Assertions.assertNotNull(followerArchived);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, followerArchived.getState());
    }

    // A-2: a run that finished naturally (SUCCESS) but is not yet archived must be archived with its
    // real terminal state (SUCCESS), never relabeled FAILED, and never dropped.
    @Test
    public void testForceKillPreservesNaturalFinishState() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        long taskId = 1002L;
        String queryId = "kill_archive_q2";
        TaskRun taskRun = addRunningTaskRun(masterTaskManager, "kill_archive_success", taskId, queryId,
                Constants.TaskRunState.SUCCESS);

        masterTaskRunManager.killRunningTaskRun(taskRun, true, "killed by TaskCleaner due to timeout");

        // archived as SUCCESS (not FAILED), removed from running map
        Assertions.assertEquals(0, masterTaskRunManager.getTaskRunScheduler().getRunningTaskCount());
        TaskRunStatus archived = masterTaskRunManager.getTaskRunHistory().getTask(queryId);
        Assertions.assertNotNull(archived);
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, archived.getState());

        // journal carries RUNNING -> SUCCESS
        TaskRunStatusChange change = (TaskRunStatusChange) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_TASK_RUN);
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS, change.getToStatus());
    }

    // A-2b: a run already present in history (e.g. checkRunningTaskRun archived it; we hold a stale
    // snapshot ref) must not be re-archived or overwritten to FAILED.
    @Test
    public void testForceKillDoesNotOverwriteAlreadyArchivedRun() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        long taskId = 1007L;
        String queryId = "kill_archive_q7";
        // run still shows RUNNING in our stale ref, but a SUCCESS record already exists in history
        TaskRun taskRun = addRunningTaskRun(masterTaskManager, "kill_archive_already", taskId, queryId,
                Constants.TaskRunState.RUNNING);
        TaskRunStatus alreadyArchived = TaskRunStatus.fromJson(taskRun.getStatus().toJSON());
        alreadyArchived.setState(Constants.TaskRunState.SUCCESS);
        masterTaskRunManager.getTaskRunHistory().addHistory(alreadyArchived);

        masterTaskRunManager.killRunningTaskRun(taskRun, true, "killed by TaskCleaner due to timeout");

        // history record untouched (still SUCCESS, not overwritten to FAILED), run removed
        Assertions.assertEquals(0, masterTaskRunManager.getTaskRunScheduler().getRunningTaskCount());
        Assertions.assertEquals(Constants.TaskRunState.SUCCESS,
                masterTaskRunManager.getTaskRunHistory().getTask(queryId).getState());
    }

    // A-2c: a run that FAILED naturally (its own error) but is not yet archived keeps its own error
    // code/message — it must not be relabeled with the kill reason.
    @Test
    public void testForceKillPreservesNaturalFailureError() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        long taskId = 1009L;
        String queryId = "kill_archive_q9";
        TaskRun taskRun = addRunningTaskRun(masterTaskManager, "kill_archive_natfail", taskId, queryId,
                Constants.TaskRunState.FAILED);
        taskRun.getStatus().setErrorCode(1064);
        taskRun.getStatus().setErrorMessage("syntax error from the query itself");

        masterTaskRunManager.killRunningTaskRun(taskRun, true, "killed by TaskCleaner due to timeout");

        TaskRunStatus archived = masterTaskRunManager.getTaskRunHistory().getTask(queryId);
        Assertions.assertNotNull(archived);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, archived.getState());
        Assertions.assertEquals(1064, archived.getErrorCode());
        Assertions.assertEquals("syntax error from the query itself", archived.getErrorMessage());
    }

    // A-3: on a follower, force kill removes the run but writes no edit log / history (replay does that).
    @Test
    public void testForceKillOnFollowerDoesNotArchive() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }
        };
        long taskId = 1003L;
        String queryId = "kill_archive_q3";
        TaskRun taskRun = addRunningTaskRun(masterTaskManager, "kill_archive_follower", taskId, queryId,
                Constants.TaskRunState.RUNNING);

        masterTaskRunManager.killRunningTaskRun(taskRun, true, "killed by TaskCleaner due to timeout");

        // removed from running map, but NOT archived and state untouched (waits for replay)
        Assertions.assertEquals(0, masterTaskRunManager.getTaskRunScheduler().getRunningTaskCount());
        Assertions.assertNull(masterTaskRunManager.getTaskRunHistory().getTask(queryId));
        Assertions.assertEquals(Constants.TaskRunState.RUNNING, taskRun.getStatus().getState());
    }

    // A-4: edit-log failure during force kill leaves memory unchanged (run stays RUNNING in the map,
    // nothing archived); recovery is left to the next cleaner tick.
    @Test
    public void testForceKillEditLogFailureLeavesMemoryUnchanged() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        long taskId = 1004L;
        String queryId = "kill_archive_q4";
        TaskRun taskRun = addRunningTaskRun(masterTaskManager, "kill_archive_editlog_fail", taskId, queryId,
                Constants.TaskRunState.RUNNING);

        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateTaskRun(any(TaskRunStatusChange.class));
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        Assertions.assertThrows(RuntimeException.class,
                () -> masterTaskRunManager.killRunningTaskRun(taskRun, true, "killed by TaskCleaner due to timeout"));

        // run still in running map, still RUNNING, not archived
        Assertions.assertEquals(1, masterTaskRunManager.getTaskRunScheduler().getRunningTaskCount());
        Assertions.assertEquals(Constants.TaskRunState.RUNNING, taskRun.getStatus().getState());
        Assertions.assertNull(masterTaskRunManager.getTaskRunHistory().getTask(queryId));
    }

    // A-5: the archived record is a snapshot — a later mutation of the live status (simulating the
    // still-running executor thread) must NOT change the FAILED history record.
    @Test
    public void testArchivedRecordIsSnapshotImmuneToLateMutation() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        long taskId = 1005L;
        String queryId = "kill_archive_q5";
        TaskRun taskRun = addRunningTaskRun(masterTaskManager, "kill_archive_snapshot", taskId, queryId,
                Constants.TaskRunState.RUNNING);

        masterTaskRunManager.killRunningTaskRun(taskRun, true, "killed by TaskCleaner due to timeout");

        TaskRunStatus archived = masterTaskRunManager.getTaskRunHistory().getTask(queryId);
        Assertions.assertNotNull(archived);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, archived.getState());

        // executor thread finishes late and flips the LIVE status to SUCCESS
        taskRun.getStatus().setState(Constants.TaskRunState.SUCCESS);

        // history record is a snapshot, unaffected
        Assertions.assertEquals(Constants.TaskRunState.FAILED,
                masterTaskRunManager.getTaskRunHistory().getTask(queryId).getState());
    }

    // A-6: a stale snapshot ref (the run was already archived+removed and a NEW run for the same
    // taskId scheduled) must NOT cause removal/archival of the newer run.
    @Test
    public void testForceKillStaleRefDoesNotTouchNewerRun() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        long taskId = 1008L;
        Task task = new Task("kill_archive_stale");
        task.setId(taskId);
        task.setType(Constants.TaskType.MANUAL);
        task.setDefinition("SELECT 1");
        task.setDbName("test_db");
        task.setCatalogName("test_catalog");
        masterTaskManager.replayCreateTask(task);

        // stale reference (simulating the cleaner's pre-lock snapshot)
        TaskRun staleTaskRun = TaskRunBuilder.newBuilder(task).build();
        staleTaskRun.setTaskId(taskId);
        staleTaskRun.initStatus("stale_query_id", System.currentTimeMillis());
        staleTaskRun.getStatus().setState(Constants.TaskRunState.RUNNING);
        masterTaskRunManager.getTaskRunScheduler().addRunningTaskRun(staleTaskRun);

        // simulate checkRunningTaskRun: the stale run is archived + removed, then a newer run for the
        // same taskId is scheduled
        masterTaskRunManager.getTaskRunScheduler().removeRunningTask(taskId);
        TaskRun newerTaskRun = TaskRunBuilder.newBuilder(task).build();
        newerTaskRun.setTaskId(taskId);
        newerTaskRun.initStatus("newer_query_id", System.currentTimeMillis());
        newerTaskRun.getStatus().setState(Constants.TaskRunState.RUNNING);
        masterTaskRunManager.getTaskRunScheduler().addRunningTaskRun(newerTaskRun);

        // force-kill the STALE reference
        masterTaskRunManager.killRunningTaskRun(staleTaskRun, true, "killed by TaskCleaner due to timeout");

        // the newer run must be untouched: still the running run for taskId, not archived
        Assertions.assertEquals(1, masterTaskRunManager.getTaskRunScheduler().getRunningTaskCount());
        Assertions.assertSame(newerTaskRun, masterTaskRunManager.getTaskRunScheduler().getRunningTaskRun(taskId));
        Assertions.assertNull(masterTaskRunManager.getTaskRunHistory().getTask("newer_query_id"));
    }

    // A-1b: end-to-end — removeExpiredTaskRuns on the leader archives a timed-out run as FAILED.
    @Test
    public void testRemoveExpiredTaskRunsArchivesTimedOutRunAsFailed() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        new MockUp<TaskRun>() {
            @Mock
            public int getExecuteTimeoutS() {
                return 1;
            }
        };
        long taskId = 1006L;
        String queryId = "kill_archive_q6";
        TaskRun taskRun = addRunningTaskRun(masterTaskManager, "kill_archive_e2e", taskId, queryId,
                Constants.TaskRunState.RUNNING);
        // createTime well in the past so (now - createTime) > getExecuteTimeoutS()*1000
        taskRun.getStatus().setCreateTime(System.currentTimeMillis() - 5000);

        masterTaskManager.removeExpiredTaskRuns(false);

        Assertions.assertEquals(0, masterTaskRunManager.getTaskRunScheduler().getRunningTaskCount());
        TaskRunStatus archived = masterTaskRunManager.getTaskRunHistory().getTask(queryId);
        Assertions.assertNotNull(archived);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, archived.getState());

        TaskRunStatusChange change = (TaskRunStatusChange) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPDATE_TASK_RUN);
        Assertions.assertNotNull(change);
        Assertions.assertEquals(Constants.TaskRunState.FAILED, change.getToStatus());
        Assertions.assertEquals(queryId, change.getQueryId());
    }
}
